# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import re
import json
import sys
import logging

from pprint import pformat
from subprocess import Popen, PIPE
from time import sleep
from datetime import datetime

from lib.util import print_debug
from lib.util import print_message
from lib.events import Event_list
from lib.util import cmd_exists
from lib.slurm import Slurm
from JobStatus import JobStatus


class Timeseries(object):
    """
    A wrapper around ncclimo, used to compute the climotologies from raw output data
    """

    def __init__(self, config, event_list):
        self.event_list = event_list
        self.config = {}
        self._status = JobStatus.INVALID
        self._type = 'timeseries'
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = []
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.inputs = {
            'year_set': '',
            'annual_mode': '',
            'start_year': '',
            'end_year': '',
            'output_directory': '',
            'var_list': '',
            'caseId': '',
            'run_scripts_path': '',
            'regrid_map_path': '',
            'file_list': '',
        }
        self.slurm_args = {
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-05:00',  # 2 hours run time
            'num_machines': '-N 1',  # run on one machine
            'oversubscribe': '--oversubscribe'
        }
        self.prevalidate(config)

    def prevalidate(self, config):
        """
        Prerun validation for inputs
        """
        if self.status == JobStatus.VALID:
            return 0
        for i in config:
            if i in self.inputs:
                self.config[i] = config.get(i)
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if self.year_set == 0:
            self.status = JobStatus.INVALID
            return
        self.output_path = self.config['output_directory']
        self.status = JobStatus.VALID

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
            'year_set': self.year_set
        })

    def execute(self, dryrun=False):
        """
        Calls ncclimo in a subprocess
        """
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'Timeseries already computed, skipping'
            self.event_list.push(message=message)
            return 0

        file_list = self.config['file_list']
        file_list.sort()
        list_string = ' '.join(file_list)
        slurm_command = ' '.join([
            'ncclimo',
            '-a', self.config['annual_mode'],
            '-c', self.config['caseId'],
            '-v', ','.join(self.config['var_list']),
            '-s', str(self.config['start_year']),
            '-e', str(self.config['end_year']),
            '-o', self.config['output_directory'],
            '--map={}'.format(self.config.get('regrid_map_path')),
            list_string
        ])

        # Submitting the job to SLURM
        expected_name = '{type}_{start:04d}_{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            type=self.type)
        run_script = os.path.join(
            self.config.get('run_scripts_path'),
            expected_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=run_script + '.out')
        slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s]
                                  for s in self.slurm_args]) + '\n'

        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(slurm_command)

        slurm = Slurm()
        print 'submitting to queue {type}: {start:04d}-{end:04d}'.format(
            type=self.type,
            start=self.start_year,
            end=self.end_year)
        self.job_id = slurm.batch(run_script, '--oversubscribe')
        self.status = JobStatus.SUBMITTED
        message = '{type} id: {id} changed state to {state}'.format(
            type=self.type,
            id=self.job_id,
            state=self.status)
        logging.info(message)
        self.event_list.push(message=message)

        return self.job_id

    def _find_year(self, filename):
        pattern = '_\d\d\d\d\d\d_\d\d\d\d\d\d.*'
        match = re.search(pattern=pattern, string=filename)
        if not match:
            return False, False
        start = int(filename[match.start() + 1: match.start() + 5])
        end = int(filename[match.start() + 8: match.start() + 12])
        return start, end

    def postvalidate(self):
        """
        Post execution validation
        """
        output_dir = os.listdir(self.config.get('output_directory'))
        if not isinstance(self.config.get('var_list'), list):
            self.config['var_list'] = [self.config.get('var_list')]
        complete = True
        for var in self.config.get('var_list'):
            found = False
            for out in output_dir:
                if var in out:
                    start, end = self._find_year(out)
                    if start == self.config['start_year'] and end == self.config['end_year']:
                        found = True
                        break

            if not found:
                complete = False
                break
        return complete

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
