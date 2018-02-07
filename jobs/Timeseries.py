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

from lib.events import EventList
from lib.slurm import Slurm
from JobStatus import JobStatus
from lib.util import (print_debug,
                      print_message,
                      cmd_exists,
                      print_line)


class Timeseries(object):
    """
    A wrapper around ncclimo, used to compute the climotologies from raw output data
    """

    def __init__(self, config, event_list):
        self.event_list = event_list
        self.config = {}
        self._status = JobStatus.INVALID
        self._type = 'timeseries'
        self.filemanager = config['filemanager']
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = []
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.inputs = {
            'account': '',
            'ui': '',
            'year_set': '',
            'annual_mode': '',
            'start_year': '',
            'end_year': '',
            'native_output_directory': '',
            'regrid_output_directory': '',
            'var_list': '',
            'caseId': '',
            'run_scripts_path': '',
            'regrid_map_path': '',
            'file_list': '',
        }
        self.slurm_args = {
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-05:00',  # 5 hours run time
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
        invalid = False
        for i in config:
            if i in self.inputs:
                self.config[i] = config.get(i)

        account = self.config.get('account')
        if account:
            self.slurm_args['account'] = '-A {}'.format(account)

        # make sure the run_scripts_path is setup
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        # make sure the var_list is setup
        if not self.config.get('var_list'):
            invalid = True
        if not isinstance(self.config.get('var_list'), list):
            self.config['var_list'] = [self.config.get('var_list')]
        # make sure the job has been added to a year_set
        if self.year_set == 0:
            invalid = True
            return
        self.output_path = self.config['regrid_output_directory']

        if invalid:
            self.status = JobStatus.INVALID
        else:
            self.status = JobStatus.VALID

    def __str__(self):
        return json.dumps({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
            'year_set': self.year_set
        }, sort_keys=True, indent=4)

    def execute(self, dryrun=False):
        """
        Submits ncclimo to slurm after checking if it had been previously run
        """
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            return 0

        file_list = self.filemanager.get_file_paths_by_year(
            start_year=self.start_year,
            end_year=self.end_year,
            _type='atm')
        file_list.sort()
        list_string = ' '.join(file_list)
        slurm_command = ' '.join([
            '~zender1/bin/ncclimo',
            '-a', self.config['annual_mode'],
            '-c', self.config['caseId'],
            '-v', ','.join(self.config['var_list']),
            '-s', str(self.config['start_year']),
            '-e', str(self.config['end_year']),
            '--ypf={}'.format(self.end_year - self.start_year + 1),
            '-o', self.config['regrid_output_directory'],
            '-O', self.config['native_output_directory'],
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
        msg = 'Submitting to queue {type}: {start:04d}-{end:04d}'.format(
            type=self.type,
            start=self.start_year,
            end=self.end_year)
        print_line(
            ui=self.config.get('ui', False),
            line=msg,
            event_list=self.event_list,
            current_state=True)
        self.job_id = slurm.batch(run_script, '--oversubscribe')

        return self.job_id

    def _find_year(self, filename):
        pattern = r'\d{6}_\d{6}'
        match = re.search(pattern=pattern, string=filename)
        if not match:
            return False, False
        start = int(filename[match.start(): match.start() + 4])
        end = int(filename[match.start() + 7: match.start() + 11])
        return start, end

    def postvalidate(self):
        """
        Post execution validation
        """
        for path in [self.config.get('native_output_directory'), self.config.get('regrid_output_directory')]:
            contents = os.listdir(path)
            
            # Loop through each variable and make sure its been put into the output directory
            for var in self.config.get('var_list'):
                pattern = '{var}_{start:04d}01_{end:04d}12'.format(
                    var=var, start=self.start_year, end=self.end_year)
                file_list = [x for x in contents if re.search(string=x, pattern=pattern)]
                if not file_list:
                    return False
        return True

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
