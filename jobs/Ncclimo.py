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
from lib.util import get_climo_output_files
from lib.slurm import Slurm
from JobStatus import JobStatus


class Climo(object):
    """
    A wrapper around ncclimo, used to compute the climotologies from raw output data
    """
    def __init__(self, config, event_list):
        self.event_list = event_list
        self.config = {}
        self._status = JobStatus.INVALID
        self._type = 'ncclimo'
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = []
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.outputs = {
            'status': self.status,
            'climos': '',
            'regrid': '',
            'console_output': ''
        }
        self.inputs = {
            'start_year': '',
            'end_year': '',
            'caseId': '',
            'annual_mode': 'sdd',
            'input_directory': '',
            'climo_output_directory': '',
            'regrid_output_directory': '',
            'regrid_map_path': '',
            'year_set': '',
            'run_scripts_path': ''
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-05:00', # 2 hours run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
            # 'oversubscribe': '--oversubscribe'
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
        self.config['output_directory'] = self.config['regrid_output_directory']
        self.output_path = self.config['regrid_output_directory']
        all_inputs = True
        for i in self.inputs:
            if i not in self.config:
                all_inputs = False
                message = 'Argument {} missing for Ncclimo, prevalidation failed'.format(i)
                self.event_list.push(message=message)
                break

        self.status = JobStatus.VALID if all_inputs else JobStatus.INVALID
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if self.year_set == 0:
            self.status = JobStatus.INVALID
        return 0

    def execute(self):
        """
        Calls ncclimo in a subprocess
        """

        # check if the output already exists and the job actually needs to run
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'Ncclimo job already computed, skipping'
            self.event_list.push(message=message)
            return 0

        self.output_path = self.config['regrid_output_directory']

        cmd = [
            'ncclimo',
            '-c', self.config['caseId'],
            '-a', self.config['annual_mode'],
            '-s', str(self.config['start_year']),
            '-e', str(self.config['end_year']),
            '-i', self.config['input_directory'],
            '-r', self.config['regrid_map_path'],
            '-o', self.config['climo_output_directory'],
            '-O', self.config['regrid_output_directory'],
            '-l'
        ]
        slurm_command = ' '.join(cmd)


        # Submitting the job to SLURM
        expected_name = 'ncclimo_set_{year_set}_{start}_{end}'.format(
            year_set=self.config.get('year_set'),
            start='{:04d}'.format(self.config.get('start_year')),
            end='{:04d}'.format(self.config.get('end_year')))
        run_script = os.path.join(self.config.get('run_scripts_path'), expected_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')
        slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'

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

    def postvalidate(self):
        """
        Post execution validation, also run before execution to determine if the output already extists
        """
        set_start_year = self.config.get('start_year')
        set_end_year = self.config.get('end_year')
        climo_dir = self.config.get('climo_output_directory')
        regrid_dir = self.config.get('regrid_output_directory')
        if not set_start_year or \
           not set_end_year or \
           not climo_dir or \
           not regrid_dir:
            self.status = JobStatus.INVALID
            return False

        # First check the climo directory
        if not os.path.exists(climo_dir):
            return False
        file_list = get_climo_output_files(
            input_path=climo_dir,
            start_year=set_start_year,
            end_year=set_end_year)
        if len(file_list) < 12:
            return False

        # Second check the regrid directory
        if not os.path.exists(regrid_dir):
            return False
        file_list = get_climo_output_files(
            input_path=regrid_dir,
            start_year=set_start_year,
            end_year=set_end_year)
        if len(file_list) < 17:
            return False

        return True

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
        })

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

