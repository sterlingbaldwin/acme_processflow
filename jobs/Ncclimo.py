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
                      get_climo_output_files,
                      print_line)


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
        self.inputs = {
            'account': '',
            'ui': '',
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
        for i in config:
            if i in self.inputs:
                self.config[i] = config.get(i)
        if config.get('ui') is None:
            config['ui'] = False
        all_inputs = True
        for i in self.inputs:
            if i not in config:
                all_inputs = False
                msg = 'Argument {} missing for Ncclimo, prevalidation failed'.format(
                    i)
                print_line(
                    ui=self.config.get('ui', False),
                    line=msg,
                    event_list=self.event_list)
                break
        if all_inputs:
            self.status = JobStatus.VALID
        else:
            JobStatus.INVALID
            return 0
        self.config['output_directory'] = self.config['regrid_output_directory']
        self.output_path = self.config['regrid_output_directory']
        self.slurm_args['account'] = self.config.get('account', '')
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if self.year_set == 0:
            self.status = JobStatus.INVALID
        return 0

    def execute(self, dryrun=False):
        """
        Calls ncclimo in a subprocess
        """

        # check if the output already exists and the job actually needs to run
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
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
            '--no_amwg_links',
        ]
        slurm_command = ' '.join(cmd)

        # Submitting the job to SLURM
        expected_name = '{type}_{start:04d}_{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            type=self.type)
        run_script = os.path.join(self.config.get(
            'run_scripts_path'), expected_name)
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

        if dryrun:
            self.status = JobStatus.COMPLETED
            return 0

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

    def postvalidate(self):
        """
        Post execution validation, also run before execution to determine if the output already extists
        """
        set_start_year = self.config.get('start_year')
        set_end_year = self.config.get('end_year')
        climo_dir = self.config.get('climo_output_directory')
        regrid_dir = self.config.get('regrid_output_directory')
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
        if len(file_list) < 12:
            return False

        return True

    def __str__(self):
        return json.dumps({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
        }, sort_keys=True, indent=4)

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
