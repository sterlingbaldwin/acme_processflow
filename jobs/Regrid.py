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
from lib.util import print_debug
from lib.util import print_message
from lib.util import cmd_exists
from lib.util import get_climo_output_files
from lib.util import print_line


class Regrid(object):
    """
    A wrapper around ncremap, used to regrid model data
    """

    def __init__(self, config, event_list):
        self.event_list = event_list
        self.config = {}
        self._status = JobStatus.INVALID
        self._type = 'regid'
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = []
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.inputs = {
            'data_type': '',
            'account': '',
            'ui': '',
            'start_year': '',
            'end_year': '',
            'caseId': '',
            'input_path': '',
            'output_path': '',
            'regrid_map_path': '',
            'year_set': '',
            'run_scripts_path': '',
            'file_type': ''
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
        
        if config['data_type'] not in ['ocn', 'lnd', 'atm']:
            self.status = JobStatus.INVALID
            msg = 'regrid-{}-{}: invalid data_type requested'.format(self.start_year, self.end_year)
            logging.error(msg)
            return

        # Assuming monthly history files
        expected_files = list()
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                name = '{year:04d}-{month:02d}'.format(year=year, month=month)
                expected_files.append(name)

        for infile in os.listdir(self.config['input_path']):
            for item in expected_files:
                if re.search(pattern=item, string=infile):
                    expected_files.remove(item)
                    break

        if len(expected_files) > 0:
            msg = 'regrid-{start:04d}-{end:04d}: missing input files {files}'.format(
                start=self.start_year, end=seld.end_year, files=expected_files)
            logging.error(msg)
            self.status = JobStatus.INVALID
            return

        self.slurm_args['account'] = self.config.get('account', '')
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if self.year_set == 0:
            self.status = JobStatus.INVALID
            return
        
        # nothing has marked the status as invalid so we should be good
        self.status = JobStatus.VALID
        return 0
    
    def postvalidate(self):
        """
        Post execution validation, also run before execution to determine if the output already extists

        Returns True if the job completed succesfuly, False otherwise
        """
        msg = 'starting postvalidation for {job}-{start:04d}-{end:04d}'.format(
            job=self.type, start=self.start_year, end=self.end_year)
        logging.info(msg)
        set_start_year = self.config['start_year']
        set_end_year = self.config['end_year']
        output_path = self.config['output_path']

        # check that the output directory exists
        if not os.path.exists(output_path):
            msg = 'Regrid output directory missing'
            logging.error(msg)
            return False

        # check that the output files exist
        file_list = os.listdir(output_path)
        expected_files = list()
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                name = '{year:04d}-{month:02d}'.format(year=year, month=month)
                expected_files.append(name)

        for outfile in os.listdir(self.config['output_path']):
            for item in expected_files:
                if re.search(pattern=item, string=infile):
                    expected_files.remove(item)
                    break

        if len(expected_files) > 0:
            msg = 'regrid-{start:04d}-{end:04d}: missing output files {files}'.format(
                start=self.start_year, end=self.end_year, files=expected_files)
            logging.error(msg)
            return False

        return True

    def get_input_file_list(self):
        """
        Gets the list of required input files, returns False if all needed files arent found
        """
        # first build the list of expected files
        input_files = list()
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                nfilestr = ''
                if self.data_type in ['atm', 'lnd']:
                    nfilestr += self.config['caseId'] + '.'
                nfilestr += '{file_type}.{year:04d}-{month:02d}'.format(
                    file_type=self.config['file_type'],
                    year=year,
                    month=month)
                if self.data_type == 'ocn':
                    nfilestr += '-01'
                nfilestr += '.nc'
                input_files.append(
                    os.path.join(
                        self.config['input_path'],
                        nfilestr))

        # second check each file exists
        for infile in input_files:
            if not os.path.exists(infile):
                return False
        return input_files


    def execute(self, dryrun=False):
        """
        Calls ncremap in a subprocess

        Returns:
            0 if precomputed
            -1 if waiting on additional data

        """

        # check if the output already exists and the job actually needs to run
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            return 0
        
        input_files = self.get_input_file_list()
        if not input_files:
            return -1
        self.output_path = self.config['output_path']

        cmd = ['ncremap']

        if self.config['data_type'] == 'lnd':
            cmd += ['-P', 'clm']
        elif self.config['data_type'] == 'ocn':
            cmd += ['-P', 'mpas']
        elif self.config['data_type'] == 'atm':
            cmd += ['-P', 'alm']
        else:
            msg = 'Unsupported regrid type'
            logging.error(msg)
            self.status = FAILED
            return 0

        cmd += ['-O', self.config['output_path'],
                '-s', self.config['source_grid_path'],
                '-d', self.config['destination_grid_path'],
                ' '.join(input_files)]
        slurm_command = ' '.join(cmd)

        # Submitting the job to SLURM
        expected_name = '{type}_{data_type}_{start:04d}_{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            type=self.type,
            data_type=self.config['data_type'])
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
            line=msg,
            event_list=self.event_list,
            current_state=True)
        self.job_id = slurm.batch(run_script, '--oversubscribe')

        return self.job_id

    def __str__(self):
        return json.dumps({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
        }, sort_keys=True, indent=4)

    @property
    def data_type(self):
        return self.config['data_type']

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
