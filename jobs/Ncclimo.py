# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import re
import json
import sys
import logging

from uuid import uuid4
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
        self.status = JobStatus.INVALID
        self.type = 'ncclimo'
        self.uuid = uuid4().hex
        self.yearset = config.get('yearset', 0)
        self.job_id = 0
        self.depends_on = []
        self.start_time = None
        self.end_time = None
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
        self.proc = None
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-05:00', # 2 hours run time
            'num_machines': '-N 1', # run on one machine
            # 'oversubscribe': '--oversubscribe'
        }
        self.prevalidate(config)

    def get_type(self):
        """
        Returns job type
        """
        return self.type

    def execute(self, batch=False):
        """
        Calls ncclimo in a subprocess
        """

        # check if the output already exists and the job actually needs to run
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'Ncclimo job already computed, skipping'
            self.event_list.push(message=message)
            return 0

        self.start_time = datetime.now()
        # ncclimo = 'ncclimo'
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
        if not batch:
            # Not running in batch mode
            while True:
                try:
                    self.proc = Popen(
                        cmd,
                        stdout=PIPE,
                        stderr=PIPE,
                        shell=False)
                except:
                    print 'problem starting job'
                    sleep(1)
                else:
                    print 'started job'
                    break
            self.status = JobStatus.RUNNING
            done = 2
            console_output = ''
            while done != 0:
                done = self.proc.poll()
                lines = self.proc.stdout.readlines()
                for line in lines:
                    console_output += line
                lines = self.proc.stderr.readlines()
                for line in lines:
                    console_output += line
                print console_output
                if done < 0:
                    break
                sleep(1)
                self.outputs['console_output'] = console_output
                print console_output

            self.status = JobStatus.COMPLETED
            return 0
        else:
            # Submitting the job to SLURM
            expected_name = 'ncclimo_set_{year_set}_{start}_{end}_{uuid}'.format(
                year_set=self.config.get('year_set'),
                start='{:04d}'.format(self.config.get('start_year')),
                end='{:04d}'.format(self.config.get('end_year')),
                uuid=self.uuid[:5])
            run_script = os.path.join(self.config.get('run_scripts_path'), expected_name)

            self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
            self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

            with open(run_script, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'
                batchfile.write(slurm_prefix)
                slurm_command = ' '.join(cmd)
                batchfile.write(slurm_command)

            slurm = Slurm()
            self.job_id = slurm.batch(run_script, '--oversubscribe')

            self.status = JobStatus.SUBMITTED
            message = '{type} id: {id} changed state to {state}'.format(
                type=self.get_type(),
                id=self.job_id,
                state=self.status)
            logging.info(message)
            self.event_list.push(message=message)

            return self.job_id

    def set_status(self, status):
        self.status = status

    def save(self, conf_path):
        """
        Saves job configuration to a json file at conf_path
        """
        try:
            with open(conf_path, 'r') as infile:
                config = json.load(infile)
            with open(conf_path, 'w') as outfile:
                config[self.uuid]['inputs'] = self.config
                config[self.uuid]['outputs'] = self.outputs
                config[self.uuid]['type'] = self.type
                json.dump(config, outfile, indent=4)
        except Exception as e:
            print_message('Error saving configuration file')
            print_debug(e)
            raise

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id,
        })

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
        return 0

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
            set_start_year=set_start_year,
            set_end_year=set_end_year)
        if len(file_list) < 12:
            return False

        # Second check the regrid directory
        if not os.path.exists(regrid_dir):
            return False
        file_list = get_climo_output_files(
            input_path=regrid_dir,
            set_start_year=set_start_year,
            set_end_year=set_end_year)
        if len(file_list) < 17:
            return False
        
        return True
