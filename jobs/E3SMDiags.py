import os
import logging

from subprocess import Popen, PIPE
from pprint import pformat
from datetime import datetime
from shutil import copyfile

from lib.util import render
from lib.util import get_climo_output_files
from lib.util import create_symlink_dir
from lib.events import Event_list
from lib.slurm import Slurm
from JobStatus import JobStatus, StatusMap


class E3SMDiags(object):
    def __init__(self, config, event_list):
        self.event_list = event_list
        self.inputs = {
            'regrided_climo_path': '',
            'reference_data_path': '',
            'test_data_path': '',
            'test_name': '',
            'seasons': '',
            'backend': '',
            'sets': '',
            'results_dir': '',
            'template_path': '',
            'run_scripts_path': '',
            'start_year': '',
            'end_year': '',
            'year_set': '',
            'experiment': '',
            'web_dir': '',
            'host_url': '',
            'output_path': ''
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 2 hour max run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
        }
        self.start_time = None
        self.end_time = None
        self.config = {}
        self._status = JobStatus.INVALID
        self._type = "e3sm_diags"
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = ['ncclimo']
        self.prevalidate(config)

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
        })

    def prevalidate(self, config):
        for key, val in config.items():
            if key in self.inputs:
                self.config[key] = val
                if key == 'sets':
                    if isinstance(val, int):
                        self.config[key] = [self.config[key]]
                    elif isinstance(val, str):
                        self.config[key] = [int(self.config[key])]

        valid = True
        for key, val in self.config.items():
            if not val or val == '':
                valid = False
                break

        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if self.year_set == 0:
            self.status = JobStatus.INVALID
        if valid:
            self.status = JobStatus.VALID

    def postvalidate(self):
        if not os.path.exists(self.config['results_dir']):
            return False
        contents = os.listdir(self.config['results_dir'])
        if 'viewer' not in contents:
            return False
        if 'index.html' not in os.listdir(os.path.join(self.config['results_dir'], 'viewer')):
            return False
        return True

    def execute(self):

        # Check if the output already exists
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'ACME diags already computed, skipping'
            self.event_list.push(message=message)
            logging.info(message)
            return 0
        # render the parameters file
        output_path = self.config['output_path']
        template_out = os.path.join(
            output_path,
            'params.py')
        variables = {
            'sets': self.config['sets'],
            'backend': self.config['backend'],
            'reference_data_path': self.config['reference_data_path'],
            'test_data_path': self.config['regrided_climo_path'],
            'test_name': self.config['test_name'],
            'seasons': self.config['seasons'],
            'results_dir': self.config['results_dir']
        }
        render(
            variables=variables,
            input_path=self.config.get('template_path'),
            output_path=template_out)
        
        run_name = 'e3sm_diag_{start:04d}_{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'))
        template_copy = os.path.join(
            self.config.get('run_scripts_path'),
            run_name)
        copyfile(
            src=template_out,
            dst=template_copy)

        # setup sbatch script
        expected_name = 'e3sm_diag_{name}'.format(
            name=run_name)
        run_script = os.path.join(
            self.config.get('run_scripts_path'),
            run_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=run_script + '.out')

        cmd = 'acme_diags_driver.py -p {template}'.format(
            template=template_out)
        slurm_args_str = ['#SBATCH {value}\n'.format(value=v) for k, v in self.slurm_args.items()]
        slurm_prefix = ''.join(slurm_args_str)
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(cmd)

        slurm = Slurm()
        self.job_id = slurm.batch(run_script, '--oversubscribe')
        status = slurm.showjob(self.job_id)
        self.status = StatusMap[status.get('JobState')]
        message = '{type} id: {id} changed state to {state}'.format(
            type=self.type,
            id=self.job_id,
            state=self.status)
        logging.info(message)
        self.event_list.push(message=message)

        return self.job_id


    def __str__(self):
        return pformat({
            'type': self.type,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
            'year_set': self.year_set
        }, indent=4)

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
