import os
import json
import logging

from subprocess import Popen, PIPE
from pprint import pformat
from datetime import datetime
from shutil import copyfile

from lib.events import EventList
from lib.slurm import Slurm
from JobStatus import JobStatus, StatusMap
from lib.util import (render,
                      get_climo_output_files,
                      create_symlink_dir,
                      print_line)


class E3SMDiags(object):
    def __init__(self, config, event_list):
        self.event_list = event_list
        self.inputs = {
            'ui': '',
            'regrid_base_path': '',
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
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-05:00',  # 2 hour max run time
            'num_machines': '-N 1',  # run on one machine
            'oversubscribe': '--oversubscribe'
        }
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.config = {}
        self._status = JobStatus.INVALID
        self.host_suffix = '/viewer/index.html'
        self._type = "e3sm_diags"
        self.year_set = config.get('year_set', 0)
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = ['ncclimo']
        self.messages = []
        self.prevalidate(config)

    def __str__(self):
        return json.dumps({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'job_id': self.job_id,
            'messages': self.messages
        }, sort_keys=True, indent=4)

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
            if val == '':
                valid = False
                msg = '{0}: {1} is missing or empty'.format(key, val)
                self.messages.append(msg)
                break

        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))
        if isinstance(self.config['seasons'], str):
            self.config['seasons'] = [self.config['seasons']]
        if self.year_set == 0:
            self.messages.append('invalid year_set')
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
        try:
            for item in ['params.py', 'params.pyc', 'viewer']:
                if item in contents:
                    contents.remove(item)
        except:
            return False
        else:
            return bool(len(contents) >= len(self.config['sets']))

    def execute(self, dryrun=False):

        # Check if the output already exists
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            return 0
        # render the parameters file
        self.output_path = self.config['output_path']
        template_out = os.path.join(
            self.output_path,
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

        run_name = '{type}_{start:04d}_{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            type=self.type)
        template_copy = os.path.join(
            self.config.get('run_scripts_path'),
            run_name)
        copyfile(
            src=template_out,
            dst=template_copy)

        # Create directory of regridded climos
        file_list = get_climo_output_files(
            input_path=self.config['regrid_base_path'],
            start_year=self.start_year,
            end_year=self.end_year)
        create_symlink_dir(
            src_dir=self.config['regrid_base_path'],
            src_list=file_list,
            dst=self.config['regrided_climo_path'])

        # setup sbatch script
        run_script = os.path.join(
            self.config.get('run_scripts_path'),
            run_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=run_script + '.out')

        cmd = 'acme_diags_driver.py -p {template}'.format(
            template=template_out)

        slurm_args_str = ['#SBATCH {value}\n'.format(
            value=v) for k, v in self.slurm_args.items()]
        slurm_prefix = ''.join(slurm_args_str)
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(cmd)

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
        status = slurm.showjob(self.job_id)
        self.status = StatusMap[status.get('JobState')]

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
