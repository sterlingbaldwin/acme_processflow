import os
import logging

from uuid import uuid4
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


class ACMEDiags(object):
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
            'host_url': ''
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
        self.status = JobStatus.INVALID
        self.type = "acme_diags"
        self.year_set = 0
        self.job_id = 0
        self.uuid = uuid4().hex
        self.depends_on = ['ncclimo']
        self.prevalidate(config)
    
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
        if valid:
            self.status = JobStatus.VALID
        
        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))

    def postvalidate(self):
        if not os.path.exists(self.config['results_dir']):
            return False
        contents = os.listdir(self.config['results_dir'])
        if 'viewer' not in contents:
            return False
        if 'index.html' not in os.listdir(os.path.join(self.config['results_dir'], 'viewer')):
            return False
        return True        
    
    def execute(self, batch='slurm', debug=False):
        
        # Check if the output already exists
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'ACME diags already computed, skipping'
            self.event_list.push(message=message)
            logging.info(message)
            return 0
        
        # set start run time
        self.start_time = datetime.now()

        # create results directory
        run_dir = self.config.get('results_dir')
        if not os.path.exists(run_dir):
            os.makedirs(run_path)
        
        # render the parameters file
        template_out = os.path.join(
            run_dir, 'params.py')
        render(
            variables=self.config,
            input_path=self.config.get('template_path'),
            output_path=template_out)
        run_script_template_out = os.path.join(
            self.config.get('run_scripts_path'),
            'acme_diag_{start}_{end}'.format(
                start=self.config.get('start_year'),
                end=self.config.get('end_year')))
        copyfile(
            src=template_out,
            dst=run_script_template_out)
        
        # setup input directory 
        file_list = get_climo_output_files(
            input_path=self.config.get('regrided_climo_path'),
            set_start_year=self.config.get('start_year'),
            set_end_year=self.config.get('end_year'))
        create_symlink_dir(
            src_dir=self.config.get('regrided_climo_path'),
            src_list=file_list,
            dst=self.config.get('test_data_path'))
        
        # setup sbatch script
        expected_name = 'acme_diag_set_{start:04d}_{end:04d}_{uuid}'.format(
            set=self.config.get('year_set'),
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            uuid=self.uuid[:5])
        run_script = os.path.join(self.config.get('run_scripts_path'), expected_name)
        if debug:
            print 'run_script: {}'.format(run_script)
        self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
        self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

        cmd = ['acme_diags_driver.py', '-p', template_out]
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            slurm_args_str = ['#SBATCH {value}\n'.format(value=v) for k, v in self.slurm_args.items()]
            slurm_prefix = ''.join(slurm_args_str)
            batchfile.write(slurm_prefix)
            slurm_command = ' '.join(cmd)
            batchfile.write(slurm_command)

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
            'uuid': self.uuid,
            'job_id': self.job_id,
            'year_set': self.year_set
        }, indent=4)

    def get_type(self):
        return self.type

    def set_status(self, status):
        self.status = status
