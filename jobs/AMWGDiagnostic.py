# system level modules
import logging
import os
import re
import json
# system module functions
from subprocess import Popen, PIPE
from pprint import pformat
from time import sleep
from datetime import datetime
from shutil import copyfile
# job modules
from JobStatus import JobStatus, StatusMap
# output_viewer modules
from output_viewer.index import OutputPage
from output_viewer.index import OutputIndex
from output_viewer.index import OutputRow
from output_viewer.index import OutputGroup
from output_viewer.index import OutputFile
# lib.util modules
from lib.util import print_debug
from lib.util import print_message
from lib.util import create_symlink_dir
from lib.util import render
from lib.slurm import Slurm
from lib.events import Event_list

class AMWGDiagnostic(object):
    """
    A job class to perform the NCAR AMWG Diagnostic
    """
    def __init__(self, config, event_list):
        """
        Setup class attributes

        inputs:
            test_casename: the name of the test case e.g. b40.20th.track1.2deg.001
            test_filetype: the filetype of the history files, either monthly_history or time_series
            test_path_history: path to the directory holding your history files
            test_path_climo: path to directory holding climo files
            test_path_diag: the output path for the diagnostics to go

        """
        self.event_list = event_list
        self._status = JobStatus.INVALID
        self.start_time = None
        self.end_time = None
        self.output_path = None
        self.year_set = config.get('year_set', 0)
        self.inputs = {
            'web_dir': '',
            'host_url': '',
            'test_casename': '',
            'test_filetype': 'monthly_history',
            'test_path_history': '',
            'test_path_climo': '',
            'test_path_diag': '',
            'regrided_climo_path': '',
            'start_year': '',
            'end_year': '',
            'year_set': '',
            'run_directory': '',
            'template_path': '',
            'run_scripts_path': '',
            'output_path': '',
            'diag_home': '',
        }
        self._type = 'amwg'
        self.config = {}
        self.start_year = config['start_year']
        self.end_year = config['end_year']
        self.job_id = 0
        self.depends_on = ['ncclimo']
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 2 hours run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
        }
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
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        for key, value in config.iteritems():
            if key in self.inputs:
                self.config[key] = value
        for key, value in self.inputs.iteritems():
            if key not in self.config:
                self.config[key] = value

        if self.year_set == 0:
            self.status = JobStatus.INVALID
            return
        self.status = JobStatus.VALID

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        returns 1 if the job is done, 0 otherwise
        """
        base = str(os.sep).join(
            self.config.get('test_path_diag').split(os.sep)[:-1])
        year_set = 'year_set_{0}'.format(
            self.config.get('year_set'))
        web_dir = '{base}/{start:04d}-{end:04d}{casename}-obs'.format(
            base=base,
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            casename=self.config.get('test_casename'))
        if os.path.exists(web_dir):
            all_files = []
            for path, dirs, files in os.walk(web_dir):
                all_files += files
            return bool(len(all_files) > 1000)
        else:
            return False

    def execute(self):
        """
        Perform the actual work
        """
        # First check if the job has already been completed
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'AMWG job already computed, skipping'
            self.event_list.push(message=message)
            logging.info(message)
            return 0

        # render the csh script into the output directory
        self.output_path = self.config['output_path']
        template_out = os.path.join(
            self.output_path,
            'amwg.csh')
        render(
            variables=self.config,
            input_path=self.config.get('template_path'),
            output_path=template_out)
        # Copy the rendered run script into the scripts directory
        run_script_template_out = os.path.join(
            self.config.get('run_scripts_path'), 'amwg_{0}-{1}.csh'.format(
                self.config.get('start_year'),
                self.config.get('end_year')))
        copyfile(
            src=template_out,
            dst=run_script_template_out)

        # setup sbatch script
        expected_name = 'amwg_{start:04d}-{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'))
        run_script = os.path.join(
            self.config.get('run_scripts_path'),
            expected_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=run_script + '.out')
        cmd = '\ncsh {template}'.format(
            template=template_out)
        slurm_args_str = ['#SBATCH {value}'.format(value=v) for k, v in self.slurm_args.items()]
        slurm_prefix = '\n'.join(slurm_args_str)
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(cmd)

        prev_dir = os.getcwd()
        #os.chdir(output_path)

        slurm = Slurm()
        print 'submitting to queue {type}: {start:04d}-{end:04d}'.format(
            type=self.type,
            start=self.start_year,
            end=self.end_year)
        self.job_id = slurm.batch(run_script, '--oversubscribe')
        #os.chdir(prev_dir)

        status = slurm.showjob(self.job_id)
        self.status = StatusMap[status.get('JobState')]
        message = '{type} id: {id} changed state to {state}'.format(
            type=self.type,
            id=self.job_id,
            state=self.status)
        logging.info(message)
        self.event_list.push(message=message)

        return self.job_id

    @property
    def type(self):
        return self._type

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
