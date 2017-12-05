
import logging
import os
import re
import json

from subprocess import Popen, PIPE
from pprint import pformat
from time import sleep
from datetime import datetime
from shutil import copyfile

from JobStatus import JobStatus, StatusMap

from lib.slurm import Slurm
from lib.events import Event_list
from lib.util import (print_debug,
                      print_message,
                      create_symlink_dir,
                      render,
                      print_line,
                      get_climo_output_files)


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
            'ui': '',
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
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-02:00',  # 2 hours run time
            'num_machines': '-N 1',  # run on one machine
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

    def execute(self, dryrun=False):
        """
        Perform the actual work
        """
        # First check if the job has already been completed
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            msg = 'AMWG job already computed, skipping'
            print_line(
                ui=self.config.get('ui', False),
                line=msg,
                event_list=self.event_list)
            logging.info(msg)
            return 0

        # Create directory of regridded climos

        regrid_path = os.path.join(
            os.sep.join(self.config['test_path_diag'].split(os.sep)[:-2]),
            'climo_regrid')
        file_list = get_climo_output_files(
            input_path=regrid_path,
            start_year=self.start_year,
            end_year=self.end_year)
        if not file_list or len(file_list) == 0:
            msg = """
ERROR: AMWG: {start:04d}-{end:04d} could not find input climatologies at {path}\n
did you add ncclimo to this year_set?""".format(start=self.start_year,
                                                end=self.end_year,
                                                path=regrid_path)
            print_line(
                ui=self.config.get('ui', False),
                line=msg,
                event_list=self.event_list)
            self.status = JobStatus.FAILED
            return 0
        if not os.path.exists(self.config['test_path_climo']):
            msg = 'creating temp directory for amwg'
            print_line(
                ui=self.config.get('ui', False),
                line=msg,
                event_list=self.event_list,
                current_state=True)
            os.makedirs(self.config['test_path_climo'])
        create_symlink_dir(
            src_dir=regrid_path,
            src_list=file_list,
            dst=self.config['test_path_climo'])

        # Rename the files to the format amwg expects
        for item in os.listdir(self.config['test_path_climo']):
            search = re.search(r'\_\d\d\d\d\d\d\_', item)
            if not search:
                continue
            index = search.start()
            os.rename(
                os.path.join(self.config['test_path_climo'], item),
                os.path.join(self.config['test_path_climo'], item[:index] + '_climo.nc'))

        # render the csh script into the output directory
        self.output_path = self.config['output_path']
        template_out = os.path.join(
            self.output_path,
            'amwg.csh')
        render(
            variables=self.config,
            input_path=self.config.get('template_path'),
            output_path=template_out)

        expected_name = '{type}_{start:04d}-{end:04d}'.format(
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            type=self.type)
        # Copy the rendered run script into the scripts directory
        run_script_template_out = os.path.join(
            self.config.get('run_scripts_path'),
            expected_name)
        copyfile(
            src=template_out,
            dst=run_script_template_out)

        # setup sbatch script

        run_script = os.path.join(
            self.config.get('run_scripts_path'),
            expected_name)
        if os.path.exists(run_script):
            os.remove(run_script)

        self.slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=run_script + '.out')
        cmd = '\ncsh {template}'.format(
            template=template_out)
        slurm_args_str = ['#SBATCH {value}'.format(
            value=v) for k, v in self.slurm_args.items()]
        slurm_prefix = '\n'.join(slurm_args_str)
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(cmd)

        if dryrun:
            self.status = JobStatus.COMPLETED
            return 0

        slurm = Slurm()
        msg = 'submitting to queue {type}: {start:04d}-{end:04d}'.format(
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
