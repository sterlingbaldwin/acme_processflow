# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import re
import shutil
import json
import logging
import time

from pprint import pformat
from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE

from lib.util import print_debug
from lib.util import print_message
from lib.util import check_slurm_job_submission
from lib.util import create_symlink_dir
from lib.util import push_event
from JobStatus import JobStatus

class Uvcmetrics(object):
    """
    Performs the ACME diagnostic job
    """
    def __init__(self, config=None, event_list=None):
        self.event_list = event_list
        self.raw_config = config
        self.config = {}
        self.proc = None
        self.type = 'uvcmetrics'
        self.status = JobStatus.INVALID
        self.year_set = config.get('year_set', 0)
        self.uuid = uuid4().hex
        self.depends_on = []
        self.job_id = 0
        self.inputs = {
            '--model': '',
            '--obs': '',
            '--outputdir': '',
            '--package': '',
            '--set': '',
            '--archive': '',
            'depends_on': '',
            'regrid_path': '',
            'diag_temp_dir': '',
            'start_year': '',
            'end_year':'',
            'dataset_name': ''
        }
        self.outputs = {
            'output_path': '',
            'console_output': '',
            'status': self.status
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-05:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
            'exclusive': '--exclusive'
        }
        self.prevalidate(config)

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id
        }, indent=4)

    def get_type(self):
        """
        Returns job type
        """
        return self.type

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

    def set_status(self, status):
        self.status = status

    def setup_input_directory(self):
        regrid_path = self.config.get('regrid_path')
        diag_temp_path = self.config.get('diag_temp_dir')
        set_start_year = self.config.get('start_year')
        set_end_year = self.config.get('end_year')
        if not regrid_path or not os.path.exists(regrid_path):
            self.status = JobStatus.INVALID
            return -1
        if not diag_temp_path or not os.path.exists(diag_temp_path):
            self.status = JobStatus.INVALID
            return -1

        diag_file_list_tmp = [s for s in os.listdir(regrid_path) if not os.path.islink(s)]
        diag_file_list = []
        for d_file in diag_file_list_tmp:
            start_search = re.search(r'\_\d\d\d\d', d_file)
            if not start_search:
                continue
            start_index = start_search.start() + 1
            start_year = int(d_file[start_index: start_index + 4])

            end_search = re.search(r'\_\d\d\d\d', d_file[start_index:])
            if not end_search:
                continue
            end_index = end_search.start() + start_index + 1
            end_year = int(d_file[end_index: end_index + 4])

            if start_year == set_start_year and end_year == set_end_year:
                diag_file_list.append(d_file)


        create_symlink_dir(
            src_dir=regrid_path,
            src_list=diag_file_list,
            dst=diag_temp_path)
        return 0

    def execute(self, batch=False):
        """
        Executes the diagnostic job.
        If archive is set to True, will create a tarbal of the output directory
        """
        if self.setup_input_directory() == -1:
            return
        self.status = JobStatus.PENDING

        cmd = ['metadiags', '--dsname', self.config.get('dataset_name')]

        cmd_config = {
            '--model': self.config.get('--model'),
            '--obs': self.config.get('--obs'),
            '--outputdir': self.config.get('--outputdir'),
            '--package': self.config.get('--package'),
            '--set': self.config.get('--set'),
        }
        for i in cmd_config:
            cmd.append(i)
            cmd.append(self.config[i])
        if not batch:
            # Not running in batch mode
            try:
                console_output = ''
                while True:
                    try:
                        self.proc = Popen(
                            cmd,
                            stdout=PIPE,
                            stdin=PIPE,
                            stderr=PIPE,
                            shell=True)
                        break
                    except:
                        sleep(1)
                self.status = JobStatus.RUNNING
                done = 2
                while done != 0:
                    done = self.proc.poll()
                    lines = self.proc.stdout.readlines()
                    for line in lines:
                        console_output += line
                    lines = self.proc.stderr.readlines()
                    for line in lines:
                        console_output += line
                    if done < 0:
                        break
                    sleep(1)
                self.outputs['console_output'] = console_output
                print console_output
                with open('config.json', 'r+') as infile:
                    config = json.load(infile)
                    config.get('diagnostic')['outputs'] = self.outputs
                with open('config.json', 'w') as outfile:
                    json.dump(config, outfile, indent=4, sort_keys=True)
                self.status = JobStatus.COMPLETED
            except Exception as e:
                self.status = 'error'
                print_debug(e)
                print_message('Error running diagnostic')
        else:
            # Submitting to SLURM queue

            expected_name = 'uvcmetrics_set_{set}_{start}_{end}_{uuid}'.format(
                set=self.year_set,
                start=self.config.get('start_year'),
                end=self.config.get('end_year'),
                uuid=self.uuid[:5])
            run_script = os.path.join(os.getcwd(), 'run_scripts', expected_name)
            self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
            self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

            with open(run_script, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'
                batchfile.write(slurm_prefix)
                batchfile.write(' '.join(cmd))


            self.event_list = push_event(self.event_list, 'Script generation complete, submitting batch job')
            slurm_cmd = ['sbatch', run_script]
            started = False
            retry_count = 0
            while not started and retry_count < 5:
                while True:
                    try:
                        self.proc = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
                        break
                    except:
                        sleep(1)
                output, err = self.proc.communicate()
                started, job_id = check_slurm_job_submission(expected_name)
                if started:
                    self.status = JobStatus.RUNNING
                    self.job_id = job_id
                    message = "## year_set {set} status change to {status}".format(
                        set=self.year_set,
                        status=self.status)
                    logging.info(message)
                else:
                    logging.warning('Failed to start diag job trying again, attempt %s', str(retry_count))
                    logging.warning('%s \n%s', output, err)
                    retry_count += 1

            if retry_count >= 5:
                self.status = JobStatus.FAILED
                message = "## year_set {set} status change to {status}".format(
                    set=self.year_set,
                    status=self.status)
                logging.error(message)
                self.job_id = 0
            return self.job_id

        if self.config['--archive'] == 'True':
            archive_path = '{}/archive'.format(self.config['--outputdir'])
            if not os.path.exists(archive_path + '.tar.gz'):
                try:
                    shutil.make_archive(
                        archive_path,
                        'gztar',
                        self.config.get('output_dir'))
                except Exception as e:
                    print_debug(e)
                    print_message('Error making archive {}'.format(archive_path + '.tar.gz'))

    def postvalidate(self):
        """
        Post run validation
        """
        valid = True
        error = ''
        output_files = os.listdir(self.config.get('output').get('--outputdir'))
        if len(output_files) < 100:
            valid = False
            error = 'Too few output files\n'
        for f in output_files:
            try:
                size = os.path.getsize(f)
            except os.error as e:
                print_debug(e)
                valid = False
                error += 'Unable to open file {}\n'.format(f.name)
            else:
                if size <= 0:
                    valid = False
                    error += 'File {} size to small\n'.format(f.name)
        return valid, error

    def prevalidate(self, config=None):
        """
        Validates the config options
        Valid options are: model_path, obs_path, output_path, package, sets
        """
        if self.status == JobStatus.VALID:
            return 0
        inputs = config
        for i in inputs:
            if i in self.inputs:
                if i == '--model':
                    self.config['--model'] = 'path=' + inputs[i] + ',climos=yes'
                elif i == '--obs':
                    self.config['--obs'] = 'path=' + inputs[i] + ',climos=yes'
                elif i == '--outputdir':
                    self.config['--outputdir'] = inputs[i]
                elif i == '--package':
                    self.config['--package'] = inputs[i]
                elif i == '--set':
                    self.config['--set'] = inputs[i]
                elif i == '--archive':
                    self.config['--archive'] = inputs[i]
                elif i == 'depends_on':
                    self.depends_on = inputs[i]
                else:
                    self.config[i] = inputs[i]

        for i in self.inputs:
            if i not in self.config:
                default = ''
                if i == '--model':
                    print_message('model_path is a required argument, exiting')
                    return 'Invalid'
                elif i == '--obs':
                    print_message('obs_path is a required argument, exiting')
                    return 'Invalid'
                elif i == '--outputdir':
                    default = os.getcwd()
                    self.config['--outputdir'] = default
                elif i == '--package':
                    default = 'amwg'
                    self.config['--package'] = default
                elif i == '--set':
                    default = '5'
                    self.config['--set'] = default
                elif i == '--archive':
                    default = 'False'
                    self.config['archive'] = default
        self.outputs['output_path'] = self.config['--outputdir']
        self.outputs['console_output'] = ''
        self.status = JobStatus.VALID

        # Check if the job has already been completed
        outputdir = os.path.join(self.config.get('--outputdir'), 'amwg')
        if os.path.exists(outputdir):
            output_contents = os.listdir(outputdir)
            if 'index.json' in output_contents:
                if len(output_contents) > 1000:
                    self.status = JobStatus.COMPLETED

        return 0
