# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import shutil
import json
import logging
import time

from pprint import pformat
from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE

from util import print_debug
from util import print_message
from util import check_slurm_job_submission


class Diagnostic(object):
    """
    Performs the ACME diagnostic job
    """
    def __init__(self, config=None):
        self.raw_config = config
        self.config = {}
        self.proc = None
        self.type = 'diagnostic'
        self.status = 'unvalidated'
        self.yearset = config.get('yearset', 0)
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
            'depends_on': ''
        }
        self.outputs = {
            'output_path': '',
            'console_output': '',
            'status': self.status
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
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

    def execute(self, batch=False):
        """
        Executes the diagnostic job.
        If archive is set to True, will create a tarbal of the output directory
        """
        dataset_name = time.strftime("%d-%m-%Y") + '-year-set-' + str(self.yearset) + '-' + self.uuid
        cmd = ['metadiags', '--dsname', dataset_name]
        for i in self.config:
            if i == '--archive':
                continue
            cmd.append(i)
            cmd.append(self.config[i])
        cmd = ' '.join(cmd)
        if not batch:
            # Not running in batch mode
            try:
                console_output = ''
                self.proc = Popen(
                    cmd,
                    stdout=PIPE,
                    stdin=PIPE,
                    stderr=PIPE,
                    shell=True)
                self.status = 'running'
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
                self.status = 'complete'
            except Exception as e:
                self.status = 'error'
                print_debug(e)
                print_message('Error running diagnostic')
        else:
            # Submitting to SLURM queue
            expected_name = 'diagnostic_job_' + str(self.uuid)
            run_script = os.path.join(os.getcwd(), 'run_scripts', expected_name)
            self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
            self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')
            with open(run_script, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'
                batchfile.write(slurm_prefix)
                batchfile.write(cmd)

            slurm_cmd = ['sbatch', run_script]
            started = False
            retry_count = 0
            while not started and retry_count < 5:
                self.proc = Popen(slurm_cmd, stdout=PIPE)
                output, err = self.proc.communicate()
                started, job_id = check_slurm_job_submission(expected_name)
                if started:
                    self.status = 'RUNNING'
                    self.job_id = job_id
                    message = "## year_set {set} status change to {status}".format(set=self.yearset, status=self.status)
                    logging.info(message)
                    # print_message('+++++ STARTING CLIMO JOB {0} +++++'.format(self.job_id))
                elif retry_count <= 0:
                    print_message("Error starting diagnostic job")
                    print_message(output)
                    self.job_id = 0
                    break
                else:
                    logging.warning('Failed to start job trying again, attempt %s', str(retry_count))
                    message = "## year_set {set} status change to {status}".format(set=self.yearset, status=self.status)
                    logging.warning(message)
                    print_message('Failed to start job, trying again')
                    retry_count += 1
                    continue
            return self.job_id

        if self.config['--archive'] == 'True':
            archive_path = '{}/archive'.format(self.config['--outputdir'])
            if not os.path.exists(archive_path + '.tar.gz'):
                try:
                    print_message(
                        'creating output archive {}'.format(archive_path + '.tar.gz'),
                        'ok')
                    shutil.make_archive(
                        archive_path,
                        'gztar',
                        self.config.get('output_dir'))
                except Exception as e:
                    print_debug(e)
                    print_message('Error making archive {}'.format(archive_path + '.tar.gz'))

            else:
                print_message('archive {} already exists'.format(archive_path + '.tar.gz'))

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
        if self.status == 'valid':
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
        self.status = 'valid'

        # Check if the job has already been completed
        outputdir = os.path.join(self.config.get('--outputdir'), 'amwg')
        if os.path.exists(outputdir):
            output_contents = os.listdir(outputdir)
            if 'index.json' in output_contents and len(output_contents) > 400:
                self.status = 'COMPLETED'
        return 0
