import json
import logging
import os

from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE
from pprint import pformat

from util import print_debug
from util import print_message
from util import format_debug
from util import check_slurm_job_submission
from output_viewer.diagsviewer import DiagnosticsViewerClient

class UploadDiagnosticOutput(object):
    def __init__(self, config):
        """
        Setup class attributes
        """
        self.inputs = {
            'path_to_diagnostic': '',
            'username': '',
            'password': '',
            'server': '',
            'depends_on': ''
        }
        self.config = {}
        self.outputs = {}
        self.uuid = uuid4().hex
        self.status = 'unvalidated'
        self.depends_on = []
        self.type = 'upload_diagnostic_output'
        self.job_id = 0
        self.proc = None
        self.slurm_args = {
            'num_cores': '-n 1', # 16 cores
            'run_time': '-t 0-02:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
        }
        self.prevalidate(config)

    def get_type(self):
        """
        Returns job type
        """
        return self.type

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid
        }, indent=4)

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
            logging.error('Error saving configuration file')
            logging.error(format_debug(e))
            raise

    def prevalidate(self, config=None):
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        if self.status == 'valid':
            return 0
        for i in config:
            if i not in self.inputs:
                logging.info('Unexpected arguement to Upload_Diagnostic: %s, %s', i, config[i])
            else:
                if i == 'depends_on':
                    self.depends_on = config.get(i)
                    self.config[i] = config[i]
                else:
                    self.config[i] = config[i]
        for i in self.inputs:
            if i not in self.config:
                logging.error('Missing UploadDiagnosticOutput argument %s', i)
                self.status = 'invalid'
                return -1
        self.status = 'valid'
        return 0

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        """
        if not self.outputs.get('dataset_id'):
            self.status = 'error'
            return
        if not self.outputs.get('id'):
            self.status = 'error'
            return
        self.status = 'COMPLETED'

    def execute(self, batch=True):
        """
        Upload the files in the given directory to the DiagnosticViewer on the given server
        """
        # not running in batch mode
        if not batch:
            client = DiagnosticsViewerClient(
                server=self.config.get('server'),
                cert=False)
            try:
                client_id, key = client.login(
                    self.config.get('username'),
                    self.config.get('password'))
            except Exception as e:
                logging.error('Upload_Diagnostic unable error connecting to server')
                logging.error(format_debug(e))
                return -1
            self.outputs['id'] = client_id
            try:
                logging.info(
                    'uploading diagnostic package from %s',
                    self.config.get('path_to_diagnostic')
                )
                dataset_id = client.upload_package(self.config.get('path_to_diagnostic'))
            except Exception as e:
                logging.error('Error uploading diagnostic set to server')
                logging.error(format_debug(e))
                return -1
            self.outputs['dataset_id'] = dataset_id
            self.status = 'COMPLETED'
        # running in batch mode
        else:
            expected_name = 'upload_diag_job_' + str(self.uuid)
            run_script = os.path.join(os.getcwd(), 'run_scripts/' + expected_name + '.py')

            # write out a python script with the upload code
            with open(run_script, 'w') as batchfile:
                batchfile.write("from output_viewer.diagsviewer import DiagnosticsViewerClient\n\
import sys\n\
client = DiagnosticsViewerClient(\n\
    server='{s}',\n\
    cert=False)\n\
try: \n\
    client_id, key = client.login('{u}','{p}')\n\
except Exception as e:\n\
    print 'Upload_Diagnostic unable error connecting to server'\n\
    sys.exit(1)\n\
try:\n\
    dataset_id = client.upload_package('{d}')\n\
except Exception as e:\n\
    print 'Error uploading diagnostic set to server'\n\
    sys.exit(1)".format(
        s=self.config.get('server'),
        u=self.config.get('username'),
        p=self.config.get('password'),
        d=self.config.get('path_to_diagnostic')))

            # write out a script to call the upload code
            batch_script_name = os.path.join(os.getcwd(), 'run_scripts', expected_name)
            with open(batch_script_name, 'w') as batchfile:
                batchfile.write('#!/bin/bash\npython {0}'.format(run_script))

            # tell slurm to submit a job for that script
            output_path = os.path.join(os.getcwd(), 'run_scripts', expected_name + '.out')
            slurm_cmd = ['sbatch', '-o', output_path, batch_script_name]
            started = False
            retry_count = 0
            while not started and retry_count < 5:
                logging.info('Starting upload_diag')
                self.proc = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
                output, err = self.proc.communicate()
                # print_message('upload job output:\n {0}\nerr: {1}'.format(output, err))
                started, job_id = check_slurm_job_submission(expected_name)
                if started:
                    self.status = 'RUNNING'
                    logging.info('Started upload_diag job with job_id %s', job_id)
                    self.job_id = job_id
                elif retry_count >= 5:
                    logging.warning("Failed starting upload_diag job\n%s", output)
                    print_message("Failed starting upload_diag job")
                    print_message(output)
                    return 0
                else:
                    logging.warning('Error starting job trying again, attempt %s', str(retry_count))
                    print_message('Error starting job, trying again')
                    retry_count += 1
                    sleep(5)
                    continue
            return self.job_id

    def set_status(self, status):
        """
        Set the jobs statys
        """
        self.status = status
