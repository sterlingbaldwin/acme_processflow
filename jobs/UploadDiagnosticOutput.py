import json
import logging
import os

from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE
from pprint import pformat

from lib.util import print_debug
from lib.util import print_message
from lib.util import format_debug
from lib.util import check_slurm_job_submission
from output_viewer.diagsviewer import DiagnosticsViewerClient
from JobStatus import JobStatus


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
            'depends_on': '',
            'year_set': '',
            'start_year': '',
            'end_year': ''
        }
        self.config = {}
        self.outputs = {}
        self.uuid = uuid4().hex
        self.status = JobStatus.INVALID
        self.depends_on = []
        self.type = 'upload_diagnostic_output'
        self.job_id = 0
        self.proc = None
        self.resubmit_max = 5
        self.batch_script_name = None
        self.expected_name = None
        self.run_script = None
        self.slurm_args = {
            'num_cores': '-n 1', # 1 core
            'run_time': '-t 0-01:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
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
        if self.status == JobStatus.VALID:
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
                self.status = JobStatus.INVALID
                return -1
        self.status = JobStatus.VALID
        return 0

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        """
        if not self.outputs.get('dataset_id'):
            self.status = JobStatus.FAILED
            return
        if not self.outputs.get('id'):
            self.status = JobStatus.FAILED
            return
        self.status = JobStatus.COMPLETED

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
                message = "## {type} job {id} unable to connect to server".format(
                    id=self.job_id,
                    type=self.type)
                logging.error(message)
                logging.error(format_debug(e))
                return -1
            self.outputs['id'] = client_id
            try:
                message = '## uploading diagnostic package from {}'.format(
                    self.config.get('path_to_diagnostic'))
                logging.info(message)
                dataset_id = client.upload_package(self.config.get('path_to_diagnostic'))
            except Exception as e:
                logging.error(format_debug(e))
                message = "## {type}: {id} has failed".format(id=self.job_id, type=self.type)
                logging.info(message)
                return -1
            self.outputs['dataset_id'] = dataset_id
            self.status = JobStatus.COMPLETED
        # running in batch mode
        else:
            self.expected_name = 'upload_diag_set_{set}_{start}_{end}_{uuid}'.format(
                set=self.config.get('year_set'),
                start=self.config.get('start_year'),
                end=self.config.get('end_year'),
                uuid=self.uuid[:5])
            self.run_script = os.path.join(os.getcwd(), 'run_scripts/' + self.expected_name + '.py')

            # write out a python script with the upload code
            with open(self.run_script, 'w') as batchfile:
                batchfile.write("from output_viewer.diagsviewer import DiagnosticsViewerClient\n\
import sys\n\
import traceback\n\
def print_debug(e):\n\
    print '1', e.__doc__\n\
    print '2', sys.exc_info()\n\
    print '3', sys.exc_info()[0]\n\
    print '4', sys.exc_info()[1]\n\
    print '5', traceback.tb_lineno(sys.exc_info()[2])\n\
    ex_type, ex, tb = sys.exc_info()\n\
    print '6', traceback.print_tb(tb)\n\
client = DiagnosticsViewerClient(\n\
    server='{s}',\n\
    cert=False)\n\
try: \n\
    client_id, key = client.login('{u}','{p}')\n\
except Exception as e:\n\
    print 'Upload_Diagnostic error connecting to server'\n\
    print_debug(e)\n\
    sys.exit(1)\n\
try:\n\
    dataset_id = client.upload_package('{d}')\n\
except Exception as e:\n\
    print 'Error uploading diagnostic set to server'\n\
    print_debug(e)\n\
    sys.exit(1)".format(
        s=self.config.get('server'),
        u=self.config.get('username'),
        p=self.config.get('password'),
        d=self.config.get('path_to_diagnostic')))

            # write out a script to call the upload code
            self.batch_script_name = os.path.join(os.getcwd(), 'run_scripts', self.expected_name)
            with open(self.batch_script_name, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                batchfile.write('\n#SBATCH '.join([val for key, val in self.slurm_args.items()]))
                batchfile.write('\npython {0}'.format(self.run_script))
            submitted = False
            while not submitted:
                try:
                    self.submit()
                except Exception as e:
                    sleep(10)
                    self.resubmit()
                else:
                    submitted = True
            return self.job_id

    def set_status(self, status):
        """
        Set the jobs statys
        """
        self.status = status

    def submit(self):
        """
        Submit the actual slurm job to the queue
        """
        # tell slurm to submit a job for that script
        output_path = os.path.join(os.getcwd(), 'run_scripts', self.expected_name + '.out')
        slurm_cmd = ['sbatch', '-o', output_path, self.batch_script_name]
        started = False
        while not started:
            while True:
                try:
                    self.proc = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
                    break
                except:
                    sleep(1)
            _, _ = self.proc.communicate()
            # print_message('upload job output:\n {0}\nerr: {1}'.format(output, err))
            started, job_id = check_slurm_job_submission(self.expected_name)
            if started:
                self.status = JobStatus.RUNNING
                message = "## {type} id: {id} status change to {status}".format(
                    type=self.type,
                    id=self.job_id,
                    status=self.status)
                logging.info(message)
                self.job_id = job_id
            else:
                logging.warning('Error starting job trying again')
                sleep(10)
                continue
        return self.job_id

    def resubmit(self):
        if self.resubmit_max > 0:
            self.resubmit_max -= 1
            self.submit()
            return 0
        else:
            return -1
