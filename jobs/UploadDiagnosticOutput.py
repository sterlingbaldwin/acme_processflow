import json
import logging

from uuid import uuid4
from diagsviewer import DiagnosticsViewerClient
from pprint import pformat

from util import print_debug
from util import print_message
from util import format_debug


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
        self.status = 'complete'

    def execute(self, batch=False):
        """
        Upload the files in the given directory to the DiagnosticViewer on the given server
        """
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
            logging.info('uploading diagnostic package from %s', self.config.get('path_to_diagnostic'))
            dataset_id = client.upload_package(self.config.get('path_to_diagnostic'))
        except Exception as e:
            logging.error('Error uploading diagnostic set to server')
            logging.error(format_debug(e))
            return -1
        self.outputs['dataset_id'] = dataset_id
        self.status = 'complete'

    def set_status(self, status):
        self.status = status
