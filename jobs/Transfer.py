# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os, sys, json
from optparse import OptionParser
from datetime import datetime, timedelta
import time
from globusonline.transfer import api_client
from globusonline.transfer.api_client import Transfer as globus_transfer
from globusonline.transfer.api_client import TransferAPIClient
from globusonline.transfer.api_client import TransferAPIError
from globusonline.transfer.api_client import x509_proxy
from globusonline.transfer.api_client.goauth import get_access_token
from util import print_debug
from util import print_message
from pprint import pformat


class Transfer(object):
    def __init__(self, config=None):
        self.config = {}
        self.status = 'unvalidated'
        self.type = 'transfer'
        self.outputs = {
            "status": "unvalidated"
        }
        self.inputs = [
            'recursive',
            'globus_username',
            'globus_password',
            'source_endpoint',
            'destination_endpoint'
        ]
        self.validate(config)
        self.msg = None

    def save(self, conf_path):
        try:
            with open(conf_path, 'r') as infile:
                config = json.load(infile)
            with open(conf_path, 'w') as outfile:
                config[self.type]['inputs'] = self.config
                config[self.type]['outputs'] = self.outputs
                json.dump(config, outfile, indent=4)
        except Exception as e:
            print_message('Error saving configuration file')
            print_debug(e)
            raise

    def __str__(self):
        return pformat({
            'config': self.config,
            'status': self.status
        }, indent=4)

    def validate(self, config=None):
        """
            Validates transfer inputs
        """
        if self.status == 'valid':
            return 0
        inputs = config.get('inputs')
        for i in inputs:
            if i not in self.inputs:
                print_message("Unexpected arguement: {}, {}".format(i, inputs[i]))
            else:
                if i == 'recursive':
                    if inputs.get(i) == 'True':
                        self.config[i] = True
                    else:
                        self.config[i] = False
                elif i == 'globus_username':
                    self.config[i] = inputs.get(i)
                elif i == 'globus_password':
                    self.config[i] = inputs.get(i)
                elif i == 'source_endpoint':
                    self.config[i] = inputs.get(i)
                elif i == 'destination_endpoint':
                    self.config[i] = inputs.get(i)

        for i in self.inputs:
            if i not in self.config:
                print_message('Missing transfer argument {}'.format(i))
                self.status = 'invalid'
                return -1
        self.status = 'valid'
        return 0

    def activate_endpoint(self, api_client, endpoint):
        code, reason, result = api_client.endpoint_autoactivate(endpoint, if_expires_in=2880)
        if result["code"] == "AutoActivationFailed":
            reqs = result
            myproxy_hostname = None
            for r in result['DATA']:
                if r['type'] == 'myproxy' and r['name'] == 'hostname':
                    myproxy_hostname = r['value']
            reqs.set_requirement_value("myproxy", "hostname", myproxy_hostname)
            reqs.set_requirement_value("myproxy", "username", self.config['credential'][endpoint]['username'])
            reqs.set_requirement_value("myproxy", "passphrase", self.config['credential'][endpoint]['password'])
            reqs.set_requirement_value("myproxy", "lifetime_in_hours", "168")
            code, reason, result = api_client.endpoint_activate(endpoint, reqs)
            if code != 200:
                msg = "Could not activate the endpoint: %s. Error: %s - %s" % (endpoint, result["code"], result["message"])
                print msg

    def get_destination_path(self, srcpath, dstpath, recursive):
        '''
        If destination path is a directory (ends with '/') adds source file name to the destination path.
        Otherwise, Globus will treat the destination path as a file in spite of the fact that it is a directory.
        '''
        if srcpath:
            if not recursive:
                if dstpath.endswith('/'):
                    basename = os.path.basename(srcpath)
                    return dstpath + basename
        return dstpath
    
    def execute(self):
        # Map legacy endpoint names to UUIDs
        srcendpoint = self.config.get('source_endpoint').get('uuid')
        dstendpoint = self.config.get('destination_endpoint').get('uuid')
        if '#' in srcendpoint:
            srcendpoint = self.config.get('source_endpoint').get('comment')
        if '#' in dstendpoint:
            dstendpoint = self.config.get('destination_endpoint').get('comment')

        # Get access token (This method of getting an acces token is deprecated and should be replaced by OAuth2 calls).
        globus_username = self.config.get('globus_username')
        globus_password = self.config.get('globus_password')
        auth_result = get_access_token(globus_username, globus_password)

        # Create a transfer submission
        api_client = TransferAPIClient(globus_username, goauth=auth_result.token)
        self.activate_endpoint(api_client, srcendpoint)
        self.activate_endpoint(api_client, dstendpoint)

        code, message, data = api_client.transfer_submission_id()
        submission_id = data["value"]
        deadline = datetime.utcnow() + timedelta(days=10)
        transfer_task = globus_transfer(submission_id, srcendpoint, dstendpoint, deadline)

        # Add srcpath to the transfer task
        source_path = self.config.get('source_endpoint').get('path')
        destination_path = self.config.get('destination_endpoint').get('path')
        if source_path:
            transfer_task.add_item(
                source_path,
                self.get_destination_path(
                    source_path,
                    destination_path,
                    self.config.get('recursive')),
                recursive=self.config.get('recursive'))
        # Add srclist to the transfer task
        # if options.srclist:
        #     with open(options.srclist) as f:
        #         srcpath = f.readline().rstrip('\n')
        #         transfer_task.add_item(srcpath,
        #             get_destination_path(srcpath, options.dstpath, options.recursive),
        #             recursive=options.recursive)

        # Start the transfer
        task_id = None
        try:
            code, reason, data = api_client.transfer(transfer_task)
            task_id = data["task_id"]
            print 'task_id %s' % task_id
        except Exception as e:
            print_message("Could not submit the transfer. Error: %s" % str(e))
            print_debug(e)
            return

        # Check a status of the transfer every minute (60 secs)
        while True:
            code, reason, data = api_client.task(task_id)
            print data['status']
            self.status = data['status']
            if data['status'] == 'SUCCEEDED':
                print_message('progress %d/%d' % (data['files_transferred'], data['files']), 'ok')
                return ('success', '')
            elif data['status'] == 'FAILED':
                return ('error', data['nice_status_details'])
            elif data['status'] == 'ACTIVE':
                print_message('progress %d/%d' % (data['files_transferred'], data['files']), 'ok')
            time.sleep(5)
