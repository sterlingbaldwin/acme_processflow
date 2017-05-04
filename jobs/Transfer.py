# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import sys
import json
import time
import logging

from pprint import pformat
from datetime import datetime, timedelta
from uuid import uuid4

from globus_cli.commands.login import do_link_login_flow, check_logged_in
from globus_cli.services.transfer import get_client, autoactivate
from globus_sdk import TransferData

from lib.util import print_debug
from lib.util import print_message
from lib.util import filename_to_year_set
from lib.util import format_debug
from lib.util import push_event
from lib.util import raw_file_cmp
from jobs.JobStatus import JobStatus

class Transfer(object):
    """
    Uses Globus to transfer files between DTNs
    """
    def __init__(self, config=None, event_list=None):
        self.event_list = event_list
        self.config = {}
        self.status = JobStatus.INVALID
        self.type = 'transfer'
        self.outputs = {
            "status": self.status
        }
        self.uuid = uuid4().hex
        self.inputs = {
            'size': '',
            'file_list': '',
            'recursive': '',
            'globus_username': '',
            'globus_password': '',
            'source_endpoint': '',
            'destination_endpoint': '',
            'source_username': '',
            'source_password': '',
            'destination_username': '',
            'destination_password': '',
            'source_path': '',
            'destination_path': '',
            'pattern': '',
        }
        self.max_size = 100
        self.prevalidate(config)
        self.msg = None
        self.job_id = 0

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

    def get_type(self):
        """
        Returns job type
        """
        return self.type

    def get_file_list(self):
        return self.inputs.get('file_list')

    def __str__(self):
        return pformat({
            'config': self.config,
            'status': self.status
        }, indent=4)

    def prevalidate(self, config=None):
        """
        Validates transfer inputs
        """
        if self.status == JobStatus.VALID:
            return 0
        if not check_logged_in():
            self.status = JobStatus.INVALID
            return 1
        for i in config:
            if i in self.inputs:
                if i == 'recursive':
                    if config.get(i) == 'True':
                        self.config[i] = True
                    else:
                        self.config[i] = False
                else:
                    self.config[i] = config.get(i)

        for i in self.inputs:
            if i not in self.config or self.config[i] == None:
                if i == 'file_list':
                    self.config[i] = ''
                elif i == 'recursive':
                    self.config[i] = False
                else:
                    print_message('Missing transfer argument {}'.format(i))
                    self.status = JobStatus.INVALID
                    return -1
        self.status = JobStatus.VALID
        size = config.get('size')
        self.max_size = int(size) if size else 100
        # only add the first n transfers up to the max
        file_list = sorted(self.config.get('file_list'), raw_file_cmp)
        transfer_list = []
        transfer_size = 0
        for index, element in enumerate(file_list):
            new_size = transfer_size + (element['size'] / 1000000000.0)
            if self.max_size >= new_size:
                transfer_list.append(element)
                transfer_size += (element['size'] / 1000000000.0)
            else:
                break
        self.config['file_list'] = transfer_list
        for file_info in file_list:
            destination = os.path.join(self.config.get('destination_path'), file_info['type'])
            if not os.path.exists(destination):
                os.makedirs(destination)
        return 0

    def postvalidate(self):
        print 'transfer postvalidate'

    def activate_endpoint(self, api_client, endpoint, username, password):
        result = api_client.endpoint_autoactivate(endpoint, if_expires_in=2880)
        if result['code'] == "AutoActivationFailed":
            reqs = json.loads(result.text)
            myproxy_hostname = None
            for r in reqs['DATA']:
                if r['type'] == 'myproxy' and r['name'] == 'hostname':
                    myproxy_hostname = r['value']
                if r['name'] == 'hostname':
                    r['value'] = myproxy_hostname
                elif r['name'] == 'username':
                    r['value'] = username
                elif r['name'] == 'passphrase':
                    r['value'] = passphrase
                elif r['name'] == 'lifetime_in_hours':
                    r['value'] = '168'
            try:
                result = api_client.endpoint_activate(endpoint, reqs)
            except:
                raise
            if result['code'] == "AutoActivationFailed":
                logging.error("Could not activate the endpoint: %s. Error: %s - %s", endpoint, result["code"], result["message"])

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

    def display_status(self, event_list, percent_complete, task_id):
        """
        Updates the event_list with a nicely formated percent completion
        """
        start_file = self.config.get('file_list')[0]
        end_file = self.config.get('file_list')[-1]
        if not 'mpas-' in start_file:
            index = start_file['filename'].rfind('-')
            start_file = start_file['filename'][index - 4: index + 3]
        if not 'mpas-' in end_file:
            index = end_file['filename'].rfind('-')
            end_file = end_file['filename'][index - 4: index + 3]

        start_end_str = '{start} to {end}'.format(
            start=start_file,
            end=end_file)
        message = 'Transfer {0} in progress ['.format(start_end_str)
        for i in range(1, 100, 5):
            if i < percent_complete:
                message += '*'
            else:
                message += '_'
        message += '] {0:.2f}%'.format(percent_complete)
        replaced = False
        for index, event in enumerate(event_list):
            if start_end_str in event:
                event_list[index] = time.strftime("%I:%M") + ' ' + message
                replaced = True
                break
        if not replaced:
            event_list = push_event(event_list, message)

    def error_cleanup(self):
        print_message('Removing partially transfered files')
        destination_contents = os.listdir(self.config.get('destination_path'))
        for transfer in self.config['file_list']:
            t_file = transfer['filename'].split(os.sep)[-1]
            if t_file in destination_contents:
                os.remove(os.path.join(self.config.get('destination_path'), t_file))

    def execute(self, event, event_list):
        # reject if job isnt valid
        if self.status != JobStatus.VALID:
            logging.error('Transfer job in invalid state')
            logging.error(str(self))
            return
        if not check_logged_in():
            self.status = JobStatus.INVALID
            logging.error('Transfer failed, not logged into globus')
            return
        # Get source and destination UUIDs
        srcendpoint = self.config.get('source_endpoint')
        dstendpoint = self.config.get('destination_endpoint')
        message = 'Starting setup for transfer job from {src} to {dst}'.format(
            src=srcendpoint,
            dst=dstendpoint)
        logging.info(message)

        # Create a transfer submission
        client = get_client()
        source_user = self.config.get('source_username')
        source_pass = self.config.get('source_password')
        try:
            self.activate_endpoint(client, srcendpoint, source_user, source_pass)
        except Exception as e:
            logging.error('Error activating source endpoing, attempt %s', int(i) + 1)
            logging.error(format_debug(e))

        dst_user = self.config.get('destination_username')
        dst_pass = self.config.get('destination_password')
        try:
            self.activate_endpoint(client, dstendpoint, dst_user, dst_pass)
        except Exception as e:
            logging.error('Error activating destination endpoing')
            logging.error(format_debug(e))

        try:
            transfer_task = TransferData(
                client,
                srcendpoint,
                dstendpoint,
                sync_level='checksum')
        except Exception as e:
            logging.error('Error creating transfer task')
            logging.error(format_debug(e))
            self.status = JobStatus.FAILED
            return

        if not self.config['file_list']:
            logging.error('Unable to transfer files without a source list')
            self.status = JobStatus.FAILED
            return

        for path in self.config['file_list']:
            dst_path = os.path.join(
                self.config.get('destination_path'),
                path['type'],
                path['filename'].split('/')[-1])
            src_path = os.path.join(
                self.config.get('source_path'),
                path['filename'])
            # print "moving file from {src} to {dst}".format(src=src_path, dst=dst_path)
            transfer_task.add_item(
                source_path=src_path,
                destination_path=dst_path,
                recursive=False)

        # Start the transfer
        task_id = None
        try:
            result = client.submit_transfer(transfer_task)
            task_id = result["task_id"]
            logging.info('starting transfer with task id %s', task_id)
        except Exception as e:
            logging.error("result: %s", str(result))
            logging.error("Could not submit the transfer")
            logging.error(format_debug(e))
            self.status = JobStatus.FAILED
            return

        # Check a status of the transfer every minute (60 secs)
        number_transfered = -1
        while True:
            try:
                while True:
                    try:
                        status = client.get_task(task_id)
                    except:
                        time.sleep(1)
                    else:
                        break
                if status['status'] == 'SUCCEEDED':
                    logging.info('progress %d/%d', status['files_transferred'], status['files'])
                    percent_complete = 100.0
                    self.display_status(event_list, percent_complete, task_id)
                    message = 'Transfer job completed'
                    self.status = JobStatus.COMPLETED
                    return
                elif status['status'] == 'FAILED':
                    logging.error('Error transfering files %s', status.get('nice_status_details'))
                    self.status = JobStatus.FAILED
                    return
                elif status['status'] == 'ACTIVE':
                    if number_transfered < status['files_transferred']:
                        number_transfered = status['files_transferred']
                        logging.info('progress %d/%d', status['files_transferred'], status['files'])
                        percent_complete = (float(status['files_transferred']) / float(status['files'])) * 100
                        self.display_status(event_list, percent_complete, task_id)
                    self.status = JobStatus.RUNNING
                if event and event.is_set():
                    client.cancel_task(task_id)
                    self.error_cleanup()
                    return
            except Exception as e:
                logging.error(format_debug(e))
                client.task_cancel(task_id)
                self.error_cleanup()
                return
            time.sleep(5)
