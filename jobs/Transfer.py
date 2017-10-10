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
from lib.util import format_debug
from lib.util import setup_globus
from lib.util import strfdelta
from lib.events import Event_list
from lib.models import DataFile
from jobs.JobStatus import JobStatus

class Transfer(object):
    """
    Uses Globus to transfer files between DTNs
    """
    def __init__(self, config=None, event_list=None):
        self.event_list = event_list
        self.config = {}
        self.status = JobStatus.INVALID
        self._type = 'transfer'
        self.uuid = uuid4().hex
        self.start_time = None
        self.end_time = None
        self.inputs = {
            'file_list': '',
            'recursive': '',
            'source_endpoint': '',
            'destination_endpoint': '',
            'source_path': '',
            'destination_path': '',
            'src_email': '',
            'display_event': '',
            'ui': ''
        }
        self.prevalidate(config)
        self.msg = None
        self.job_id = 0

    @property
    def file_list(self):
        return self.config.get('file_list')
    
    @file_list.setter
    def file_list(self, _file_list):
        self.config['file_list'] = _file_list

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

    @property
    def type(self):
        return self._type

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
                if i == 'recursive':
                    self.config[i] = False
                else:
                    print_message('Missing transfer argument {}'.format(i))
                    self.status = JobStatus.INVALID
                    return -1
        self.status = JobStatus.VALID
        return 0

    def postvalidate(self):
        """
        TODO: validate that all files were moved correctly
        """
        pass


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

    def display_status(self, percent_complete, task_id, num_completed, num_total):
        """
        Updates the event_list with a nicely formated percent completion
        """

        spacer = ' ' if num_completed < 10 else ''
        message = 'Transfer in progress {spacer}({completed}/{total}) ['.format(
            completed=num_completed,
            spacer=spacer,
            total=num_total)

        # now get the percent completion and elapsed time
        for i in range(1, 100, 5):
            if i < percent_complete:
                message += '*'
            else:
                message += '_'
        message += '] {percent:.2f}%'.format(percent=percent_complete)

        # check if the event has already been pushed into the event_list
        replaced = False
        from lib.util import print_debug
        try:
            for index, event in enumerate(self.event_list.list):
                if task_id == event.data:
                    msg = '{time} {msg}'.format(
                        time=time.strftime("%I:%M"),
                        msg=message)
                    self.event_list.replace(
                        index=index,
                        message=msg)
                    replaced = True
                    break
            if not replaced:
                msg = '{time} {msg}'.format(
                    time=time.strftime("%I:%M"),
                    msg=message)
                self.event_list.push(
                    message=msg,
                    data=task_id)
        except Exception as e:
            print_debug(e)

    def execute(self, event):
        """
        Start the transfer
        
        Parameters:
            event (thread.event): event to trigger job cancel
        """
        # reject if job isnt valid
        self.prevalidate()
        if self.status != JobStatus.VALID:
            logging.error('Transfer job in invalid state')
            logging.error(str(self))
            return
        if not check_logged_in():
            self.status = JobStatus.INVALID
            logging.error('Transfer failed, not logged into globus')
            return
        self.start_time = datetime.now()
        # Get source and destination UUIDs
        srcendpoint = self.config.get('source_endpoint')
        dstendpoint = self.config.get('destination_endpoint')
        message = 'Starting setup for transfer job from {src} to {dst}'.format(
            src=srcendpoint,
            dst=dstendpoint)
        logging.info(message)

        # Log into globus and activate endpoints
        endpoints = [srcendpoint, dstendpoint]
        setup_globus(
            endpoints=endpoints,
            event_list=self.event_list,
            no_ui=not self.config.get('ui', True),
            src=self.config.get('src'),
            dst=self.config.get('src'),
            display_event=self.config.get('display_event'))
        client = get_client()
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

        for datafile in self.config['file_list']:
            transfer_task.add_item(
                source_path=datafile.remote_path,
                destination_path=datafile.local_path,
                recursive=False)

        # Start the transfer
        task_id = None
        result = None
        try:
            result = client.submit_transfer(transfer_task)
            task_id = result["task_id"]
            logging.info('starting transfer with task id %s', task_id)
        except Exception as e:
            if result:
                logging.error("result: %s", str(result))
            logging.error("Could not submit the transfer")
            logging.error(format_debug(e))
            self.status = JobStatus.FAILED
            return

        # Check a status of the transfer every 10 secs
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
                    self.display_status(
                        percent_complete=percent_complete,
                        task_id=task_id,
                        num_completed=int(status['files_transferred']) + int(status['files_skipped']) ,
                        num_total=status['files'])
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
                        percent_complete = (float(status['files_transferred'] + float(status['files_skipped'])) / float(status['files'])) * 100
                        self.display_status(
                            percent_complete=percent_complete,
                            task_id=task_id,
                            num_completed=int(status['files_transferred']) + int(status['files_skipped']),
                            num_total=status['files'])
                    self.status = JobStatus.RUNNING
                if event and event.is_set():
                    client.cancel_task(task_id)
                    #self.error_cleanup()
                    return
            except Exception as e:
                logging.error(format_debug(e))
                client.cancel_task(task_id)
                #self.error_cleanup()
                return
            time.sleep(5)
