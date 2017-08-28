# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import logging
import json
import re
from pprint import pformat
from time import sleep

from globus_cli.services.transfer import get_client
from globus_cli.commands.ls import _get_ls_res as get_ls

from util import setup_globus
from lib.events import Event_list


class Monitor(object):
    """
    A class to monitor a remote directory, and pull down any files matching the given regex
    """
    def __init__(self, **kwargs):
        """
        Initializes the monitoring system,
        
        Parameters: 
            remote_host (str): the hostname of the remote system
            remote_dir (str): the path on the remote system to look for files
            source_endpoint (globus UUID): the ID of the data source
            patterns (list: str): A list of file patterns to look for
            no_ui (boolean: optional): The current UI mode
            src (str): An email address source for prompts
            dst (str): An email address destination for prompts
            event_list (Event_list): A list of events for pushing updates into
            display_event (Threadding_event): A Threadding_event to turn off the display for user prompts
            client (globus client): The globus client to use for file transfers
        """
        self.source_endpoint = kwargs.get('source_endpoint')
        self.remote_dir = kwargs.get('remote_dir')
        self.patterns = kwargs.get('patterns')
        self.no_ui = kwargs.get('no_ui', False)
        self.src = kwargs.get('src')
        self.dst = kwargs.get('dst')
        self.event_list = kwargs.get('event_list')
        self.display_event = kwargs.get('display_event')
        self.client = None
        self._known_files = []
        self._new_files = []
    
    def __str__(self):
        return pformat({
            "source_endpoint": self.source_endpoint,
            "remote_dir": self.remote_dir,
            "patterns": self.patterns,
            "no_ui": self.no_ui,
            "email_src": self.src,
            "email_dst": self.dst
        })

    def connect(self):
        """
        Activates the remote endpoint with globus credentials

        return False if error, True on success
        """
        setup_globus(
            endpoints=self.source_endpoint,
            no_ui=self.no_ui,
            src=self.src,
            dst=self.dst,
            event_list=self.event_list,
            display_event=self.display_event)
        self.client = get_client()
        result = self.client.endpoint_autoactivate(self.source_endpoint, if_expires_in=2880)
        if result['code'] == "AutoActivationFailed":
            return (False, result['message'])
        else:
            return (True, result['message'])

    def check(self):
        """
        Checks to remote_dir for any files that arent in the known files list
        """
        status, message = self.connect()
        if not status:
            msg = 'Unable to connect to globus endpoint: {}'.format(message)
            logging.error(msg)
            return False, msg
        self.new_files = []
        fail_count = 0
        while fail_count < 10:
            try:
                res = get_ls(
                    self.client,
                    self.remote_dir,
                    self.source_endpoint,
                    False, 0, False)
            except:
                fail_count += 1
                if fail_count >= 10:
                    msg = 'Unable to get remote directory contents after 10 tries'
                    logging.error(msg)
                    return False, msg
            else:
                break
        for file_info in res:
            for pattern in self.patterns:
                if not re.search(pattern=pattern, string=file_info['name']):
                    continue
                self.new_files.append({
                    'filename': file_info['name'],
                    'date': file_info['last_modified'],
                    'size': file_info['size']})
                break
        return True, 'success'

    @property
    def known_files(self):
        return self._known_files

    @known_files.setter
    def known_files(self, files):
        self._known_files = files

    @property
    def new_files(self):
        return self._new_files

    @new_files.setter
    def new_files(self, new_files):
        self._new_files = new_files
