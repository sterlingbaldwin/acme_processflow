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
from util import push_event


class Monitor(object):
    """
    A class to monitor a remote directory, and pull down any files matching the given regex
    """
    def __init__(self, config=None):
        """
        Initializes the monitoring system,
        inputs: remote_host, remote_dir, username, password (optional), keyfile (optional)
        """
        if not config:
            print "No configuration for monitoring system"
            return None
        self.source_endpoint = config.get('source_endpoint')
        if not self.source_endpoint:
            return None
        self.remote_dir = config.get('remote_dir')
        if not self.remote_dir:
            return None
        self.patterns = config.get('patterns')
        if not self.patterns:
            return None
        self.no_ui = config.get('no_ui', False)
        self.src = config.get('src')
        self.dst = config.get('dst')
        self.event_list = config.get('event_list')
        self.display_event = config.get('display_event')
        self.client = None
        self.known_files = []
        self.new_files = []

    def connect(self):
        """
        Activates the remote endpoint with globus credentials

        return False if error, True on success
        """
        self.event_list = push_event(self.event_list, 'attempting connection')
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

    def set_known_files(self, files):
        """
        Sets the list of known files.
        inputs: files, a list of filenames
        """
        self.known_files = files

    def get_known_files(self):
        """
        Returns the list of known files
        """
        return self.known_files

    def check(self):
        """
        Checks to remote_dir for any files that arent in the known files list
        """
        status, message = self.connect()
        if not status:
            logging.error('Unable to connect to globus endpoint: %s', message)
            return False
        self.new_files = []
        fail_count = 0
        while True:
            try:
                res = get_ls(
                    self.client,
                    self.remote_dir,
                    self.source_endpoint,
                    False, 0, False)
            except:
                fail_count += 1
                if fail_count > 10:
                    logging.error('Unable to get remote directory contents after 10 tries')
                    return False
                sleep(2)
            else:
                break
        for f in res:
            for p in self.patterns:
                if not re.search(pattern=p, string=f['name']):
                    continue
                self.new_files.append({
                    'filename': f['name'],
                    'date': f['last_modified'],
                    'size': f['size']
                })
                break

    def remove_new_file(self, rfile):
        """
        Removes a files from the new_files listls -l
        """
        if rfile in self.new_files:
            self.new_files.remove(file)

    def get_new_files(self):
        """
            Returns a list of only the new files that have been added since the last check
        """
        return self.new_files
