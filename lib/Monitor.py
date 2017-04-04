import paramiko
import logging

from time import sleep
from util import print_debug
from util import print_message
from util import format_debug
from getpass import getpass
from pprint import pformat

from paramiko import PasswordRequiredException
from paramiko import SSHException


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
        self.remote_host = config.get('remote_host')
        if not self.remote_host:
            print "No remote host specified"
            return None
        self.remote_dir = config.get('remote_dir')
        if not self.remote_dir:
            print "No remote directory specified"
            return None
        self.username = config.get('username')
        if not self.username:
            print "No username given"
            return None
        self.patterns = config.get('patterns')
        if not self.patterns:
            print "No search pattern given"
            return None
        self.password = config.get('password', None)
        self.keyfile = config.get('keyfile', None)
        self.client = None
        self.known_files = []
        self.new_files = []

    def connect(self):
        """
            Connects to the remote host over ssh
        """
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.keyfile and self.password:
            try:
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    password=self.password)
            except Exception as e:
                print "Unable to connect to host with given username/password"
                print_debug(e)
                return -1
        elif self.keyfile:
            try:
                key = paramiko.RSAKey.from_private_key_file(self.keyfile)
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    pkey=key)
            except PasswordRequiredException as pwe:
                for attempt in range(3):
                    success = False
                    try:
                        keypass = getpass("Enter private key password: ")
                        key = paramiko.RSAKey.from_private_key_file(self.keyfile, password=keypass)
                        success = True
                    except SSHException as e:
                        print_message('Unable to unlock key file, retry')
                if not success:
                    print_message('To many unlock attempts, exiting')
                    return -1
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    pkey=key)
            except Exception as e:
                print "Unable to connect to remote host with given private key"
                print_debug(e)
                return -1
        elif self.password:
            try:
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    password=self.password)
            except Exception as e:
                print "Unable to connect to host with given username/password"
                print_debug(e)
                return -1
        else:
            print "no password or keyfile"
            return -1
        return 0

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

    def get_remote_file_info(self, filepath):
        cmd = 'ls -la {}'.format(filepath)
        _, stdout, _ = self.client.exec_command(cmd)
        info = stdout.read()
        info = info.split()
        return info[4], ' '.join(info[5:7])

    def get_remote_file_info_batch(self, filelist):
        cmd = 'ls -la {}'.format(' '.join(filelist))
        _, stdout, _ = self.client.exec_command(cmd)
        info = stdout.read()
        info = info.split('\n')
        out = []
        for line in info:
            lineinfo = line.split()
            if not lineinfo or len(lineinfo) < 8:
                continue
            out.append((lineinfo[4], ' '.join(lineinfo[5:8]), lineinfo[-1]))
        return out

    def check(self):
        """
        Checks to remote_dir for any files that arent in the known files list
        """
        # cmd = 'ls {path} | grep {pattern}'.format(
        #     path=self.remote_dir,
        #     pattern=self.pattern)
        self.new_files = []
        for pattern in self.patterns:
            if isinstance(pattern, str) or isinstance(pattern, unicode):
                name = '-name "*{}*"'.format(pattern)
            else:
                name = '-name *"' + '"* -or -name *"'.join(pattern) + '*"'
            cmd = 'find {dir} {name}'.format(
                name=name,
                dir=self.remote_dir)
            print cmd
            _, stdout, stderr = self.client.exec_command(cmd)
            files = stdout.read()
            files.strip()
            files = files.split()
            fileinfo = self.get_remote_file_info_batch(files)
            for info in fileinfo:
                try:
                    (item for item in self.known_files if item.get('filename') == info[2]).next()
                except StopIteration:
                    new_file = {
                        'size': info[0],
                        'date': info[1],
                        'filename': info[2]
                    }
                    self.known_files.append(new_file)

    def remove_new_file(self, file):
        """
        Removes a files from the new_files listls -l
        """
        if file in self.new_files:
            self.new_files.remove(file)

    def get_new_files(self):
        """
            Returns a list of only the new files that have been added since the last check
        """
        return self.new_files
