from time import sleep
from util import print_debug
from util import print_message
from getpass import getpass

import paramiko
from paramiko import PasswordRequiredException

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
            return
        print config
        self.remote_host = config.get('remote_host')
        if not self.remote_host:
            print "No remote host specified"
            return
        self.remote_dir = config.get('remote_dir')
        if not self.remote_dir:
            print "No remote directory specified"
            return
        self.username = config.get('username')
        if not self.username:
            print "No username given"
            return
        self.pattern = config.get('pattern')
        if not self.pattern:
            print "No search pattern given"
            return
        self.password = config.get('password')
        self.keyfile = config.get('keyfile')
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
        if self.keyfile:
            try:
                key = paramiko.RSAKey.from_private_key_file(self.keyfile)
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    pkey=key
                )
            except PasswordRequiredException as pwe:
                keypass = getpass("Enter private key password: ")
                key = paramiko.RSAKey.from_private_key_file(self.keyfile, password=keypass)
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    pkey=key
                )
            except Exception as e:
                print "Unable to connect to remote host with given private key"
                print_debug(e)
                return -1
        else:
            try:
                self.client.connect(
                    port=22,
                    hostname=self.remote_host,
                    username=self.username,
                    password=self.password
                )
            except Exception as e:
                print "Unable to connect to host with given username/password"
                print_debug(e)
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

    def check(self):
        """
            Checks to remote_dir for any files that arent in the known files list
        """
        cmd = 'ls {path} | grep {pattern}'.format(
            path=self.remote_dir,
            pattern=self.pattern
        )
        stdin, stdout, stderr = self.client.exec_command(cmd)
        files = stdout.read()
        files.strip()
        files = files.split()
        self.new_files = []
        count = 0
        for f in files:
            if f not in self.known_files:
                self.known_files.append(f)
                self.new_files.append(f)

    def get_new_files(self):
        """
            Returns a list of only the new files that have been added since the last check
        """
        return self.new_files
