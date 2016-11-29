from time import sleep
import paramiko


class Monitor(object):
    """
        A class to monitor a remote directory, and pull down any files matching the given regex
    """
    def __init__(self, config=None):
        if not config:
            print "No configuration for monitoring system"
            return -1
        self.remote_host = config.get('remote_host')
        if not self.host:
            print "No remote host specified"
            return -1
        self.remote_dir = config.get('remote_dir')
        if not self.remote_dir:
            print "No remote directory specified"
            return -1
        self.username = config.get('username')
        if not self.username:
            print "No username given"
            return -1