from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE
from util import print_debug
from util import print_message
from pprint import pformat


class CMOREjob(object):
    def __init__(self, config):
        """
        Setup class attributes
        """
        self.inputs = {}
        self.outputs = {}
        self.uuid = uuid4().hex
        self.status = 'unvalidated'
        self.proc = None
        self.job_id = None
        self.depends_on = []
        self.type = 'publication'
        self.config = {}
        self.prevalidate(config)

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid
        }, indent=4)

    def get_type(self):
        """
        Returns job type
        """
        return self.type

    def prevalidate(self, config):
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        depends = config.get('depends_on')
        if depends:
            self.depends_on = depends
        self.status = 'valid'

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        """
        print 'postvalidate'

    def set_status(self, status):
        """
        Sets the status field to the string input status
        """
        self.status = status

    def execute(self, batch=False):
        """
        Perform the actual work
        """
        print 'executing'
