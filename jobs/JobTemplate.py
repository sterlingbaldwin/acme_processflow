from uuid import uuid4


class JobTemplate(object):
    def __init__(self, config):
        """
            Setup class attributes
        """
        self.inputs = {}
        self.outputs = {}
        self.uuid = uuid4().hex
        print "init"

    def prevalidate(self, config):
        """
            Iterate over given config dictionary making sure all the inputs are set
            and rejecting any inputs that arent in the input dict
        """
        print 'prevalidate'

    def postvalidate(self):
        """
            Check that what the job was supposed to do actually happened
        """
        print 'postvalidate'

    def execute(self):
        """
            Perform the actual work
        """
        print 'execute'
