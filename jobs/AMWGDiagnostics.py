from uuid import uuid4


class AMWGDiagnostic(object):
    def __init__(self, config):
        """
            Setup class attributes

            inputs:
                test_casename: the name of the test case e.g. b40.20th.track1.2deg.001
                test_filetype: the filetype of the history files, either monthly_history or time_series
                test_path_history: path to the directory holding your history files
                test_path_climo: path to directory holding climo files
                test_path_diag: the output path for the diagnostics to go
                control: what type to use for the control set, either OBS for observations, or USER for another model
                    the following are only set with control==USER
                    cntl_casename: the case_name of the control case
                    cntl_filetype: either monthly_history or time_series
                    cntl_path_history: path to the control history file
                    cntl_path_climo: path to the control climo files
        """
        self.inputs = {
            'test_casename': '',
            'test_filetype': ''
        }
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
