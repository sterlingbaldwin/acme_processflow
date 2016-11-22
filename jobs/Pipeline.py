# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import json
import paramiko
from util import print_debug
from util import print_message


class Pipeline(object):
    """
        A job pipeline. Stores, validates, and executes ACME workflow jobs.
    """
    def __init__(self, remote=False, username=None, password=None, privatekey=None):
        self.pipe = []
        self.status = 'empty'
        self.outputs = []
        self.conf_path = ''
        self.remote = remote
        self.username = username
        self.password = password
        self.privatekey = privatekey

    def generate_jobs(self, data_path):
        """
            Populate the pipeline based on the data in the data_path
        """
        if self.remote:
            # handle remote files
            print 'remote'
        else:
            data_files = os.listdir(data_path)

    def set_conf_path(self, path):
        self.conf_path = path

    def append(self, job):
        """
            Adds a job to the pipeline
        """
        self.pipe.append(job)
        self.status = 'unvalidated'

    def validate(self):
        """
            Validates the pipeline. Sorts the jobs, makes sure
            the outputs map into the inputs for the next in line.
            On error returns -1, returns 0 otherwise.
        """
        valid = True
        for job in self.pipe:
            if job.prevalidate() == -1:
                valid = False
        if valid:
            self.status = 'valid'
            return 0
        else:
            return -1

    def execute(self):
        """
            Executes all jobs in the pipeline,
            feeding the outputs from job i into the inputs of job i + 1
        """
        if self.status != 'valid':
            print_message('Unable to execute in an invalid state')
            return
        for i, j in enumerate(self.pipe):
            outputs = j.execute()
            valid, error = j.postvalidate()
            if i != len(self.pipe) - 1:
                # self.pipe[i + 1].inputs.update(outputs)
                if j.type == 'diagnostic' and self.pipe[i + 1].type == 'transfer':
                    self.pipe[i + 1].config.get('source_endpoint')['path'] = j.outputs.get('output_path')
                elif j.type == 'transfer' and self.pipe[i + 1].type == 'diagnostic':
                    self.pipe[i + 1].config['--model'] = j.outputs.get('destination_endpoint').get('path')
                print self.pipe[i + 1]
                self.pipe[i + 1].save(self.conf_path)
            self.outputs.append(outputs)
        with open('workflow_output.json', 'w') as outfile:
            json.dump(self.outputs, outfile, indent=4)
    