from uuid import uuid4
from time import sleep
from subprocess import Popen, PIPE
from util import print_debug
from util import print_message
from pprint import pformat


class TestJob(object):
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
        self.type = 'test'
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
        self.status = status

    def execute(self, batch=False):
        """
            Perform the actual work
        """
        if not batch:
            sleep(30)
            self.status = 'complete'
        else:
            slurm_prefix = '#!/bin/bash\n#SBATCH -n 1\n#SBATCH -N 1\n#SBATCH -t 0-00:05\n'
            run_script = './run_scripts/test_job_' + self.uuid
            with open(run_script, 'w') as batchfile:
                batchfile.write(slurm_prefix)
                batchfile.write('sleep 30')

            slurm_cmd = ['sbatch', run_script]
            started = False
            retry_count = 5
            while not started:
                self.proc = Popen(slurm_cmd, stdout=PIPE)
                output = self.proc.communicate()[0]
                print_message('+++++ STARTING TEST JOB +++++\n{}'.format(output), 'ok')
                if 'Submitted batch job' in output or retry_count <= 0:
                    started = True
                    if retry_count <= 0:
                        print_message("Error starting climo job")
                        print_message(output)
                        self.job_id = 0
                        break
                else:
                    retry_count -= 1
                    continue
                self.status = 'running'
                index = output.find('job') + 4
                try:
                    self.job_id = int(output[index:].strip())
                except Exception as e:
                    retry_count -= 1
                    continue
                print_message('+++++ job_id: {} *****'.format(self.job_id), 'ok')
            return self.job_id
