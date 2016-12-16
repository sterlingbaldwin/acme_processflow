# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
from util import print_debug
from util import print_message
from uuid import uuid4
import json
from pprint import pformat
from subprocess import Popen, PIPE
from time import sleep
class Climo(object):
    """
        A wrapper around ncclimo, used to compute the climotologies from raw output data
    """
    def __init__(self, config):
        self.config = {}
        self.status = 'unvarified'
        self.type = 'climo'
        self.uuid = uuid4().hex
        self.yearset = config.get('yearset', 0)
        self.job_id = 0
        self.outputs = {
            'status': self.status,
            'climos': '',
            'regrid': '',
            'console_output': ''
        }
        self.inputs = {
            'start_year': '',
            'end_year': '',
            'caseId': '',
            'annual_mode': 'sdd',
            'input_directory': '',
            'climo_output_directory': '',
            'regrid_output_directory': '',
            'regrid_map_path': '',
            'yearset': '',
        }
        self.proc = None
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '0-01:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
        }
        self.prevalidate(config)

    def get_type(self):
        """
            Returns job type
        """
        return self.type

    def execute(self, batch=False):
        """
            Calls ncclimo in a subprocess
        """
        cmd = [
            '/export/baldwin32/scripts/ncclimo',
            '-c', self.config['caseId'],
            '-a', self.config['annual_mode'],
            '-s', self.config['start_year'],
            '-e', self.config['end_year'],
            '-i', self.config['input_directory'],
            '-r', self.config['regrid_map_path'],
            '-o', self.config['climo_output_directory'],
            '-O', self.config['regrid_output_directory'],
        ]
        if not batch:
            # Not running in batch mode
            self.proc = Popen(
                cmd,
                stdout=PIPE,
                stderr=PIPE,
                shell=False)
            self.status = 'running'
            done = 2
            console_output = ''
            while done != 0:
                done = self.proc.poll()
                lines = self.proc.stdout.readlines()
                for line in lines:
                    console_output += line
                lines = self.proc.stderr.readlines()
                for line in lines:
                    console_output += line
                print console_output
                if done < 0:
                    break
                sleep(1)
                self.outputs['console_output'] = console_output
                print console_output

            self.status = 'complete'
        else:
            # Submitting the job to SLURM
            cmd.append('-p mpi')
            run_script = './run_scripts/ncclimo_job_' + str(self.uuid)
            with open(run_script, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'
                batchfile.write(slurm_prefix)
                slurm_command = ' '.join(cmd)
                batchfile.write(slurm_command)
            slurm_cmd = ['sbatch', run_script]
            self.proc = Popen(slurm_cmd, stdout=PIPE)
            out = self.proc.communicate()
            self.status = 'running'
            index = out[0].find('job') + 4
            self.job_id = int(out[0][index:].strip())
            return self.job_id


    def save(self, conf_path):
        """
            Saves job configuration to a json file at conf_path
        """
        try:
            with open(conf_path, 'r') as infile:
                config = json.load(infile)
            with open(conf_path, 'w') as outfile:
                config[self.uuid]['inputs'] = self.config
                config[self.uuid]['outputs'] = self.outputs
                config[self.uuid]['type'] = self.type
                json.dump(config, outfile, indent=4)
        except Exception as e:
            print_message('Error saving configuration file')
            print_debug(e)
            raise

    def __str__(self):
        return pformat({
            'config': self.config,
            'status': self.status
        }, indent=4)

    def prevalidate(self, config):
        """
            Prerun validation for inputs
        """
        if self.status == 'valid':
            return 0
        for i in config:
            if i not in self.inputs:
                print_message("Unexpected arguement: {}, {}".format(i, config[i]))
            else:
                self.config[i] = config.get(i)
        self.status = 'valid'
        return 0

    def postvalidate(self):
        """
            Post execution validation
        """
        print "post validation"
