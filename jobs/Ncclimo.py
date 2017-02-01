# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import os
import re
import json
import logging

from uuid import uuid4
from pprint import pformat
from subprocess import Popen, PIPE
from time import sleep

from util import print_debug
from util import print_message
from util import check_slurm_job_submission

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
        self.depends_on = []
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
            'ncclimo_path': ''
        }
        self.proc = None
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 1 hour run time
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
        ncclimo = os.path.join(self.config['ncclimo_path'], 'ncclimo')
        cmd = [
            ncclimo,
            '-c', self.config['caseId'],
            '-a', self.config['annual_mode'],
            '-s', str(self.config['start_year']),
            '-e', str(self.config['end_year']),
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
            self.status = 'RUNNING'
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

            self.status = 'COMPLETED'
            return 0
        else:
            # Submitting the job to SLURM
            expected_name = 'ncclimo_job_' + str(self.uuid)
            run_scripts_path = os.path.join(os.getcwd(), 'run_scripts')
            run_script = os.path.join(run_scripts_path, expected_name)

            if not os.path.exists(run_scripts_path):
                os.makedirs(run_scripts_path)

            self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
            self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

            with open(run_script, 'w') as batchfile:
                batchfile.write('#!/bin/bash\n')
                slurm_prefix = '\n'.join(['#SBATCH ' + self.slurm_args[s] for s in self.slurm_args]) + '\n'
                batchfile.write(slurm_prefix)
                slurm_command = ' '.join(cmd)
                batchfile.write(slurm_command)

            slurm_cmd = ['sbatch', run_script]
            started = False
            retry_count = 0
            while not started and retry_count < 5:
                self.proc = Popen(slurm_cmd, stdout=PIPE)
                output, err = self.proc.communicate()
                started, job_id = check_slurm_job_submission(expected_name)
                if started:
                    self.status = 'RUNNING'
                    self.job_id = job_id
                    logging.info('Starting climo job with job_id %s', job_id)
                    # print_message('+++++ STARTING CLIMO JOB {0} +++++'.format(self.job_id))
                elif retry_count <= 0:
                    logging.warning("Error starting climo job\n%s", output)
                    print_message("Error starting climo job")
                    print_message(output)
                    self.job_id = 0
                    break
                else:
                    logging.warning('Failed to start job trying again, attempt %s', str(retry_count))
                    print_message('Failed to start job, trying again')
                    retry_count += 1
                    continue
            return self.job_id

    def set_status(self, status):
        self.status = status

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
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id
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

        # after checking that the job is valid to run,
        # check if the output already exists and the job actually needs to run
    #    if os.path.exists(self.config.get('climo_output_directory')):
    #        contents = os.listdir(self.config.get('climo_output_directory'))
    #        if len(contents) <= 10:
    #            return 0
    #        else:
    #            for i in contents:
    #                if os.path.isdir(self.config.get('climo_output_directory') + '/' + i):
    #                    continue
    #                if not re.match(self.config.get('caseId'), i):
    #                    return 0
    #            self.status = 'COMPLETED'
        return 0

    def postvalidate(self):
        """
        Post execution validation
        """
        print "post validation"
