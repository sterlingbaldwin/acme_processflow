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
            'regrid_map_path': ''
        }
        self.proc = None

    def execute(self):
        """
            Calls ncclimo in a subprocess
        """
        cmd = [
            'ncclimo',
            '-c', self.inputs['caseId'],
            '-a', self.inputs['annual_mode'],
            '-s', self.inputs['start_year'],
            '-e', self.inputs['end_year'],
            '-i', self.inputs['input_directory'],
            '-r', self.inputs['regrid_map_path'],
            '-o', self.inputs['climo_output_directory'],
            '-O', self.inputs['regrid_output_directory']
        ]
        self.proc = Popen(
            cmd,
            stdout=PIPE,
            stderr=PIPE,
            shell=True)
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
            if done < 0:
                break
            sleep(1)
            self.outputs['console_output'] = console_output
            print console_output

        self.status = 'complete'

    def save(self, conf_path):
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
