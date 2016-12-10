# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
from subprocess import Popen, PIPE
import os
import shutil
import json
from pprint import pformat
from uuid import uuid4
from time import sleep
from util import print_debug
from util import print_message


class Diagnostic(object):
    """
        Performs the ACME diagnostic job
    """
    def __init__(self, config=None):
        self.raw_config = config
        self.config = {}
        self.proc = None
        self.type = 'diagnostic'
        self.status = 'unvalidated'
        self.yearset = config.get('yearset', 0)
        self.uuid = uuid4().hex
        self.inputs = {
            '--model': '',
            '--obs': '',
            '--outputdir': '',
            '--package': '',
            '--set': '',
            '--archive': ''
        }
        self.outputs = {
            'output_path': '',
            'console_output': '',
            'status': self.status
        }
        self.validate(config)

    def __str__(self):
        return pformat({
            'config': self.config,
            'status': self.status
        }, indent=4)

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

    def execute(self):
        """
            Executes the diagnostic job.
            If archive is set to True, will create a tarbal of the output directory
        """
        cmd = ['metadiags']
        for i in self.config:
            if i == '--archive':
                continue
            cmd.append(i)
            cmd.append(self.config[i])
        cmd.append('--dryrun')
        cmd = ' '.join(cmd)
        try:
            console_output = ''
            print_message(cmd, 'ok')
            self.proc = Popen(
                cmd,
                stdout=PIPE,
                stdin=PIPE,
                stderr=PIPE,
                shell=True)
            self.status = 'running'
            done = 2
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
            with open('config.json', 'r+') as infile:
                config = json.load(infile)
                config.get('diagnostic')['outputs'] = self.outputs
            with open('config.json', 'w') as outfile:
                json.dump(config, outfile, indent=4, sort_keys=True)
            self.status = 'complete'
        except Exception as e:
            self.status = 'error'
            print_debug(e)
            print_message('Error running diagnostic')

        if self.config['--archive'] == 'True':
            archive_path = '{}/archive'.format(self.config['--outputdir'])
            if not os.path.exists(archive_path + '.tar.gz'):
                try:
                    print_message(
                        'creating output archive {}'.format(archive_path + '.tar.gz'),
                        'ok')
                    shutil.make_archive(
                        archive_path,
                        'gztar',
                        self.config.get('output_dir'))
                except Exception as e:
                    print_debug(e)
                    print_message('Error making archive {}'.format(archive_path + '.tar.gz'))

            else:
                print_message('archive {} already exists'.format(archive_path + '.tar.gz'))

    def postvalidate(self):
        """
            Post run validation
        """
        valid = True
        error = ''
        output_files = os.listdir(self.config.get('output').get('--outputdir'))
        if len(output_files) < 100:
            valid = False
            error = 'Too few output files\n'
        for f in output_files:
            try:
                size = os.path.getsize(f)
            except os.error as e:
                print_debug(e)
                valid = False
                error += 'Unable to open file {}\n'.format(f.name)
            else:
                if size <= 0:
                    valid = False
                    error += 'File {} size to small\n'.format(f.name)
        return valid, error

    def prevalidate(self, config=None):
        """
            Validates the config options
            Valid options are: model_path, obs_path, output_path, package, sets
        """
        if self.status == 'valid':
            return 0
        inputs = config.get('inputs')
        for i in inputs:
            if i not in self.inputs:
                print_message('Unexpected argument: {}, {}'.format(i, config[i]))
            else:
                if i == '--model':
                    self.config['--model'] = 'path=' + inputs[i] + ',climos=yes'
                elif i == '--obs':
                    self.config['--obs'] = 'path=' + inputs[i] + ',climos=yes'
                elif i == '--outputdir':
                    self.config['--outputdir'] = inputs[i]
                elif i == '--package':
                    self.config['--package'] = inputs[i]
                elif i == '--set':
                    self.config['--set'] = inputs[i]
                elif i == '--archive':
                    self.config['--archive'] = inputs[i]

        for i in self.inputs:
            if i not in self.config:
                default = ''
                if i == '--model':
                    print_message('model_path is a required argument, exiting')
                    return 'Invalid'
                elif i == '--obs':
                    print_message('obs_path is a required argument, exiting')
                    return 'Invalid'
                elif i == '--outputdir':
                    default = os.getcwd()
                    self.config['--outputdir'] = default
                elif i == '--package':
                    default = 'amwg'
                    self.config['--package'] = default
                elif i == '--set':
                    default = '5'
                    self.config['--set'] = default
                elif i == '--archive':
                    default = 'False'
                    self.config['archive'] = default
                print_message('{} not found in config, using {}'.format(i, default), 'ok')
        self.outputs['output_path'] = self.config['--outputdir']
        self.outputs['console_output'] = ''
        self.status = 'valid'
        return 0

