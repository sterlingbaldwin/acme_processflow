from subprocess import Popen, PIPE
from pprint import pformat
from time import sleep
from util import print_debug
from util import print_message


class Diagnostic(object):
    """
        Performs the ACME diagnostic job
    """
    def __init__(self, config=None):
        self.config = {}
        self.proc = None
        self.inputs = {
            '--model': '',
            '--obs': '',
            '--outputdir': '',
            '--package': '',
            '--set': '',
            'archive': ''
        }
        self.outputs = {
            'output_path': '',
            'console_output': '',
        }
        self.status = self.validate(config)

    def __str__(self):
        return pformat({
            'config': self.config,
            'status': self.status
        }, indent=4)

    def execute(self):
        """
            Executes the diagnostic job.
            If archive is set to True, will create a tarbal of the output directory
        """
        cmd = ['metadiags']
        for i in self.config:
            if i == 'archive':
                continue
            cmd.append(i)
            cmd.append(self.config[i])
        try:
            console_output = ''
            print_message(cmd, 'ok')
            self.proc = Popen(
                cmd,
                stdout=PIPE,
                shell=True)
            self.status = 'running'
            done = 2
            while done != 0:
                done = self.proc.poll()
                line = self.proc.stdout.readline()
                console_output += line
                print line
                if done < 0:
                    break
                sleep(1)
            console_output_path = '{}/console_output.txt'.format(self.config.get('output_path'))
            with open(console_output_path, 'w') as outfile:
                outfile.write(console_output)
            self.outputs['console_output'] = console_output
            self.status = 'complete'
        except Exception as e:
            self.status = 'error'
            print_debug(e)
            print_message('Error running diagnostic')

    def validate(self, config=None):
        """
            Validates the config options
            Valid options are: model_path, obs_path, output_path, package, sets
        """
        for i in config:
            if i not in self.inputs:
                print_message('Unexpected argument: {}, {}'.format(i, config[i]))
            else:
                if i == '--model':
                    self.config['--model'] = 'path=' + config[i] + ',climos=yes'
                elif i == '--obs':
                    self.config['--obs'] = 'path=' + config[i] + ',climos=yes'
                elif i == '--outputdir':
                    self.config['--outputdir'] = config[i]
                elif i == '--package':
                    self.config['--package'] = config[i]
                elif i == '--set':
                    self.config['--set'] = config[i]


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
                    default = '.'
                    self.config['--outputdir'] = default
                elif i == '--package':
                    default = 'amwg'
                    self.config['--package'] = default
                elif i == '--set':
                    default = '5'
                    self.config['--set'] = default
                elif i == 'archive':
                    default = 'False'
                    self.config['archive'] = default
                print_message('{} not found in config, using {}'.format(i, default), 'ok')
        self.outputs['output_path'] = self.config['--outputdir']
        return 'valid'

