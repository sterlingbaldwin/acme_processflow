from subprocess import Popen, PIPE
from time import sleep
from util import print_debug
from util import print_message
from pprint import pformat



class Diagnostic(object):
    """
        Performs the ACME diagnostic job
    """
    def __init__(self, config=None):
        self.config = {}
        self.status = self.validate(config)
        self.inputs = [
            'model_path',
            'obs_path',
            'output_path',
            'package',
            'set',
            'archive'
        ]

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
            cmd.append(i)
            cmd.append(self.config[i])
        try:
            proc = Popen(cmd, stdout=PIPE)
            done = 2
            while done != 0:
                done = proc.poll()
                line = proc.stdout.readline()
                if done < 0:
                    break
                sleep(1)
        except Exception as e:
            print_debug(e)
            print_message('Error running diagnostic')

    def validate(self, config=None):
        """
            Validates the config options
            Valid options are: model_path, obs_path, output_path, package, sets
        """

        for i in config:
            if i not in valid_arguments:
                print_message('Unexpected argument: {}, {}'.format(i, config[i]))
            else:
                self.config[i] = config[i]

        for i in valid_arguments:
            if i not in self.config:
                default = ''
                if i == 'model_path':
                    print_message('model_path is a required argument, exiting')
                    return 'Invalid'
                elif i == 'obs_path':
                    print_message('obs_path is a required argument, exiting')
                    return 'Invalid'
                elif i == 'output_path':
                    default = '.'
                elif i == 'package':
                    default = 'amwg'
                elif i == 'set':
                    default = '5'
                elif i == 'archive':
                    default = 'False'
                self.config[i] = default
                print_message('{} not found in config, using {}'.format(i, default), 'ok')
        return 'valid'

