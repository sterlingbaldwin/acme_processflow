from subprocess import Popen, PIPE
from util import print_debug
from util import print_message


class Diagnostic(object):
    """
        Performs the ACME diagnostic job
    """
    def __init__(self, config=None):
        self.config = {}
        self.status = self.validate(config)

    def execute(self):
        """
            Executes the diagnostic job
        """
        print_message('Im totally executing', 'ok')

    def validate(self, config=None):
        """
            Validates the config options
            Valid options are: model_path, obs_path, output_path, package, sets
        """
        valid_arguments = [
            'model_path',
            'obs_path',
            'output_path',
            'package',
            'sets'
        ]
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
                elif i == 'sets':
                    default = '5'
                print_message('{} not found in config, using {}'.format(i, default))

