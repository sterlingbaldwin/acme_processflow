from util import print_debug
from util import print_message


class Pipeline(object):

    def __init__(self):
        self.pipe = []
        self.status = 'empty'
        self.outputs = []

    def validate(self):
        for j in self.pipe:
            try:
                if j.validate():
                    continue
                else:
                    self.status = 'invalid'
                    return
            except Exception as e:
                print_debug(e)
                print_message('Error validating job ' + str(j))
        self.status = 'valid'

    def append(self, job):
        self.pipe.append(job)

    def execute(self):
        if self.status != 'valid':
            print_message('Unable to execute in an invalid state')
            return
        for i, j in enumerate(self.pipe):
            outputs = j.execute()
            if i != len(self.pipe) - 1:
                self.pipe[i + 1].inputs.append(outputs)
            self.outputs.append(outputs)
    