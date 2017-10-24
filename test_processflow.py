import unittest
import os
import shutil
from processflow import main
from configobj import ConfigObj

class TestProcessflow(unittest.TestCase):

    def test_processflow(self):
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')

        config = ConfigObj(config_path)
        project_path = config['global']['project_path']
        if os.path.exists(project_path):
            shutil.rmtree(project_path)

        testargs = ['-c', config_path, '-n', '-f']
        ret = main(test=True, testargs=testargs)
        self.assertTrue(ret)

if __name__ == '__main__':
    unittest.main()
