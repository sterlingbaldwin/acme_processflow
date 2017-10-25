import unittest
import os
import shutil
from processflow import main
from configobj import ConfigObj

class TestProcessflow(unittest.TestCase):

    def test_processflow_with_inplace_data(self):
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')

        config = ConfigObj(config_path)
        project_path = config['global']['project_path']
        if os.path.exists(os.path.join(project_path, 'input')):
            print 'testing with inplace data'
            testargs = ['-c', config_path, '-n', '-f']
            ret = main(test=True, testargs=testargs)
            self.assertEqual(ret, 0)
        else:
            print 'data not yet produced, skipping inplace data check'

    def test_processflow_from_scratch(self):
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')

        config = ConfigObj(config_path)
        project_path = config['global']['project_path']
        if os.path.exists(project_path):
            print "removing previous project directory"
            shutil.rmtree(project_path, ignore_errors=True)
            print "project cleanup complete"
        testargs = ['-c', config_path, '-n', '-f']
        ret = main(test=True, testargs=testargs)
        self.assertEqual(ret, 0)

if __name__ == '__main__':
    unittest.main()
