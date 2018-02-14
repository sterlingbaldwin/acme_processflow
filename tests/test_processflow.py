import unittest
import os, sys
import shutil
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from configobj import ConfigObj

from processflow import main


class TestProcessflow(unittest.TestCase):

    def test_processflow_with_inplace_data(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config_path = os.path.join(
            os.getcwd(), 'tests', 'test_no_sta_minimal.cfg')

        config = ConfigObj(config_path)
        project_path = config['global']['project_path']
        if os.path.exists(os.path.join(project_path, 'input')):
            print 'testing with inplace data'
            testargs = ['-c', config_path, '-n', '-f']
            ret = main(test=True, testargs=testargs)
            self.assertEqual(ret, 0)
        else:
            print 'data not yet produced, skipping inplace data check'

    def test_processflow_from_scratch_no_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config_path = os.path.join(
            os.getcwd(), 'tests', 'test_no_sta_minimal.cfg')

        config = ConfigObj(config_path)
        project_path = config['global']['project_path']
        if os.path.exists(project_path):
            print "removing previous project directory {}".format(project_path)
            shutil.rmtree(project_path, ignore_errors=True)
            print "project cleanup complete"
        testargs = ['-c', config_path, '-n', '-f']
        ret = main(test=True, testargs=testargs)
        self.assertEqual(ret, 0)

    def test_processflow_from_scratch_yes_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config_path = os.path.join(
            os.getcwd(), 'tests', 'test_yes_sta_minimal.cfg')

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
