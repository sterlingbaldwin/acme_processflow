import unittest
import os
import sys
import shutil
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from configobj import ConfigObj
from lib.util import print_message
from processflow import main


class TestProcessflow(unittest.TestCase):

    def test_processflow_with_inplace_data(self):
        """
        End to end test of the processflow with inplace data
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        config_path = os.path.join(
            os.getcwd(),
            'tests',
            'test_configs',
            'test_processflow_with_inplace_data.cfg')
        output_path = os.path.join(
            '/export/baldwin32/jenkins/workspace/',
            inspect.stack()[0][3])
        if os.path.exists(output_path):
            print 'removing previous output directory'
            shutil.rmtree(output_path)
        print "---- project cleanup complete ----"

        config = ConfigObj(config_path)
        testargs = ['-c', config_path, '-f', '-r',
                    './resources', '-s', '-o', output_path, '-d']
        ret = main(test=True, testargs=testargs)
        self.assertEqual(ret, 0)


    # def test_processflow_from_scratch_no_sta(self):
    #     """
    #     End to end test of the processflow from scratch
    #     """
    #     print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
    #     config_path = os.path.join(
    #         os.getcwd(),
    #         'tests',
    #         'test_configs',
    #         'test_run_no_sta.cfg')
    #     output_path = os.path.join(
    #         '/export/baldwin32/jenkins/workspace/',
    #         inspect.stack()[0][3],
    #         'output')
    #     input_path = os.path.join(
    #         '/export/baldwin32/jenkins/workspace/',
    #         inspect.stack()[0][3],
    #         'input')
    #     if os.path.exists(output_path):
    #         shutil.rmtree(output_path)
    #     if os.path.exists(input_path):
    #         shutil.rmtree(input_path)

    #     config = ConfigObj(config_path)
    #     testargs = ['-c', config_path, '-n', '-f', '-r',
    #                 './resources', '-s', '-o', output_path, '-i', input_path]
    #     ret = main(test=True, testargs=testargs)
    #     self.assertEqual(ret, 0)

    def test_processflow_from_scratch_yes_sta(self):
        """
        End to end test from scrath with no sta
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        config_path = os.path.join(
            os.getcwd(),
            'tests',
            'test_configs',
            'test_processflow_with_inplace_data.cfg')
        output_path = os.path.join(
            '/export/baldwin32/jenkins/workspace/',
            inspect.stack()[0][3],
            'output')
        input_path = os.path.join(
            '/export/baldwin32/jenkins/workspace/',
            inspect.stack()[0][3],
            'input')
        if os.path.exists(output_path):
            print "removing previous output directory"
            shutil.rmtree(output_path)
        if os.path.exists(input_path):
            print "removing previous input directory"
            shutil.rmtree(input_path)
        print "---- project cleanup complete ----"
        testargs = ['-c', config_path, '-n', '-f',
                    '-r', './resources', '-o', output_path, '-i', input_path, '-d']
        ret = main(test=True, testargs=testargs)
        self.assertEqual(ret, 0)

    # def test_processflow_from_minimal_comprehensive(self):
    #     print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
    #     config_path = os.path.join(
    #         os.getcwd(), 'tests', 'test_configs', 'test_minimal_comprehensive.cfg')
    #     output_path = os.path.join(
    #         '/export/baldwin32/jenkins/workspace/', inspect.stack()[0][3])
    #     if os.path.exists(output_path):
    #         shutil.rmtree(output_path)

    #     config = ConfigObj(config_path)
    #     project_path = config['global']['project_path']
    #     if os.path.exists(project_path):
    #         print "removing previous project directory"
    #         shutil.rmtree(project_path, ignore_errors=True)
    #         print "project cleanup complete"
    #     testargs = ['-c', config_path, '-f', '-r',
    #                 './resources', '-o', output_path]
    #     ret = main(test=True, testargs=testargs)
    #     self.assertEqual(ret, 0)

    #     print "Starting completed run over again"
    #     ret = main(test=True, testargs=testargs)
    #     self.assertEqual(ret, 0)


if __name__ == '__main__':
    unittest.main()
