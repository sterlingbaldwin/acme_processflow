import os, sys
import unittest
import shutil
import inspect

from configobj import ConfigObj

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from jobs.JobStatus import JobStatus
from jobs.AMWGDiagnostic import AMWGDiagnostic
from lib.events import EventList
from lib.util import print_message

class TestAMWGDiagnostic(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAMWGDiagnostic, self).__init__(*args, **kwargs)
        config_path = os.path.join(
            os.getcwd(),
            'tests',
            'test_configs',
            'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)

    def test_AMWG_setup(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        self.config['global']['project_path'] = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete'
        self.config['global']['exeriment'] = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        start_year = 1
        end_year = 10
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string])
        self.config['global']['output_path'] = os.path.join(
            self.config['global']['project_path'],
            'output')
        self.config['global']['run_scripts_path'] = os.path.join(
            self.config['global']['project_path'],
            'output',
            'run_scripts')
        self.config['global']['resource_path'] = os.path.join(
            os.getcwd(),
            'resources')
        regrid_path = os.path.join(
            self.config['global']['output_path'],
            'pp',
            'fv129x256',
            'climo',
            '10yr')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'amwg_diag',
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'amwg_template.csh')
        diag_home = self.config['amwg']['diag_home']
        temp_path = os.path.join(
            output_path,
            'tmp',
            'amwg',
            set_string)
        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.config.get('global').get('experiment'),
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'output_path': self.config['global']['output_path'],
            'test_casename': self.config.get('global').get('experiment'),
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': 1,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg = AMWGDiagnostic(
            config=config,
            event_list=EventList())
        self.assertEqual(amwg.status.name, 'VALID')
        self.assertFalse(amwg.postvalidate())

    def test_AMWG_no_file_list(self):
        """
        Test that when given a directory with no files, the job
        marks itself as INVALID, then when asked to run sets its
        status to FAILED and exits
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        self.config['global']['project_path'] = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_not_complete'
        self.config['global']['exeriment'] = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        start_year = 1
        end_year = 10
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string])
        self.config['global']['output_path'] = os.path.join(
            self.config['global']['project_path'],
            'output')
        self.config['global']['run_scripts_path'] = os.path.join(
            self.config['global']['project_path'],
            'output',
            'run_scripts')
        self.config['global']['resource_path'] = os.path.join(
            os.getcwd(),
            'resources')
        regrid_path = os.path.join(
            self.config['global']['output_path'],
            'pp',
            'fv129x256',
            'climo',
            '10yr')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'diags',
            self.config['global']['remap_grid_name'],
            self.config['amwg']['host_directory'],
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'amwg_template.csh')
        diag_home = self.config['amwg']['diag_home']
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'amwg',
            set_string)
        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.config.get('global').get('experiment'),
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'output_path': self.config['global']['output_path'],
            'test_casename': self.config.get('global').get('experiment'),
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': 1,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg = AMWGDiagnostic(
            config=config,
            event_list=EventList())
        self.assertEqual(amwg.status.name, 'INVALID')
        amwg.execute(dryrun=False)
        self.assertEqual(amwg.status.name, 'FAILED')
        self.assertFalse(amwg.postvalidate())

    def test_AMWG_execute_not_completed(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        start_year = 1
        end_year = 10
        self.config['global']['project_path'] = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete'
        self.config['global']['exeriment'] = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string])
        self.config['global']['output_path'] = os.path.join(
            self.config['global']['project_path'],
            'output')
        self.config['global']['run_scripts_path'] = os.path.join(
            self.config['global']['project_path'],
            'output',
            'run_scripts')
        self.config['global']['resource_path'] = os.path.join(
            os.getcwd(),
            'resources')
        regrid_path = os.path.join(
            self.config['global']['output_path'],
            'pp',
            'fv129x256',
            'climo',
            '10yr')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'diags',
            self.config['global']['remap_grid_name'],
            self.config['amwg']['host_directory'],
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'amwg_template.csh')
        diag_home = self.config['amwg']['diag_home']
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'amwg',
            set_string)
        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.config.get('global').get('experiment'),
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'output_path': self.config['global']['output_path'],
            'test_casename': self.config.get('global').get('experiment'),
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': 1,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg = AMWGDiagnostic(
            config=config,
            event_list=EventList())
        self.assertEqual(amwg.status.name, 'VALID')
        self.assertFalse(amwg.postvalidate())
    
    def test_AMWG_execute_missing_output_path(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        start_year = 10
        end_year = 100
        self.config['global']['project_path'] = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_not_complete'
        self.config['global']['exeriment'] = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string])
        self.config['global']['output_path'] = os.path.join(
            self.config['global']['project_path'],
            'output')
        self.config['global']['run_scripts_path'] = os.path.join(
            self.config['global']['project_path'],
            'output',
            'run_scripts')
        self.config['global']['resource_path'] = os.path.join(
            os.getcwd(),
            'resources')
        regrid_path = os.path.join(
            self.config['global']['output_path'],
            'pp',
            'fv129x256',
            'climo',
            '10yr')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'diags',
            self.config['global']['remap_grid_name'],
            self.config['amwg']['host_directory'],
            set_string)
        img_output_path = os.path.join(
            '/p/cscratch/acme/baldwin32/20171016/output/amwg_diag/',
            '{start:04d}-{end:04d}{experiment}-obs'.format(
                start=start_year,
                end=end_year,
                experiment=self.config['global']['experiment']))
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        if os.path.exists(img_output_path):
            shutil.rmtree(img_output_path)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'amwg_template.csh')
        diag_home = self.config['amwg']['diag_home']
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'amwg',
            set_string)
        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.config.get('global').get('experiment'),
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'output_path': self.config['global']['output_path'],
            'test_casename': self.config.get('global').get('experiment'),
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': 1,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg = AMWGDiagnostic(
            config=config,
            event_list=EventList())
        self.assertEqual(amwg.status.name, 'INVALID')
        self.assertFalse(amwg.postvalidate())

    def test_AMWG_execute_completed(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        start_year = 1
        end_year = 10
        self.config['global']['project_path'] = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete'
        self.config['global']['exeriment'] = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('amwg').get('host_directory'),
            set_string])
        self.config['global']['output_path'] = os.path.join(
            self.config['global']['project_path'],
            'output')
        self.config['global']['run_scripts_path'] = os.path.join(
            self.config['global']['project_path'],
            'output',
            'run_scripts')
        self.config['global']['resource_path'] = os.path.join(
            os.getcwd(),
            'resources')
        regrid_path = os.path.join(
            self.config['global']['output_path'],
            'pp',
            'fv129x256',
            'climo',
            '10yr')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'amwg_diag',
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'amwg_template.csh')
        diag_home = self.config['amwg']['diag_home']
        temp_path = os.path.join(
            output_path,
            'tmp',
            'amwg',
            set_string)
        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.config.get('global').get('experiment'),
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'output_path': self.config['global']['output_path'],
            'test_casename': self.config.get('global').get('experiment'),
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': 1,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg = AMWGDiagnostic(
            config=config,
            event_list=EventList())
        self.assertEqual(amwg.status.name, 'VALID')
        amwg.execute(dryrun=True)
        self.assertEqual(amwg.status.name, 'COMPLETED')
        # self.assertTrue(amwg.postvalidate())


if __name__ == '__main__':
    unittest.main()
