import unittest
import os, sys
import inspect

from configobj import ConfigObj

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from jobs.E3SMDiags import E3SMDiags
from lib.events import EventList
from jobs.JobStatus import JobStatus
from lib.util import print_message


class TestE3SM(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestE3SM, self).__init__(*args, **kwargs)
        config_path = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete/input/run.cfg'
        self.config = ConfigObj(config_path)
        self.caseID = self.config['global']['experiment']
        self.event_list = EventList()

    def test_prevalidate(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        start_year = 1
        end_year = 10
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            set_string])
        
        # setup the config
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
            self.config['e3sm_diags']['host_directory'],
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'e3sm_diags_template.py')
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'e3sm_diags',
            set_string)
        # seasons = 'ANN'
        backend = 'mpl'
        sets = '5'
        config = {
            'short_name': 'e3sm_diags_test_prevalidate',
            'regrid_base_path': regrid_path,
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'regrided_climo_path': temp_path,
            'reference_data_path': self.config['e3sm_diags']['reference_data_path'],
            'test_name': self.caseID,
            # 'seasons': seasons,
            'backend': backend,
            'sets': sets,
            'results_dir': output_path,
            'template_path': template_path,
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'end_year': end_year,
            'start_year': start_year,
            'year_set': 1,
            'output_path': output_path
        }
        e3sm_diag = E3SMDiags(
            config=config,
            event_list=self.event_list)
        self.assertEqual(e3sm_diag.status.name, 'VALID')
        e3sm_diag.execute(dryrun=True)
        self.assertEqual(e3sm_diag.status.name, 'COMPLETED')
        self.assertTrue(e3sm_diag.postvalidate())
    
    def test_prevalidate_missing_output_path(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        start_year = 10
        end_year = 20
        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            set_string])
        
        # setup the config
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
            self.config['e3sm_diags']['host_directory'],
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'e3sm_diags_template.py')
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'e3sm_diags',
            set_string)
        # seasons = 'ANN'
        backend = 'mpl'
        sets = '5'
        config = {
            'short_name': 'e3sm_diags_test_prevalidate',
            'regrid_base_path': regrid_path,
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'regrided_climo_path': temp_path,
            'reference_data_path': self.config['e3sm_diags']['reference_data_path'],
            'test_name': self.caseID,
            # 'seasons': seasons,
            'backend': backend,
            'sets': sets,
            'results_dir': output_path,
            'template_path': template_path,
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'end_year': end_year,
            'start_year': start_year,
            'year_set': 1,
            'output_path': output_path
        }
        e3sm_diag = E3SMDiags(
            config=config,
            event_list=self.event_list)
        self.assertEqual(e3sm_diag.status, JobStatus.INVALID)
    
    def test_completed(self):
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
            self.config.get('e3sm_diags').get('host_directory'),
            set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            set_string])
        
        # setup the config
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
            self.config['e3sm_diags']['host_directory'],
            set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'e3sm_diags_template.py')
        temp_path = os.path.join(
            self.config['global']['output_path'],
            'tmp',
            'e3sm_diags',
            set_string)
        # seasons = 'ANN'
        backend = 'mpl'
        sets = '5'
        config = {
            'regrid_output_path': regrid_path,
            'short_name': 'e3sm_diags_test_completed',
            'regrid_base_path': regrid_path,
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'regrided_climo_path': temp_path,
            'reference_data_path': self.config['e3sm_diags']['reference_data_path'],
            'test_name': self.caseID,
            'backend': backend,
            'sets': sets,
            'results_dir': output_path,
            'template_path': template_path,
            'run_scripts_path': self.config['global']['run_scripts_path'],
            'end_year': end_year,
            'start_year': start_year,
            'year_set': 1,
            'output_path': output_path
        }
        e3sm_diag = E3SMDiags(
            config=config,
            event_list=self.event_list)
        self.assertEqual(e3sm_diag.status, JobStatus.VALID)
        e3sm_diag.execute(dryrun=True)
        self.assertEqual(e3sm_diag.status.name, 'COMPLETED')
        self.assertTrue(e3sm_diag.postvalidate())


if __name__ == '__main__':
    unittest.main()
