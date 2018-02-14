import os, sys
import unittest
import inspect
from configobj import ConfigObj

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from jobs.Ncclimo import Climo as Ncclimo
from jobs.JobStatus import JobStatus
from lib.events import EventList


class TestNcclimo(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestNcclimo, self).__init__(*args, **kwargs)
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)
        self.project_path = os.path.join(os.getcwd(), '..', 'testproject')

    def test_ncclimo_setup(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config = {
            'account': '',
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'ne30', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'fv129x256', 'climo', '5yr'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)

    def test_ncclimo_valid_prevalidate(self):
        """
        Test that valid input config will be marked as valid by the job
        """
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config = {
            'account': '',
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'ne30', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'fv129x256', 'climo', '5yr'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        ncclimo.status = JobStatus.VALID
        self.assertFalse(ncclimo.prevalidate(config))

    def test_ncclimo_missing_input(self):
        """
        Test that a missing input item will invalidate the job
        """
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        config = {
            'account': '',
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            #'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'ne30', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'fv129x256', 'climo', '5yr'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status.name, 'INVALID')

    def test_ncclimo_execute_not_completed(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        start_year = 51
        end_year = 55
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
            'account': '',
            'year_set': 1,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.config['global']['project_path'], 'input', 'atm'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'ne30', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'fv129x256', 'climo', '5yr'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.config['global']['project_path'], 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)
        self.assertFalse(ncclimo.execute(dryrun=True))
        self.assertEqual(ncclimo.status.name, 'COMPLETED')

    def test_ncclimo_execute_completed(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        start_year = 1
        end_year = 5
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/test_2018-1-31'
        self.config['global']['exeriment'] = '20171122.beta3rc10_1850.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
            'account': '',
            'year_set': 1,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.config['global']['project_path'], 'input', 'atm'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'ne30', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'pp', 'fv129x256', 'climo', '5yr'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.config['global']['project_path'], 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)
        self.assertTrue(ncclimo.postvalidate())

    def test_ncclimo_execute_bad_year(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        start_year = 55
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
            'account': '',
            'year_set': 1,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.config['global']['project_path'], 'input', 'atm'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'climo_regrid'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.config['global']['project_path'], 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)
        self.assertFalse(ncclimo.postvalidate())

    def test_ncclimo_execute_bad_regrid_dir(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        start_year = 56
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
            'account': '',
            'year_set': 1,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.config['global']['project_path'], 'input', 'atm'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'climo', '5yr'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'XXYYZZ'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.config['global']['project_path'], 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)
        self.assertFalse(ncclimo.postvalidate())

    def test_ncclimo_execute_bad_climo_dir(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        start_year = 56
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
            'account': '',
            'year_set': 1,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.config['global']['project_path'], 'input', 'atm'),
            'climo_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'climo', 'XXYYZZ'),
            'regrid_output_directory': os.path.join(self.config['global']['project_path'], 'output', 'climo_regrid'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.config['global']['project_path'], 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)
        self.assertFalse(ncclimo.postvalidate())


if __name__ == '__main__':
    unittest.main()
