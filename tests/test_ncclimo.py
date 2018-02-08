import os
import unittest

from configobj import ConfigObj

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
        config = {
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.project_path, 'output', 'climo'),
            'regrid_output_directory': os.path.join(self.project_path, 'output', 'regrid_climo'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status, JobStatus.VALID)

    def test_ncclimo_valid_prevalidate(self):
        config = {
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.project_path, 'output', 'climo'),
            'regrid_output_directory': os.path.join(self.project_path, 'output', 'regrid_climo'),
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
        config = {
            'year_set': 1,
            'start_year': 50,
            'end_year': 55,
            'caseId': self.config['global']['experiment'],
            'annual_mode': 'sdd',
            'input_directory': os.path.join(self.project_path, 'input'),
            'climo_output_directory': os.path.join(self.project_path, 'output', 'climo'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'year_set': 1,
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts')
        }
        ncclimo = Ncclimo(
            config=config,
            event_list=EventList())
        self.assertEqual(ncclimo.status.name, 'INVALID')

    def test_ncclimo_execute_not_completed(self):
        start_year = 51
        end_year = 55
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
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
        self.assertFalse(ncclimo.execute(dryrun=True))
        self.assertEqual(ncclimo.status.name, 'COMPLETED')

    def test_ncclimo_execute_completed(self):
        start_year = 56
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
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
        self.assertTrue(ncclimo.postvalidate())

    def test_ncclimo_execute_bad_year(self):
        start_year = 55
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
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
        start_year = 56
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
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
        start_year = 56
        end_year = 60
        self.config['global']['project_path'] = '/p/cscratch/acme/baldwin32/20171016/'
        self.config['global']['exeriment'] = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        config = {
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
