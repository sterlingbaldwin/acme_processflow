import os
import unittest

from Ncclimo import Climo as Ncclimo
from configobj import ConfigObj
from JobStatus import JobStatus

from lib.events import Event_list


class TestNcclimo(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestNcclimo, self).__init__(*args, **kwargs)
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)
        self.project_path = os.path.join(os.getcwd(), '..', 'testproject')

    def test_ncclimo_setup(self):
        ncclimo = Ncclimo({
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
        }, Event_list())
        self.assertEqual(ncclimo.status, JobStatus.VALID)

if __name__ == '__main__':
    unittest.main()
