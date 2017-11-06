import os
import unittest

from Timeseries import Timeseries
from configobj import ConfigObj
from JobStatus import JobStatus

from lib.events import Event_list


class TestTimeseries(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTimeseries, self).__init__(*args, **kwargs)
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)
        self.project_path = os.path.join(os.getcwd(), '..', 'testproject')

    def test_timeseries_setup(self):
        timeseries = Timeseries({
            'year_set': 1,
            'annual_mode': 'sdd',
            'start_year': 50,
            'end_year': 55,
            'output_directory': os.path.join(self.project_path, 'output', 'monthly'),
            'var_list': self.config['ncclimo']['var_list'],
            'caseId': self.config['global']['experiment'],
            'run_scripts_path': os.path.join(self.project_path, 'output', 'run_scripts'),
            'regrid_map_path': self.config['ncclimo']['regrid_map_path'],
            'file_list': [],
        }, Event_list())
        self.assertEqual(timeseries.status, JobStatus.VALID)


if __name__ == '__main__':
    unittest.main()
