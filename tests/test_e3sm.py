import unittest
import os

from configobj import ConfigObj

from jobs.E3SMDiags import E3SMDiags
from lib.events import EventList


class TestE3SM(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestE3SM, self).__init__(*args, **kwargs)
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)
        self.caseID = self.config['global']['experiment']
        self.event_list = EventList()

    def test_prevalidate_seasons_as_str(self):
        start_year = 51
        end_year = 55
        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)
        web_directory = os.path.join(
            self.config.get('global').get('host_directory'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            year_set_string)
        host_url = '/'.join([
            self.config.get('global').get('img_host_server'),
            os.environ['USER'],
            self.config.get('global').get('experiment'),
            self.config.get('e3sm_diags').get('host_directory'),
            year_set_string])
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
            'climo_regrid')
        output_path = os.path.join(
            self.config['global']['output_path'],
            'e3sm_diags',
            year_set_string)
        template_path = os.path.join(
            self.config['global']['resource_path'],
            'e3sm_diags_template.py')
        temp_path = os.path.join(
            output_path,
            'tmp',
            'e3sm_diags',
            year_set_string)
        seasons = 'ANN'
        backend = 'mpl'
        sets = '5'
        config = {
            'regrid_base_path': regrid_path,
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'regrided_climo_path': temp_path,
            'reference_data_path': self.config['e3sm_diags']['reference_data_path'],
            'test_name': self.caseID,
            'seasons': seasons,
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
        self.assertTrue(isinstance(e3sm_diag.config['seasons'], list))


if __name__ == '__main__':
    unittest.main()
