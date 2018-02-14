import os, sys
import threading
import unittest
import shutil
import inspect

from configobj import ConfigObj

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.runmanager import RunManager
from lib.filemanager import FileManager
from lib.events import EventList


class TestRunManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestRunManager, self).__init__(*args, **kwargs)
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        self.config = ConfigObj(config_path)
        self.project_path = os.path.join(os.getcwd(), '..', 'testproject')
        self.output_path = os.path.join(self.project_path, 'output')
        self.input_path = os.path.join(self.project_path, 'input')
        self.run_scripts_path = os.path.join(
            self.project_path, 'output', 'run_scripts')
        self.mutex = threading.Lock()
        self.remote_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        self.remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        self.local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        self.config['global']['output_path'] = self.output_path
        self.config['global']['input_path'] = self.input_path
        self.config['global']['run_scripts_path'] = self.run_scripts_path
        self.config['global']['resource_dir'] = os.path.abspath('./resources')

    def test_runmanager_setup(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        db_path = os.path.join(self.project_path, 'test.db')
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(self.project_path):
            shutil.rmtree(self.project_path)
        os.makedirs(self.project_path)
        filemanager = FileManager(
            ui=False,
            event_list=EventList(),
            database=db_path,
            types=['atm'],
            sta=False,
            mutex=self.mutex,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.project_path)
        runmanager = RunManager(
            short_name='testname',
            account='',
            resource_path='',
            ui=False,
            event_list=EventList(),
            output_path=self.output_path,
            caseID=self.config['global']['experiment'],
            scripts_path=self.run_scripts_path,
            thread_list=[],
            event=threading.Event())
        runmanager.setup_job_sets(
            set_frequency=[5, 10],
            sim_start_year=int(self.config['global']['simulation_start_year']),
            sim_end_year=int(self.config['global']['simulation_end_year']),
            config=self.config,
            filemanager=filemanager)

        self.assertEqual(len(runmanager.job_sets), 3)
        for job_set in runmanager.job_sets:
            if job_set.set_number == 1:
                self.assertEqual(job_set.length, 5)
                self.assertEqual(job_set.set_start_year, 51)
                self.assertEqual(job_set.set_end_year, 55)
                job_names = job_set.get_job_names()
                self.assertTrue('ncclimo' in job_names)
                self.assertTrue('amwg' in job_names)
            if job_set.set_number == 2:
                self.assertEqual(job_set.set_start_year, 56)
                self.assertEqual(job_set.set_end_year, 60)
                self.assertEqual(job_set.length, 5)
                job_names = job_set.get_job_names()
                self.assertTrue('ncclimo' in job_names)
                self.assertTrue('amwg' in job_names)
            if job_set.set_number == 3:
                self.assertEqual(job_set.set_start_year, 51)
                self.assertEqual(job_set.set_end_year, 60)
                self.assertEqual(job_set.length, 10)
                job_names = job_set.get_job_names()
                self.assertTrue('ncclimo' in job_names)
                self.assertTrue('amwg' in job_names)
                self.assertTrue('e3sm_diags' in job_names)

    def test_runmanager_write(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        db_path = os.path.join(self.project_path, 'test.db')
        if os.path.exists(db_path):
            os.remove(db_path)
        # if os.path.exists(self.project_path):
        #     shutil.rmtree(self.project_path)
        # os.makedirs(self.project_path)
        filemanager = FileManager(
            ui=False,
            event_list=EventList(),
            database=db_path,
            types=['atm'],
            sta=False,
            mutex=self.mutex,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.project_path)
        runmanager = RunManager(
            short_name='testname',
            account='',
            resource_path='',
            ui=False,
            event_list=EventList(),
            output_path=self.output_path,
            caseID=self.config['global']['experiment'],
            scripts_path=self.run_scripts_path,
            thread_list=[],
            event=threading.Event())
        runmanager.setup_job_sets(
            set_frequency=[5, 10],
            sim_start_year=int(self.config['global']['simulation_start_year']),
            sim_end_year=int(self.config['global']['simulation_end_year']),
            config=self.config,
            filemanager=filemanager)
        path = os.path.join(self.project_path, 'output', 'job_state.txt')
        runmanager.write_job_sets(path)
        self.assertTrue(os.path.exists(path))


if __name__ == '__main__':
    unittest.main()
