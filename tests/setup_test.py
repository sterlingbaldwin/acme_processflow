import os
import unittest
import threading
from lib.setup import setup
from lib.events import Event_list
from peewee import *
from lib.models import DataFile
from lib.filemanager import FileManager

class TestSetup(unittest.TestCase):
    """
    A test class for validating the project setup
    
    These tests should be run from the main project directory
    """
    def test_expected_config(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests', 'test_run_no_sta.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = Event_list()
        thread_list = []
        config, filemanager, runmanager = setup(
            args,
            display_event,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)

        expected_config = {
            'global': {
                'project_path': project_path, 
                'source_path': '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/', 
                'simulation_start_year': 51, 
                'simulation_end_year': 60, 
                'set_frequency': [5, 10], 
                'experiment': '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison', 
                'email': 'baldwin32@llnl.gov', 
                'short_term_archive': 0, 
                'img_host_server': 'https://acme-viewer.llnl.gov', 
                'host_directory': '/var/www/acme/acme-diags/', 
                'file_types': ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice'], 
                'resource_dir': resource_path, 
                'input_path': os.path.join(project_path, 'input'), 
                'output_path': os.path.join(project_path, 'output'), 
                'log_path': os.path.join(project_path, 'output', 'workflow.log'), 
                'run_scripts_path': os.path.join(project_path, 'output', 'run_scripts'), 
                'tmp_path': os.path.join(project_path, 'output', 'tmp'), 
                'ui': False, 
                'no_cleanup': False, 
                'no_monitor': False, 
                'print_file_list': True, 
                'set_jobs': {
                    'ncclimo': ['5', '10'], 
                    'timeseries': '20', 
                    'amwg': ['5', '10'], 
                    'aprime_diags': '', 
                    'e3sm_diags': '10'}}, 
            'e3sm_diags': {
                'host_directory': 'e3sm-diags', 
                'backend': 'mpl', 
                'seasons': ['DJF', 'MAM', 'JJA', 'SON', 'ANN'], 
                'reference_data_path': '/p/cscratch/acme/data/obs_for_acme_diags', 
                'sets': ['3', '4', '5', '7', '13']}, 
            'transfer': {
                'destination_endpoint': 'a871c6de-2acd-11e7-bc7c-22000b9a448b', 
                'source_endpoint': '9d6d994a-6d04-11e5-ba46-22000b92c6ec'}, 
            'amwg': {
                'diag_home': '/p/cscratch/acme/amwg/amwg_diag', 
                'host_directory': 'amwg'},
            'ncclimo': {
                'regrid_map_path': '/p/cscratch/acme/data/map_ne30np4_to_fv129x256_aave.20150901.nc', 
                'var_list': ['FSNTOA', 'FLUT', 'FSNT', 'FLNT', 'FSNS', 'FLNS', 'SHFLX', 'QFLX', 'PRECC', 'PRECL', 'PRECSC', 'PRECSL', 'TS', 'TREFHT']},
            'aprime_diags': {
                'host_directory': 'aprime-diags', 
                'aprime_code_path': '/p/cscratch/acme/data/a-prime',
                'test_atm_res': 'ne30',
                'test_mpas_mesh_name': 'oEC60to30v3'}}

        for key, value in config.items():
            if isinstance(value, dict):
                for k2, v2 in value.items():
                    self.assertEqual(expected_config[key][k2], config[key][k2])
            else:
                self.assertEqual(expected_config[key], config[key])

class TestFileManagerSetup(unittest.TestCase):
    
    def test_filemanager_setup_no_sta(self):
        mutex = threading.Lock()
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        
        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertEqual(tail, 'run')
        os.remove(database)
    
    def test_filemanager_setup_with_sta(self):
        mutex = threading.Lock()
        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        
        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertNotEqual(tail, 'run')
        os.remove(database)
    
    def test_filemanager_populate(self):
        mutex = threading.Lock()
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        
        simlength = simend - simstart + 1
        atm_file_names = [x.name for x in DataFile.select().where(DataFile.datatype == 'atm')] 
        self.assertTrue(len(atm_file_names) == (simlength * 12))

        for year in range(simstart, simend +1):
            for month in range(1, 13):
                name = '{exp}.cam.h0.{year:04d}-{month:02d}.nc'.format(
                    exp=experiment,
                    year=year,
                    month=month)
                self.assertTrue(name in atm_file_names)
if __name__ == '__main__':
    unittest.main()