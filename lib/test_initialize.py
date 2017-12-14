import os
import unittest
import threading
from initialize import initialize
from events import EventList
from peewee import *
from models import DataFile
from YearSet import YearSet
from shutil import rmtree

from filemanager import FileManager
from runmanager import RunManager


class TestInitialize(unittest.TestCase):
    """
    A test class for validating the project setup

    These tests should be run from the main project directory
    """

    def test_expected_config(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_no_sta.cfg'), '-f', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        if not os.environ.get('JENKINS_URL'):
            config['global']['project_path'] = project_path
            config['global']['resource_dir'] = resource_path
            config['global']['input_path'] = os.path.join(
                project_path, 'input')
            config['global']['output_path'] = os.path.join(
                project_path, 'output')
            config['global']['log_path'] = os.path.join(
                project_path, 'output', 'workflow.log')
            config['global']['run_scripts_path'] = os.path.join(
                project_path, 'output', 'run_scripts')
            config['global']['tmp_path'] = os.path.join(
                project_path, 'output', 'tmp')
            config['global']['error_path'] = os.path.join(
                project_path, 'output', 'workflow.error')

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
                'error_path': os.path.join(project_path, 'output', 'workflow.error'),
                'ui': False,
                'no_cleanup': False,
                'no_monitor': False,
                'print_file_list': True,
                'set_jobs': {
                    'ncclimo': ['5', '10'],
                    'timeseries': '10',
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
            for k2, v2 in value.items():
                self.assertEqual(expected_config[key][k2], config[key][k2])

    def test_expected_config_no_ui(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_no_sta.cfg'), '-f', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        if not os.environ.get('JENKINS_URL'):
            config['global']['project_path'] = project_path
            config['global']['resource_dir'] = resource_path
            config['global']['input_path'] = os.path.join(
                project_path, 'input')
            config['global']['output_path'] = os.path.join(
                project_path, 'output')
            config['global']['log_path'] = os.path.join(
                project_path, 'output', 'workflow.log')
            config['global']['run_scripts_path'] = os.path.join(
                project_path, 'output', 'run_scripts')
            config['global']['error_path'] = os.path.join(
                project_path, 'output', 'workflow.error')
            config['global']['tmp_path'] = os.path.join(
                project_path, 'output', 'tmp')

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
                'error_path': os.path.join(project_path, 'output', 'workflow.error'),
                'ui': False,
                'no_cleanup': False,
                'no_monitor': False,
                'print_file_list': True,
                'set_jobs': {
                    'ncclimo': ['5', '10'],
                    'timeseries': '10',
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
            for k2, v2 in value.items():
                if expected_config[key][k2] != config[key][k2]:
                    print key, k2, expected_config[key][k2], config[key][k2]
                self.assertEqual(expected_config[key][k2], config[key][k2])

    def test_config_no_white_space(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_no_white_space.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_config_extra_white_space(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_no_sta_whitespace.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)

        self.assertTrue(isinstance(config, dict))
        self.assertEqual(config['global']['source_path'],
                         '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/')

    def test_config_bad_destination_endpoint(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_bad_destination_endpoint.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_config_bad_source_path(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_bad_source_path.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_config_config_doesnt_exist(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'DOES_NOT_EXIST.cfg'), '-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_config_invalid_config(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'invalid.cfg'), '-f', '-n']
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_config_no_config(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-f', '-n', '-r', resource_path]
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_bad_config_key(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_invalid_key.cfg'), '-f', '-n']
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)

    def test_no_global(self):
        base_path = os.getcwd()
        resource_path = os.path.join(base_path, 'resources')
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        args = ['-c', os.path.join(base_path, 'tests',
                                   'test_run_no_global.cfg'), '-f', '-n']
        display_event = threading.Event()
        thread_kill_event = threading.Event()
        mutex = threading.Lock()
        event_list = EventList()
        thread_list = []
        config, filemanager, runmanager = initialize(
            argv=args,
            event_list=event_list,
            thread_list=thread_list,
            kill_event=thread_kill_event,
            mutex=mutex)
        self.assertFalse(config)
        self.assertFalse(filemanager)
        self.assertFalse(runmanager)


if __name__ == '__main__':
    project_path = os.path.abspath(os.path.join('..', 'testproject'))
    if os.path.exists(project_path):
        rmtree(project_path)
    unittest.main()
