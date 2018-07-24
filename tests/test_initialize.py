import os
import sys
import unittest
import threading
import inspect
import shutil

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.initialize import initialize, parse_args
from lib.events import EventList
from lib.models import DataFile
from lib.YearSet import YearSet
from lib.filemanager import FileManager
from lib.runmanager import RunManager
from lib.util import print_message

__version__ = '2.0.0'
__branch__ = 'master'

class TestInitialize(unittest.TestCase):
    """
    A test class for validating the project setup

    These tests should be run from the main project directory
    """

    def test_parse_args_valid(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        argv = ['-c', 'tests/test_configs/valid_config_simple.cfg', 
                '-l', 'pflow.log',
                '-s',
                '-r', 'resources/',
                '-d',
                '-m', '999',
                '--dryrun']
        pargs = parse_args(argv)
        self.assertEqual(pargs.config, 'tests/test_configs/valid_config_simple.cfg')
        self.assertEqual(pargs.resource_path, 'resources/')
        self.assertEqual(pargs.log, 'pflow.log')
        self.assertEqual(pargs.max_jobs, 999)
        self.assertTrue(pargs.scripts)
        self.assertTrue(pargs.debug)
        self.assertTrue(pargs.dryrun)
        self.assertFalse(pargs.always_copy)
        self.assertFalse(pargs.file_list)
    
    def test_parse_args_print_help(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        argv = ['-h']
        with self.assertRaises(SystemExit) as exitexception:
            pargs = parse_args(argv)
        self.assertEqual(exitexception.exception.code, 0)

        argv = []
        pargs = parse_args(argv, print_help=True)
        self.assertEqual(pargs, None)
    
    def test_init_print_version(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        argv = ['-v']
        with self.assertRaises(SystemExit) as exitexception:
            a, b, c = initialize(argv=argv, version=__version__)
        self.assertEqual(exitexception.exception.code, 0)
    
    def test_init_no_config(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        argv = []
        a, b, c = initialize(argv=argv)
        self.assertEqual(a, False)
        self.assertEqual(b, False)
        self.assertEqual(c, False)

    def test_init_valid_config_simple(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/valid_config_simple.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
    
    def test_init_config_doesnt_exist_simple(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/this_file_doesnt_exist.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertEqual(config, False)
        self.assertEqual(filemanager, False)
        self.assertEqual(runmanager, False)
    
    def test_init_config_no_white_space_simple(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/invalid_config_no_white_space.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertEqual(config, False)
        self.assertEqual(filemanager, False)
        self.assertEqual(runmanager, False)
    
    def test_init_cant_parse_config(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/invalid_config_cant_parse.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertEqual(config, False)
        self.assertEqual(filemanager, False)
        self.assertEqual(runmanager, False)
    
    def test_init_missing_lnd(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/invalid_config_missing_lnd.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertEqual(config, False)
        self.assertEqual(filemanager, False)
        self.assertEqual(runmanager, False)
    
    def test_init_from_scratch_config(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/valid_config_from_scratch.cfg',
                 '-m', '1']
        project_path = '/p/user_pub/e3sm/baldwin32/testing/empty/'
        if os.path.exists(project_path):
            shutil.rmtree(project_path)

        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertNotEqual(config, False)
        self.assertNotEqual(filemanager, False)
        self.assertNotEqual(runmanager, False)

        self.assertEqual(os.path.exists(project_path), True)
        if os.path.exists(project_path):
            shutil.rmtree(project_path)
    
    def test_init_from_scratch_config_bad_project_dir(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/valid_config_from_scratch_bad_project_path.cfg']
        project_path = '/usr/testing/'
        with self.assertRaises(SystemExit) as exitexception:
            config, filemanager, runmanager = initialize(
                argv=pargv,
                version=__version__,
                branch=__branch__,
                event_list=EventList(),
                kill_event=threading.Event(),
                mutex=threading.Lock(),
                testing=True)
            self.assertEqual(config, False)
            self.assertEqual(filemanager, False)
            self.assertEqual(runmanager, False)

        self.assertEqual(os.path.exists(project_path), False)
        self.assertEqual(exitexception.exception.code, 1)
    
    def test_init_from_scratch_config_globus(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/valid_config_from_scratch_globus.cfg']
        project_path = '/p/user_pub/e3sm/baldwin32/testing/empty/'
        if os.path.exists(project_path):
            shutil.rmtree(project_path)

        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertNotEqual(config, False)
        self.assertNotEqual(filemanager, False)
        self.assertNotEqual(runmanager, False)

        self.assertEqual(os.path.exists(project_path), True)
        if os.path.exists(project_path):
            shutil.rmtree(project_path)
    
    def test_init_from_scratch_config_globus_bad_uuid(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['-c', 'tests/test_configs/valid_config_from_scratch_globus_bad_uuid.cfg']
        project_path = '/p/user_pub/e3sm/baldwin32/testing/empty/'
        if os.path.exists(project_path):
            shutil.rmtree(project_path)

        config, filemanager, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__,
            event_list=EventList(),
            kill_event=threading.Event(),
            mutex=threading.Lock(),
            testing=True)
        self.assertEqual(config, False)
        self.assertEqual(filemanager, False)
        self.assertEqual(runmanager, False)

        self.assertEqual(os.path.exists(project_path), True)
        if os.path.exists(project_path):
            shutil.rmtree(project_path)



#     def config_compare(self, configA, configB):
#         for key, value in configB.items():
#             for k2, v2 in value.items():
#                 if configA[key].get(k2) is None or configB[key].get(k2) is None or configA[key][k2] != configB[key][k2]:
#                     print key, k2, configA[key].get(k2), configB[key].get(k2)
#                 self.assertEqual(configA[key][k2], configB[key][k2])

#     def test_expected_config(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'test_run_no_sta.cfg'), '-f', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertTrue(isinstance(config, dict))
#         self.assertFalse(isinstance(filemanager, int))
#         self.assertFalse(isinstance(filemanager, bool))
#         self.assertFalse(isinstance(runmanager, int))
#         self.assertFalse(isinstance(runmanager, bool))
#         if not os.environ.get('JENKINS_URL'):
#             config['global']['project_path'] = project_path
#             config['global']['resource_path'] = resource_path
#             config['global']['input_path'] = os.path.join(
#                 project_path, 'input')
#             config['global']['output_path'] = os.path.join(
#                 project_path, 'output')
#             config['global']['log_path'] = os.path.join(
#                 project_path, 'output', 'workflow.log')
#             config['global']['run_scripts_path'] = os.path.join(
#                 project_path, 'output', 'run_scripts')
#             config['global']['tmp_path'] = os.path.join(
#                 project_path, 'output', 'tmp')
#             config['global']['error_path'] = os.path.join(
#                 project_path, 'output', 'workflow.error')

#         expected_config = {
#             'global': {
#                 'url_prefix': '',
#                 'custom_archive': False,
#                 'always_copy': False,
#                 'account': '',
#                 'pp_path':'/p/user_pub/e3sm/baldwin32/E3SM_test_data/testproject/output/pp',
#                 'diags_path': '/p/user_pub/e3sm/baldwin32/E3SM_test_data/testproject/output/diags',
#                 'no_scripts': False,
#                 'short_name': 'beta2_FCT2-icedeep_branch',
#                 'native_grid_cleanup': '0',
#                 'native_grid_name': 'ne30',
#                 'remap_grid_name': 'fv129x256',
#                 'project_path': project_path,
#                 'source_path': '/global/homes/r/renata/ACME_simulations/20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil',
#                 'simulation_start_year': 1,
#                 'simulation_end_year': 5,
#                 'set_frequency': [5],
#                 'experiment': '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil',
#                 'email': 'baldwin32@llnl.gov',
#                 'short_term_archive': 0,
#                 'img_host_server': 'https://acme-viewer.llnl.gov',
#                 'host_directory': '/var/www/acme/acme-diags/',
#                 'file_types': ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport'],
#                 'resource_path': resource_path,
#                 'input_path': os.path.join(project_path, 'input'),
#                 'output_path': os.path.join(project_path, 'output'),
#                 'log_path': os.path.join(project_path, 'output', 'workflow.log'),
#                 'run_scripts_path': os.path.join(project_path, 'output', 'run_scripts'),
#                 'tmp_path': os.path.join(project_path, 'output', 'tmp'),
#                 'error_path': os.path.join(project_path, 'output', 'workflow.error'),
#                 'ui': False,
#                 'no_host': False,
#                 'no_monitor': False,
#                 'print_file_list': True,
#                 'set_jobs': {
#                     'ncclimo': '5', 
#                     'timeseries': '5', 
#                     'aprime_diags': '5', 
#                     'e3sm_diags': '5'
#                 },
#             },
#             'e3sm_diags': {
#                 'host_directory': 'e3sm-diags',
#                 'backend': 'mpl',
#                 # 'seasons': ['DJF', 'MAM', 'JJA', 'SON', 'ANN'],
#                 'reference_data_path': '/p/cscratch/acme/data/obs_for_acme_diags',
#                 'sets': ['3', '4', '5', '7', '13']
#             },
#             'transfer': {
#                 'destination_endpoint': 'a871c6de-2acd-11e7-bc7c-22000b9a448b',
#                 'source_endpoint': '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
#             },
#             'amwg': {
#                 'diag_home': '/p/cscratch/acme/amwg/amwg_diag',
#                 'host_directory': 'amwg'
#             },
#             'ncclimo': {
#                 'regrid_map_path': '/p/cscratch/acme/data/map_ne30np4_to_fv129x256_aave.20150901.nc',
#                 'var_list': ['FSNTOA', 'FLUT', 'FSNT', 'FLNT', 'FSNS', 'FLNS', 'SHFLX', 'QFLX', 'PRECC', 'PRECL', 'PRECSC', 'PRECSL', 'TS', 'TREFHT']
#             },
#             'aprime_diags': {
#                 'host_directory': 'aprime-diags',
#                 'aprime_code_path': '/p/cscratch/acme/data/a-prime',
#                 'test_atm_res': 'ne30',
#                 'test_mpas_mesh_name': 'oEC60to30v3'
#             }
#         }

#         self.config_compare(expected_config, config)

#     def test_expected_config_no_ui(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'test_run_no_sta.cfg'), '-f', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertTrue(isinstance(config, dict))
#         self.assertFalse(isinstance(filemanager, int))
#         self.assertFalse(isinstance(filemanager, bool))
#         self.assertFalse(isinstance(runmanager, int))
#         self.assertFalse(isinstance(runmanager, bool))
#         if not os.environ.get('JENKINS_URL'):
#             config['global']['project_path'] = project_path
#             config['global']['resource_path'] = resource_path
#             config['global']['input_path'] = os.path.join(
#                 project_path, 'input')
#             config['global']['output_path'] = os.path.join(
#                 project_path, 'output')
#             config['global']['log_path'] = os.path.join(
#                 project_path, 'output', 'workflow.log')
#             config['global']['run_scripts_path'] = os.path.join(
#                 project_path, 'output', 'run_scripts')
#             config['global']['error_path'] = os.path.join(
#                 project_path, 'output', 'workflow.error')
#             config['global']['tmp_path'] = os.path.join(
#                 project_path, 'output', 'tmp')

#         expected_config = {
#             'global': {
#                 'url_prefix': '',
#                 'custom_archive': False,
#                 'always_copy': False,
#                 'account': '',
#                 'pp_path': '/p/user_pub/e3sm/baldwin32/E3SM_test_data/testproject/output/pp',
#                 'diags_path': '/p/user_pub/e3sm/baldwin32/E3SM_test_data/testproject/output/diags',
#                 'short_name': 'beta2_FCT2-icedeep_branch',
#                 'native_grid_cleanup': '0',
#                 'no_scripts': False,
#                 'native_grid_name': 'ne30',
#                 'remap_grid_name': 'fv129x256',
#                 'project_path': project_path,
#                 'source_path': '/global/homes/r/renata/ACME_simulations/20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil',
#                 'simulation_start_year': 1,
#                 'simulation_end_year': 5,
#                 'set_frequency': [5],
#                 'experiment': '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil',
#                 'email': 'baldwin32@llnl.gov',
#                 'short_term_archive': 0,
#                 'img_host_server': 'https://acme-viewer.llnl.gov',
#                 'host_directory': '/var/www/acme/acme-diags/',
#                 'file_types': ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport'],
#                 'resource_path': resource_path,
#                 'input_path': os.path.join(project_path, 'input'),
#                 'output_path': os.path.join(project_path, 'output'),
#                 'log_path': os.path.join(project_path, 'output', 'workflow.log'),
#                 'run_scripts_path': os.path.join(project_path, 'output', 'run_scripts'),
#                 'tmp_path': os.path.join(project_path, 'output', 'tmp'),
#                 'error_path': os.path.join(project_path, 'output', 'workflow.error'),
#                 'ui': False,
#                 'no_host': False,
#                 'no_monitor': False,
#                 'print_file_list': True,
#                 'set_jobs': {
#                     'ncclimo': '5', 
#                     'timeseries': '5',
#                     'aprime_diags': '5', 
#                     'e3sm_diags': '5'
#                 },
#             },
#             'e3sm_diags': {
#                 'host_directory': 'e3sm-diags',
#                 'backend': 'mpl',
#                 # 'seasons': ['DJF', 'MAM', 'JJA', 'SON', 'ANN'],
#                 'reference_data_path': '/p/cscratch/acme/data/obs_for_acme_diags',
#                 'sets': ['3', '4', '5', '7', '13']
#             },
#             'transfer': {
#                 'destination_endpoint': 'a871c6de-2acd-11e7-bc7c-22000b9a448b',
#                 'source_endpoint': '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
#             },
#             'amwg': {
#                 'diag_home': '/p/cscratch/acme/amwg/amwg_diag',
#                 'host_directory': 'amwg'
#             },
#             'ncclimo': {
#                 'regrid_map_path': '/p/cscratch/acme/data/map_ne30np4_to_fv129x256_aave.20150901.nc',
#                 'var_list': ['FSNTOA', 'FLUT', 'FSNT', 'FLNT', 'FSNS', 'FLNS', 'SHFLX', 'QFLX', 'PRECC', 'PRECL', 'PRECSC', 'PRECSL', 'TS', 'TREFHT']
#             },
#             'aprime_diags': {
#                 'host_directory': 'aprime-diags',
#                 'aprime_code_path': '/p/cscratch/acme/data/a-prime',
#                 'test_atm_res': 'ne30',
#                 'test_mpas_mesh_name': 'oEC60to30v3'
#             }
#         }

#         self.config_compare(expected_config, config)

#     def test_config_no_white_space(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'test_run_no_white_space.cfg'), '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_config_extra_white_space(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(
#             base_path,
#             'resources')
#         project_path = os.path.abspath(
#             os.path.join(
#                 '..',
#                 'testproject'))
#         config_path = os.path.join(
#             base_path,
#             'tests',
#             'test_configs',
#             'test_run_no_sta_whitespace.cfg')
#         args = ['-c', config_path, '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)

#         self.assertTrue(isinstance(config, dict))
#         self.assertEqual(
#             config['global']['source_path'],
#             '/global/cscratch1/sd/golaz/ACME_simulations/20180215.DECKv1b_1pctCO2.ne30_oEC.edison')

#     def test_config_bad_destination_endpoint(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'test_run_bad_destination_endpoint.cfg'), '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_config_bad_source_path(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'test_run_bad_source_path.cfg'), '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_config_config_doesnt_exist(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-c', os.path.join(base_path, 'tests', 'test_configs',
#                                    'DOES_NOT_EXIST.cfg'), '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_config_invalid_config(self):
#         """
#         run initialize with a badly formatted config
#         """
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         config_path = os.path.join(
#             base_path,
#             'tests',
#             'test_configs',
#             'invalid.cfg')
#         args = ['-c', config_path, '-f', '-n']
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_config_no_config(self):
#         """
#         test that initialize correctly exists if not passed a config file
#         """
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         args = ['-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         caught_exception = False
#         config = None
#         filemanager = None
#         runmanager = None
#         try:
#             config, filemanager, runmanager = initialize(
#                 argv=args,
#                 version=__version__,
#                 branch=__branch__,
#                 event_list=event_list,
#                 thread_list=thread_list,
#                 kill_event=thread_kill_event,
#                 mutex=mutex,
#                 testing=True)
#         except:
#             caught_exception = True
#         self.assertTrue(caught_exception)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_bad_config_key(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         config_path = os.path.join(
#             base_path,
#             'tests',
#             'test_configs',
#             'test_run_invalid_key.cfg')
#         args = ['-c', config_path, '-f', '-n', '-r', resource_path]
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)

#     def test_no_global(self):
#         print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

#         base_path = os.getcwd()
#         resource_path = os.path.join(base_path, 'resources')
#         project_path = os.path.abspath(os.path.join('..', 'testproject'))
#         config_path = os.path.join(
#             base_path,
#             'tests',
#             'test_configs',
#             'test_run_no_global.cfg')
#         args = ['-c', config_path, '-f', '-n']
#         display_event = threading.Event()
#         thread_kill_event = threading.Event()
#         mutex = threading.Lock()
#         event_list = EventList()
#         thread_list = []
#         config, filemanager, runmanager = initialize(
#             argv=args,
#             version=__version__,
#             branch=__branch__,
#             event_list=event_list,
#             thread_list=thread_list,
#             kill_event=thread_kill_event,
#             mutex=mutex,
#             testing=True)
#         self.assertFalse(config)
#         self.assertFalse(filemanager)
#         self.assertFalse(runmanager)


if __name__ == '__main__':
    unittest.main()
