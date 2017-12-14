import os
import shutil
import unittest
import threading
from util import transfer_directory
from util import path_exists
from util import cmd_exists
from util import render
from events import EventList


class TestFileManager(unittest.TestCase):

    def test_transfer_directory(self):
        project_path = os.path.abspath(
            os.path.join('..', 'testproject_transfer'))
        transfer_directory(
            source_endpoint='9d6d994a-6d04-11e5-ba46-22000b92c6ec',
            destination_endpoint='a871c6de-2acd-11e7-bc7c-22000b9a448b',
            src_path='/global/homes/s/sbaldwin/test_directory',
            dst_path=project_path,
            event_list=EventList(),
            event=threading.Event())

        self.assertTrue(os.path.exists(project_path))
        contents = os.listdir(project_path)
        for item in ['a', 'b', 'c', 'd']:
            self.assertTrue(item in contents)
        shutil.rmtree(project_path)

    def test_path_exists_valid(self):
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        config = {
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
                'resource_dir': '',
                'input_path': os.path.join(project_path, 'input'),
                'output_path': os.path.join(project_path, 'output'),
                'log_path': os.path.join(project_path, 'output', 'workflow.log'),
                'run_scripts_path': os.path.join(project_path, 'output', 'run_scripts'),
                'tmp_path': os.path.join(project_path, 'output', 'tmp'),
                'error_path': os.path.join(project_path, 'output', 'workflow.error'),
                'ui': True,
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
        status = path_exists(config)
        self.assertTrue(status)

    def test_path_exists_invalid(self):
        project_path = os.path.abspath(os.path.join('..', 'testproject'))
        config = {
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
                'resource_dir': '',
                'input_path': os.path.join(project_path, 'input'),
                'output_path': os.path.join(project_path, 'output'),
                'log_path': os.path.join(project_path, 'output', 'workflow.log'),
                'run_scripts_path': os.path.join(project_path, 'output', 'run_scripts'),
                'tmp_path': os.path.join(project_path, 'output', 'tmp'),
                'error_path': os.path.join(project_path, 'output', 'workflow.error'),
                'ui': True,
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
                'regrid_map_path': '/NOT/A/FILE.nc',
                'var_list': ['FSNTOA', 'FLUT', 'FSNT', 'FLNT', 'FSNS', 'FLNS', 'SHFLX', 'QFLX', 'PRECC', 'PRECL', 'PRECSC', 'PRECSL', 'TS', 'TREFHT']},
            'aprime_diags': {
                'host_directory': 'aprime-diags',
                'aprime_code_path': '/p/cscratch/acme/data/a-prime',
                'test_atm_res': 'ne30',
                'test_mpas_mesh_name': 'oEC60to30v3'}}
        status = path_exists(config)
        self.assertFalse(status)

    def test_cmd_exists_valid(self):
        self.assertTrue(cmd_exists('ncclimo'))

    def test_cmd_exists_invalid(self):
        self.assertFalse(cmd_exists('not_a_cmd'))

    def test_render(self):
        render_target = os.path.join(
            os.getcwd(), 'tests', 'test_render_target.txt')
        render_reference = os.path.join(
            os.getcwd(), 'tests', 'test_render_reference.txt')
        render_output = os.path.join(os.getcwd(), 'tests', 'render_output.txt')
        reference = ''
        with open(render_reference, 'r') as fp:
            for line in fp.readlines():
                reference += line

        vals = {
            'a': 'a',
            'b': 'b',
            'd': 'd',
            'e': 'e'
        }
        self.assertTrue(render(vals, render_target, render_output))
        with open(render_output, 'r') as fp:
            self.assertTrue(fp.readline() in reference)

    def test_render_bad_input_file(self):
        render_target = os.path.join(os.getcwd(), 'tests', 'DOES_NOT_EXIST')
        render_output = os.path.join(os.getcwd(), 'tests', 'render_output.txt')
        self.assertFalse(render({}, render_target, render_output))

    def test_render_bad_outout_file(self):
        render_target = os.path.join(
            os.getcwd(), 'tests', 'test_render_target.txt')
        render_output = '/usr/local/NO_PERMISSIONS'
        self.assertFalse(render({}, render_target, render_output))


if __name__ == '__main__':
    unittest.main()
