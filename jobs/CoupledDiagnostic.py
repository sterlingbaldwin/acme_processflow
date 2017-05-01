import os
import sys
import logging
import time
import re

from uuid import uuid4
from pprint import pformat
from subprocess import Popen, PIPE
from time import sleep

from cdp.cdp_viewer import OutputViewer

from lib.util import render
from lib.util import print_message
from lib.util import print_debug
from lib.util import check_slurm_job_submission
from lib.util import cmd_exists
from lib.util import create_symlink_dir
from lib.util import push_event
from JobStatus import JobStatus


class CoupledDiagnostic(object):
    def __init__(self, config, event_list):
        """
        Setup class attributes
        """
        self.event_list = event_list
        self.inputs = {
            'mpas_regions_file': '',
            'web_dir': '',
            'mpas_am_dir': '',
            'rpt_dir': '',
            'mpas_cice_dir': '',
            'mpas_o_dir': '',
            'streams_dir': '',
            'host_prefix': '',
            'host_directory': '',
            'run_id': '',
            'year_set': '',
            'climo_temp_dir': '',
            'coupled_project_dir': '',
            'test_casename': '',
            'test_native_res': '',
            'test_archive_dir': '',
            'test_begin_yr_climo': '',
            'test_end_yr_climo': '',
            'test_begin_yr_ts': '',
            'test_end_yr_ts': '',
            'ref_case': '',
            'ref_archive_dir': '',
            'mpas_meshfile': '',
            'mpas_remapfile': '',
            'pop_remapfile': '',
            'remap_files_dir': '',
            'GPCP_regrid_wgt_file': '',
            'CERES_EBAF_regrid_wgt_file': '',
            'ERS_regrid_wgt_file': '',
            'coupled_home_directory': '',
            'coupled_template_path': '',
            'rendered_output_path': '',
            'obs_ocndir': '',
            'obs_seaicedir': '',
            'obs_sstdir': '',
            'dataset_name': '',
            'output_base_dir': '',
            'run_scripts_path': ''
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
        }
        self.var_list = [
            'PRECT',
            'RESTOM',
            'FLNT',
            'FSNT',
            'FSNTOA',
            'FLUT',
            'SWCF',
            'LWCF',
            'TAU'
        ]
        self.config = {}
        self.status = JobStatus.INVALID
        self.type = 'coupled_diagnostic'
        self.outputs = {}
        self.year_set = 0
        self.uuid = uuid4().hex
        self.job_id = 0
        self.depends_on = []
        self.prevalidate(config)

    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id
        }, indent=4)

    def get_type(self):
        return self.type

    def set_status(self, status):
        self.status = status

    def prevalidate(self, config):
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        self.config = config
        self.depends_on = config.get('depends_on')
        self.year_set = config.get('year_set')
        self.status = JobStatus.VALID

        web_dir = outter_dir = os.path.join(
            self.config.get('host_directory'),
            self.config.get('run_id'),
            'year_set_' + str(self.config.get('year_set')))
        self.config['web_dir'] = web_dir

        if not os.path.exists(self.config.get('run_scripts_path')):
            os.makedirs(self.config.get('run_scripts_path'))


    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        returns 1 if the job is done, 0 otherwise
        """
        # find the directory generated by coupled diags
        output_path = self.config.get('output_base_dir')
        if not os.path.exists(output_path):
            return False
        try:
            output_contents = os.listdir(output_path)
        except IOError:
            return False
        if not output_contents:
            return False
        output_directory = None
        for item in output_contents:
            if item.split('-')[-1] == 'obs':
                output_directory = item
        if not output_directory:
            return False
        output_directory = os.path.join(output_path, output_directory)
        if os.path.exists(output_directory):
            contents = os.listdir(output_directory)
            return bool(len(contents) >= 60)
        else:
            return False

    def setup_input_directory(self):
        climo_temp_path = self.config.get('climo_tmp_dir')
        set_start_year = self.config.get('start_year')
        set_end_year = self.config.get('end_year')

        if not climo_temp_path or not os.path.exists(climo_temp_path):
            self.status = JobStatus.INVALID
            return False

        run_dir = os.path.join(
            self.config.get('test_archive_dir'),
            self.config.get('test_casename'),
            'run')
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        # create atm links
        climo_src_list = os.listdir(climo_temp_path)
        create_symlink_dir(
            src_dir=climo_temp_path,
            src_list=climo_src_list,
            dst=run_dir)
        # craete mpaso.hist.am links and mpascice links
        for mpas_dir in ['mpas_am_dir', 'mpas_cice_dir']:
            mpas_temp_list = []
            all_years = []
            mpas_path = self.config.get(mpas_dir)
            for mpas in os.listdir(mpas_path):
                start = re.search(r'\.\d\d\d\d', mpas)
                s_index = start.start() + 1
                year = int(mpas[s_index: s_index + 4])
                if year > set_end_year or year < set_start_year:
                    if year == set_end_year + 1:
                        month = int(mpas[s_index + 5: s_index + 7])
                        if month == 1:
                            mpas_temp_list.append(mpas)
                    continue
                mpas_temp_list.append(mpas)
                if year not in all_years:
                    all_years.append(year)

            if len(all_years) < (set_end_year - set_start_year):
                return False
            create_symlink_dir(
                src_dir=mpas_path,
                src_list=mpas_temp_list,
                dst=run_dir)

        extras = ['mpas_am_dir', 'mpas_cice_in_dir', 'mpas_o_dir', 'streams_dir', 'mpas_rst_dir', 'rpt_dir']
        for extra in extras:
            path = self.config.get(extra)
            src_list = os.listdir(path)
            if not src_list:
                return False
            create_symlink_dir(
                src_dir=path,
                src_list=src_list,
                dst=run_dir)
        return True

    def generateIndex(self):
        """
        Generates the index.json for uploading to the diagnostic viewer
        """
        self.event_list = push_event(
            self.event_list,
            'Starting index generataion for coupled diagnostic')

        viewer = OutputViewer(index_name="Coupled Diagnostic")
        viewer.add_page("Coupled Diagnostics Output", ["Description", "ANN", "DJF", "JJA"])

        viewer.add_group('Time Series Plots: Global and Zonal-band means (ATM)')
        viewer.add_row('Precipitation Rate (GPCP)')
        viewer.add_col('PRECT')
        viewer.add_col('case_scripts_PRECT_ANN_reg_ts.png', is_file=True)

        viewer.add_row('TOM Net Radiative Flux')
        viewer.add_col('RESTOM')
        viewer.add_col('case_scripts_RESTOM_ANN_reg_ts.png', is_file=True)
        viewer.add_row('TOM Net LW Flux')
        viewer.add_col('FLNT')
        viewer.add_col('case_scripts_FLNT_ANN_reg_ts.png', is_file=True)
        viewer.add_row('TOM Net SW Flux')
        viewer.add_col('FSNT')
        viewer.add_col('case_scripts_FSNT_ANN_reg_ts.png', is_file=True)

        viewer.add_group('Time Series Plots: Global/Hemispheric means (OCN/ICE)')
        viewer.add_row('Global SST')
        viewer.add_col('Global Sea Surface Tempurature')
        viewer.add_col('sst_global_case_scripts.png', is_file=True)
        viewer.add_row('Global SST')
        viewer.add_col('Global Sea Surface Tempurature')
        viewer.add_col('sst_global_case_scripts.png', is_file=True)
        viewer.add_row('Global OHC')
        viewer.add_col('Global OHC')
        viewer.add_col('ohc_global_case_scripts.png', is_file=True)
        viewer.add_row('NH Ice Area')
        viewer.add_col('Northern Hemisphere Ice Area')
        viewer.add_col('iceAreaCellNH_case_scripts.png', is_file=True)
        viewer.add_row('SH Ice Area')
        viewer.add_col('Southern Hemisphere Ice Area')
        viewer.add_col('iceAreaCellSH_case_scripts.png', is_file=True)
        viewer.add_row('NH Ice Volume')
        viewer.add_col('Northern Hemisphere Ice Volume')
        viewer.add_col('iceVolumeCellNH_case_scripts.png', is_file=True)
        viewer.add_row('SH Ice Volume')
        viewer.add_col('Southern Hemisphere Ice Volume')
        viewer.add_col('iceVolumeCellSH_case_scripts.png', is_file=True)

        viewer.add_group('Climatology Plots (ATM)')
        viewer.add_row('PRECT')
        viewer.add_col('Precipitation Rate')
        viewer.add_col('case_scripts-GPCP_PRECT_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-GPCP_PRECT_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-GPCP_PRECT_climo_JJA.png', is_file=True)
        viewer.add_row('FSNTOA')
        viewer.add_col('TOA net SW flux')
        viewer.add_col('case_scripts-CERES-EBAF_FSNTOA_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_FSNTOA_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_FSNTOA_climo_JJA.png', is_file=True)
        viewer.add_row('FLUT')
        viewer.add_col('TOA upward LW flux')
        viewer.add_col('case_scripts-CERES-EBAF_FLUT_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_FLUT_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_FLUT_climo_JJA.png', is_file=True)
        viewer.add_row('SWCF')
        viewer.add_col('TOA shortwave cloud forcing')
        viewer.add_col('case_scripts-CERES-EBAF_SWCF_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_SWCF_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_SWCF_climo_JJA.png', is_file=True)
        viewer.add_row('LWCF')
        viewer.add_col('TOA longwave cloud forcing')
        viewer.add_col('case_scripts-CERES-EBAF_LWCF_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_LWCF_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-CERES-EBAF_LWCF_climo_JJA.png', is_file=True)
        viewer.add_row('TAU')
        viewer.add_col('Ocean Wind Stress')
        viewer.add_col('case_scripts-ERS_TAU_climo_ANN.png', is_file=True)
        viewer.add_col('case_scripts-ERS_TAU_climo_DJF.png', is_file=True)
        viewer.add_col('case_scripts-ERS_TAU_climo_JJA.png', is_file=True)

        viewer.add_group('Climatology Plots (OCN/ICE)')
        viewer.add_row('SST')
        viewer.add_col('SST Hadley-NOAA-OI')
        viewer.add_col('sstHADOI_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('sstHADOI_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('sstHADOI_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('SSS')
        viewer.add_col('SSS Aquarius')
        viewer.add_col('sssAquarius_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('sssAquarius_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('sssAquarius_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('MLD')
        viewer.add_col('MLD Holte-Talley ARGO')
        viewer.add_col('mldHolteTalleyARGO_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('mldHolteTalleyARGO_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('mldHolteTalleyARGO_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Conc.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('iceconcNASATeamNH_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcNASATeamNH_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcNASATeamNH_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Conc.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('iceconcBootstrapNH_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcBootstrapNH_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcBootstrapNH_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Thick.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('icethickNH_case_scripts_FM_years0001-0010.png', is_file=True)
        viewer.add_col('icethickNH_case_scripts_ON_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Conc.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('iceconcNASATeamSH_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcNASATeamSH_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcNASATeamSH_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Conc.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('iceconcBootstrapSH_case_scripts_ANN_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcBootstrapSH_case_scripts_JFM_years0001-0010.png', is_file=True)
        viewer.add_col('iceconcBootstrapSH_case_scripts_JAS_years0001-0010.png', is_file=True)
        viewer.add_row('Ice Thick.')
        viewer.add_col('Northern Hemisphere Sea-ice')
        viewer.add_col('icethickSH_case_scripts_FM_years0001-0010.png', is_file=True)
        viewer.add_col('icethickSH_case_scripts_ON_years0001-0010.png', is_file=True)

        viewer.generate_viewer(prompt_user=False)

    def execute(self, batch=False):
        """
        Perform the actual work
        """
        # First check if the job has already been completed
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'Coupled_diag job already computed, skipping'
            self.event_list = push_event(self.event_list, message)
            return 0
        # create symlinks to the input data
        if not self.setup_input_directory():
            return -1
        # self.config['test_archive_dir'] = os.path.join(
        #     os.path.abspath(os.path.dirname(__file__)),
        #     '..',
        #     self.config.get('test_archive_dir'),
        #     self.config.get('test_casename'),
        #     'run')
        # self.config['test_archive_dir'] = os.path.join(
        #     os.path.abspath(os.path.dirname(__file__)),
        #     '..',
        #     self.config.get('test_archive_dir'))

        # render the run_AIMS.csh script
        render(
            variables=self.config,
            input_path=self.config.get('coupled_template_path'),
            output_path=self.config.get('rendered_output_path'),
            delimiter='%%')

        cmd = 'csh {run_AIMS}'.format(run_AIMS=self.config.get('rendered_output_path'))

        expected_name = 'coupled_diag_set_{set}_{start}_{end}_{uuid}'.format(
            set=self.config.get('year_set'),
            start=self.config.get('test_begin_yr_climo'),
            end=self.config.get('test_end_yr_climo'),
            uuid=self.uuid[:5])

        run_script = os.path.join(self.config.get('run_scripts_path'), expected_name)
        self.slurm_args['error_file'] = '-e {err}'.format(err=run_script + '.err')
        self.slurm_args['out_file'] = '-o {out}'.format(out=run_script + '.out')
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            slurm_args = ['#SBATCH {}'.format(self.slurm_args[s]) for s in self.slurm_args]
            slurm_prefix = '\n'.join(slurm_args) + '\n'
            batchfile.write(slurm_prefix)
            batchfile.write(cmd)

        # slurm_cmd = ['sbatch', run_script, '--oversubscribe']
        slurm_cmd = ['sbatch', run_script, '--oversubscribe']
        started = False
        retry_count = 0

        # handle coupled diags crazyness
        prev_dir = os.getcwd()
        os.chdir(self.config.get('coupled_diags_home'))

        while not started:
            while True:
                try:
                    self.proc = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
                except:
                    sleep(1)
                else:
                    break
            output, err = self.proc.communicate()
            started, job_id = check_slurm_job_submission(expected_name)
            if started:
                os.chdir(prev_dir)
                self.job_id = job_id
                message = "## {job} id: {id} changed status to {status}".format(
                    job=self.type,
                    id=self.job_id,
                    status=self.status)
                logging.info(message)
            else:
                logging.warning('Failed to start diag job, trying again')
                logging.warning('%s \n%s', output, err)

        return self.job_id
