import os
import sys
import logging
import time

from uuid import uuid4
from pprint import pformat
from subprocess import Popen, PIPE

from output_viewer.index import OutputPage
from output_viewer.index import OutputIndex
from output_viewer.index import OutputRow
from output_viewer.index import OutputGroup
from output_viewer.index import OutputFile

from lib.util import render
from lib.util import print_message
from lib.util import print_debug
from lib.util import check_slurm_job_submission
from lib.util import cmd_exists
from lib.util import create_symlink_dir
from JobStatus import JobStatus


class CoupledDiagnostic(object):
    def __init__(self, config):
        """
        Setup class attributes
        """
        self.inputs = {
            'year_set': '',
            'nco_path': '',
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
            'obs_iceareaNH': '',
            'obs_iceareaSH': '',
            'obs_icevolNH': '',
            'obs_icevolSH': ''
        }
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 1 hour run time
            'num_machines': '-N 1', # run on one machine
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

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        """
        print 'postvalidate'

    def generateIndex(self):
        print_message('Starting index generataion', 'ok')
        outpage = OutputPage(self.config.get('test_casename'))
        version = time.strftime("%d-%m-%Y-%I:%M") + '-year-set-' + str(self.year_set) + '-coupled-diagnostic'
        index = OutputIndex(self.config.get('test_casename'), version=version)

        images_path = os.path.join(
            self.config.get('coupled_project_dir'),
            os.environ['USER'])
        suffix = [s for s in os.listdir(images_path) if 'coupled' in s and not s.endswith('.logs')].pop()
        images_path = os.path.join(images_path, suffix)

        image_list = os.listdir(images_path)
        file_list = []
        row_list = []
        group_list = []
        for image in image_list:
            title = image[ len(self.config.get('test_casename')) + 1: -4]
            outfile = OutputFile(
                path=image,
                title=title)
            file_list.append(outfile)

        for var in self.var_list:
            tmp_row_list = []
            for file in file_list:
                if var in file.path:
                    # print 'adding {} to {} row'.format(file.path, var)
                    tmp_row_list.append(file)
            row_list.append(OutputRow(var, tmp_row_list))

        outgroup = OutputGroup('Coupled Diag')
        outpage.addGroup(outgroup)
        for row in range(len(row_list)):
            outpage.addRow(row_list[row], 0)

        index.addPage(outpage)
        outpath = os.path.join(images_path, 'index.json')
        print_message('writing index file to {}'.format(outpath))
        index.toJSON(outpath)

    def setup_input_directory(self):
        set_start_year = self.config.get('start_year')
        set_end_year = self.config.get('end_year')

        run_dir = os.path.join(
            self.config.get('test_archive_dir'),
            self.config.get('test_casename'),
            'run')
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        src_list = os.listdir(self.config.get('climo_tmp_dir'))

        create_symlink_dir(
            src_dir=self.config.get('climo_tmp_dir'),
            src_list=src_list,
            dst=run_dir)

        return 0


    def execute(self, batch=False):
        """
        Perform the actual work
        """
        # render the run_AIMS.csh script
        render(
            variables=self.config,
            input_path=self.config.get('coupled_template_path'),
            output_path=self.config.get('rendered_output_path'),
            delimiter='%%')
        # create symlinks to the input data
        self.setup_input_directory()
        # add nco to the system path if its not there
        nco_path = self.config.get('nco_path')
        if not cmd_exists('ncremap') and nco_path:
            sys.path.append(nco_path)

        if not batch:
            pass
        else:
            if batch == 'slurm':
                cmd = 'csh {run_AIMS}'.format(
                    run_AIMS=self.config.get('rendered_output_path'))

                expected_name = 'coupled_diag_' + str(self.uuid)
                run_script = os.path.join(os.getcwd(), 'run_scripts', expected_name)
                self.slurm_args['error_file'] = '-e {err}'.format(err=run_script + '.err')
                self.slurm_args['out_file'] = '-o {out}'.format(out=run_script + '.out')
                with open(run_script, 'w') as batchfile:
                    batchfile.write('#!/bin/bash\n')
                    slurm_args = ['#SBATCH {}'.format(self.slurm_args[s]) for s in self.slurm_args]
                    slurm_prefix = '\n'.join(slurm_args) + '\n'
                    batchfile.write(slurm_prefix)
                    batchfile.write(cmd)

                slurm_cmd = ['sbatch', run_script]
                started = False
                retry_count = 0

                # handle coupled diags crazyness
                prev_dir = os.getcwd()
                os.chdir(self.config.get('coupled_diags_home'))
                user_dir = os.path.join(self.config.get('coupled_project_dir'), os.environ['USER'])
                if not os.path.exists(user_dir):
                    os.makedirs(user_dir)

                while not started and retry_count < 5:
                    self.proc = Popen(slurm_cmd, stdout=PIPE)
                    output, err = self.proc.communicate()
                    started, job_id = check_slurm_job_submission(expected_name)
                    if started:
                        os.chdir(prev_dir)
                        self.status = JobStatus.RUNNING
                        self.job_id = job_id
                        message = "## {job} id: {id} changed status to {status}".format(
                            job=self.type,
                            id=self.job_id,
                            status=self.status)
                        logging.info(message)
                    else:
                        logging.warning('Failed to start diag job trying again, attempt %s', str(retry_count))
                        logging.warning('%s \n%s', output, err)
                        retry_count += 1

                if retry_count >= 5:
                    self.status = JobStatus.FAILED
                    message = "## {job} id: {id} changed status to {status}".format(
                        job=self.type,
                        id=self.job_id,
                        status=self.status)
                    logging.error(message)
                    self.job_id = 0
                return self.job_id

            elif batch == 'pbs':
                pass
            else:
                print_message('Unrecognized batch system {}'.format(batch))
        return

