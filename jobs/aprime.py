import os
import re
import json
import logging
import subprocess

from bs4 import BeautifulSoup
from shutil import move

from jobs.diag import Diag
from lib.util import render, print_line
from lib.JobStatus import JobStatus

class Aprime(Diag):
    def __init__(self, *args, **kwargs):
        super(Aprime, self).__init__(*args, **kwargs)
        self._job_type = 'aprime'
        self._requires = ''
        self._host_path = ''
        self._host_url = ''
        self._short_comp_name = ''
        self._slurm_args = {
            'num_cores': '-n 24',  # 24 cores
            'run_time': '-t 0-10:00',  # 10 hours run time
            'num_machines': '-N 1',  # run on one machine
            'working_dir': ''
        }
        self._data_required = ['atm', 'cice', 'ocn', 
                               'ocn_restart', 'cice_restart', 
                               'ocn_streams', 'cice_streams', 
                               'ocn_in', 'cice_in', 
                               'meridionalHeatTransport']
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        aprime doesnt depend on any other jobs
        """
        return
    # -----------------------------------------------
    def prevalidate(self, *args, **kwargs):
        return self.data_ready
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        self._output_path = os.path.join(
            config['global']['project_path'],
            'output', 'diags', self.short_name, 'aprime',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

        self._slurm_args['working_dir'] = '-D {}'.format(
            config['diags']['aprime']['aprime_code_path'])
        
        # fix the input paths
        self._fix_input_paths()
        
        self._host_path = os.path.join(
            config['img_hosting']['host_directory'],
            self.case,
            'aprime',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        
        # setup template
        template_out = os.path.join(
            config['global']['run_scripts_path'],
            'aprime_{start:04d}_{end:04d}_{case}_vs_{comp}.bash'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))
        variables = dict()
        input_path, _ = os.path.split(self._input_file_paths[0])
        variables['test_casename'] = self.case
        variables['output_base_dir'] = self._output_path
        variables['test_archive_dir'] = input_path + os.sep
        variables['test_atm_res'] = config['simulations'][self.case]['native_grid_name']
        variables['test_mpas_mesh_name'] = config['simulations'][self.case]['native_mpas_grid_name']
        variables['test_begin_yr_climo'] = self.start_year
        variables['test_end_yr_climo'] = self.end_year
        variables['www_dir'] = self._host_path

        template_input_path = os.path.join(
            config['global']['resource_path'],
            'aprime_template_vs_obs.bash')
        
        render(
            variables=variables,
            input_path=template_input_path,
            output_path=template_out)
        
        cmd = ['bash', template_out]
        return self._submit_cmd_to_slurm(config, cmd)
    # -----------------------------------------------
    def postvalidate(self, config):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        self._host_path = os.path.join(
            config['img_hosting']['host_directory'],
            self.case,
            'aprime',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        return self._check_links(config)
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        if self.status != JobStatus.COMPLETED:
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job failed, not running completion handler'.format(
                job=self.job_type, 
                comp=self._short_comp_name,
                start=self.start_year,
                end=self.end_year,
                case=self._short_name)
            print_line(msg, event_list)
            logging.info(msg)
            return
        else:
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job complete'.format(
                job=self.job_type, 
                comp=self._short_comp_name,
                start=self.start_year,
                end=self.end_year,
                case=self._short_name)
            print_line(msg, event_list)
            logging.info(msg)
        
        # if hosting is turned off, simply return
        if not config['global']['host']:
            return

        img_source = os.path.join(
            self._output_path,
            'coupled_diagnostics',
            '{case}_vs_{comp}'.format(case=self.case, comp=self._short_comp_name),
            '{case}_years{start}-{end}_vs_{comp}'.format(
                case=self.case,
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))

        # setup the web hosting
        hostname = config['img_hosting']['img_host_server']
        self._host_path = os.path.join(
            config['img_hosting']['host_directory'],
            self.case,
            'aprime',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        
        self.setup_hosting(config, img_source, self._host_path, event_list)
        
        self._host_url = 'https://{server}/{prefix}/{case}/aprime/{start:04d}_{end:04d}_vs_{comp}/{case}_years{start}-{end}_vs_{comp}/index.html'.format(
            server=config['img_hosting']['img_host_server'],
            prefix=config['img_hosting']['host_prefix'],
            case=self.case,
            start=self.start_year,
            end=self.end_year,
            comp=self._short_comp_name)
    # -----------------------------------------------
    def _check_links(self, config):
        """
        Check that all the links exist in the output page
        returns True if all the links are found, False otherwise
        """
        found = False
        host_directory = "{experiment}_years{start}-{end}_vs_{comp}".format(
            experiment=self.case,
            start=self.start_year,
            end=self.end_year,
            comp=self._short_comp_name)

        web_dir = os.path.join(
            self._host_path,
            host_directory)

        page_path = os.path.join(web_dir, 'index.html')

        if not os.path.exists(page_path):
            msg = 'aprime-{start:04d}-{end:04d}-{case}-vs-{comp}: No output page found'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name,
                case=self.short_name)
            logging.error(msg)
            return False
        else:
            msg = 'aprime-{start:04d}-{end:04d}-{case}-vs-{comp}: found output index.html at {page}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name,
                case=self.short_name,
                page=page_path)
            logging.info(msg)

        missing_pages = list()
        with open(page_path, 'r') as fp:
            page = BeautifulSoup(fp, 'lxml')
            links = page.findAll('a')
            for link in links:
                link_path = os.path.join(web_dir, link.attrs['href'])
                if not os.path.exists(link_path):
                    missing_pages.append(link.attrs['href'])

        if missing_pages:
            msg = 'aprime-{start:04d}-{end:04d}-{case}-vs-{comp}: missing plots'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name,
                case=self.short_name,
                page=page_path)
            logging.error(msg)
            logging.error(missing_pages)
            return False
        else:
            msg = 'aprime-{start:04d}-{end:04d}-{case}-vs-{comp}: all links found'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self.comparison)
            logging.info(msg)
            return True
    # -----------------------------------------------
    def _fix_input_paths(self):
        """
        Aprime has some hardcoded paths setup that have to be fixed or it will crash
        """
        tail, head = os.path.split(self._input_file_paths[0])
        fixed_input_path = os.path.join(
            tail, self.case, 'run')
        
        if not os.path.exists(fixed_input_path):
            os.makedirs(fixed_input_path)
        
        for item in self._input_file_paths:
            # move the input file, then update the pointer
            new_path = os.path.join(
                item, fixed_input_path)
            if os.path.exists(new_path):
                continue
            move(item, fixed_input_path)
            tail, head = os.path.split(item)
            item = os.path.join(fixed_input_path, head)