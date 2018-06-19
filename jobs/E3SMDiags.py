import os
import json
import logging
import subprocess

from bs4 import BeautifulSoup
from shutil import copytree, rmtree

from jobs.diag import Diag
from lib.util import render, print_line
from lib.JobStatus import JobStatus

class E3SMDiags(Diag):
    def __init__(self, *args, **kwargs):
        super(E3SMDiags, self).__init__(*args, **kwargs)
        self._job_type = 'e3sm_diags'
        self._requires = 'climo'
        self._data_required = ['climo_regrid']
        self._host_path = ''
        self._host_url = ''
        self._short_comp_name = ''
        self._slurm_args = {
            'num_cores': '-n 24',  # 16 cores
            'run_time': '-t 0-10:00',  # 5 hours run time
            'num_machines': '-N 1',  # run on one machine
            'oversubscribe': '--oversubscribe'
        }
    # -----------------------------------------------
    def _dep_filter(self, job):
        """
        find the climo job we're waiting for, assuming there's only
        one climo job in this case with the same start and end years
        """
        if job.job_type != self._requires: return False
        if job.start_year != self.start_year: return False
        if job.end_year != self.end_year: return False
        return True
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        AMWG requires climos
        """
        jobs = kwargs['jobs']
        if self.comparison != 'obs':
            other_jobs = kwargs['comparison_jobs']
            self_climo, = filter(lambda job: self._dep_filter(job), jobs)
            comparison_climo, = filter(lambda job: self._dep_filter(job), other_jobs)
            self.depends_on.extend((self_climo.id, comparison_climo.id))
        else:
            climo, = filter(lambda job: self._dep_filter(job), jobs)
            self.depends_on.append(climo.id)
    # -----------------------------------------------
    def prevalidate(self, *args, **kwargs):
        """
        e3sm_diags requires that ncclimo be run before it on atm input
        """
        if self._dryrun:
            return True
        return self.data_ready
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        self._output_path = os.path.join(
            config['global']['project_path'],
            'output', 'diags', self.short_name, 'e3sm_diags',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
        
        # render the parameter file from the template
        param_template_out = os.path.join(
            config['global']['run_scripts_path'],
            'e3sm_diags_{start:04d}_{end:04d}_{case}_vs_{comp}_params.py'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))
        variables = dict()
        input_path, _ = os.path.split(self._input_file_paths[0])
        variables['short_test_name'] = self.short_name
        variables['test_data_path'] = input_path
        variables['test_name'] = self.case
        variables['backend'] = config['diags']['e3sm_diags']['backend']
        variables['results_dir'] = self._output_path

        if self.comparison == 'obs':
            template_input_path = os.path.join(
                config['global']['resource_path'],
                'e3sm_diags_template_vs_obs.py')
            variables['reference_data_path'] = config['diags']['e3sm_diags']['reference_data_path']
        else:
            template_input_path = os.path.join(
                config['global']['resource_path'],
                'e3sm_diags_template_vs_model.py')
            input_path, _ = os.path.split(self._input_file_paths[0])
            variables['reference_data_path'] = input_path
            variables['ref_name'] = self.comparison
            variables['reference_name'] = config['simulations'][self.comparison]['short_name']
        
        render(
            variables=variables,
            input_path=template_input_path,
            output_path=param_template_out)
        
        if not dryrun:
            self._dryrun = False
            if not self.prevalidate():
                return False
            if self.postvalidate(config):
                self.status = JobStatus.COMPLETED
                return True
        else:
            self._dryrun = True
            return

        # create the run command and submit it
        cmd = ['acme_diags_driver.py', '-p', param_template_out]
        return self._submit_cmd_to_slurm(config, cmd)
    # -----------------------------------------------
    def postvalidate(self, config):
        return self._check_links(config)
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        if self.status != JobStatus.COMPLETED:
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job failed'.format(
                job=self.job_type, 
                comp=self._short_comp_name,
                start=self.start_year,
                end=self.end_year,
                case=self._short_name)
            print_line(msg, event_list)
            logging.info(msg)
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

        # else setup the web hosting
        hostname = config['img_hosting']['img_host_server']
        self.host_path = os.path.join(
            config['img_hosting']['host_directory'],
            self.case,
            'e3sm_diags',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        
        self.setup_hosting(config, self._output_path, self.host_path, event_list)
        
        self._host_url = 'https://{server}/{prefix}/{case}/e3sm_diags/{start:04d}_{end:04d}_vs_{comp}/viewer/index.html'.format(
            server=config['img_hosting']['img_host_server'],
            prefix=config['img_hosting']['host_prefix'],
            case=self.case,
            start=self.start_year,
            end=self.end_year,
            comp=self._short_comp_name)
    # -----------------------------------------------
    def _check_links(self, config):
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = config['simulations'][self.comparison]['short_name']
        self._output_path = os.path.join(
            config['global']['project_path'],
            'output', 'diags', self.short_name, 'e3sm_diags',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        viewer_path = os.path.join(self._output_path, 'viewer', 'index.html')
        if not os.path.exists(viewer_path):
            return False
        viewer_head = os.path.join(self._output_path, 'viewer')
        if not os.path.exists(viewer_head):
            return False
        missing_links = list()
        with open(viewer_path, 'r') as viewer_pointer:
            viewer_page = BeautifulSoup(viewer_pointer, 'lxml')
            viewer_links = viewer_page.findAll('a')
            for link in viewer_links:
                link_path = os.path.join(viewer_head, link.attrs['href'])
                if not os.path.exists(link_path):
                    missing_links.append(link_path)
                    continue
                if link_path[-4:] == 'html':
                    link_tail, _ = os.path.split(link_path)
                    with open(link_path, 'r') as link_pointer:
                        link_page = BeautifulSoup(link_pointer, 'lxml')
                        link_links = link_page.findAll('a')
                        for sublink in link_links:
                            try:
                                sublink_preview = sublink.attrs['data-preview']
                            except:
                                continue
                            else:
                                sublink_path = os.path.join(link_tail, sublink_preview)
                                if not os.path.exists(sublink_path):
                                    missing_links.append(sublink_path)
        if missing_links:
            msg = 'e3sm_diags-{start:04d}-{end:04d}-{case}-vs-{comp}: missing the following links'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self.comparison)
            logging.error(msg)
            logging.error(missing_links)
            return False
        else:
            msg = 'e3sm_diags-{start:04d}-{end:04d}-{case}-vs-{comp}: all links found'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self.comparison)
            logging.info(msg)
            return True
    # -----------------------------------------------
