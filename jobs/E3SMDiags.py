import os
import json
import logging
import subprocess

from bs4 import BeautifulSoup
from shutil import copytree, rmtree

from jobs.diag import Diag
from lib.util import render, print_line
from lib.jobstatus import JobStatus

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
            'num_cores': '-n 24',  # 24 cores
            'run_time': '-t 0-10:00',  # 10 hours run time
            'num_machines': '-N 1',  # run on one machine
        }
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = kwargs['config']['simulations'][self.comparison]['short_name']
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
            try:
                self_climo, = filter(lambda job: self._dep_filter(job), jobs)
            except ValueError:
                raise Exception('Unable to find climo for {}, is this case set to generate climos?'.format(self.msg_prefix()))
            try:
                comparison_climo, = filter(lambda job: self._dep_filter(job), other_jobs)
            except ValueError:
                raise Exception('Unable to find climo for {}, is that case set to generates climos?'.format(self.comparison))
            self.depends_on.extend((self_climo.id, comparison_climo.id))
        else:
            try:
                self_climo, = filter(lambda job: self._dep_filter(job), jobs)
            except ValueError:
                raise Exception('Unable to find climo for {}, is this case set to generate climos?'.format(self.msg_prefix()))
            self.depends_on.append(self_climo.id)
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        
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
    def postvalidate(self, config, *args, **kwargs):
        return self._check_links(config)
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        
        if self.status != JobStatus.COMPLETED:
            msg = '{prefix}: Job failed'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
            logging.info(msg)
        else:
            msg = '{prefix}: Job complete'.format(
                prefix=self.msg_prefix())
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
            prefix=config['img_hosting']['url_prefix'],
            case=self.case,
            start=self.start_year,
            end=self.end_year,
            comp=self._short_comp_name)
    # -----------------------------------------------
    def _check_links(self, config):
        
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
            msg = '{prefix}: missing the following links'.format(
                prefix=self.msg_prefix())
            logging.error(msg)
            logging.error(missing_links)
            return False
        else:
            msg = '{prefix}: all links found'.format(
                prefix=self.msg_prefix())
            logging.info(msg)
            return True
    # -----------------------------------------------
