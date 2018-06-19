import json
import os
import logging

from uuid import uuid4
from shutil import copytree, rmtree
from subprocess import call

from lib.slurm import Slurm
from lib.JobStatus import JobStatus
from lib.util import create_symlink_dir, print_line
from jobs.job import Job


class Diag(Job):
    def __init__(self, *args, **kwargs):
        super(Diag, self).__init__(*args, **kwargs)
        self._comparison = kwargs.get('comparison', 'obs')
    # -----------------------------------------------
    @property
    def comparison(self):
        return self._comparison
    # -----------------------------------------------
    def __str__(self):    
        return json.dumps({
            'type': self._job_type,
            'start_year': self._start_year,
            'end_year': self._end_year,
            'data_required': self._data_required,
            'depends_on': self._depends_on,
            'id': self._id,
            'comparison': self._comparison,
            'status': self._status.name,
            'case': self._case
        }, sort_keys=True, indent=4)
    # -----------------------------------------------
    def setup_hosting(self, config, img_source, host_path, event_list):
        if config['global']['always_copy']:
            if os.path.exists(host_path):
                msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Removing previous output from host location'.format(
                    job=self.job_type,
                    start=self.start_year,
                    end=self.end_year,
                    case=self.short_name,
                    comp=self._short_comp_name)
                print_line(msg, event_list)
                rmtree(host_path)
        if not os.path.exists(host_path):
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Moving files for web hosting'.format(
                job=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
            print_line(msg, event_list)
            copytree(
                src=img_source,
                dst=host_path)
            
            # fix permissions for apache
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Fixing permissions'.format(
                job=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
            print_line(msg, event_list)
            call(['chmod', '-R', 'a+rx', host_path])
            tail, _ = os.path.split(host_path)
            for _ in range(3):
                call(['chmod', 'a+rx', tail])
                tail, _ = os.path.split(tail)
        else:
            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Files already present at host location, skipping'.format(
                job=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
            print_line(msg, event_list)
    # -----------------------------------------------
    def get_report_string(self):
        if self.status == JobStatus.COMPLETED:
            msg = '{job}-{start:04d}-{end:04d}-vs-{comp} :: {status} :: {url}'.format(
                job=self.job_type,
                start=self.start_year,
                end=self.end_year,
                status=self.status.name,
                comp=self._short_comp_name,
                url=self._host_url)
        else:
            msg = '{job}-{start:04d}-{end:04d}-vs-{comp} :: {status} :: {console_path}'.format(
                job=self.job_type,
                start=self.start_year,
                end=self.end_year,
                status=self.status.name,
                comp=self._short_comp_name,
                console_path=self._console_output_path)
        return msg