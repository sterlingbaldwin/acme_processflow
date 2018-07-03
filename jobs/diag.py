import json
import os
import logging

from uuid import uuid4
from shutil import copytree, rmtree
from subprocess import call

from lib.slurm import Slurm
from lib.jobstatus import JobStatus
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
            if os.path.exists(host_path) and self.job_type != 'aprime':
                msg = '{prefix}: Removing previous output from host location'.format(
                    prefix=self.msg_prefix())
                print_line(msg, event_list)
                rmtree(host_path)
        if not os.path.exists(host_path):
            msg = '{prefix}: Moving files for web hosting'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
            copytree(
                src=img_source,
                dst=host_path)
            
            # fix permissions for apache
            msg = '{prefix}: Fixing permissions'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
        else:
            msg = '{prefix}: Files already present at host location, skipping'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
        call(['chmod', '-R', 'a+rx', host_path])
        tail, _ = os.path.split(host_path)
        for _ in range(2):
            call(['chmod', 'a+rx', tail])
            tail, _ = os.path.split(tail)
    # -----------------------------------------------
    def get_report_string(self):
        if self.status == JobStatus.COMPLETED:
            msg = '{prefix} :: {status} :: {url}'.format(
                prefix=self.msg_prefix(),
                status=self.status.name,
                url=self._host_url)
        else:
            msg = '{prefix} :: {status} :: {console_path}'.format(
                prefix=self.msg_prefix(),
                status=self.status.name,
                console_path=self._console_output_path)
        return msg