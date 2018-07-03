import json
import os
import re
import logging

from jobs.job import Job
from lib.jobstatus import JobStatus
from lib.slurm import Slurm
from lib.util import print_line, get_data_output_files
from lib.filemanager import FileStatus

class Regrid(Job):
    """
    Perform regridding with no climatology or timeseries generation on atm, lnd, and orn data
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize a regrid job
        Parameters:
            data_type (str): what type of data to run on (atm, lnd)
        """
        super(Regrid, self).__init__(*args, **kwargs)
        self._job_type = 'regrid'
        self._data_required = [self._run_type]
        self._slurm_args = {
            'num_cores': '-n 2',  # 2 cores
            'run_time': '-t 0-10:00',  # 10 hours run time
            'num_machines': '-N 1',  # run on one machine
        }
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        Regrid doesnt require any other jobs
        """
        return True
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        regrid_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['post-processing']['regrid'][self.run_type]['destination_grid_name'],
            self._short_name, self.job_type, self.run_type)
        self._output_path = regrid_path

        if not dryrun:
            self._dryrun = False
            if not self.prevalidate():
                return False
            if self.postvalidate(config):
                self.status = JobStatus.COMPLETED
                return True
        else:
            self._dryrun = True

        cmd = ['export PATH=/export/zender1/bin:$PATH\n', 
               'ncks --version\n',
               'ncremap --version\n',
               'ls |', 'ncremap']

        if self.run_type == 'lnd':
            cmd.extend([
                '-P', 'sgs',
                '-a', 'conserve',
                '-s', config['post-processing']['regrid']['lnd']['source_grid_path'],
                '-g', config['post-processing']['regrid']['lnd']['destination_grid_path']
            ])
        elif self.run_type == 'ocn':
            cmd.extend([
                '-P', 'mpas',
                '-m', config['post-processing']['regrid'][self.run_type]['regrid_map_path']
            ])
        elif self.run_type == 'atm':
            cmd.extend([
                '-m', config['post-processing']['regrid'][self.run_type]['regrid_map_path']
            ])
        else:
            msg = 'Unsupported regrid type'
            logging.error(msg)
            self.status = FAILED
            return 0

        input_path, _ = os.path.split(self._input_file_paths[0])

        # clean up the input directory to make sure there's only nc files
        for item in os.listdir(input_path):
            if not item[-3:] == '.nc':
                os.remove(os.path.join(input_path, item))
        self._slurm_args['working_dir'] = '-D {}'.format(input_path)
        cmd.extend([
            '-O', self._output_path,
        ])

        return self._submit_cmd_to_slurm(config, cmd)
    # -----------------------------------------------
    def postvalidate(self, config, *args, **kwargs): 
        self._output_path = os.path.join(
            config['global']['project_path'], 
            'output', 
            'pp',
            config['post-processing']['regrid'][self.run_type]['destination_grid_name'],
            self._short_name, 
            self.job_type, 
            self.run_type)

        if not self._output_path or not os.path.exists(self._output_path):
            return False
        
        contents = os.listdir(self._output_path)
        contents.sort()
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                pattern = r'%04d-%02d' % (year, month)
                found = False
                for item in contents:
                    if re.search(pattern, item):
                        found = True
                        break
                if not found:
                    if not self._has_been_executed:
                        msg = '{prefix}: Unable to find regridded output file for {yr}-{mon}'.format(
                            prefix=self.msg_prefix(),
                            yr=year,
                            mon=month)
                        logging.error(msg)
                    return False
        return True
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        if self.status != JobStatus.COMPLETED:
            msg = '{prefix}: Job failed, not running completion handler'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
            logging.info(msg)
            return
        else:
            msg = '{prefix}: Job complete'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
            logging.info(msg)
        
        new_files = list()
        for regrid_file in get_data_output_files(self._output_path, self.case, self.start_year, self.end_year):
            new_files.append({
                'name': regrid_file,
                'local_path': os.path.join(self._output_path, regrid_file),
                'case': self.case,
                'year': self.start_year,
                'local_status': FileStatus.PRESENT.value
            })
        filemanager.add_files(
                data_type='regrid',
                file_list=new_files)
        if not config['data_types'].get('regrid'):
            config['data_types']['regrid'] = {'monthly': True}
    # -----------------------------------------------
    @property
    def run_type(self):
        return self._run_type
    # -----------------------------------------------
    @property
    def data_type(self):
        return self._data_type
    # -----------------------------------------------
