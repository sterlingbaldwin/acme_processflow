import logging
import os
from jobs.job import Job
from lib.jobstatus import JobStatus
from lib.filemanager import FileStatus
from lib.util import get_climo_output_files, print_line
from lib.slurm import Slurm

class Climo(Job):
    def __init__(self, *args, **kwargs):
        super(Climo, self).__init__(*args, **kwargs)
        self._data_required = ['atm']
        self._job_type = 'climo'
        self._dryrun = True if kwargs.get('dryrun') == True else False
        self._slurm_args = {
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-10:00',  # 5 hours run time
            'num_machines': '-N 1',  # run on one machine
        }
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        Climo doesnt require any other jobs
        """
        return True
    # -----------------------------------------------
    def postvalidate(self, config, *args, **kwargs):
        """
        Postrun validation for Ncclimo
        
        Ncclimo outputs 17 files, one for each month and then one for the 5 seasons
        """
        if self._dryrun:
            return True
        regrid_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['post-processing']['climo']['destination_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))
        climo_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))
        self._output_path = climo_path

        # check the output directories exist
        if not os.path.exists(regrid_path):
            return False
        if not os.path.exists(climo_path):
            return False
        file_list = get_climo_output_files(
            input_path=regrid_path,
            start_year=self.start_year,
            end_year=self.end_year)
        if len(file_list) < 17: # number of months plus seasons and annual
            msg = '{prefix}: Failed to produce all regridded climos'.format(
                prefix=self.msg_prefix())
            logging.error(msg)
            return False
        file_list = get_climo_output_files(
            input_path=climo_path,
            start_year=self.start_year,
            end_year=self.end_year)
        if len(file_list) < 17: # number of months plus seasons and annual
            msg = '{prefix}: Failed to produce all native grid climos'.format(
                prefix=self.msg_prefix())
            logging.error(msg)
            return False

        # nothing's gone wrong, so we must be done
        return True
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        regrid_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['post-processing']['climo']['destination_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))
        if not os.path.exists(regrid_path):
            os.makedirs(regrid_path)

        climo_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))
        if not os.path.exists(climo_path):
            os.makedirs(climo_path)

        self._output_path = climo_path

        if not dryrun:
            self._dryrun = False
            if not self.prevalidate():
                return False
            if self.postvalidate(config):
                self.status = JobStatus.COMPLETED
                return True
        else:
            self._dryrun = True
              
        input_path, _ = os.path.split(self._input_file_paths[0])
        cmd = [
            'ncclimo',
            '-c', self.case,
            '-a', 'sdd',
            '-s', str(self.start_year),
            '-e', str(self.end_year),
            '-i', input_path,
            '-r', config['post-processing']['climo']['regrid_map_path'],
            '-o', climo_path,
            '-O', regrid_path,
            '--no_amwg_links',
        ]
        slurm_command = ' '.join(cmd)
        
        return self._submit_cmd_to_slurm(config, cmd)
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

        regrid_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['post-processing']['climo']['destination_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))

        new_files = list()
        for regrid_file in get_climo_output_files(regrid_path, self.start_year, self.end_year):
            new_files.append({
                'name': regrid_file,
                'local_path': os.path.join(regrid_path, regrid_file),
                'case': self.case,
                'year': self.start_year,
                'local_status': FileStatus.PRESENT.value
            })
        filemanager.add_files(
            data_type='climo_regrid',
            file_list=new_files)
        if not config['data_types'].get('climo_regrid'):
            config['data_types']['climo_regrid'] = {'monthly': True}
        
        climo_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'climo', '{length}yr'.format(length=self.end_year-self.start_year+1))

        for climo_file in get_climo_output_files(climo_path, self.start_year, self.end_year):
            new_files.append({
                'name': climo_file,
                'local_path': os.path.join(regrid_path, climo_file),
                'case': self.case,
                'year': self.start_year,
                'local_status': FileStatus.PRESENT.value
            })
        filemanager.add_files(
            data_type='climo_native',
            file_list=new_files)
        if not config['data_types'].get('climo_native'):
            config['data_types']['climo_native'] = {'monthly': True}
        
        msg = '{prefix}: Job completion handler done'.format(
            prefix=self.msg_prefix())
        print_line(msg, event_list)
        logging.info(msg)
