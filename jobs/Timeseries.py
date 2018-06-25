import json
import os
import logging
from jobs.job import Job
from lib.JobStatus import JobStatus
from lib.slurm import Slurm
from lib.util import get_ts_output_files, print_line
from lib.filemanager import FileStatus

class Timeseries(Job):
    def __init__(self, *args, **kwargs):
        super(Timeseries, self).__init__(*args, **kwargs)
        self._job_type = 'timeseries'
        self._data_required = [self._run_type]
        self._regrid = False
        self._slurm_args = {
            'num_cores': '-n 16',  # 16 cores
            'run_time': '-t 0-10:00',  # 5 hours run time
            'num_machines': '-N 1',  # run on one machine
        }

    def setup_dependencies(self, *args, **kwargs):
        """
        Timeseries doesnt require any other jobs
        """
        return True
    # -----------------------------------------------
    def postvalidate(self, config):
        regrid_map_path = config['post-processing']['timeseries'].get('regrid_map_path')
        if regrid_map_path:
            regrid_path = os.path.join(
                config['global']['project_path'], 'output', 'pp',
                config['post-processing']['timeseries']['destination_grid_name'],
                self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))
            self._regrid = True
            self._output_path = regrid_path
        else:
            self._regrid = False
            self._output_path = ts_path

        if self._dryrun:
            return True

        # First check that all the native grid ts files were created
        ts_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))
        self._output_path = ts_path

        for var in config['post-processing']['timeseries'][self._run_type]:
            file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                var=var, start=self.start_year, end=self.end_year)
            file_path = os.path.join(ts_path, file_name)
            if not os.path.exists(file_path):
                return False

        # next, if regridding is turned on check that all regrid ts files were created
        if self._regrid:
            regrid_path = os.path.join(
                config['global']['project_path'], 'output', 'pp',
                config['post-processing']['timeseries']['destination_grid_name'],
                self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))
            for var in config['post-processing']['timeseries'][self._run_type]:
                file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                var=var, start=self.start_year, end=self.end_year)
                file_path = os.path.join(regrid_path, file_name)
                if not os.path.exists(file_path):
                    return False

        # if nothing was missing then we must be done
        return True
    # -----------------------------------------------
    def execute(self, config, dryrun=False):
        
        # setup the ts output path
        ts_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))
        if not os.path.exists(ts_path):
            os.makedirs(ts_path)

        regrid_map_path = config['post-processing']['timeseries'].get('regrid_map_path')
        if regrid_map_path:
            regrid_path = os.path.join(
                config['global']['project_path'], 'output', 'pp',
                config['post-processing']['timeseries']['destination_grid_name'],
                self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))
            self._regrid = True
            self._output_path = regrid_path
        else:
            self._regrid = False
            self._output_path = ts_path

        # sort the input files
        self._input_file_paths.sort()
        list_string = ' '.join(self._input_file_paths)

        # create the ncclimo command string
        var_list = config['post-processing']['timeseries'][self._run_type]
        cmd = [
            'ncclimo',
            '-a', 'sdd',
            '-c', self.case,
            '-v', ','.join(var_list),
            '-s', str(self.start_year),
            '-e', str(self.end_year),
            '--ypf={}'.format(self.end_year - self.start_year + 1),
            '-o', ts_path
        ]
        if self._regrid:
            cmd.extend([
                '-O', regrid_path,
                '--map={}'.format(regrid_map_path),
            ])
        cmd.append(list_string)
        slurm_command = ' '.join(cmd)

        return self._submit_cmd_to_slurm(config, cmd)
            
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        
        if self.status != JobStatus.COMPLETED:
            msg = '{job}-{run_type}-{start:04d}-{end:04d}-{case}: Job failed, not running completion handler'.format(
                job=self.job_type, 
                run_type=self.run_type,
                start=self.start_year,
                end=self.end_year,
                case=self._short_name)
            print_line(msg, event_list)
            logging.info(msg)
            return
        else:
            msg = '{job}-{run_type}-{start:04d}-{end:04d}-{case}: Job complete'.format(
                job=self.job_type, 
                run_type=self.run_type,
                start=self.start_year,
                end=self.end_year,
                case=self._short_name)
            print_line(msg, event_list)
            logging.info(msg)

        var_list = config['post-processing']['timeseries'][self._run_type]

        # add native timeseries files to the filemanager db
        ts_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            config['simulations'][self.case]['native_grid_name'],
            self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))

        new_files = list()
        for ts_file in get_ts_output_files(ts_path, var_list, self.start_year, self.end_year):
            new_files.append({
                'name': ts_file,
                'local_path': os.path.join(ts_path, ts_file),
                'case': self.case,
                'year': self.start_year,
                'local_status': FileStatus.PRESENT.value
            })
        filemanager.add_files(
            data_type='ts_native',
            file_list=new_files)
        if not config['data_types'].get('ts_native'):
            config['data_types']['ts_native'] = {'monthly': False}
        
        if self._regrid:
            # add regridded timeseries files to the filemanager db
            regrid_path = os.path.join(
                config['global']['project_path'], 'output', 'pp',
                config['post-processing']['timeseries']['destination_grid_name'],
                self._short_name, 'ts', '{length}yr'.format(length=self.end_year-self.start_year+1))

            new_files = list()
            for regrid_file in get_ts_output_files(ts_path, var_list, self.start_year, self.end_year):
                new_files.append({
                    'name': regrid_file,
                    'local_path': os.path.join(regrid_path, regrid_file),
                    'case': self.case,
                    'year': self.start_year,
                    'local_status': FileStatus.PRESENT.value
                })
            filemanager.add_files(
                data_type='ts_regrid',
                file_list=new_files)
            if not config['data_types'].get('ts_regrid'):
                config['data_types']['ts_regrid'] = {'monthly': False}
        
        msg = '{job}-{start:04d}-{end:04d}-{case}: Job completion handler done'.format(
            job=self.job_type, start=self.start_year, end=self.end_year, case=self._short_name)
        print_line(msg, event_list)
        logging.info(msg)
    # -----------------------------------------------
    def __str__(self):    
        return json.dumps({
            'type': self._job_type,
            'run_type': self._run_type,
            'start_year': self._start_year,
            'end_year': self._end_year,
            'data_required': self._data_required,
            'depends_on': self._depends_on,
            'id': self._id,
            'status': self._status.name,
            'case': self.short_name
        }, sort_keys=True, indent=4)
