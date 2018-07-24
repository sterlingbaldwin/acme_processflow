"""
A module for the base Job class that all jobs decend from
"""
import json
import os
import logging
from uuid import uuid4
from lib.slurm import Slurm
from lib.jobstatus import JobStatus
from lib.util import create_symlink_dir, print_line


class Job(object):
    """
    A base job class for all post-processing and diagnostic jobs
    """
    def __init__(self, start, end, case, short_name, data_required=None, **kwargs):
        self._start_year = start
        self._end_year = end
        self._data_required = data_required
        self._data_ready = False
        self._depends_on = list()
        self._id = uuid4().hex[:10]
        self._job_id = 0
        self._has_been_executed = False
        self._status = JobStatus.VALID
        self._case = case
        self._short_name = short_name
        self._run_type = kwargs.get('run_type')
        self._job_type = None
        self._input_file_paths = list()
        self._console_output_path = None
        self._output_path = ''
        self._dryrun = True if kwargs.get('dryrun') == True else False
        self._slurm_args = dict()
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        msg = '{} has not implemented the setup_dependencies method'.format(self.job_type)
        raise Exception(msg)
    # -----------------------------------------------
    def prevalidate(self, *args, **kwargs):
        msg = '{} has not implemented the prevalidate method'.format(self.job_type)
        raise Exception(msg)
    # -----------------------------------------------
    def execute(self, *args, **kwargs):
        msg = '{} has not implemented the execute method'.format(self.job_type)
        raise Exception(msg)
    # -----------------------------------------------
    def postvalidate(self, *args, **kwargs):
        msg = '{} has not implemented the postvalidate method'.format(self.job_type)
        raise Exception(msg)
    # -----------------------------------------------
    def handle_completion(self, *args, **kwargs):
        msg = '{} has not implemented the handle_completion method'.format(self.job_type)
        raise Exception(msg)
    # -----------------------------------------------
    def get_output_path(self):
        if self.status == JobStatus.COMPLETED:
            return self._output_path
        else:
            return self._console_output_path
    # -----------------------------------------------
    def get_report_string(self):
        return '{prefix} :: {status} :: {output}'.format(
            prefix=self.msg_prefix(),
            status=self.status.name,
            output=self.get_output_path())
    # -----------------------------------------------
    def setup_data(self, config, filemanager, case):
        """
        symlinks all data_types sepecified in the jobs _data_required field,
        and puts a copy of the path for the links into the _input_file_paths field
        """
        for datatype in self._data_required:
            monthly = config['data_types'][datatype].get('monthly')
            # first get the list of file paths to the data
            if monthly == 'True' or monthly == True:
                files = filemanager.get_file_paths_by_year(
                    datatype=datatype,
                    case=case,
                    start_year=self._start_year,
                    end_year=self._end_year)
            else:
                files = filemanager.get_file_paths_by_year(
                    datatype=datatype,
                    case=case)
            if not files or len(files) == 0:
                msg = '{prefix}: filemanager cant find input files for datatype {datatype}'.format(
                    prefix=self.msg_prefix(),
                    datatype=datatype)
                logging.error(msg)
                continue
            
            # setup the temp directory to hold symlinks
            if self._run_type is not None:
                temp_path = os.path.join(
                    config['global']['project_path'],
                    'output', 'temp', self._short_name, 
                    '{}_{}'.format(self._job_type, self._run_type), 
                    '{:04d}_{:04d}'.format(self._start_year, self._end_year))
            elif isinstance(self, Diag):
                if self._comparison == 'obs':
                    comp = 'obs'
                else:
                    comp = config['simulations'][self.comparison]['short_name']
                temp_path = os.path.join(
                    config['global']['project_path'],
                    'output', 'temp', self._short_name, self._job_type, 
                    '{:04d}_{:04d}_vs_{}'.format(self._start_year, self._end_year, comp))
            else:
                temp_path = os.path.join(
                    config['global']['project_path'],
                    'output', 'temp', self._short_name, self._job_type, 
                    '{:04d}_{:04d}'.format(self._start_year, self._end_year))
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)
            
            # extract the file names
            filesnames = list()
            for file in files:
                tail, head = os.path.split(file)
                filesnames.append(head)

            # create the symlinks
            create_symlink_dir(
                src_dir=tail,
                src_list=filesnames,
                dst=temp_path)
            
            # keep a reference to the input data for later
            self._input_file_paths.extend([os.path.join(temp_path, x) for x in filesnames])
        return
    # -----------------------------------------------
    def check_data_ready(self, filemanager):
        """
        Checks that the data needed for the job is present on the machine, in the input directory
        """
        if self._data_ready == True:
            return
        else:
            self._data_ready = filemanager.check_data_ready(
                data_required=self._data_required,
                case=self._case,
                start_year=self.start_year,
                end_year=self.end_year)
        return
    # -----------------------------------------------
    def check_data_in_place(self):
        """
        Checks that the data needed for the job has been symlinked into the jobs temp directory

        This assumes that the job.setup_data method worked correctly and all files needed are in 
            the _input_file_paths list
        """
        if len(self._input_file_paths) == 0:
            return False

        for item in self._input_file_paths:
            if not os.path.exists(item):
                msg = '{prefix}: File not found in input temp directory {file}'.format(
                    prefix=self.msg_prefix(),
                    file=item)
                logging.error(msg)
                return False
        # nothing was missing
        return True
    # -----------------------------------------------
    def msg_prefix(self):
        if self._run_type:
            return '{type}-{run_type}-{start:04d}-{end:04d}-{case}'.format(
                type=self.job_type,
                run_type=self._run_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name)
        elif isinstance(self, Diag):
            return '{type}-{start:04d}-{end:04d}-{case}-vs-{comp}'.format(
                type=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
        else:
            return '{type}-{start:04d}-{end:04d}-{case}'.format(
                type=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name)
    # -----------------------------------------------
    def _submit_cmd_to_slurm(self, config, cmd):
        """
        Takes the jobs main cmd, generates a batch script and submits the script
        to the slurm controller
        
        Parameters:
            cmd (str): the command to submit
            config (dict): the global configuration object
        Retuns:
            job_id (int): the slurm job_id
        """    
        # setup for the run script
        scripts_path = os.path.join(
            config['global']['project_path'],
            'output', 'scripts')
        if self._run_type is not None:
            run_name = '{type}_{run_type}_{start:04d}_{end:04d}_{case}'.format(
                type=self.job_type,
                run_type=self._run_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name)
        elif isinstance(self, Diag):
            run_name = '{type}_{start:04d}_{end:04d}_{case}_vs_{comp}'.format(
                type=self.job_type,
                run_type=self._run_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
        else:
            run_name = '{type}_{start:04d}_{end:04d}_{case}'.format(
                type=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name)
        run_script = os.path.join(scripts_path, run_name)
        self._console_output_path = '{}.out'.format(run_script)
        if os.path.exists(run_script):
            os.remove(run_script)

        # generate the run script using the slurm arguments and command
        slurm_command = ' '.join(cmd)
        self._slurm_args['output_file'] = '-o {output_file}'.format(
            output_file=self._console_output_path)
        slurm_prefix = ''
        for key, val in self._slurm_args.items():
            slurm_prefix += '#SBATCH {}\n'.format(val)

        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            batchfile.write(slurm_prefix)
            batchfile.write(slurm_command)

        # if this is a dry run, set the status and exit
        if self._dryrun:
            self.status = JobStatus.COMPLETED
            return 0

        # submit the run script to the slurm controller
        slurm = Slurm()
        self._job_id = slurm.batch(run_script)
        self._has_been_executed = True
        return self._job_id
    # -----------------------------------------------
    def prevalidate(self, *args, **kwargs):
        if not self.data_ready:
            return False
        if not self.check_data_in_place():
            return False
        return True
    # -----------------------------------------------
    @property
    def short_name(self):
        return self._short_name        
    # -----------------------------------------------
    @property
    def comparison(self):
        return 'obs'
    # -----------------------------------------------
    @property
    def case(self):
        return self._case
    # -----------------------------------------------
    @property
    def start_year(self):
        return self._start_year
    # -----------------------------------------------
    @property
    def end_year(self):
        return self._end_year
    # -----------------------------------------------
    @property
    def job_type(self):
        return self._job_type
    # -----------------------------------------------
    @property
    def depends_on(self):
        return self._depends_on
    # -----------------------------------------------
    @property
    def id(self):
        return self._id
    # -----------------------------------------------
    @property
    def data_ready(self):
        return self._data_ready
    @data_ready.setter
    def data_ready(self, ready):
        if not isinstance(ready, bool):
            raise Exception('Invalid data type, data_ready only accepts bools')
        self._data_ready = ready
    # -----------------------------------------------
    @property
    def run_type(self):
        return self._run_type
    # -----------------------------------------------
    @property
    def data_required(self):
        return self._data_required
    @data_required.setter
    def data_required(self, types):
        self._data_required = types
    # -----------------------------------------------
    @property
    def status(self):
        return self._status
    @status.setter
    def status(self, nstatus):
        self._status = nstatus
    # -----------------------------------------------
    def __str__(self):    
        return json.dumps({
            'type': self._job_type,
            'start_year': self._start_year,
            'end_year': self._end_year,
            'data_required': self._data_required,
            'data_ready': self._data_ready,
            'depends_on': self._depends_on,
            'id': self._id,
            'status': self._status.name,
            'case': self._case,
            'short_name': self._short_name
        }, sort_keys=True, indent=4)
    # -----------------------------------------------
from jobs.diag import Diag