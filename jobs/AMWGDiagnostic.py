# system level modules
import logging
import os
import re
import json
# system module functions
from uuid import uuid4
from subprocess import Popen, PIPE
from pprint import pformat
from time import sleep
import time
# job modules
from JobStatus import JobStatus
# output_viewer modules
from output_viewer.index import OutputPage
from output_viewer.index import OutputIndex
from output_viewer.index import OutputRow
from output_viewer.index import OutputGroup
from output_viewer.index import OutputFile
# lib.util modules
from lib.util import print_debug
from lib.util import print_message
from lib.util import check_slurm_job_submission
from lib.util import create_symlink_dir
from lib.util import push_event
from lib.util import render
from lib.util import get_climo_output_files


class AMWGDiagnostic(object):
    """
    A job class to perform the NCAR AMWG Diagnostic
    """
    def __init__(self, config, event_list):
        """
        Setup class attributes

        inputs:
            test_casename: the name of the test case e.g. b40.20th.track1.2deg.001
            test_filetype: the filetype of the history files, either monthly_history or time_series
            test_path_history: path to the directory holding your history files
            test_path_climo: path to directory holding climo files
            test_path_diag: the output path for the diagnostics to go
            control: what type to use for the control set, either OBS for observations, or USER for another model
                the following are only set with control==USER
                cntl_casename: the case_name of the control case
                cntl_filetype: either monthly_history or time_series
                cntl_path_history: path to the control history file
                cntl_path_climo: path to the control climo files
        """
        self.event_list = event_list
        self.status = JobStatus.INVALID
        self.inputs = {
            'host_directory': '',
            'host_prefix': '',
            'run_id': '',
            'diag_home': '',
            'test_casename': '',
            'test_path': '',
            'test_filetype': 'monthly_history',
            'test_path_history': '',
            'test_path_climo': '',
            'test_path_diag': '',
            'regrided_climo_path': '',
            'start_year': '',
            'end_year': '',
            'year_set': '',
            'run_directory': '',
            'template_path': '',
            'dataset_name': '',
        }
        self.type = 'amwg_diagnostic'
        self.outputs = {}
        self.config = {}
        self.uuid = uuid4().hex
        self.job_id = 0
        self.depends_on = []
        self.slurm_args = {
            'num_cores': '-n 16', # 16 cores
            'run_time': '-t 0-02:00', # 2 hours run time
            'num_machines': '-N 1', # run on one machine
            'oversubscribe': '--oversubscribe'
        }
        self.prevalidate(config)

    def get_type(self):
        """
        Getter for the job type
        """
        return self.type

    def set_status(self, status):
        """
        Setter for the job status
        """
        self.status = status

    def prevalidate(self, config):
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        for key, value in config.iteritems():
            if key in self.inputs:
                self.config[key] = value
        for key, value in self.inputs.iteritems():
            if key not in self.config:
                self.config[key] = value

        for depend in config.get('depends_on'):
            self.depends_on.append(depend)
        self.status = JobStatus.VALID

    def get_set(self, filename):
        """
        Find the files year_set
        """
        for i in range(len(filename)):
            if filename[i].isdigit():
                s_index = i
                break
        year_set = filename[i:]
        return year_set

    def get_attrs(self, filename):
        """
        From an AMWG output file, find that files attributes
        """
        filesplit = filename.split('_')
        set_id = self.get_set(filesplit[0])
        seasons = ['DJF', 'MAM', 'JJA', 'SON', 'ANN']
        if filesplit[1] in seasons:
            col = filesplit[1]
            row = filesplit[2]
            group = '_'.join(filesplit[3:])[:-4]
        else:
            s_index = 2
            for i in range(s_index, len(filesplit)):
                if filesplit[i] in seasons:
                    s_index = i
                    break
            col = filesplit[s_index]
            row = '_'.join(filesplit[1: s_index])
            group = '_'.join(filesplit[s_index:])[:-4]
        return set_id, group, row, col

    def generateIndex(self):
        """
        Generates the index.json for the DiagnosticViewer
        """
        self.event_list = push_event(
            self.event_list, 'Starting index generataion for AMWG diagnostic')
        contents = [s for s in os.listdir(self.config.get('run_directory')) if s.endswith('png')]
        dataset_name = self.config.get('dataset_name')
        index = OutputIndex('AMWG Diagnostic', version=dataset_name)

        pages = {}
        for item in contents:
            page, group, row, col = self.get_attrs(item)
            if not pages.get(page):
                pages[page] = {}
            if not pages.get(page).get(group):
                pages[page][group] = {}
            if not pages.get(page).get(group).get(row):
                pages[page][group][row] = {}
            if not pages.get(page).get(group).get(row).get(col):
                pages[page][group][row][col] = []
            pages[page][group][row][col].append(OutputFile(path=item, title=item))
            # pages[page][group][row][col].append(item)

        # with open('amwg_index.json', 'w') as f:
        #     json.dump(pages, f)

        for pi, page in pages.items():
            outpage = OutputPage(pi)
            for gi, group in page.items():
                outgroup = OutputGroup(gi)
                group_ind = len(outpage.groups)
                outpage.addGroup(outgroup)
                for ri, row in group.items():
                    tmp_row_list = []
                    for key, val in row.items():
                        tmp_row_list.append(val)
                    outrow = OutputRow(ri, tmp_row_list)
                    outpage.addRow(outrow, group_ind)
            index.addPage(outpage)
        try:
            index.toJSON(os.path.join(self.config.get('run_directory'), 'index.json'))
        except:
            self.event_list = push_event(self.event_list, 'Index generation failed')
        else:
            self.event_list = push_event(self.event_list, 'Index generataion complete')

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        returns 1 if the job is done, 0 otherwise
        """
        base = str(os.sep).join(self.config.get('test_path_diag').split('/')[:-1])
        year_set = 'year_set_{0}'.format(self.config.get('year_set'))
        web_dir = '{base}/{year_set}{casename}-obs'.format(
            base=base,
            year_set=year_set,
            casename=self.config.get('test_casename'))
        if os.path.exists(web_dir):
            out = Popen(['find', web_dir, '-type', 'f'], stdout=PIPE).communicate()
            contents = out[0].split('\n')
            return bool(len(contents) > 2300)
        else:
            return False

    def execute(self, batch='slurm'):
        """
        Perform the actual work
        """
        # First check if the job has already been completed
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'AMWG job already computed, skipping'
            self.event_list = push_event(self.event_list, message)
            return 0


        # setup the output directory
        run_dir = self.config.get('run_directory')
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)
        # render the csh script
        template_out = os.path.join(run_dir, 'amwg.csh')
        render(
            variables=self.config,
            input_path=self.config.get('template_path'),
            output_path=template_out)
        # get the list of climo files for this diagnostic
        file_list = get_climo_output_files(
            input_path=self.config.get('regrided_climo_path'),
            set_start_year=self.config.get('start_year'),
            set_end_year=self.config.get('end_year'))
        # create the directory of symlinks
        create_symlink_dir(
            src_dir=self.config.get('regrided_climo_path'),
            src_list=file_list,
            dst=self.config.get('test_path_climo'))

        for item in os.listdir(self.config.get('test_path_climo')):
            start_search = re.search(r'\_\d\d\d\d', item)
            s_index = start_search.start()
            os.rename(
                os.path.join(self.config.get('test_path_climo'), item),
                os.path.join(self.config.get('test_path_climo'), item[:s_index] + '_climo.nc'))
        # setup sbatch script
        expected_name = 'amwg_set_{year_set}_{start}_{end}_{uuid}'.format(
            year_set=self.config.get('year_set'),
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            uuid=self.uuid[:5])
        run_scripts_path = os.path.join(os.getcwd(), 'run_scripts')
        run_script = os.path.join(run_scripts_path, expected_name)

        if not os.path.exists(run_scripts_path):
            os.makedirs(run_scripts_path)

        self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
        self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

        cmd = ['csh', template_out]
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            slurm_prefix = ''.join(['#SBATCH {value}\n'.format(value=v)
                                    for k, v in self.slurm_args.iteritems()])
            batchfile.write(slurm_prefix)
            slurm_command = ' '.join(cmd)
            batchfile.write(slurm_command)

        prev_dir = os.getcwd()
        os.chdir(run_dir)
        slurm_cmd = ['sbatch', run_script, '--oversubscribe']
        started = False
        while not started:
            while True:
                try:
                    self.proc = Popen(slurm_cmd, stdout=PIPE, stderr=PIPE)
                    break
                except:
                    sleep(1)
            output, err = self.proc.communicate()
            started, job_id = check_slurm_job_submission(expected_name)
            if started:
                os.chdir(prev_dir)
                self.status = JobStatus.SUBMITTED
                self.job_id = job_id
                message = '## {type} id: {id} changed state to {state}'.format(
                    type=self.get_type(),
                    id=self.job_id,
                    state=self.status)
                logging.info(message)
                message = '{type} id: {id} changed state to {state}'.format(
                    type=self.get_type(),
                    id=self.job_id,
                    state=self.status)
                self.event_list = push_event(self.event_list, message)

        return self.job_id
