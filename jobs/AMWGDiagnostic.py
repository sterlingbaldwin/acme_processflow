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
from datetime import datetime
from shutil import copyfile
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
from lib.util import render
from lib.util import get_climo_output_files
from lib.events import Event_list

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

        """
        self.event_list = event_list
        self.status = JobStatus.INVALID
        self.start_time = None
        self.end_time = None
        self.inputs = {
            'web_dir': '',
            'host_url': '',
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
            'run_scripts_path': '',
            'experiment': ''
        }
        self.type = 'amwg'
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
    
    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id,
        })

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

    def generateIndex(self, output_dir):
        """
        Generates the index.json for the DiagnosticViewer
        """
        return
        self.event_list.push(message='Starting index generataion for AMWG diagnostic')
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
            self.event_list.push(message='Index generation failed')
        else:
            self.event_list.push(message='Index generataion complete')

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        returns 1 if the job is done, 0 otherwise
        """
        base = str(os.sep).join(
            self.config.get('test_path_diag').split(os.sep)[:-1])
        year_set = 'year_set_{0}'.format(
            self.config.get('year_set'))
        web_dir = '{base}/{start:04d}-{end:04d}{casename}-obs'.format(
            base=base,
            start=self.config.get('start_year'),
            end=self.config.get('end_year'),
            casename=self.config.get('test_casename'))
        if os.path.exists(web_dir):
            all_files = []
            for path, dirs, files in os.walk(web_dir):
                all_files += files
            return bool(len(all_files) > 1000)
        else:
            return False

    def execute(self, batch='slurm', debug=False):
        """
        Perform the actual work
        """
        # First check if the job has already been completed
        if self.postvalidate():
            self.status = JobStatus.COMPLETED
            message = 'AMWG job already computed, skipping'
            self.event_list.push(message=message)
            logging.info(message)
            return 0
        else:
            self.status = JobStatus.PENDING

        self.start_time = datetime.now()
        # setup the output directory
        run_dir = self.config.get('run_directory')
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)
        # render the csh script
        template_out = os.path.join(
            run_dir,
            'amwg.csh')
        render(
            variables=self.config,
            input_path=self.config.get('template_path'),
            output_path=template_out)
        run_script_template_out = os.path.join(
            self.config.get('run_scripts_path'), 'amwg_{0}-{1}.csh'.format(
                self.config.get('start_year'),
                self.config.get('end_year')))
        copyfile(
            src=template_out,
            dst=run_script_template_out)
        if debug:
            print 'run script rendering complete'
        # get the list of climo files for this diagnostic
        file_list = get_climo_output_files(
            input_path=self.config.get('regrided_climo_path'),
            set_start_year=self.config.get('start_year'),
            set_end_year=self.config.get('end_year'))
        if debug:
            print 'gathering climo files from {}'.format(self.config.get('regrided_climo_path'))
            print pformat(file_list)
        # create the directory of symlinks
        create_symlink_dir(
            src_dir=self.config.get('regrided_climo_path'),
            src_list=file_list,
            dst=self.config.get('test_path_climo'))
        if debug:
            print 'symlinks created'
        for item in os.listdir(self.config.get('test_path_climo')):
            start_search = re.search(r'_\d\d\d\d\d\d_', item)
            s_index = start_search.start()
            os.rename(
                os.path.join(self.config.get('test_path_climo'), item),
                os.path.join(self.config.get('test_path_climo'), item[:s_index] + '_climo.nc'))
        # setup sbatch script
        expected_name = 'amwg_set_{year_set}_{start}_{end}_{uuid}'.format(
            year_set=self.config.get('year_set'),
            start='{:04d}'.format(self.config.get('start_year')),
            end='{:04d}'.format(self.config.get('end_year')),
            uuid=self.uuid[:5])
        run_script = os.path.join(self.config.get('run_scripts_path'), expected_name)
        if debug:
            print 'run_script: {}'.format(run_script)
        self.slurm_args['error_file'] = '-e {error_file}'.format(error_file=run_script + '.err')
        self.slurm_args['output_file'] = '-o {output_file}'.format(output_file=run_script + '.out')

        cmd = ['csh', template_out]
        with open(run_script, 'w') as batchfile:
            batchfile.write('#!/bin/bash\n')
            slurm_args_str = ['#SBATCH {value}\n'.format(value=v) for k, v in self.slurm_args.items()]
            slurm_prefix = ''.join(slurm_args_str)
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
                except:
                    sleep(1)
                else:
                    break
            output, err = self.proc.communicate()
            started, job_id = check_slurm_job_submission(expected_name)
            if started:
                os.chdir(prev_dir)
                self.status = JobStatus.SUBMITTED
                self.job_id = job_id
                message = '{type} id: {id} changed state to {state}'.format(
                    type=self.get_type(),
                    id=self.job_id,
                    state=self.status)
                logging.info(message)
                self.event_list.push(message=message)

        return self.job_id
