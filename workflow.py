#!/usr/bin/python
# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import argparse
import json
import sys
import os
import re
import threading
import atexit
import logging
import time

from math import floor
from shutil import copy
from shutil import rmtree
from shutil import move
from getpass import getpass
from time import sleep
from pprint import pformat
from subprocess import Popen, PIPE

from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from jobs.Ncclimo import Climo
from jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
from jobs.Publication import Publication
from jobs.PrimaryDiagnostic import PrimaryDiagnostic
from Monitor import Monitor

from util import print_debug
from util import print_message
from util import filename_to_file_list_key
from util import filename_to_year_set
from util import create_symlink_dir
from util import file_list_cmp
from util import thread_sleep
from util import format_debug

import pdb

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')
parser.add_argument('-d', '--debug', help='Run in debug mode', action='store_true')
parser.add_argument('-s', '--state', help='Path to a json state file')

logging.basicConfig(
    format='%(asctime)s:%(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    filename='workflow.log',
    filemode='w',
    level=logging.DEBUG
)

@atexit.register
def save_state():
    state_path = config.get('state_path')
    if not state_path:
        print_message('no state path')
        return
    print_message('saving execution state to {0}'.format(state_path))
    try:
        with open(state_path, 'w') as outfile:
            state = {
                'file_list': file_list,
                'job_sets': job_sets,
                'config': config
            }
            json.dump(state, outfile)
    except IOError as e:
        logging.error("Error saving state file")
        logging.error(format_debug(e))
        # print_debug(e)
        # print_message("Error saving state file")

def setup(parser):
    """
    Setup the config, file_list, and job_sets variables from either the config file passed from the parser
    of from the previously saved state
    """
    global debug
    global config
    global file_list
    global job_sets
    global from_saved_state

    args = parser.parse_args()
    if args.debug:
        debug = True
        print_message('Running in debug mode', 'ok')

    config = {}

    if args.state:
        state_path = os.path.abspath(args.state)
        if not os.path.exists(state_path):
            print_message('Ready to save state to {0}'.format(state_path))
            config['state_path'] = state_path
            from_saved_state = False
        else:
            print_message('Restoring from previous state')
            from_saved_state = True
            try:
                with open(args.state, 'r') as statefile:
                    state = json.load(statefile)
                if debug:
                    print_message('Loading from saved state {}'.format(args.state))
                    logging.info('Loading from saved state {}'.format(args.state))
                config = state.get('config')
                file_list = state.get('file_list')
                job_sets = state.get('job_sets')
                config['state_path'] = args.state
            except IOError as e:
                logging.error('Error loading from state file {}'.format(args.state))
                logging.error(format_debug(e))
                # print_debug(e)
                # print_message('Error loading state file')
                sys.exit(1)
            if debug:
                print_message('saved file_list: \n{}'.format(pformat(sorted(file_list, cmp=file_list_cmp))))
                print_message('saved job_sets: \n{}'.format(pformat(job_sets)))
                print_message('saved config: \n{}'.format(pformat(config)))

    if not from_saved_state:
        required_fields = [
            "output_path",
            "output_pattern",
            "data_cache_path",
            "compute_host",
            "compute_username",
            "compute_password",
            "compute_keyfile",
            "processing_host",
            "processing_username",
            "processing_password",
            "globus_username",
            "globus_password",
            "source_endpoint",
            "destination_endpoint",
            "source_path",
            "batch_system_type",
            "experiment",
        ]
        if args.config:
            try:
                with open(args.config, 'r') as conf:
                    config.update(json.load(conf))
            except Exception as e:
                logging.error('Unable to read config file, is it properly formatted json?')
                logging.error(format_debug(e))
                return -1

            for field in required_fields:
                """
                if field not in config or len(config[field]) == 0:
                    if field == 'compute_password' and config.get('monitor').get('compute_keyfile'):
                        continue
                    if field == 'compute_keyfile' and config.get('monitor').get('compute_password'):
                        continue
                    if 'password' in field:
                        config[field] = getpass("{0} not specified in config, please enter: ".format(field))
                    else:
                        config[field] = raw_input("{0} not specified in config, please enter: ".format(field))
                """
                if field == 'output_pattern':
                    patterns = ['YYYY-MM', 'YYYY-MM-DD']
                    output_pattern = config.get('global').get(field)
                    for p in patterns:
                        index = re.search(p, output_pattern)
                        if index:
                            start = index.start()
                            end = start + len(p)
                            date_pattern = output_pattern[start:end]
                            config['global']['output_pattern'] = config['global']['output_pattern'][:start] + p + '.nc'
                            config['global']['date_pattern'] = date_pattern
                    if not config.get('global').get('date_pattern'):
                        msg = 'Unable to parse output_pattern {}, exiting'.format(output_pattern)
                        print_message(msg)
                        logging.error(msg)
                        sys.exit(1)
        else:
            parser.print_help()
            print 'No configuration file given, initiating manual setup'
            config = {}
            for field in required_fields:
                if 'password' in field:
                    config[field] = getpass('{0}: '.format(field))
                else:
                    config[field] = raw_input('{0}: '.format(field))
    return config

def add_jobs(job_set):
    global job_sets

    # each required job is a key, the value is if its in the job list already or not
    # this is here incase the jobs have already been added
    required_jobs = {
        'climo': False,
        'diagnostic': False,
        'upload_diagnostic_output': False,
    }
    for job in job_set['jobs']:
        if not required_jobs[job.get_type()]:
            required_jobs[job.get_type()] = True

    # first initialize the climo job
    if not required_jobs['climo']:
        climo_output_dir = os.path.join(config.get('global').get('output_path'))
        if not os.path.exists(climo_output_dir):
            if debug:
                logging.info("Creating climotology output directory {}".format(climo_output_dir))
            os.makedirs(climo_output_dir)
        regrid_output_dir = os.path.join(config.get('global').get('output_path'), 'regrid')
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)

        # Setup variables for climo job
        # climo_start_year = config.get('global').get('simulation_start_year') + ((year_set - 1) * config.get('global').get('set_frequency'))
        # climo_end_year = climo_start_year + config.get('global').get('set_frequency') - 1
        climo_start_year = job_set.get('set_start_year')
        climo_end_year = job_set.get('set_end_year')
        year_set = job_set.get('year_set')

        # create a temp directory, and fill it with symlinks to the actual data
        key_list = []
        for year in range(climo_start_year, climo_end_year + 1):
            for month in range(13):
                key_list.append('{0}-{1}'.format(year, month))

        climo_file_list = [file_name_list.get(x) for x in key_list]
        climo_temp_dir = os.path.join(os.getcwd(), 'tmp', 'climo', 'year_set_' + str(year_set))
        create_symlink_dir(
            src_dir=config.get('global').get('data_cache_path'),
            src_list=climo_file_list,
            dst=climo_temp_dir
        )
        # create the configuration object for the climo job
        climo_config = {
            'start_year': climo_start_year,
            'end_year': climo_end_year,
            'caseId': config.get('global').get('experiment'),
            'annual_mode': 'sdd',
            'regrid_map_path': config.get('ncclimo').get('regrid_map_path'),
            'input_directory': climo_temp_dir,
            'climo_output_directory': climo_output_dir,
            'regrid_output_directory': regrid_output_dir,
            'yearset': year_set
        }
        climo = Climo(climo_config)
        logging.info('Adding Ncclimo job to the job list with config: %s', str(climo_config))
        job_set['jobs'].append(climo)

    # init the diagnostic job
    if not required_jobs['diagnostic']:
        # create the output directory
        diag_output_path = os.path.join(config.get('global').get('output_path'), 'diagnostics', 'year_set_' + str(year_set))
        if not os.path.exists(diag_output_path):
            os.makedirs(diag_output_path)

        # create a temp directory full of just symlinks to the regridded output we need for this diagnostic job
        diag_temp_dir = os.path.join(os.getcwd(), 'tmp', 'diag', 'year_set_' + str(year_set))
        if not os.path.exists(diag_temp_dir):
            os.makedirs(diag_temp_dir)
        create_symlink_dir(
            src_dir=regrid_output_dir,
            src_list=climo_file_list,
            dst=diag_temp_dir
        )
        # create the configuration object for the diag job
        diag_config = {
            '--model': diag_temp_dir,
            '--obs': config.get('meta_diags').get('obs_for_diagnostics_path'),
            '--outputdir': diag_output_path,
            '--package': 'amwg',
            '--set': '5',
            '--archive': 'False',
            'yearset': year_set,
            'depends_on': [len(job_set['jobs']) - 1] # set the diag job to wait for the climo job to finish befor running
        }
        diag = Diagnostic(diag_config)
        logging.info('Adding Diagnostic job to the job list with config: %s', str(diag_config))
        job_set['jobs'].append(diag)

        """
        coupled_project_dir = os.path.join(os.getcwd(), 'coupled_daigs', str(job_set.get('year_set')))
        if not os.path.exists(coupled_project_dir):
            os.makedirs(coupled_project_dir)
        coupled_diag_config = {
            'coupled_project_dir': coupled_project_dir,
            'test_casename': config.get('global').get('experiment'),
            'test_native_res': config.get('primary_diags').get('test_native_res'),
            'test_archive_dir': diag_temp_dir,
            'test_begin_yr_climo': job_set.get('set_start_year'),
            'test_end_yr_climo': job_set.get('set_end_year'),
            'test_begin_yr_ts': job_set.get('set_start_year'),
            'test_end_yr_ts': job_set.get('set_end_year'),
            'ref_case': config.get('primary_diags').get('obs'),
            'ref_archive_dir': config.get('meta_diags').get('obs_for_diagnostics_path'),
            'mpas_meshfile': config.get('primary_diags').get('mpas_meshfile'),
            'mpas_remapfile': config.get('primary_diags').get('mpas_remapfile'),
            'pop_remapfile': config.get('primary_diags').get('pop_remapfile'),
            'remap_files_dir': config.get('primary_diags').get('remap_files_dir'),
            'GPCP_regrid_wgt_file': config.get('primary_diags').get('GPCP_regrid_wgt_file'),
            'CERES_EBAF_regrid_wgt_file': config.get('primary_diags').get('CERES_EBAF_regrid_wgt_file'),
            'ERS_regrid_wgt_file': config.get('primary_diags').get('ERS_regrid_wgt_file'),
            'coupled_home_directory': '/export/baldwin32/projects/PreAndPostProcessing/coupled_diags',
            'coupled_template_path': os.path.join(os.getcwd(), 'resources', 'run_AIMS_template.csh'),
            'rendered_output_path': os.path.join(coupled_project_dir, 'run_AIMS.csh'),
            'obs_ocndir': config.get('primary_diags').get('obs_ocndir'),
            'obs_seaicedir': config.get('primary_diags').get('obs_seaicedir'),
            'obs_sstdir': config.get('primary_diags').get('obs_sstdir'),
            'obs_iceareaNH': config.get('primary_diags').get('obs_iceareaNH'),
            'obs_iceareaSH': config.get('primary_diags').get('obs_iceareaSH'),
            'obs_icevolNH': config.get('primary_diags').get('obs_icevolNH'),
            'obs_icevolSH': 'None',
            'depends_on': [len(job_set['jobs']) - 2],
            'yr_offset': config.get('primary_diags').get('yr_offset')
        }
        job = PrimaryDiagnostic(coupled_diag_config)
        print_message(str(job))
        job.execute()
        sys.exit(1)
        """
    # init the upload job
    if not required_jobs['upload_diagnostic_output']:
        upload_config = {
            'path_to_diagnostic': os.path.join(diag_output_path, 'amwg'),
            'username': config.get('upload_diagnostic').get('diag_viewer_username'),
            'password': config.get('upload_diagnostic').get('diag_viewer_password'),
            'server': config.get('upload_diagnostic').get('diag_viewer_server'),
            'depends_on': [len(job_set['jobs']) - 1] # set the upload job to wait for the diag job to finish
        }
        upload = UploadDiagnosticOutput(upload_config)
        logging.info('Adding Upload job to the job list with config: %s', str(upload_config))
        job_set['jobs'].append(upload)


    # finally init the publication job
    # if not required_jobs['publication']:
    #     publication_config = {
    #         'place': 'holder',
    #         'yearset': year_set,
    #         'depends_on': [len(job_set['jobs']) - 2] # wait for the diagnostic job to finish, but not the upload job
    #     }
    #     publish = Publication(publication_config)
    #     job_set['jobs'].append(publish)
    #     print_message('adding publication job')
    return job_set



def monitor_check(monitor):
    """
    Check the remote directory for new files that match the given pattern,
    if there are any new files, create new transfer jobs. If they're in a new job_set,
    spawn the jobs for that set.

    inputs: 
        monitor: a monitor object setup with a remote directory and an SSH session
    """
    global job_sets
    global active_transfers

    # if there are already three or more transfers in progress
    # hold off on starting any new ones until they complete
    if active_transfers > 1:
        return
    monitor.check()
    new_files = monitor.get_new_files()
    checked_new_files = []
    for f in new_files:
        key = filename_to_file_list_key(f, config.get('global').get('output_pattern'), config.get('date_pattern'))
        status = file_list.get(key)
        if status and status != 'data ready':
            checked_new_files.append(f)

    # if there are any new files
    if  not checked_new_files:
        if debug:
            print_message('No new files found', 'ok')
        return

    if debug:
        print_message('Found new files:\n  {}'.format(
            pformat(checked_new_files, indent=4)), 'ok')
    # find which year set the data belongs to
    for f in new_files:
        for freq in config.get('global').get('global').get('set_frequency'):
            year_set = filename_to_year_set(f, config.get('global').get('output_pattern'), freq)
            for job_set in job_sets:
                if job_set.get('year_set') == year_set:
                    # if before we got here, the job_set didnt have any data, now that we have some data
                    # we create the processing jobs and add them to the job_sets list of jobs
                    if job_set.get('status') == 'no data':
                        # Change job_sets' state
                        job_set['status'] = 'data in transit'

                        # Spawn jobs for that yearset
                        job_set = add_jobs(job_set)

    # construct list of files to transfer
    f_list = ['{path}/{file}'.format(path=config.get('monitor').get('source_path'), file=f)  for f in checked_new_files]

    transfer_config = {
        'file_list': f_list,
        'globus_username': config.get('monitor').get('globus_username'),
        'globus_password': config.get('monitor').get('globus_password'),
        'source_username': config.get('monitor').get('compute_username'),
        'source_password': config.get('monitor').get('compute_password'),
        'destination_username': config.get('monitor').get('processing_username'),
        'destination_password': config.get('monitor').get('processing_password'),
        'source_endpoint': config.get('monitor').get('source_endpoint'),
        'destination_endpoint': config.get('monitor').get('destination_endpoint'),
        'source_path': config.get('monitor').get('source_path'),
        'destination_path': config.get('global').get('data_cache_path') + '/',
        'recursive': 'False',
        'final_destination_path': config.get('global').get('data_cache_path'),
        'pattern': config.get('global').get('output_pattern')
    }
    logging.info('Starting transfer with config: %s', pformat(transfer_config))
    transfer = Transfer(transfer_config)
    thread = threading.Thread(target=handle_transfer, args=(transfer, checked_new_files, thread_kill_event))
    thread_list.append(thread)
    thread.start()
    active_transfers += 1


def handle_transfer(transfer_job, f_list, event):
    global active_transfers
    """
    Wrapper around the transfer.execute() method, ment to be run inside a thread

    inputs:
        transfer_job: the transfer job to execute and monitor
        f_list: the list of files being transfered
        event: a thread event to handle shutting down from keyboard exception, not used in this case
            but it needs to be there for any threads handlers
    """
    # start the transfer job
    transfer_job.execute()
    # the transfer is complete, so we can decrement the active_transfers counter
    active_transfers -= 1

    if transfer_job.status != 'COMPLETED':
        logging.error('Failed to complete transfer job\n  %s', pformat(str(transfer_job)))
        return
        # print_message('Faild to transfer files correctly')

    # update the file_list all the files that were transferred
    for f in f_list:
        list_key = filename_to_file_list_key(f, config.get('global').get('output_pattern'))
        file_list[list_key] = 'data ready'
        file_name_list[list_key] = f
    if debug:
        logging.info('trasfer of files %s completed', pformat(f_list))
        logging.info('file_list status: %s', pformat(sorted(file_list, cmp=file_list_cmp)))
        print_message('file_list status: ', 'ok')
        for key in sorted(file_list, cmp=file_list_cmp):
            print_message('{key}: {val}'.format(key=key, val=file_list[key]), 'ok')

def check_year_sets():
    """
    Checks the file_list, and sets the year_set status to ready if all the files are in place,
    otherwise, checks if there is partial data, or zero data
    """
    global job_sets
    sim_start_year = config.get('global').get('simulation_start_year')
    sim_end_year = config.get('global').get('simulation_end_year')
    number_of_sim_years = sim_end_year - (sim_start_year - 1)

    incomplete_job_sets = [s for s in job_sets if s['status'] != 'COMPLETED' and s['status'] != 'RUNNING']
    for job_set in incomplete_job_sets:

        start_year = job_set.get('set_start_year')
        end_year = job_set.get('set_end_year')

        non_zero_data = False
        data_ready = True
        for i in range(start_year, end_year + 1):
            for j in range(1, 13):
                file_key = '{0}-{1}'.format(i, j)
                status = file_list[file_key]

                if status == 'no data':
                    data_ready = False
                elif status == 'data ready':
                    non_zero_data = True
        if data_ready:
            job_set['status'] = 'data ready'
            status = 'data ready'
            job_set = add_jobs(job_set)
            continue
        if not data_ready and non_zero_data:
            job_set['status'] = 'partial data'
            continue
        if not data_ready and not non_zero_data:
            job_set['status'] = 'no data'

    if debug:
        for job_set in job_sets:
            start_year = job_set.get('set_start_year')
            end_year = job_set.get('set_end_year')
            print_message('year_set: {0}: {1}'.format(job_set.get('year_set'), job_set.get('status')), 'ok')
            for i in range(start_year, end_year + 1):
                for j in range(1, 13):
                    file_key = '{0}-{1}'.format(i, j)
                    status = file_list[file_key]
                    print_message('  {key}: {value}'.format(key=file_key, value=status), 'ok')

def check_for_inplace_data():
    """
    Checks the data cache for any files that might already be in place,
    updates the file_list and job_sets accordingly
    """
    global file_list
    global file_name_list
    global all_data
    global job_sets

    cache_path = config.get('global').get('data_cache_path')
    date_pattern = config.get('global').get('date_pattern')
    output_pattern = config.get('global').get('output_pattern')

    print 'date_pattern: ' + date_pattern


    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
        return

    for climo_file in os.listdir(cache_path):
        file_key = filename_to_file_list_key(
            filename=climo_file,
            output_pattern=output_pattern,
            date_pattern=date_pattern)
        file_list[file_key] = 'data ready'
        file_name_list[file_key] = climo_file

    all_data = True
    for key in file_list:
        if file_list[key] != 'data ready':
            all_data = False
            break

def start_ready_job_sets():
    """
    Iterates over the job sets, and starts ready jobs
    """
    global thread_list
    # iterate over the job_sets
    if debug:
        print_message('== Checking for ready jobs ==', 'ok')
    for job_set in job_sets:
        # if the job state is ready, but hasnt started yet
        if debug:
            msg = 'year_set: {0} status: {1}'.format(job_set.get('year_set'), job_set.get('status'))
            print_message(msg, 'ok')
            logging.info(msg)
        if job_set['status'] == 'data ready' or job_set['status'] == 'RUNNING':
            for job in job_set['jobs']:
                # if the job is a climo, and it hasnt been started yet, start it
                if debug:
                    msg = '    job type: {0}, job_status: {1}, job_id: {2}'.format(
                        job.get_type(),
                        job.status,
                        job.job_id)
                    print_message(msg, 'ok')
                    logging.info(msg)

                if job.get_type() == 'climo' and job.status == 'valid':
                    job_set['status'] = 'RUNNING'
                    job_id = job.execute(batch=True)
                    job.set_status('starting')
                    logging.info('Starting Ncclimo for year set %s', job_set['year_set'])
                    thread = threading.Thread(target=monitor_job, args=(job_id, job, job_set, thread_kill_event))
                    thread_list.append(thread)
                    thread.start()
                    return
                # if the job isnt a climo, and the job that it depends on is done, start it
                elif job.get_type() != 'climo' and job.status == 'valid':
                    ready = True
                    for dependancy in job.depends_on:
                        if job_set['jobs'][dependancy].status != 'COMPLETED':
                            ready = False
                            break
                    if ready:
                        job_id = job.execute(batch=True)
                        job.set_status('starting')
                        logging.info('Starting %s job for year_set %s', job.get_type(), job_set['year_set'])
                        thread = threading.Thread(target=monitor_job, args=(job_id, job, job_set, thread_kill_event))
                        thread_list.append(thread)
                        thread.start()
                        return
                elif job.status == 'invalid':
                    logging.error('Job in invalid state: \n%s', pformat(str(job)))
                    print_message('===== INVALID JOB =====\n{}'.format(str(job)))

def monitor_job(job_id, job, job_set, event=None):
    """
    Monitor the slurm job, and update the status to 'complete' when it finishes
    This function should only be called from within a thread
    """
    def handle_slurm():
        """
        handle interfacing with the SLURM controller
        Checkes the SLURM queue status and changes the job status appropriately
        """
        count = 0
        valid = False
        while count < 10 and not valid:
            cmd = ['scontrol', 'show', 'job', str(job_id)]
            out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
            # sometimes there will be a communication error with the SLURM controller
            # in which case the controller returns 'error: some message'
            if 'error' in out or len(out) == 0:
                logging.info('Error communication with SLURM controller, attempt number %s', count)
                valid = False
                count += 1
                if thread_sleep(5, event):
                    return
            else:
                valid = True

        if not valid:
            # if the controller errors 5 times in a row, its probably an unrecoverable error
            logging.info('SLURM controller not responding')
            return None

        # loop through the scontrol output looking for the JobState field
        job_status = None
        run_time = None
        for line in out.split('\n'):
            for word in line.split():
                if 'JobState' in word:
                    index = word.find('=')
                    job_status = word[index + 1:]
                    continue
                if 'RunTime' in word:
                    index = word.find('=') + 1
                    run_time = word[index:]
                    break
            if job_status and run_time:
                break

        # if debug:
        #     msg = '{0} status: {1}'.format(job_id, job_status)
        #     print_message(msg, 'ok')
        #     logging.info(msg)
        if not job_status:
            if debug:
                print_message('Error parsing job output\n{0}'.format(out))
            logging.warning('Unable to parse scontrol output: %s', out)

        return job_status, run_time

    def handle_pbs():
        print 'dealing with pbs'
        cmd = ['qstat']
        out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
        # do some work
        job_status = 'DANGER WILL ROBENSON'
        return job_status

    def handle_none():
        print 'you should really be running this with slurm'
        return 'Zug zug'

    error_count = 0
    while True:
        # this check is here in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        batch_system = config.get('global').get('batch_system_type')
        if batch_system == 'slurm':
            status, run_time = handle_slurm()
        elif batch_system == 'pbs':
            status = handle_pbs()
        elif batch_system == 'none':
            cmd = ['']
            status = handle_none()
            # TODO: figure out how to get this working

        if not status:
            if error_count <= 10:
                logging.error('Unable to communicate to controller after 10 attempts')
                logging.error('Setting %s job with job_id %s to status error', job.get_type(), job_id)
                job.status = 'error'
            error_count += 1
            if thread_sleep(5, event):
                return
            continue

        if job.status != status:
            if debug:
                if status != 'error':
                    print_message('Setting job status: {0}'.format(status), 'ok')
                else:
                    print_message('Setting job status: {0}'.format(status))
            logging.info('Setting %s job with job_id %s to status %s', job.get_type(), job_id, status)
            job.status = status
            if status == 'RUNNING':
                job_set['status'] = 'RUNNING'

        # if the job is done, or there has been an error, exit
        if status == 'COMPLETED':
            logging.info('%s job  with job_id %s completed after %s', job.get_type(), job_id, run_time)
            job_set_done = True
            for job in job_set['jobs']:
                if job.status != 'COMPLETED':
                    job_set_done = False
                    break

            if job_set_done:
                job_set['status'] = 'COMPLETED'
            return
        if status == 'error':
            logging.info('%s job  with job_id %s ERRORED after %s', job.get_type(), job_id, run_time)
            return
        # wait for 10 seconds, or if the kill_thread event has been set, exit
        if thread_sleep(10, event):
            return

def is_all_done():
    """
    Check if all job_sets are done, and all processing has been completed
    """
    for job_set in job_sets:
        print_message('job_set {0} status {1}'.format(job_set['year_set'], job_set['status']))
        if job_set['status'] != 'COMPLETED':
            return False
    return True

def cleanup():
    """
    Clean up temp files created during the run
    """
    logging.info('Cleaning up temp directories')
    try:
        cwd = os.getcwd()
        tmp_path = os.path.join(cwd, 'tmp')
        rmtree(tmp_path)
    except Exception as e:
        logging.error(format_debug(e))
        print_message('Error removing temp directories')

    try:
        archive_path = os.path.join(cwd, 'run_script_archive', time.strftime("%d-%m-%Y"))
        if not os.path.exists(archive_path):
            os.makedirs(archive_path)
        run_script_path = os.path.join(cwd, 'run_scripts')
        move(run_script_path, archive_path)
    except Exception as e:
        logging.error(format_debug(e))
        print_message('Error archiving run_scripts directory')


if __name__ == "__main__":

    # A list of all the expected files
    file_list = {}
    # A list of all the file names
    file_name_list = {}
    # Describes the state of each job jet
    job_sets = {}
    # The master configuration object
    config = {}
    # A list of all the threads
    thread_list = []
    # An event to kill the threads on terminal exception
    thread_kill_event = threading.Event()
    debug = False
    from_saved_state = False
    # The number of active globus transfers
    active_transfers = 0
    # A flag to tell if we have all the data locally
    all_data = False
    # Read in parameters from config
    config = setup(parser)

    if config == -1:
        print "Error in setup, exiting"
        sys.exit(1)

    # compute number of expected year sets
    sim_start_year = int(config.get('global').get('simulation_start_year'))
    sim_end_year = int(config.get('global').get('simulation_end_year'))
    number_of_sim_years = sim_end_year - (sim_start_year - 1)
    if not from_saved_state:
        job_sets = []
    for freq in config.get('global').get('set_frequency'):
        freq = int(freq)
        year_set = number_of_sim_years / freq
        if debug:
            print_message(
                ' set_frequency: {freq},\n     number of year_sets: {ys}\n'.format(
                    ys=year_set,
                    freq=freq),
                'ok')

        if not from_saved_state:
            # initialize the job_sets dict
            for i in range(1, year_set + 1):
                set_start_year = sim_start_year + ((i - 1) * freq)
                set_end_year = set_start_year + freq - 1
                job_set = {
                    'status': 'no data',
                    'year_set': len(job_sets) + 1,
                    'jobs': [],
                    'set_start_year': set_start_year,
                    'set_end_year': set_end_year
                }
                job_sets.append(job_set)

    for job_set in job_sets:
        print_message(job_set)

    # initialize the file_list
    if debug:
        print_message('initializing file_list with {num_years} years'.format(
            num_years=number_of_sim_years))

    for year in range(1, number_of_sim_years + 1):
        for month in range(1, 13):
            key = str(year) + '-' + str(month)
            file_list[key] = 'no data'
            file_name_list[key] = ''

    # Check for any data already on the System
    check_for_inplace_data()
    check_year_sets()
    # if debug:
    #     for s in job_sets:
    #         print_message('year_set {0}:'.format(s.get('year_set')), 'ok')
    #         for job in s['jobs']:
    #             print_message('    {}'.format(str(job)))
    # if debug:
    #     print_message('printing year sets', 'ok')
    #     for key in job_sets:
    #         print_message(str(key['year_set']) + ': ' + key['status'], 'ok')
    #     print_message('printing file list', 'ok')
    #     for key in sorted(file_list, cmp=file_list_cmp):
    #         print_message(key + ': ' + file_list[key], 'ok')

    #     if all_data:
    #         print_message('All data is local, disabling remote monitor')
    #     else:
    #         print_message('Data is missing, enabling remote monitor')

    # if all the data is local, dont start the monitor
    if not all_data:
        monitor_config = {
            'remote_host': config.get('monitor').get('compute_host'),
            'remote_dir': config.get('monitor').get('source_path'),
            'username': config.get('monitor').get('compute_username'),
            'pattern': config.get('global').get('output_pattern')
        }
        if config.get('monitor').get('compute_password'):
            monitor_config['password'] = config.get('monitor').get('compute_password')
        if config.get('monitor').get('compute_keyfile'):
            monitor_config['keyfile'] = config.get('monitor').get('compute_keyfile')
        else:
            print_message('No password or keyfile path given for compute resource, please add to your config and try again')
            sys.exit(1)
        monitor = Monitor(monitor_config)

        print_message('attempting connection to {}'.format(config.get('monitor').get('compute_host')), 'ok')
        if monitor.connect() == 0:
            print_message('connected', 'ok')
        else:
            print_message('unable to connect, exiting')
            sys.exit(1)

    # Main loop
    try:
        while True:
            # Setup remote monitoring system
            if not all_data:
                monitor_check(monitor)
            # Check if a year_set is ready to run
            check_year_sets()
            start_ready_job_sets()
            check_for_inplace_data()
            if is_all_done():
                # cleanup()
                print_message('All processing complete')
                logging.info("All processes complete, exiting")
                sys.exit(0)
            sleep(10)
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        if config.get('state_path'):
            save_state()
        print_message('cleaning up threads', 'ok')
        for t in thread_list:
            thread_kill_event.set()
            t.join()
