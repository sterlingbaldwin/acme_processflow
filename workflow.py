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

from shutil import rmtree
from shutil import move
from getpass import getpass
from time import sleep
from pprint import pformat
from subprocess import Popen

from acme_workflow.jobs.Diagnostic import Diagnostic
from acme_workflow.jobs.Transfer import Transfer
from acme_workflow.jobs.Ncclimo import Climo
from acme_workflow.jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
from acme_workflow.jobs.Publication import Publication
from acme_workflow.jobs.PrimaryDiagnostic import PrimaryDiagnostic
from acme_workflow.jobs.JobStatus import JobStatus
from acme_workflow.lib.Monitor import Monitor
from acme_workflow.lib.YearSet import YearSet
from acme_workflow.lib.YearSet import SetStatus

from acme_workflow.lib.util import *

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')
parser.add_argument('-d', '--debug', help='Run in debug mode', action='store_true')
parser.add_argument('-s', '--state', help='Path to a json state file')

logging.basicConfig(
    format='%(asctime)s:%(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    filename='workflow.log',
    filemode='w',
    level=logging.DEBUG)

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
                    print_message('Loading from saved state {}'.format(args.state), 'ok')
                    logging.info('Loading from saved state {}'.format(args.state))
                    message = "## year_set {set} status change to {status}".format(set=year_set.set_number, status=year_set.status)
                    logging.info(message)
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
                msg = 'Unable to read config file, is it properly formatted json?'
                print_message(msg)
                logging.error(msg)
                logging.error(format_debug(e))
                message = "## year_set {set} status change to {status}".format(set=year_set.set_number, status=year_set.status)
                logging.info(message)
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
                        message = "## year_set {set} status change to {status}".format(set=year_set.set_number, status=year_set.status)
                        logging.error(message)
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


def path_exists(config_items):
    for k, v in config_items.items():
        if type(v) != dict:
            continue
        for j, m in v.items():
            if j != 'output_pattern':
                if str(m).endswith('.nc'):
                    if not os.path.exists(m):
                        print "File {key}: {value} does not exist, exiting.".format(key=j, value=m)
                        sys.exit(1)


def add_jobs(year_set):
    """
    Initializes and adds all the jobs to the year_set
    """
    # each required job is a key, the value is if its in the job list already or not
    # this is here in case the jobs have already been added
    required_jobs = {
        'climo': False,
        'diagnostic': False,
        'upload_diagnostic_output': False,
    }
    for job in year_set.jobs:
        if not required_jobs[job.get_type()]:
            required_jobs[job.get_type()] = True

    year_set_str = 'year_set_' + str(year_set.set_number)
    # first initialize the climo job
    if not required_jobs['climo']:
        climo_output_dir = config.get('global').get('output_path')
        if not os.path.exists(climo_output_dir):
            if debug:
                msg = "Creating climotology output directory {}".format(climo_output_dir)
                logging.info(msg)
            os.makedirs(climo_output_dir)

        regrid_output_dir = os.path.join(climo_output_dir, 'regrid')
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)

        # Setup variables for climo job
        climo_start_year = year_set.set_start_year
        climo_end_year = year_set.set_end_year

        # create a temp directory, and fill it with symlinks to the actual data
        key_list = []
        for year in range(climo_start_year, climo_end_year + 1):
            for month in range(13):
                key_list.append('{0}-{1}'.format(year, month))

        climo_file_list = [file_name_list.get(x) for x in key_list]
        climo_temp_dir = os.path.join(os.getcwd(), 'tmp', 'climo', year_set_str)
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
            'year_set': year_set.set_number,
            'ncclimo_path': config.get('ncclimo').get('ncclimo_path')
        }
        climo = Climo(climo_config)
        msg = 'Adding Ncclimo job to the job list: {}'.format(str(climo))
        logging.info(msg)
        year_set.add_job(climo)

    # init the diagnostic job
    if not required_jobs['diagnostic']:
        # create the output directory
        output_path = config.get('global').get('output_path')

        diag_output_path = os.path.join(output_path, 'diagnostics', year_set_str)
        if not os.path.exists(diag_output_path):
            os.makedirs(diag_output_path)

        # create a temp directory full of just symlinks to the regridded output we need for this diagnostic job
        diag_temp_dir = os.path.join(os.getcwd(), 'tmp', 'diag', year_set_str)
        if not os.path.exists(diag_temp_dir):
            os.makedirs(diag_temp_dir)

        diag_config = {
            '--model': diag_temp_dir,
            '--obs': config.get('meta_diags').get('obs_for_diagnostics_path'),
            '--outputdir': diag_output_path,
            '--package': 'amwg',
            '--set': '5',
            '--archive': 'False',
            'year_set': year_set.set_number,
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'depends_on': [len(year_set.jobs) - 1], # set the diag job to wait for the climo job to finish befor running
            'regrid_path': regrid_output_dir,
            'diag_temp_dir': diag_temp_dir
        }
        diag = Diagnostic(diag_config)
        msg = 'Adding Diagnostic to the job list: {}'.format(str(diag))
        logging.info(msg)
        year_set.add_job(diag)

        # coupled_project_dir = os.path.join(os.getcwd(), 'coupled_daigs', str(year_set.set_number))
        # if not os.path.exists(coupled_project_dir):
        #     os.makedirs(coupled_project_dir)
        # g_config = config.get('global')
        # p_config = config.get('primary_diags')
        # coupled_diag_config = {
        #     'coupled_project_dir': coupled_project_dir,
        #     'test_casename': g_config.get('experiment'),
        #     'test_native_res': p_config.get('test_native_res'),
        #     'test_archive_dir': diag_temp_dir,
        #     'test_begin_yr_climo': year_set.set_start_year,
        #     'test_end_yr_climo': year_set.set_end_year,
        #     'test_begin_yr_ts': year_set.set_start_year,
        #     'test_end_yr_ts': year_set.set_end_year,
        #     'ref_case': p_config.get('obs'),
        #     'ref_archive_dir': config.get('meta_diags').get('obs_for_diagnostics_path'),
        #     'mpas_meshfile': p_config.get('mpas_meshfile'),
        #     'mpas_remapfile': p_config.get('mpas_remapfile'),
        #     'pop_remapfile': p_config.get('pop_remapfile'),
        #     'remap_files_dir': p_config.get('remap_files_dir'),
        #     'GPCP_regrid_wgt_file': p_config.get('GPCP_regrid_wgt_file'),
        #     'CERES_EBAF_regrid_wgt_file': p_config.get('CERES_EBAF_regrid_wgt_file'),
        #     'ERS_regrid_wgt_file': p_config.get('ERS_regrid_wgt_file'),
        #     'coupled_home_directory': p_config.get('coupled_home_directory'),
        #     'coupled_template_path': os.path.join(os.getcwd(), 'resources', 'run_AIMS_template.csh'),
        #     'rendered_output_path': os.path.join(coupled_project_dir, 'run_AIMS.csh'),
        #     'obs_ocndir': p_config.get('obs_ocndir'),
        #     'obs_seaicedir': p_config.get('obs_seaicedir'),
        #     'obs_sstdir': p_config.get('obs_sstdir'),
        #     'obs_iceareaNH': p_config.get('obs_iceareaNH'),
        #     'obs_iceareaSH': p_config.get('obs_iceareaSH'),
        #     'obs_icevolNH': p_config.get('obs_icevolNH'),
        #     'obs_icevolSH': 'None',
        #     'depends_on': [len(year_set.jobs) - 2],
        #     'yr_offset': p_config.get('yr_offset')
        # }
        # job = PrimaryDiagnostic(coupled_diag_config)
        # print_message(str(job))
        # job.execute()
        # sys.exit(1)

    # init the upload job
    if not required_jobs['upload_diagnostic_output']:
        upload_config = {
            'path_to_diagnostic': os.path.join(diag_output_path, 'amwg'),
            'username': config.get('upload_diagnostic').get('diag_viewer_username'),
            'password': config.get('upload_diagnostic').get('diag_viewer_password'),
            'server': config.get('upload_diagnostic').get('diag_viewer_server'),
            'depends_on': [len(year_set.jobs) - 1] # set the upload job to wait for the diag job to finish
        }
        upload = UploadDiagnosticOutput(upload_config)

        msg = 'Adding Upload job to the job list: {}'.format(str(upload))
        logging.info(message)

        logging.info(msg)
        year_set.add_job(upload)

    """
    # finally init the publication job
    if not required_jobs['publication']:
        publication_config = {
            'place': 'holder',
            'yearset': year_set,
            'depends_on': [len(job_set['jobs']) - 2] # wait for the diagnostic job to finish, but not the upload job
        }
        publish = Publication(publication_config)
        job_set['jobs'].append(publish)
        print_message('adding publication job')
    """
    return year_set



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

    output_pattern = config.get('global').get('output_pattern')
    date_pattern = config.get('global').get('date_pattern')
    frequencies = config.get('global').get('set_frequency')

    for f in new_files:
        file_key = filename_to_file_list_key(f, output_pattern, date_pattern)
        status = file_list.get(file_key)
        if status and status != SetStatus.DATA_READY:
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
    for file in checked_new_files:
        for freq in frequencies:
            year_set = filename_to_year_set(file, output_pattern, freq)
            for job_set in job_sets:
                if job_set.set_number == year_set and job_set.status == SetStatus.NO_DATA:

                    job_set.status = SetStatus.PARTIAL_DATA
                    # Spawn jobs for that yearset
                    job_set = add_jobs(job_set)

    # construct list of files to transfer
    f_path = config.get('transfer').get('source_path')
    f_list = ['{path}/{file}'.format(path=f_path, file=f)  for f in checked_new_files]

    t_config = config.get('transfer')
    g_config = config.get('global')
    m_config = config.get('monitor')

    transfer_config = {
        'file_list': f_list,
        'globus_username': t_config.get('globus_username'),
        'globus_password': t_config.get('globus_password'),
        'source_username': m_config.get('compute_username'),
        'source_password': m_config.get('compute_password'),
        'destination_username': t_config.get('processing_username'),
        'destination_password': t_config.get('processing_password'),
        'source_endpoint': t_config.get('source_endpoint'),
        'destination_endpoint': t_config.get('destination_endpoint'),
        'source_path': t_config.get('source_path'),
        'destination_path': g_config.get('data_cache_path') + '/',
        'recursive': 'False',
        'final_destination_path': config.get('global').get('data_cache_path'),
        'pattern': config.get('global').get('output_pattern'),
        'ncclimo_path': config.get('ncclimo').get('ncclimo_path')
    }
    transfer = Transfer(transfer_config)
    logging.info('## Starting transfer %s with config: %s', transfer.uuid, pformat(transfer_config))
    print_message('Starting file transfer', 'ok')
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

    if transfer_job.status != JobStatus.COMPLETED:
        print_message("File transfer failed")
        message = "## Transfer {uuid} has failed".format(uuid=transfer_job.uuid)
        logging.error(message)
        return
    else:
        print_message('Finished file transfer', 'ok')
        message = "## Transfer {uuid} has completed".format(uuid=transfer_job.uuid)
        logging.info(message)

    # update the file_list all the files that were transferred
    output_pattern = config.get('global').get('output_pattern')
    date_pattern = config.get('global').get('date_pattern')
    for file in f_list:
        list_key = filename_to_file_list_key(file, output_pattern, date_pattern)
        file_list[list_key] = SetStatus.DATA_READY
        file_name_list[list_key] = file

    if debug:
        print_message('file_list status: ', 'ok')
        for key in sorted(file_list, cmp=file_list_cmp):
            print_message('{key}: {val}'.format(key=key, val=file_list[key]), 'ok')

def is_all_done():
    """
    Check if all job_sets are done, and all processing has been completed
    """
    for job_set in job_sets:
        if job_set.status != SetStatus.COMPLETED:
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
        message = "## year_set {set} status change to {status}".format(set=year_set.set_number, status=year_set.status)
        logging.error(message)
        print_message('Error archiving run_scripts directory')


if __name__ == "__main__":

    # A list of all the expected files
    file_list = {}
    # A list of all the file names
    file_name_list = {}
    # Describes the state of each job jet
    job_sets = []
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

    # check that all netCDF files exist
    path_exists(config)

    # compute number of expected year_sets
    sim_start_year = int(config.get('global').get('simulation_start_year'))
    sim_end_year = int(config.get('global').get('simulation_end_year'))
    number_of_sim_years = sim_end_year - (sim_start_year - 1)
    frequencies = config.get('global').get('set_frequency')
    if not from_saved_state:
        job_sets = []

        for freq in frequencies:
            freq = int(freq)
            year_set = number_of_sim_years / freq

            # initialize the job_sets dict
            for i in range(1, year_set + 1):
                set_start_year = sim_start_year + ((i - 1) * freq)
                set_end_year = set_start_year + freq - 1
                new_set = YearSet(
                    set_number=len(job_sets) + 1,
                    start_year=set_start_year,
                    end_year=set_end_year)
                job_sets.append(new_set)

    # initialize the file_list
    for year in range(1, number_of_sim_years + 1):
        for month in range(1, 13):
            key = str(year) + '-' + str(month)
            file_list[key] = SetStatus.NO_DATA
            file_name_list[key] = ''

    # Check for any data already on the System
    all_data = check_for_inplace_data(
        file_list=file_list,
        file_name_list=file_name_list,
        job_sets=job_sets,
        config=config)

    check_year_sets(
        job_sets=job_sets,
        file_list=file_list,
        sim_start_year=config.get('global').get('simulation_start_year'),
        sim_end_year=config.get('global').get('simulation_end_year'),
        debug=debug,
        add_jobs=add_jobs)

    if debug:
        for s in job_sets:
            print_message('year_set {0}:'.format(s.set_number), 'ok')
            for job in s.jobs:
                print_message('    {}'.format(str(job)))

        print_message('printing year sets', 'ok')
        for key in job_sets:
            msg = '{0}: {1}'.format(key.set_number, key.status)
            print_message(msg, 'ok')
        print_message('printing file list', 'ok')
        for key in sorted(file_list, cmp=file_list_cmp):
            msg = '{0}: {1}'.format(key, file_list[key])
            print_message(msg, 'ok')

    if all_data:
        print_message('All data is local, disabling remote monitor', 'ok')
    else:
        print_message('More data needed, enabling remote monitor', 'ok')

    # If all the data is local, dont start the monitor
    if not all_data:
        pattern = config.get('global').get('output_pattern').replace('YYYY', '[0-9][0-9][0-9][0-9]')
        pattern = pattern.replace('MM', '[0-9][0-9]')
        monitor_config = {
            'remote_host': config.get('monitor').get('compute_host'),
            'remote_dir': config.get('transfer').get('source_path'),
            'username': config.get('monitor').get('compute_username'),
            'pattern': pattern
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
            check_year_sets(
                job_sets=job_sets,
                file_list=file_list,
                sim_start_year=config.get('global').get('simulation_start_year'),
                sim_end_year=config.get('global').get('simulation_end_year'),
                debug=debug,
                add_jobs=add_jobs)
            all_data = check_for_inplace_data(
                file_list=file_list,
                file_name_list=file_name_list,
                job_sets=job_sets,
                config=config)
            start_ready_job_sets(
                job_sets=job_sets,
                thread_list=thread_list,
                debug=debug,
                event=thread_kill_event)
            if is_all_done():
                # cleanup()
                print_message('All processing complete')
                logging.info("## All processes complete")
                sys.exit(0)
            sleep(10)
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        print_message('cleaning up threads', 'ok')
        for t in thread_list:
            thread_kill_event.set()
            t.join()
