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

from math import floor
from shutil import copy, rmtree
from getpass import getpass
from time import sleep
from pprint import pformat
from subprocess import Popen, PIPE

from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from jobs.Ncclimo import Climo
from jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
from jobs.Publication import Publication
from jobs.CMORjob import CMOREjob
from Monitor import Monitor

from util import print_debug
from util import print_message
from util import filename_to_file_list_key
from util import filename_to_year_set
from util import create_symlink_dir
from util import file_list_cmp

from jobs.TestJob import TestJob

import pdb

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')
parser.add_argument('-d', '--debug', help='Run in debug mode', action='store_true')
parser.add_argument('-s', '--state', help='Path to a json state file')

@atexit.register
def save_state():
    try:
        state_path = config.get('state_path')
        if not state_path:
            return
        with open(state_path, 'w') as outfile:
            state = {
                'file_list': file_list,
                'job_sets': job_sets,
                'config': config
            }
            json.dump(state, outfile)
    except IOError as e:
        print_debug(e)
        print_message("Error saving state file")

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
    if args.state:
        if not os.path.exists(args.state):
            config['state_path'] = args.state
        else:
            try:
                with open(args.state, 'r') as statefile:
                    state = json.load(statefile)
                if debug:
                    print_message('Loading from saved state {}'.format(args.state))
                config = state.get('config')
                file_list = state.get('file_list')
                job_sets = state.get('job_sets')
                config['state_path'] = args.state
            except IOError as e:
                print_debug(e)
                print_message('Error loading state file')
                sys.exit(1)
            from_saved_state = True
            if debug:
                print_message('saved file_list: \n{}'.format(pformat(sorted(file_list, cmp=file_list_cmp))))
                print_message('saved job_sets: \n{}'.format(pformat(job_sets)))
                print_message('saved config: \n{}'.format(pformat(config)))
    if not from_saved_state:
        required_fields = [
            "output_path",
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
            "batch_system_type"
        ]
        if args.config:
            try:
                with open(args.config, 'r') as conf:
                    config = json.load(conf)
            except Exception as e:
                print_debug(e)
                print_message('Unable to read config file, is it properly formatted json?')
                return -1

            for field in required_fields:
                if field not in config or len(config[field]) == 0:
                    if field == 'compute_password' and config.get('compute_keyfile'):
                        continue
                    if field == 'compute_keyfile' and config.get('compute_password'):
                        continue
                    if 'password' in field:
                        config[field] = getpass("{0} not specified in config, please enter: ".format(field))
                    else:
                        config[field] = raw_input("{0} not specified in config, please enter: ".format(field))
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

def add_jobs(year_set, job_set):
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
        climo_output_dir = os.path.join(config.get('output_path'))
        if not os.path.exists(climo_output_dir):
            if debug:
                print_message("Creating climotology output directory {}".format(climo_output_dir))
            os.makedirs(climo_output_dir)
        regrid_output_dir = config.get('output_path') + '/regrid/'
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)

        # Setup variables for climo job
        climo_start_year = config.get('simulation_start_year') + ((year_set - 1) * config.get('set_frequency'))
        climo_end_year = climo_start_year + config.get('set_frequency') - 1
        # create a temp directory, and fill it with symlinks to the actual data
        key_list = []
        for year in range(climo_start_year, climo_end_year + 1):
            for month in range(13):
                key_list.append('{0}-{1}'.format(year, month))
        climo_file_list = [file_name_list.get(x) for x in key_list]
        climo_temp_dir = os.path.join(os.getcwd(), 'tmp_climo', 'year_set_' + str(year_set))
        if not os.path.exists(climo_temp_dir):
            os.makedirs(climo_temp_dir)
        create_symlink_dir(
            src_dir=config.get('data_cache_path'),
            src_list=climo_file_list,
            dst=climo_temp_dir
        )
        # create the configuration object for the climo job
        climo_config = {
            'start_year': climo_start_year,
            'end_year': climo_end_year,
            'caseId': config.get('experiment'),
            'annual_mode': 'sdd',
            'regrid_map_path': config.get('regrid_map_path'),
            'input_directory': climo_temp_dir,
            'climo_output_directory': climo_output_dir,
            'regrid_output_directory': regrid_output_dir,
            'yearset': year_set
        }
        climo = Climo(climo_config)
        job_set['jobs'].append(climo)

    # second init the diagnostic job
    if not required_jobs['diagnostic']:
        diag_output_path = config.get('output_path') + '/diagnostics/year_set_' + str(year_set)
        if not os.path.exists(diag_output_path):
            os.makedirs(diag_output_path)
        # create a temp directory full of just the regridded output we need for this diagnostic job
        diag_temp_dir = os.path.join(os.getcwd(), 'tmp_diag', 'year_set_' + str(year_set))
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
            '--obs': config.get('obs_for_diagnostics_path'),
            '--outputdir': diag_output_path,
            '--package': 'amwg',
            '--set': '5',
            '--archive': 'False',
            'yearset': year_set,
            'depends_on': [len(job_set['jobs']) - 1] # set the diag job to wait for the climo job to finish befor running
        }
        diag = Diagnostic(diag_config)
        job_set['jobs'].append(diag)

    # third init the upload job
    if not required_jobs['upload_diagnostic_output']:
        upload = UploadDiagnosticOutput({
            'path_to_diagnostic': diag_output_path + '/amwg/',
            'username': config.get('diag_viewer_username'),
            'password': config.get('diag_viewer_password'),
            'server': config.get('diag_viewer_server'),
            'depends_on': [len(job_set['jobs']) - 1] # set the upload job to wait for the diag job to finish
        })
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
    if active_transfers >= 3:
        return
    monitor.check()
    new_files = monitor.get_new_files()
    checked_new_files = []
    for f in new_files:
        key = filename_to_file_list_key(f, config.get('output_pattern'))
        if file_list.get(key) and not file_list[key] == 'data ready':
            checked_new_files.append(f)

    # if there are any new files
    if checked_new_files:
        if debug:
            print_message('Found new files: {}\n setting up transfer'.format(
                pformat(checked_new_files, indent=4)), 'ok')
        # find which year set the data belongs to
        for f in new_files:
            year_set = filename_to_year_set(f, config.get('output_pattern'), config.get('set_frequency'))
            for job_set in job_sets:
                if job_set.get('year_set') == year_set:
                    # if before we got here, the job_set didnt have any data, now that we have some data
                    # we create the processing jobs and add them to the job_sets list of jobs
                    if job_set.get('status') == 'no data':
                        # Change job_sets' state
                        job_set['status'] = 'data in transit'

                        # Spawn jobs for that yearset
                        job_set = add_jobs(year_set, job_set)

        f_list = ['{path}/{file}'.format(path=config.get('source_path'), file=f)  for f in checked_new_files]
        # tmpdir = os.getcwd() + '/tmp/'
        transfer_config = {
            'file_list': f_list,
            'globus_username': config.get('globus_username'),
            'globus_password': config.get('globus_password'),
            'source_username': config.get('compute_username'),
            'source_password': config.get('compute_password'),
            'destination_username': config.get('processing_username'),
            'destination_password': config.get('processing_password'),
            'source_endpoint': config.get('source_endpoint'),
            'destination_endpoint': config.get('destination_endpoint'),
            'source_path': config.get('source_path'),
            'destination_path': config.get('data_cache_path') + '/',
            'recursive': 'False',
            'final_destination_path': config.get('data_cache_path'),
            'pattern': config.get('output_pattern'),
            'frequency': config.get('set_frequency')
        }
        transfer = Transfer(transfer_config)
        thread = threading.Thread(target=handle_transfer, args=(transfer, checked_new_files, thread_kill_event))
        thread_list.append(thread)
        thread.start()
        active_transfers += 1
    else:
        if debug:
            print_message('No new files found', 'ok')

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

    if transfer_job.status != 'complete':
        print_message('Faild to transfer files correctly')
    # the transfer is complete, so we can decrement the active_transfers counter
    active_transfers -= 1
    # handle post processing for transfered data
    tmpdir = os.getcwd() + '/tmp/'
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    for f in f_list:
        # update the file_list for this file to reflect that the transfer is complete
        list_key = filename_to_file_list_key(f, config.get('output_pattern'))
        file_list[list_key] = 'data ready'
        file_name_list[list_key] = f
    if debug:
        print_message("transfer job complete", 'ok')
        print_message('file_list status: ', 'ok')
        for key in sorted(file_list, cmp=file_list_cmp):
            print_message('{key}: {val}'.format(key=key, val=file_list[key]), 'ok')

def check_year_sets():
    """
    Checks the file_list, and sets the year_set status to ready if all the files are in place
    """
    if debug:
        print_message('job_sets:'.format(sets=pformat(job_sets)))
        for s in job_sets:
            for job in s['jobs']:
                print_message('    {}'.format(str(job)))
    for s in job_sets:
        set_start_year = config.get('simulation_start_year') + ((s.get('year_set') - 1) * config.get('set_frequency'))
        set_end_year = set_start_year + config.get('set_frequency') - 1
        if debug:
            print_message('set_start_year: {0}'.format(set_start_year), 'ok')
            print_message('set_end_year  : {0}'.format(set_end_year), 'ok')
        ready = True
        for year in range(set_start_year, set_end_year + 1):
            for month in range(1, 13):
                key = str(year) + '-' + str(month)
                if file_list.get(key) and file_list[key] != 'data ready':
                    ready = False
        if ready:
            status = 'data ready'
            year_set = int(floor(set_start_year / config.get('set_frequency')) + 1)
            s = add_jobs(year_set, s)
        else:
            status = s['status']
        s['status'] = status

def check_for_inplace_data():
    """
    Checks the data cache for any files that might already be in place,
    updates the file_list and job_sets accordingly
    """
    global file_list
    global file_name_list
    cache_path = config.get('data_cache_path')
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    for climo_file in os.listdir(cache_path):
        key = filename_to_file_list_key(climo_file, config.get('output_pattern'))
        file_list[key] = 'data ready'
        file_name_list[key] = climo_file

def start_ready_job_sets():
    """
    Iterates over the job sets, and starts ready jobs
    """
    global thread_list
    # iterate over the job_sets
    print_message('=========== Checking for ready jobs =================', 'ok')
    for job_set in job_sets:
        # if the job state is ready, but hasnt started yet
        # print_message('job_set status: {}'.format(job_set['status']))
        if job_set['status'] == 'data ready':
            for job in job_set['jobs']:
                # if the job is a climo, and it hasnt been started yet, start it
                print_message('job type: {0}, job_status: {1}'.format(job.get_type(), job.status), 'ok')
                if job.get_type() == 'climo' and job.status == 'valid':
                    job_id = job.execute(batch=True)
                    job.set_status('starting')
                    print_message('Starting climo job for year_set {}'.format(job_set['year_set']), 'ok')
                    thread = threading.Thread(target=monitor_job, args=(job_id, job, thread_kill_event))
                    thread_list.append(thread)
                    thread.start()
                    return
                # if the job isnt a climo, and the job that it depends on is done, start it
                elif job.get_type() != 'climo' and job.status == 'valid':
                    ready = True
                    for dependancy in job.depends_on:
                        if job_set['jobs'][dependancy].status != 'COMPLETE':
                            ready = False
                            break
                    if ready:
                        job_id = job.execute(batch=True)
                        job.set_status('starting')
                        print_message('Starting {0} job for year_set {1}'.format(job.get_type(), job_set['year_set']), 'ok')
                        thread = threading.Thread(target=monitor_job, args=(job_id, job, thread_kill_event))
                        thread_list.append(thread)
                        thread.start()
                        return
                elif job.status == 'invalid':
                    print_message('===== INVALID JOB =====\n{}'.format(str(job)))

def monitor_job(job_id, job, event=None):
    """
    Monitor the slurm job, and update the status to 'complete' when it finishes
    This function should only be called from within a thread
    """
    def handle_slurm():
        """
        handle interfacing with the SLURM controller
        Checkes the SLURM queue status and changes the job status appropriately
        """
        print_message('checking SLURM queue status', 'ok')
        count = 5
        valid = False
        while count > 0 and not valid:
            cmd = ['scontrol', 'show', 'job', str(job_id)]
            out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
            # sometimes there will be a communication error with the SLURM controller
            # in which case the controller returns 'error: some message'
            if 'error' in out or len(out.split('\n')) == 0:
                valid = False
                count -= 1
            else:
                valid = True

        if not valid:
            # if the controller errors 5 times in a row, its probably an unrecoverable error
            print_message('-------- Unable to communicate with SLURM controller ----------')
            return 'ERROR'

        # we're looking for the JobState field, which is on the 4th line
        # count = 0
        # for line in out.split('\n'):
        #     print '{0}: {1}'.format(count, line)
        #     count += 1
        job_status = None
        for line in out.split('\n'):
            for word in line.split():
                if 'JobState' in word:
                    index = word.find('=')
                    job_status = word[index + 1:]
                    break
            if job_status:
                break

        print_message(' ======= end job monitor, status: {} ======='.format(job_status), 'ok')
        return job_status

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

    while True:
        print_message('======= monitoring job {0} ========='.format(job_id), 'ok')
        # this check is here in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        batch_system = config.get('batch_system_type')
        if batch_system == 'slurm':
            status = handle_slurm()
        elif batch_system == 'pbs':
            status = handle_pbs()
        elif batch_system == 'none':
            cmd = ['']
            status = handle_none()
            # TODO: figure out how to get this working

        if not status:
            if event and event.is_set():
                return
            continue

        if job.status != status:
            if debug:
                print_message('Setting job status: {0}'.format(status))
            job.status = status
        if status == 'COMPLETE' or status == 'error':
            break

        # instead of sleeping for 10 seconds, sleep for 1 second 10 times
        # so that the event can be checked and quiting doesnt take 10 seconds
        for i in range(10):
            if event and event.is_set():
                return
            sleep(1)

if __name__ == "__main__":

    # A list of all the expected fiels
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

    # Read in parameters from config
    config = setup(parser)
    if config == -1:
        print "Error in setup, exiting"
        sys.exit(1)

    # compute number of expected year sets
    year_sets = (int(config.get('simulation_end_year')) - (int(config.get('simulation_start_year') - 1))) / int(config.get('set_frequency'))
    if debug:
        print_message('start_year: {sy},\n     end_year: {ey},\n     set_frequency: {freq},\n     number of year_sets: {ys}'.format(
            sy=config.get('simulation_start_year'),
            ey=config.get('simulation_end_year'),
            ys=year_sets,
            freq=config.get('set_frequency')
        ), 'ok')

    if not from_saved_state:
        # initialize the job_sets dict
        job_sets = [{'status': 'no data', 'year_set': i, 'jobs': []} for i in range(1, year_sets+1)]
        start_year = config.get("simulation_start_year")
        end_year = config.get("simulation_end_year")
        sim_length = end_year - start_year + 1
        # initialize the file_list
        if debug:
            print_message('initializing file_list with {num_years} years'.format(
                num_years=sim_length))
        for year in range(1, sim_length + 1):
            for month in range(1, 13):
                key = str(year) + '-' + str(month)
                file_list[key] = 'no data'
                file_name_list[key] = ''

    # Check for any data already on the System
    check_for_inplace_data()
    check_year_sets()
    if debug:
        print_message('printing year sets', 'ok')
        for key in job_sets:
            print_message(str(key['year_set']) + ': ' + key['status'], 'ok')
        print_message('printing file list', 'ok')
        for key in sorted(file_list, cmp=file_list_cmp):
            print_message(key + ': ' + file_list[key], 'ok')

    monitor_config = {
        'remote_host': config.get('compute_host'),
        'remote_dir': config.get('source_path'),
        'username': config.get('compute_username'),
        'pattern': config.get('output_pattern')
    }
    if config.get('compute_password'):
        monitor_config['password'] = config.get('compute_password')
    if config.get('compute_keyfile'):
        monitor_config['keyfile'] = config.get('compute_keyfile')
    else:
        print_message('No password or keyfile path given for compute resource, please add to your config and try again')
        sys.exit(1)
    monitor = Monitor(monitor_config)

    print_message('attempting connection to {}'.format(config.get('compute_host')), 'ok')
    if monitor.connect() == 0:
        print_message('connected', 'ok')
    else:
        print_message('unable to connect, exiting')
        sys.exit(1)

    # Main loop
    try:
        while True:
            # Setup remote monitoring system
            monitor_check(monitor)
            # Check if a year_set is ready to run
            check_year_sets()
            start_ready_job_sets()

            sleep(10)
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        if config.get('state_path'):
            print_message('saving state', 'ok')
            save_state()
        print_message('cleaning up threads', 'ok')
        for t in thread_list:
            thread_kill_event.set()
            t.join()




