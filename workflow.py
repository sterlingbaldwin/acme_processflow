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

from jobs.TestJob import TestJob

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
                print_message('saved file_list: \n{}'.format(pformat(file_list)))
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
        climo_output_dir = os.path.join(config.get('output_path') + '/year_set_' + str(year_set))
        if not os.path.exists(climo_output_dir):
            if debug:
                print_message("Creating climotology output directory {}".format(climo_output_dir))
            os.makedirs(climo_output_dir)
        regrid_output_dir = config.get('output_path') + '/regrid/year_set_' + str(year_set)
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)
        climo_start_year = config.get('simulation_start_year') + ((year_set - 1) * config.get('set_frequency'))
        climo_end_year = climo_start_year + config.get('set_frequency') - 1
        model_path = config.get('data_cache_path') + '/year_set_' + str(year_set)
        climo_config = {
            'start_year': climo_start_year,
            'end_year': climo_end_year,
            'caseId': config.get('experiment'),
            'annual_mode': 'sdd',
            'regrid_map_path': config.get('regrid_map_path'),
            'input_directory': model_path,
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
        diag_config = {
            '--model': regrid_output_dir,
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

    monitor.check()
    new_files = monitor.get_new_files()
    checked_new_files = []
    for f in new_files:
        key = filename_to_file_list_key(f)
        if file_list.get(key) and not file_list[key] == 'data ready':
            checked_new_files.append(f)
    # if there are any new files
    if checked_new_files:
        if debug:
            print_message('Found new files: {}\n setting up transfer'.format(
                pformat(checked_new_files, indent=4)), 'ok')

        # find which year set the data belongs to
        for f in new_files:
            year_set = filename_to_year_set(f)
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
        tmpdir = os.getcwd() + '/tmp/'
        transfer = Transfer({
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
            'destination_path': tmpdir,
            'recursive': 'False'
        })
        thread = threading.Thread(target=handle_transfer, args=(transfer, checked_new_files, thread_kill_event))
        thread_list.append(thread)
        thread.start()

def handle_transfer(transfer_job, f_list, event):
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
    # handle post processing for transfered data
    tmpdir = os.getcwd() + '/tmp/'
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    for f in f_list:
        # check that a folder for this year set exists, if not make one
        year_set = filename_to_year_set(f)
        new_path = os.path.join(config.get('data_cache_path'), 'year_set_' + str(year_set))
        if not os.path.exists(new_path):
            os.mkdir(new_path)

        src = os.path.join(tmpdir, f)
        # copy the file to the correct year set folder
        try:
            copy(src=src, dst=new_path)
        except Exception as e:
            print_debug(e)
            print_message('Error moving file from {src} to {dst}'.format(src=src, dst=new_path))
        # remove the old files
        try:
            os.remove(src)
        except Exception as e:
            print_debug(e)
            print_message('Error removing file {0}'.format(src))

        # update the file_list for this file to reflect that the transfer is complete
        key = filename_to_file_list_key(f)
        file_list[key] = 'data ready'
    if debug:
        print_message("transfer job complete", 'ok')
        print_message('file_list status: ', 'ok')
        print_message(pformat(file_list), 'ok')

def filename_to_file_list_key(filename):
    """
    Takes a filename and returns the key for the file_list
    """
    # these offsets need to change if the output_pattern changes. This is unavoidable given the escape characters
    start_offset = 8
    end_offset = 12
    month_start_offset = end_offset + 1
    month_end_offset = month_start_offset + 2
    index = re.search(config.get('output_pattern'), filename).start()
    year = int(filename[index + start_offset: index + end_offset])
    month = int(filename[index + month_start_offset: index + month_end_offset])
    key = "{year}-{month}".format(year=year, month=month)
    return key

def filename_to_year_set(filename):
    """
    Takes a filename and returns the year_set that the file belongs to
    """
    # these offsets need to change if the output_pattern changes. This is unavoidable given the escape characters
    start_offset = 8
    end_offset = 12
    index = re.search(config.get('output_pattern'), filename).start()
    year = int(filename[index + start_offset: index + end_offset])
    year_set = int(floor(year / config.get('set_frequency'))) + 1
    return year_set

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
            year_set = floor(set_start_year / config.get('set_frequency')) + 1
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
    cache_path = config.get('data_cache_path')
    for folder in os.listdir(cache_path):
        for f in os.listdir(os.path.join(cache_path, folder)):
            key = filename_to_file_list_key(f)
            file_list[key] = 'data ready'

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
                        if job_set['jobs'][dependancy].status != 'complete':
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
            cmd = ['squeue']
            out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]

            # sometimes there will be a communication error with the SLURM controller
            # in which case the controller returns 'error: some message'
            if 'error' in out:
                valid = False
                count -= 1
            else:
                valid = True
            print out, count

        if not valid:
            # if the controller errors 5 times in a row, its probably an unrecoverable error
            print_message('-------- Unable to communicate with SLURM controller ----------')
            return 'ERROR'

        job_status = 'running'
        found_job = False
        valid = False
        for line in out.split('\n'):
            words = filter(None, line.split(' '))
            if debug:
                print_message(words, 'ok')
            # the 0th word is the job_id, except for the first line
            if len(words) == 0:
                break
            elif words[0] == 'JOBID':
                # if we dont see the line '['JOBID', 'PARTITION', 'NAME', 'USER', 'ST', 'TIME', 'NODES', 'NODELIST(REASON)']' at least onece then we know squeue didnt work right
                valid = True
                continue
            try:
                line_id = int(words[0])
            except Exception as e:
                print_message('Unable to convert {} into an int'.format(words[0]))
                continue
            if line_id == job_id:
                found_job = True
                # the 4th word is the job status
                for word in words:
                    if word == 'R':
                        job_status = 'running'
                        break
                    elif word == 'PD':
                        job_status = 'waiting in queue'
                        break
                    else:
                        job_status = 'unrecognized state'
            if debug and job_status != 'running' and job_status != 'waiting in queue':
                print_message('Unrecognized job state: {}'.format(words[4]))
            if job_status != job.status:
                if debug:
                    print_message('setting job status to {}'.format(job_status), 'ok')
                return job_status
        if not found_job:
            if not valid:
                # I might want to put a counter here, since if SLURM goes down entirely it would just loop forever
                # but Ive seen it need to request 5-10 times before getting a response (rare, but its happened)
                sleep(1)
                return None
            # if the job isnt in the queue anymore, that means its complete
            job_status = 'complete'

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
        # this check is hear in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        batch_system = config.get('batch_system_type')
        count = 5
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
        if status == 'complete' or status == 'error':
            break

        # instead of sleeping for 10 seconds, sleep for 1 second 10 times
        # so that the event can be checked and quiting doesnt take 10 seconds
        for i in range(10):
            if event and event.is_set():
                return
            sleep(1)

if __name__ == "__main__":

    file_list = {}
    job_sets = {}
    config = {}
    thread_list = []
    thread_kill_event = threading.Event()
    debug = False
    from_saved_state = False

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

    # Check for any data already on the System
    check_for_inplace_data()

    if debug:
        print_message('printing year sets', 'ok')
        for key in job_sets:
            print_message(str(key['year_set']) + ': ' + key['status'], 'ok')
        print_message('printing file list', 'ok')
        for key in sorted(file_list):
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




