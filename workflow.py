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
from Monitor import Monitor
from util import print_debug
from util import print_message


from jobs.TestJob import TestJob


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')
parser.add_argument('-d', '--debug', help="Run in debug mode", action='store_true')

def setup(parser):
    global debug
    global job_list
    global job_counter
    global config
    global file_list
    args = parser.parse_args()
    if args.debug:
        debug = True
        print_message('Running in debug mode', 'ok')
    required_fields = [
        "output_path",
        "data_cache_path",
        "compute_host",
        "compute_username",
        "compute_password",
        "processing_host",
        "processing_username",
        "processing_password",
        "globus_username",
        "globus_password",
        "source_endpoint",
        "destination_endpoint",
        "source_path",
    ]
    if args.config:
        with open(args.config, 'r') as conf:
            try:
                config = json.load(conf)
            except Exception as e:
                print_debug(e)
                print_message('Unable to read config file, is it properly formatted json?')
                return -1

        for field in required_fields:
            if field not in config or len(config[field]) == 0:
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

def monitor_check(monitor):
    global job_sets

    monitor.check()
    new_files = monitor.get_new_files()
    checked_new_files = []
    for f in new_files:
        key = filename_to_file_list_key(f)
        if not file_list[key] == 'data ready':
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
                        """
                        # some test jobs
                        test_1 = TestJob({})
                        job_set['jobs'].append(test_1)
                        print_message('Creating test job 1')

                        test_2 = TestJob({
                            'depends_on': [len(job_set['jobs']) - 1]
                        })
                        job_set['jobs'].append(test_2)
                        print_message('Creating test job 2')

                        test_3 = TestJob({
                            'depends_on': [len(job_set['jobs']) - 1]
                        })
                        job_set['jobs'].append(test_3)
                        print_message('Creating test job 3')
                        """
                        # Spawn jobs for that yearset
                        # first initialize the climo job
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
                        print_message('adding climo job')

                        # # second init the diagnostic job
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
                        # diag = Diagnostic(diag_config)
                        # job_set['jobs'].append(diag)
                        # print_message('adding diag job')

                        # third init the upload job
                        upload = UploadDiagnosticOutput({
                            'path_to_diagnostic': diag_output_path + '/amwg/',
                            'username': config.get('diag_viewer_username'),
                            'password': config.get('diag_viewer_password'),
                            'server': config.get('diag_viewer_server'),
                            'depends_on': [len(job_set['jobs']) - 1] # set the upload job to wait for the diag job to finish
                        })
                        job_set['jobs'].append(upload)
                        print_message('adding upload job')

                        # finally init the publication job
                        # TODO: create the publication job class and add it here

        # if debug:
        #     for job_set in job_sets:
        #         print_message("Yearset: {ys} status: {status}, jobs: \n{}".format(
        #             ys=job_set.get('year_set'),
        #             status=job_set.get('status'),
        #             jobs=pformat(job_set.get('jobs'))
        #         ), 'ok')


        f_list = ['{path}/{file}'.format(path=config.get('source_path'), file=f)  for f in checked_new_files]
        tmpdir = os.getcwd() + '/tmp/'
        t = Transfer({
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
        thread = threading.Thread(target=handle_transfer, args=(t, checked_new_files, thread_kill_event))
        thread_list.append(thread)
        thread.start()

def handle_transfer(transfer_job, f_list, event):
    # if debug:
    #     print_message("starting transfer job for given files:", 'ok')
    #     print pformat(f_list)

    # start the transfer job
    transfer_job.execute()

    # handle post processing for transfered data
    tmpdir = os.getcwd() + '/tmp/'
    for f in f_list:
        # check that a folder for this year set exists, if not make one
        year_set = filename_to_year_set(f)
        new_path = os.path.join(config.get('data_cache_path'), 'year_set_' + str(year_set))
        src = os.path.join(tmpdir, f)
        if not os.path.exists(new_path):
            os.mkdir(new_path)

        # copy the file to the correct year set folder
        try:
            copy(src=src, dst=new_path)
        except Exception as e:
            print_debug(e)
            print_message('Error moving file from {src} to {dst}'.format(src=src, dst=new_path))
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
            print_message('set_start_yaer: {0}'.format(set_start_year), 'ok')
            print_message('set_end_year: {0}'.format(set_end_year), 'ok')
        ready = True
        for year in range(set_start_year, set_end_year + 1):
            for month in range(1, 13):
                key = str(year) + '-' + str(month)
                if file_list[key] != 'data ready':
                    ready = False
        if ready:
            status = 'data ready'
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
    # TODO: get the jobs starting
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
                    # TODO: setup a queue monitoring system
                # if the job isnt a climo, and the job that it depends on is done, start it
                elif job.get_type() != 'climo' and job.status == 'valid':
                    ready = True
                    for dependancy in job.depends_on:
                        if job_set['jobs'][dependancy].status != 'complete':
                            ready = False
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
    """
    while True:
        print_message('======= monitoring job {} ========='.format(job_id), 'ok')
        # this function should only called in its own thread
        # this check is hear in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        cmd = ['squeue']
        count = 5
        out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]

        # sometimes there will be a communication error with the SLURM controller
        # in which case the controller returns 'error: some message'
        if 'error' in out:
            valid = False
        else:
            valid = True
        # re-request the queue status if there was an error
        while not valid and count >= 0:
            out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
            if 'error' in out:
                valid = False
                count -= 1
            else:
                valid = True
        if not valid:
            # if the controller errors 5 times in a row, its probably an unrecoverable error
            print_message('-------- Unable to communicate with SLURM controller ----------')
            return

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
                # if we dont see the line '['JOBID', 'PARTITION', 'NAME', 'USER', 'ST', 'TIME', 'NODES', 'NODELIST(REASON)']' then we know squeue didnt work right
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
                job.set_status(job_status)
        if not found_job:
            if not valid:
                # I might want to put a counter here, since if SLURM goes down entirely it would just loop forever
                # but Ive seen it need to request 5-10 times before getting a response (rare, but its happened)
                sleep(1)
                continue
            # if the job isnt in the queue anymore, that means its complete
            job_status = 'complete'
            print_message(' ======= end job monitor, status: {} ======='.format(job_status), 'ok')
            job.set_status(job_status)
            return
        if job_status == 'complete':
            break
        # instead of sleeping for 10 seconds, sleep for 1 second 10 times
        # so that the event can be checked and quiting doesnt take 10 seconds
        for i in range(10):
            if event and event.is_set():
                return
            sleep(1)


if __name__ == "__main__":

    file_list = {}
    thread_list = []
    thread_kill_event = threading.Event()
    debug = False

    # Read in parameters from config
    config = setup(parser)
    if config == -1:
        print "Error in setup, exiting"
        sys.exit()
    # compute number of expected year sets
    year_sets = (int(config.get('simulation_end_year')) - (int(config.get('simulation_start_year') - 1))) / int(config.get('set_frequency'))
    if debug:
        print_message('start_year: {sy},\n     end_year: {ey},\n     set_frequency: {freq},\n     number of year_sets: {ys}'.format(
            sy=config.get('simulation_start_year'),
            ey=config.get('simulation_end_year'),
            ys=year_sets,
            freq=config.get('set_frequency')
        ), 'ok')
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
    monitor = Monitor({
        'remote_host': config.get('compute_host'),
        'remote_dir': config.get('source_path'),
        'username': config.get('compute_username'),
        'password': config.get('compute_password'),
        'pattern': config.get('output_pattern')
    })
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

            # if debug:
            #     for job_set in job_sets:
            #         for job in job_set['jobs']:
            #             print_message(str(job))
            sleep(10)
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        print_message('cleaning up threads')
        for t in thread_list:
            thread_kill_event.set()
            t.join()




