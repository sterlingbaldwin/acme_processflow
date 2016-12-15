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
from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from jobs.Ncclimo import Climo
from jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
from Monitor import Monitor
from util import print_debug
from util import print_message


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
        print 'No configuration file given, initiating manual setup'
        config = {}
        for field in required_fields:
            if 'password' in field:
                config[field] = getpass('{0}: '.format(field))
            else:
                config[field] = raw_input('{0}: '.format(field))
    return config

def monitor_check(monitor):
    # global job_list
    global job_counter
    # global config
    # global file_list
    monitor.check()
    new_files = monitor.get_new_files()
    checked_new_files = []
    for f in new_files:
        key = filename_to_file_list_key(f)
        if not file_list[key] == 'data ready':
            checked_new_files.append(f)
    if checked_new_files:
        if debug:
            print_message('Found new files: {}\n setting up transfer'.format(
                pformat(checked_new_files, indent=4)), 'ok')

        # find which year set the data belongs to
        for f in new_files:
            year_set = filename_to_year_set(f)
            for key in job_sets:
                if key.get('year_set') == year_set:
                    if key.get('status') == 'no data':
                        # Change year set state
                        key['status'] = 'data in transit'
                        # Spawn jobs for that yearset
                        climo_output_dir = config.get('output_path') + '/year_set_' + str(year_set)
                        if not os.path.exists(climo_output_dir):
                            os.makedirs(climo_output_dir)
                        regrid_output_dir = config.get('output_path') + '/regrid'
                        if not os.path.exists(regrid_output_dir):
                            os.makedirs(regrid_output_dir)
                        climo_config = {
                            'start_year': year_set,
                            'end_year': year_set + 5,
                            'caseId': config.get('experiment'),
                            'anual_mode': 'sdd',
                            'regrid_map_path': config.get('regrid_map_path'),
                            'input_directory': config.get('data_cache_path') + '/year_set_' + str(year_set),
                            'climo_output_directory': climo_output_dir,
                            'regrid_output_directory': regrid_output_dir,
                            'yearset': year_set
                        }
                        climo = Climo(climo_config)
                        job_counter += 1
                        job_list[job_counter] = climo
                        diag_output_path = config.get('output_path') + '/diagnostics/year_set_' + str(year_set)
                        if not os.path.exists(diag_output_path):
                            os.makedirs(diag_output_path)
                        diag_config = {
                            '--model': config.get('data_cache_path') + '/year_set_' + str(year_set),
                            '--obs': config.get('obs_for_diagnostics_path'),
                            '--outputdir': diag_output_path,
                            '--package': 'amwg',
                            '--set': '5',
                            '--archive': 'False',
                            'yearset': year_set,
                            'depends_on': job_counter
                        }
                        diag = Diagnostic(diag_config)
                        job_counter += 1
                        job_list[job_counter] = diag
                        upload = UploadDiagnosticOutput({
                            'path': diag_config.get('--outputdir'),
                            'username': config.get('diag_viewer_username'),
                            'password': config.get('diag_viewer_password'),
                            'server': config.get('diag_viewer_server'),
                            'depends_on': job_counter
                        })
                        job_counter += 1
                        job_list[job_counter] = upload


        if debug:
            for key in job_sets:
                print_message("Yearset: {ys} status: {status}".format(
                    ys=key.get('year_set'),
                    status=key.get('status')
                ), 'ok')


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
        job_counter += 1
        job_list[job_counter] = t
        thread = threading.Thread(target=handle_transfer, args=(t, checked_new_files))
        thread.start()

def handle_transfer(transfer_job, f_list):
    if debug:
        print_message("starting transfer job for given files:", 'ok')
        print pformat(f_list)
    # change job_sets status for this set to 'data in transit'
    # TODO

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

        if debug:
            print_message('copying from {src} to {dst}'.format(
                src=src,
                dst=new_path
            ), 'ok')

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
        if debug:
            print_message('setting {key} status to data ready'.format(key=key))
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
        Sets the status of the job_set tracker if the data is ready
    """
    if debug:
        print_message('job_sets: \n{sets}'.format(sets=pformat(job_sets)))
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
        if debug:
            print_message('setting {year_set} status to {status}'.format(
                year_set=s['year_set'],
                status=status
            ))
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
    # TODO: get the jobs starting 
    # the best way to do this is to change their execute functions to submit to a SLURM queue
    # for s in job_sets:
    #     if s['status'] = 'data ready':
    #         for job in job_list:
    #             if job.get_type == 'climo':
                    
                    


if __name__ == "__main__":

    file_list = {}
    debug = False
    # Initialize the job list
    job_list = {}
    job_counter = 0

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
    job_sets = [{'status': 'no data', 'year_set': i} for i in range(1, year_sets+1)]
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
    print_message('attempting connection', 'ok')
    if monitor.connect() == 0:
        print_message('connected', 'ok')
    else:
        print_message('unable to connect')

    # Main loop
    while True:
        # Setup remote monitoring system
        monitor_check(monitor)
        # Check if a year_set is ready to run
        check_year_sets()
        if debug:
            print_message(pformat(job_sets))
        sleep(10)




