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
    global job_list
    global job_counter
    global config
    global file_list
    monitor.check()
    print_message('Found new files: {}\n setting up transfer'.format(pformat(monitor.get_new_files(), indent=4) ), 'ok')
    if monitor.get_new_files():
        if debug:
            print "New remote files found {}, starting transfer".format(monitor.get_new_files)

        # find which year set the data belongs to
        for f in monitor.get_new_files():
            index = re.search("cam\\.h0\\.[0-9][0-9][0-9][0-9]", f).start()
            year = int(f[index + 7: index + 11])
            month = int(f[index + 12: index + 14])
            year_set = int(floor(year / config.get('set_frequency'))) + 1

            for key in job_sets:
                if key.get('year_set') == year_set:
                    if key.get('status') == 'no data':
                        # Change year set state
                        key['status'] = 'data in transit'
                        # Spawn jobs for that yearset
                        # climo = Climo({
                        #     'start_year': year_set,
                        #     'end_year': year_set + 5,
                        #     'caseId': config.get('experiment'),
                        #     'anual_mode': 'sdd',
                        #     'regrid_map_path': os.getcwd() + '/resources/map_ne30np4_to_fv129x256_aave.20150901.nc',
                        #     'input_directory': os.getcwd() + '/cache/',
                        #     'climo_output_directory': os.getcwd() + '/output',
                        #     'regrid_output_directory': os.getcwd() + 'output/regrid/',
                        #     'yearset': year_set
                        # })
                        # job_counter += 1
                        # job_list[job_counter] = climo
                        # diag = Diagnostic({
                        #     '--model': os.getcwd() + '/cache',
                        #     '--obs': os.getcwd() + '/resources/obs_for_diagnostics',
                        #     '--outputdir': os.getcwd() + '/output/diagnostics',
                        #     '--package': 'amwg',
                        #     '--set': '5',
                        #     '--archive': 'False',
                        #     'yearset': year_set
                        # })
                        # job_counter += 1
                        # job_list[job_counter] = diag

        for key in job_sets:
            print "Yearset: {ys} status: {status}".format(
                ys=key.get('year_set'),
                status=key.get('status')
            )


        f_list = ['{path}/{file}'.format(path=config.get('source_path'), file=f)  for f in monitor.get_new_files()]
        print '**** ' + pformat(monitor.get_new_files())
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
        thread = threading.Thread(target=handle_transfer, args=(t, monitor.get_new_files()))
        thread.start()

def handle_transfer(transfer_job, f_list):
    if debug:
        print_message("starting transfer job", 'ok')
        print pformat(f_list)
    # change job_sets status for this set to 'data in transit'
    # Ill get to this later

    # start the transfer job
    transfer_job.execute()

    # handle post processing for transfered data
    tmpdir = os.getcwd() + '/tmp/'
    print_message(f_list)
    for f in f_list:
        if debug:
            print_message('copying from {tmp}{file} to {cache}'.format(
                tmp=tmpdir,
                file=f,
                cache=config.get('data_cache_path')
            ), 'ok')
        copy(
            src='{tmp}{file}'.format(tmp=tmpdir, file=f),
            dst=config.get('data_cache_path')
        )
        os.remove(tmpdir + '/' + f)

        # update the file_list for this file to reflect that the transfer is complete
        index = re.search("cam\\.h0\\.[0-9][0-9][0-9][0-9]", f).start()
        year = int(f[index + 7: index + 11])
        month = int(f[index + 12: index + 14])
        year_set = int(floor(year / config.get('set_frequency'))) + 1
        key = str(year) + '-' + str(month)
        if debug:
            print_message('setting {key} status to data ready'.format(key=key))
        file_list[key] = 'data ready'
    if debug:
        print_message("transfer job complete", 'ok')
        print_message('file_list status: ', 'ok')
        print_message(pformat(file_list), 'ok')

def check_year_sets():
    global file_list
    global job_sets
    global config

    print 'job_sets: \n{sets}'.format(sets=pformat(job_sets))
    for s in job_sets:
        start_year = config.get('simulation_start_year') + ((s.get('year_set') - 1) * config.get('set_frequency'))
        end_year = start_year + config.get('set_frequency') - 1
        ready = True
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                key = str(year) + '-' + str(month)
                if file_list[key] != 'data ready':
                    ready = False
        if ready:
            s['status'] = 'data ready'

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
    year_sets = (int(config.get('simulation_end_year')) - (int(config.get('simulation_start_year') - 1))) / int(config.get('set_frequency'))
    if debug:
        print_message('start_year: {sy},\n     end_year: {ey},\n     set_frequency: {freq},\n     number of year_sets: {ys}'.format(
            sy=config.get('simulation_start_year'),
            ey=config.get('simulation_end_year'),
            ys=year_sets,
            freq=config.get('set_frequency')
        ), 'ok')
    job_sets = [{'status': 'no data', 'year_set': i} for i in range(1, year_sets+1)]
    start_year = config.get("simulation_start_year")
    end_year = config.get("simulation_end_year")
    sim_length = end_year - start_year
    for year in range(1, sim_length + 1):
        for month in range(1, 13):
            key = str(year) + '-' + str(month)
            file_list[key] = 'no data'

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




