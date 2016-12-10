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
    args = parser.parse_args()
    if args.debug:
        debug = True
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

def monitor_check():
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
    monitor.check()
    print_message('Found new files: {}, setting up transfer'.format(monitor.get_new_files()), 'ok')
    if monitor.get_new_files():
        if debug:
            print "New remote files found {}, starting transfer".format(monitor.get_new_files)

        # find which year set the data belongs to
        for f in monitor.get_new_files():
            index = re.search("[0-9][0-9][0-9][0-9]", f).start()
            year = int(f[index: index + 4])
            year_set = int(floor(year / config.get('set_frequency')))
            if year_sets[year_set].status == 'no data':
                # Change year set state
                year_sets[year_set].status = 'data in transit'
                # Spawn jobs for that yearset
                climo = Climo({
                    'start_year': year_set,
                    'end_year': year_set + 5,
                    'caseId': config.get('experiment'),
                    'anual_mode': 'sdd',
                    'regrid_map_path': os.getcwd() + '/resources/map_ne30np4_to_fv129x256_aave.20150901.nc',
                    'input_directory': os.getcwd() + '/cache/',
                    'climo_output_directory': os.getcwd() + '/output',
                    'regrid_output_directory': os.getcwd() + 'output/regrid/',
                    'yearset': year_set
                })
                job_counter += 1
                job_list[job_counter] = climo
                diag = Diagnostic({
                    '--model': os.getcwd() + '/cache',
                    '--obs': os.getcwd() + '/resources/obs_for_diagnostics',
                    '--outputdir': os.getcwd() + '/output/diagnostics',
                    '--package': 'amwg',
                    '--set': '5',
                    '--archive': 'False',
                    'yearset': year_set
                })
                job_counter += 1
                job_list[job_counter] = diag


        file_list = ['{path}/{file}'.format(path=config.get('source_path'), file=f)  for f in monitor.get_new_files()]
        tmpdir = os.getcwd() + '/tmp/'
        t = Transfer({
            'file_list': file_list,
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
        thread = threading.Thread(target=handle_transfer, args=t)
        thread.start()

def handle_transfer(transfer_job):
    if debug:
        print_message("starting transfer job", 'ok')
    transfer_job.execute()
    for file in monitor.get_new_files():
        copy(
            src='{tmp}/{file}'.format(tmp=tmpdir, file=file),
            dst=config.get('data_cache_path') + '/'
        )
    rmtree(tmpdir)
    if debug:
        print_message("transfer job complete", 'ok')

if __name__ == "__main__":
    global debug
    global job_list
    global job_counter
    global config
    debug = False
    # Initialize the job list
    job_list = {}
    job_counter = 0

    # Read in parameters from config
    config = setup(parser)
    if config == -1:
        print "Error in setup, exiting"
        sys.exit()
    year_sets = (int(config.get('simulation_end_year')) - int(config.get('simulation_start_year'))) / int(config.get('set_frequency'))
    job_sets = [{'status': 'no data'} for i in range(year_sets)]
    
    # Main loop
    while True:
        # Setup remote monitoring system
        monitor_check()
        # Check if a year_set is ready to run
        sleep(10)




