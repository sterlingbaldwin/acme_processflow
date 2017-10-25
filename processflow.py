# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301

import argparse
import json
import sys
import os
import threading
import logging
import time
import stat

from shutil import rmtree
from shutil import move
from shutil import copyfile
from getpass import getpass
from time import sleep
from uuid import uuid4
from pprint import pformat
from datetime import datetime

from globus_cli.services.transfer import get_client

from jobs.Transfer import Transfer
from jobs.JobStatus import JobStatus

from lib.YearSet import YearSet, SetStatus
from lib.mailer import Mailer
from lib.events import Event, Event_list
from lib.setup import setup, finishup
from lib.filemanager import FileManager
from lib.runmanager import RunManager
from lib.display import start_display
from lib.util import *

# check for NCL
if not os.environ.get('NCARG_ROOT'):
    ncar_path = '/usr/local/src/NCL-6.3.0/'
    if os.path.exists(ncar_path):
        os.environ['NCARG_ROOT'] = ncar_path
    else:
        print 'No NCARG_ROOT found in environment variables, make sure NCL installed on the machine and add its path to your ~/.bashrc'
        sys.exit()

# set variable to make vcs shut up
os.environ['UVCDAT_ANONYMOUS_LOG'] = 'False'

# create global Event_list
event_list = Event_list()

def main(test=False, **kwargs):
    # The master configuration object
    config = {}

    # A list of all the threads
    thread_list = []

    # An event to kill the threads on terminal exception
    thread_kill_event = threading.Event()
    mutex = threading.Lock()
    display_event = threading.Event()
    debug = False
    from_saved_state = False

    # A flag to tell if we have all the data locally
    all_data = False
    all_data_remote = False

    # get a globus client
    client = get_client()

    # Read in parameters from config
    args = kwargs['testargs'] if test else sys.argv[1:]
    config, filemanager, runmanager = setup(
        args,
        display_event,
        event_list=event_list,
        thread_list=thread_list,
        kill_event=thread_kill_event,
        mutex=mutex)

    if isinstance(config, int):
        print "Error in setup, exiting"
        return -1
    logging.info('Config setup complete')
    logging.info(str(config))

    # check that all netCDF files exist
    path_exists(config)
    # cleanup any temp directories from previous runs
    # cleanup(config)
    if not os.path.exists(config['global']['run_scripts_path']):
        os.makedirs(config['global']['run_scripts_path'])
    if not os.path.exists(config['global']['tmp_path']):
        os.makedirs(config['global']['tmp_path'])

    if config['global'].get('ui'):
        try:
            sys.stdout.write('Turning on the display')
            for i in range(8):
                sys.stdout.write('.')
                sys.stdout.flush()
                sleep(0.1)
            print '\n'
            args = (display_event, runmanager.job_sets)
            diaplay_thread = threading.Thread(
                target=start_display,
                args=args)
            diaplay_thread.start()

        except KeyboardInterrupt as e:
            print 'keyboard exit'
            display_event.set()
            return -1

    state_path = os.path.join(
        config.get('global').get('output_path'),
        'run_state.txt')
    filemanager.update_local_status()
    all_data = filemanager.all_data_local()
    if not all_data:
        filemanager.update_remote_status(client)
        all_data_remote = filemanager.all_data_remote()
    write_human_state(
        event_list=event_list,
        job_sets=runmanager.job_sets,
        state_path=state_path,
        print_file_list=config['global'].get('print_file_list'),
        mutex=mutex)

    dryrun = config['global'].get('dry_run')
    if dryrun:
        runmanager.dryrun(True)
        event_list.push(message='Running in dry-run mode')
        write_human_state(
            event_list=event_list,
            job_sets=job_sets,
            state_path=state_path,
            print_file_list=config['global'].get('print_file_list'),
            mutex=mutex)
        if config['global'].get('ui'):
            sleep(50)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join(timeout=1.0)
            return -1

    # check if the case_scripts directory is present
    # if its not, transfer it over
    case_scripts_dir = os.path.join(
        config['global']['input_path'],
        'case_scripts')

    if not os.path.exists(case_scripts_dir) \
       and not config['global']['no_monitor']:
        msg = 'case_scripts not local, transfering remote copy'
        print msg
        event_list.push(message=msg)
        logging.info(msg)
        src_path = os.path.join(config['global']['source_path'], 'case_scripts')
        while True:
            try:
                args = {
                    'source_endpoint': config['transfer']['source_endpoint'],
                    'destination_endpoint': config['transfer']['destination_endpoint'],
                    'src_path': src_path,
                    'dst_path': case_scripts_dir,
                    'event_list': event_list,
                    'event': thread_kill_event
                }
                thread = threading.Thread(
                    target=transfer_directory,
                    name='transfer_directory',
                    kwargs=args)
            except:
                sleep(1)
            else:
                thread_list.append(thread)
                thread.start()
                break

    # Main loop
    remote_check_delay = 60
    local_check_delay = 2
    printed = False
    try:
        loop_count = 0
        print "--- Entering main loop ---"
        print "Current status can be found at {}".format(state_path)
        while True:
            # Check the remote status once every 5 minutes
            if loop_count == remote_check_delay:
                loop_count = 0
                if config.get('global').get('no_monitor', False):
                    loop_count += 1
                    continue
                if not all_data_remote:
                    all_data_remote = filemanager.all_data_remote()
                if not all_data_remote:
                    print 'Updating remote status'
                    filemanager.update_remote_status(client)
                if not all_data:
                    all_data = filemanager.all_data_local()
                if not all_data or not all_data_remote:
                    print 'Updating local status'
                    filemanager.update_local_status()
            # check the local status every 10 seconds
            if loop_count == local_check_delay:
                if not all_data:
                    all_data = filemanager.all_data_local()
                else:
                    if not printed:
                        print 'All data local, turning off remote checks'
                        printed = True
                if not all_data \
                and not config['global']['no_monitor']:
                    transfer_started = filemanager.transfer_needed(
                        event_list=event_list,
                        event=thread_kill_event,
                        remote_endpoint=config['transfer']['source_endpoint'],
                        ui=config['global']['ui'],
                        display_event=display_event,
                        emailaddr=config['global']['email'],
                        thread_list=thread_list)
                    if transfer_started:
                        print 'starting file transfer'

            filemanager.check_year_sets(runmanager.job_sets)
            runmanager.start_ready_job_sets()
            runmanager.monitor_running_jobs()
            write_human_state(
                event_list=event_list,
                job_sets=runmanager.job_sets,
                state_path=state_path,
                print_file_list=config.get('global').get('print_file_list'),
                mutex=mutex)
            status = runmanager.is_all_done()
            if status >= 0:
                first_print = True
                while not filemanager.all_data_local():
                    if first_print:
                        print "All jobs complete, moving additional files"
                        first_print = False
                    started = filemanager.transfer_needed(
                        event_list=event_list,
                        event=thread_kill_event,
                        remote_endpoint=config['transfer']['source_endpoint'],
                        ui=config['global']['ui'],
                        display_event=display_event,
                        emailaddr=config['global']['email'],
                        thread_list=thread_list)
                    if not started:
                        sleep(5)
                    else:
                        print "Transfer started"
                finishup(
                    config=config,
                    job_sets=runmanager.job_sets,
                    state_path=state_path,
                    event_list=event_list,
                    status=status,
                    display_event=display_event,
                    thread_list=thread_list,
                    kill_event=thread_kill_event)
                # SUCCESS EXIT
                return 0
            sleep(5)
            loop_count += 1
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERRUPT -----')
        print_message('----- cleaning up threads ---', 'ok')
        event_list.push(message="Exiting due to keyboard interrupt")
        write_human_state(
            event_list=event_list,
            job_sets=runmanager.job_sets,
            state_path=state_path,
            print_file_list=True,
            mutex=mutex)
        display_event.set()
        thread_kill_event.set()
        for thread in thread_list:
            thread.join(timeout=1.0)

if __name__ == "__main__":
    if sys.argv[1] == 'test':
        config_path = os.path.join(os.getcwd(), 'tests', 'test_run_no_sta.cfg')
        testargs = ['-c', config_path, '-n', '-f']
        ret = main(test=True, testargs=testargs)
    else:
        ret = main()
    sys.exit(ret)