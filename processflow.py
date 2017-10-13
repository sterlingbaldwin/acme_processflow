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
import curses
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
from lib.util import *

# setup argument parser
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file.')
parser.add_argument('-n', '--no-ui', help='Turn off the GUI.', action='store_true')
parser.add_argument('-l', '--log', help='Path to logging output file.')
parser.add_argument('-u', '--no-cleanup', help='Don\'t perform pre or post run cleanup. This will leave all run scripts in place.', action='store_true')
parser.add_argument('-m', '--no-monitor', help='Don\'t run the remote monitor or move any files over globus.', action='store_true')
parser.add_argument('-f', '--file-list', help='Turn on debug output of the internal file_list so you can see what the current state of the model files are', action='store_true')
parser.add_argument('-r', '--resource-dir', help='Path to custom resource directory')

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

def xy_check(x, y, hmax, wmax):
    if y >= hmax or x >= wmax:
        return -1
    else:
        return 0

def write_line(pad, line, x=None, y=None, color=None):
    if not color:
        color = curses.color_pair(4)
    try:
        pad.addstr(y, x, line, color)
    except:
        pass

def display(stdscr, event, config):
    """
    Display current execution status via curses
    """

    # setup variables
    initializing = True
    height, width = stdscr.getmaxyx()
    hmax = height - 3
    wmax = width - 5
    spinner = ['\\', '|', '/', '-']
    spin_index = 0
    spin_len = 4
    try:
        # setup curses
        stdscr.nodelay(True)
        curses.curs_set(0)
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_WHITE)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(5, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(6, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
        curses.init_pair(7, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_BLACK)
        stdscr.bkgd(curses.color_pair(8))

        pad = curses.newpad(hmax, wmax)
        last_y = 0
        while True:
            # Check if screen was re-sized (True or False)
            resize = curses.is_term_resized(height, width)

            # Action in loop if resize is True:
            if resize is True:
                height, width = stdscr.getmaxyx()
                hmax = height - 3
                wmax = width - 5    
                stdscr.clear()
                curses.resizeterm(height, width)
                stdscr.refresh()
                pad = curses.newpad(hmax, wmax)
                try:
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                except:
                    sleep(0.5)
                    continue
            now = datetime.now()
            # sleep until there are jobs
            if len(job_sets) == 0:
                sleep(1)
                continue
            pad.refresh(0, 0, 3, 5, hmax, wmax)
            pad.clear()
            y = 0
            x = 0
            for year_set in job_sets:
                line = 'Year_set {num}: {start} - {end}'.format(
                    num=year_set.set_number,
                    start=year_set.set_start_year,
                    end=year_set.set_end_year)
                try:
                    pad.addstr(y, x, line, curses.color_pair(1))
                except:
                    continue
                pad.clrtoeol()
                y += 1

                color_pair = curses.color_pair(4)
                if year_set.status == SetStatus.COMPLETED:
                    # set color to green
                    color_pair = curses.color_pair(5)
                elif year_set.status == SetStatus.FAILED:
                    # set color to red
                    color_pair = curses.color_pair(3)
                elif year_set.status == SetStatus.RUNNING:
                    # set color to purple
                    color_pair = curses.color_pair(6)
                line = 'status: {status}'.format(
                    status=year_set.status)
                try:
                    pad.addstr(y, x, line, color_pair)
                except:
                    continue
                if initializing:
                    sleep(0.01)
                    try:
                        pad.refresh(0, 0, 3, 5, hmax, wmax)
                    except:
                        continue
                pad.clrtoeol()
                y += 1

                # if the job_set is done collapse it
                if year_set.status == SetStatus.COMPLETED \
                    or year_set.status == SetStatus.NO_DATA \
                    or year_set.status == SetStatus.PARTIAL_DATA:
                    continue
                for job in year_set.jobs:
                    line = '  >   {type} -- {id} '.format(
                        type=job.get_type(),
                        id=job.job_id)
                    try:
                        pad.addstr(y, x, line, curses.color_pair(4))
                    except:
                        continue
                    color_pair = curses.color_pair(4)
                    if job.status == JobStatus.COMPLETED:
                        color_pair = curses.color_pair(5)
                    elif job.status in [JobStatus.FAILED, 'CANCELED', JobStatus.INVALID]:
                        color_pair = curses.color_pair(3)
                    elif job.status == JobStatus.RUNNING:
                        color_pair = curses.color_pair(6)
                    elif job.status == JobStatus.SUBMITTED or job.status == JobStatus.PENDING:
                        color_pair = curses.color_pair(7)
                    # if the job is running, print elapsed time
                    if job.status == JobStatus.RUNNING:
                        delta = now - job.start_time
                        deltastr = strfdelta(delta, "{H}:{M}:{S}")
                        #deltastr = str(delta)
                        line = '{status} elapsed time: {time}'.format(
                            status=job.status,
                            time=deltastr)
                    # if job has ended, print total time
                    elif job.status in [JobStatus.COMPLETED, JobStatus.FAILED] \
                         and job.end_time \
                         and job.start_time:
                        delta = job.end_time - job.start_time
                        line = '{status} elapsed time: {time}'.format(
                            status=job.status,
                            time=strfdelta(delta, "{H}:{M}:{S}"))
                    else:
                        line = '{status}'.format(status=job.status)
                    try:
                        pad.addstr(line, color_pair)
                    except:
                        continue
                    pad.clrtoeol()
                    if initializing:
                        sleep(0.01)
                        pad.refresh(0, 0, 3, 5, hmax, wmax)
                    y += 1

            x = 0
            if last_y:
                y = last_y
            pad.clrtobot()
            y += 1
            events = event_list.list
            for line in events[-10:]:
                if 'Transfer' in line.message:
                    continue
                if 'hosted' in line.message:
                    continue
                if 'failed' in line.message or 'FAILED' in line.message:
                    prefix = '[-]  '
                    try:
                        pad.addstr(y, x, prefix, curses.color_pair(4))
                    except:
                        continue
                else:
                    prefix = '[+]  '
                    try:
                        pad.addstr(y, x, prefix, curses.color_pair(5))
                    except:
                        continue
                try:
                    pad.addstr(y, x, line.message, curses.color_pair(4))
                except:
                    continue
                pad.clrtoeol()
                if initializing:
                    sleep(0.01)
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                y += 1
                if xy_check(x, y, hmax, wmax) == -1:
                    sleep(1)
                    break
            pad.clrtobot()
            y += 1
            if xy_check(x, y, hmax, wmax) == -1:
                sleep(1)
                continue

            file_start_y = y
            file_end_y = y
            file_display_list = []
            current_year = 1
            year_ready = True
            partial_data = False

            y = file_end_y + 1
            x = 0
            msg = 'Active transfers: {}'.format(active_transfers)
            try:
                pad.addstr(y, x, msg, curses.color_pair(4))
            except:
                pass
            pad.clrtoeol()
            if active_transfers:
                for line in events:
                    if 'Transfer' in line.message:
                        index = line.message.find('%')
                        if index:
                            s_index = line.message.rfind(' ', 0, index)
                            percent = float(line.message[s_index: index])
                            if percent < 100:
                                y += 1
                                try:
                                    pad.addstr(y, x, line.message, curses.color_pair(4))
                                except:
                                    pass
                                pad.clrtoeol()
            for line in events:
                if 'hosted' in line.message:
                    y += 1
                    try:
                        pad.addstr(y, x, line.message, curses.color_pair(4))
                    except:
                        pass
            spin_line = spinner[spin_index]
            spin_index += 1
            if spin_index == spin_len:
                spin_index = 0
            y += 1
            try:
                pad.addstr(y, x, spin_line, curses.color_pair(4))
            except:
                pass
            pad.clrtoeol()
            pad.clrtobot()
            y += 1
            if event and event.is_set():
                # enablePrint()
                return
            try:
                pad.refresh(0, 0, 3, 5, hmax, wmax)
            except:
                pass
            initializing = False
            sleep(1)

    except KeyboardInterrupt as e:
        raise

def sigwinch_handler(n, frame):
    curses.endwin()
    curses.initscr()

def start_display(config, event):
    try:
        curses.wrapper(display, event, config)
    except KeyboardInterrupt as e:
        return

if __name__ == "__main__":

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

    # get a globus client
    client = get_client()

    # Read in parameters from config
    config, filemanager, runmanager = setup(
        parser,
        display_event,
        event_list=event_list,
        thread_list=thread_list,
        kill_event=thread_kill_event,
        mutex=mutex)

    if config == -1:
        print "Error in setup, exiting"
        sys.exit(1)
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

    if config.get('global').get('ui', False):
        try:
            sys.stdout.write('Turning on the display')
            for i in range(8):
                sys.stdout.write('.')
                sys.stdout.flush()
                sleep(0.1)
            print '\n'
            diaplay_thread = threading.Thread(target=start_display, args=(config, display_event))
            diaplay_thread.start()

        except KeyboardInterrupt as e:
            print 'keyboard exit'
            display_event.set()
            sys.exit()

    state_path = os.path.join(
        config.get('global').get('output_path'),
        'run_state.txt')
    filemanager.update_local_status()
    all_data = filemanager.all_data_local()
    if not all_data:
        filemanager.update_remote_status(client)
    write_human_state(
        event_list=event_list,
        job_sets=runmanager.job_sets,
        state_path=state_path,
        ui_mode=config.get('global').get('ui'),
        print_file_list=config.get('global').get('print_file_list'),
        types=filemanager.types,
        mutex=mutex)

    if config.get('global').get('dry_run', False):
        event_list.push(message='Running in dry-run mode')
        write_human_state(
            event_list=event_list,
            job_sets=job_sets,
            state_path=state_path,
            ui_mode=config.get('global').get('ui'),
            print_file_list=config.get('global').get('print_file_list'),
            types=filemanager.types,
            mutex=mutex)
        if config.get('global').get('ui'):
            sleep(50)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join(timeout=1.0)
            sys.exit()

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

    try:
        loop_count = 0
        print "--- Entering main loop ---"
        print "Current status can be found at {}".format(state_path)
        while True:
            # Check the remote status once every 5 minutes
            if not all_data \
            and not config.get('global').get('no_monitor', False) \
            and loop_count == remote_check_delay:
                print 'Updating remote status'
                filemanager.update_remote_status(client)
                filemanager.update_local_status()
                loop_count = 0
            # check the local status every 10 seconds
            if loop_count % local_check_delay == 0 \
            and not all_data:
                all_data = filemanager.all_data_local()
                if not all_data \
                and filemanager.active_transfers < 2 \
                and not config['global']['no_monitor']:
                    print 'starting file transfer'
                    filemanager.transfer_needed(
                        event_list=event_list,
                        event=thread_kill_event,
                        remote_endpoint=config['transfer']['source_endpoint'],
                        ui=config['global']['ui'],
                        display_event=display_event,
                        emailaddr=config['global']['email'],
                        thread_list=thread_list)
                if all_data:
                    print 'All data local, turning off remote checks'
            filemanager.check_year_sets(runmanager.job_sets)
            runmanager.start_ready_job_sets()
            runmanager.monitor_running_jobs()
            write_human_state(
                event_list=event_list,
                job_sets=runmanager.job_sets,
                state_path=state_path,
                ui_mode=config.get('global').get('ui'),
                print_file_list=config.get('global').get('print_file_list'),
                types=filemanager.types,
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
                        sleep(30)
                    else:
                        print "Transfering additional files"
                finishup(
                    config=config,
                    job_sets=runmanager.job_sets,
                    state_path=state_path,
                    event_list=event_list,
                    status=status,
                    display_event=display_event,
                    thread_list=thread_list,
                    kill_event=thread_kill_event)
                sys.exit(0)
            sleep(5)
            loop_count += 1
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        print_message('----- cleaning up threads ---', 'ok')
        display_event.set()
        thread_kill_event.set()
        for thread in thread_list:
            thread.join(timeout=1.0)
