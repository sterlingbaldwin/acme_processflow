# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301

import sys
import os
import json
import threading
import logging

from time import sleep
from globus_cli.services.transfer import get_client

from lib.events import EventList
from lib.initialize import initialize
from lib.finalize import finalize
from lib.filemanager import FileManager
from lib.runmanager import RunManager
from lib.util import print_line
from lib.util import print_message
from lib.util import print_debug

__version__ = '2.0.0'
__branch__ = 'manymodel'

# set variable to make vcs shut up
os.environ['UVCDAT_ANONYMOUS_LOG'] = 'False'
os.environ['NCO_PATH_OVERRIDE'] = 'Yes'

# create global EventList
event_list = EventList()


def main(test=False, **kwargs):
    """
    Processflow main
    """

    # The master configuration object
    config = {}

    # An event to kill the threads on terminal exception
    thread_kill_event = threading.Event()
    mutex = threading.Lock()

    # A flag to tell if we have all the data locally
    all_data = False
    all_data_remote = False

    # get a globus client
    client = get_client()

    # Read in parameters from config
    if test:
        print '=========================================='
        print '---- Processflow running in test mode ----'
        print '=========================================='
        _args = kwargs['testargs']
        config, filemanager, runmanager = initialize(
            argv=_args,
            version=__version__,
            branch=__branch__,
            event_list=event_list,
            kill_event=thread_kill_event,
            mutex=mutex,
            testing=True)
    else:
        config, filemanager, runmanager = initialize(
            argv=sys.argv[1:],
            version=__version__,
            branch=__branch__,
            event_list=event_list,
            kill_event=thread_kill_event,
            mutex=mutex)
    # setup returned an error code
    if isinstance(config, int):
        print "Error in setup, exiting"
        return -1
    logging.info('Config setup complete')
    debug = True if config['global'].get('debug') else False

    msg = "Updating local file status"
    print_line(
        line=msg,
        event_list=event_list)
    filemanager.update_local_status()
    all_data_local = filemanager.all_data_local()
    if not all_data_local:
        filemanager.transfer_needed(
            event_list=event_list,
            event=thread_kill_event)
    
    # msg = "Writing human readable state to file"
    # print_line(msg, event_list)

    # check if the case_scripts directory is present
    # if its not, transfer it over
    if config['global'].get('get_scripts'):
        pass
        # msg = 'transfering case_scripts from remote machine'
        # print_line(
        #     line=msg,
        #     event_list=event_list)
        # case_scripts_dir = os.path.join(
        #     config['global']['input_path'],
        #     'case_scripts')

        # if not os.path.exists(case_scripts_dir):
        #     logging.info(msg)
        #     src_path = os.path.join(
        #         config['global']['source_path'], 'case_scripts')
        #     while True:
        #         try:
        #             args = {
        #                 'source_endpoint': config['transfer']['source_endpoint'],
        #                 'destination_endpoint': config['transfer']['destination_endpoint'],
        #                 'src_path': src_path,
        #                 'dst_path': case_scripts_dir,
        #                 'event_list': event_list,
        #                 'event': thread_kill_event
        #             }
        #             thread = threading.Thread(
        #                 target=transfer_directory,
        #                 name='transfer_directory',
        #                 kwargs=args)
        #         except:
        #             sleep(1)
        #         else:
        #             thread_list.append(thread)
        #             thread.start()
        #             break

    # Main loop
    printed = False
    loop_delay = 10
    state_path = os.path.join(
        config['global']['project_path'],
        'output',
        'state.txt')
    try:
        print "--------------------------"
        print " Entering Main Loop "
        print " Status file: {}".format(state_path)
        print "--------------------------"
        while True:
            if not all_data_local:
                if debug: print_line(' -- Updating local status --', event_list)    

                if filemanager.update_local_status():
                    msg = filemanager.report_files_local()
                    print_line(msg, event_list)
                    filemanager.write_database()
                all_data_local = filemanager.all_data_local()
            if not all_data_local:
                if debug: print_line(' -- Additional data needed --', event_list)
                filemanager.transfer_needed(
                    event_list,
                    thread_kill_event)

            if debug: print_line(' -- checking data -- ', event_list)
            runmanager.check_data_ready()
            if debug: print_line(' -- starting ready jobs --', event_list)
            runmanager.start_ready_jobs()
            if debug: print_line(' -- monitoring running jobs --', event_list)
            runmanager.monitor_running_jobs()

            if debug: print_line(' -- writing out state -- ', event_list)
            runmanager.write_job_sets(state_path)
            
            status = runmanager.is_all_done()
            # return -1 if still running
            # return 0 if a jobset failed
            # return 1 if all complete
            if status >= 0:
                msg = "Finishing up run"
                print_line(msg, event_list)

                printed = False
                while not filemanager.all_data_local():
                    if not printed:
                        printed = True
                        msg = 'Jobs are complete, but additional data is being transfered'
                        print_line(msg, event_list)
                    filemanager.update_local_status()
                    if not filemanager.all_data_local():
                        filemanager.transfer_needed(
                            event_list=event_list,
                            event=thread_kill_event)
                    sleep(10)
                filemanager.write_database()
                finalize(
                    config=config,
                    event_list=event_list,
                    status=status,
                    kill_event=thread_kill_event,
                    runmanager=runmanager)
                # SUCCESS EXIT
                return 0
            if debug: print_line(' -- sleeping', event_list)
            sleep(loop_delay)
    except KeyboardInterrupt as e:
        print_message('\n----- KEYBOARD INTERRUPT -----')
        runmanager.write_job_sets(state_path)
        filemanager.terminate_transfers()
        print_message('-----  cleanup complete  -----', 'ok')
    except Exception as e:
        print_message('----- AN UNEXPECTED EXCEPTION OCCURED -----')
        print_debug(e)
        runmanager.write_job_sets(state_path)
        filemanager.terminate_transfers()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        ret = main(test=True, testargs=['-h'])
    else:
        ret = main()
    sys.exit(ret)
