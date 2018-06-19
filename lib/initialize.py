import sys
import os
import logging
import json
import time
import stat
import argparse

from pprint import pformat
from shutil import rmtree, copy
from configobj import ConfigObj
from shutil import copyfile

from YearSet import YearSet, SetStatus
from filemanager import FileManager
from runmanager import RunManager
from mailer import Mailer
from JobStatus import JobStatus
from globus_interface import setup_globus
from util import print_message
from util import print_debug
from util import print_line
from verify_config import verify_config, check_config_white_space

def parse_args(argv=None, print_help=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file.')
    parser.add_argument(
        '-v', '--version',
        help='Print version informat and exit.',
        action='store_true')
    parser.add_argument(
        '-l', '--log',
        help='Path to logging output file.')
    parser.add_argument(
        '-n', '--no-host',
        help='Don\'t move output plots into the web host directory.',
        action='store_true')
    parser.add_argument(
        '-s', '--scripts',
        help='Copy the case_scripts directory from the remote machine.',
        action='store_true')
    parser.add_argument(
        '-f', '--file-list',
        help='Turn on debug output of the internal file_list so you can see what the current state of the model files are',
        action='store_true')
    parser.add_argument(
        '-r', '--resource-path',
        help='Path to custom resource directory')
    parser.add_argument(
        '-a', '--always-copy',
        help='Always copy diagnostic output, even if the output already exists in the host directory. This is much slower but ensures old output will be overwritten',
        action='store_true')
    parser.add_argument(
        '-d', '--debug',
        help='Set log level to debug',
        action='store_true')
    parser.add_argument(
        '--dryrun',
        help='Do everything up to starting the jobs, but dont start any jobs',
        action='store_true')
    parser.add_argument(
        '-m', '--max-jobs',
        help='maximum number of running jobs',
        type=int)
    if print_help:
        parser.print_help()
        return
    return parser.parse_args(argv)


def initialize(argv, **kwargs):
    """
    Parse the commandline arguments, and setup the master config dict

    Parameters:
        parser (argparse.ArgumentParser): The parser object
        event_list (EventList): The main list of events
        thread_list (list): the main list of all running threads
        mutex (threading.Lock): A mutex to handle db access
        kill_event (threading.Event): An event used to kill all running threads
        version (str): the current version number for processflow
        branch (str): the branch this version was built from
    """
    # Setup the parser
    args = parse_args(argv=argv)
    if args.version:
        msg = 'Processflow version {}'.format(kwargs['version'])
        print msg
        sys.exit()
    if not args.config:
        parse_args(print_help=True)
        return False, False, False
    event_list = kwargs['event_list']
    thread_list = kwargs['thread_list']
    mutex = kwargs['mutex']
    event = kwargs['kill_event']
    print_line(
        line='Entering setup',
        event_list=event_list)

    # check if globus config is valid, else remove it
    globus_config = os.path.join(os.path.expanduser('~'), '.globus.cfg')
    if os.path.exists(globus_config):
        try:
            conf = ConfigObj(globus_config)
        except:
            os.remove(globus_config)

    if not os.path.exists(args.config):
        print "Invalid config, {} does not exist".format(args.config)
        return False, False, False

    # Check that there are no white space errors in the config file
    line_index = check_config_white_space(args.config)
    if line_index != 0:
        print '''
ERROR: line {num} does not have a space after the \'=\', white space is required.
Please add a space and run again.'''.format(num=line_index)
        return False, False, False

    # read the config file and setup the config dict
    try:
        config = ConfigObj(args.config)
    except Exception as e:
        print_debug(e)
        print "Error parsing config file {}".format(args.config)
        parse_args(print_help=True)
        return False, False, False

    # run validator for config file
    if config.get('global'):
        if args.resource_path:
            config['global']['resource_path'] = args.resource_path
        else:
            config['global']['resource_path'] = os.path.join(
                sys.prefix,
                'share',
                'processflow',
                'resources')
    else:
        return False, False, False

    # Setup boolean config flags
    config['global']['host'] = False if args.no_host else True
    config['global']['print_file_list'] = True if args.file_list else False
    config['global']['scripts'] = True if args.scripts else False
    config['global']['always_copy'] = True if args.always_copy else False
    config['global']['dryrun'] = True if args.dryrun else False
    config['global']['debug'] = True if args.debug else False
    config['global']['max_jobs'] = args.max_jobs if args.max_jobs else False

    if args.max_jobs:
        print_line(
            line="running with maximum {} jobs".format(args.max_jobs),
            event_list=event_list)

    messages = verify_config(config)
    if messages:
        for message in messages:
            print_message(message)
        return False, False, False
    logging.info("Running with config:")
    msg = json.dumps(config, sort_keys=False, indent=4)
    logging.info(msg)

    if args.no_host or not config.get('img_hosting'):
        print_line(
            line='Not hosting img output',
            event_list=event_list)
    
    # setup input and output directories
    input_path = os.path.join(
        config['global']['project_path'],
        'input')
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    output_path = os.path.join(
        config['global']['project_path'],
        'output')
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # setup logging
    if args.log:
        log_path = args.log
    else:
        log_path = os.path.join(
            config['global']['project_path'],
            'output',
            'processflow.log')
    print_line(
        line='Log saved to {}'.format(log_path),
        event_list=event_list)
    if not kwargs.get('testing'):
        from imp import reload
        reload(logging)
    config['global']['log_path'] = log_path
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=log_path,
        filemode='w',
        level=log_level)
    logging.getLogger('globus_sdk').setLevel(logging.ERROR)
    logging.getLogger('globus_cli').setLevel(logging.ERROR)

    msg = 'processflow version {} branch {}'.format(
        kwargs['version'],
        kwargs['branch'])
    logging.info(msg)

    try:
        setup_directories(args, config)
    except Exception as e:
        print_message('Failed to setup directories')
        print_debug(e)
        sys.exit(1)

    # Copy the config into the input directory for safe keeping
    input_config_path = os.path.join(
        config['global']['project_path'], 
        'input', 'run.cfg')
    try:
        copy(args.config, input_config_path)
    except:
        pass

    if config['global']['always_copy']:
        msg = 'Running in forced-copy mode, all previous diagnostic output will be overwritten'
    else:
        msg = 'Running without forced-copy, previous diagnostic output will be preserved'
    print_line(
        line=msg,
        event_list=event_list)

    # initialize the filemanager
    db = os.path.join(
        config['global'].get('project_path'),
        'output',
        'processflow.db')
    msg = 'Initializing file manager'
    print_line(msg, event_list)
    filemanager = FileManager(
        database=db,
        event_list=event_list,
        mutex=mutex,
        config=config)
    
    filemanager.populate_file_list()
    msg = 'Starting local status update'
    print_line(msg, event_list)

    filemanager.update_local_status()
    msg = 'Local status update complete'
    print_line(msg, event_list)

    msg = filemanager.report_files_local()
    print_line(msg, event_list)

    filemanager.write_database()
    all_data = filemanager.all_data_local()
    if all_data:
        msg = 'all data is local'
    else:
        msg = 'Additional data needed'
    print_line(msg, event_list)

    logging.info("FileManager setup complete")
    logging.info(str(filemanager))

    if all_data:
        print_line(
            line="skipping globus setup",
            event_list=event_list)
    else:
        endpoints = [endpoint for endpoint in filemanager.get_endpoints()]
        local_endpoint = config['global'].get('local_globus_uuid')
        if local_endpoint:
            endpoints.append(local_endpoint)
        addr = config['global'].get('email')
        msg = 'Checking authentication for {} endpoints'.format(endpoints)
        print_line(line=msg, event_list=event_list)
        setup_success = setup_globus(
            endpoints=endpoints,
            event_list=event_list)

        if not setup_success:
            print "Globus setup error"
            return False, False, False
        else:
            print_line(
                line='Globus authentication complete',
                event_list=event_list)
    # setup the runmanager
    runmanager = RunManager(
        event_list=event_list,
        thread_list=thread_list,
        event=event,
        config=config,
        filemanager=filemanager)
    runmanager.setup_cases()
    runmanager.setup_jobs()
    runmanager.write_job_sets(
        os.path.join(config['global']['project_path'],
        'output', 'state.txt'))
    return config, filemanager, runmanager

def setup_directories(_args, config):
    """
    Setup the input, output, pp, and diags directories
    """
    # setup output directory

    output_path = os.path.join(
        config['global']['project_path'],
        'output')
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # setup input directory
    input_path = os.path.join(
        config['global']['project_path'],
        'input')
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_input = os.path.join(input_path, sim)
        if not os.path.exists(sim_input):
            os.makedirs(sim_input)

    # setup post processing dir
    pp_path = os.path.join(output_path, 'pp')
    config['global']['pp_path'] = pp_path
    if not os.path.exists(pp_path):
        os.makedirs(pp_path)

    # setup diags dir
    diags_path = os.path.join(output_path, 'diags')
    config['global']['diags_path'] = diags_path
    if not os.path.exists(diags_path):
        os.makedirs(diags_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_diags = os.path.join(diags_path, config['simulations'][sim]['short_name'])
        if not os.path.exists(sim_diags):
            os.makedirs(sim_diags)

    # setup run_scripts_path
    run_script_path = os.path.join(
        output_path,
        'scripts')
    config['global']['run_scripts_path'] = run_script_path
    if not os.path.exists(run_script_path):
        os.makedirs(run_script_path)

