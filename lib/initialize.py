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
from lib.filemanager import FileManager
from lib.runmanager import RunManager
from lib.mailer import Mailer
from jobs.JobStatus import JobStatus
from lib.globus_interface import setup_globus
from util import print_message
from util import print_debug
from util import print_line


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
        '-u', '--ui',
        help='Turn on the GUI.',
        action='store_true')
    parser.add_argument(
        '-l', '--log',
        help='Path to logging output file.')
    parser.add_argument(
        '-n', '--no-host',
        help='Don\'t move output plots into the web host directory.',
        action='store_true')
    parser.add_argument(
        '-m', '--no-monitor',
        help='Don\'t run the remote monitor or move any files over globus.',
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
        '-r', '--resource-dir',
        help='Path to custom resource directory')
    parser.add_argument(
        '-i', '--input-path',
        help='Custom input path')
    parser.add_argument(
        '-o', '--output-path',
        help='Custom output path')
    parser.add_argument(
        '-a', '--always-copy',
        help='Always copy diagnostic output, even if the output already exists in the host directory. This is much slower but ensures old output will be overwritten',
        action='store_true')
    parser.add_argument(
        '--custom-archive-path',
        help='A custom remote archive path used for short term archiving. This will over rule the normal path interpolation when moving files. This option should only be used when short term archiving is turned on',
        action='store')
    parser.add_argument(
        '-d', '--debug',
        help='Set log level to debug',
        action='store_true')
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
        event_list=event_list,
        current_state=True)

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
        if args.resource_dir:
            config['global']['resource_dir'] = args.resource_dir
        else:
            config['global']['resource_dir'] = os.path.join(
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
            event_list=event_list,
            current_state=True)
    
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
        event_list=event_list,
        current_state=True)
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
        config['global']['input_path'], 'run.cfg')
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
        event_list=event_list,
        current_state=True)

    # initialize the filemanager
    msg = 'Initializing file manager'
    db = os.path.join(
        config['global'].get('project_path'),
        'output',
        'processflow.db')
    print_line(
        line=msg,
        event_list=event_list,
        current_state=True)
    filemanager = FileManager(
        database=db,
        event_list=event_list,
        mutex=mutex,
        config=config)
    filemanager.populate_file_list()
    filemanager.write_database()
    print_line(
        line='Starting local status update',
        event_list=event_list,
        current_state=True)
    filemanager.update_local_status()
    print_line(
        line='Local status update complete',
        event_list=event_list,
        current_state=True)
    filemanager.write_database()
    all_data = filemanager.all_data_local()
    if all_data:
        line = 'All data is local'
    else:
        line = 'Additional data needed'
    print_line(
        line=line,
        event_list=event_list,
        current_state=True)
    logging.info("FileManager setup complete")
    logging.info(str(filemanager))

    if all_data or args.no_monitor:
        print_line(
            line="skipping globus setup",
            event_list=event_list,
            current_state=True)
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
                event_list=event_list,
                current_state=True)
    # setup the runmanager
    runmanager = RunManager(
        short_name=config['global']['short_name'],
        account=config['global']['account'],
        resource_path=config['global']['resource_dir'],
        ui=ui,
        event_list=event_list,
        output_path=config['global']['output_path'],
        caseID=config['global']['experiment'],
        scripts_path=run_script_path,
        thread_list=thread_list,
        event=event,
        no_host=config['global']['no_host'],
        url_prefix=config['global']['url_prefix'],
        always_copy=config['global']['always_copy'])
    import ipdb; ipdb.set_trace()
    runmanager.setup_job_sets(
        set_frequency=config['global']['set_frequency'],
        sim_start_year=sim_start_year,
        sim_end_year=sim_end_year,
        config=config,
        filemanager=filemanager)

    logging.info('Starting run with config:')
    logging.info(json.dumps(config, indent=4, sort_keys=True))
    return config, filemanager, runmanager

def setup_directories(_args, config):
    """
    Setup the input, output, tmp, pp, and diags directories
    """
    # setup output directory
    if _args.output_path:
        output_path = _args.output_path
    else:
        output_path = os.path.join(
            config['global']['project_path'],
            'output')
    config['global']['output_path'] = output_path
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # setup input directory
    if _args.input_path:
        input_path = _args.input_path
    else:
        input_path = os.path.join(
            config['global']['project_path'],
            'input')
    config['global']['input_path'] = input_path
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_input = os.path.join(input_path, sim)
        if not os.path.exists(sim_input):
            os.makedirs(sim_input)
    # setup temp directory
    tmp_path = os.path.join(output_path, 'tmp')
    if os.path.exists(tmp_path):
        msg = 'removing previous temp directory {}'.format(tmp_path)
        logging.info(msg)
        rmtree(tmp_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_tmp = os.path.join(tmp_path, sim)
        if not os.path.exists(sim_tmp):
            os.makedirs(sim_tmp)
    # setup post processing dir
    pp_path = os.path.join(output_path, 'pp')
    config['global']['pp_path'] = pp_path
    if not os.path.exists(pp_path):
        os.makedirs(pp_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_pp = os.path.join(pp_path, sim)
        if not os.path.exists(sim_pp):
            os.makedirs(sim_pp)
    # setup diags dir
    diags_path = os.path.join(output_path, 'diags')
    config['global']['diags_path'] = diags_path
    if not os.path.exists(diags_path):
        os.makedirs(diags_path)
    for sim in config['simulations']:
        if sim in ['start_year', 'end_year', 'comparisons']: continue
        sim_diags = os.path.join(diags_path, sim)
        if not os.path.exists(sim_diags):
            os.makedirs(sim_diags)
    # setup run_scipts_path
    run_script_path = os.path.join(
        config['global']['output_path'],
        'run_scripts')
    config['global']['run_scripts_path'] = run_script_path
    if not os.path.exists(run_script_path):
        os.makedirs(run_script_path)

def verify_config(config):
    messages = list()
    # check that each mandatory section exists
    if not config.get('simulations'):
        msg = 'No simulations section found in config'
        messages.append(msg)
    if not config.get('global'):
        msg = 'No global section found in config'
        messages.append(msg)
    else:
        if not config['global'].get('project_path'):
            msg = 'no project_path in global options'
            messages.append(msg)
    if not config.get('file_types'):
        msg = 'No file_types section found in config'
        messages.append(msg)
    if messages:
        return messages

    # check simulations
    if not config['simulations'].get('comparisons'):
        msg = 'no comparisons specified'
        messages.append(msg)
    else:
        for comp in config['simulations']['comparisons']:
            if not isinstance(config['simulations']['comparisons'][comp], list):
                config['simulations']['comparisons'][comp] = [config['simulations']['comparisons'][comp]]
            for other_sim in config['simulations']['comparisons'][comp]:
                if other_sim in ['obs', 'all']: continue
                if other_sim not in config['simulations']:
                    msg = '{} not found in config.simulations'.format(other_sim)
                    messages.append(msg)
    for sim in config.get('simulations'):
        if sim in ['comparisons', 'start_year', 'end_year']:
            continue
        if not config['simulations'][sim].get('transfer_type'):
            msg = '{} is missing trasfer_type, if the data is local, set transfer_type to \'local\''.format(sim)
            messages.append(msg)
        else:
            if config['simulations'][sim]['transfer_type'] == 'globus' and not config['simulations'][sim].get('remote_uuid'):
                msg = 'case {} has transfer_type of globus, but is missing remote_uuid'.format(sim)
                messages.append(msg)
            elif config['simulations'][sim]['transfer_type'] == 'sftp' and not config['simulations'][sim].get('remote_hostname'):
                msg = 'case {} has transfer_type of sftp, but is missing remote_hostname'.format(sim)
                messages.append(msg)
            if config['simulations'][sim]['transfer_type'] != 'local' and not config['simulations'][sim].get('remote_path'):
                msg = 'case {} has non-local data, but no remote_path given'.format(sim)
                messages.append(msg)
            if config['simulations'][sim]['transfer_type'] == 'local' and not config['simulations'][sim].get('local_path'):
                msg = 'case {} is set for local data, but no local_path is set'.format(sim)
                messages.append(msg)
        if not config['simulations'].get('start_year'):
            msg = 'no start_year set for simulations'
            messages.append(msg)
        else:
            config['simulations']['start_year'] = int(config['simulations']['start_year'])
        if not config['simulations'].get('end_year'):
            msg = 'no end_year set for simulations'
            messages.append(msg)
        else:
            config['simulations']['end_year'] = int(config['simulations']['end_year'])
        if int(config['simulations'].get('end_year')) < int(config['simulations'].get('start_year')):
            msg = 'simulation end_year is less then start_year, is time going backwards!?'
            messages.append(msg)

    # check file_types
    for ftype in config.get('file_types'):
        if not config['file_types'][ftype].get('file_format'):
            msg = '{} has no file_format'.format(ftype)
            messages.append(msg)
        if not config['file_types'][ftype].get('remote_path'):
            msg = '{} has no remote_path'.format(ftype)
            messages.append(msg)
        if not config['file_types'][ftype].get('local_path'):
            msg = '{} has no local_path'.format(ftype)
            messages.append(msg)
        if config['file_types'][ftype].get('monthly') == 'True':
            config['file_types'][ftype]['monthly'] = True
        if config['file_types'][ftype].get('monthly') == 'False':
            config['file_types'][ftype]['monthly'] = False
    # check img_hosting
    if config.get('img_hosting'):
        if not config['img_hosting'].get('img_host_server'):
            msg = 'image hosting is turned on, but no img_host_server specified'
            messages.append(msg)
        if not config['img_hosting'].get('host_directory'):
            msg = 'image hosting is turned on, but no host_directory specified'
            messages.append(msg)

    if config.get('diags'):
        # check e3sm_diags
        if config['diags'].get('e3sm_diags'):
            if config.get('img_hosting') and not config['diags']['e3sm_diags'].get('host_directory'):
                msg = 'image hosting turned on, but no host_directory given for e3sm_diags'
                messages.append(msg)
            if not config['diags']['e3sm_diags'].get('backend'):
                msg = 'no backend given for e3sm_diags'
                messages.append(msg)
            if not config['diags']['e3sm_diags'].get('reference_data_path'):
                msg = 'no reference_data_path given for e3sm_diags'
                messages.append(msg)
            if not config['diags']['e3sm_diags'].get('sets'):
                msg = 'no sets given for e3sm_diags'
                messages.append(msg)
            if not config['diags']['e3sm_diags'].get('run_frequency'):
                msg = 'no run_frequency given for e3sm_diags'
                messages.append(msg)
        
        # check amwg
        if config['diags'].get('amwg'):
            if not config['diags']['amwg'].get('diag_home'):
                msg = 'no diag_home given for amwg'
                messages.append(msg)
            if not config['diags']['amwg'].get('run_frequency'):
                msg = 'no diag_home given for amwg'
                messages.append(msg)
            if config.get('img_hosting') and not config['diags']['amwg'].get('host_directory'):
                msg = 'img_hosting turned on, but no host_directory given for amwg'
                messages.append(msg)
        
        # check aprime
        if config['diags'].get('aprime'):
            if not config['diags']['aprime'].get('run_frequency'):
                msg = 'no run_frequency given for aprime'
                messages.append(msg)
            if config.get('img_hosting') and not config['diags']['aprime'].get('host_directory'):
                msg = 'img_hosting turned on but no host_directory given for aprime'
                messages.append(msg)
            if not config['diags']['aprime'].get('aprime_code_path'):
                msg = 'no aprime_code_path given for aprime'
                messages.append(msg)
            if not config['diags']['aprime'].get('test_atm_res'):
                msg = 'no test_atm_res given for aprime'
                messages.append(msg)
            if not config['diags']['aprime'].get('test_mpas_mesh_name'):
                msg = 'no test_mpas_mesh_name given for aprime'
                messages.append(msg) 

    if config.get('post-processing'):
        # check regrid
        if config['post-processing'].get('regrid'):
            for item in config['post-processing']['regrid']:
                if not config['post-processing']['regrid'][item].get('source_grid_path'):
                    msg = 'no source_grid_path given for {} regrid'.format(item)
                    messages.append(msg)
                if not config['post-processing']['regrid'][item].get('destination_grid_path'):
                    msg = 'no destination_grid_path given for {} regrid'.format(item)
                    messages.append(msg)
                if not config['post-processing']['regrid'][item].get('destination_grid_name'):
                    msg = 'no destination_grid_name given for {} regrid'.format(item)
                    messages.append(msg)

        # check ncclimo
        if config['post-processing'].get('ncclimo'):
            if not config['post-processing']['ncclimo'].get('regrid_map_path'):
                msg = 'no regrid_map_path given for ncclimo'
                messages.append(msg)
            if not config['post-processing']['ncclimo'].get('run_frequency'):
                msg = 'no run_frequency given for ncclimo'
                messages.append(msg)

        # check timeseries
        if config['post-processing'].get('timeseries'):
            if not config['post-processing']['timeseries'].get('run_frequency'):
                msg = 'no run_frequency given for timeseries'
                messages.append(msg)
            for item in config['post-processing']['timeseries']:
                if item == 'run_frequency':
                    continue
                if item not in ['atm', 'lnd', 'ocn']:
                    msg = '{} is an unsupported timeseries type'.format(item)
                    message.append(msg)
                if not isinstance(config['post-processing']['timeseries'][item], list):
                    config['post-processing']['timeseries'][item] = [config['post-processing']['timeseries'][item]]

    return messages


def check_config_white_space(filepath):
    line_index = 0
    found = False
    with open(filepath, 'r') as infile:
        for line in infile.readlines():
            line_index += 1
            index = line.find('=')
            if index == -1:
                found = False
                continue
            if line[index + 1] != ' ':
                found = True
                break
    if found:
        return line_index
    else:
        return 0
