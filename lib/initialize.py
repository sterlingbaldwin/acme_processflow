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
from YearSet import YearSet, SetStatus
from shutil import copyfile
from lib.filemanager import FileManager
from lib.runmanager import RunManager
from lib.mailer import Mailer
from jobs.JobStatus import JobStatus
from util import (setup_globus,
                  check_globus,
                  print_message,
                  print_debug,
                  print_line)


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
        '-s', '--no-scripts',
        help='Don\'t copy the case_scripts directory from the remote machine.',
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
        ui=False,
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
    ui = args.ui
    config['global']['ui'] = True if ui else False
    config['global']['no_host'] = True if args.no_host else False
    config['global']['no_monitor'] = True if args.no_monitor else False
    config['global']['print_file_list'] = True if args.file_list else False
    config['global']['no_scripts'] = True if args.no_scripts else False
    config['global']['always_copy'] = True if args.always_copy else False
    config['global']['custom_archive'] = args.custom_archive_path if args.custom_archive_path else False

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
        ui=False,
        line='Not hosting img output',
        event_list=event_list,
        current_state=True)

    try:
        setup_directories(args)
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

    # setup logging
    if args.log:
        log_path = args.log
    else:
        log_path = os.path.join(
            config.get('global').get('output_path'),
            'processflow.log')
    print_line(
        ui=ui,
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

    if config['global']['always_copy']:
        msg = 'Running in forced-copy mode, all previous diagnostic output will be overwritten'
    else:
        msg = 'Running without forced-copy, previous diagnostic output will be preserved'
    print_line(ui=ui,
               line=msg,
               event_list=event_list,
               current_state=True)

    # initialize the filemanager
    msg = 'Initializing file manager'
    print_line(ui=ui,
               line=msg,
               event_list=event_list,
               current_state=True)
    filemanager = FileManager(
        event_list=event_list,
        ui=ui,
        mutex=mutex,
        config=config)
    filemanager.populate_file_list()
    print_line(
        ui=ui,
        line='Starting local status update',
        event_list=event_list,
        current_state=True)
    filemanager.update_local_status()
    print_line(
        ui=ui,
        line='Local status update complete',
        event_list=event_list,
        current_state=True)
    all_data = filemanager.all_data_local()
    if all_data:
        line = 'All data is local'
    else:
        line = 'Additional data needed'
    print_line(
        ui=ui,
        line=line,
        event_list=event_list,
        current_state=True)
    # TODO: rest of method
    logging.info("FileManager setup complete")
    logging.info(str(filemanager))

    if all_data or args.no_monitor:
        print_line(
            ui=ui,
            line="skipping globus setup",
            event_list=event_list,
            current_state=True)
    else:
        endpoints = [endpoint for endpoint in config['transfer'].values()]
        addr = config.get('global').get('email')
        if not addr:
            print 'When running in text mode, you must enter an email address.'
            return False, False, False
        setup_success = setup_globus(
            endpoints=endpoints,
            ui=False,
            src=config.get('global').get('email'),
            dst=config.get('global').get('email'),
            event_list=event_list)

        if not setup_success:
            print "Globus setup error"
            return False, False, False
        else:
            print_line(
                ui=ui,
                line='Globus authentication complete',
                event_list=event_list,
                current_state=True)

        line = 'Checking file access on globus transfer nodes'
        print_line(
            ui=ui,
            line=line,
            event_list=event_list,
            current_state=True)
        setup_success, endpoint = check_globus(
            source_endpoint=config['transfer']['source_endpoint'],
            source_path=config['global']['source_path'],
            destination_endpoint=config['transfer']['destination_endpoint'],
            destination_path=config['global']['input_path'])
        if not setup_success:
            print 'ERROR! Unable to access {} globus node'.format(endpoint['type'])
            print 'The node may be down, or you may not have access to the requested directory'
            return False, False, False

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
    runmanager.setup_job_sets(
        set_frequency=config['global']['set_frequency'],
        sim_start_year=sim_start_year,
        sim_end_year=sim_end_year,
        config=config,
        filemanager=filemanager)

    logging.info('Starting run with config:')
    logging.info(json.dumps(config, indent=4, sort_keys=True))
    return config, filemanager, runmanager

def setup_directories(_args):
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
    # setup temp directory
    tmp_path = os.path.join(output_path, 'tmp')
    if os.path.exists(tmp_path):
        msg = 'removing previous temp directory {}'.format(tmp_path)
        logging.info(msg)
        shutil.rmtree(tmp_path)
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
    if not config.get('models'):
        msg = 'No models section found in config'
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
    # check models
    if not config['models'].get('comparisons'):
        msg = 'no comparisons specified'
        messages.append(msg)
    else:
        for comp in config['models']['comparisons']:
            if config['models']['comparisons'][comp] != 'all':
                if not isinstance(config['models']['comparisons'][comp], list):
                    config['models']['comparisons'][comp] = [config['models']['comparisons'][comp]]
                for other_model in config['models']['comparisons'][comp]:
                    if other_model not in config['models'] and other_model != 'obs':
                        msg = 'model {}:{} not found in config.models'.format(comp, config['models']['comparisons'][comp])
                        messages.append(msg)
    for model in config.get('models'):
        if model == 'comparisons':
            continue
        if not config['models'][model].get('transfer_type'):
            msg = '{} is missing trasfer_type, if its data is local, set transfer_type to \'local\''.format(model)
            messages.append(msg)
        else:
            if config['models'][model]['transfer_type'] == 'globus' and not config['models'][model].get('globus_uuid'):
                msg = '{} has transfer_type of globus, but is missing globus_uuid'.format(model)
                messages.append(msg)
            elif config['models'][model]['transfer_type'] == 'sftp' and not config['models'][model].get('source_hostname'):
                msg = '{} has transfer_type of sftp, but is missing source_hostname'.format(model)
                messages.append(msg)
            if config['models'][model]['transfer_type'] != 'local' and not config['models'][model].get('source_path'):
                msg = '{} has non-local data, but no source_path given'.format(model)
                messages.append(msg)
        if not config['models'][model].get('start_year'):
            msg = '{} has no start_year'.format(model)
            messages.append(msg)
        if not config['models'][model].get('end_year'):
            msg = '{} has no end_year'.format(model)
            messages.append(msg)
        if int(config['models'][model]['end_year']) < int(config['models'][model]['start_year']):
            '{} end_year is less then start_year, is time going backwards!?'.format(model)
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
    if config.get('img_hosting'):
        if not config['img_hosting'].get('img_host_server'):
            msg = 'image hosting is turned on, but no img_host_server specified'
            messages.append(msg)
        if not config['img_hosting'].get('host_directory'):
            msg = 'image hosting is turned on, but no host_directory specified'
            messages.append(msg)
    if config.get('e3sm_diags'):
        if config.get('img_hosting') and not config['e3sm_diags'].get('host_directory'):
            msg = 'image hosting turned on, but no host_directory given for e3sm_diags'
            messages.append(msg)
        if not config['e3sm_diags'].get('backend'):
            msg = 'no backend given for e3sm_diags'
            messages.append(msg)
        if not config['e3sm_diags'].get('reference_data_path'):
            msg = 'no reference_data_path given for e3sm_diags'
            messages.append(msg)
        if not config['e3sm_diags'].get('sets'):
            msg = 'no sets given for e3sm_diags'
            messages.append(msg)
        if not config['e3sm_diags'].get('run_frequency'):
            msg = 'no run_frequency given for e3sm_diags'
            messages.append(msg)
    # check regrid
    if config.get('regrid'):
        if not config['regrid'].get('run_frequency'):
            msg = 'no run_frequency given for regrid'
            messages.append(msg)
        for item in config['regrid']:
            if item == 'run_frequency':
                continue
            if not config['regrid'][item].get('source_grid_path'):
                msg = 'no source_grid_path given for {} regrid'.format(item)
                messages.append(msg)
            if not config['regrid'][item].get('destination_grid_path'):
                msg = 'no destination_grid_path given for {} regrid'.format(item)
                messages.append(msg)
            if not config['regrid'][item].get('destination_grid_name'):
                msg = 'no destination_grid_name given for {} regrid'.format(item)
                messages.append(msg)
    # check amwg
    if config.get('amwg'):
        if not config['amwg'].get('diag_home'):
            msg = 'no diag_home given for amwg'
            messages.append(msg)
        if not config['amwg'].get('run_frequency'):
            msg = 'no diag_home given for amwg'
            messages.append(msg)
        if config.get('img_hosting') and not config['amwg'].get('host_directory'):
            msg = 'img_hosting turned on, but no host_directory given for amwg'
            messages.append(msg)
    # check ncclimo
    if config.get('ncclimo'):
        if not config['ncclimo'].get('regrid_map_path'):
            msg = 'no regrid_map_path given for ncclimo'
            messages.append(msg)
        if not config['ncclimo'].get('run_frequency'):
            msg = 'no run_frequency given for ncclimo'
            messages.append(msg)
    # check timeseries
    if config.get('timeseries'):
        if not config['timeseries'].get('run_frequency'):
            msg = 'no run_frequency given for timeseries'
            messages.append(msg)
        if not config['timeseries'].get('data_types'):
            msg = 'no data_types given for timeseries, acceptable options are atm, lnd, ocn'
            messages.append(msg)
        else:
            for dtype in config['timeseries']['data_types']:
                if not config['timeseries']['data_types'][dtype].get('var_list'):
                    msg = 'no var_list given for {}'.format(dtype)
                    messages.append(msg)
    # check aprime
    if config.get('aprime'):
        if not config['aprime'].get('run_frequency'):
            msg = 'no run_frequency given for aprime'
            messages.append(msg)
        if config.get('img_hosting') and not config['aprime'].get('host_directory'):
            msg = 'img_hosting turned on but no host_directory given for aprime'
            messages.append(msg)
        if not config['aprime'].get('aprime_code_path'):
            msg = 'no aprime_code_path given for aprime'
            messages.append(msg)
        if not config['aprime'].get('test_atm_res'):
            msg = 'no test_atm_res given for aprime'
            messages.append(msg)
        if not config['aprime'].get('test_mpas_mesh_name'):
            msg = 'no test_mpas_mesh_name given for aprime'
            messages.append(msg) 
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
