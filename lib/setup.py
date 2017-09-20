import sys
import os
import logging
import json
from uuid import uuid4

from configobj import ConfigObj
from YearSet import YearSet, SetStatus
from shutil import copyfile
from util import (check_config_white_space, 
                  check_for_inplace_data,
                  setup_globus,
                  check_globus)

def setup(parser, display_event, **kwargs):
    """
    Parse the commandline arguments, and setup the master config dict

    Parameters:
        parser (argparse.ArgumentParser): The parser object
        display_event (Threadding_event): The event to turn the display on and off
    """
    file_type_map = kwargs['file_type_map']

    # Setup the parser
    args = parser.parse_args() 
    if not args.config:
        parser.print_help()
        sys.exit()
    try:
        event_list = kwargs['event_list']
        file_list = kwargs['file_list']
        file_name_list = kwargs['file_name_list']
        job_sets = kwargs['job_sets']
    except:
        print "Setup parameters not met"
        print event_list, file_list, file_name_list, job_sets
        sys.exit()
    
    # check if globus config is valid, else remove it
    globus_config = os.path.join(os.path.expanduser('~'), '.globus.cfg')
    if os.path.exists(globus_config):
        try:
            conf = ConfigObj(globus_config)
        except:
            os.remove(globus_config)

    # Check that there are no white space errors in the config file
    line_index = check_config_white_space(args.config)
    if line_index != 0:
        print 'ERROR: line {num} does not have a space after the \'=\', white space is required. Please add a space and run again.'.format(
            num=line_index)
        sys.exit(-1)

    # read the config file and setup the config dict
    try: 
        config = ConfigObj(args.config)
    except Exception as e:
        print "Error parsing config file {}".format(args.config)
        parser.print_help()
        sys.exit()
    
    if args.resource_dir:
        config['global']['resource_dir'] = args.resource_dir
    else:
        config['global']['resource_dir'] = os.path.join(
            sys.prefix,
            'share',
            'processflow',
            'resources')
    
    # run validator for config file
    template_path = os.path.join(
        config['global']['resource_dir'],
        'config_template.json')

    with open(template_path, 'r') as template_file:
        template = json.load(template_file)
    valid, messages = verify_config(config, template)
    if not valid:
        for message in messages:
            print message
        sys.exit(-1)
    
    if not os.path.exists(config['global'].get('data_cache_path')):
        os.makedirs(config['global'].get('data_cache_path'))
    if not os.path.exists(config['global'].get('output_path')):
        os.makedirs(config['global'].get('output_path'))
    
    # Copy the config into the input directory for safe keeping
    input_config_path = os.path.join(config['global'].get('data_cache_path'), 'run.cfg')
    if not os.path.exists(input_config_path):
        copyfile(
            src=args.config,
            dst=input_config_path)
    
    # setup output and cache directories
    if not os.path.exists(config['global']['output_path']):
        os.makedirs(config['global']['output_path'])
    if not os.path.exists(config['global']['data_cache_path']):
        os.makedirs(config['global']['data_cache_path'])
    
    # setup logging
    if args.log:
        log_path = args.log
    else:
        log_path = os.path.join(
            config.get('global').get('output_path'),
            'workflow.log')
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=log_path,
        filemode='w',
        level=logging.INFO)
    logging.getLogger('globus_sdk').setLevel(logging.WARNING)

    # Make sure the set_frequency is a list of ints
    set_frequency = config['global']['set_frequency']
    if not isinstance(set_frequency, list):
        set_frequency = [int(set_frequency)]
    else:
        # These are sometimes strings which break things later
        new_freqs = []
        for freq in set_frequency:
            new_freqs.append(int(freq))
        set_frequency = new_freqs
    config['global']['set_frequency'] = set_frequency

    # setup config for file type directories
    for key, val in config['global']['patterns'].items():
        new_dir = os.path.join(
            config['global']['data_cache_path'],
            key)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        if key in file_type_map:
            config['global'][file_type_map[key][0]] = new_dir
        else:
            if not config.get('global').get('other_data'):
                config['global']['other_data'] = []
            config['global']['other_data'].append(new_dir)

    # setup run_scipts_path
    run_script_path = os.path.join(
        config['global']['output_path'],
        'run_scripts')
    config['global']['run_scripts_path'] = run_script_path
    if not os.path.exists(run_script_path):
        os.makedirs(run_script_path)

    # setup tmp_path
    tmp_path = os.path.join(
        config['global']['output_path'],
        'tmp')
    config['global']['tmp_path'] = tmp_path
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    
    # setup the year_set list
    sim_start_year = int(config['global']['simulation_start_year'])
    sim_end_year = int(config['global']['simulation_end_year'])
    number_of_sim_years = sim_end_year - (sim_start_year - 1)
    line = 'Initializing year sets'
    event_list.push(message=line)
    for freq in set_frequency:
        year_set = number_of_sim_years / freq

        # initialize the job_sets dict
        for i in range(1, year_set + 1):
            set_start_year = sim_start_year + ((i - 1) * freq)
            set_end_year = set_start_year + freq - 1
            new_set = YearSet(
                set_number=len(job_sets) + 1,
                start_year=set_start_year,
                end_year=set_end_year)
            job_sets.append(new_set)
    
    line = 'Added {num} year sets to the queue'.format(
        num=len(job_sets))
    event_list.push(message=line)
    
    # initialize the file_list
    line = 'Initializing file list'
    event_list.push(message=line)
    file_list = setup_file_list(
        file_list=file_list,
        file_name_list=file_name_list,
        patterns=config['global']['patterns'],
        sim_start_year=sim_start_year,
        sim_end_year=sim_end_year,
        file_type_map=file_type_map)
    
    # check if we have all the data already, if not setup globus
    all_data = check_for_inplace_data(
        file_list=kwargs.get('file_list'),
        file_name_list=kwargs.get('file_name_list'),
        config=config,
        file_type_map=file_type_map)

    if all_data or args.no_monitor:
        print "All data is present, skipping globus setup"
    else:
        endpoints = [endpoint for endpoint in config['transfer'].values()]
        if args.no_ui:
            print 'Running in no-ui mode'
            addr = config.get('global').get('email')
            if not addr:
                print 'When running in no-ui mode, you must enter an email address.'
                sys.exit()
            setup_globus(
                endpoints=endpoints,
                no_ui=True,
                src=config.get('global').get('email'),
                dst=config.get('global').get('email'),
                event_list=event_list)
        else:
            output_path = config.get('global').get('output_path')
            error_output = os.path.join(
                output_path,
                'workflow.error')
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            sys.stderr = open(error_output, 'w')
            msg = 'Activating endpoints {}'.format(' '.join(endpoints))
            logging.info(msg)
            setup_success = setup_globus(
                endpoints=endpoints,
                display_event=display_event,
                no_ui=False)
            if not setup_success:
                print "Globus setup error"
                return -1
            print 'Globus authentication complete'
        print 'Checking file access on globus transfer nodes'
        setup_success, endpoint = check_globus(
            source_endpoint=config['transfer']['source_endpoint'],
            source_path=config['global']['source_path'],
            destination_endpoint=config['transfer']['destination_endpoint'],
            destination_path=config['global']['data_cache_path'])
        if not setup_success:
            print 'ERROR! Unable to access {} globus node'.format(endpoint['type'])
            print 'The node may be down, or you may not have access to the requested directory'
            sys.exit(-1)

    config['global']['ui'] = False if args.no_ui else True
    config['global']['no_cleanup'] = True if args.no_cleanup else False
    config['global']['no_monitor'] = True if args.no_monitor else False
    config['transfer']['size'] = args.size if args.size else 100
    config['global']['run_id'] = uuid4().hex[:6]
    config['global']['print_file_list'] = True if args.file_list else False
    
    return config

def setup_file_list(**kwargs):
    file_list = kwargs['file_list']
    file_name_list = kwargs['file_name_list']
    patterns = kwargs['patterns']
    sim_start_year = kwargs['sim_start_year']
    sim_end_year = kwargs['sim_end_year']
    file_type_map = kwargs['file_type_map']

    for key, val in patterns.items():
        file_list[key] = {}
        file_name_list[key] = {}
        if key in file_type_map:
            if file_type_map[key][1]:
                for year in range(sim_start_year, sim_end_year + 1):
                    for month in range(1, 13):
                        file_key = str(year) + '-' + str(month)
                        file_list[key][file_key] = SetStatus.NO_DATA
                        file_name_list[key][file_key] = ''
            else:
                if file_type_map[key][2]:
                    file_list[key][file_type_map[key][2]] = SetStatus.NO_DATA

    file_list['RPT']['rpointer.ocn'] = SetStatus.NO_DATA
    file_list['RPT']['rpointer.atm'] = SetStatus.NO_DATA
    file_list['STREAMS']['streams.ocean'] = SetStatus.NO_DATA
    file_list['STREAMS']['streams.cice'] = SetStatus.NO_DATA

    from pprint import pformat
    return file_list

def verify_config(config, template):
    messages = []
    valid = True
    for key, val in config.items():
        if key not in template:
            msg = '{key} is not a valid config option, is it misspelled?'.format(key=key)
            messages.append(msg)
            valid = False
    for key, val in template.items():
        if key not in config:
            msg = '{key} is missing from your config'.format(key=key)
            messages.append(msg)
            valid = False
        for item in val:
            if item not in config[key]:
                msg = '{key} requires {val} but it is missing from your config'.format(
                    key=key, val=item)
                messages.append(msg)
                valid = False
    return valid, messages