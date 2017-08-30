import sys
import os
import logging
from uuid import uuid4

from configobj import ConfigObj
from YearSet import YearSet, SetStatus
from util import (check_config_white_space, 
                  check_for_inplace_data,
                  setup_globus)

def setup(parser, display_event, **kwargs):
    """
    Parse the commandline arguments, and setup the master config dict

    Parameters:
        parser (argparse.ArgumentParser): The parser object
        display_event (Threadding_event): The event to turn the display on and off
    """

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
        level=logging.DEBUG)
    
    # Copy the config into the input directory for safe keeping
    input_config_path = os.path.join(config['global'].get('data_cache_path'), 'run.cfg')
    if not os.path.exists(input_config_path):
        copyfile(
            src=args.config,
            dst=input_config_path)

    # Make sure the set_frequency is a list of ints
    set_frequency = config['global']['set_frequency']
    if not isinstance(set_frequency, list):
        config['global']['set_frequency'] = [int(set_frequency)]
    # These are sometimes strings which break things later
    new_freqs = []
    for freq in set_frequency:
        new_freqs.append(int(freq))
    set_frequency = new_freqs
    
    file_type_map = {
        'MPAS_AM': ('mpas_dir', 1),
        'MPAS_CICE': ('mpas_cice_dir', 1),
        'ATM': ('atm_dir', 1),
        'MPAS_RST': ('mpas_rst_dir', 0),
        'MPAS_O_IN': ('mpas_o-in_dir', 0),
        'MPAS_CICE_IN': ('mpas_cice-in_dir', 0),
        'STREAMS': ('streams_dir', 0)
    }

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

    # setup output and cache directories
    if not os.path.exists(config['global']['output_path']):
        os.makedirs(config['global']['output_path'])
    if not os.path.exists(config['global']['data_cache_path']):
        os.makedirs(config['global']['data_cache_path'])

    # setup run_scipts_path
    config['global']['run_scripts_path'] = os.path.join(
        config['global']['output_path'],
        'run_scripts')

    # setup tmp_path
    config['global']['tmp_path'] = os.path.join(
        config['global']['output_path'],
        'tmp')
    
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
    for key, val in config['global']['patterns'].items():
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
                file_list[key][file_type_map[key]] = SetStatus.NO_DATA

    file_list[key]['rpointer.ocn'] = SetStatus.NO_DATA
    file_list[key]['rpointer.atm'] = SetStatus.NO_DATA
    file_list[key]['streams.ocean'] = SetStatus.NO_DATA
    file_list[key]['streams.cice'] = SetStatus.NO_DATA
    
    # check if we have all the data already, if not setup globus
    all_data = check_for_inplace_data(
        file_list=kwargs.get('file_list'),
        file_name_list=kwargs.get('file_name_list'),
        config=config)

    if all_data:
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
            print 'Globus setup complete'

    config['global']['ui'] = False if args.no_ui else True
    config['global']['no_cleanup'] = True if args.no_cleanup else False
    config['global']['no_monitor'] = True if args.no_monitor else False
    config['transfer']['size'] = args.size if args.size else 100
    config['global']['run_id'] = uuid4().hex[:6]
    
    return config