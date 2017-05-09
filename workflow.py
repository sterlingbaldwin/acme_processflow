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
import ConfigParser

from shutil import rmtree
from shutil import move
from getpass import getpass
from time import sleep
from pprint import pformat

from jobs.Transfer import Transfer
from jobs.Ncclimo import Climo
from jobs.Timeseries import Timeseries
from jobs.AMWGDiagnostic import AMWGDiagnostic
from jobs.CoupledDiagnostic import CoupledDiagnostic
from jobs.JobStatus import JobStatus
from lib.Monitor import Monitor
from lib.YearSet import YearSet
from lib.YearSet import SetStatus
from lib.mailer import Mailer

from lib.util import *

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file.')
parser.add_argument('-v', '--debug', help='Run in debug mode.', action='store_true')
parser.add_argument('-d', '--daemon', help='Run in daemon mode.', action='store_true')
parser.add_argument('-n', '--no-ui', help='Turn off the GUI.', action='store_true')
parser.add_argument('-r', '--dry-run', help='Do all setup, but dont submit jobs.', action='store_true')
parser.add_argument('-l', '--log', help='Path to logging output file.')
parser.add_argument('-u', '--no-cleanup', help='Don\'t perform pre or post run cleanup. This will leave all run scripts in place.', action='store_true')
parser.add_argument('-m', '--no-monitor', help='Don\'t run the remote monitor or move any files over globus.', action='store_true')
parser.add_argument('-V', '--viewer', help='Turn on generation for output_viewer style web pages.', action='store_true')
parser.add_argument('-s', '--size', help='The maximume size in gigabytes of a single transfer, defaults to 100. Must be larger then the largest single file.')

if not os.environ.get('NCARG_ROOT'):
    ncar_path = '/usr/local/src/NCL-6.3.0/'
    if os.path.exists(ncar_path):
        os.environ['NCARG_ROOT'] = ncar_path
    else:
        print 'No NCARG_ROOT found in environment variables, is NCL installed on the machine?'
        sys.exit()

def setup(parser, event_list, display_event):
    """
    Setup the config, file_list, and job_sets variables from either the config file passed from the parser
    of from the previously saved state
    """
    global debug
    global config
    global file_list
    global job_sets
    global from_saved_state

    args = parser.parse_args()

    if args.debug:
        debug = True
        print_message('Running in debug mode', 'ok')

    # read through the config file and setup the config dict
    config = {}
    if not args.config:
        parser.print_help()
        sys.exit()
    else:
        try:
            confParse = ConfigParser.ConfigParser()
            confParse.read(args.config)
            for section in confParse.sections():
                config[section] = {}
                for option in confParse.options(section):
                    opt = confParse.get(section, option)
                    if not opt:
                        if 'pass' in option and not args.no_monitor:
                            opt = getpass('>> ' + option + ': ')
                        else:
                            opt = raw_input('>> ' + option + ': ')
                    if opt.startswith('[') or opt.startswith('{'):
                        opt = json.loads(opt)
                    config[section][option] = opt
        except Exception as e:
            msg = 'Unable to read config file, is it properly formatted json?'
            print_message(msg)
            print_debug(e)
            return -1

    endpoints = [config['transfer']['source_endpoint'], config['transfer']['destination_endpoint']]
    if args.no_ui:
        config['global']['ui'] = False
        addr = config.get('global').get('email')
        if not addr:
            print 'When running in headless mode, you must enter an email address.'
            sys.exit()
        setup_globus(
            endpoints=endpoints,
            no_ui=True,
            src=config.get('global').get('email'),
            dst=config.get('global').get('email'),
            event_list=event_list)
    else:
        debug = False
        config['global']['ui'] = True
        msg = '## activating endpoints {}'.format(' '.join(endpoints))
        logging.info(msg)
        if not setup_globus(
            endpoints=endpoints,
            display_event=display_event):
            return -1
        print 'Globus setup complete'

    if args.dry_run:
        config['global']['dry_run'] = True
    else:
        config['global']['dry_run'] = False

    if args.no_cleanup:
        config['global']['no_cleanup'] = True
    else:
        config['global']['no_cleanup'] = False

    if args.no_monitor:
        config['global']['no_monitor'] = True
        print "Turning off remote monitoring"
    else:
        config['global']['no_monitor'] = False
    
    if args.size:
        config['transfer']['size'] = args.size
    else:
        config['transfer']['size'] = 100
    
    if args.viewer:
        print 'Turning on output_viewer mode'
        config['global']['viewer'] = True
    else:
        config['global']['viewer'] = False

    # setup config for file type directories
    for key, val in config.get('global').get('output_patterns').items():
        new_dir = os.path.join(
            config['global']['data_cache_path'],
            key)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        if val == 'mpaso.hist.am.timeSeriesStatsMonthly':
            config['global']['mpas_dir'] = new_dir
        elif val == 'mpascice.hist.am.timeSeriesStatsMonthly':
            config['global']['mpas_cice_dir'] = new_dir
        elif val == 'cam.h0':
            config['global']['atm_dir'] = new_dir
        elif val == 'mpaso.rst.0':
            config['global']['mpas_rst_dir'] = new_dir
        elif val == 'rpointer':
            config['global']['rpt_dir'] = new_dir
        elif val == 'mpas-o_in':
            config['global']['mpas_o-in_dir'] = new_dir
        elif val == 'mpas-cice_in':
            config['global']['mpas_cice-in_dir'] = new_dir
        elif 'stream' in val:
            config['global']['streams_dir'] = new_dir

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
    
    return config

def add_jobs(year_set):
    """
    Initializes and adds all the jobs to the year_set
    """
    # each required job is a key, the value is if its in the job list already or not
    # this is here in case the jobs have already been added
    run_coupled = 'coupled_diag' not in config.get('global').get('set_jobs', True)
    patterns = config.get('global').get('output_patterns')
    if not patterns.get('STREAMS') or \
       not patterns.get('MPAS_AM') or \
       not patterns.get('MPAS_O_IN') or \
       not patterns.get('MPAS_CICE_IN'):
        run_coupled = True
    required_jobs = {
        'climo': 'ncclimo' not in config.get('global').get('set_jobs', True),
        'timeseries': 'timeseries' not in config.get('global').get('set_jobs', True),
        'uvcmetrics': 'uvcmetrics' not in config.get('global').get('set_jobs', True),
        'coupled_diagnostic': run_coupled,
        'amwg_diagnostic': 'amwg' not in config.get('global').get('set_jobs', True)
    }
    year_set_str = 'year_set_{}'.format(year_set.set_number)
    dataset_name = '{time}_{set}_{start}_{end}'.format(
        time=time.strftime("%d-%m-%Y"),
        set=year_set.set_number,
        start=year_set.set_start_year,
        end=year_set.set_end_year)

    # create a temp directory full of just symlinks to the regridded output we need for this diagnostic job
    diag_temp_dir = os.path.join(
        config.get('global').get('tmp_path'),
        'diag',
        year_set_str)
    if not os.path.exists(diag_temp_dir):
        os.makedirs(diag_temp_dir)

    for job in year_set.jobs:
        if not required_jobs[job.get_type()]:
            required_jobs[job.get_type()] = True

    # create a temp directory, and fill it with symlinks to the actual data
    key_list = []
    for year in range(year_set.set_start_year, year_set.set_end_year + 1):
        for month in range(1, 13):
            key_list.append('{0}-{1}'.format(year, month))

    climo_file_list = [file_name_list['ATM'].get(x) for x in key_list if file_name_list['ATM'].get(x)]
    climo_temp_dir = os.path.join(config['global']['tmp_path'], 'climo', year_set_str)
    create_symlink_dir(
        src_dir=config.get('global').get('atm_dir'),
        src_list=climo_file_list,
        dst=climo_temp_dir)

    g_config = config.get('global')
    # first initialize the climo job
    if not required_jobs['climo']:
        required_jobs['climo'] = True
        climo_output_dir = os.path.join(
            config.get('global').get('output_path'),
            'climo')
        if not os.path.exists(climo_output_dir):
            os.makedirs(climo_output_dir)

        regrid_output_dir = os.path.join(
            config.get('global').get('output_path'),
            'regrid')
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)

        # create the configuration object for the climo job
        climo_config = {
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'caseId': config.get('global').get('experiment'),
            'annual_mode': 'sdd',
            'regrid_map_path': config.get('ncclimo').get('regrid_map_path'),
            'input_directory': climo_temp_dir,
            'climo_output_directory': climo_output_dir,
            'regrid_output_directory': regrid_output_dir,
            'year_set': year_set.set_number,
            'ncclimo_path': config.get('ncclimo').get('ncclimo_path'),
        }
        climo = Climo(climo_config, event_list=event_list)
        msg = 'Adding Ncclimo job to the job list: {}'.format(str(climo))
        logging.info(msg)
        year_set.add_job(climo)

    if not required_jobs['timeseries']:
        required_jobs['timeseries'] = True
        timeseries_output_dir = os.path.join(
            config.get('global').get('output_path'),
            'timeseries',
            'year_set_{}'.format(year_set.set_number))
        if not os.path.exists(timeseries_output_dir):
            msg = 'Creating timeseries output directory'
            logging.info(msg)
            os.makedirs(timeseries_output_dir)

        # create temp directory of symlinks to history files
        # we can reuse the input directory for the climo generation
        timeseries_config = {
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'annual_mode': 'sdd',
            'caseId': config.get('global').get('experiment'),
            'year_set': year_set.set_number,
            'var_list': config.get('ncclimo').get('var_list'),
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'input_directory': climo_temp_dir,
            'output_directory': timeseries_output_dir,
        }
        timeseries = Timeseries(timeseries_config, event_list=event_list)
        timeseries.depends_on = []
        msg = 'Adding Timeseries job to the job list: {}'.format(str(timeseries))
        logging.info(msg)
        year_set.add_job(timeseries)

    if not required_jobs['coupled_diagnostic']:
        required_jobs['coupled_diagnostic'] = True
        coupled_project_dir = os.path.join(
            config.get('global').get('output_path'),
            'coupled_diags',
            'year_set_' + str(year_set.set_number))
        if not os.path.exists(coupled_project_dir):
            os.makedirs(coupled_project_dir)

        host_prefix = os.path.join(
            config.get('global').get('img_host_server'),
            config.get('coupled_diags').get('host_prefix'))

        c_config = config.get('coupled_diags')
        coupled_diag_config = {
            'rpt_dir': g_config.get('rpt_dir'),
            'mpaso_regions_file': c_config.get('mpaso_regions_file'),
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'output_base_dir': coupled_project_dir,
            'mpas_am_dir': g_config.get('mpas_dir'),
            'mpas_cice_dir': g_config.get('mpas_cice_dir'),
            'mpas_cice_in_dir': g_config.get('mpas_cice-in_dir'),
            'mpas_o_dir': g_config.get('mpas_o-in_dir'),
            'mpas_rst_dir': g_config.get('mpas_rst_dir'),
            'streams_dir': g_config.get('streams_dir'),
            'host_prefix': host_prefix,
            'host_directory': c_config.get('host_directory'),
            'run_id': config.get('global').get('run_id'),
            'dataset_name': dataset_name,
            'year_set': year_set.set_number,
            'climo_tmp_dir': climo_temp_dir,
            'regrid_path': regrid_output_dir,
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'nco_path': config.get('ncclimo').get('ncclimo_path'),
            'coupled_project_dir': coupled_project_dir,
            'test_casename': g_config.get('experiment'),
            'test_native_res': c_config.get('test_native_res'),
            'test_archive_dir': diag_temp_dir,
            'test_begin_yr_climo': year_set.set_start_year,
            'test_end_yr_climo': year_set.set_end_year,
            'test_begin_yr_ts': year_set.set_start_year,
            'test_end_yr_ts': year_set.set_end_year,
            'ref_case': c_config.get('ref_case'),
            'ref_archive_dir': c_config.get('ref_archive_dir'),
            'mpas_meshfile': c_config.get('mpas_meshfile'),
            'mpas_remapfile': c_config.get('mpas_remapfile'),
            'pop_remapfile': c_config.get('pop_remapfile'),
            'remap_files_dir': c_config.get('remap_files_dir'),
            'GPCP_regrid_wgt_file': c_config.get('gpcp_regrid_wgt_file'),
            'CERES_EBAF_regrid_wgt_file': c_config.get('ceres_ebaf_regrid_wgt_file'),
            'ERS_regrid_wgt_file': c_config.get('ers_regrid_wgt_file'),
            'coupled_diags_home': c_config.get('coupled_diags_home'),
            'coupled_template_path': os.path.join(os.path.abspath(os.path.dirname(__file__)), 'resources', 'run_AIMS_template.csh'),
            'rendered_output_path': os.path.join(coupled_project_dir, 'run_AIMS.csh'),
            'obs_ocndir': c_config.get('obs_ocndir'),
            'obs_seaicedir': c_config.get('obs_seaicedir'),
            'obs_sstdir': c_config.get('obs_sstdir'),
            'depends_on': [], # 'climo'
            'yr_offset': c_config.get('yr_offset')
        }
        coupled_diag = CoupledDiagnostic(coupled_diag_config, event_list)
        msg = 'Adding CoupledDiagnostic job to the job list: {}'.format(str(coupled_diag))
        logging.info(msg)
        year_set.add_job(coupled_diag)

    if not required_jobs['amwg_diagnostic']:
        required_jobs['amwg_diagnostic'] = True
        amwg_project_dir = os.path.join(
            config.get('global').get('output_path'),
            'amwg_diags',
            'year_set_{}'.format(year_set.set_number))
        if not os.path.exists(amwg_project_dir):
            os.makedirs(amwg_project_dir)

        host_prefix = os.path.join(
            config.get('global').get('img_host_server'),
            config.get('amwg').get('host_prefix'))

        amwg_temp_dir = os.path.join(config['global']['tmp_path'], 'amwg', year_set_str)
        if not os.path.exists(diag_temp_dir):
            os.makedirs(diag_temp_dir)
        template_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'resources', 'amwg_template.csh')
        amwg_config = {
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'run_id': config.get('global').get('run_id'),
            'host_directory': config.get('amwg').get('host_directory'),
            'host_prefix': host_prefix,
            'dataset_name': dataset_name,
            'diag_home': config.get('amwg').get('diag_home'),
            'test_path': amwg_project_dir + os.sep,
            'test_casename': g_config.get('experiment'),
            'test_path_history': climo_temp_dir + os.sep,
            'regrided_climo_path': regrid_output_dir,
            'test_path_climo': amwg_temp_dir,
            'test_path_diag': amwg_project_dir,
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'year_set': year_set.set_number,
            'run_directory': amwg_project_dir,
            'template_path': template_path,
            'depends_on': ['climo']
        }
        amwg_diag = AMWGDiagnostic(amwg_config, event_list)
        msg = 'Adding AMWGDiagnostic job to the job list: {}'.format(str(amwg_config))
        logging.info(msg)
        year_set.add_job(amwg_diag)

    return year_set

def monitor_check(monitor, config, file_list, event_list, display_event):
    """
    Check the remote directory for new files that match the given pattern,
    if there are any new files, create new transfer jobs. If they're in a new job_set,
    spawn the jobs for that set.

    inputs:
        monitor: a monitor object setup with a remote directory and an SSH session
    """
    global job_sets
    global active_transfers
    global transfer_list
    # if there are already three or more transfers in progress
    # hold off on starting any new ones until they complete
    if active_transfers >= 2:
        return
    event_list = push_event(event_list, "Running check for remote files")
    monitor.check()
    new_files = monitor.new_files
    patterns = config.get('global').get('output_patterns')
    for file_info in new_files:
        for folder, file_type in patterns.items():
            if file_type in file_info['filename']:
                file_info['type'] = folder
                break

    checked_new_files = []

    for new_file in new_files:
        file_type = new_file.get('type')
        if not file_type:
            event_list = push_event(event_list, "Failed accessing remote directory, do you have access permissions?")
            continue
        file_key = ""
        if file_type in ['ATM', 'MPAS_AM', 'MPAS_CICE', 'MPAS_RST']:
            file_key = filename_to_file_list_key(new_file['filename'])
        elif file_type == 'MPAS_CICE_IN':
            file_key = 'mpas-cice_in'
        elif file_type == 'MPAS_O_IN':
            file_key = 'mpas-o_in'
        elif file_type == 'STREAMS':
            file_key = 'streams.cice' if 'cice' in new_file['filename'] else 'streams.ocean'
        elif file_type == 'RPT':
            if 'ocn' in new_file['filename']:
                file_key = 'rpointer.ocn'
            elif 'atm' in new_file['filename']:
                file_key = 'rpointer.atm'
            else:
                continue
        try:
            status = file_list[file_type][file_key]
        except KeyError:
            continue
        if not status:
            continue
        if status == SetStatus.DATA_READY:
            local_path = os.path.join(
                config.get('global').get('data_cache_path'),
                new_file['type'],
                new_file['filename'].split('/')[-1])
            if not os.path.exists(local_path):
                checked_new_files.append(new_file)
                continue
            if not int(os.path.getsize(local_path)) == int(new_file['size']):
                os.remove(local_path)
                checked_new_files.append(new_file)
        if status == SetStatus.NO_DATA:
            checked_new_files.append(new_file)

    # if there are any new files
    if not checked_new_files:
        # print 'no new files'
        return
    else:
        # print pformat(checked_new_files)
        pass

    # find which year set the data belongs to
    frequencies = config.get('global').get('set_frequency')
    for file_info in checked_new_files:
        if file_info['type'] != 'ATM':
            continue
        for freq in frequencies:
            year_set = filename_to_year_set(file_info['filename'], freq)
            for job_set in job_sets:
                if job_set.set_number == year_set and job_set.status == SetStatus.NO_DATA:
                    job_set.status = SetStatus.PARTIAL_DATA
                    # Spawn jobs for that yearset
                    job_set = add_jobs(job_set)

    t_config = config.get('transfer')
    g_config = config.get('global')
    m_config = config.get('monitor')

    transfer_config = {
        'size': t_config.get('size'),
        'file_list': checked_new_files,
        'source_endpoint': t_config.get('source_endpoint'),
        'destination_endpoint': t_config.get('destination_endpoint'),
        'source_path': t_config.get('source_path'),
        'destination_path': g_config.get('data_cache_path') + '/',
        'recursive': 'False',
        'pattern': config.get('global').get('output_patterns'),
        'ncclimo_path': config.get('ncclimo').get('ncclimo_path'),
        'src_email': config.get('global').get('email'),
        'display_event': display_event,
        'no_ui': config.get('global').get('ui')
    }

    # Check if the user is logged in, and all endpoints are active
    endpoints = [config['transfer']['source_endpoint'], config['transfer']['destination_endpoint']]
    client = get_client()
    for endpoint in endpoints:
        r = client.endpoint_autoactivate(endpoint, if_expires_in=3600)
        if r["code"] == "AutoActivationFailed":
            display_event.set()
            sleep(3)
            while not setup_globus(endpoints):
                sleep(1)
            display_event.clear()
            diaplay_thread = threading.Thread(target=start_display, args=(config, display_event))
            diaplay_thread.start()
            
    transfer = Transfer(transfer_config, event_list)

    for item in transfer.config.get('file_list'):
        item_name = item['filename'].split('/').pop()
        item_type = item['type']
        if item_type in ['ATM', 'MPAS_AM']:
            file_key = filename_to_file_list_key(item_name)
        elif item_type == 'MPAS_CICE':
            file_key = 'mpas-cice_in'
        elif item_type == 'MPAS_O':
            file_key = 'mpas-o_in'
        elif item_type == 'MPAS_RST':
            file_key = '0002-01-01'
        elif item_type == 'RPT':
                file_key = 'rpointer.ocn' if 'ocn' in item_name else 'rpointer.atm'
        elif item_type == 'STREAMS':
            file_key == 'streams.cice' if 'cice' in item_name else 'streams.ocean'
        file_list[item_type][file_key] = SetStatus.IN_TRANSIT

    start_file = transfer.config.get('file_list')[0]['filename']
    end_file = transfer.config.get('file_list')[-1]['filename']
    index = start_file.find('-')
    start_readable = start_file[index - 4: index + 3]
    index = end_file.find('-')
    end_readable = end_file[index - 4: index + 3]
    message = 'Found {0} new remote files, creating transfer job from {1} to {2}'.format(
        len(checked_new_files),
        start_readable,
        end_readable)
    event_list = push_event(event_list, message)
    logging.info('## ' + message)

    if not config.get('global').get('dry_run', False):
        while True:
            try:
                thread = threading.Thread(target=handle_transfer, args=(transfer, checked_new_files, thread_kill_event, event_list))
            except:
                sleep(1)
            else:
                thread_list.append(thread)
                thread.start()
                break

def handle_transfer(transfer_job, f_list, event, event_list):
    global active_transfers
    """
    Wrapper around the transfer.execute() method, ment to be run inside a thread

    inputs:
        transfer_job: the transfer job to execute and monitor
        f_list: the list of files being transfered
        event: a thread event to handle shutting down from keyboard exception, not used in this case
            but it needs to be there for any threads handlers
    """
    active_transfers += 1
    # start the transfer job
    transfer_job.execute(event, event_list)
    # the transfer is complete, so we can decrement the active_transfers counter
    active_transfers -= 1

    if transfer_job.status != JobStatus.COMPLETED:
        print_message("File transfer failed")
        message = "## Transfer {uuid} has failed".format(uuid=transfer_job.uuid)
        logging.error(message)
        event_list = push_event(event_list, 'Tranfer failed')
        return
    else:
        message = "## Transfer {uuid} has completed".format(uuid=transfer_job.uuid)
        logging.info(message)
        for item in transfer_job.config['file_list']:
            item_name = item['filename'].split('/').pop()
            item_type = item['type']
            if item_type in ['ATM', 'MPAS_AM', 'MPAS_RST']:
                file_key = filename_to_file_list_key(item_name)
            elif item_type == 'MPAS_CICE_IN':
                file_key = 'mpas-cice_in'
            elif item_type == 'MPAS_O_IN':
                file_key = 'mpas-o_in'
            elif item_type == 'STREAMS':
                file_key == 'streams.cice' if 'cice' in item_name else 'streams.ocean'
            elif item_type == 'RPT':
                file_key = 'rpointer.ocn' if 'ocn' in item_name else 'rpointer.atm'
            file_list[item_type][file_key] = SetStatus.DATA_READY

def is_all_done():
    """
    Check if all job_sets are done, and all processing has been completed
    """
    for job_set in job_sets:
        if job_set.status != SetStatus.COMPLETED:
            return False
    return True

def cleanup():
    """
    Clean up temp files created during the run
    """
    if config.get('global').get('no_cleanup'):
        return
    logging.info('Cleaning up temp directories')
    try:
        tmp_path = config.get('global').get('tmp_path')
        if os.path.exists(tmp_path):
            rmtree(tmp_path)
    except Exception as e:
        logging.error(format_debug(e))
        print_message('Error removing temp directories')

    try:
        archive_path = os.path.join(
            config.get('global').get('output_path'),
            'script_archive',
            time.strftime("%Y-%m-%d-%I-%M"))
        if not os.path.exists(archive_path):
            os.makedirs(archive_path)
        run_script_path = config.get('global').get('run_scripts_path')
        if os.path.exists(run_script_path):
            while os.path.exists(archive_path):
                archive_path = archive_path[:-1] + str(int(archive_path[-1]) + 1)
            move(run_script_path, archive_path)
    except Exception as e:
        logging.error(format_debug(e))
        logging.error('Error archiving run_scripts directory')

def xy_check(x, y, hmax, wmax):
    if y >= hmax or x >= wmax:
        return -1
    else:
        return 0

def write_line(pad, line, x, y, color):
    try:
        pad.addstr(y, x, line, color)
    except:
        pass

def display(stdscr, event, config):
    """
    Display current execution status via curses
    """

    initializing = True
    # blockPrint()
    height, width = stdscr.getmaxyx()
    hmax = height - 3
    wmax = width - 5
    spinner = ['\\', '|', '/', '-']
    spin_index = 0
    spin_len = 4
    try:
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
            c = stdscr.getch()
            if c == curses.KEY_RESIZE:
                height, width = stdscr.getmaxyx()
                hmax = height - 3
                wmax = width - 5
                pad.resize(hmax, wmax)
            elif c == ord('w'):
                config['global']['ui'] = False
                pad.clear()
                del pad
                curses.endwin()
                return
            if len(job_sets) == 0:
                sleep(1)
                continue
            pad.clrtobot()
            y = 0
            x = 0
            for year_set in job_sets:
                line = 'Year_set {num}: {start} - {end}'.format(
                    num=year_set.set_number,
                    start=year_set.set_start_year,
                    end=year_set.set_end_year)
                #pad.addstr(y, x, line, curses.color_pair(1))
                write_line(pad, line, x, y, curses.color_pair(1))
                pad.clrtoeol()
                y += 1
                # if xy_check(x, y, hmax, wmax) == -1:
                #     sleep(1)
                #     break
                color_pair = curses.color_pair(4)
                if year_set.status == SetStatus.COMPLETED:
                    color_pair = curses.color_pair(5)
                elif year_set.status == SetStatus.FAILED:
                    color_pair = curses.color_pair(3)
                elif year_set.status == SetStatus.RUNNING:
                    color_pair = curses.color_pair(6)
                line = 'status: {status}'.format(
                    status=year_set.status)
                #pad.addstr(y, x, line, color_pair)
                write_line(pad, line, x, y, color_pair)
                if initializing:
                    sleep(0.01)
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                pad.clrtoeol()
                y += 1
                # if xy_check(x, y, hmax, wmax) == -1:
                #     sleep(1)
                #     break
                # if y >= (hmax/3):
                #     last_y = y
                #     y = 0
                #     x += (wmax/2)
                #     if x >= wmax:
                #         break
                if year_set.status == SetStatus.COMPLETED \
                    or year_set.status == SetStatus.NO_DATA \
                    or year_set.status == SetStatus.PARTIAL_DATA:
                    continue
                for job in year_set.jobs:
                    line = '  >   {type} -- {id} '.format(
                        type=job.get_type(),
                        id=job.job_id)
                    # pad.addstr(y, x, line, curses.color_pair(4))
                    write_line(pad, line, x, y, curses.color_pair(4))
                    color_pair = curses.color_pair(4)
                    if job.status == JobStatus.COMPLETED:
                        color_pair = curses.color_pair(5)
                    elif job.status in [JobStatus.FAILED, 'CANCELED', JobStatus.INVALID]:
                        color_pair = curses.color_pair(3)
                    elif job.status == JobStatus.RUNNING:
                        color_pair = curses.color_pair(6)
                    elif job.status == JobStatus.SUBMITTED or job.status == JobStatus.PENDING:
                        color_pair = curses.color_pair(7)
                    line = '{status}'.format(status=job.status)
                    pad.addstr(line, color_pair)
                    pad.clrtoeol()
                    if initializing:
                        sleep(0.01)
                        pad.refresh(0, 0, 3, 5, hmax, wmax)
                    y += 1
                # if y >= (hmax/3):
                #     last_y = y
                #     y = 0
                #     x += (wmax/2)
                #     if x >= wmax:
                #         break

            x = 0
            if last_y:
                y = last_y
            # pad.refresh(0, 0, 3, 5, hmax, wmax)
            pad.clrtobot()
            y += 1
            # if xy_check(x, y, hmax, wmax) == -1:
            #     sleep(1)
            #     continue
            for line in event_list[-10:]:
                if 'Transfer' in line:
                    continue
                if 'hosted' in line:
                    continue
                if 'failed' in line or 'FAILED' in line:
                    prefix = '[-]  '
                    pad.addstr(y, x, prefix, curses.color_pair(3))
                else:
                    prefix = '[+]  '
                    pad.addstr(y, x, prefix, curses.color_pair(5))
                pad.addstr(line, curses.color_pair(4))
                pad.clrtoeol()
                if initializing:
                    sleep(0.01)
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                #pad.refresh(0, 0, 3, 5, hmax, wmax)
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
            # for line in sorted(file_list, cmp=file_list_cmp):
            #     index = line.find('-')
            #     year = int(line[:index])
            #     month = int(line[index + 1:])
            #     if month == 1:
            #         year_ready = True
            #         partial_data = False
            #     if file_list[line] != SetStatus.DATA_READY:
            #         year_ready = False
            #     else:
            #         partial_data = True
            #     if month == 12:
            #         if year_ready:
            #             status = SetStatus.DATA_READY
            #         else:
            #             if partial_data:
            #                 status = SetStatus.PARTIAL_DATA
            #             else:
            #                 status = SetStatus.NO_DATA
            #         file_display_list.append('Year {year} - {status}'.format(
            #             year=year,
            #             status=status))

            # line_length = len(file_display_list[0])
            # num_cols = wmax/line_length
            # for line in file_display_list:
            #     if x + len(line) >= wmax:
            #         diff = wmax - (x + len(line))
            #         line = line[:diff]
            #     pad.addstr(y, x, line, curses.color_pair(4))
            #     pad.clrtoeol()
            #     y += 1
            #     if y >= (hmax-10):
            #         y = file_start_y
            #         x += line_length + 5
            #         if x >= wmax:
            #             break
            #     if y > file_end_y:
            #         file_end_y = y

            y = file_end_y + 1
            x = 0
            msg = 'Active transfers: {}'.format(active_transfers)
            pad.addstr(y, x, msg, curses.color_pair(4))
            pad.clrtoeol()
            if active_transfers:
                for line in event_list:
                    if 'Transfer' in line:
                        index = line.find('%')
                        if index:
                            s_index = line.rfind(' ', 0, index)
                            percent = float(line[s_index: index])
                            if percent < 100:
                                y += 1
                                pad.addstr(y, x, line, curses.color_pair(4))
                                pad.clrtoeol()
            for line in event_list:
                if 'hosted' in line:
                    y += 1
                    pad.addstr(y, x, line, curses.color_pair(4))
            spin_line = spinner[spin_index]
            spin_index += 1
            if spin_index == spin_len:
                spin_index = 0
            y += 1
            pad.addstr(y, x, spin_line, curses.color_pair(4))
            pad.clrtoeol()
            pad.clrtobot()
            y += 1
            if event and event.is_set():
                # enablePrint()
                return
            pad.refresh(0, 0, 3, 5, hmax, wmax)
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

    # A list of all the expected files
    file_list = {}
    # A list of all the file names
    file_name_list = {}
    # Describes the state of each job jet
    job_sets = []
    # The master configuration object
    config = {}
    # A list of all the threads
    thread_list = []
    # An event to kill the threads on terminal exception
    thread_kill_event = threading.Event()
    display_event = threading.Event()
    debug = False
    from_saved_state = False
    # The number of active globus transfers
    active_transfers = 0
    # A flag to tell if we have all the data locally
    all_data = False
    # A list of strings for holding display info
    event_list = []
    # Read in parameters from config
    config = setup(parser, event_list, display_event)
    
    # A list of files that have been transfered
    transfer_list = []

    state = []

    if config == -1:
        print "Error in setup, exiting"
        sys.exit(1)

    # check that all netCDF files exist
    path_exists(config)
    # cleanup any temp directories from previous runs
    cleanup()
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
            print 'keyboard'
            display_event.set()
            sys.exit()

    # compute number of expected year_sets
    sim_start_year = int(config.get('global').get('simulation_start_year'))
    sim_end_year = int(config.get('global').get('simulation_end_year'))
    number_of_sim_years = sim_end_year - (sim_start_year - 1)
    frequencies = config.get('global').get('set_frequency')
    if not from_saved_state:
        job_sets = []
        line = 'Initializing year sets'
        event_list = push_event(event_list, line)
        for freq in frequencies:
            freq = int(freq)
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

    # initialize the file_list
    line = 'Initializing file list'
    event_list = push_event(event_list, line)
    for key, val in config.get('global').get('output_patterns').items():
        file_list[key] = {}
        file_name_list[key] = {}
        if key in ['ATM', 'MPAS_AM', 'MPAS_CICE']:
            for year in range(1, number_of_sim_years + 1):
                for month in range(1, 13):
                    file_key = str(year) + '-' + str(month)
                    file_list[key][file_key] = SetStatus.NO_DATA
                    file_name_list[key][file_key] = ''
        elif key == 'MPAS_CICE_IN':
            file_list[key]['mpas-cice_in'] = SetStatus.NO_DATA
        elif key == 'MPAS_O_IN':
            file_list[key]['mpas-o_in'] = SetStatus.NO_DATA
        elif key == 'RPT':
            file_list[key]['rpointer.ocn'] = SetStatus.NO_DATA
            file_list[key]['rpointer.atm'] = SetStatus.NO_DATA
        elif key == 'MPAS_RST':
            for year in range(2, number_of_sim_years + 1):
                file_key = '{year}-1'.format(year=year)
                file_list[key][file_key] = SetStatus.NO_DATA
        elif key == 'STREAMS':
            file_list[key]['streams.ocean'] = SetStatus.NO_DATA
            file_list[key]['streams.cice'] = SetStatus.NO_DATA

    # Check for any data already on the System
    all_data = check_for_inplace_data(
        file_list=file_list,
        file_name_list=file_name_list,
        job_sets=job_sets,
        config=config)
    check_year_sets(
        job_sets=job_sets,
        file_list=file_list,
        sim_start_year=config.get('global').get('simulation_start_year'),
        sim_end_year=config.get('global').get('simulation_end_year'),
        debug=debug,
        add_jobs=add_jobs)
    state_path = os.path.join(
        config.get('global').get('output_path'),
        'run_state.txt')
    if config.get('global').get('dry_run', False):
        event_list = push_event(event_list, 'Running in dry-run mode')
        write_human_state(event_list, job_sets, state_path)
        if not config.get('global').get('no-ui', False):
            sleep(50)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join()
            sys.exit()

    if all_data:
        # print_message('All data is local, disabling remote monitor', 'ok')
        line = 'All data is local, disabling remote monitor'
        event_list = push_event(event_list, line)
    else:
        # print_message('More data needed, enabling remote monitor', 'ok')
        line = 'More data needed, enabling remote monitor'
        event_list = push_event(event_list, line)

    # If all the data is local, dont start the monitor
    if all_data or config.get('global').get('no_monitor', False):
        monitor = None
    else:
        output_pattern = config.get('global').get('output_patterns')
        patterns = [v for k, v in config.get('global').get('output_patterns').items()]
        monitor_config = {
            'source_endpoint': config.get('transfer').get('source_endpoint'),
            'remote_dir': config.get('transfer').get('source_path'),
            'username': config.get('monitor').get('compute_username'),
            'patterns': patterns,
            'file_list': file_list,
            'event_list': event_list,
            'display_event': display_event
        }
        if not config.get('global').get('ui'):
            addr = config.get('global').get('email')
            monitor_config['no_ui'] = True
            monitor_config['src'] = addr
            monitor_config['dst'] = addr
        logging.info('Setting up monitor with config {}'.format(pformat(monitor_config)))
        monitor = Monitor(monitor_config)
        if not monitor:
            line = 'error setting up monitor'
            event_list = push_event(event_list, line)
            sys.exit()

        line = 'Attempting connection to {}'.format(config.get('monitor').get('source_endpoint'))
        event_list = push_event(event_list, line)

        status, message = monitor.connect()
        event_list = push_event(event_list, message)
        if not status:
            line = "Unable to connect to globus service, exiting"
            logging.error(line)
            event_list = push_event(event_list, line)
            sleep(4)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join()
            sleep(1)
            print line
            print message
            sleep(1)
            sys.exit(1)
        else:
            line = 'Connected'
            logging.info(line)
            event_list = push_event(event_list, line)

    # Main loop
    try:
        loop_count = 6
        while True:
            # only check the monitor once a minute, but check for jobs every loop
            if monitor and \
               not all_data and \
               not config.get('global').get('no_monitor', False) and \
               loop_count >= 6:
                monitor_check(monitor, config, file_list, event_list, display_event)
                loop_count = 0
            all_data = check_for_inplace_data(
                file_list=file_list,
                file_name_list=file_name_list,
                job_sets=job_sets,
                config=config)
            check_year_sets(
                job_sets=job_sets,
                file_list=file_list,
                sim_start_year=config.get('global').get('simulation_start_year'),
                sim_end_year=config.get('global').get('simulation_end_year'),
                debug=debug,
                add_jobs=add_jobs)
            start_ready_job_sets(
                job_sets=job_sets,
                thread_list=thread_list,
                debug=debug,
                event=thread_kill_event,
                upload_config=config.get('upload_diagnostic'),
                event_list=event_list)
            write_human_state(event_list, job_sets, state_path)
            if is_all_done():
                if not config.get('global').get('no-cleanup', False):
                    cleanup()
                message = ' ---- All processing complete ----'
                emailaddr = config.get('global').get('email')
                if emailaddr:
                    try:
                        msg = '''
Processing job {id} has completed successfully

You can view your diagnostic output here:\n'''.format(
                            id= config.get('global').get('run_id'))
                        for event in event_list:
                            if 'hosted' in event:
                                msg += event + '\n'
                        m = Mailer(src=emailaddr, dst=emailaddr)
                        m.send(
                            status=message,
                            msg=msg)
                    except Exception as e:
                        logging.error(format_debug(e))
                event_list = push_event(event_list, message)
                sleep(5)
                display_event.set()
                sleep(2)
                print_message(message, 'ok')
                logging.info("## All processes complete")
                sys.exit(0)
            sleep(10)
            loop_count += 1
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        print_message('cleaning up threads', 'ok')
        display_event.set()
        for t in thread_list:
            thread_kill_event.set()
            t.join()
