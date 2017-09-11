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

from shutil import rmtree
from shutil import move
from shutil import copyfile
from getpass import getpass
from time import sleep
from uuid import uuid4
from pprint import pformat
from datetime import datetime
from jobs.Transfer import Transfer
from jobs.Ncclimo import Climo
from jobs.Timeseries import Timeseries
from jobs.AMWGDiagnostic import AMWGDiagnostic
from jobs.CoupledDiagnostic import CoupledDiagnostic
from jobs.ACMEDiags import ACMEDiags
from jobs.JobStatus import JobStatus

from lib.Monitor import Monitor
from lib.YearSet import YearSet, SetStatus
from lib.mailer import Mailer
from lib.events import Event, Event_list
from lib.setup import setup
from lib.util import *

# setup argument parser
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file.')
parser.add_argument('-n', '--no-ui', help='Turn off the GUI.', action='store_true')
parser.add_argument('-l', '--log', help='Path to logging output file.')
parser.add_argument('-u', '--no-cleanup', help='Don\'t perform pre or post run cleanup. This will leave all run scripts in place.', action='store_true')
parser.add_argument('-m', '--no-monitor', help='Don\'t run the remote monitor or move any files over globus.', action='store_true')
parser.add_argument('-s', '--size', help='The maximume size in gigabytes of a single transfer, defaults to 100. Must be larger then the largest single file.')

# check for NCL
if not os.environ.get('NCARG_ROOT'):
    ncar_path = '/usr/local/src/NCL-6.3.0/'
    if os.path.exists(ncar_path):
        os.environ['NCARG_ROOT'] = ncar_path
    else:
        print 'No NCARG_ROOT found in environment variables, make sure NCL installed on the machine and add its path to your ~/.bashrc'
        sys.exit()

# create global Event_list
event_list = Event_list()

# Map for keeping track of file types
file_type_map = {
    'MPAS_AM': ('mpas_dir', 1, None),
    'MPAS_CICE': ('mpas_cice_dir', 1, None),
    'ATM': ('atm_dir', 1, None),
    'MPAS_RST': ('mpas_rst_dir', 0, 'mpaso.rst.0003-01-01_00000.nc'),
    'MPAS_O_IN': ('mpas_o-in_dir', 0, 'mpas-o_in'),
    'MPAS_CICE_IN': ('mpas_cice-in_dir', 0, 'mpas-cice_in'),
    'STREAMS': ('streams_dir', 0, None),
    'RPT': ('rpt_dir', 0, None)
}

def add_jobs(year_set):
    """
    Initializes and adds all the jobs to the year_set

    Parameters:
        year_set (YearSet): The YearSet to populate with jobs
    """
    # each required job is a key, the value is if its in the job list already or not
    # this is here in case the jobs have already been added
    required_jobs = {}
    # print pformat(config.get('global').get('set_jobs').items())
    for job_type, freqs in config.get('global').get('set_jobs').items():
        if not freqs:
            # This job shouldnt be run
            required_jobs[job_type] = True
        if isinstance(freqs, str):
            # there is only one frequency for this job
            if freqs and int(freqs) == year_set.length:
                # this job should be run
                required_jobs[job_type] = False
            else:
                required_jobs[job_type] = True
        else:
            # this is a list if frequencies
            for freq in freqs:
                freq = int(freq)
                if freq == year_set.length:
                    required_jobs[job_type] = False
                    break
                else:
                    required_jobs[job_type] = True

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
    climo_temp_dir = os.path.join(
        config['global']['tmp_path'],
        'climo',
        year_set_str)
    create_symlink_dir(
        src_dir=config.get('global').get('atm_dir'),
        src_list=climo_file_list,
        dst=climo_temp_dir)

    global_config = config.get('global')
    # first initialize the climo job
    if not required_jobs['ncclimo']:
        required_jobs['ncclimo'] = True
        climo_output_dir = os.path.join(
            config.get('global').get('output_path'),
            'climo',
            '{}yr'.format(year_set.length))
        if not os.path.exists(climo_output_dir):
            os.makedirs(climo_output_dir)

        regrid_output_dir = os.path.join(
            config.get('global').get('output_path'),
            'climo_regrid')
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
            'monthly',
            '{}yr'.format(year_set.length))
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
            'regrid_map_path': config.get('ncclimo').get('regrid_map_path')
        }
        timeseries = Timeseries(timeseries_config, event_list=event_list)
        timeseries.depends_on = []
        msg = 'Adding Timeseries job to the job list: {}'.format(str(timeseries))
        logging.info(msg)
        year_set.add_job(timeseries)

    if not required_jobs['coupled_diags']:
        required_jobs['coupled_diags'] = True
        coupled_config = config.get('coupled_diags')

        coupled_project_dir = os.path.join(
            config.get('global').get('output_path'),
            'coupled_diags',
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year))
        if not os.path.exists(coupled_project_dir):
            os.makedirs(coupled_project_dir)
        
        web_directory = os.path.join(
            config.get('global').get('host_directory'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('coupled_diags').get('host_directory'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year))
        host_url = '/'.join([
            config.get('global').get('img_host_server'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('coupled_diags').get('host_url_prefix'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year)])

        template_path = os.path.join(
            sys.prefix,
            'share',
            'acme_workflow',
            'resources',
            'run_AIMS_template.csh')

        coupled_diaglobal_config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': config.get('global').get('experiment'),
            'rpt_dir': global_config.get('rpt_dir'),
            'run_ocean': coupled_config.get('run_ocean', False),
            'mpaso_regions_file': coupled_config.get('mpaso_regions_file'),
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'output_base_dir': coupled_project_dir,
            'mpas_am_dir': global_config.get('mpas_dir'),
            'mpas_cice_dir': global_config.get('mpas_cice_dir'),
            'mpas_cice_in_dir': global_config.get('mpas_cice-in_dir'),
            'mpas_o_dir': global_config.get('mpas_o-in_dir'),
            'mpas_rst_dir': global_config.get('mpas_rst_dir'),
            'streams_dir': global_config.get('streams_dir'),
            'run_id': config.get('global').get('run_id'),
            'dataset_name': dataset_name,
            'year_set': year_set.set_number,
            'climo_tmp_dir': climo_temp_dir,
            'regrid_path': regrid_output_dir,
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'nco_path': config.get('ncclimo').get('ncclimo_path'),
            'coupled_project_dir': coupled_project_dir,
            'test_casename': global_config.get('experiment'),
            'test_native_res': coupled_config.get('test_native_res'),
            'test_archive_dir': diag_temp_dir,
            'test_begin_yr_climo': year_set.set_start_year,
            'test_end_yr_climo': year_set.set_end_year,
            'test_begin_yr_ts': year_set.set_start_year,
            'test_end_yr_ts': year_set.set_end_year,
            'ref_case': coupled_config.get('ref_case'),
            'ref_archive_dir': coupled_config.get('ref_archive_dir'),
            'mpas_meshfile': coupled_config.get('mpas_meshfile'),
            'mpas_remapfile': coupled_config.get('mpas_remapfile'),
            'pop_remapfile': coupled_config.get('pop_remapfile'),
            'remap_files_dir': coupled_config.get('remap_files_dir'),
            'GPCP_regrid_wgt_file': coupled_config.get('gpcp_regrid_wgt_file'),
            'CERES_EBAF_regrid_wgt_file': coupled_config.get('ceres_ebaf_regrid_wgt_file'),
            'ERS_regrid_wgt_file': coupled_config.get('ers_regrid_wgt_file'),
            'coupled_diags_home': coupled_config.get('coupled_diags_home'),
            'coupled_template_path': template_path,
            'rendered_output_path': os.path.join(coupled_project_dir, 'run_AIMS.csh'),
            'obs_ocndir': coupled_config.get('obs_ocndir'),
            'obs_seaicedir': coupled_config.get('obs_seaicedir'),
            'obs_sstdir': coupled_config.get('obs_sstdir'),
            'depends_on': [],
            'yr_offset': coupled_config.get('yr_offset')
        }
        coupled_diag = CoupledDiagnostic(coupled_diaglobal_config, event_list)
        msg = 'Adding CoupledDiagnostic job to the job list: {}'.format(str(coupled_diag))
        logging.info(msg)
        year_set.add_job(coupled_diag)

    if not required_jobs['amwg']:
        required_jobs['amwg'] = True
        amwg_project_dir = os.path.join(
            config.get('global').get('output_path'),
            'amwg',
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year))
        if not os.path.exists(amwg_project_dir):
            os.makedirs(amwg_project_dir)

        amwg_temp_dir = os.path.join(config['global']['tmp_path'], 'amwg', year_set_str)
        if not os.path.exists(diag_temp_dir):
            os.makedirs(diag_temp_dir)
        template_path = os.path.join(
            sys.prefix,
            'share',
            'acme_workflow',
            'resources',
            'amwg_template.csh')

        web_directory = os.path.join(
            config.get('global').get('host_directory'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('amwg').get('host_directory'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year))
        host_url = '/'.join([
            config.get('global').get('img_host_server'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('amwg').get('host_url_prefix'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year)])

        amwg_config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': config.get('global').get('experiment'),
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'run_id': config.get('global').get('run_id'),
            'host_directory': config.get('amwg').get('host_directory'),
            'dataset_name': dataset_name,
            'diag_home': config.get('amwg').get('diag_home'),
            'test_path': amwg_project_dir + os.sep,
            'test_casename': global_config.get('experiment'),
            'test_path_history': climo_temp_dir + os.sep,
            'regrided_climo_path': regrid_output_dir,
            'test_path_climo': amwg_temp_dir,
            'test_path_diag': amwg_project_dir,
            'start_year': year_set.set_start_year,
            'end_year': year_set.set_end_year,
            'year_set': year_set.set_number,
            'run_directory': amwg_project_dir,
            'template_path': template_path,
            'depends_on': ['ncclimo']
        }
        amwg_diag = AMWGDiagnostic(amwg_config, event_list)
        msg = 'Adding AMWGDiagnostic job to the job list: {}'.format(str(amwg_diag))
        logging.info(msg)
        year_set.add_job(amwg_diag)
    
    if not required_jobs['acme_diags']:
        required_jobs['acme_diags'] = True

        acme_diags_project_dir = os.path.join(
            config.get('global').get('output_path'),
            'acme_diags',
            'year_set_{}'.format(year_set.set_number))
        if not os.path.exists(acme_diags_project_dir):
            os.makedirs(acme_diags_project_dir)
        
        host_prefix = '{server}/{prefix}'.format(
            server=config.get('global').get('img_host_server', 'https://acme-viewer.llnl.gov'),
            prefix=config.get('acme_diags').get('host_url_prefix'))
        
        acme_diags_temp_dir = os.path.join(
            config.get('global').get('tmp_path'),
            'acme_diags',
            year_set_str)
        
        if not os.path.exists(acme_diags_temp_dir):
            os.makedirs(acme_diags_temp_dir)

        acme_config = config.get('acme_diags')

        web_directory = os.path.join(
            config.get('global').get('host_directory'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('acme_diags').get('host_directory'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year))
        host_url = '/'.join([
            config.get('global').get('img_host_server'),
            os.environ['USER'],
            config.get('global').get('experiment'),
            config.get('acme_diags').get('host_url_prefix'),
            '{:04d}-{:04d}'.format(year_set.set_start_year, year_set.set_end_year)])
        
        template_path = os.path.join(
            sys.prefix,
            'share',
            'acme_workflow',
            'resources',
            'acme_diags_template.py')
        

        acme_diags_config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': config.get('global').get('experiment'),
            'regrided_climo_path': regrid_output_dir,
            'reference_data_path': acme_config.get('reference_data_path'),
            'test_data_path': acme_diags_temp_dir,
            'test_name': config.get('global').get('experiment'),
            'seasons': acme_config.get('seasons'),
            'backend': acme_config.get('backend'),
            'sets': acme_config.get('sets'),
            'results_dir': acme_diags_project_dir,
            'diff_colormap': acme_config.get('diff_colormap'),
            'template_path': template_path,
            'run_scripts_path': config.get('global').get('run_scripts_path'),
            'end_year': year_set.set_end_year,
            'start_year': year_set.set_start_year,
            'year_set': year_set.set_number
        }
        acme_diag = ACMEDiags(acme_diags_config, event_list)
        msg = "Adding ACME Diagnostic to the job list: {}".format(
            str(acme_diag))
        logging.info(msg)
        year_set.add_job(acme_diag)

    return year_set

def monitor_check(monitor, config, file_list, event_list, display_event):
    """
    Check the remote directory for new files that match the given pattern,
    if there are any new files, create new transfer jobs. If they're in a new job_set,
    spawn the jobs for that set.

    inputs:
        monitor: a monitor object setup with a remote directory and an globus session
    """
    global job_sets
    global active_transfers
    global transfer_list

    # Only allow 2 transfers at once
    if active_transfers >= 2:
        return
    event_list.push(message="Running check for remote files")
    status, msg = monitor.check()
    if not status:
        event_list.push(message=msg)
        return
    new_files = monitor.new_files

    # extract file type information
    patterns = config.get('global').get('patterns')
    for file_info in new_files:
        for folder, file_type in patterns.items():
            if re.search(pattern=file_type, string=file_info['filename']):
                file_info['type'] = folder
                break

    checked_new_files = []

    custom_filetype = False
    for new_file in new_files:
        file_type = new_file.get('type')
        if not file_type:
            message = "File {0} has no file_type".format(
                new_file['filename'])
            event_list.push(message=message)
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
        else:
            file_key = new_file.get('filename')
            custom_filetype = True

        if custom_filetype:
            if not file_list[file_type].get(file_key):
                file_list[file_type][file_key] = SetStatus.NO_DATA
                status = SetStatus.NO_DATA
        else:
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

    if not checked_new_files:
        return

    t_config = config.get('transfer')
    global_config = config.get('global')
    m_config = config.get('monitor')

    transfer_config = {
        'size': t_config.get('size'),
        'file_list': checked_new_files,
        'source_endpoint': t_config.get('source_endpoint'),
        'destination_endpoint': t_config.get('destination_endpoint'),
        'source_path': global_config.get('source_path'),
        'destination_path': global_config.get('data_cache_path') + '/',
        'recursive': 'False',
        'pattern': config.get('global').get('patterns'),
        'ncclimo_path': config.get('ncclimo').get('ncclimo_path'),
        'src_email': config.get('global').get('email'),
        'display_event': display_event,
        'ui': config.get('global').get('ui')
    }

    # Check if the user is logged in, and all endpoints are active
    endpoints = [config['transfer']['source_endpoint'], config['transfer']['destination_endpoint']]
    setup_globus(
        endpoints=endpoints,
        display_event=display_event,
        no_ui=not config.get('global').get('ui'),
        src=config.get('global').get('email'),
        dst=config.get('global').get('email'),
        event_list=event_list)
            
    transfer = Transfer(transfer_config, event_list)

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

    # After the transfer has filterd out what items its actually going to transfer
    # update the file_list 
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
        else:
            file_key = item['filename']
        file_list[item_type][file_key] = SetStatus.IN_TRANSIT

    # create the message to display the transfer info
    start_file_name = transfer.config.get('file_list')[0]['filename']
    stype = transfer.config.get('file_list')[0]['type']
    end_file_name = transfer.config.get('file_list')[-1]['filename']
    etype = transfer.config.get('file_list')[-1]['type']
    index = start_file_name.find('-')
    start_readable = start_file_name[index - 4: index + 3]
    index = end_file_name.find('-')
    end_readable = end_file_name[index - 4: index + 3]
    message = 'Found {numfiles} new remote files, moving data from {stype}:{sname} to {etype}:{ename}'.format(
        numfiles=len(checked_new_files),
        sname=start_readable,
        ename=end_readable,
        stype=stype,
        etype=etype)
    event_list.push(
        message=message,
        data=transfer)
    message = "Starting file transfer with file_list: {file_list}".format(
        file_list=pformat(transfer.file_list))
    logging.info(message)

    if config.get('global').get('dry_run', False):
        return
    while True:
        try:
            thread = threading.Thread(
                target=handle_transfer,
                name='handle_transfer',
                args=(
                    transfer,
                    checked_new_files,
                    thread_kill_event,
                    event_list))
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
        transfer_job (TransferJob): the transfer job to execute and monitor
        f_list (list): the list of files being transfered
        event: a thread event to handle shutting down from keyboard exception
    """
    active_transfers += 1
    # start the transfer job
    transfer_job.execute(event)
    # the transfer is complete, so we can decrement the active_transfers counter
    active_transfers -= 1

    if transfer_job.status != JobStatus.COMPLETED:
        print_message("File transfer failed")
        message = "## Transfer {uuid} has failed".format(uuid=transfer_job.uuid)
        logging.error(message)
        event_list.push(message='Tranfer failed')
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

    # the master list of all job sets
    job_sets = []

    # Read in parameters from config
    config = setup(
        parser,
        display_event,
        file_list=file_list,
        file_name_list=file_name_list,
        job_sets=job_sets,
        event_list=event_list,
        file_type_map=file_type_map)

    # for section in config:
    #     print section
    #     for item in config[section]:
    #         print '\t', item, config[section][item]
    # sys.exit()


    # A list of files that have been transfered
    transfer_list = []

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
            print 'keyboard exit'
            display_event.set()
            sys.exit()

    # Check for any data already on the System
    all_data = check_for_inplace_data(
        file_list=file_list,
        file_name_list=file_name_list,
        config=config,
        file_type_map=file_type_map)
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
        event_list.push(message='Running in dry-run mode')
        write_human_state(
            event_list=event_list,
            job_sets=job_sets,
            state_path=state_path,
            ui_mode=config.get('global').get('ui'))
        if config.get('global').get('ui'):
            sleep(50)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join(timeout=1.0)
            sys.exit()

    # If all the data is local, dont start the monitor
    if all_data or config.get('global').get('no_monitor', False):
        monitor = None
    else:
        patterns = [v for k, v in config.get('global').get('patterns').items()]
        monitor_config = {
            'source_endpoint': config.get('transfer').get('source_endpoint'),
            'remote_dir': config.get('global').get('source_path'),
            'patterns': patterns,
            'file_list': file_list,
            'event_list': event_list,
            'display_event': display_event
        }
        if not config.get('global').get('ui'):
            addr = config.get('global').get('email')
            src = addr
            dst = addr
            ui_mode = False
        else:
            ui_mode = True
            src = None
            dst = None

        logging.info('Setting up monitor with config {}'.format(pformat(monitor_config)))
        monitor = Monitor(
            source_endpoint=config.get('transfer').get('source_endpoint'),
            remote_dir=config.get('global').get('source_path'),
            patterns=patterns,
            file_list=file_list,
            event_list=event_list,
            display_event=display_event,
            no_ui=ui_mode,
            src=src,
            dst=src)
        if not monitor:
            line = 'error setting up monitor'
            event_list.push(message=line)
            print line
            sys.exit()

        line = 'Attempting connection to source endpoint'
        event_list.push(message=line)
        # if not config.get('global').get('ui'):
        #     print line
        status, message = monitor.connect()
        event_list.push(message=message)
        if not status:
            line = "Unable to connect to globus service, exiting"
            logging.error(line)
            event_list.push(message=line)
            sleep(4)
            display_event.set()
            for t in thread_list:
                thread_kill_event.set()
                t.join(timeout=1.0)
            sleep(1)
            print line
            print message
            sleep(1)
            sys.exit(1)
        else:
            line = 'Connected'
            logging.info(line)
            event_list.push(message=line)
    
    # check if the case_scripts directory is present
    # if its not, transfer it over
    case_scripts_dir = os.path.join(
        config['global']['data_cache_path'], 'case_scripts')
    if not os.path.exists(case_scripts_dir):
        msg = 'case_scripts not local, transfering remote copy'
        event_list.push(message=msg)
        logging.info(msg)
        head, _ = os.path.split(config['global']['source_path'])
        src_path = os.path.join(head, 'case_scripts')
        while True:
            try:
                args = {
                    'source_endpoint': config['transfer']['source_endpoint'],
                    'destination_endpoint': config['transfer']['destination_endpoint'],
                    'src_path': src_path,
                    'dst_path': case_scripts_dir,
                    'event_list': event_list
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
                config=config,
                file_type_map=file_type_map)
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
                event_list=event_list)
            write_human_state(
                event_list=event_list,
                job_sets=job_sets,
                state_path=state_path,
                ui_mode=config.get('global').get('ui'))
            status = is_all_done(job_sets)
            if status >= 0:
                if not config.get('global').get('no-cleanup', False):
                    cleanup()
                message = 'All processing complete' if status == 1 else "One or more job failed"
                emailaddr = config.get('global').get('email')
                if emailaddr:
                    event_list.push(message='Sending notification email to ' + emailaddr)
                    try:
                        if status == 1:
                            diag_msg = ''
                            if config['global']['set_jobs'].get('coupled_diag') or \
                               config['global']['set_jobs'].get('amwg') or \
                               config['global']['set_jobs'].get('acme_diag'):
                                diag_msg = 'Your diagnostics can be found here:\n'
                                for evt in event_list.list:
                                    if 'hosted' in evt.message:
                                        diag_msg += evt.message + '\n'
                            msg = 'Post processing jobs have completed successfully\n\n{diag}'.format(
                                id=config.get('global').get('run_id'),
                                diag=diag_msg)
                        else:
                            msg = 'One or more job failed\n\n'
                            with open(state_path, 'r') as state_file:
                                for line in state_file.readlines():
                                    msg += line

                        m = Mailer(src=emailaddr, dst=emailaddr)
                        m.send(
                            status=message,
                            msg=msg)
                    except Exception as e:
                        logging.error(format_debug(e))
                event_list.push(message=message)
                sleep(5)
                display_event.set()
                print_type = 'ok' if status == 1 else 'error'
                print_message(message, print_type)
                sleep(2)
                logging.info("All processes complete")
                for t in thread_list:
                    thread_kill_event.set()
                    t.join(timeout=1.0)
                sys.exit(0)
            sleep(10)
            loop_count += 1
    except KeyboardInterrupt as e:
        print_message('----- KEYBOARD INTERUPT -----')
        print_message('cleaning up threads', 'ok')
        display_event.set()
        for t in thread_list:
            thread_kill_event.set()
            t.join(timeout=1.0)
