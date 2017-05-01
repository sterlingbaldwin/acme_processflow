from time import sleep
from time import strftime
import logging
from subprocess import Popen, PIPE
import subprocess
import sys
import traceback
import re
import os
import threading
from pprint import pformat
from shutil import copytree, rmtree

from YearSet import SetStatus
from YearSet import YearSet
from jobs.JobStatus import JobStatus

def year_from_filename(filename):
    pattern = r'\.\d\d\d\d-'
    index = re.search(pattern, filename)
    if index:
        year = int(filename[index.start() + 1: index.start() + 5])
        return year
    else:
        return 0

def get_climo_output_files(input_path, set_start_year, set_end_year):
    contents = os.listdir(input_path)
    file_list_tmp = [s for s in contents if not os.path.isdir(s)]
    file_list = []
    for climo_file in file_list_tmp:
        start_search = re.search(r'\_\d\d\d\d\d\d', climo_file)
        if not start_search:
            continue
        start_index = start_search.start() + 1
        start_year = int(climo_file[start_index: start_index + 4])
        if not start_year == set_start_year:
            continue
        end_search = re.search(r'\_\d\d\d\d\d\d', climo_file[start_index:])
        if not end_search:
            continue
        end_index = end_search.start() + start_index + 1
        end_year = int(climo_file[end_index: end_index + 4])
        if not end_year == set_end_year:
            continue
        file_list.append(climo_file)
    return file_list

def path_exists(config_items):
    """
    Checks the config for any netCDF file paths and validates that they exist
    """
    for section, options in config_items.items():
        if type(options) != dict:
            continue
        for key, val in options.items():
            if key == 'output_pattern':
                continue
            if not type(val) == str:
                continue
            if val.endswith('.nc') and not os.path.exists(val):
                print "File {key}: {value} does not exist, exiting.".format(key=key, value=val)
                sys.exit(1)

def check_year_sets(job_sets, file_list, sim_start_year, sim_end_year, debug, add_jobs):
    """
    Checks the file_list, and sets the year_set status to ready if all the files are in place,
    otherwise, checks if there is partial data, or zero data
    """
    incomplete_job_sets = [s for s in job_sets
                           if s.status != SetStatus.COMPLETED
                           and s.status != SetStatus.RUNNING
                           and s.status != SetStatus.FAILED]
    for job_set in incomplete_job_sets:

        start_year = job_set.set_start_year
        end_year = job_set.set_end_year

        non_zero_data = False
        data_ready = True
        for i in range(start_year, end_year + 1):
            for j in range(1, 13):
                file_key = '{0}-{1}'.format(i, j)
                status = file_list['ATM'][file_key]

                if status in [SetStatus.NO_DATA, SetStatus.IN_TRANSIT, SetStatus.PARTIAL_DATA]:
                    data_ready = False
                elif status == SetStatus.DATA_READY:
                    non_zero_data = True

        if data_ready:
            job_set.status = SetStatus.DATA_READY
            job_set = add_jobs(job_set)
            continue
        if not data_ready and non_zero_data:
            job_set.status = SetStatus.PARTIAL_DATA
            continue
        if not data_ready and not non_zero_data:
            job_set.status = SetStatus.NO_DATA

    # if debug:
    #     for job_set in job_sets:
    #         start_year = job_set.set_start_year
    #         end_year = job_set.set_end_year
    #         print_message('year_set: {0}: {1}'.format(job_set.set_number, job_set.status), 'ok')
    #         for i in range(start_year, end_year + 1):
    #             for j in range(1, 13):
    #                 file_key = '{0}-{1}'.format(i, j)
    #                 status = file_list[file_key]
    #                 print_message('  {key}: {value}'.format(key=file_key, value=status), 'ok')


def start_ready_job_sets(job_sets, thread_list, debug, event, upload_config, event_list):
    """
    Iterates over the job sets checking for ready ready jobs, and starts them

    input:
        job_sets: a list of YearSets,
        thread_list: the list of currently running threads,
        debug: boolean debug flag,
        event: an event to pass to any threads we start so we can destroy them if needed
    """
    for job_set in job_sets:
        # if the job state is ready, but hasnt started yet
        if job_set.status == SetStatus.DATA_READY or job_set.status == SetStatus.RUNNING:
            for job in job_set.jobs:
                if job.depends_on:
                    ready = False
                    for dependancy in job.depends_on:
                        for djob in job_set.jobs:
                            if djob.get_type() == dependancy \
                               and djob.status == JobStatus.COMPLETED:
                                ready = True
                                break
                else:
                    ready = True
                if not ready:
                    continue
                # if the job isnt a climo, and the job that it depends on is done, start it
                if job.status == JobStatus.VALID:
                    while True:
                        try:
                            args = (
                                job,
                                job_set, event,
                                debug, 'slurm',
                                upload_config, event_list)
                            thread = threading.Thread(
                                target=monitor_job,
                                args=args)
                            thread_list.append(thread)
                            thread.start()
                        except Exception as e:
                            print_debug(e)
                            sleep(1)
                        else:
                            break
                if job.status == JobStatus.INVALID:
                    message = "{type} id: {id} status changed to {status}".format(
                        id=job.job_id,
                        status=job.status,
                        type=job.get_type())
                    logging.error('## ' + message)

def cmd_exists(cmd):
    return any(os.access(os.path.join(path, cmd), os.X_OK) for path in os.environ["PATH"].split(os.pathsep))

def handle_completed_job(job, job_set, event_list):
    """
    Perform post execution tasks
    """
    if not job.postvalidate():
        event_list = push_event(
            event_list,
            '{} completed but doesnt have expected output'.format(job.get_type()))
        job.status = JobStatus.FAILED

    if job.get_type() == 'coupled_diagnostic':
        img_dir = 'coupled_diagnostics_{casename}-obs'.format(
            casename=job.config.get('test_casename'))
        img_src = os.path.join(
            job.config.get('coupled_project_dir'),
            img_dir)
        setup_local_hosting(job, event_list, img_src)
    elif job.get_type() == 'amwg_diagnostic':
        img_dir = 'year_set_{year}{casename}-obs'.format(
            year=job.config.get('year_set'),
            casename=job.config.get('test_casename'))
        img_src = os.path.join(
            job.config.get('test_path_diag'),
            '..',
            img_dir)
        setup_local_hosting(job, event_list, img_src)
    elif job.get_type() == 'uvcmetrics':
        img_src = os.path.join(job.config.get('--outputdir'), 'amwg')
        setup_local_hosting(job, event_list, img_src)
    job_set_done = True
    for job in job_set.jobs:
        if job.status != JobStatus.COMPLETED:
            job_set_done = False
            break
        if job.status == JobStatus.FAILED:
            job_set.status = SetStatus.FAILED
            return
    if job_set_done:
        job_set.status = SetStatus.COMPLETED

def monitor_job(job, job_set, event=None, debug=False, batch_type='slurm', upload_config=None, event_list=None):
    """
    Monitor the slurm job, and update the status to 'complete' when it finishes
    This function should only be called from within a thread
    """
    job_id = job.execute(batch='slurm')
    if job_id == 0:
        job.set_status(JobStatus.COMPLETED)
    else:
        while job_id == -1:
            job.set_status(JobStatus.WAITING_ON_INPUT)
            if thread_sleep(60, event):
                return
            job_id = job.execute(batch='slurm')
        job.set_status(JobStatus.SUBMITTED)
        message = 'Submitted {0} for year_set {1}'.format(
            job.get_type(),
            job_set.set_number)
        event_list = push_event(event_list, message)
        logging.info('## ' + message)

    job.postvalidate()
    if job.status == JobStatus.COMPLETED:
        handle_completed_job(job, job_set, event_list)
        return

    exit_list = [JobStatus.VALID, JobStatus.SUBMITTED, JobStatus.RUNNING, JobStatus.PENDING]
    none_exit_list = [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.SUBMITTED]
    while True:
        # this check is here in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        if job.status not in exit_list:
            if job.status == JobStatus.INVALID:
                return
            if job.status == JobStatus.FAILED:
                job_set.status = SetStatus.FAILED
                return
            # if the job is done, or there has been an error, exit
            if job.status == JobStatus.COMPLETED:
                handle_completed_job(job, job_set, event_list)
                return
        elif job.status in none_exit_list and job_id != 0:
            cmd = ['scontrol', 'show', 'job', str(job_id)]
            while True:
                try:
                    out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
                except:
                    sleep(1)
                else:
                    break
            # loop through the scontrol output looking for the JobState field
            job_status = None
            for line in out.split('\n'):
                for word in line.split():
                    if 'JobState' in word:
                        index = word.find('=')
                        job_status = word[index + 1:]
                        break
                if job_status:
                    break
            if not job_status:
                sleep(5)
                continue

            status = None
            if job_status == 'RUNNING':
                status = JobStatus.RUNNING
            elif job_status == 'PENDING':
                status = JobStatus.PENDING
            elif job_status == 'FAILED':
                status = JobStatus.FAILED
            elif job_status == 'COMPLETED':
                status = JobStatus.COMPLETED

            if status and status != job.status:
                if status == JobStatus.FAILED:
                    msg = 'Job {0} has failed'.format(job_id)
                    event_list = push_event(event_list, msg)
                    if debug:
                        print_message(msg)

                job.status = status
                message = "{type}: {id} status changed to {status}".format(
                    id=job.job_id,
                    status=status,
                    type=job.get_type())
                logging.info('##' + message)
                # event_list = push_event(event_list, message)

                if status == JobStatus.RUNNING and job_set.status != SetStatus.RUNNING:
                    job_set.status = SetStatus.RUNNING

            # wait for 10 seconds, or if the kill_thread event has been set, exit
            if thread_sleep(10, event):
                return

def setup_local_hosting(job, event_list, img_src, generate=False):
    """
    Sets up the local directory for hosting diagnostic sets
    """
    msg = 'Setting up local hosting for {}'.format(job.get_type())
    event_list = push_event(event_list, msg)
    outter_dir = os.path.join(
        job.config.get('host_directory'),
        job.config.get('run_id'))
    if not os.path.exists(outter_dir):
        os.makedirs(outter_dir)
    host_dir = os.path.join(
        outter_dir,
        'year_set_{}'.format(str(job.config.get('year_set'))))
    if not os.path.exists(img_src):
        msg = '{job} hosting failed, no image source at {path}'.format(
            job=job.get_type(),
            path=img_src)
        logging.error(msg)
        return
    if os.path.exists(host_dir):
        try:
            msg = 'removing and replacing previous files from {}'.format(host_dir)
            logging.info(msg)
            rmtree(host_dir)
        except Exception as e:
            logging.error(format_debug(e))
            print_debug(e)
    try:
        msg = 'copying images from {src} to {dst}'.format(src=img_src, dst=host_dir)
        logging.info(msg)
        copytree(src=img_src, dst=host_dir)
    except Exception as e:
        logging.error(format_debug(e))
        msg = 'Error copying {} to host directory'.format(job.get_type())
        event_list = push_event(event_list, 'Error copying coupled_diag to host_location')
        return

    if generate:
        prev_dir = os.getcwd()
        os.chdir(host_dir)
        job.generateIndex(output_dir=host_dir)
        os.chdir(prev_dir)

    subprocess.call(['chmod', '-R', '777', outter_dir])

    host_location = os.path.join(
        job.config.get('host_prefix'),
        job.config.get('run_id'),
        'year_set_{}'.format(str(job.config.get('year_set'))),
        'index.html')
    msg = '{job} hosted at {url}'.format(
        url=host_location,
        job=job.get_type())
    event_list = push_event(event_list, msg)

def check_for_inplace_data(file_list, file_name_list, job_sets, config):
    """
    Checks the data cache for any files that might alread   y be in place,
    updates the file_list and job_sets accordingly
    """
    cache_path = config.get('global').get('data_cache_path')
    sim_end_year = int(config.get('global').get('simulation_end_year'))
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
        return

    patterns = config.get('global').get('output_patterns')
    input_dirs = [os.path.join(cache_path, key) for key, val in patterns.items()]
    for input_dir in input_dirs:
        file_type = input_dir.split(os.sep)[-1]
        for input_file in os.listdir(input_dir):
            input_file_path = os.path.join(input_dir, input_file)
            file_key = ""
            if file_type in ['ATM', 'MPAS_AM', 'MPAS_CICE', 'MPAS_RST']:
                file_key = filename_to_file_list_key(filename=input_file)
                index = file_key.find('-')
                year = int(file_key[:index])
                if year > sim_end_year:
                    continue
                if not file_list[file_type][file_key] == SetStatus.IN_TRANSIT:
                    file_list[file_type][file_key] = SetStatus.DATA_READY
            elif file_type == 'MPAS_CICE_IN':
                file_key = 'mpas-cice_in'
                if os.path.exists(os.path.join(input_dir, input_file)) and \
                   not file_list[file_type][file_key] == SetStatus.IN_TRANSIT:
                    file_list[file_type][file_key] = SetStatus.DATA_READY
            elif file_type == 'MPAS_O_IN':
                file_key = 'mpas-o_in'
                if os.path.exists(os.path.join(input_dir, input_file)) and \
                   not file_list[file_type][file_key] == SetStatus.IN_TRANSIT:
                    file_list[file_type][file_key] = SetStatus.DATA_READY
            elif file_type == 'STREAMS':
                for file_key in ['streams.cice', 'streams.ocean']:
                    file_name_list[file_type][file_key] = input_file
                    if os.path.exists(os.path.join(input_dir, input_file)) and \
                       not file_list[file_type][file_key] == SetStatus.IN_TRANSIT:
                        file_list[file_type][file_key] = SetStatus.DATA_READY
            elif file_type == 'RPT':
                for file_key in ['rpointer.ocn', 'rpointer.atm']:
                    file_name_list[file_type][file_key] = input_file
                    if os.path.exists(os.path.join(input_dir, input_file)) and \
                       not file_list[file_type][file_key] == SetStatus.IN_TRANSIT:
                        file_list[file_type][file_key] = SetStatus.DATA_READY
            file_name_list[file_type][file_key] = input_file

    for key, val in patterns.items():
        for file_key in file_list[key]:
            if file_list[key][file_key] != SetStatus.DATA_READY:
                # print 'file: {} {} is not ready'.format(key, file_key)
                # sys.exit()
                return False
    return True

def print_debug(e):
    """
    Print an exceptions relavent information
    """
    print '1', e.__doc__
    print '2', sys.exc_info()
    print '3', sys.exc_info()[0]
    print '4', sys.exc_info()[1]
    print '5', traceback.tb_lineno(sys.exc_info()[2])
    _, _, tb = sys.exc_info()
    print '6', traceback.print_tb(tb)

def format_debug(e):
    """
    Return a string of an exceptions relavent information
    """
    _, _, tb = sys.exc_info()
    return '1: {doc} \n2: {exec_info} \n3: {exec_0} \n 4: {exec_1} \n5: {lineno} \n6: {stack}'.format(
        doc=e.__doc__,
        exec_info=sys.exc_info(),
        exec_0=sys.exc_info()[0],
        exec_1=sys.exc_info()[1],
        lineno=traceback.tb_lineno(sys.exc_info()[2]),
        stack=traceback.print_tb(tb))

def write_human_state(event_list, job_sets, state_path='run_state.txt'):
    """
    Writes out a human readable representation of the current execution state
    """
    import datetime

    try:
        with open(state_path, 'w') as outfile:
            line = "Execution state as of {0}\n".format(datetime.datetime.now().strftime('%d, %b %Y %I:%M'))
            out_str = line
            out_str += 'Running under process {0}\n\n'.format(os.getpid())
            for year_set in job_sets:
                line = 'Year_set {num}: {start} - {end}\n'.format(
                    num=year_set.set_number,
                    start=year_set.set_start_year,
                    end=year_set.set_end_year)
                out_str += line

                line = 'status: {status}\n'.format(
                    status=year_set.status)
                out_str += line

                for job in year_set.jobs:
                    line = '  >   {type} -- {id}: {status}\n'.format(
                        type=job.get_type(),
                        id=job.job_id,
                        status=job.status)
                    out_str += line
                out_str += '\n'
            out_str += '\n'
            for line in event_list[-20:]:
                if 'Transfer' in line:
                    continue
                if 'hosted' in line:
                    continue
                out_str += line + '\n'
            out_str += line + '\n'

            for line in event_list:
                if 'Transfer' not in line:
                    continue
                out_str += line + '\n'

            for line in event_list:
                if 'hosted' not in line:
                    continue
                out_str += line + '\n'
            outfile.write(out_str)
    except Exception as e:
        logging.error(format_debug(e))
        return

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def push_event(event_list, line):
    line = strftime("%I:%M") + ' ' + line
    event_list.append(line)
    # diff = len(event_list) - 5
    # if diff > 0:
    #     event_list = event_list[diff:]
    return event_list

def print_message(message, status='error'):
    if status == 'error':
        print colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message) + colors.ENDC
    elif status == 'ok':
        print colors.OKGREEN + '[+] ' + colors.ENDC + str(message)

def render(variables, input_path, output_path, delimiter='%%'):
    """
    Takes an input file path, an output file path, a set of variables, and a delimiter.
    For each instance of that delimiter wrapped around a substring, replaces the
    substring with its matching element from the varialbes dict

    An example variable dict and delimiter would be:

    variables = {
        'test_casename': '20161117.beta0.A_WCYCL1850S.ne30_oEC_ICG.edison',
        'test_native_res': 'ne30',
        'test_archive_dir': '/space2/test_data/ACME_simulations',
        'test_short_term_archive': '0',
        'test_begin_yr_climo': '6',
        'test_end_yr_climo': '10'
    }
    delim = '%%'

    """

    try:
        infile = open(input_path, 'r')
    except IOError as e:
        print 'unable to open input file: {}'.format(input_path)
        print_debug(e)
        return
    try:
        outfile = open(output_path, 'w')
    except IOError as e:
        print 'unable to open output file: {}'.format(output_path)
        print_debug(e)
        return

    for line in infile.readlines():
        rendered_string = ''
        match = re.search(delimiter + '[a-zA-Z_0-9]*' + delimiter, line)
        if match:
            delim_index = [m.start() for m in re.finditer(delimiter, line)]
            if len(delim_index) < 2:
                continue

            template_string = line[delim_index[0] + len(delimiter): delim_index[1]]
            for item in variables:
                if item == template_string:
                    rendered_start = line[:delim_index[0]]
                    rendered_middle = variables[item]
                    rendered_end = line[delim_index[0] + len(delimiter) + len(item) + len(delimiter):]
                    rendered_string += str(rendered_start) + str(rendered_middle) + str(rendered_end)
                else:
                    continue
        else:
            rendered_string = line
        outfile.write(rendered_string)

def filename_to_file_list_key(filename):
    """
    Takes a filename and returns the key for the file_list
    """
    pattern = r'\.\d\d\d\d-'
    index = re.search(pattern, filename)
    if index:
        year = int(filename[index.start() + 1: index.start() + 5])
    else:
        return '0-0'
    # the YYYY field is 4 characters long, the month is two
    year_offset = index.start() + 5
    # two characters for the month, and one for the - between year and month
    month_offset = year_offset + 3
    month = int(filename[year_offset + 1: month_offset])
    key = "{year}-{month}".format(year=year, month=month)
    return key

def filename_to_year_set(filename, freq):
    """
    Takes a filename and returns the year_set that the file belongs to
    """
    year = year_from_filename(filename)
    if year % freq == 0:
        return int(year / freq)
    else:
        return int(year / freq) + 1

def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in src_list
    """
    if not src_list:
        return
    message = "creating symlink directory at {dst} with files {src_list}".format(
        dst=dst,
        src_list=pformat(src_list))
    logging.info(message)
    if not os.path.exists(dst):
        os.makedirs(dst)
    for src_file in src_list:
        if not src_file:
            continue
        source = os.path.join(src_dir, src_file)
        destination = os.path.join(dst, src_file)
        if os.path.lexists(destination):
            continue
        try:
            os.symlink(source, destination)
        except Exception as e:
            msg = format_debug(e)
            logging.error(e)

def file_list_cmp(a, b):
    """
    A custom comparator function for the file_list object
    """
    a_index = a.find('-')
    b_index = b.find('-')
    a_year = int(a[:a_index])
    b_year = int(b[:b_index])
    if a_year > b_year:
        return 1
    elif a_year < b_year:
        return -1
    else:
        a_month = int(a[a_index + 1:])
        b_month = int(b[b_index + 1:])
        if a_month > b_month:
            return 1
        elif a_month < b_month:
            return -1
        else:
            return 0

def raw_file_cmp(a, b):
    a = a['filename'].split('/')[-1]
    b = b['filename'].split('/')[-1]
    if not filter(str.isdigit, a) or not filter(str.isdigit, b):
        return a > b
    a_index = a.find('-')
    b_index = b.find('-')
    try:
        a_year = int(a[a_index - 4: a_index])
    except:
        print a
    b_year = int(b[b_index - 4: b_index])
    if a_year > b_year:
        return 1
    elif a_year < b_year:
        return -1
    else:
        a_month = int(a[a_index + 1: a_index + 3])
        b_month = int(b[b_index + 1: b_index + 3])
        if a_month > b_month:
            return 1
        elif a_month < b_month:
            return -1
        else:
            return 0

def thread_sleep(seconds, event):
    """
    Allows a thread to sleep for one second at at time, and cancel when if the
    thread event is set
    """
    for i in range(seconds):
        if event and event.is_set():
            return 1
        sleep(1)
    return 0

def check_slurm_job_submission(expected_name):
    """
    Checks if a job with the expected_name is in the slurm queue
    """
    cmd = ['scontrol', 'show', 'job']
    job_id = 0
    found_job = False
    while True:
        while True:
            try:
                out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
                break
            except:
                sleep(1)
        out = out.split('\n')
        if 'error' in out[0]:
            sleep(1)
            msg = 'Error checking job status for {0}'.format(expected_name)
            logging.warning(msg)
            continue
        for line in out:
            for word in line.split():
                if 'JobId' in word:
                    index = word.find('=') + 1
                    job_id = int(word[index:])
                    # continue
                if 'Name' in word:
                    index = word.find('=') + 1
                    if word[index:] == expected_name:
                        found_job = True

                if found_job and job_id != 0:
                    return found_job, job_id
        sleep(1)
    return found_job, job_id

from jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
