from time import sleep
from time import strftime
import logging
from subprocess import Popen, PIPE
import sys
import traceback
import re
import os
import threading
from pprint import pformat

from YearSet import SetStatus
from YearSet import YearSet
from jobs.JobStatus import JobStatus

def get_climo_output_files(input_path, set_start_year, set_end_year):
    contents = os.listdir(input_path)
    file_list_tmp = [s for s in contents if not os.path.isdir(s)]
    file_list = []
    for file in file_list_tmp:
        start_search = re.search(r'\_\d\d\d\d', file)
        if not start_search:
            continue
        start_index = start_search.start() + 1
        start_year = int(file[start_index: start_index + 4])

        end_search = re.search(r'\_\d\d\d\d', file[start_index:])
        if not end_search:
            continue
        end_index = end_search.start() + start_index + 1
        end_year = int(file[end_index: end_index + 4])

        if start_year == set_start_year and end_year == set_end_year:
            file_list.append(file)
    return file_list

def path_exists(config_items):
    """
    Checks the config for any netCDF file paths and validates that they exist
    """
    for k, v in config_items.items():
        if type(v) != dict:
            continue
        for j, m in v.items():
            if j != 'output_pattern':
                if str(m).endswith('.nc'):
                    if not os.path.exists(m):
                        print "File {key}: {value} does not exist, exiting.".format(key=j, value=m)
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
                status = file_list[file_key]

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
        if debug:
            msg = 'year_set: {0} status: {1}'.format(job_set.set_number, job_set.status)
            print_message(msg, 'ok')
            logging.info(msg)
        if job_set.status == SetStatus.DATA_READY or job_set.status == SetStatus.RUNNING:
            for job in job_set.jobs:
                # if the job is a climo, and it hasnt been started yet, start it
                if debug:
                    msg = '    job type: {0}, job_status: {1}, job_id: {2}'.format(
                        job.get_type(),
                        job.status,
                        job.job_id)
                    print_message(msg, 'ok')
                    logging.info(msg)

                if job.get_type() == 'climo' and job.status == JobStatus.VALID:
                    # for debug purposes only
                    # job.status = JobStatus.COMPLETED
                    # return

                    job_id = job.execute(batch=True)
                    job.set_status(JobStatus.SUBMITTED)

                    message = 'Submitted Ncclimo for year_set {}'.format(job_set.set_number)
                    event_list = push_event(event_list, message)

                    message = "## {job} id: {id} status changed to {status}".format(
                        job=job.get_type(),
                        id=job.job_id,
                        status=job.status)
                    logging.info(message)

                    thread = threading.Thread(
                        target=monitor_job,
                        args=(job_id, job, job_set, event, debug, 'slurm', upload_config, event_list))
                    thread_list.append(thread)
                    thread.start()
                    return

                # if the job isnt a climo, and the job that it depends on is done, start it
                elif job.get_type() != 'climo' and job.status == JobStatus.VALID:
                    ready = True
                    for dependancy in job.depends_on:
                        if job_set.jobs[dependancy].status != JobStatus.COMPLETED:
                            ready = False
                            break

                    if ready:
                        job_id = job.execute(batch='slurm')
                        job.set_status(JobStatus.SUBMITTED)

                        message = 'Submitted {type} for year_set {set}'.format(
                            set=job_set.set_number,
                            type=job.get_type())
                        event_list = push_event(event_list, message)

                        message = "## {job}: {id} status changed to {status}".format(
                            job=job.get_type(),
                            id=job.job_id,
                            status=job.status)
                        logging.info(message)

                        thread = threading.Thread(
                            target=monitor_job,
                            args=(job_id, job, job_set, event, debug, 'slurm', upload_config, event_list))
                        thread_list.append(thread)
                        thread.start()
                        return
                elif job.status == 'invalid':
                    message = "{type} id: {id} status changed to {status}".format(
                        id=job.job_id,
                        status=job.status,
                        type=job.get_type())
                    logging.error(message)
                    print_message('===== INVALID JOB =====\n{}'.format(str(job)))

def cmd_exists(cmd):
    return any(os.access(os.path.join(path, cmd), os.X_OK) for path in os.environ["PATH"].split(os.pathsep))

def monitor_job(job_id, job, job_set, event=None, debug=False, batch_type='slurm', upload_config=None, event_list=None):
    """
    Monitor the slurm job, and update the status to 'complete' when it finishes
    This function should only be called from within a thread
    """

    status = None
    while True:
        # this check is here in case the loop is stuck and the thread needs to be canceled
        if event and event.is_set():
            return
        cmd = ['scontrol', 'show', 'job', str(job_id)]
        out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
        if not out or len(out) == 0:
            status = None

        # loop through the scontrol output looking for the JobState field
        job_status = None
        run_time = None
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

        if job_status == 'RUNNING':
            status = JobStatus.RUNNING
        elif job_status == 'PENDING':
            status = JobStatus.PENDING
        elif job_status == 'FAILED':
            status = JobStatus.FAILED
        elif job_status == 'COMPLETED':
            status = JobStatus.COMPLETED

        if status and status != job.status:
            if debug:
                if status != JobStatus.FAILED:
                    message = "## {type}: {id} status changed to {status}".format(
                        id=job.job_id,
                        status=status,
                        type=job.get_type())
                    print_message(message, 'ok')
                else:
                    message = "## {type}: {id} status changed to {status}".format(
                        id=job.job_id,
                        status=status,
                        type=job.get_type())
                    print_message(message)

            if status == JobStatus.FAILED:
                print_message('Job {0} has failed'.format(job_id))

            job.status = status
            message = "{type}: {id} status changed to {status}".format(
                id=job.job_id,
                status=status,
                type=job.get_type())
            logging.info('##' + message)
            event_list = push_event(event_list, message)

            if status == JobStatus.RUNNING and job_set.status != SetStatus.RUNNING:
                job_set.status = SetStatus.RUNNING

        if status == JobStatus.FAILED:
            job_set.status = SetStatus.FAILED
            if getattr(job, 'resubmit', None):
                if job.resubmit() == -1:
                    return
                else:
                    event_list = push_event(event_list, 'Resubmitting {type} job'.format(
                        type=job.get_type()))
                    continue
            return

        # if the job is done, or there has been an error, exit
        if status == JobStatus.COMPLETED:

            if job.get_type() == 'coupled_diagnostic':
                job.generateIndex()
                    # coupled_diag upload
                index_path = os.path.join(
                    job.config.get('coupled_project_dir'),
                    os.environ['USER'])
                if not os.path.exists(index_path):
                    os.makedirs(index_path)
                suffix = [s for s in os.listdir(index_path)
                          if 'coupled' in s
                          and not s.endswith('.logs')].pop()
                index_path = os.path.join(index_path, suffix)

                upload_config = {
                    'year_set': job_set.set_number,
                    'start_year': job_set.set_start_year,
                    'end_year': job_set.set_end_year,
                    'path_to_diagnostic': index_path,
                    'username': upload_config.get('diag_viewer_username'),
                    'password': upload_config.get('diag_viewer_password'),
                    'server': upload_config.get('diag_viewer_server'),
                    'depends_on': [len(job_set.jobs) - 2] # set the upload job to wait for the coupled_diag job to finish
                }
                upload_2 = UploadDiagnosticOutput(upload_config)
                msg = 'Adding Upload job to the job list: {}'.format(str(upload_2))
                logging.info(msg)
                job_set.add_job(upload_2)

            job_set_done = True
            for job in job_set.jobs:
                if job.status != JobStatus.COMPLETED:
                    job_set_done = False
                    break
                if job.status == JobStatus.FAILED:
                    # print_message('Setting set to status FAILED')
                    job_set.status = SetStatus.FAILED
                    return

            if job_set_done:
                job_set.status = SetStatus.COMPLETED

            return

        # wait for 10 seconds, or if the kill_thread event has been set, exit
        if thread_sleep(10, event):
            return

def check_for_inplace_data(file_list, file_name_list, job_sets, config):
    """
    Checks the data cache for any files that might alread   y be in place,
    updates the file_list and job_sets accordingly
    """
    cache_path = config.get('global').get('data_cache_path')
    date_pattern = config.get('global').get('date_pattern')
    output_pattern = config.get('global').get('output_pattern')

    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
        return

    for climo_file in os.listdir(cache_path):
        climo_file_path = os.path.join(cache_path, climo_file)
        file_key = filename_to_file_list_key(
            filename=climo_file,
            output_pattern=output_pattern,
            date_pattern=date_pattern)

        index = file_key.find('-')
        year = int(file_key[:index])
        in_range = False
        for job_set in job_sets:
            if year <= job_set.set_end_year:
                in_range = True
                break
        if not in_range:
            continue
        # if the file is less the 1MB, its probably still in transit
        if os.path.getsize(climo_file_path) / 100000000 < 1:
            file_list[file_key] = SetStatus.IN_TRANSIT
        else:
            file_list[file_key] = SetStatus.DATA_READY
        file_name_list[file_key] = climo_file

    all_data = True
    for key in file_list:
        if file_list[key] != SetStatus.DATA_READY:
            all_data = False
            break
    return all_data

def print_debug(e):
    print '1', e.__doc__
    print '2', sys.exc_info()
    print '3', sys.exc_info()[0]
    print '4', sys.exc_info()[1]
    print '5', traceback.tb_lineno(sys.exc_info()[2])
    ex_type, ex, tb = sys.exc_info()
    print '6', traceback.print_tb(tb)

def format_debug(e):
    ex_type, ex, tb = sys.exc_info()
    return '1: {doc} \n2: {exec_info} \n3: {exec_0} \n 4: {exec_1} \n5: {lineno} \n6: {stack}'.format(
        doc=e.__doc__,
        exec_info=sys.exc_info(),
        exec_0=sys.exc_info()[0],
        exec_1=sys.exc_info()[1],
        lineno=traceback.tb_lineno(sys.exc_info()[2]),
        stack=traceback.print_tb(tb)
    )

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

def filename_to_file_list_key(filename, output_pattern, date_pattern):
    """
    Takes a filename and returns the key for the file_list
    """
    date_pattern = date_pattern.replace('YYYY', '[0-9][0-9][0-9][0-9]')
    date_pattern = date_pattern.replace('MM', '[0-9][0-9]')
    date_pattern = date_pattern.replace('DD', '[0-9][0-9]')
    output_pattern = output_pattern.replace('YYYY', '0000')
    output_pattern = output_pattern.replace('MM', '00')
    output_pattern = output_pattern.replace('DD', '00')

    index = re.search(date_pattern, filename)
    if index:
        index = index.start()
    else:
        msg = 'Unable to find pattern {0} in {1}'.format(date_pattern, filename)
        print_message(msg)
        logging.error(msg)
        return ''
    # the YYYY field is 4 characters long, the month is two
    year_offset = index + 4
    # two characters for the month, and one for the - between year and month
    month_offset = year_offset + 3
    try:
        year = int(filename[index: year_offset])
    except:
        print 'filename ' + filename
        print 'index ' + str(index)
        print 'year ' + filename[index: year_offset]
    try:
        month = int(filename[year_offset + 1: month_offset])
    except:
        print 'filename ' + filename
        print 'index ' + str(index)
        print 'month ' + filename[year_offset + 1: month_offset]
    key = "{year}-{month}".format(year=year, month=month)
    return key

def filename_to_year_set(filename, pattern, freq):
    """
    Takes a filename and returns the year_set that the file belongs to
    """
    pattern_format = 'YYYY-MM'
    file_format = '.nc'
    if not filename.endswith(file_format):
        print_message('unable to find year set, unexpected file format')
        return 0
    file_date = filename[-(len(pattern_format) + len(file_format)): - len(file_format)]
    year = int(file_date[:4])
    if year % freq == 0:
        return int(year / freq)
    else:
        return int(year / freq) + 1

def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in src_list
    """
    if not os.path.exists(dst):
        os.makedirs(dst)
    for f in src_list:
        if not f or not src_list:
            continue
        source = os.path.join(src_dir, f)
        destination = os.path.join(dst, f)
        if os.path.lexists(destination):
            continue
        os.symlink(source, destination)

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
    error_count = 0
    job_id = 0
    found_job = False
    while True:
        out = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[0]
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
                    continue
                if 'Name' in word:
                    index = word.find('=') + 1
                    if word[index:] == expected_name:
                        found_job = True

                if found_job and job_id:
                    return found_job, job_id
        sleep(1)
        error_count += 1
    return found_job, job_id

from jobs.UploadDiagnosticOutput import UploadDiagnosticOutput
