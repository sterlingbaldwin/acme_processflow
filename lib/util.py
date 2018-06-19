import logging
import sys
import traceback
import re
import os
import socket

from shutil import rmtree
from time import sleep
from datetime import datetime
from string import Formatter

from lib.JobStatus import ReverseMap, JobStatus
from mailer import Mailer
from models import DataFile


def print_line(line, event_list, ignore_text=False):
    """
    Prints a message to either the console, the event_list, or the current event

    Parameters:
        line (str): The message to print
        event_list (EventList): the event list
        ignore_text (bool): should this be printed to the console if in text mode
    """
    logging.info(line)
    if not ignore_text:
        now = datetime.now()
        timestr = '{hour}:{min}:{sec}'.format(
            hour=now.strftime('%H'),
            min=now.strftime('%M'),
            sec=now.strftime('%S'))
        msg = '{time}: {line}'.format(
            time=timestr,
            line=line)
        print msg
        sys.stdout.flush()

def strfdelta(tdelta, fmt):
    """
    Turn a time delta into a string

    Parameters:
        tdelta (time.delta): the delta time to convert
        fmt (str): the format string to convert to
    Returns:
        A string with the formatted delta
    """
    f = Formatter()
    d = {}
    l = {'D': 86400, 'H': 3600, 'M': 60, 'S': 1}
    k = map(lambda x: x[1], list(f.parse(fmt)))
    rem = int(tdelta.total_seconds())

    for i in ('D', 'H', 'M', 'S'):
        if i in k and i in l.keys():
            d[i], rem = divmod(rem, l[i])

    return f.format(fmt, **d)


def get_climo_output_files(input_path, start_year, end_year):
    """
    Return a list of ncclimo climatologies from start_year to end_year

    Parameters:
        input_path (str): the directory to look in
        start_year (int): the first year of climos to add to the list
        end_year (int): the last year
    Returns:
        file_list (list(str)): A list of the climo files in the directory
    """
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    pattern = r'_{start:04d}\d\d_{end:04d}\d\d_climo\.nc'.format(
        start=start_year,
        end=end_year)
    return [x for x in contents if re.search(pattern=pattern, string=x)]

def get_ts_output_files(input_path, var_list, start_year, end_year):
    """
    Return a list of ncclimo timeseries files from a list of variables, start_year to end_year

    Parameters:
        input_path (str): the directory to look in
        var_list (list): a list of strings of variable names
        start_year (int): the first year of climos to add to the list
        end_year (int): the last year
    Returns:
        ts_list (list): A list of the ts files
    """
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    ts_list = list()
    for var in var_list:
        pattern = r'{var}_{start:04d}01_{end:04d}12\.nc'.format(
            var=var,
            start=start_year,
            end=end_year)
        for item in contents:
            if re.search(pattern, item):
                ts_list.append(item)
                break
    return ts_list

def get_data_output_files(input_path, case, start_year, end_year):
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    contents.sort()
    data_list = list()
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            pattern = r'%s.*\.%04d-%02d.nc' % (case, year, month)
            for item in contents:
                if re.match(pattern, item):
                    data_list.append(item)
                    break
    return data_list

def path_exists(config_items):
    """
    Checks the config for any netCDF file paths and validates that they exist

    Parameters:
        config_items (dict): The config to be checked
    Returns:
        bool, True if all netCDF files are present, False otherwise
    """
    for _, options in config_items.items():
        if not isinstance(options, dict):
            continue
        for key, val in options.items():
            if not isinstance(val, str):
                continue
            if val.endswith('.nc') and not os.path.exists(val):
                print "File {key}: {value} does not exist, exiting.".format(key=key, value=val)
                return False
    return True


def cmd_exists(cmd):
    return any(os.access(os.path.join(path, cmd), os.X_OK) for path in os.environ["PATH"].split(os.pathsep))

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
    return """
1: {doc}
2: {exec_info}
3: {exec_0}
4: {exec_1}
5: {lineno}
6: {stack}
""".format(
    doc=e.__doc__,
    exec_info=sys.exc_info(),
    exec_0=sys.exc_info()[0],
    exec_1=sys.exc_info()[1],
    lineno=traceback.tb_lineno(sys.exc_info()[2]),
    stack=traceback.print_tb(tb))

def write_human_state(event_list, job_sets, mutex, state_path='run_state.txt', print_file_list=False):
    """
    Writes out a human readable representation of the current execution state

    Paremeters
        event_list (EventList): The global list of all events
        state_path (str): The path to where to write the run_state
        ui_mode (bool): The UI mode, True if the UI is on, False if the UI is off
    """
    try:
        with open(state_path, 'w') as outfile:
            line = "Execution state as of {0}\n".format(
                datetime.now().strftime('%d, %b %Y %I:%M'))
            out_str = line
            out_str += 'Running under process {0}\n\n'.format(os.getpid())

            for year_set in job_sets:
                line = 'Year_set {num}: {start} - {end}\n'.format(
                    num=year_set.set_number,
                    start=year_set.set_start_year,
                    end=year_set.set_end_year)
                out_str += line

                line = 'status: {status}\n'.format(
                    status=SetStatusMap[year_set.status])
                out_str += line

                for job in year_set.jobs:
                    msg = ''
                    if job.status == JobStatus.COMPLETED:
                        if job.config.get('host_url'):
                            msg += '    > {job} - COMPLETED  :: output hosted :: {url}\n'.format(
                                url=job.config['host_url'],
                                job=job.type)
                        else:
                            msg += '    > {job} - COMPLETED  :: output located :: {output}\n'.format(
                                output=job.output_path,
                                job=job.type)
                    elif job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                        output_path = os.path.join(
                            job.config['run_scripts_path'],
                            '{job}_{start:04d}_{end:04d}.out'.format(
                                job=job.type,
                                start=job.start_year,
                                end=job.end_year))
                        msg += '    > {job} - {status} :: console output :: {output}\n'.format(
                            output=output_path,
                            job=job.type,
                            status=ReverseMap[job.status])
                    else:
                        msg += '    > {job} - {state}\n'.format(
                            job=job.type,
                            state=ReverseMap[job.status])

                    out_str += msg

                out_str += '\n'

            out_str += '\n'
            for line in event_list.list:
                if 'Transfer' in line.message:
                    continue
                if 'hosted' in line.message:
                    continue
                out_str += line.message + '\n'

            # out_str += line.message + '\n'
            for line in event_list.list:
                if 'Transfer' not in line.message:
                    continue
                out_str += line.message + '\n'

            for line in event_list.list:
                if 'hosted' not in line.message:
                    continue
                out_str += line.message + '\n'
            outfile.write(out_str)
            # if not ui_mode:
            #     print '\n'
            #     print out_str
            #     print '\n================================================\n'
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

def print_message(message, status='error'):
    """
    Prints a message with either a green + or a red -

    Parameters:
        message (str): the message to print
        status (str): th"""
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
        return False
    try:
        outfile = open(output_path, 'w')
    except IOError as e:
        print 'unable to open output file: {}'.format(output_path)
        print_debug(e)
        return False

    for line in infile.readlines():
        rendered_string = ''
        match = re.search(delimiter + '[a-zA-Z_0-9]*' + delimiter, line)
        if match:
            while match:
                delim_index = [m.start() for m in re.finditer(delimiter, line)]
                if len(delim_index) < 2:
                    continue

                template_string = line[delim_index[0] +
                                    len(delimiter): delim_index[1]]
                for item in variables:
                    if item == template_string:
                        rendered_start = line[:delim_index[0]]
                        rendered_middle = variables[item]
                        rendered_end = line[delim_index[0] +
                                            len(delimiter) + len(item) + len(delimiter):]
                        line_tmp = str(rendered_start) + \
                            str(rendered_middle) + str(rendered_end)
                        line = line_tmp
                    else:
                        continue
                match = re.search(delimiter + '[a-zA-Z_0-9]*' + delimiter, line_tmp)
            rendered_string += line_tmp
        else:
            rendered_string = line
        outfile.write(rendered_string)
    return True

def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in src_list

    Parameters:
        src_dir (str): the path to the source directory
        src_list (list): a list of strings of filenames
        dst (str): the path to the directory that should hold the symlinks
    """
    if not src_list:
        return
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
            logging.error(msg)

def thread_sleep(seconds, event):
    """
    Allows a thread to sleep for one second at at time, and cancel when if the
    thread event is set
    """
    for _ in range(seconds):
        if event and event.is_set():
            return 1
        sleep(1)
    return 0
