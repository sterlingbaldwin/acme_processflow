import logging
import subprocess
import sys
import traceback
import re
import os
import threading
import socket
import glob
import time

from pprint import pformat
from shutil import copytree, rmtree, move
from subprocess import Popen, PIPE
from time import sleep
from time import strftime
from datetime import datetime
from uuid import uuid4

from globus_cli.commands.login import do_link_login_flow, check_logged_in
from globus_cli.commands.ls import _get_ls_res as get_ls
from globus_cli.services.transfer import get_client
from globus_sdk import TransferData

from YearSet import SetStatus
from YearSet import YearSet
from jobs.JobStatus import JobStatus, StatusMap
from mailer import Mailer
from string import Formatter
from slurm import Slurm
from models import DataFile


def transfer_directory(**kwargs):
    """
    Transfer all the contents from source_endpoint:src_path to destination_endpoint:dst_path

    parameters:
        source_endpoint (str) the globus UUID for the source files
        destination_endpoint (str) the globus UUID for the destination
        src_path (str) the path to the source directory to copy
        dst_path (str) the path on the destination directory
    """
    source_endpoint = kwargs['source_endpoint']
    destination_endpoint = kwargs['destination_endpoint']
    src_path = kwargs['src_path']
    dst_path = kwargs['dst_path']
    event_list = kwargs['event_list']
    event = kwargs['event']

    client = get_client()
    transfer = TransferData(
        client,
        source_endpoint,
        destination_endpoint,
        sync_level='checksum')
    transfer.add_item(
        source_path=src_path,
        destination_path=dst_path,
        recursive=True)
    try:
        result = client.submit_transfer(transfer)
        task_id = result['task_id']
    except:
        return False

    directory_name = src_path.split(os.sep)[-1]
    msg = '{dir} transfer starting'.format(dir=directory_name)
    event_list.push(message=msg)
    while True:
        if event and event.is_set():
            client.cancel_task(task_id)
            return
        status = client.get_task(task_id).get('status')
        if status == 'SUCCEEDED':
            msg = '{dir} transfer complete'.format(dir=directory_name)
            return True
        elif status == 'FAILED':
            msg = '{dir} transfer FAILED'.format(dir=directory_name)
            return False
        else:
            event_list.push(message=msg)
            sleep(5)


def check_globus(**kwargs):
    """
    Check that the globus endpoints are not only active but will return information
    about the paths we're interested in.

    Im assuming that the endpoints have already been activated
    """
    try:
        endpoints = [{
            'type': 'source',
            'id': kwargs['source_endpoint'],
            'path': kwargs['source_path']
        }, {
            'type': 'destination',
            'id': kwargs['destination_endpoint'],
            'path': kwargs['destination_path']
        }]
    except Exception as e:
        print_debug(e)

    client = get_client()
    try:
        for endpoint in endpoints:
            res = get_ls(
                client,
                endpoint['path'],
                endpoint['id'],
                False, 0, False)
    except Exception as e:
        print_debug(e)
        return False, endpoint
    else:
        print "Access granted"
        return True, None


def strfdelta(tdelta, fmt):
    f = Formatter()
    d = {}
    l = {'D': 86400, 'H': 3600, 'M': 60, 'S': 1}
    k = map(lambda x: x[1], list(f.parse(fmt)))
    rem = int(tdelta.total_seconds())

    for i in ('D', 'H', 'M', 'S'):
        if i in k and i in l.keys():
            d[i], rem = divmod(rem, l[i])

    return f.format(fmt, **d)


def setup_globus(endpoints, no_ui=False, **kwargs):
    """
    Check globus login status and login as nessisary, then
    iterate over a list of endpoints and activate them all

    Parameters:
       endpoints: list of strings containing globus endpoint UUIDs
       no_ui: a boolean flag, true if running without the UI

       kwargs:
        event_list: the display event list to push user notifications
        display_event: the thread event for the ui to turn off the ui for grabbing input for globus
        src: an email address to send notifications to if running in no_ui mode
        dst: a destination email address
    return:
       True if successful, False otherwise
    """
    message_sent = False
    display_event = kwargs.get('display_event')

    if no_ui:
        if kwargs.get('src') is None or kwargs.get('dst') is None:
            logging.error('No source or destination given to setup_globus')
            print "No email address found"
            return False
        mailer = Mailer(
            src='processflowbot@llnl.gov',
            dst=kwargs['dst'])

    # First go through the globus login process
    while not check_logged_in():
        # if in no_ui mode, send an email to the user with a link to log in
        if no_ui:
            if kwargs.get('event_list'):
                line = 'Waiting on user to log into globus, email sent to {addr}'.format(
                    addr=kwargs['src'])
                kwargs['event_list'].push(message=line)
            if not message_sent:
                status = 'Globus login needed'
                message = 'Your automated post processing job requires you log into globus. Please ssh into {host} activate the environment and run {cmd}\n\n'.format(
                    host=socket.gethostname(),
                    cmd='"globus login"')
                print 'sending login message to {}'.format(kwargs['src'])
                message_sent = mailer.send(
                    status=status,
                    msg=message)
            sleep(30)
        # if in ui mode, set the display_event and ask for user input
        else:
            if not no_ui:
                display_event.set()
            print '================================================'
            do_link_login_flow()

    if not endpoints:
        if not no_ui:
            display_event.clear()
        return True
    if isinstance(endpoints, str):
        endpoints = [endpoints]

    message_sent = False
    activated = False
    email_msg = ''
    client = get_client()
    while not activated:
        activated = True
        for endpoint in endpoints:
            msg = 'activating endpoint {}'.format(endpoint)
            logging.info(msg)
            try:
                r = client.endpoint_autoactivate(endpoint, if_expires_in=3600)
            except Exception as e:
                print_debug(e)
                return False
            logging.info(r['code'])
            if r["code"] == "AutoActivationFailed":
                activated = False
                logging.info('endpoint autoactivation failed, going to manual')
                server_document = client.endpoint_server_list(endpoint)
                for server in server_document['DATA']:
                    hostname = server["hostname"]
                    break
                message = """
Data transfer server {server} requires manual activation.
Please open the following URL in a browser to activate the endpoint:
https://www.globus.org/app/endpoints/{endpoint}/activate

""".format(endpoint=endpoint, server=server['hostname'])
                print message
                if no_ui:
                    email_msg += message
                else:
                    raw_input("Press ENTER after activating the endpoint")
                    r = client.endpoint_autoactivate(endpoint, if_expires_in=3600)
                    if not r["code"] == "AutoActivationFailed":
                        activated = True

        if not activated:
            if not message_sent:
                print 'sending activation message to {}'.format(kwargs['dst'])
                message_sent = mailer.send(
                    status='Endpoint activation required',
                    msg=email_msg)
                if not message_sent:
                    print "Error sending notification email"
                    logging.error("Unable to send notification email")
                    return False
            sleep(30)
    if not no_ui:
        display_event.clear()
    return True


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
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    pattern = r'_{start:04d}\d\d_{end:04d}\d\d_'.format(
        start=start_year,
        end=end_year)
    return [x for x in contents if re.search(pattern=pattern, string=x)]


def path_exists(config_items):
    """
    Checks the config for any netCDF file paths and validates that they exist
    """
    for section, options in config_items.items():
        if type(options) != dict:
            continue
        for key, val in options.items():
            if not type(val) == str:
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
    return '1: {doc} \n2: {exec_info} \n3: {exec_0} \n 4: {exec_1} \n5: {lineno} \n6: {stack}'.format(
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
        event_list (Event_list): The global list of all events
        job_sets (list: YearSet): The global list of all YearSets
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
                    status=year_set.status)
                out_str += line

                for job in year_set.jobs:
                    line = '  >   {type} -- {id}: {status}\n'.format(
                        type=job.type,
                        id=job.job_id,
                        status=job.status)
                    out_str += line

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

    if print_file_list:
        head, _ = os.path.split(state_path)
        file_list_path = os.path.join(head, 'file_list.txt')
        if not os.path.exists(head):
            os.makedirs(head)
        with open(file_list_path, 'w') as fp:
            mutex.acquire()
            types = [x.datatype for x in DataFile.select(
                DataFile.datatype).distinct()]
            try:
                for _type in types:
                    fp.write(_type + ':\n')
                    datafiles = DataFile.select().where(DataFile.datatype == _type)
                    for datafile in datafiles:

                        filestr = '------------------------------------------'
                        filestr += '\n     name: ' + datafile.name + '\n     local_status: '
                        if datafile.local_status == 0:
                            filestr += ' present, '
                        elif datafile.local_status == 1:
                            filestr += ' missing, '
                        else:
                            filestr += ' in transit, '
                        filestr += '\n     remote_status: '
                        if datafile.remote_status == 0:
                            filestr += ' present'
                        elif datafile.remote_status == 1:
                            filestr += ' missing'
                        else:
                            filestr += ' in transit'
                        filestr += '\n     local_size: ' + \
                            str(datafile.local_size)
                        filestr += '\n     local_path: ' + datafile.local_path
                        filestr += '\n     remote_size: ' + \
                            str(datafile.remote_size)
                        filestr += '\n     remote_path: ' + datafile.remote_path + '\n'
                        fp.write(filestr)
            except Exception as e:
                print_debug(e)
            finally:
                if mutex.locked():
                    mutex.release()


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
                    rendered_string += str(rendered_start) + \
                        str(rendered_middle) + str(rendered_end)
                else:
                    continue
        else:
            rendered_string = line
        outfile.write(rendered_string)
    return True


def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in src_list
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
            logging.error(e)


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
