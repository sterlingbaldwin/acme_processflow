import os
import re
import sys
import threading
import logging
import random

from time import sleep
from peewee import *
from enum import IntEnum
from threading import Thread

from models import DataFile
from lib.jobstatus import JobStatus
from lib.util import print_debug
from lib.util import print_line

from lib.globus_interface import transfer as globus_transfer
from globus_cli.services.transfer import get_client

from lib.ssh_interface import transfer as ssh_transfer
from lib.ssh_interface import get_ssh_client


class FileStatus(IntEnum):
    PRESENT = 0
    NOT_PRESENT = 1
    IN_TRANSIT = 2


class FileManager(object):
    """
    Manage all files required by jobs
    """

    def __init__(self, mutex, event_list, config, database='processflow.db'):
        """
        Parameters:
            mutex (theading.Lock) the mutext for accessing the database
            database (str): the path to where to create the sqlite database file
            config (dict): the global configuration dict
        """
        self._mutex = mutex
        self._event_list = event_list
        self._db_path = database
        self._config = config

        if os.path.exists(database):
            os.remove(database)

        self._mutex.acquire()
        DataFile._meta.database.init(database)
        if DataFile.table_exists():
            DataFile.drop_table()

        DataFile.create_table()
        if self._mutex.locked():
            self._mutex.release()
        
        self.thread_list = list()
        self.kill_event = threading.Event()

    def __str__(self):
        # TODO: make this better
        return str({
            'db_path': self._db_path,
        })

    def get_endpoints(self):
        """
        Return a list of globus endpoints for all cases
        """
        self._mutex.acquire()
        q = (DataFile
             .select()
             .where(
                 DataFile.transfer_type == 'globus'))
        endpoints = list()
        for x in q.execute():
            if x.remote_uuid not in endpoints:
                endpoints.append(x.remote_uuid)
        self._mutex.release()
        return endpoints

    def write_database(self):
        """
        Write out a human readable version of the database for debug purposes
        """
        file_list_path = os.path.join(
            self._config['global']['project_path'],
            'output',
            'file_list.txt')
        with open(file_list_path, 'w') as fp:
            self._mutex.acquire()
            try:
                for case in self._config['simulations']:
                    if case in ['start_year', 'end_year', 'comparisons']:
                        continue
                    fp.write('+++++++++++++++++++++++++++++++++++++++++++++')
                    fp.write('\n\t{case}\t\n'.format(case=case))
                    fp.write('+++++++++++++++++++++++++++++++++++++++++++++\n')
                    q = (DataFile
                         .select(DataFile.datatype)
                         .where(DataFile.case == case)
                         .distinct())
                    for df_type in q.execute():
                        _type = df_type.datatype
                        fp.write('===================================\n')
                        fp.write('\t' + _type + ':\n')
                        datafiles = (DataFile
                                     .select()
                                     .where(
                                            (DataFile.datatype == _type) &
                                            (DataFile.case == case)))
                        for datafile in datafiles.execute():
                            filestr = '-------------------------------------'
                            filestr += '\n\t     name: ' + datafile.name + '\n\t     local_status: '
                            if datafile.local_status == 0:
                                filestr += ' present, '
                            elif datafile.local_status == 1:
                                filestr += ' missing, '
                            else:
                                filestr += ' in transit, '
                            filestr += '\n\t     remote_status: '
                            if datafile.remote_status == 0:
                                filestr += ' present'
                            elif datafile.remote_status == 1:
                                filestr += ' missing'
                            else:
                                filestr += ' in transit'
                            filestr += '\n\t     local_size: ' + \
                                str(datafile.local_size)
                            filestr += '\n\t     local_path: ' + datafile.local_path
                            filestr += '\n\t     remote_path: ' + datafile.remote_path + '\n'
                            fp.write(filestr)
            except Exception as e:
                print_debug(e)
            finally:
                if self._mutex.locked():
                    self._mutex.release()

    # def render_string(self, instring, **kwargs):
    #     """
    #     take a list of keyword arguments and replace uppercase instances of them in the input string
    #     """
    #     for string, val in kwargs.items():
    #         if string in instring:
    #             instring = instring.replace(string, val)
    #     return instring

    def check_data_ready(self, data_required, case, start_year=None, end_year=None):
        self._mutex.acquire()
        try:
            for datatype in data_required:
                if start_year and end_year:
                    q = (DataFile
                            .select()
                            .where(
                                (DataFile.year >= start_year) &
                                (DataFile.year <= end_year) &
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype)))
                else:
                    q = (DataFile
                            .select()
                            .where(
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype)))
                datafiles = q.execute()
                for df in datafiles:
                    if not os.path.exists(df.local_path) and df.local_status == FileStatus.PRESENT.value:
                        df.local_status = FileStatus.NOT_PRESENT.value
                        df.save()
                    elif os.path.exists(df.local_path) and df.local_status == FileStatus.NOT_PRESENT.value:
                        df.local_status = FileStatus.PRESENT.value
                        df.save()
                    if df.local_status != FileStatus.PRESENT.value:
                        return False
            return True
        finally:
            self._mutex.release()

    def render_file_string(self, data_type, data_type_option, case, year=None, month=None):
        """
        Takes strings from the data_types dict and replaces the keywords with the appropriate values
        """
        # setup the replacement dict
        start_year = int(self._config['simulations']['start_year'])
        end_year = int(self._config['simulations']['end_year'])

        replace = {
            'PROJECT_PATH': self._config['global']['project_path'],
            'REMOTE_PATH': self._config['simulations'][case].get('remote_path', ''),
            'CASEID': case,
            'REST_YR': '{:04d}'.format(start_year + 1),
            'START_YR': '{:04d}'.format(start_year),
            'END_YR': '{:04d}'.format(end_year)
        }
        if year is not None:
            replace['YEAR'] = '{:04d}'.format(year)
        if month is not None:
            replace['MONTH'] = '{:02d}'.format(month)

        if self._config['data_types'][data_type].get(case):
            if self._config['data_types'][data_type][case].get(data_type_option):
                instring = self._config['data_types'][data_type][case][data_type_option]
                for item in self._config['simulations'][case]:
                    if item.upper() in self._config['data_types'][data_type][case][data_type_option]:
                        instring = instring.replace(item.upper(), self._config['simulations'][case][item])
                return instring
        
        instring = self._config['data_types'][data_type][data_type_option]
        for string, val in replace.items():
            if string in instring:
                instring = instring.replace(string, val)
        return instring

    def populate_file_list(self):
        """
        Populate the database with the required DataFile entries
        """
        msg = 'Creating file table'
        print_line(
            line=msg,
            event_list=self._event_list)
        newfiles = list()
        start_year = int(self._config['simulations']['start_year'])
        end_year = int(self._config['simulations']['end_year'])
        with DataFile._meta.database.atomic():
            self._mutex.acquire()

            # for each case
            for case in self._config['simulations']:
                if case in ['start_year', 'end_year', 'comparisons']:
                    continue
                # for each data type
                for _type in self._config['data_types']:
                    data_types_for_case = self._config['simulations'][case]['data_types']
                    if 'all' not in data_types_for_case:
                        if _type not in data_types_for_case:
                            continue

                    # setup the base local_path
                    local_path = self.render_file_string(
                        data_type=_type,
                        data_type_option='local_path',
                        case=case)

                    new_files = list()
                    if self._config['data_types'][_type].get('monthly'):
                        # handle monthly data
                        for year in range(start_year, end_year + 1):
                            for month in range(1, 13):
                                filename = self.render_file_string(
                                    data_type=_type,
                                    data_type_option='file_format',
                                    case=case,
                                    year=year,
                                    month=month)
                                r_path = self.render_file_string(
                                    data_type=_type,
                                    data_type_option='remote_path',
                                    case=case,
                                    year=year,
                                    month=month)
                                new_files.append({
                                    'name': filename,
                                    'remote_path': os.path.join(r_path, filename),
                                    'local_path': os.path.join(local_path, filename),
                                    'local_status': FileStatus.NOT_PRESENT.value,
                                    'case': case,
                                    'remote_status': FileStatus.NOT_PRESENT.value,
                                    'year': year,
                                    'month': month,
                                    'datatype': _type,
                                    'local_size': 0,
                                    'transfer_type': self._config['simulations'][case]['transfer_type'],
                                    'remote_uuid': self._config['simulations'][case].get('remote_uuid', ''),
                                    'remote_hostname': self._config['simulations'][case].get('remote_hostname', '')
                                })
                    else:
                        # handle one-off data
                        filename = self.render_file_string(
                                    data_type=_type,
                                    data_type_option='file_format',
                                    case=case)
                        r_path = self.render_file_string(
                                    data_type=_type,
                                    data_type_option='remote_path',
                                    case=case)
                        new_files.append({
                            'name': filename,
                            'remote_path': os.path.join(r_path, filename),
                            'local_path': os.path.join(local_path, filename),
                            'local_status': FileStatus.NOT_PRESENT.value,
                            'case': case,
                            'remote_status': FileStatus.NOT_PRESENT.value,
                            'year': 0,
                            'month': 0,
                            'datatype': _type,
                            'local_size': 0,
                            'transfer_type': self._config['simulations'][case]['transfer_type'],
                            'remote_uuid': self._config['simulations'][case].get('remote_uuid', ''),
                            'remote_hostname': self._config['simulations'][case].get('remote_hostname', '')
                        })
                    tail, _ = os.path.split(new_files[0]['local_path'])
                    if not os.path.exists(tail):
                        os.makedirs(tail)
                    step = 50
                    for idx in range(0, len(new_files), step):
                        DataFile.insert_many(
                            new_files[idx: idx + step]).execute()

            if self._mutex.locked():
                self._mutex.release()
            msg = 'Database update complete'
            print_line(
                line=msg,
                event_list=self._event_list)
    
    def terminate_transfers(self):
        self.kill_event.set()
        for thread in self.thread_list:
            msg = 'terminating {}, this may take a moment'.format(thread.name)
            print_line(msg, self._event_list)
            thread.join()

    def print_db(self):
        self._mutex.acquire()
        for df in DataFile.select():
            print {
                'case': df.case,
                'type': df.datatype,
                'name': df.name,
                'local_path': df.local_path,
                'remote_path': df.remote_path,
                'transfer_type': df.transfer_type,
            }
        self._mutex.release()
    
    def add_files(self, data_type, file_list):
        """
        Add files to the database
        
        Parameters:
            data_type (str): the data_type of the new files
            file_list (list): a list of dictionaries in the format
                local_path (str): path to the file,
                case (str): the case these files belong to
                name (str): the filename
                remote_path (str): the remote path of these files, optional
                transfer_type (str): the transfer type of these files, optional
                year (int): the year of the file, optional
                month (int): the month of the file, optional
                remote_uuid (str): remote globus endpoint id, optional
                remote_hostname (str): remote hostname for sftp transfer, optional
        """
        self._mutex.acquire()
        try:
            new_files = list()
            for file in file_list:
                new_files.append({
                    'name': file['name'],
                    'local_path': file['local_path'],
                    'local_status': file.get('local_status', FileStatus.NOT_PRESENT.value),
                    'datatype': data_type,
                    'case': file['case'],
                    'year': file.get('year', 0),
                    'month': file.get('month', 0),
                    'remote_uuid': file.get('remote_uuid', ''),
                    'remote_hostname': file.get('remote_hostname', ''),
                    'remote_path': file.get('remote_path', ''),
                    'remote_status': FileStatus.NOT_PRESENT.value,
                    'local_size': 0,
                    'transfer_type': file.get('transfer_type', 'local')
                })
            step = 50
            for idx in range(0, len(new_files), step):
                DataFile.insert_many(
                    new_files[idx: idx + step]).execute()
        finally:
            self._mutex.release()
        

    def update_local_status(self):
        """
        Update the database with the local status of the expected files

        Return True if there was new local data found, False othewise
        """
        self._mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                            (DataFile.local_status == FileStatus.NOT_PRESENT.value) |
                            (DataFile.local_status == FileStatus.IN_TRANSIT.value)))
            printed = False
            change = False
            for datafile in query.execute():
                marked = False
                if os.path.exists(datafile.local_path):
                    if datafile.local_status == FileStatus.NOT_PRESENT.value or datafile.local_status == FileStatus.IN_TRANSIT.value:
                        datafile.local_status = FileStatus.PRESENT.value
                        marked = True
                        change = True
                else:
                    if datafile.transfer_type == 'local':
                        msg = '{case} transfer_type is local, but {filename} is not present'.format(
                            case=datafile.case, filename=datafile.name)
                        logging.error(msg)
                        if not printed:
                            print_line(msg, self._event_list)
                            printed = True
                    if datafile.local_status == FileStatus.PRESENT.value:
                        datafile.local_status = FileStatus.NOT_PRESENT.value
                        marked = True
                if marked:
                    datafile.save()
        except OperationalError as operror:
            line = 'Error writing to database, database is locked by another process'
            print_line(
                line=line,
                event_list=self._event_list)
            logging.error(line)
        finally:
            if self._mutex.locked():
                self._mutex.release()
        return change

    def all_data_local(self):
        """
        Returns True if all data is local, False otherwise
        """
        self._mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                         (DataFile.local_status == FileStatus.NOT_PRESENT.value) |
                         (DataFile.local_status == FileStatus.IN_TRANSIT.value)))
            missing_data = query.execute()
            # if any of the data is missing, not all data is local
            if missing_data:
                logging.debug('All data is not local, missing the following')
                logging.debug([x.name for x in missing_data])
                return False
        except Exception as e:
            print_debug(e)
        finally:
            if self._mutex.locked():
                self._mutex.release()
        logging.debug('All data is local')
        return True

    def transfer_needed(self, event_list, event):
        """
        Start a transfer job for any files that arent local, but do exist remotely

        Globus user must already be logged in
        """

        # required files dont exist locally, do exist remotely
        # or if they do exist locally have a different local and remote size
        target_files = list()
        self._mutex.acquire()
        try:
            q = (DataFile
                 .select(DataFile.case)
                 .where(
                     DataFile.local_status == FileStatus.NOT_PRESENT.value))
            caselist = [x.case for x in q.execute()]
            if not caselist or len(caselist) == 0:
                return
            cases = list()
            for case in caselist:
                if case not in cases:
                    cases.append(case)

            for case in cases:
                q = (DataFile
                     .select()
                     .where(
                            (DataFile.case == case) &
                            (DataFile.local_status == FileStatus.NOT_PRESENT.value)))
                required_files = [x for x in q.execute()]
                for file in required_files:
                    if file.transfer_type == 'local':
                        required_files.remove(file)
                if not required_files:
                    msg = 'ERROR: all missing files are marked as local'
                    print_line(msg, self._event_list)
                    return
                # mark files as in-transit so we dont double-copy
                q = (DataFile
                     .update({DataFile.local_status: FileStatus.IN_TRANSIT})
                     .where(DataFile.name << [x.name for x in required_files]))
                q.execute()

                for file in required_files:
                    target_files.append({
                        'local_path': file.local_path,
                        'remote_path': file.remote_path,
                    })

                if required_files[0].transfer_type == 'globus':
                    msg = 'Starting globus file transfer of {} files'.format(
                        len(required_files))
                    print_line(msg, self._event_list)
                    msg = 'See https://www.globus.org/app/activity for transfer details'
                    print_line(msg, self._event_list)

                    client = get_client()
                    remote_uuid = required_files[0].remote_uuid
                    local_uuid = self._config['global']['local_globus_uuid']
                    thread_name = '{}_globus_transfer'.format(required_files[0].case)
                    _args = (client, remote_uuid,
                             local_uuid, target_files,
                             self.kill_event)
                    thread = Thread(
                        target=globus_transfer,
                        name=thread_name,
                        args=_args)
                    self.thread_list.append(thread)
                    thread.start()
                elif required_files[0].transfer_type == 'sftp':
                    msg = 'Starting sftp file transfer of {} files'.format(
                        len(required_files))
                    print_line(msg, self._event_list)

                    client = get_ssh_client(required_files[0].remote_hostname)
                    thread_name = '{}_sftp_transfer'.format(required_files[0].case)
                    _args = (target_files, client, self.kill_event)
                    thread = Thread(
                        target=self._ssh_transfer,
                        name=thread_name,
                        args=_args)
                    self.thread_list.append(thread)
                    thread.start()
        except Exception as e:
            print_debug(e)
            return False
        finally:
            if self._mutex.locked():
                self._mutex.release()

    def _ssh_transfer(self, target_files, client, event):
        sftp_client = client.open_sftp()
        for file in target_files:
            if event.is_set():
                return
            _, filename = os.path.split(file['local_path'])
            msg = 'sftp transfer from {} to {}'.format(
                file['remote_path'], file['local_path'])
            logging.info(msg)

            msg = 'starting sftp transfer for {}'.format(filename)
            print_line(msg, self._event_list)

            ssh_transfer(sftp_client, file)
            
            msg = 'sftp transfer complete for {}'.format(filename)
            print_line(msg, self._event_list)

            msg = self.report_files_local()
            print_line(msg, self._event_list)

    def years_ready(self, data_type, start_year, end_year):
        """
        Checks if data_type files exist from start year to end of endyear

        Parameters:
            start_year (int): the first year to start checking
            end_year (int): the last year to check for
        Returns:
            -1 if no data present
            0 if partial data present
            1 if all data present
        """
        data_ready = True
        non_zero_data = False

        self._mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                            (DataFile.datatype == data_type) &
                            (DataFile.year >= start_year) &
                            (DataFile.year <= end_year)))
            for datafile in query.execute():
                if datafile.local_status != FileStatus.NOT_PRESENT.value:
                    data_ready = False
                else:
                    non_zero_data = True
        except Exception as e:
            print_debug(e)
        finally:
            if self._mutex.locked():
                self._mutex.release()

        if data_ready:
            return 1
        elif not data_ready and non_zero_data:
            return 0
        elif not data_ready and not non_zero_data:
            return -1

    def report_files_local(self):
        """
        Return a string in the format 'X of Y files availabe locally' where X is the number here, and Y is the total
        """
        q = (DataFile
             .select(DataFile.local_status)
             .where(DataFile.local_status == FileStatus.PRESENT.value))
        local = len([x.local_status for x in q.execute()])

        q = (DataFile.select(DataFile.local_status))
        total = len([x.local_status for x in q.execute()])

        msg = '{local}/{total} files available locally or {prec:.2f}%'.format(
            local=local, total=total, prec=((local*1.0)/total)*100)
        return msg

    def get_file_paths_by_year(self, datatype, case, start_year=None, end_year=None):
        """
        Return paths to files that match the given type, start, and end year

        Parameters:
            datatype (str): the type of data
            case (str): the name of the case to return files for
            monthly (bool): is this datatype monthly frequency
            start_year (int): the first year to return data for
            end_year (int): the last year to return data for
        """
        self._mutex.acquire()
        try:
            if start_year and end_year:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.year <= end_year) &
                                (DataFile.year >= start_year) &
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype) &
                                (DataFile.local_status == FileStatus.PRESENT.value)))
            else:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype) &
                                (DataFile.local_status == FileStatus.PRESENT.value)))
            datafiles = query.execute()
            if datafiles is None or len(datafiles) == 0:
                return None
            return [x.local_path for x in datafiles]
        except Exception as e:
            print_debug(e)
        finally:
            if self._mutex.locked():
                self._mutex.release()

