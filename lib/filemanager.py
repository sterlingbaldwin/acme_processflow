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
from jobs.JobStatus import JobStatus
from lib.YearSet import SetStatus
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
                            filestr += '\n\t     remote_size: ' + \
                                str(datafile.remote_size)
                            filestr += '\n\t     remote_path: ' + datafile.remote_path + '\n'
                            fp.write(filestr)
            except Exception as e:
                print_debug(e)
            finally:
                if self._mutex.locked():
                    self._mutex.release()

    def render_string(self, instring, **kwargs):
        """
        take a list of keyword arguments and replace uppercase instances of them in the input string
        """
        for string, val in kwargs.items():
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
                if case in ['start_year', 'end_year', 'comparisons']: continue
                # for each data type
                for _type in self._config['file_types']:
                    # setup the replacement dict
                    replace = {
                        'PROJECT_PATH': self._config['global']['project_path'],
                        'REMOTE_PATH': self._config['simulations'][case].get('remote_path', 'data_local'),
                        'CASEID': case,
                        'REST_YR': '{:04d}'.format(start_year + 1),
                        'START_YR': '{:04d}'.format(start_year),
                        'END_YR': '{:04d}'.format(end_year)
                    }
                    # setup the base remote_path 
                    if self._config['file_types'][_type].get(case):
                        # if this case has a special remote_path
                        remote_path = self._config['file_types'][_type][case]['remote_path']
                    else:
                        # normal case
                        remote_path = self._config['file_types'][_type]['remote_path']
                    # setup the base local_path
                    if self._config['simulations'][case]['transfer_type'] == 'local':
                        # if the data is already locally staged
                        local_path = os.path.join(
                            self._config['simulations'][case]['local_path'],
                            _type)
                    else:
                        local_path = self.render_string(
                            self._config['file_types'][_type]['local_path'],
                            **replace)
                    new_files = list()
                    if self._config['file_types'][_type].get('monthly'):
                        # handle monthly data
                        for year in range(start_year, end_year + 1):
                            for month in range(1, 13):
                                replace['YEAR'] = '{:04d}'.format(year)
                                replace['MONTH'] = '{:02d}'.format(month)
                                filename = self.render_string(
                                    self._config['file_types'][_type]['file_format'],
                                    **replace)
                                r_path = self.render_string(
                                    remote_path,
                                    **replace)
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
                                    'remote_size': 0,
                                    'local_size': 0,
                                    'transfer_type': self._config['simulations'][case]['transfer_type'],
                                    'remote_uuid': self._config['simulations'][case].get('remote_uuid', ''),
                                    'remote_hostname': self._config['simulations'][case].get('remote_hostname', '')
                                })
                    else:
                        # handle one-off data
                        filename = self.render_string(
                            self._config['file_types'][_type]['file_format'],
                            **replace)
                        r_path = self.render_string(
                            remote_path,
                            **replace)
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
                            'remote_size': 0,
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
                        DataFile.insert_many(new_files[idx: idx + step]).execute()

            if self._mutex.locked():
                self._mutex.release()
            msg = 'Database update complete'
            print_line(
                line=msg,
                event_list=self._event_list)

    def print_db(self):
        self._mutex.acquire()
        for df in DataFile.select():
            print {
                'case': df.case,
                'type': df.datatype,
                'name': df.name,
                'local_path': df.local_path,
                'remote_path': df.remote_path,
                'transfer_tyoe': df.transfer_type,
            }
        self._mutex.release()

    def update_local_status(self):
        """
        Update the database with the local status of the expected files
        """
        self._mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                            (DataFile.local_status == FileStatus.NOT_PRESENT.value) |
                            (DataFile.local_status == FileStatus.IN_TRANSIT.value)))
            for datafile in query.execute():
                marked = False
                if os.path.exists(datafile.local_path):
                    if datafile.transfer_type == 'local':
                        if datafile.local_status == FileStatus.NOT_PRESENT.value:
                            datafile.local_status = FileStatus.PRESENT.value
                            marked = True
                    else:
                        local_size = os.path.getsize(datafile.local_path)
                        if local_size == datafile.remote_size and local_size != 0:
                            if datafile.local_status == FileStatus.NOT_PRESENT.value:
                                datafile.local_status = FileStatus.PRESENT.value
                                datafile.local_size = local_size
                                marked = True
                else:
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
        thread_list = list()
        self._mutex.acquire()
        try:
            q = (DataFile
                    .select(DataFile.case)
                    .where(
                        (DataFile.local_status == FileStatus.NOT_PRESENT.value) |
                        (DataFile.local_size != DataFile.remote_size)))
            cases = [x.case for x in q.execute()]
            for case in cases:
                q = (DataFile
                        .select()
                        .where(
                            (DataFile.case == case) &
                            ((DataFile.local_status == FileStatus.NOT_PRESENT.value) |
                             (DataFile.local_size != DataFile.remote_size))))
                required_files = [x for x in q.execute()]
                if len(required_files) == 0:
                    msg = 'No new files needed for case: {}'.format(case)
                    logging.info(msg)
                    continue

                for file in required_files:
                    target_files.append({
                        'local_path': file.local_path,
                        'remote_path': file.remote_path,
                    })
                if required_files[0]['transfer_type'] == 'globus':
                    client = globus_sdk.get_client()
                    remote_uuid = required_files[0]['remote_uuid']
                    local_uuid = required_files[0]['local_uuid']
                    _args = (client, remote_uuid, local_uuid, target_files, event)
                    thread = Threading.Thread(
                        target=globus_transfer,
                        name='filenamager_globus_transfer',
                        args=_args)
                    thread_list.append(thread)
                    thread.start()
                elif required_files[0]['transfer_type'] == 'sftp':
                    client = get_ssh_client(required_files[0]['hostname'])
                    _args = (target_files, client, event)
                    thread = Threading.Thread(
                        target=self._ssh_transfer,
                        name='filenamager_ssh_transfer',
                        args=_args)
                    thread_list.append(thread)
                    thread.start()
        except Exception as e:
            print_debug(e)
            return False
        finally:
            if self._mutex.locked():
                self._mutex.release()


    def _ssh_transfer(self, target_files, client, event):
        sftp_client = client.get_ssh_client()
        for file in target_files:
            if event.is_set():
                return
            msg = 'sftp transfer from {} to {}'.format(file['remote_path'], file['local_path'])
            logging.info(msg)
            ssh_transfer(sftp_client, file)

    def years_ready(self, start_year, end_year):
        """
        Checks if atm files exist from start year to end of endyear

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
                            (DataFile.datatype == 'atm') &
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

    def get_file_paths_by_year(self, start_year, end_year, _type):
        """
        Return paths to files that match the given type, start, and end year
        
        Parameters:
            start_year (int): the first year to return data for
            end_year (int): the last year to return data for
            _type (str): the type of data
        """
        monthly = ['atm', 'ocn', 'ice']
        self._mutex.acquire()
        try:
            if _type not in monthly:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.datatype == _type) &
                                (DataFile.local_status == FileStatus.PRESENT.value)))
            else:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.datatype == _type) &
                                (DataFile.year >= start_year) &
                                (DataFile.year <= end_year) &
                                (DataFile.local_status == FileStatus.PRESENT.value)))
            datafiles = query.execute()
            if datafiles is None or len(datafiles) == 0:
                files = []
            else:
                files = [x.local_path for x in datafiles]
        except Exception as e:
            print_debug(e)
            files = []
        finally:
            if self._mutex.locked():
                self._mutex.release()
        return files

    def check_year_sets(self, job_sets):
        """
        Checks the file_list, and sets the year_set status to ready if all the files are in place,
        otherwise, checks if there is partial data, or zero data
        """
        incomplete_job_sets = [s for s in job_sets
                               if s.status != SetStatus.COMPLETED
                               and s.status != SetStatus.RUNNING
                               and s.status != SetStatus.FAILED]

        for job_set in incomplete_job_sets:
            data_ready = self.years_ready(
                start_year=job_set.set_start_year,
                end_year=job_set.set_end_year)

            if data_ready == 1:
                if job_set.status != SetStatus.DATA_READY:
                    job_set.status = SetStatus.DATA_READY
                    msg = "{start:04d}-{end:04d} is ready".format(
                        start=job_set.set_start_year,
                        end=job_set.set_end_year)
                    print_line(
                        line=msg,
                        event_list=self._event_list)
            elif data_ready == 0:
                if job_set.status != SetStatus.PARTIAL_DATA:
                    job_set.status = SetStatus.PARTIAL_DATA
                    msg = "{start:04d}-{end:04d} has partial data".format(
                        start=job_set.set_start_year,
                        end=job_set.set_end_year)
                    print_line(
                        line=msg,
                        event_list=self._event_list)
            elif data_ready == -1:
                if job_set.status != SetStatus.NO_DATA:
                    job_set.status = SetStatus.NO_DATA
                    msg = "{start:04d}-{end:04d} has no data".format(
                        start=job_set.set_start_year,
                        end=job_set.set_end_year)
                    print_line(
                        line=msg,
                        event_list=self._event_list)
