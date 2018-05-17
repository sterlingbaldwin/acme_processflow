import os
import re
import sys
import threading
import logging
import random

from time import sleep
from peewee import *
from enum import IntEnum

from models import DataFile
from jobs.Transfer import Transfer
from jobs.JobStatus import JobStatus
from lib.YearSet import SetStatus
from lib.util import print_debug
from lib.util import print_line


class FileStatus(IntEnum):
    PRESENT = 0
    NOT_PRESENT = 1
    IN_TRANSIT = 2


class FileManager(object):
    """
    Manage all files required by jobs
    """

    def __init__(self, mutex, event_list, config, database='processflow.db', ui=False):
        """
        Parameters:
            mutex (theading.Lock) the mutext for accessing the database
            database (str): the path to where to create the sqlite database file
            config (dict): the global configuration dict
        """
        self._mutex = mutex
        self._event_list = event_list
        self._ui = ui
        self._active_transfers = 0
        self._db_path = database
        self._config = config

        if os.path.exists(database):
            os.remove(database)

        self.mutex.acquire()
        DataFile._meta.database.init(database)
        if DataFile.table_exists():
            DataFile.drop_table()

        DataFile.create_table() 
        if self.mutex.locked():
            self.mutex.release()

    def __str__(self):
        # TODO: make this better
        return str({
            'db_path': self.db_path
        })
    
    def write_database(self, output_path):
        """
        Write out a human readable version of the database for debug purposes
        """
        file_list_path = os.path.join(
            self._config['output_path'],
            'file_list.txt')
        with open(file_list_path, 'w') as fp:
            self._mutex.acquire()
            try:
                for model in self._config['models']:
                    if model == 'comparisons':
                        continue
                fp.write('+++++++++++++++++++++++++++++++++++++++++++++')
                fp.write('      {model}'.format(model=model))
                fp.write('+++++++++++++++++++++++++++++++++++++++++++++')
                q = (DataFile
                        .select(DataFile.datatype)
                        .where(DataFile.model == model)
                        .distinct())
                for _type in q.execute():
                    fp.write('===================================\n')
                    fp.write('\t' + _type + ':\n')
                    datafiles = DataFile.select().where(DataFile.datatype == _type)
                    for datafile in datafiles:

                        filestr = '------------------------------------------'
                        filestr += '\n\t     name: ' + datafile.name + '\n     local_status: '
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
                if mutex.locked():
                    mutex.release()

    def render_string(self, instring, **kwargs):
        """
        take a list of keyword arguments and replace uppercase instances of them in the input string
        """
        for string, val in kwargs.items():
            if string.uppper() in instring:
                instring = instring.replace(string.upper(), val)
        return instring

    def populate_file_list(self):
        """
        Populate the database with the required DataFile entries
        """
        import ipdb; ipdb.set_trace()
        msg = 'Creating file table'
        print_line(
            ui=False,
            line=msg,
            event_list=self.event_list)
        newfiles = list()
        with DataFile._meta.database.atomic():
            self.mutex.acquire()
            for model in self._config['models']:
                if model == 'comparisons':
                    continue
                for _type in self._config['file_types']:
                    if self._config['file_types'][_type].get(model):
                        source_path = self._config['file_types'][_type][model]['remote_path']
                    else:
                        source_path = self._config['file_types'][_type]['remote_path']
                    replace = {
                        'project_path': self._config['project_path'],
                        'source_path': remote_path,
                        'caseid': model,
                        'restart_year': int(self._config['models'][model]['start_year']) + 1
                    }
                    new_files = list()
                    if self._config['file_types'][_type].get('monthly'):
                        # TODO: handle monthly output
                        start_year = self._config['models'][model]['start_year']
                        end_year = self._config['models'][model]['end_year']
                        for year in range(start_year, end_year + 1):
                            for month in range(1, 13):
                                replace['year'] = '{:04d}'.format(year)
                                replace['month'] = '{:02d}'.format(month)
                            filename = self.render_string(
                                self._config['file_types'][_type]['file_format'],
                                **replace)
                            remote_path = self.render_string(
                                self._config['file_types'][_type][model]['remote_path'],
                                **replace)
                            filename = self.render_string(
                                self._config['file_types'][_type]['local_path'],
                                **replace)
                            new_files.append({
                                'filename': filename,
                                'remote_path': remote_path,
                                'local_path': local_path,
                                'local_status': FileStatus.NOT_PRESENT,
                                'model': model,
                                'remote_status': FileStatus.NOT_PRESENT,
                                'year': year,
                                'month': month,
                                'datatype': _type,
                                'remote_size': 0,
                                'local_size': 0
                            })
                    else:
                        filename = self.render_string(
                            self._config['file_types'][_type]['file_format'],
                            **replace)
                        remote_path = self.render_string(
                            self._config['file_types'][_type][model]['remote_path'],
                            **replace)
                        filename = self.render_string(
                            self._config['file_types'][_type]['local_path'],
                            **replace)
                        new_files.append({
                            'filename': filename,
                            'remote_path': remote_path,
                            'local_path': local_path,
                            'local_status': FileStatus.NOT_PRESENT,
                            'model': model,
                            'remote_status': FileStatus.NOT_PRESENT,
                            'year': 0,
                            'month': 0,
                            'datatype': _type,
                            'remote_size': 0,
                            'local_size': 0
                        })
                    DataFile.insert_many(new_files).execute()

            if self.mutex.locked():
                self.mutex.release()
            msg = 'Database update complete'
            print_line(
                ui=self.ui,
                line=msg,
                event_list=self.event_list)

    def print_db(self):
        self.mutex.acquire()
        for df in DataFile.select():
            print {
                'name': df.name,
                'local_path': df.local_path,
                'remote_path': df.remote_path
            }
        self.mutex.release()

    def update_remote_status(self, client):
        """
        Check remote location for existance of the files on our list
        If they exist, update their status in the DB

        Parameters:
            client (globus_sdk.client): the globus client to use for remote query
        """
        result = client.endpoint_autoactivate(
            self.remote_endpoint, if_expires_in=2880)
        if result['code'] == "AutoActivationFailed":
            return False

        # find the list of files that are still needed
        remote_directories = list()
        q = (DataFile
                .select()
                .where(
                    DataFile.remote_status == filestatus['NOT_EXIST']))
        data_files_needed = q.execute()
        file_names_needed = [x.name for x in data_files_needed]
        
        msg = '{} additional files needed'.format(
            len(file_names_needed))
        print_line(
            ui=self.ui,
            line=msg,
            event_list=self.event_list)
        # find all the unique directories that hold those files
        for remote_path in [x.remote_path for x in data_files_needed]:
            tail, head = os.path.split(remote_path)
            if tail not in remote_directories:
                remote_directories.append(tail)

        remote_files = list()
        for remote_directory in remote_directories:
            msg = 'Checking remote directory {}'.format(remote_directory)
            print_line(
                ui=self.ui,
                line=msg,
                event_list=self.event_list)
            res = self._get_ls(client, remote_directory)
            names = [x['name'] for x in res if x['name'] in file_names_needed]
            sizes = [x['size'] for x in res if x['name'] in file_names_needed]
            self.mutex.acquire()
            try:
                for name in names:
                    remote_path = os.path.join(remote_directory, name)
                    DataFile.update(
                        remote_status=filestatus['EXISTS'],
                        remote_size=sizes[names.index(name)],
                        remote_path=remote_path
                    ).where(
                        DataFile.name == name
                    ).execute()
            except Exception as e:
                print_debug(e)
                print "Do you have the correct start and end dates and experiment name?"
            except OperationalError as operror:
                line = 'Error writing to database, database is locked by another process'
                print_line(
                    ui=self.ui,
                    line=line,
                    event_list=self.event_list)
                logging.error(line)
            finally:
                if self.mutex.locked():
                    self.mutex.release()
        msg = 'remote update complete'
        print_line(
            ui=self.ui,
            line=msg,
            event_list=self.event_list)

    def _get_ls(self, client, path):
        for fail_count in xrange(10):
            try:
                res = get_ls(
                    client,
                    path,
                    self.remote_endpoint,
                    False, 0, False)
            except Exception as e:
                sleep(fail_count)
                if fail_count >= 9:
                    print_debug(e)
            else:
                return res

    def update_remote_rest_sta_path(self, client, pattern='mpaso.rst'):
        if not self.sta:
            return
        path = os.path.join(
            self.remote_path,
            'archive',
            'rest')
        res = self._get_ls(
            client=client,
            path=path)
        contents = res['DATA']
        subdir = contents[1]['name']
        path = os.path.join(path, subdir)
        contents = self._get_ls(
            client=client,
            path=path)
        remote_name = ''
        remote_path = ''
        size = 0
        for remote_file in contents:
            if re.search(pattern=pattern, string=remote_file['name']):
                remote_name = remote_file['name']
                remote_path = os.path.join(
                    self.remote_path,
                    'archive',
                    'rest',
                    subdir,
                    remote_file['name'])
                size = remote_file['size']
                break
        return remote_name, remote_path, size

    def update_local_status(self):
        """
        Update the database with the local status of the expected files

        Parameters:
            types (list(str)): the list of files types to expect, must be members of file_type_map
        """

        self.mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                            (DataFile.local_status == filestatus['NOT_EXIST']) |
                            (DataFile.local_status == filestatus['IN_TRANSIT'])))
            for datafile in query.execute():
                if os.path.exists(datafile.local_path):
                    local_size = os.path.getsize(datafile.local_path)
                    if local_size == datafile.remote_size and local_size != 0:
                        datafile.local_status = filestatus['EXISTS']
                        datafile.local_size = local_size
                        datafile.save()
        except OperationalError as operror:
            line = 'Error writing to database, database is locked by another process'
            print_line(
                ui=self.ui,
                line=line,
                event_list=self.event_list)
            logging.error(line)
        finally:
            if self.mutex.locked():
                self.mutex.release()

    def all_data_local(self):
        """
        Returns True if all data is local, False otherwise
        """
        self.mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                         (DataFile.local_status == filestatus['NOT_EXIST']) |
                         (DataFile.local_status == filestatus['IN_TRANSIT'])))
            missing_data = query.execute()
            # if any of the data is missing, not all data is local
            if missing_data:
                msg = 'All data is not local, missing the following'
                logging.debug(msg)
                logging.debug([x.name for x in missing_data])
                return False
        except Exception as e:
            print_debug(e)
        finally:
            if self.mutex.locked():
                self.mutex.release()
        msg = 'All data is local'
        logging.debug(msg)
        return True

    def all_data_remote(self):
        self.mutex.acquire()
        try:
            for data in DataFile.select():
                if data.remote_status != filestatus['EXISTS']:
                    return False
        except Exception as e:
            print_debug(e)
        finally:
            if self.mutex.locked():
                self.mutex.release()
        return True

    def transfer_needed(self, event_list, event, remote_endpoint, ui, display_event, emailaddr, thread_list):
        """
        Start a transfer job for any files that arent local, but do exist remotely

        Globus user must already be logged in

        Parameters:
            event_list (EventList): the list to push information into
            event (threadding.event): the thread event to trigger a cancel
        """
        if self.active_transfers >= 2:
            msg = 'Currently have {} transfers active, not starting any new ones'.format(
                self.active_transfers)
            logging.info(msg)
            return False
        # required files dont exist locally, do exist remotely
        # or if they do exist locally have a different local and remote size
        self.mutex.acquire()
        try:
            required_files = [x for x in DataFile.select().where(
                (DataFile.remote_status == filestatus['EXISTS']) &
                (DataFile.local_status != filestatus['IN_TRANSIT']) &
                ((DataFile.local_status == filestatus['NOT_EXIST']) |
                 (DataFile.local_size != DataFile.remote_size))
            ).execute()]
            if len(required_files) == 0:
                msg = 'No new files needed'
                logging.info(msg)
                return False
            target_files = []
            transfer_names = []
            target_size = 1e11  # 100 GB
            total_size = 0
            for file in required_files:
                if total_size + file.remote_size < target_size:
                    target_files.append({
                        'name': file.name,
                        'local_size': file.local_size,
                        'local_path': file.local_path,
                        'local_status': file.local_status,
                        'remote_size': file.remote_size,
                        'remote_path': file.remote_path,
                        'remote_status': file.remote_status
                    })
                    transfer_names.append(file.name)
                    total_size += file.remote_size
                else:
                    break
        except Exception as e:
            print_debug(e)
            return False
        finally:
            if self.mutex.locked():
                self.mutex.release()

        logging.info('Transfering required files')
        msg = 'total transfer size {size} gigabytes for {nfiles} files'.format(
            size=(total_size / 1e9),
            nfiles=len(target_files))
        print_line(
            ui=self.ui,
            line=msg,
            event_list=self.event_list)
        transfer_config = {
            'file_list': target_files,
            'source_endpoint': self.remote_endpoint,
            'destination_endpoint': self.local_endpoint,
            'source_path': self.remote_path,
            'destination_path': self.local_path,
            'source_email': emailaddr,
            'display_event': display_event,
            'ui': ui,
        }
        transfer = Transfer(
            config=transfer_config,
            event_list=event_list)
        self.mutex.acquire()
        try:
            step = 100
            for idx in range(0, len(transfer_names), step):
                DataFile.update(
                    local_status=filestatus['IN_TRANSIT']
                ).where(
                    DataFile.name << transfer_names[idx: idx + step]
                ).execute()
        except Exception as e:
            print_debug(e)
            return False
        except OperationalError as operror:
            line = 'Error writing to database, database is locked by another process'
            print_line(
                ui=self.ui,
                line=line,
                event_list=self.event_list)
            logging.error(line)
            return False
        finally:
            if self.mutex.locked():
                self.mutex.release()

        msg = 'Starting file transfer'
        print_line(
            ui=self.ui,
            line=msg,
            event_list=self.event_list)
        args = (transfer, event, event_list)
        thread = threading.Thread(
            target=self._handle_transfer,
            name='filemanager_transfer',
            args=args)
        thread_list.append(thread)
        thread.start()
        return True

    def _handle_transfer(self, transfer, event, event_list):
        # this is to stop the simultanious print issue
        sleep(random.uniform(0.01, 0.1))

        self.active_transfers += 1
        transfer.execute(event)
        self.active_transfers -= 1

        if transfer.status == JobStatus.FAILED:
            msg = "Transfer has failed"
            print_line(
                ui=self.ui,
                line=msg,
                event_list=self.event_list)
            logging.error(msg)
            return
        else:
            self.transfer_cleanup(transfer)

    def transfer_cleanup(self, transfer):
        try:
            self.mutex.acquire()
            names = [x['name'] for x in transfer.file_list]
            query = (DataFile
                     .select()
                     .where(DataFile.name << names))
            for datafile in query.execute():
                if os.path.exists(datafile.local_path) \
                        and os.path.getsize(datafile.local_path) == datafile.remote_size:
                    datafile.local_status = filestatus['EXISTS']
                    datafile.local_size = os.path.getsize(datafile.local_path)
                else:
                    msg = 'file transfer error on {}'.format(datafile.name)
                    print_line(
                        ui=self.ui,
                        line=msg,
                        event_list=self.event_list)
                    datafile.local_status = filestatus['NOT_EXIST']
                    datafile.local_size = 0
                datafile.save()
            total_files = DataFile.select().count()
            local_files = DataFile.select().where(
                DataFile.local_status == filestatus['EXISTS']
            ).count()
            msg = 'Transfer complete: {local}/{total} files local'.format(
                local=local_files,
                total=total_files)
            print_line(
                ui=self.ui,
                line=msg,
                event_list=self.event_list)
        except OperationalError as operror:
            line = 'Error writing to database, database is locked by another process'
            print_line(
                ui=self.ui,
                line=line,
                event_list=self.event_list)
            logging.error(line)
        if self.mutex.locked():
            self.mutex.release()

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

        self.mutex.acquire()
        try:
            query = (DataFile
                     .select()
                     .where(
                            (DataFile.datatype == 'atm') &
                            (DataFile.year >= start_year) &
                            (DataFile.year <= end_year)))
            for datafile in query.execute():
                if datafile.local_status != filestatus['EXISTS']:
                    data_ready = False
                else:
                    non_zero_data = True
        except Exception as e:
            print_debug(e)
        finally:
            if self.mutex.locked():
                self.mutex.release()

        if data_ready:
            return 1
        elif not data_ready and non_zero_data:
            return 0
        elif not data_ready and not non_zero_data:
            return -1

    def get_file_paths_by_year(self, start_year, end_year, _type):
        monthly = ['atm', 'ocn', 'ice']
        self.mutex.acquire()
        try:
            if _type not in monthly:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.datatype == _type) &
                                (DataFile.local_status == filestatus['EXISTS'])))
            else:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.datatype == _type) &
                                (DataFile.year >= start_year) &
                                (DataFile.year <= end_year) &
                                (DataFile.local_status == filestatus['EXISTS'])))
            datafiles = query.execute()
            if datafiles is None or len(datafiles) == 0:
                files = []
            else:
                files = [x.local_path for x in datafiles]
        except Exception as e:
            print_debug(e)
            files = []
        finally:
            if self.mutex.locked():
                self.mutex.release()
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
                        ui=self.ui,
                        line=msg,
                        event_list=self.event_list)
            elif data_ready == 0:
                if job_set.status != SetStatus.PARTIAL_DATA:
                    job_set.status = SetStatus.PARTIAL_DATA
                    msg = "{start:04d}-{end:04d} has partial data".format(
                        start=job_set.set_start_year,
                        end=job_set.set_end_year)
                    print_line(
                        ui=self.ui,
                        line=msg,
                        event_list=self.event_list)
            elif data_ready == -1:
                if job_set.status != SetStatus.NO_DATA:
                    job_set.status = SetStatus.NO_DATA
                    msg = "{start:04d}-{end:04d} has no data".format(
                        start=job_set.set_start_year,
                        end=job_set.set_end_year)
                    print_line(
                        ui=self.ui,
                        line=msg,
                        event_list=self.event_list)
