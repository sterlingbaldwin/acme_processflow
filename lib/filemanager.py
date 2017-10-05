import os
import sys
import threading
import logging

from time import sleep
from peewee import *
from globus_cli.commands.ls import _get_ls_res as get_ls

from models import DataFile
from jobs.Transfer import Transfer
from jobs.JobStatus import JobStatus
from lib.YearSet import SetStatus

filestatus = {
    'EXISTS': 0,
    'NOT_EXIST': 1,
    'IN_TRANSIT': 2
}

file_type_map = {
    'atm': 'EXPERIMENT.cam.h0.YEAR-MONTH.nc',
    'ice': 'mpascice.hist.am.timeSeriesStatsMonthly.YEAR-MONTH-01.nc',
    'ocn': 'mpaso.hist.am.timeSeriesStatsMonthly.YEAR-MONTH-01.nc',
    'rest': 'mpaso.rst.YEAR-01-01_00000.nc'
}


class FileManager(object):

    def __init__(self, database, types, sta=False, **kwargs):
        self.sta = sta
        self.types = types
        self.active_transfers = 0
        self.db_path = database
        self.db = SqliteDatabase(database)
        self.db.connect()
        if DataFile.table_exists():
            DataFile.drop_table()
        DataFile.create_table()
        self.remote_endpoint = kwargs.get('remote_endpoint')
        self.local_path = kwargs.get('local_path')
        self.local_endpoint = kwargs.get('local_endpoint')
        self.remote_path = kwargs.get('remote_path')

    def __str__(self):
        return str({
            'short term archive': self.sta,
            'active_transfers': self.active_transfers,
            'remote_path': self.remote_path,
            'remote_endpoint': self.remote_endpoint,
            'local_path': self.local_path,
            'local_endpoint': self.local_endpoint,
            'db_path': self.db_path
        })

    def populate_file_list(self, simstart, simend, experiment):
        """
        Populate the database with the required DataFile entries
        
        Parameters:
            simstart (int): the start year of the simulation,
            simend (int): the end year of the simulation,
            types (list(str)): the list of file types to add, must be members of file_type_map,
            experiment (str): the name of the experiment ex: 20170915.beta2.A_WCYCL1850S.ne30_oECv3_ICG.edison
        """
        newfiles = []
        with self.db.atomic():
            for _type in self.types:
                if _type not in file_type_map:
                    continue
                if _type == 'rest':
                    name = file_type_map[_type].replace('YEAR', '0002')
                    local_path = os.path.join(self.local_path, 'input', 'rest', name)
                    if self.sta:
                        remote_path = os.path.join(self.remote_path, 'archive', 'rest', '0002-01-01-00000', name)
                    else:
                        remote_path = os.path.join(self.remote_path, name)
                    newfiles = self._add_file(
                        newfiles=newfiles,
                        name=name,
                        local_path=local_path,
                        remote_path=remote_path,
                        _type=_type)
                if _type == 'streams.ocean' or _type == 'streams.cice':
                    name = _type
                    local_path = local_path = os.path.join(self.local_path, 'input', 'streams', name)
                    remote_path = os.path.join(self.remote_path, 'run', name)
                    newfiles = self._add_file(
                        newfiles=newfiles,
                        name=name,
                        local_path=local_path,
                        remote_path=remote_path,
                        _type=_type)
                else:
                    for year in xrange(simstart, simend + 1):
                        for month in xrange(1, 13):
                            if _type == 'atm':
                                name = file_type_map[_type].replace('EXPERIMENT', experiment)
                            else:
                                name = file_type_map[_type]
                            yearstr = '{0:04d}'.format(year)
                            monthstr = '{0:02d}'.format(month)
                            name = name.replace('YEAR', yearstr)
                            name = name.replace('MONTH', monthstr)
                            local_path = os.path.join(self.local_path, _type, name)
                            if self.sta:
                                remote_path = os.path.join(self.remote_path, 'archive', _type, 'hist', name)
                            else:
                                remote_path = os.path.join(self.remote_path, name)
                            newfiles = self._add_file(
                                newfiles=newfiles,
                                name=name,
                                local_path=local_path,
                                remote_path=remote_path,
                                _type=_type,
                                year=year,
                                month=month)
            step = 50
            for idx in range(0, len(newfiles), step):
                DataFile.insert_many(newfiles[idx: idx + step]).execute()
    
    def _add_file(self, newfiles, **kwargs):
        newfiles.append({
            'name': kwargs['name'],
            'local_path': kwargs['local_path'],
            'local_status': filestatus['NOT_EXIST'],
            'remote_path': kwargs['remote_path'],
            'remote_status': filestatus['NOT_EXIST'],
            'year': kwargs.get('year', 0),
            'month': kwargs.get('month', 0),
            'datatype': kwargs['_type'],
            'local_size': 0,
            'remote_size': 0
        })
        return newfiles

    def update_remote_status(self, client):
        """
        Check remote location for existance of the files on our list
        If they exist, update their status in the DB

        Parameters:
            client (globus_sdk.client): the globus client to use for remote query
        """
        result = client.endpoint_autoactivate(self.remote_endpoint, if_expires_in=2880)
        if result['code'] == "AutoActivationFailed":
            return False
        if self.sta:
            for _type in self.types:
                if _type == 'rest':
                    remote_path = os.path.join(self.remote_path, 'archive', _type, '0002-01-01-00000')
                else:
                    remote_path = os.path.join(self.remote_path, 'archive', _type, 'hist')
                res = self._get_ls(
                    client=client,
                    path=remote_path)                
                to_update_name = [x['name'] for x in res]
                to_update_size = [x['size'] for x in res]
                for datafile in DataFile.select().where(DataFile.datatype == _type):
                    if datafile.name in to_update_name \
                    and datafile.remote_status == filestatus['NOT_EXIST']:
                        datafile.remote_status = filestatus['EXISTS']
                        datafile.remote_size = to_update_size[to_update_name.index(datafile.name)]
                        datafile.save()
        else:
            remote_path = os.path.join(self.remote_path, 'run')
            res = self._get_ls(
                client=self.client,
                path=remote_path)
            to_update_name = [x['name'] for x in res]
            to_update_size = [x['size'] for x in res]
            for datafile in DataFile.select():
                if datafile.name in to_update_name \
                and datafile.remote_status == filestatus['NOT_EXIST']:
                    datafile.remote_status = filestatus['EXISTS']
                    datafile.remote_size = to_update_size[to_update_name.index(datafile.name)]
                    datafile.save()

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
                    from lib.util import print_debug
                    print_debug(e)
                    sys.exit()
            else:
                return res

    def update_local_status(self):
        """
        Update the database with the local status of the expected files

        Parameters:
            types (list(str)): the list of files types to expect, must be members of file_type_map
        """
        for datafile in DataFile.select():
            if os.path.exists(datafile.local_path):
                if datafile.local_status == filestatus['NOT_EXIST']:
                    datafile.local_status = filestatus['EXISTS']
                    datafile.local_size = os.path.getsize(datafile.local_path)                
                    datafile.save()

    def all_data_local(self):
        for data in DataFile.select():
            if data.local_status != filestatus['EXISTS']:
                return False
        return True

    def transfer_needed(self, event_list, event, remote_endpoint, ui, display_event, emailaddr, thread_list):
        """
        Start a transfer job for any files that arent local, but do exist remotely

        Globus user must already be logged in

        Parameters:
            event_list (EventList): the list to push information into
            event (threadding.event): the thread event to trigger a cancel
        """

        # required files dont exist locally, do exist remotely
        # or if they do exist locally have a different local and remote size
        required_files = [x for x in DataFile.select().where(
            (DataFile.remote_status == filestatus['EXISTS']) &
            ((DataFile.local_status == filestatus['NOT_EXIST']) |
             (DataFile.local_size != DataFile.remote_size))
        ).limit(20)]
        logging.info('Transfering required files')
        for file in required_files:
            print file.name
            logging.info(file.name)
        transfer_config = {
            'file_list': required_files,
            'source_endpoint': self.remote_endpoint,
            'destination_endpoint': self.local_endpoint,
            'source_path': self.remote_path,
            'destination_path': self.local_path,
            'src_email': emailaddr,
            'display_event': display_event,
            'ui': ui,
        }
        transfer = Transfer(
            config=transfer_config,
            event_list=event_list)
        for file in transfer.file_list:
            DataFile.update(local_status=filestatus['IN_TRANSIT']).where(DataFile.name == file)
        args = (transfer, event, event_list)
        thread = threading.Thread(
            target=self._handle_transfer,
            name='filemanager_transfer',
            args=args)
        thread_list.append(thread)
        thread.start()

    def _handle_transfer(self, transfer, event, event_list):
        self.active_transfers += 1
        event_list.push(message='Starting file transfer')
        transfer.execute(event)
        self.active_transfers -= 1

        if transfer.status != JobStatus.COMPLETED:
            message = "Transfer {uuid} has failed".format(uuid=transfer.uuid)
            logging.error(message)
            event_list.push(message='Tranfer failed')
            return

        message = "Transfer has completed"
        logging.info(message)
        event_list.push(message=message)
        for datafile in DataFile.select().where(DataFile.name in transfer.file_list):
            datafile.local_status = filestatus['EXISTS']
            datafile.local_size = os.path.getsize(datafile.local_path)
            datafile.save()
        print '--- Transfer complete ---'

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

        datafiles = DataFile.select().where(
            (DataFile.datatype == 'atm') &
            (DataFile.year >= start_year) &
            (DataFile.year <= end_year))
        for datafile in datafiles:
            if datafile.local_status in [filestatus['NOT_EXIST'], filestatus['IN_TRANSIT']]:
                data_ready = False
            else:
                non_zero_data = True

        if data_ready:
            return 1
        elif not data_ready and non_zero_data:
            return 0
        elif not data_ready and not non_zero_data:
            return -1

    def get_file_paths_by_year(self, start_year, end_year, _type):
        datafiles = DataFile.select().where(
            (DataFile.type == _type) &
            (DataFile.year >= start_year) &
            (DataFile.year <= end_year))
        return [x.local_path for x in datafiels]

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
                job_set.status = SetStatus.DATA_READY
            elif data_ready == 0:
                job_set.status = SetStatus.PARTIAL_DATA
            elif data_ready == -1:
                job_set.status = SetStatus.NO_DATA
