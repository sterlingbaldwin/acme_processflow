import os
from time import sleep
from peewee import *
from models import DataFile
from globus_cli.commands.ls import _get_ls_res as get_ls

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

    def __init__(self, database, sta=False):
        self.sta = sta
        self.db = SqliteDatabase(database)
        self.db.connect()
        if DataFile.table_exists():
            DataFile.drop_table()
        DataFile.create_table()
    
    def populate_file_list(self, simstart, simend, types, localdir, remotedir, experiment):
        if self.sta:
            remotedir = os.path.join(remotedir, 'archive')
        else:
            remotedir = os.path.join(remotedir, 'run')

        newfiles = []
        with self.db.atomic():
            for _type in types:
                if _type not in file_type_map:
                    continue
                if _type == 'rest':
                    name = file_type_map[_type].replace('YEAR', '0002')
                    local_path = os.path.join(localdir, 'input', 'rest', name)
                    remote_path = os.path.join(remotedir, '0002-01-01-00000', name)
                    local_status = filestatus['EXISTS'] if os.path.exists(local_path) else filestatus['NOT_EXIST']
                    newfiles.append({
                        'name': name,
                        'local_path': local_path,
                        'local_status': local_status,
                        'remote_path': remote_path,
                        'remote_status': filestatus['NOT_EXIST']
                    })
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
                            local_path = os.path.join(localdir, _type, name)
                            if self.sta:
                                remote_path = os.path.join(remotedir, 'input', _type, name)
                            else:
                                remote_path = os.path.join(remotedir, 'input', name)
                            local_status = filestatus['EXISTS'] if os.path.exists(local_path) else filestatus['NOT_EXIST']
                            newfiles.append({
                                'name': name,
                                'local_path': local_path,
                                'local_status': local_status,
                                'remote_path': remote_path,
                                'remote_status': filestatus['NOT_EXIST']
                            })
            step = 100
            for idx in range(0, len(newfiles), step):
                DataFile.insert_many(newfiles[idx: idx + step]).execute()

    def update_remote_status(self, client, remote_endpoint, remote_path):
        if self.sta:
            remote_path = os.path.join(remote_path, 'archive')
        else:
            remote_path = os.path.join(remote_path, 'run')
        result = client.endpoint_autoactivate(remote_endpoint, if_expires_in=2880)
        if result['code'] == "AutoActivationFailed":
            return False
        for fail_count in xrange(10):
            try:
                res = get_ls(
                    client,
                    remote_path,
                    remote_endpoint,
                    False, 0, False)
            except:
                sleep(fail_count)
            else:
                break
        to_update = [x['name'] for x in res]
        for datafile in DataFile.select():
            if datafile.name in to_update and datafile.remote_status == filestatus['NOT_EXIST']:
                datafile.remote_status = filestatus['EXISTS']
                datafile.save()
    
    def update_local_status(self, localdir, types):
        for _type in types:
            type_path = os.path.join(localdir, _type)
            local_files = os.listdir(type_path)
            for datafile in DataFile.select():
                if datafile.name in local_files \
                   and datafile.local_status == filestatus['NOT_EXIST']:
                    datafile.local_status = filestatus['EXISTS']
                    datafile.save()

    def transfer_needed(self):
        required_files = [x for x in DataFile.select().where(
            (DataFile.local_status == filestatus['NOT_EXIST']) &
            (DataFile.remote_status == filestatus['EXISTS'])
        )]
        for f in required_files:
            print f.name
            