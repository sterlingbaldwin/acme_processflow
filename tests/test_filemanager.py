import os, sys
import threading
import unittest
import shutil
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.filemanager import FileManager
from lib.models import DataFile
from lib.events import EventList
from globus_cli.services.transfer import get_client


class TestFileManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestFileManager, self).__init__(*args, **kwargs)
        self.mutex = threading.Lock()
        self.local_path = os.path.abspath(
            os.path.join('..', '..', 'testproject'))
        self.remote_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        self.remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        self.local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'

    def test_filemanager_setup_no_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        filemanager = FileManager(
            mutex=self.mutex,
            event_list=EventList(),
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)

        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertEqual(tail, 'run')
        os.remove(database)

    def test_filemanager_setup_with_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        filemanager = FileManager(
            mutex=self.mutex,
            event_list=EventList(),
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)

        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertNotEqual(tail, 'run')
        os.remove(database)

    def test_filemanager_populate(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            event_list=EventList(),
            mutex=self.mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)

        simlength = simend - simstart + 1
        atm_file_names = [x.name for x in DataFile.select().where(
            DataFile.datatype == 'atm')]
        self.assertTrue(len(atm_file_names) == (simlength * 12))

        for year in range(simstart, simend + 1):
            for month in range(1, 13):
                name = '{exp}.cam.h0.{year:04d}-{month:02d}.nc'.format(
                    exp=experiment,
                    year=year,
                    month=month)
                self.assertTrue(name in atm_file_names)

    def test_filemanager_update_local(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            event_list=EventList(),
            mutex=self.mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        self.mutex.acquire()
        df = DataFile.select().limit(1)
        name = df[0].name
        head, tail = os.path.split(df[0].local_path)
        if not os.path.exists(head):
            os.makedirs(head)
        with open(df[0].local_path, 'w') as fp:
            fp.write('this is a test file')
        if self.mutex.locked():
            self.mutex.release()
        filemanager.update_local_status()
        self.mutex.acquire()
        df = DataFile.select().where(DataFile.name == name)[0]
        self.assertEqual(df.local_status, 0)
        self.assertTrue(df.local_size > 0)

    def test_filemanager_update_remote_no_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean', 'mpascice.rst']
        database = 'test.db'
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            event_list=EventList(),
            mutex=self.mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        client = get_client()
        filemanager.update_remote_status(client)
        self.mutex.acquire()
        for datafile in DataFile.select():
            if datafile.remote_status != 0:
                print datafile.name, datafile.remote_path, datafile.remote_status, datafile.datatype
            self.assertEqual(datafile.remote_status, 0)
        if self.mutex.locked():
            self.mutex.release()
        self.assertTrue(filemanager.all_data_remote())

    def test_filemanager_update_remote_yes_sta(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = True
        types = ['atm', 'ice', 'ocn', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        simstart = 51
        source_path = '/global/cscratch1/sd/golaz/ACME_simulations/20170915.beta2.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        simend = 60
        experiment = '20170915.beta2.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            event_list=EventList(),
            mutex=self.mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=source_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        client = get_client()
        filemanager.update_remote_status(client)
        self.mutex.acquire()
        for datafile in DataFile.select():
            if datafile.remote_status != 0:
                print datafile.name, datafile.remote_path
            self.assertEqual(datafile.remote_status, 0)
        if self.mutex.locked():
            self.mutex.release()
        self.assertTrue(filemanager.all_data_remote())

    def test_filemanager_all_data_local(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])

        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        simstart = 51
        simend = 60
        event_list = EventList()
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        if os.path.exists(self.local_path):
            shutil.rmtree(self.local_path)
        filemanager = FileManager(
            event_list=EventList(),
            mutex=self.mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        filemanager.update_local_status()
        self.assertFalse(filemanager.all_data_local())

        self.mutex.acquire()
        for df in DataFile.select():
            name = df.name
            head, tail = os.path.split(df.local_path)
            if not os.path.exists(head):
                os.makedirs(head)
            with open(df.local_path, 'w') as fp:
                fp.write('this is a test file')
            size = os.path.getsize(df.local_path)
            df.remote_size = size
            df.local_size = size
            df.save()
        if self.mutex.locked():
            self.mutex.release()
        filemanager.update_local_status()
        self.assertTrue(filemanager.all_data_local())


if __name__ == '__main__':
    unittest.main()
    local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
