import os, sys
import threading
import unittest
import shutil
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.filemanager import FileManager, file_type_map
from lib.models import DataFile
from lib.events import EventList
from lib.util import print_message
from globus_cli.services.transfer import get_client


class TestFileManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestFileManager, self).__init__(*args, **kwargs)
        self.file_types = ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport', 'lnd']
        self.local_path = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete'
        self.remote_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        self.remote_path = '/global/cscratch1/sd/golaz/ACME_simulations/20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        self.local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        self.experiment = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'

    def test_filemanager_setup_no_sta(self):
        """
        run filemansger setup with no sta
        """

        """ 
        ##############  SETUP   ################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = False
        database = '{}.db'.format(inspect.stack()[0][3])
        remote_path = '/global/homes/r/renata/ACME_simulations/20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'
        mutex = threading.Lock()
        experiment = '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'

        """ 
        ###############  TEST   ################
        """
        filemanager = FileManager(
            mutex=mutex,
            event_list=EventList(),
            sta=sta,
            types=self.file_types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=experiment)

        self.assertTrue(isinstance(filemanager, FileManager))
        self.assertTrue(os.path.exists(database))
        """ 
        ##############  CLEANUP  ###############
        """
        os.remove(database)


    def test_filemanager_setup_with_sta(self):
        """
        run the filemanager setup with sta turned on
        """

        """ 
        ##############  SETUP  ###############
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport', 'lnd']
        database = '{}.db'.format(inspect.stack()[0][3])
        mutex = threading.Lock()

        """ 
        ##############  TEST  ###############
        """
        filemanager = FileManager(
            mutex=mutex,
            event_list=EventList(),
            sta=sta,
            types=self.file_types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=self.experiment)

        self.assertTrue(isinstance(filemanager, FileManager))
        self.assertTrue(os.path.exists(database))
        """ 
        ##############  CLEANUP  ###############
        """
        os.remove(database)

    def test_filemanager_populate_no_sta(self):
        """
        run filemanager set and populate with sta turned off
        """

        """ 
        ###############  SETUP   ################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = False
        database = '{}.db'.format(inspect.stack()[0][3])
        simstart = 1
        simend = 10
        experiment = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        mutex = threading.Lock()
        """ 
        ##############    TEST    ###############
        """
        filemanager = FileManager(
            event_list=EventList(),
            mutex=mutex,
            sta=sta,
            types=self.file_types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=self.remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=self.experiment)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)

        filemanager.mutex.acquire()
        simlength = simend - simstart + 1
        
        for _type in ['atm', 'lnd', 'ocn', 'ice']:
            file_names = [x.name for x in DataFile.select().where(DataFile.datatype == _type)]
            if not len(file_names) == (simlength * 12):
                print _type + ' does not have ' + str(simlength * 12) + ' files'
            self.assertEqual(len(file_names), (simlength * 12))
    
            for year in range(simstart, simend + 1):
                for month in range(1, 13):
                    name = (file_type_map[_type]
                                .replace('EXPERIMENT', experiment)
                                .replace('YEAR', '{:04d}'.format(year))
                                .replace('MONTH', '{:02}'.format(month)))
                    self.assertTrue(name in file_names)
        filemanager.mutex.release()
        """ 
        ##############  CLEANUP  ###############
        """
        os.remove(database)

    def test_filemanager_update_local(self):
        """
        run filemanager set and populate, then create a dummy file in the 
        input directory and run update_local which should mark it as present
        """

        """ 
        #############   SETUP   ################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = '{}.db'.format(inspect.stack()[0][3])
        simstart = 51
        simend = 60
        remote_path = '/global/homes/r/renata/ACME_simulations/20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'
        experiment = '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'
        mutex = threading.Lock()
        """ 
        ###############  TEST   #################
        """
        filemanager = FileManager(
            event_list=EventList(),
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=experiment)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)

        filemanager.mutex.acquire()
        df = DataFile.select().limit(1)
        filemanager.mutex.release()

        name = df[0].name
        head, tail = os.path.split(df[0].local_path)
        if not os.path.exists(head):
            os.makedirs(head)
        dummy_file_path = df[0].local_path
        print '----- writing out dummy file at {} -----'.format(dummy_file_path)
        with open(dummy_file_path, 'w') as fp:
            fp.write('this is a test file')

        filemanager.update_local_status()
        filemanager.mutex.acquire()
        df = DataFile.select().where(DataFile.name == name)[0]
        filemanager.mutex.release()
        self.assertEqual(df.local_status, 0)
        self.assertTrue(df.local_size > 0)
        """ 
        ###############  CLEANUP   #################
        """
        os.remove(database)

    def test_filemanager_update_remote_no_sta(self):
        """
        run filemanager setup and populate, then run update_remote_status 
        with 10 years of atm output, and finally run all_data_remote to show that
        all the remote data has been recognized
        """

        """ 
        #############   SETUP   ##################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        remote_path = '/global/homes/r/renata/ACME_simulations/20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'
        sta = False
        types = ['atm']
        database = '{}.db'.format(inspect.stack()[0][3])
        simstart = 51
        simend = 60
        experiment = '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'
        mutex = threading.Lock()
        """ 
        ################  TEST  ##################
        """
        filemanager = FileManager(
            event_list=EventList(),
            mutex=mutex,
            sta=False,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=remote_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=experiment)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        
        client = get_client()
        filemanager.update_remote_status(client)

        filemanager.mutex.acquire()
        for datafile in DataFile.select():
            if datafile.remote_status != 0:
                print datafile.name, datafile.remote_path, datafile.remote_status, datafile.datatype
            self.assertEqual(datafile.remote_status, 0)
        if filemanager.mutex.locked():
            filemanager.mutex.release()
        self.assertTrue(filemanager.all_data_remote())
        """ 
        ##############  CLEANUP  ###############
        """
        os.remove(database)

    def test_filemanager_update_remote_yes_sta(self):
        """
        run filemanager setup and populate, then run update_remote_status on a directory
        that has been short term archived
        """

        """ 
        ############### SETUP  #################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport']
        database = '{}.db'.format(inspect.stack()[0][3])
        simstart = 51
        source_path = '/global/cscratch1/sd/golaz/ACME_simulations/20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        simend = 60
        experiment = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        mutex = threading.Lock()

        """ 
        ###############   TEST  #################
        """
        filemanager = FileManager(
            event_list=EventList(),
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=source_path,
            local_endpoint=self.local_endpoint,
            local_path=self.local_path,
            experiment=self.experiment)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        client = get_client()
        filemanager.update_remote_status(client)
        filemanager.mutex.acquire()
        for datafile in DataFile.select():
            if datafile.remote_status != 0:
                print datafile.name, datafile.remote_path
            self.assertEqual(datafile.remote_status, 0)
        if filemanager.mutex.locked():
            filemanager.mutex.release()
        self.assertTrue(filemanager.all_data_remote())
        """ 
        ##############  CLEANUP  ###############
        """
        os.remove(database)

    def test_filemanager_all_data_local(self):
        """
        Create a dummy project and populate it with empty files 
        to test that filemanager.all_data_local works correctly"""


        """ 
        ############### SETUP ##################
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        sta = True
        database = '{}.db'.format(inspect.stack()[0][3])
        simstart = 1
        simend = 10
        event_list = EventList()
        remote_path = '/dummy/remote/20180215.DECKv1b_1pctCO2.ne30_oEC.edison/run/something'
        local_path = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/dummyproject'
        experiment = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        types = ['atm', 'ocn', 'lnd', 'ice']
        mutex = threading.Lock()
        if os.path.exists(local_path):
            shutil.rmtree(local_path)
        """ 
        ############### TEST ##################
        """
        filemanager = FileManager(
            event_list=EventList(),
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=self.remote_endpoint,
            remote_path=remote_path,
            local_endpoint=self.local_endpoint,
            local_path=local_path,
            experiment=self.experiment)
        self.assertEqual(filemanager.remote_path, '/dummy/remote/20180215.DECKv1b_1pctCO2.ne30_oEC.edison')
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        filemanager.update_local_status()
        self.assertFalse(filemanager.all_data_local())

        filemanager.mutex.acquire()
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
        if filemanager.mutex.locked():
            filemanager.mutex.release()
        filemanager.update_local_status()
        self.assertTrue(filemanager.all_data_local())
        """ 
        #########################################
        """
        os.remove(database)

if __name__ == '__main__':
    unittest.main()
