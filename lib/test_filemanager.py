import os
import threading
import unittest
from filemanager import FileManager
from models import DataFile

class TestFileManagerSetup(unittest.TestCase):
    
    def test_filemanager_setup_no_sta(self):
        mutex = threading.Lock()
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        
        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertEqual(tail, 'run')
        os.remove(database)
    
    def test_filemanager_setup_with_sta(self):
        mutex = threading.Lock()
        sta = True
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        
        self.assertTrue(os.path.exists(database))
        head, tail = os.path.split(filemanager.remote_path)
        self.assertNotEqual(tail, 'run')
        os.remove(database)
    
    def test_filemanager_populate(self):
        mutex = threading.Lock()
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        
        simlength = simend - simstart + 1
        atm_file_names = [x.name for x in DataFile.select().where(DataFile.datatype == 'atm')] 
        self.assertTrue(len(atm_file_names) == (simlength * 12))

        for year in range(simstart, simend +1):
            for month in range(1, 13):
                name = '{exp}.cam.h0.{year:04d}-{month:02d}.nc'.format(
                    exp=experiment,
                    year=year,
                    month=month)
                self.assertTrue(name in atm_file_names)
    
    def test_filemanager_update_local(self):
        mutex = threading.Lock()
        sta = False
        types = ['atm', 'ice', 'ocn', 'rest', 'streams.cice', 'streams.ocean']
        database = 'test.db'
        remote_endpoint = 'b9d02196-6d04-11e5-ba46-22000b92c6ec'
        remote_path = '/global/homes/r/renata/ACME_simulations/20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison/'
        local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        local_path = os.path.abspath(os.path.join('..', '..', 'testproject'))
        simstart = 51
        simend = 60
        experiment = '20171011.beta2_FCT2-icedeep_branch.A_WCYCL1850S.ne30_oECv3_ICG.edison'
        filemanager = FileManager(
            mutex=mutex,
            sta=sta,
            types=types,
            database=database,
            remote_endpoint=remote_endpoint,
            remote_path=remote_path,
            local_endpoint=local_endpoint,
            local_path=local_path)
        filemanager.populate_file_list(
            simstart=simstart,
            simend=simend,
            experiment=experiment)
        mutex.acquire()
        df = DataFile.select().limit(1)
        name = df[0].name
        head, tail = os.path.split(df[0].local_path)
        if not os.path.exists(head):
            os.makedirs(head)
        with open(df[0].local_path, 'w') as fp:
            fp.write('this is a test file')
        mutex.release()
        filemanager.update_local_status()
        mutex.acquire()
        df = DataFile.select().where(DataFile.name == name)
        self.assertEqual(df[0].local_status, 0)
        self.assertTrue(df[0].local_size > 0)
        
        

if __name__ == '__main__':
    unittest.main()
