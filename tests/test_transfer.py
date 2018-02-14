import unittest
import os, sys
import shutil
import threading
import logging
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.util import transfer_directory
from lib.events import EventList

from jobs.Transfer import Transfer
from jobs.JobStatus import JobStatus

project_path = os.path.abspath(os.path.join('..', 'testproject_transfer'))


class TestTransfer(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTransfer, self).__init__(*args, **kwargs)
        self.source_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        self.destination_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'

    # def test_transfer_directory(self):
    #     transfer_directory(
    #         source_endpoint='9d6d994a-6d04-11e5-ba46-22000b92c6ec',
    #         destination_endpoint='a871c6de-2acd-11e7-bc7c-22000b9a448b',
    #         src_path='/global/homes/s/sbaldwin/test_directory',
    #         dst_path=project_path,
    #         event_list=EventList(),
    #         event=threading.Event())

    #     self.assertTrue(os.path.exists(project_path))
    #     contents = os.listdir(project_path)
    #     for item in ['a', 'b', 'c', 'd']:
    #         self.assertTrue(item in contents)
    #     shutil.rmtree(project_path)

    def test_transfer_file(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        source_file = {
            'remote_path': '/global/homes/s/sbaldwin/test_directory/test_file.txt',
            'local_path': os.path.join(project_path, 'test_file.txt')}
        source_path = '/global/homes/s/sbaldwin/test_directory'
        transfer = Transfer({
            'file_list': [source_file],
            'recursive': False,
            'source_endpoint': self.source_endpoint,
            'destination_endpoint': self.destination_endpoint,
            'source_path': source_path,
            'destination_path': project_path,
            'source_email': 'baldwin32@llnl.gov',
            'display_event': threading.Event(),
            'ui': False
        }, event_list=EventList())
        transfer.execute(event=threading.Event())
        self.assertTrue(transfer.postvalidate())
        self.assertEqual(transfer.status.name, 'COMPLETED')


if __name__ == '__main__':
    if os.path.exists(project_path):
        shutil.rmtree(project_path)
    unittest.main()
    if os.path.exists(project_path):
        shutil.rmtree(project_path)
