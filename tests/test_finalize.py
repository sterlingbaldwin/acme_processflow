import os
import sys
import unittest
import threading

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.initialize import initialize
from lib.finalize import finalize
from lib.events import EventList


class TestFinalize(unittest.TestCase):
    """
    A test class for processflows finilization methods

    These tests should be run from the main project directory
    """

    def test_finilize_complete(self):
        pargv = ['-c', 'tests/test_configs/valid_config_simple.cfg']

        event_list = EventList()
        kill_event = threading.Event()
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version="0.0.0",
            branch="000",
            event_list=event_list,
            kill_event=kill_event,
            mutex=threading.Lock(),
            testing=True)
        self.assertNotEqual(config, False)
        self.assertNotEqual(filemanager, False)
        self.assertNotEqual(runmanager, False)
    
        # all jobs should be complete already
        # this will mark them as such
        runmanager.check_data_ready()
        runmanager.start_ready_jobs()
        runmanager.monitor_running_jobs()
        finalize(
            config=config,
            event_list=event_list,
            kill_event=kill_event,
            status=runmanager.is_all_done(),
            runmanager=runmanager)
        self.assertTrue(runmanager.is_all_done())
    
    def test_finilize_complete_marked_failed(self):
        pargv = ['-c', 'tests/test_configs/valid_config_simple.cfg']

        event_list = EventList()
        kill_event = threading.Event()
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version="0.0.0",
            branch="000",
            event_list=event_list,
            kill_event=kill_event,
            mutex=threading.Lock(),
            testing=True)
        self.assertNotEqual(config, False)
        self.assertNotEqual(filemanager, False)
        self.assertNotEqual(runmanager, False)
    
        finalize(
            config=config,
            event_list=event_list,
            kill_event=kill_event,
            status=-1,
            runmanager=runmanager)
        self.assertEqual(runmanager.is_all_done(), -1)

if __name__ == '__main__':
    unittest.main()