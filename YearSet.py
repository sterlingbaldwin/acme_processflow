from enum import Enum


class SetStatus(Enum):
    NO_DATA = 0
    PARTIAL_DATA = 1
    DATA_READY = 2
    RUNNING = 3
    COMPLETED = 4
    FAILED = 5


class YearSet(object):

    def __init__(self, set_number, start_year, end_year):
        """
        Initialize member variables
        """
        self.status = SetStatus.NO_DATA
        self.jobs = []
        self.set_number = set_number
        self.set_start_year = start_year
        self.set_end_year = end_year

    def add_job(self, job):
        self.jobs.append(job)
    