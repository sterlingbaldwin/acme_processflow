from enum import Enum
from pprint import pformat

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

        set_number, the identifier for this set
        start_year, the simulation year this set starts
        end_year, the simulation yaer this set ends
        """
        self.status = SetStatus.NO_DATA
        self.jobs = []
        self.set_number = set_number
        self.set_start_year = start_year
        self.set_end_year = end_year
        self.freq = self.set_end_year - self.set_start_year + 1

    def add_job(self, job):
        self.jobs.append(job)

    def __str__(self):
        return "status: {status}\nset_number: {num}\nstart_year: {start}\nend_year: {end}\njobs: {jobs}".format(
            status=self.status,
            jobs=pformat(self.jobs),
            num=self.set_number,
            start=self.set_start_year,
            end=self.set_end_year)
