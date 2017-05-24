from enum import Enum
from pprint import pformat

class SetStatus(Enum):
    NO_DATA = 0
    PARTIAL_DATA = 1
    DATA_READY = 2
    RUNNING = 3
    COMPLETED = 4
    FAILED = 5
    IN_TRANSIT = 6


class YearSet(object):

    def __init__(self, set_number, start_year, end_year):
        """
        Initialize member variables

        Parameters:
            set_number (int): the identifier for this set
            set_start_year (int): the simulation year this set starts
            set_end_year (int): the simulation year this set ends
        """
        self._status = SetStatus.NO_DATA
        self._jobs = []
        self._set_number = set_number
        self._set_start_year = start_year
        self._set_end_year = end_year
        self._freq = self.set_end_year - self.set_start_year + 1

    def add_job(self, job):
        self._jobs.append(job)
    
    @property
    def length(self):
        return self._set_end_year - (self._set_start_year - 1)

    @property
    def jobs(self):
        return self._jobs
    
    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, status):
        self._status = status
    
    @property
    def set_number(self):
        return self._set_number
    
    @set_number.setter
    def set_number(self, num):
        self._set_number = num
    
    @property
    def set_start_year(self):
        return self._set_start_year
    
    @set_start_year.setter
    def set_start_year(self, num):
        self._set_start_year = num
    
    @property
    def set_end_year(self):
        return self._set_end_year
    
    @set_end_year.setter
    def set_end_year(self, num):
        self._set_end_year = num

    def __str__(self):
        return "status: {status}\nset_number: {num}\nstart_year: {start}\nend_year: {end}\njobs: {jobs}".format(
            status=self.status,
            jobs=pformat(self.jobs),
            num=self.set_number,
            start=self.set_start_year,
            end=self.set_end_year)
