from enum import Enum

class JobStatus(Enum):
    VALID = 0
    INVALID = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    SUBMITTED = 5
    PENDING = 6
