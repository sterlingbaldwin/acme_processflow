from enum import IntEnum


class JobStatus(IntEnum):
    VALID = 0
    INVALID = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    SUBMITTED = 5
    PENDING = 6
    WAITING_ON_INPUT = 7
    CANCELLED = 8


StatusMap = {
    'VALID': JobStatus.VALID,
    'INVALID': JobStatus.INVALID,
    'RUNNING': JobStatus.RUNNING,
    'COMPLETED': JobStatus.COMPLETED,
    'FAILED': JobStatus.FAILED,
    'SUBMITTED': JobStatus.SUBMITTED,
    'PENDING': JobStatus.PENDING,
    'WAITING_ON_INPUT': JobStatus.WAITING_ON_INPUT,
    'CANCELLED': JobStatus.CANCELLED,
    'COMPLETING': JobStatus.COMPLETED
}

ReverseMap = {
    JobStatus.VALID: "valid",
    JobStatus.INVALID: "invalid",
    JobStatus.RUNNING: "running",
    JobStatus.COMPLETED: "completed",
    JobStatus.FAILED: "failed",
    JobStatus.SUBMITTED: "submitted",
    JobStatus.PENDING: "pending",
    JobStatus.WAITING_ON_INPUT: "waiting on additional input",
    JobStatus.CANCELLED: "canceled"
}
