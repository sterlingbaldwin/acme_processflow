from peewee import *
from jobs.JobStatus import JobStatus

class DataFile(Model):
    name = CharField()
    local_path = CharField()
    local_status = IntegerField()
    remote_path = CharField()
    remote_status = IntegerField()
    