from peewee import *


class DataFile(Model):
    name = CharField()
    local_path = CharField()
    local_status = IntegerField()
    remote_path = CharField()
    remote_status = IntegerField()
    year = IntegerField()
    month = IntegerField()
    datatype = CharField()
    remote_size = IntegerField()
    local_size = IntegerField()
