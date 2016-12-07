#!/usr/bin/python
# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import argparse
import json
import sys
import os
from shutil import copy, rmtree
from getpass import getpass
from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from Monitor import Monitor
from util import print_debug
from util import print_message

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')

def setup(parser):
    args = parser.parse_args()
    required_fields = [
        "output_path",
        "data_cache_path",
        "compute_host",
        "compute_username",
        "compute_password",
        "processing_host",
        "processing_username",
        "processing_password",
        "globus_username",
        "globus_password",
        "source_endpoint",
        "destination_endpoint",
        "source_path",
    ]
    if args.config:
        with open(args.config, 'r') as conf:
            try:
                config = json.load(conf)
            except Exception as e:
                print_debug(e)
                print_message('Unable to read config file, is it properly formatted json?')
                return -1

        for field in required_fields:
            if field not in config or len(config[field]) == 0:
                if 'password' in field:
                    config[field] = getpass("{0} not specified in config, please enter: ".format(field))
                else:
                    config[field] = raw_input("{0} not specified in config, please enter: ".format(field))
    else:
        print 'No configuration file given, initiating manual setup'
        config = {}
        for field in required_fields:
            if 'password' in field:
                config[field] = getpass('{0}: '.format(field))
            else:
                config[field] = raw_input('{0}: '.format(field))
    return config

if __name__ == "__main__":

    config = setup(parser)
    if config == -1:
        print "Error in setup, exiting"
        sys.exit()
    monitor = Monitor({
        'remote_host': config.get('compute_host'),
        'remote_dir': config.get('source_path'),
        'username': config.get('compute_username'),
        'password': config.get('compute_password'),
        'pattern': config.get('output_pattern')
    })
    print_message('attempting connection', 'ok')
    if monitor.connect() == 0:
        print_message('connected', 'ok')
    else:
        print_message('unable to connect')
    monitor.check()
    print_message('Found new files: {}, setting up transfer'.format(monitor.get_new_files()), 'ok')
    file_list = ['{path}/{file}'.format(path=config.get('source_path'), file=f)  for f in monitor.get_new_files()]
    tmpdir = os.getcwd() + '/tmp/'
    t = Transfer(config={
        'file_list': file_list,
        'globus_username': config.get('globus_username'),
        'globus_password': config.get('globus_password'),
        'source_username': config.get('compute_username'),
        'source_password': config.get('compute_password'),
        'destination_username': config.get('processing_username'),
        'destination_password': config.get('processing_password'),
        'source_endpoint': config.get('source_endpoint'),
        'destination_endpoint': config.get('destination_endpoint'),
        'source_path': config.get('source_path'),
        'destination_path': tmpdir,
        'recursive': 'False'
    })
    t.execute()
    for file in monitor.get_new_files():
        copy(
            src='{tmp}/{file}'.format(tmp=tmpdir, file=file),
            dst=config.get('data_cache_path') + '/'
        )
    rmtree(tmpdir)



