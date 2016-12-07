#!/usr/bin/python
# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import argparse
import json
import sys
from getpass import getpass
from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from Monitor import Monitor
from util import print_debug
from util import print_message

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Path to configuration file')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.config:
        with open(args.config, 'r') as conf:
            try:
                config = json.load(conf)
            except Exception as e:
                print_debug(e)
                print_message('Unable to read config file, is it properly formatted json?')
                sys.exit()
        required_fields = [
            "output_path",
            "data_cache_path",
            "compute_host",
            "compute_user",
            "compute_password",
            "processing_host",
            "processing_user",
            "processing_password",
            "globus_user",
            "gobus_password",
            "source_endpoint",
            "destination_endpoint",
            "source_path",
        ]
        for field in required_fields:
            if field not in config or len(config[field]) == 0:
                if 'password' in field:
                    config[field] = getpass("{0} not specified in config, please enter:".format(field))
                else:
                    config[field] = raw_input("{0} not specified in config, please enter:".format(field))
    else:
       print 'No configuration file given, initiating manual setup'
    



