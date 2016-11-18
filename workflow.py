#!/usr/bin/python
# pylint: disable=C0103
# pylint: disable=C0111
# pylint: disable=C0301
import argparse
import json
from jobs.Diagnostic import Diagnostic
from jobs.Transfer import Transfer
from jobs.Pipeline import Pipeline
from util import print_debug
from util import print_message

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--model', help='Run the ACME model', action='store_true')
parser.add_argument('-c', '--climos', help='Compute climotologies from model output', action='store_true')
parser.add_argument('-d', '--diagnostic', help='Run ACME diagnostics', action='store_true')
parser.add_argument('-t', '--transfer', help='Transfer the diagnostics', action='store_true')
parser.add_argument('-p', '--publish', help='Publish the output', action='store_true')
parser.add_argument('-cm', '--cmore', help='Run CMOR on the model output', action='store_true')
parser.add_argument('-ud', '--upload-diagnostic', help='upload the diagnostic output to the diagnostic viewer', action='store_true')
parser.add_argument('-f', '--file', help='config file')

if __name__ == "__main__":
    args = parser.parse_args()
    pipe = Pipeline()
    if args.file:
        with open(args.file, 'r') as conf:
            config = json.load(conf)
            pipe.set_conf_path(args.file)
    if args.diagnostic:
        diag_conf = config.get('diagnostic')
        d = Diagnostic(diag_conf)
        if d.status == 'valid':
            pipe.append(d)
        else:
            print_message('Invalid diagnostic configuration: {}'.format(d))
    if args.transfer:
        transfer_config = config.get('transfer')
        t = Transfer(transfer_config)
        print t
        if t.status == 'valid':
            pipe.append(t)
        else:
            print_message('Invalid transfer configuration: {}'.format(t))
    if pipe.validate() == -1:
        print_message('Invalid Pipeline')
    else:
        pipe.execute()

