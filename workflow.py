#!/usr/bin/python
# pylint: disable=C0103
# pylint: disable=C0111
import argparse
from jobs.Diagnostic import Diagnostic
from jobs.Pipeline import Pipeline
from util import print_debug
from util import print_message

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--model-path', help='A path to the model output')
parser.add_argument('-c', '--climos', help='Compute climotologies from model output')
parser.add_argument('-d', '--diagnostic', help='Run ACME diagnostics', action='store_true')
parser.add_argument('-t', '--transfer', help='Transfer the diagnostics')
parser.add_argument('-p', '--publish', help='Publish the output')
parser.add_argument('-f', '--file', help='config file')

if __name__ == "__main__":
    args = parser.parse_args()
    print args
    if args.diagnostic:
        config = {
            '--model': args.model_path,
            '--obs': '/test',
        }
        d = Diagnostic(config)
        if d.status == 'valid':
            d.execute()
        else:
            print_message('Invalid diagnostic configuration: {}'.format(d))

