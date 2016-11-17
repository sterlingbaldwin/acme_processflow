#!/usr/bin/python
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--model-path', help='A path to the model output')
parser.add_argument('-c', '--climos', help='Compute climotologies from model output')
parser.add_argument('-d', '--diagnostic', help='Run ACME diagnostics')
parser.add_argument('-t', '--transfer', help='Transfer the diagnostics')
parser.add_argument('-p', '--publish', help='Publish the output')
parser.add_argument('-f', '--file', help='config file')

if __name__ == "__main__":
    arguments = parser.parse_args()
    print arguments