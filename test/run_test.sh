#!/bin/bash

echo "activating environment"
source activate /p/cscratch/acme/bin/acme

echo "removing cached ATM files"
rm /p/cscratch/acme/baldwin32/ci-bot-test/input/ATM/case_scripts.cam.h0.0005-12.nc

echo "starting tests"
nohup python /export/baldwin32/projects/acme_workflow/workflow.py -c /export/baldwin32/projects/acme_workflow/test_run.cfg --no-ui &