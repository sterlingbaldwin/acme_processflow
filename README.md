# acme_workflow
A workflow tool for the ACME project

# Installation

    git clone https://github.com/sterlingbaldwin/acme_workflow.git
    cd acme_workflow
    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt


# Usage

usage: workflow.py [-h] [-m MODEL_PATH] [-c CLIMOS] [-d DIAGNOSTIC]
                   [-t TRANSFER] [-p PUBLISH] [-f FILE]

optional arguments:
  -h, --help            show this help message and exit
  -m MODEL_PATH, --model-path MODEL_PATH
                        A path to the model output
  -c CLIMOS, --climos CLIMOS
                        Compute climotologies from model output
  -d DIAGNOSTIC, --diagnostic DIAGNOSTIC
                        Run ACME diagnostics
  -t TRANSFER, --transfer TRANSFER
                        Transfer the diagnostics
  -p PUBLISH, --publish PUBLISH
                        Publish the output
  -f FILE, --file FILE  config file


# Supported job types

* ACME model (on the back burner)
* ACME diagnostics (in development)
* climotologies (on the back burner)
* globus transfer (on the back burner)
* ESGF publication(on the back burner)

# Extensibility

To add your own custom job type, create a new job in the jobs folder. Each job should have a validation method and an execution method. See existing job types for examples.