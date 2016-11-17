# acme_workflow
A workflow tool for the ACME project

# Installation

    git clone https://github.com/sterlingbaldwin/acme_workflow.git
    cd acme_workflow
    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt


# Usage

    usage: workflow.py [-h] [-m] [-c] [-d] [-t] [-p] [-cm] [-ud] [-f FILE]

    optional arguments:
      -h, --help            show this help message and exit
      -m, --model           Run the ACME model
      -c, --climos          Compute climotologies from model output
      -d, --diagnostic      Run ACME diagnostics
      -t, --transfer        Transfer the diagnostics
      -p, --publish         Publish the output
      -cm, --cmore          Run CMOR on the model output
      -ud, --upload-diagnostic
                            upload the diagnostic output to the diagnostic viewer
      -f FILE, --file FILE  config file


# Supported job types

* ACME model (on the back burner)
* ACME diagnostics (in development)
* climotologies (on the back burner)
* globus transfer (on the back burner)
* ESGF publication(on the back burner)

# Extensibility

To add your own custom job type, create a new job in the jobs folder. Each job should have a validation method and an execution method. See existing job types for examples.