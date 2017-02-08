compute system
A workflow tool for the ACME project

# Installation

** Note ** 
If you're going to be running this on a server, instead of uvcdat, use uvcdat-nox
        conda create -n <SOME ENVIRONMENT NAME> -c conda-forge -c uvcdat uvcdat-nox



    git clone https://github.com/sterlingbaldwin/acme_workflow.git
    conda create -n <SOME ENVIRONMENT NAME> -c conda-forge -c uvcdat uvcdat
    source activate <SOME ENVIRONMENT NAME>

    git clone https://github.com/globusonline/transfer-api-client-python.git
    cd transfer-api-client-python
    python setup.py install
    cd ..
    rm -rf transfer-api-client-python

    git clone https://github.com/UV-CDAT/cdms.git
    cd cdms
    git checkout Qi_xml_str_missing_issue
    python setup.py install
    cd ..
    rm -rf cdms

    pip -r requirements.txt

# Usage

    usage: workflow.py [-h] [-c CONFIG] [-d]

    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                        Path to configuration file
      -d, --debug           Run in debug mode

# Configuration
## Global
* output_path: A path to the output directory
* data_cache_path: an empty local directory for holding raw model output
* output_pattern: how output is formatted
* simulation_start_year: first year where simulation starts
* simulation_end_year: year where simulation ends
* set_frequency: a list of year lengths to run the processing on (i.e. [2, 5] will run every 2 years and every 5 years)
* experiment: caseID of the experiment
* batch_system_type: the type of batch system that this will be run under. Allowed values are 'slurm', 'pbs', or 'none'

## Monitor
* compute_host: name of hosting computer
* compute_username: username to get into compute system
* compute_password: password to get into compute system
* compute_keyfile: the path to the users ssh key for the compute enclave

## NCClimo
* regrid_map_path: the path to the regrid map used in generating climos, a copy should be included in the repo

## Meta Diags
* obs_for_diagnostics_path: the path to the observation set used for diagnostics

## Primary Diags
* mpas_meshfile:
* mpas_remapfile:
* pop_remapfile:
* remap_files_dir:
* GPCP_regrid_wgt_file:
* CERES_EBAF_regrid_wgt_file:
* ERS_regrid_wgt_file:
* test_native_res:
* yr_offset:
* ref_case:
* test_native_res:
* obs_ocndir:
* obs_seaicedir:
* obs_sstdir:
* obs_iceareaNH:
* obs_iceareaSH:
* obs_icevolNH:

## Upload Diagnostic
* diag_viewer_username: credentials for the diagnostic viewer
* diag_viewer_password: credentials for the diagnostic viewer
* diag_viewer_server: credentials for the diagnostic viewer

## Transfer
* source_path: the path to where on the compute machine the data will be located
* source_endpoint: Globus UUIDs for the source destination (post processing machine)
* processing_host: hostname of the machine doing the post processing (the machine running the workflow script)
* processing_username: credentials for that machine
* processing_password: credentials for that machine
* globus_username: Globus credentials
* globus_password: Globus credentials
* destination_endpoint: desired endpoint


## Example
An example can be found: [here](https://github.com/sterlingbaldwin/acme_workflow/blob/master/config.json)
