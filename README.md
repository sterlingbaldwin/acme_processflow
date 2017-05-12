compute system
A workflow tool for the ACME project

# Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)
3. [Configuration](#config)
4. [Quick Start](doc/quick_start_guide.md)
5. [Configuration Guide](doc/config_guide.md)
6. [Troubleshooting Guide](doc/troubleshooting_guide.md)



# Installation<a name="installation"></a>

If you don't have Anaconda installed, follow [this guide](doc/anaconda_install_guide.md).

If you're not running on acme1.llnl.gov or aims4.llnl.gov, use the [setup guide](doc/setup_guide.md) for more detailed instructions.
If you are running on acme1/aims4, follow [the quick start guide](doc/quick_start_guide.md)

If you're on a machine behind a firewall that wont allow ssh connections to git, use

    wget https://github.com/sterlingbaldwin/acme_workflow/archive/master.zip
    unzip master.zip
    mv acme_workflow-master acme_workflow

else clone like normal

    git clone https://github.com/sterlingbaldwin/acme_workflow.git

next, create a new conda environment with the required packages:

    cd acme_workflow
    conda create -n acme --file env.txt
    source activate acme

    pip install -r requirements.txt

Due to the coupled_diags, to get MPAS diagnostics you MUST have an ssh key on the server thats
associated with an authorized github account for their repo. You can find instructions on how to setup a github
ssh key [here](https://help.github.com/articles/connecting-to-github-with-ssh/)

To run AMWG, make sure NCL is installed, and the line `export NCARG_ROOT=/usr/local/src/NCL-6.3.0/`
has been added to your .bashrc

If this is a fresh install on a system that has not been configured to run this before, there
are some additional tools you'll need. These include the following:

* [NCL](https://www.ncl.ucar.edu/current_release.shtml) for plot generation
* [SLURM](https://slurm.schedmd.com/quickstart_admin.html) for job management
* [ImageMagick](https://www.vultr.com/docs/install-imagemagick-on-centos-6) for image conversion
* If you plan on using the transfer mechanism, the system will have to be attached to a [Globus DTN](https://docs.globus.org/globus-connect-server-installation-guide/#centos_fedora_red_hat_enterprise_linux_scientific_linux)


# Usage<a name="usage"></a>

        usage: workflow.py [-h] [-c CONFIG] [-v] [-d] [-n] [-r] [-l LOG] [-u] [-m]
                        [-V] [-s SIZE]

        optional arguments:
        -h, --help            show this help message and exit
        -c CONFIG, --config CONFIG
                                Path to configuration file.
        -v, --debug           Run in debug mode.
        -d, --daemon          Run in daemon mode.
        -n, --no-ui           Turn off the GUI.
        -r, --dry-run         Do all setup, but dont submit jobs.
        -l LOG, --log LOG     Path to logging output file.
        -u, --no-cleanup      Don't perform pre or post run cleanup. This will leave
                                all run scripts in place.
        -m, --no-monitor      Don't run the remote monitor or move any files over
                                globus.
        -V, --viewer          Turn on generation for output_viewer style web pages.
        -s SIZE, --size SIZE  The maximume size in gigabytes of a single transfer,
                                defaults to 100. Must be larger then the largest
                                single file.

*NOTE*
When running in GUI mode, resizing the window is discouraged. Although there is some checking
for window resizes, the likely outcome will be a hard crash. This is a known bug.  

### Common run commands

* Basic run after configuration
```python workflow.py -c run.cfg```

* Uninterupted run in headless mode that wont stop if you close the terminal, writing to a custom log location, with no cleanup after completion
```nohup workflow.py -c run.cfg --no-ui --log my_new_run.log --no-cleanup &```


# Configuration<a name="config"></a>

## Example
An example can be found here:  https://github.com/sterlingbaldwin/acme_workflow/blob/master/run.cfg

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
