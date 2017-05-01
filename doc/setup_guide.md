# Automated Post Processing Quick Start



This utility has been designed with the goal of being as simple to use as possible, but there are a number of configuration options that must be set before the first run. You will need to collect a few nessesary pieces of information. This document will assume you're running on the acme1.llnl.gov server, but the only thing that would be different if running elsewhere would be paths.

All this information will need to be written to the run configuration file run.cfg.


## Setup

### First time setup keys

The keys you need to change before running the first time are:
```
[global]
output_path = /p/cscratch/acme/<YOUR_USERNAME>/output
data_cache_path = /p/cscratch/acme/<YOUR_USERNAME>/input
simulation_end_year = SOMENUMBER
set_frequency = [LIST, OF, NUMBERS]
run_id = YOUR_RUN_ID

[monitor]
compute_username = YOUR_EDISON_USERNAME
compute_password = YOUR_EDISON_PASSWORD (optional)

[transfer]
source_path
processing_username = YOU_ACME1_USERNAME
processing_password = YOUR_ACME1_PASSWORD (optional)
globus_username = YOUR_GLOBUS_USERNAME
globus_password = YOUR_GLOBUS_PASSWORD (optional)
```

Once these are set, only the data_cache_path, output_path, and run_id need to be changed for each subsequent run.

For each run, the contents of output_path will be overwritten.

### Running

* Once the setup process is done, running is simple. Simply activate your conda environment, and run the following command to start the post processor in interactive mode, which will start a new run, and start downloading the data.

In interactive mode, if the terminal is closed or you log out, it will stop the process (but the runs managed by SLURM will continue). See below for headless mode instructions.

    python workflow.py -c run.cfg

![initial run](http://imgur.com/ZGuJUCk)

Once globus has transfered the first year_set of data, it will start running the post processing jobs.

![run in progress](http://imgur.com/URU4OVY)


### headless mode
Uninterupted run in headless mode that wont stop if you close the terminal, writing to a custom log location, with no cleanup after completion
```
nohup python workflow.py -c run.cfg --no-ui --log my_new_run.log --no-cleanup &
```

This run can continue after you close the termincal and log off the computer. While running in headless mode, you can check run_state.txt for the run status.

```
less run_state.txt
```

![run_state](http://imgur.com/zS8f57g)

#### The model run directory on the compute machine.

This is the output directory for the model on the HPC facility. An example would be the 20170313.beta1_2 run by Chris Golaz on Edison:

    /scratch2/scratchdirs/golaz/ACME_simulations/20170313.beta1_02.A_WCYCL1850S.ne30_oECv3_ICG.edison/run

This path should be written to the run.cfg under
* [transfer] source_path = /remote/model/path/run

#### Your desired output path.

This is the directory that all post processing will be placed in. An example would be:

    /p/cscratch/acme/[USERNAME]/output_20170313

This should be written to
* [global] output_path = /your/output/path

#### The local model storage path.

This is the local location that the model data will be stored. An example:

    /p/cscratch/acme/[USERNAME]/input_20170313


This should be written to
* [global] input_path = /your/input/path

#### The length of the simulation.

This is the end year of the simulation. This doesnt have to match the actual model run, its for our purposes only. If the simulation ran for 100 years, but you only want to run against the first 50 years, thats fine. Conversely, if the simulation is currently running and is only 5 year in, you can set the end year to be 100 years even if it doesnt produce 100 years of output. The simulation start year is assumed to be 1, but you can set it to anything you like.

This should be written to
* [global] simulation_end_year = SOMENUMBER

#### The run frequency

This is the length of each set of diagnostic runs. If you set it to 10 years, then for every 10 years the climatologies, time series, and diagnostics will be produced. This value is a list of year lengths, so you could set it to [10, 50], which would cause output to be generated for every ten year span as well as every 50 year span.

This should be written to
* [global] set_frequency = [LIST, OF, NUMBERS]

#### The run id.

This is a unique name for this run of the automated post processor. This is needed to differentiate the paths to the diagnostic output. If you were using the example model, an appropriate run_id would be 20170313.beta1_02

This should be written to
* [global] run_id = YOUR_RUN_ID

#### compute_username and compute_password

Although its not required to add your password to the config file, it makes the run process faster.This should be written to:

* [monitor] compute_username = YOUR_EDISON_USERNAME and [monitor] compute_password = YOUR_EDISON_PASSWORD

#### processing_username and processing_password

* [transfer] processing_username = YOU_ACME1_USERNAME
* [transfer] processing_password = YOUR_ACME1_PASSWORD
* [transfer] globus_username = YOUR_GLOBUS_USERNAME
* [transfer] globus_password = YOUR_GLOBUS_PASSWORD

### Example run configuration

This may seam like a lot of stuff, but you only need to adust the keys mentioned above. Everything else is needed for the run, but the default values shouldn't need to be changed.

You can find a complete sample run.cfg [here](../run.cfg)

### NOTE

These instructions are for running the automated workflow with the coupled_diagnostics (AKA A-Prime, AKA Primary Diagnostics). To just run the atmospheric diagnostics, simply change the [global] output_patterns key to the following

    [global] output_patterns = {"ATM": "cam.h0"}

### Subsequent runs

After your initial setup, to start new runs the only values you should need to change are
```
[global] output_path
[global] data_cache_path
[global] set_frequency
[global] simulation_end_year
[global] run_id
[transfer] source_path
```
