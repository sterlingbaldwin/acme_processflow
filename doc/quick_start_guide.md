# Super Quick Start Guide

Use this guide if you're already an acme1 or aims4 user.

For a new run all you will need is to setup your runs configuration file. Make a copy of the sample config file
```
wget https://raw.githubusercontent.com/sterlingbaldwin/acme_workflow/master/run.cfg
```
Note: Due to a recent change to the lab firewall settings, internal users cannot access raw content from github. You can get a recent copy of the example run.cfg
from acme1:/p/cscratch/acme/data/run.cfg.example


Once you have the file, open it in your favorite editor. There are are 12 values that must be changed before you're ready to run. You can find an explanation of each of them [here](doc/quick_start_instructions.ipynb)

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

### Running

Running on acme1 and aims4 is very easy. Simply activate the conda environment provided, and run the script.
```
source activate /p/cscratch/acme/bin/acme
python /p/cscratch/acme/bin/acme_workflow/workflow.py -c /path/to/your/run.cfg
```

In interactive mode, if the terminal is closed or you log out, it will stop the process (but the runs managed by SLURM will continue). See below for headless mode instructions.

    python workflow.py -c run.cfg

![initial run](http://imgur.com/ZGuJUCk)

Once globus has transfered the first year_set of data, it will start running the post processing jobs.

![run in progress](http://imgur.com/URU4OVY)


### headless mode
Uninterupted run in headless mode that wont stop if you close the terminal, writing to a custom log location, with no cleanup after completion
```
nohup python workflow.py -c run.cfg --no-ui &
```

This run can continue after you close the termincal and log off the computer. While running in headless mode, you can check run_state.txt for the run status. This file can be found in your output directory.

```
less run_state.txt
```

![run_state](http://imgur.com/zS8f57g)
