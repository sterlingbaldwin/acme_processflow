# Troubleshooting Guide

## Table of Contents

1. [Bashrc not being sourced](#bashrc)
2. [Jobs failing](#failed_jobs)



#### bashrc<a name="bashrc"></a>

On some linux systems, bash doesnt source your ~/.bashrc directly, but instead runs your ~/.bash_profile.
If your bashrc isnt loading, create or edit your ~/.bash_profile, and add
```
if [ -f ~/.bashrc ]; then
  . ~/.bashrc
fi
```


#### Jobs failing<a name="failed_jobs"></a>

Check your output path for a directory called run_scripts. This directory holds all the slurm run scripts as
well as the raw output from the slurm runs. For example if a Ncclimo job failed for year\_set 2, checking the contents of ```output_path/run_scipts/ncclimo_1_1_10_SOME_RANDOME_HEX.out``` would give you the output from the command.

