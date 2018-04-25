# ACME Automated Workflow

The processflow performs post processing jobs automatically, removing many of the difficulties of performing 
diagnostics on model data. 

[Documentation can be found here](https://e3sm-project.github.io/processflow/docs/html/index.html)

## Installation<a name="installation"></a>

```
conda create --name <YOUR_NEW_ENVIRONMENT> -c acme -c conda-forge -c uvcdat processflow
```

# Usage<a name="usage"></a>

        usage: processflow.py [-h] [-c CONFIG] [-u] [-l LOG] [-n] [-m] [-s] [-f]
                      [-r RESOURCE_DIR] [-i INPUT_PATH] [-o OUTPUT_PATH]

        optional arguments:
        -h, --help            show this help message and exit
        -c CONFIG, --config CONFIG
                                Path to configuration file.
        -u, --ui              Turn on the GUI.
        -l LOG, --log LOG     Path to logging output file.
        -n, --no-host         Don't move output plots into the web host directory.
        -m, --no-monitor      Don't run the remote monitor or move any files over
                                globus.
        -s, --no-scripts      Don't copy the case_scripts directory from the remote
                                machine.
        -f, --file-list       Turn on debug output of the internal file_list so you
                                can see what the current state of the model files are
        -r RESOURCE_DIR, --resource-dir RESOURCE_DIR
                                Path to custom resource directory
        -i INPUT_PATH, --input-path INPUT_PATH
                                Custom input path
        -o OUTPUT_PATH, --output-path OUTPUT_PATH
                                Custom output path
