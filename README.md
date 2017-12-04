# ACME Automated Workflow

The processflow performs post processing jobs automatically, removing many of the difficulties of performing 
diagnostics on model data. 

[Documentation can be found here](https://acme-climate.github.io/acme_processflow/docs/html/index.html)


# Usage<a name="usage"></a>

        usage: processflow.py [-h] [-c CONFIG] [-n] [-l LOG] [-u] [-m] [-s SIZE] [-f]
                      [-r RESOURCE_DIR]

        optional arguments:
        -h, --help            show this help message and exit
        -c CONFIG, --config CONFIG
                                Path to configuration file.
        -u, --ui              Turn on the GUI.
        -l LOG, --log LOG     Path to logging output file.
        -u, --no-cleanup      Don't perform pre or post run cleanup. This will leave
                                all run scripts in place.
        -m, --no-monitor      Don't run the remote monitor or move any files over
                                globus.
        -s SIZE, --size SIZE  The maximume size in gigabytes of a single transfer,
                                defaults to 100. Must be larger then the largest
                                single file.
        -f, --file-list       Turn on debug output of the internal file_list so you
                                can see what the current state of the model files are
        -r RESOURCE_DIR, --resource-dir RESOURCE_DIR
                                Path to custom resource directory
