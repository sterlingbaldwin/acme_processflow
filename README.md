# ACME Automated Workflow

The acme_workflow performs post processing jobs automatically, removing many of the difficulties of performing 
diagnostics on model data. 

[Documentation can be found here](https://acme-climate.github.io/acme_workflow/docs/html/index.html)


# Usage<a name="usage"></a>

        usage: workflow.py [-h] [-c CONFIG] [-v] [-d] [-n] [-r] [-l LOG] [-u] [-m]
                        [-V] [-s SIZE]

        optional arguments:
        -h, --help            show this help message and exit
        -c CONFIG, --config CONFIG
                                Path to configuration file.
        -n, --no-ui           Turn off the GUI.
        -l LOG, --log LOG     Path to logging output file.
        -u, --no-cleanup      Don't perform pre or post run cleanup. This will leave
                                all run scripts in place.
        -m, --no-monitor      Don't run the remote monitor or move any files over
                                globus.
        -s SIZE, --size SIZE  The maximume size in gigabytes of a single transfer,
                                defaults to 100. Must be larger then the largest
                                single file.

