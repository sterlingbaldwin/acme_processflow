
import sys
import os
import time
import argparse
from Util import *

parser = argparse.ArgumentParser(description="install conda",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("-w", "--workdir",
                    help="working directory -- miniconda will be installed in a subdirectory under this directory")

args = parser.parse_args()
workdir = args.workdir
#current_time = time.localtime(time.time())
#time_str = time.strftime("%b.%d.%Y.%H.%M.%S", current_time)
#workdir = top_workdir + '/workdir.' + time_str 

# create a unique dir
if os.path.isdir(workdir) == True:
    print('Work directory ' + workdir + ' already exists')
    if os.path.isdir(workdir + '/miniconda2/bin') == True:
        print('Miniconda seems to be already installed')
        sys.exit(FAILURE)

os.makedirs(workdir)

# get miniconda
source_url = 'https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh'
cmd = 'wget --no-check ' + source_url + ' -O ' + workdir + '/miniconda2.sh'
print("xxx CMD: " + cmd)
ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# install miniconda
cmd = 'bash ' + workdir + '/miniconda2.sh -b -p ' + workdir + '/miniconda2'
ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# check conda command
cmd = 'ls -l ' + workdir + '/miniconda2/bin/conda'
ret_code = run_cmd(cmd, True, False, True)

if ret_code == SUCCESS:
    print("\nMiniconda is successfully installed under: " + workdir)

sys.exit(ret_code)



