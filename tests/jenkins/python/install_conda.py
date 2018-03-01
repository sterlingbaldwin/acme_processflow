
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

# create a unique dir
if os.path.isdir(workdir) == True:
    print("Work directory {} already exists".format(workdir))
    conda_bin = os.path.join(workdir, 'miniconda', 'bin')

    if os.path.isdir(conda_bin):
        print('Miniconda seems to be already installed')
        sys.exit(FAILURE)
else:
    os.makedirs(workdir)

# get miniconda
#source_url = 'https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh'
source_url = 'https://repo.continuum.io/miniconda/Miniconda2-4.3.31-Linux-x86_64.sh'
#cmd = 'wget --no-check ' + source_url + ' -O ' + workdir + '/miniconda2.sh'

conda_script = os.path.join(workdir, 'miniconda2.sh')
cmd = "wget --no-check {url} -O {the_script}".format(url=source_url,
                                                     the_script=conda_script)

ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# install miniconda
#cmd = 'bash ' + workdir + '/miniconda2.sh -b -p ' + workdir + '/miniconda2'

conda_path = os.path.join(workdir, 'miniconda2')
cmd = "bash {conda_script} -b -p {conda_path}".format(conda_script=conda_script,
                                                      conda_path=conda_path)
ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# check conda command
conda_cmd = os.path.join(conda_path, 'bin', 'conda')
cmd = "ls -l {conda_cmd}".format(conda_cmd=conda_cmd)
ret_code = run_cmd(cmd, True, False, True)

if ret_code == SUCCESS:
    print("\nMiniconda is successfully installed under~: " + workdir)

sys.exit(ret_code)



