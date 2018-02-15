#
# This script installes processflow 'nightly' or 'latest' version.
#

import sys
import os
import argparse
from Util import *

parser = argparse.ArgumentParser(description="install processflow",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("-w", "--workdir",
                    help="working directory -- where conda env was installed")
parser.add_argument("-v", "--version",
                    help="version -- 'nightly' or 'latest'")

args = parser.parse_args()
workdir = args.workdir

if os.path.isdir(workdir) != True:
    print('FAIL: ' + workdir + ' directory where conda should have been ')
    print('       installed does not exist')
    sys.exit(FAILURE)

def check_version(version):
    
    if version == 'nightly':
        version_str = 'acme/label/nightly'
    elif version == 'latest':
        version_str = 'acme'

        
    # print out whole version of processflow -- just for logging purpose
    cmds_list = []
    cmd = "conda list processflow | grep '^processflow'"
    cmds_list.append(cmd)
    ret_code = run_in_conda_env(conda_path, env, cmds_list) 
    if ret_code != SUCCESS:
        return(ret_code)

    cmds_list = []
    cmd = "conda list processflow | grep '^processflow' | awk -F\\\" \\\" '{ print \$4 }'"
    cmds_list.append(cmd)

    (ret_code, output) = run_in_conda_env_capture_output(conda_path, env, cmds_list) 
    if ret_code != SUCCESS:
        return(ret_code)    

    if version_str == output[0].rstrip():
        print('Version matched: ' + version)
        ret_code = SUCCESS
    else:
        print('version: ' + version_str)
        print('output : ' + output[0])
        print("Version does not match!!!")
        ret_code = FAILURE

    return(ret_code)
 
#
# main
#

env_dir = workdir + '/miniconda2/envs/processflow'
if os.path.isdir(env_dir) == True:
    print('INFO: ' + env_dir + ' already exists')
    print('INFO: please cleanup if you want to recreate the env')
    sys.exit(FAILURE)

# get env.yml
env_url = 'https://raw.githubusercontent.com/ACME-Climate/acme_processflow/master/env.yml'
cmd = 'wget ' + env_url + ' -O ' + workdir + '/env.yml'

ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# create processflow env from the env file
conda_path = workdir + '/miniconda2/bin/'
env = 'processflow'
cmd = conda_path + 'conda create --name ' + env + ' --file ' + workdir + '/env.yml'
ret_code = run_cmd(cmd, True, False, True)
if ret_code != SUCCESS:
    sys.exit(FAILURE)

# update to version -- 'nightly' or 'latest'
cmds_list = []
cmd = 'conda config --set always_yes yes'
cmds_list.append(cmd)
if args.version == 'nightly':
    cmd = 'conda update -c acme/label/nightly -c acme '
    cmd += '-c conda-forge -c uvcdat processflow'
else:
    cmd = 'conda update -c acme -c conda-forge -c uvcdat processflow'
cmds_list.append(cmd)
ret_code = run_in_conda_env(conda_path, env, cmds_list)

# check that we can activate processflow env
cmd = 'conda list processflow'
cmds_list = []
cmds_list.append(cmd)
ret_code = run_in_conda_env(conda_path, env, cmds_list)

# check version of processflow
ret_code = check_version(args.version)
sys.exit(ret_code)








