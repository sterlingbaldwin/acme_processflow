import os
import subprocess
import shlex
import time

SUCCESS = 0
FAILURE = 1

def run_cmd(cmd, join_stderr=True, shell_cmd=False, verbose=True):
    print("CMD: " + cmd)
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)

    if join_stderr:
        stderr = subprocess.STDOUT
    else:
        stderr = subprocess.PIPE

    P = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr,
        bufsize=0, cwd=os.getcwd(), shell=shell_cmd)
    out = []
    while P.poll() is None:
        read = P.stdout.readline().rstrip()
        out.append(read)
        if verbose == True:
            print(read)

    ret_code = P.returncode
    return(ret_code)

def git_clone_repo(workdir, repo_name):
    """ git clone https://github.com/UV-CDAT/<repo_name> and place it in
        <workdir>/<repo_name> directory                                              
    """
    if repo_name == 'pcmdi_metrics':
        url = 'https://github.com/pcmdi/' + repo_name
    else:
        url = 'https://github.com/UV-CDAT/' + repo_name

    
    cmd = 'git clone ' + url + ' ' + workdir + '/' + repo_name
        
    ret_code = run_cmd(cmd)
    if ret_code != SUCCESS:
        print("FAIL..." + cmd)
        return ret_code

    return(ret_code)

def run_in_conda_env(conda_path, env, cmds_list):
    cmd = 'bash -c \"export PATH=' + conda_path + ':$PATH; '
    cmd += 'source activate ' + env + '; '
    
    for a_cmd in cmds_list:
        cmd += a_cmd + '; '
    cmd += 'source deactivate \"'
    print('CMD: ' + cmd)

    ret_code = os.system(cmd)
    print(ret_code)
    return(ret_code)

def run_in_conda_env_capture_output(conda_path, env, cmds_list):

    current_time = time.localtime(time.time())
    time_str = time.strftime("%b.%d.%Y.%H:%M:%S", current_time)
    tmp_file = '/tmp/processFlow.' + time_str

    cmd = 'bash -c \"export PATH=' + conda_path + ':$PATH; '
    cmd += 'source activate ' + env + '; '
    
    for a_cmd in cmds_list:
        cmd += a_cmd + '; '
    cmd += 'source deactivate \"'
    cmd += ' > ' + tmp_file
    print('CMD: ' + cmd)

    ret_code = os.system(cmd)
    print(ret_code)
    if ret_code != SUCCESS:
        return(FAILURE, None)

    with open(tmp_file) as f:
        output = f.readlines()
    #os.remove(tmp_file)
    return(ret_code, output)

    



