import os
from time import sleep
from subprocess import Popen, PIPE


class Slurm(object):
    """
    A python interface for slurm using subprocesses
    """

    def __init__(self):
        """
        Check if the system has Slurm installed
        """
        if not any(os.access(os.path.join(path, 'sinfo'), os.X_OK) for path in os.environ["PATH"].split(os.pathsep)):
            raise Exception(
                'Unable to find slurm, is it installed on this sytem?')

    def batch(self, cmd, sargs=None):
        """
        Submit to the batch queue in non-interactive mode

        Parameters:
            cmd (str): The path to the run script that should be submitted
            sargs (str): The additional arguments to pass to slurm
        Returns:
            job id of the new job (int)
        """

        out, err = self._submit('sbatch', cmd, sargs)
        if err:
            raise Exception('SLURM ERROR: ' + err)
        out = out.split()
        if 'error' in out:
            return 0
        job_id = int(out[-1])
        return job_id

    # def run(self, cmd, sargs=False):
    #     """
    #     Submit to slurm controller in interactive mode

    #     NOTE: THIS IS A BLOCKING CALL. Control will not return until the command completes

    #     Parameters:
    #         cmd (str): the command to run
    #     Returns:
    #         the output of the job (str)
    #     """
    #     out, err = self._submit('srun', cmd, sargs)
    #     if 'error' in out:
    #         return False, err
    #     else:
    #         return True, out

    def _submit(self, subtype, cmd, sargs=None):

        cmd = [subtype, cmd, sargs] if sargs else [subtype, cmd]
        tries = 0
        while tries != 10:
            try:
                proc = Popen(cmd, shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                if 'Transport endpoint is not connected' in err:
                    tries += 1
                    sleep(tries)
                else:
                    break
            except:
                sleep(1)
        if tries == 10:
            raise Exception('SLURM ERROR: Transport endpoint is not connected')
        if 'Invalid job id specified' in err:
            raise Exception('SLURM ERROR: ' + err)

        return out, err

    def showjob(self, jobid):
        """
        A wrapper around scontrol show job

        Parameters:
            jobid (str): the job id to get information about
        Returns:
            A dictionary of information provided by slurm about the job
        """
        if not isinstance(jobid, str):
            jobid = str(jobid)
        success = False
        while not success:
            try:
                proc = Popen(['scontrol', 'show', 'job', jobid],
                             shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                if 'Transport endpoint is not connected' in err:
                    sleep(1)
                else:
                    success = True
            except:
                success = False
                sleep(1)

        if 'Invalid job id specified' in err:
            raise Exception('SLURM ERROR: ' + err)
        jobinfo = {}
        for item in out.split('\n'):
            for j in item.split(' '):
                index = j.find('=')
                if index <= 0:
                    continue
                jobinfo[j[:index]] = j[index + 1:]
        return jobinfo

    def shownode(self, nodeid):
        """
        A wrapper around scontrol show node

        Parameters:
            jobid (str): the node id to get information about
        Returns:
            A dictionary of information provided by slurm about the node
        """
        tries = 0
        while tries != 10:
            try:
                proc = Popen(['scontrol', 'show', 'node', nodeid],
                             shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                if 'Transport endpoint is not connected' in err:
                    tries += 1
                    sleep(tries)
                else:
                    break
            except:
                sleep(1)
        if tries == 10:
            raise Exception('SLURM ERROR: Transport endpoint is not connected')
        if 'Invalid job id specified' in err:
            raise Exception('SLURM ERROR: ' + err)
        nodeinfo = {}
        for item in out.split('\n'):
            for j in item.split(' '):
                index = j.find('=')
                if index <= 0:
                    continue
                nodeinfo[j[:index]] = j[index + 1:]
        return nodeinfo

    def get_node_number(self):
        """
        Use sinfo to return the number of nodes in the cluster
        """
        cmd = 'sinfo show nodes | grep up | wc -l'
        p = Popen([cmd], stderr=PIPE, stdout=PIPE, shell=True)
        out, err = p.communicate()
        while 'Transport endpoint is not connected' in out and not e:
            sleep(1)
            p = Popen([cmd], stderr=PIPE, stdout=PIPE, shell=True)
            err, out = p.communicate()
        return int(out)

    def queue(self):
        """
        Get job queue status

        Returns: list of jobs in the queue
        """
        tries = 0
        while tries != 10:
            try:
                proc = Popen(['squeue'], shell=False, stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                if 'Transport endpoint is not connected' in err:
                    tries += 1
                    sleep(tries)
                else:
                    break
            except:
                sleep(1)
        if tries == 10:
            raise Exception('SLURM ERROR: Transport endpoint is not connected')

        queueinfo = []
        for item in out.split('\n')[1:]:
            if not item:
                break
            line = [x for x in item.split(' ') if x]
            queueinfo.append({
                'JOBID': line[0],
                'PARTITIION': line[1],
                'NAME': line[2],
                'USER': line[3],
                'STATE': line[4],
                'TIME': line[5],
                'NODES': line[6],
                'NODELIST(REASON)': line[7]
            })
        return queueinfo

    def cancel(self, jobid):
        """
        Cancel a job by id

        Parameters:
            jobid (str): The id of the job to cancel
        Returns:
            True of the job was canceled, False otherwise
        """
        if not isinstance(jobid, str):
            jobid = str(jobid)
        tries = 0
        while tries != 10:
            try:
                proc = Popen(['scancel', jobid], shell=False,
                             stderr=PIPE, stdout=PIPE)
                out, err = proc.communicate()
                if 'Transport endpoint is not connected' in err:
                    tries += 1
                    sleep(tries)
                else:
                    break
            except:
                sleep(1)
        if tries == 10:
            raise Exception('SLURM ERROR: Transport endpoint is not connected')

        jobinfo = self.showjob(jobid)
        if jobinfo['JobState'] in ['CANCELLED', 'COMPLETED', 'COMPLETING']:
            return True
        else:
            return False
