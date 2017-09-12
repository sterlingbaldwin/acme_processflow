import os
from subprocess import Popen, PIPE


class Slurm(object):
    """A python interface for slurm using subprocesses
    
    """
    def __init__(self):
        """
        Check if the system has Slurm installed
        """
        if not any(os.access(os.path.join(path, 'sinfo'), os.X_OK) for path in os.environ["PATH"].split(os.pathsep)):
            raise Exception('Unable to find slurm, is it installed on this sytem?')

    
    def run(self, path, **kwargs):
        """
        Submit to the batch queue in non-interactive mode
        
        Parameters:
            path (str): The path to the run script that should be submitted
        
        returns:
            job id of the new job (int)
        """
        pass
    
    def srun(self, cmd, **kwargs):
        """
        Submit to slurm controller in interactive mode

        Parameters:
            cmd (str): the command to run
        
        returns:
            the output of the job (str)
        """
        pass
     
    def control(self, cmd, **kwargs):
        """
        Send commands to the slurm controller
        
        Parameters:
            cmd (str): the primary command to send
            kwargs: several subcommands are allowed here, for example
                if cmd = 'show', then kwarg could have {'subcommand': job', 'job_id': '42'} to run the command
                'scontrol show job 42'

        returns:
            """
        pass
    
    def queue(self):
        """
        Get job queue status
        
        returns: list of jobs in the queue
        """
        pass