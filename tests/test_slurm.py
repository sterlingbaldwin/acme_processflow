import unittest
import os, sys
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.slurm import Slurm


class TestSlurm(unittest.TestCase):

    def test_batch(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        slurm = Slurm()
        command = os.path.join('tests', 'test_slurm_batch.sh')
        job_id = slurm.batch(command, '-n 1 -N 1')
        self.assertTrue(job_id)
        self.assertTrue(isinstance(job_id, int))

        info = slurm.showjob(job_id)
        self.assertTrue(info['JobState'] in ['PENDING',
                                             'RUNNING', 'COMPLETE', 'COMPLETING'])

        info = slurm.queue()
        in_queue = False
        for item in info:
            if int(item['JOBID']) == job_id:
                in_queue = True
                self.assertTrue(item['STATE'] in ['PD', 'R'])
                break
        self.assertTrue(in_queue)
        slurm.cancel(job_id)

    def test_shownode(self):
        print '---- Starting Test: {} ----'.format(inspect.stack()[0][3])
        slurm = Slurm()
        node = 'acme1'
        node_info = slurm.shownode(node)
        self.assertTrue(node_info['Arch'] == 'x86_64')
        self.assertTrue(node_info['CoresPerSocket'] == '24')

    # def test_slurmrun(self):
    #     slurm = Slurm()
    #     command = 'hostname'
    #     sargs = '-n 1 -N 1'
    #     output = slurm.run(command, sargs=sargs)
    #     self.assertEqual(output, 'acme1.llnl.gov')


if __name__ == '__main__':
    unittest.main()
