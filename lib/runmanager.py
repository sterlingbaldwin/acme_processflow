import os
import logging
import time
from datetime import datetime
from shutil import copytree, move, rmtree, copy2
from subprocess import Popen
from time import sleep

from lib.slurm import Slurm
from lib.util import get_climo_output_files
from lib.util import create_symlink_dir
from lib.util import print_line
from lib.util import render
from lib.util import format_debug

from jobs.job import Job
from jobs.diag import Diag
from jobs.climo import Climo
from jobs.regrid import Regrid
from jobs.timeseries import Timeseries
from jobs.amwg import AMWG
from jobs.e3smdiags import E3SMDiags
from jobs.aprime import Aprime
from lib.jobstatus import JobStatus, StatusMap, ReverseMap

job_map = {
    'climo': Climo,
    'timeseries': Timeseries,
    'regrid': Regrid,
    'e3sm_diags': E3SMDiags,
    'amwg': AMWG,
    'aprime': Aprime
}


class RunManager(object):

    def __init__(self, event_list, event, config, filemanager):


        self.config = config
        self.account = config['global'].get('account', '')
        self.event_list = event_list
        self.filemanager = filemanager
        self.dryrun = True if config['global']['dryrun'] == True else False
        self.debug = True if config['global']['debug'] == True else False
        self._resource_path = config['global']['resource_path']
        """
        A list of cases, dictionaries structured as:
            case (str): the full case name
            jobs (list): a list of job.Jobs
            short_name (str): the short name of the case
        """
        self.cases = list()

        self.running_jobs = list()
        self.kill_event = event
        self._job_total = 0
        self._job_complete = 0

        self.slurm = Slurm()
        max_jobs = config['global']['max_jobs']
        self.max_running_jobs = max_jobs if max_jobs else self.slurm.get_node_number() * 6
        while self.max_running_jobs == 0:
            sleep(1)
            msg = 'Unable to communication with scontrol, checking again'
            print_line(msg, event_list)
            logging.error(msg)
            self.max_running_jobs = self.slurm.get_node_number() * 6

    def check_max_running_jobs(self):
        """
        Checks if the maximum number of jobs are running

        Returns True if the max or more are running, false otherwise
        """
        try:
            job_info = self.slurm.queue()
        except:
            return True
        else:
            running_jobs = 0
            for job in job_info:
                if job['STATE'] in ['R', 'PD']:
                    running_jobs += 1
                if running_jobs >= self.max_running_jobs:
                    return True
            return False

    def add_pp_type_to_cases(self, freqs, job_type, start, end, case, run_type=None):
        """
        Add post processing jobs to the case.jobs list
        
        Parameters:
            freqs (list, int, None): the year length frequency to add this job
            job_type (str): what type of job to add
            start (int): the first year of simulated data
            end (int): the last year of simulated data
            data_type (str): what type of data to run this job on (regrid atm or lnd only)
            case (dict): the case to add this job to
            """
        if not freqs:
            freqs = end - start + 1
        if not isinstance(freqs, list): freqs = [freqs]
        
        for year in range(start, end + 1):
            for freq in freqs:
                freq = int(freq)
                if (year - start) % freq == 0:
                    new_job = job_map[job_type](
                        short_name=case['short_name'],
                        case=case['case'],
                        start=year,
                        end=year + freq - 1,
                        run_type=run_type)
                    case['jobs'].append(new_job)
    
    def add_diag_type_to_cases(self, freqs, job_type, start, end, case):
        """
        Add diagnostic jobs to the case.jobs list
        
        Parameters:
            freqs (list): a list of year lengths to add this job for
            job_type (str): the name of the job type to add
            start (int): the first year of simulated data
            end (int): the last year of simulated data
            case (dict): the case to add this job to
        """
        if not isinstance(freqs, list): freqs = [freqs]
        for year in range(start, end + 1):
            for freq in freqs:
                freq = int(freq)
                if (year - start) % freq == 0:
                    # get the comparisons from the config                    
                    comparisons = self.config['simulations']['comparisons'][case['case']]
                    if job_type == 'aprime':
                        comparisons = ['obs']
                    # for each comparison, add a job to this case
                    for item in comparisons:
                        if item == 'all':
                            for other_case in self.config['simulations']:
                                if other_case in ['start_year', 'end_year', 'comparisons', case['case']]: continue

                                new_diag = job_map[job_type](
                                    short_name=case['short_name'],
                                    case=case['case'],
                                    start=year,
                                    end=year + freq - 1,
                                    comparison=other_case,
                                    config=self.config)
                                case['jobs'].append(new_diag)

                            new_diag = job_map[job_type](
                                short_name=case['short_name'],
                                case=case['case'],
                                start=year,
                                end=year + freq - 1,
                                comparison='obs',
                                config=self.config)
                            case['jobs'].append(new_diag)
                        else:
                            new_diag = job_map[job_type](
                                short_name=case['short_name'],
                                case=case['case'],
                                start=year,
                                end=year + freq - 1,
                                comparison=item,
                                config=self.config)
                            case['jobs'].append(new_diag)

    def setup_cases(self):
        """
        Setup each case with all the jobs it will need
        """
        start = self.config['simulations']['start_year']
        end = self.config['simulations']['end_year']
        for case in self.config['simulations']:
            if case in ['start_year', 'end_year', 'comparisons']: continue
            self.cases.append({
                'case': case,
                'short_name': self.config['simulations'][case]['short_name'],
                'jobs': list()
            })
        
        pp = self.config.get('post-processing')
        if pp:
            for key, val in pp.items():
                cases_to_add = list()
                for case in self.cases:
                    if not self.config['simulations'][case['case']].get('job_types'): 
                        continue
                    if 'all' in self.config['simulations'][case['case']]['job_types'] or key in self.config['simulations'][case['case']]['job_types']: 
                        cases_to_add.append(case)
                if key in ['regrid', 'timeseries']:
                    for dtype in val:
                        if dtype not in self.config['data_types']: 
                            continue
                        for case in cases_to_add:
                            if 'all' in self.config['simulations'][case['case']]['data_types'] or dtype in self.config['simulations'][case['case']]['data_types']:
                                self.add_pp_type_to_cases(
                                    freqs=val.get('run_frequency'),
                                    job_type=key,
                                    start=start,
                                    end=end,
                                    run_type=dtype,
                                    case=case)
                else:
                    for case in cases_to_add:
                        self.add_pp_type_to_cases(
                            freqs=val.get('run_frequency'),
                            job_type=key,
                            start=start,
                            end=end,
                            case=case)
        diags = self.config.get('diags')
        if diags:
            for key, val in diags.items():
                # cases_to_add = list()
                for case in self.cases:
                    if not self.config['simulations'][case['case']].get('job_types'): 
                        continue
                    if 'all' in self.config['simulations'][case['case']]['job_types'] or key in self.config['simulations'][case['case']]['job_types']:
                        # cases_to_add.append(case)
                        self.add_diag_type_to_cases(
                            freqs=diags[key]['run_frequency'],
                            job_type=key,
                            start=start,
                            end=end,
                            case=case)

        self._job_total = 0
        for case in self.cases:
            self._job_total += len(case['jobs'])

    def setup_jobs(self):
        """
        Setup the dependencies for each job in each case
        """
        for case in self.cases:
            for job in case['jobs']:
                if job.comparison != 'obs':
                    other_case, = filter(lambda case: case['case'] == job.comparison, self.cases)
                    job.setup_dependencies(
                        jobs=case['jobs'],
                        comparison_jobs=other_case['jobs'])
                else:
                    job.setup_dependencies(
                        jobs=case['jobs'])
    
    def check_data_ready(self):
        """
        Loop over all jobs, checking if their data is ready, and setting
        the internal job.data_ready variable
        """
        for case in self.cases:
            for job in case['jobs']:
                job.check_data_ready(self.filemanager)
    
    def start_ready_jobs(self):
        """
        Loop over the list of jobs for each case, first setting up the data for, and then
        submitting each job to the queue
        """

        for case in self.cases:
            for job in case['jobs']:
                if job.status != JobStatus.VALID:
                    continue
                if len(self.running_jobs) >= self.max_running_jobs:
                    msg = 'running {} of {} jobs, waiting for queue to shrink'.format(
                        len(self.running_jobs), self.max_running_jobs)
                    if self.debug: 
                        print_line(msg, self.event_list)
                    return
                deps_ready = True
                for depjobid in job.depends_on:
                    depjob = self.get_job_by_id(depjobid)
                    if depjob.status != JobStatus.COMPLETED:
                        deps_ready = False
                        break
                if deps_ready and job.data_ready:
                    
                    # if the job was finished by a previous run of the processflow
                    valid = job.postvalidate(self.config, event_list=self.event_list)
                    if valid:
                        job.status = JobStatus.COMPLETED
                        self._job_complete += 1
                        job.handle_completion(
                            self.filemanager,
                            self.event_list,
                            self.config)
                        self.report_completed_job()
                        if isinstance(job, Diag):
                            msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job previously computed, skipping'.format(
                                job=job.job_type, start=job.start_year, end=job.end_year, case=job.short_name, comp=job._short_comp_name)
                        else:
                            msg = '{job}-{start:04d}-{end:04d}-{case}: Job previously computed, skipping'.format(
                                job=job.job_type, start=job.start_year, end=job.end_year, case=job.short_name)
                        print_line(msg, self.event_list)
                        continue

                    # the job is ready for submission
                    if job.run_type is not None:
                        msg = '{job}-{run_type}-{start:04d}-{end:04d}-{case}: Job ready, submitting to queue'.format(
                            job=job.job_type,
                            start=job.start_year,
                            end=job.end_year,
                            case=job.short_name,
                            run_type=job.run_type)
                    elif isinstance(job, Diag):
                        msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job ready, submitting to queue'.format(
                            job=job.job_type, 
                            start=job.start_year, 
                            end=job.end_year, 
                            case=job.short_name, 
                            comp=job._short_comp_name)
                    else:
                        msg = '{job}-{start:04d}-{end:04d}-{case}: Job ready, submitting to queue'.format(
                            job=job.job_type, 
                            start=job.start_year, 
                            end=job.end_year, 
                            case=job.short_name)
                    print_line(msg, self.event_list)

                    # set to pending before data setup so we dont double submit
                    job.status = JobStatus.PENDING
                    job.setup_data(
                        config=self.config,
                        filemanager=self.filemanager,
                        case=job.case)
                    if isinstance(job, Diag):
                        if job.comparison != 'obs':
                            job.setup_data(
                                config=self.config,
                                filemanager=self.filemanager,
                                case=job.comparison)
                    slurmid = job.execute(
                        config=self.config,
                        dryrun=self.dryrun)

                    if slurmid is False:
                        msg = '{job}-{start:04d}-{end:04d}-{case}: Prevalidation FAILED'.format(
                            job=job.job_type,
                            start=job.start_year,
                            end=job.end_year,
                            case=job.short_name)
                        print_line(msg, self.event_list)
                        job.status = JobStatus.FAILED
                    else:
                        self.running_jobs.append({
                            'slurm_id': slurmid,
                            'job_id': job.id
                        })
    
    def get_job_by_id(self, jobid):
        for case in self.cases:
            for job in case['jobs']:
                if job.id == jobid:
                    return job
        raise Exception("no job with id {} found".format(jobid))

    def write_job_sets(self, path):
        out_str = ''
        with open(path, 'w') as fp:
            for case in self.cases:
                out_str += '\n==' + '='*len(case['case']) + '==\n'
                out_str += '# {} #\n'.format(case['case'])
                out_str += '==' + '='*len(case['case']) + '==\n'
                for job in case['jobs']:
                    out_str += '\n\tname: ' + job.job_type
                    out_str += '\n\tperiod: {:04d}-{:04d}'.format(job.start_year, job.end_year)
                    if job._run_type:
                        out_str += '\n\trun_type: ' + job._run_type
                    out_str += '\n\tstatus: ' + job.status.name
                    deps_jobs = [self.get_job_by_id(x) for x in job.depends_on]
                    if deps_jobs: 
                        out_str += '\n\tdependent_on: ' + str(
                            ['{}'.format(x.msg_prefix()) for x in deps_jobs])
                    out_str += '\n\tdata_ready: ' + str(job.data_ready)
                    out_str += '\n\tid: ' + job.id 
                    if case['jobs'].index(job) != len(case['jobs']) - 1:
                        out_str += '\n------------------------------------'
                    else:
                        out_str += '\n'
            fp.write(out_str)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    def _precheck(self, year_set, jobtype, data_type=None):
        """
        Check that the jobtype for that given yearset isnt
        already in the job_list

        Parameters:
            set_number (int): the yearset number to check
            jobtype (str): the type of job to check for
        Returns:
            1 if the job/yearset combo are NOT in the job_list
            0 if they are
        """
        for job in year_set.jobs:
            if job.type == jobtype:
                if job.type != 'regrid':
                    return False
                else: # regrid is the only job type that can have multiple instances in a year_set
                    if job.data_type == data_type: # but only one instance per data type
                        return False
        return True

    def report_completed_job(self):
        msg = '{complete}/{total} jobs complete or {percent:.2f}%'.format(
            complete=self._job_complete,
            total=self._job_total,
            percent=(((self._job_complete * 1.0)/self._job_total)*100))
        print_line(msg, self.event_list)

    def monitor_running_jobs(self):
        slurm = Slurm()
        for_removal = list()
        for item in self.running_jobs:
            job = self.get_job_by_id(item['job_id'])
            if item['slurm_id'] == 0:
                self._job_complete += 1
                for_removal.append(item)
                job.handle_completion(
                    self.filemanager,
                    self.event_list,
                    self.config)
                self.report_completed_job()
                continue
            try:
                job_info = slurm.showjob(item['slurm_id'])
                if not job_info or job_info.get('JobState') is None:
                    continue
            except Exception as e:
                # if the job is old enough it wont be in the slurm list anymore
                # which will throw an exception
                self._job_complete += 1
                for_removal.append(item)
                
                valid = job.postvalidate(self.config, event_list=self.event_list)
                if valid:
                    job.status = JobStatus.COMPLETED
                    job.handle_completion(
                        self.filemanager,
                        self.event_list,
                        self.config)
                    self.report_completed_job()
                else:
                    line = "slurm lookup error for {job}: {id}".format(
                        job=job.job_type,
                        id=item['job_id'])
                    print_line(
                        line=line,
                        event_list=self.event_list)
                continue
            status = StatusMap[job_info.get('JobState')]
            if status != job.status:
                if job.run_type is not None:
                    msg = '{job}-{run_type}-{start:04d}-{end:04d}-{case}: Job changed from {s1} to {s2}'.format(
                        job=job.job_type,
                        start=job.start_year,
                        end=job.end_year,
                        s1=ReverseMap[job.status],
                        s2=ReverseMap[status],
                        case=job.short_name,
                        run_type=job.run_type)
                elif isinstance(job, Diag):
                    msg = '{job}-{start:04d}-{end:04d}-{case}-vs-{comp}: Job changed from {s1} to {s2}'.format(
                        job=job.job_type,
                        start=job.start_year,
                        end=job.end_year,
                        s1=ReverseMap[job.status],
                        s2=ReverseMap[status],
                        case=job.short_name,
                        comp=job._short_comp_name)
                else:
                    msg = '{job}-{start:04d}-{end:04d}-{case}: Job changed from {s1} to {s2}'.format(
                        job=job.job_type,
                        start=job.start_year,
                        end=job.end_year,
                        s1=ReverseMap[job.status],
                        s2=ReverseMap[status],
                        case=job.short_name)
                print_line(msg, self.event_list)
                job.status = status

                if status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    self._job_complete += 1
                    valid = job.postvalidate(self.config, event_list=self.event_list)
                    if not valid:
                        job.status = JobStatus.FAILED
                    job.handle_completion(
                        self.filemanager,
                        self.event_list,
                        self.config)
                    for_removal.append(item)
                    self.report_completed_job()
                    if status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                        for depjob in self.get_jobs_that_depend(job.id):
                            depjob.status = JobStatus.FAILED
        if not for_removal:
            return
        else:
            self.running_jobs = [x for x in self.running_jobs if x not in for_removal]
        return

    def get_jobs_that_depend(self, job_id):
        """
        returns a list of all jobs that depend on the give job
        """
        jobs = list()
        for case in self.cases:
            for job in case['jobs']:
                for depid in job.depends_on:
                    if depid == job_id:
                        jobs.append(job)
        return jobs

    def is_all_done(self):
        """
        Check if all jobs are done, and all processing has been completed

        return -1 if still running
        return 0 if a job failed
        return 1 if all complete
        """
        if len(self.running_jobs) > 0:
            return -1

        failed = False
        for case in self.cases:
            for job in case['jobs']:
                if job.status in [JobStatus.VALID, JobStatus.PENDING, JobStatus.RUNNING]:
                    return -1
                if job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                    failed = True
        if failed:
            return 0
        return 1
