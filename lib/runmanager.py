import os
import logging
import time
import re
from datetime import datetime
from threading import Thread
from shutil import copytree
from subprocess import Popen

from lib.slurm import Slurm
from lib.util import get_climo_output_files
from lib.util import create_symlink_dir

from lib.YearSet import YearSet, SetStatus
from jobs.Ncclimo import Climo
from jobs.Timeseries import Timeseries
from jobs.AMWGDiagnostic import AMWGDiagnostic
from jobs.APrimeDiags import APrimeDiags
from jobs.E3SMDiags import E3SMDiags
from jobs.JobStatus import JobStatus, StatusMap


class RunManager(object):
    def __init__(self, event_list, output_path, caseID, scripts_path, thread_list, event):
        self.output_path = output_path
        self.slurm = Slurm()
        self.event_list = event_list
        self.caseID = caseID
        self.job_sets = []
        self.running_jobs = []
        self.monitor_thread = None
        self.thread_list = thread_list
        self.kill_event = event
        self._dryrun = False
        self.scripts_path = scripts_path
        if not os.path.exists(self.scripts_path):
            os.makedirs(self.scripts_path)

    def setup_job_sets(self, set_frequency, sim_start_year, sim_end_year, config, filemanager):
        sim_length = sim_end_year - sim_start_year + 1
        for freq in set_frequency:
            number_of_sets_at_freq = sim_length / freq

            # initialize the YearSet
            for i in range(1, number_of_sets_at_freq + 1):
                start_year = sim_start_year + ((i - 1) * freq)
                end_year = start_year + freq - 1
                print 'Creating job_set for {}-{}'.format(start_year, end_year)
                new_set = YearSet(
                    set_number=len(self.job_sets) + 1,
                    start_year=start_year,
                    end_year=end_year)
                self.add_jobs(
                    config=config,
                    year_set=new_set,
                    filemanager=filemanager)
                self.job_sets.append(new_set)

    def write_job_sets(self, path):
        if os.path.exists(path):
            os.remove(path)
        out_str = ''
        fp = open(path, 'w')
        for year_set in self.job_sets:
            out_str += 'Year_set {num}: {start:04d} - {end:04d}\n'.format(
                num=year_set.set_number,
                start=year_set.set_start_year,
                end=year_set.set_end_year)

            out_str += 'status: {status}\n'.format(
                status=year_set.status)

            for job in year_set.jobs:
                out_str += '  >   {type} -- {id}: {status}\n'.format(
                    type=job.type,
                    id=job.job_id,
                    status=job.status)
            out_str += '\n'
            fp.write(out_str)
        fp.close()

    def add_jobs(self, config, year_set, filemanager):
        """
        Initializes and adds all the jobs to the year_set

        Parameters:
            year_set (YearSet): The YearSet to populate with jobs
            config (dict): the master configuration dict
            filemanager (FileManager): The global file manager instance
        """
        required_jobs = {}
        for job_type, freqs in config.get('global').get('set_jobs').items():
            # Default to not running
            required_jobs[job_type] = False
            if not freqs:
                # This job shouldnt be run
                continue
            if isinstance(freqs, str):
                freqs = [freqs]
            for freq in freqs:
                freq = int(freq)
                if freq == year_set.length:
                    # Add the job to this year_set
                    required_jobs[job_type] = True
                    break
        start_year = year_set.set_start_year
        end_year = year_set.set_end_year

        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)

        atm_path = os.path.join(
            config['global']['project_path'], 'input', 'atm')
        output_base_path = config['global']['output_path']
        run_scripts_path = config['global']['run_scripts_path']

        regrid_output_dir = os.path.join(
            output_base_path,
            'climo_regrid')
        if not os.path.exists(regrid_output_dir):
            os.makedirs(regrid_output_dir)

        if required_jobs.get('ncclimo'):
            # Add the ncclimo job to the runmanager
            self.add_climo(
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                year_set=year_set,
                input_path=atm_path,
                regrid_map_path=config['ncclimo']['regrid_map_path'],
                output_path=output_base_path,
                regrid_output_dir=regrid_output_dir)

        if required_jobs.get('timeseries'):
            file_list = filemanager.get_file_paths_by_year(
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                _type='atm')
            # Add the timeseries job to the runmanager
            self.add_timeseries(
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                year_set=year_set,
                input_path=atm_path,
                regrid_map_path=config['ncclimo']['regrid_map_path'],
                var_list=config['ncclimo']['var_list'],
                output_path=output_base_path,
                file_list=file_list)

        if required_jobs.get('aprime'):
            # Add the aprime job
            web_directory = os.path.join(
                config['global']['host_directory'],
                os.environ['USER'],
                config['global']['experiment'],
                config['aprime_diags']['host_directory'],
                set_string)
            host_url = '/'.join([
                config['global']['img_host_server'],
                os.environ['USER'],
                config['global']['experiment'],
                config['aprime_diags']['host_directory'],
                set_string])

            self.add_aprime(
                web_directory=web_directory,
                host_url=host_url,
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                year_set=year_set,
                input_base_path=config['global']['output_path'],
                resource_path=config['global']['resource_dir'],
                test_atm_res=config['aprime_diags']['test_atm_res'],
                test_mpas_mesh_name=config['aprime_diags']['test_mpas_mesh_name'],
                aprime_code_path=config['aprime_diags']['aprime_code_path'],
                filemanager=filemanager)

        if required_jobs.get('amwg'):
            # Add AMWG
            web_directory = os.path.join(
                config.get('global').get('host_directory'),
                os.environ['USER'],
                config.get('global').get('experiment'),
                config.get('amwg').get('host_directory'),
                set_string)
            host_url = '/'.join([
                config.get('global').get('img_host_server'),
                os.environ['USER'],
                config.get('global').get('experiment'),
                config.get('amwg').get('host_directory'),
                set_string])

            self.add_amwg(
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                year_set=year_set,
                resource_path=config['global']['resource_dir'],
                web_directory=web_directory,
                host_url=host_url,
                output_path=output_base_path,
                regrid_path=regrid_output_dir,
                diag_home=config['amwg']['diag_home'])

        if required_jobs.get('e3sm_diags'):
            # Add the e3sm diags
            web_directory = os.path.join(
                config['global']['host_directory'],
                os.environ['USER'],
                config['global']['experiment'],
                config['e3sm_diags']['host_directory'],
                set_string)
            host_url = '/'.join([
                config['global']['img_host_server'],
                os.environ['USER'],
                config['global']['experiment'],
                config['e3sm_diags']['host_directory'],
                set_string])

            self.add_e3sm(
                start_year=year_set.set_start_year,
                end_year=year_set.set_end_year,
                year_set=year_set,
                resource_path=config['global']['resource_dir'],
                web_directory=web_directory,
                host_url=host_url,
                reference_data_path=config['e3sm_diags']['reference_data_path'],
                output_path=output_base_path,
                seasons=config['e3sm_diags']['seasons'],
                backend=config['e3sm_diags']['backend'],
                sets=config['e3sm_diags']['sets'])

    def add_climo(self, **kwargs):
        """
        Add an ncclimo job to the job_list

        Parameters:
            start_year (int): the first year
            end_year (int): the last year
            year_set (int): the set number
            input_path (str): the path to the raw cam.h0 files
            regrid_map_path (str): the path to the regrid map
        """
        start_year = kwargs['start_year']
        output_path = kwargs['output_path']
        end_year = kwargs['end_year']
        year_set = kwargs['year_set']
        input_path = kwargs['input_path']
        regrid_map_path = kwargs['regrid_map_path']
        regrid_output_dir = kwargs['regrid_output_dir']

        if not self._precheck(year_set, 'ncclimo'):
            return

        set_length = end_year - start_year + 1
        climo_output_dir = os.path.join(
            self.output_path,
            'climo',
            '{}yr'.format(set_length))
        if not os.path.exists(climo_output_dir):
            os.makedirs(climo_output_dir)

        config = {
            'run_scripts_path': self.scripts_path,
            'start_year': start_year,
            'end_year': end_year,
            'caseId': self.caseID,
            'annual_mode': 'sdd',
            'regrid_map_path': regrid_map_path,
            'input_directory': input_path,
            'climo_output_directory': climo_output_dir,
            'regrid_output_directory': regrid_output_dir,
            'year_set': year_set.set_number,
        }
        climo = Climo(
            config=config,
            event_list=self.event_list)
        msg = 'Adding Ncclimo job to the job list: {}'.format(str(climo))
        logging.info(msg)
        year_set.add_job(climo)

    def add_timeseries(self, **kwargs):
        """
        Add a timeseries job to the job_list

        Parameters:
            start_year (int): the first year
            end_year (int): the last year
            year_set (int): the set number
            input_path (str): the path to the raw cam.h0 files
            regrid_map_path (str): the path to the regrid map
            var_list (list(str)): the list of variables to extract
        """
        start_year = kwargs['start_year']
        end_year = kwargs['end_year']
        year_set = kwargs['year_set']
        input_path = kwargs['input_path']
        regrid_map_path = kwargs['regrid_map_path']
        var_list = kwargs['var_list']
        file_list = kwargs['file_list']

        if not self._precheck(year_set, 'timeseries'):
            return

        set_length = end_year - start_year + 1
        timeseries_output_dir = os.path.join(
            self.output_path,
            'monthly',
            '{}yr'.format(set_length))
        if not os.path.exists(timeseries_output_dir):
            os.makedirs(timeseries_output_dir)

        config = {
            'file_list': file_list,
            'run_scripts_path': self.scripts_path,
            'annual_mode': 'sdd',
            'caseId': self.caseID,
            'year_set': year_set.set_number,
            'var_list': var_list,
            'start_year': start_year,
            'end_year': end_year,
            'output_directory': timeseries_output_dir,
            'regrid_map_path': regrid_map_path
        }
        timeseries = Timeseries(
            config=config,
            event_list=self.event_list)
        msg = 'Adding Timeseries job to the job list: {}'.format(
            str(timeseries))
        logging.info(msg)
        year_set.add_job(timeseries)

    def add_aprime(self, **kwargs):
        """
        Add an APrime job to the job_list

        Parameters:
            web_directory (str): The path to the directory to store images for hosting
            host_url (str): The url to access the images once their hosted
            year_set (YearSet): The YearSet that this job belongs to
            start_year (int): The start year of this set
            end_year (int): The end year of this set
            input_base_path (str): the global input path
            resource_path (str): Path to the resource directory
            test_atm_res (str): the atm resolution for the test input
            test_mpas_mesh_name (str): The test mpas mesh name
            aprime_code_path (str): the path to the aprime code
        """
        web_directory = kwargs['web_directory']
        host_url = kwargs['host_url']
        year_set = kwargs['year_set']
        start_year = kwargs['start_year']
        end_year = kwargs['end_year']
        input_base_path = kwargs['input_base_path']
        resource_path = kwargs['resource_path']
        test_atm_res = kwargs['test_atm_res']
        test_mpas_mesh_name = kwargs['test_mpas_mesh_name']
        aprime_code_path = kwargs['aprime_code_path']
        filemanager = kwargs['filemanager']

        if not self._precheck(year_set, 'aprime_diags'):
            return

        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)

        project_dir = os.path.join(
            self.output_path,
            'aprime_diags',
            year_set_string)
        if not os.path.exists(project_dir):
            os.makedirs(project_dir)

        input_path = os.path.join(
            input_base_path,
            'tmp',
            'aprime',
            year_set_string)
        if not os.path.exists(input_path):
            os.makedirs(input_path)

        # Varify the template
        template_path = os.path.join(
            resource_path,
            'aprime_template.bash')
        if not os.path.exists(template_path):
            msg = 'Unable to find amwg template at {path}'.format(
                path=template_path)
            print msg
            logging.error(msg)
            return

        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'run_scripts_path': self.scripts_path,
            'year_set': year_set.set_number,
            'input_path': input_path,
            'start_year': start_year,
            'end_year': end_year,
            'output_path': project_dir,
            'template_path': template_path,
            'test_atm_res': test_atm_res,
            'test_mpas_mesh_name': test_mpas_mesh_name,
            'aprime_code_path': aprime_code_path,
            'filemanager': filemanager
        }
        aprime = APrimeDiags(
            config=config,
            event_list=self.event_list)
        msg = 'Creating aprime diagnostic: {}'.format(str(aprime))
        logging.info(msg)
        logging.info('Prevalidating aprime')
        if aprime.status == JobStatus.VALID:
            logging.info('Aprime is valid, adding it to the job_list')
            year_set.add_job(aprime)
        else:
            logging.info('Aprime is NOT valid, rejecting')

    def add_amwg(self, **kwargs):
        """
        Add an amwg job to the job_list

        Parameters:
            start_year (int): the first year
            end_year (int): the last year
            year_set (int): the set number
            resource_path (str): path to the resource directory
            web_directory (str): the directory to store files for hosting
            host_url (str): the url to view the hosted files
            regrid_path (str): path to the regridded history files
        """
        start_year = kwargs['start_year']
        end_year = kwargs['end_year']
        year_set = kwargs['year_set']
        resource_path = kwargs['resource_path']
        web_directory = kwargs['web_directory']
        host_url = kwargs['host_url']
        regrid_path = kwargs['regrid_path']
        diag_home = kwargs['diag_home']

        if not self._precheck(year_set, 'amwg'):
            return

        year_set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)

        # Setup the amwg output directory
        output_path = os.path.join(
            self.output_path,
            'amwg_diag',
            year_set_string)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # Setup the AMWG temp directory
        temp_path = os.path.join(
            self.output_path,
            'tmp',
            'amwg',
            year_set_string)
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)

        # Varify the template
        template_path = os.path.join(
            resource_path,
            'amwg_template.csh')
        if not os.path.exists(template_path):
            msg = 'Unable to find amwg template at {path}'.format(
                path=template_path)
            logging.error(msg)
            return

        config = {
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'run_scripts_path': self.scripts_path,
            'output_path': output_path,
            'test_casename': self.caseID,
            'test_path_history': regrid_path + os.sep,
            'regrided_climo_path': regrid_path + os.sep,
            'test_path_climo': temp_path,
            'test_path_diag': output_path,
            'start_year': start_year,
            'end_year': end_year,
            'year_set': year_set.set_number,
            'run_directory': output_path,
            'template_path': template_path,
            'diag_home': diag_home
        }
        amwg_diag = AMWGDiagnostic(
            config=config,
            event_list=self.event_list)
        msg = 'Adding AMWGDiagnostic job to the job list: {}'.format(
            str(amwg_diag))
        logging.info(msg)
        year_set.add_job(amwg_diag)

    def add_e3sm(self, **kwargs):
        """
        Add an E3SM job to the job_list

        Parameters:
            start_year (int): the first year
            end_year (int): the last year
            year_set (int): the set number
            resource_path (str): path to the resource directory
            web_directory (str): the directory to store files for hosting
            host_url (str): the url to view the hosted files
            reference_data_path (str): Path to the e3sm reference data
        """
        start_year = kwargs['start_year']
        end_year = kwargs['end_year']
        year_set = kwargs['year_set']
        resource_path = kwargs['resource_path']
        web_directory = kwargs['web_directory']
        host_url = kwargs['host_url']
        reference_data_path = kwargs['reference_data_path']
        seasons = kwargs['seasons']
        backend = kwargs['backend']
        sets = kwargs['sets']
        resource_path = kwargs['resource_path']

        if not self._precheck(year_set, 'e3sm_diags'):
            print 'rejecting e3sm_diags'
            return

        set_string = '{start:04d}-{end:04d}'.format(
            start=start_year,
            end=end_year)

        # Setup output directory
        output_path = os.path.join(
            self.output_path,
            'e3sm_diags',
            set_string)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        # Setup temp directory
        temp_path = os.path.join(
            self.output_path,
            'tmp',
            'e3sm_diags',
            set_string)
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)

        # Varify the template
        template_path = os.path.join(
            resource_path,
            'e3sm_diags_template.py')
        if not os.path.exists(template_path):
            msg = 'Unable to find amwg template at {path}'.format(
                path=template_path)
            logging.error(msg)
            return

        regrid_path = os.path.join(
            self.output_path,
            'climo_regrid')

        config = {
            'regrid_base_path': regrid_path,
            'web_dir': web_directory,
            'host_url': host_url,
            'experiment': self.caseID,
            'regrided_climo_path': temp_path,
            'reference_data_path': reference_data_path,
            'test_name': self.caseID,
            'seasons': seasons,
            'backend': backend,
            'sets': sets,
            'results_dir': output_path,
            'template_path': template_path,
            'run_scripts_path': self.scripts_path,
            'end_year': end_year,
            'start_year': start_year,
            'year_set': year_set.set_number,
            'output_path': output_path
        }
        e3sm_diag = E3SMDiags(
            config=config,
            event_list=self.event_list)
        msg = "Adding ACME Diagnostic to the job list: {}".format(
            str(e3sm_diag))
        logging.info(msg)
        year_set.add_job(e3sm_diag)

    def _precheck(self, year_set, jobtype):
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
                return False
        return True

    def start_ready_job_sets(self):
        """
        Iterates over the job sets checking for ready ready jobs, and starts them
        """

        for job_set in self.job_sets:
            # if the job state is ready, but hasnt started yet
            if job_set.status in [SetStatus.DATA_READY, SetStatus.RUNNING]:
                # Iterate over all jobs in the set
                for job in job_set.jobs:
                    if job.status == JobStatus.INVALID:
                        msg = 'ERROR: {job}-{start:04d}-{end:04d} is in an INVALID state, see log for details'.format(
                            job=job.type,
                            start=job_set.start_year,
                            end=job_set.end_year)
                        self.event_list.push(message=msg)
                        logging.error('INVALID JOB')
                        logging.error(str(job))
                        continue
                    # If the job has any dependencies
                    # iterate through the list to see if they're done
                    ready = True
                    if job.depends_on and len(job.depends_on) > 0:
                        for dependancy in job.depends_on:
                            for dependent_job in job_set.jobs:
                                if dependent_job.type == dependancy:
                                    if dependent_job.status != JobStatus.COMPLETED:
                                        ready = False
                                        msg = '{job} is waiting on {dep}'.format(
                                            job=job.type,
                                            dep=dependent_job.type)
                                        logging.info(msg)
                                        break
                    # If the job isnt ready, skip it
                    if not ready:
                        continue
                    # If the job is valid, start it
                    if job.status == JobStatus.VALID:
                        job.execute(dryrun=self._dryrun)
                        self.running_jobs.append(job)
                        self.monitor_running_jobs()

    def monitor_running_jobs(self):
        slurm = Slurm()
        for job in self.running_jobs:
            if job.job_id == 0:
                self.handle_completed_job(job)
                self.running_jobs.remove(job)
                continue
            job_info = slurm.showjob(job.job_id)
            status = job_info.get('JobState')
            if not status:
                print 'No status yet for {}'.format(job.type)
                continue
            status = StatusMap[status]
            if status != job.status:
                msg = '{job}-{start:04d}-{end:04d}:{id} changed from {s1} to {s2}'.format(
                    job=job.type,
                    start=job.start_year,
                    end=job.end_year,
                    s1=job.status,
                    s2=status,
                    id=job.job_id)
                print msg
                self.event_list.push(message=msg)
                job.status = status

                if status == JobStatus.RUNNING:
                    job.start_time = datetime.now()
                    for job_set in self.job_sets:
                        if job_set.set_number == job.year_set \
                                and job_set.status != SetStatus.FAILED:
                            job_set.status = SetStatus.RUNNING
                            break
                elif status == JobStatus.COMPLETED:
                    job.end_time = datetime.now()
                    self.handle_completed_job(job)
                    self.running_jobs.remove(job)
                elif status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                    job.end_time = datetime.now()
                    for job_set in self.job_sets:
                        if job_set.set_number == job.year_set:
                            job_set.status = SetStatus.FAILED
                            break
                    self.running_jobs.remove(job)

    def handle_completed_job(self, job):
        """
        Perform post execution tasks
        """
        print 'handling completion for {job}: {start:04d}-{end:04d}'.format(
            job=job.type,
            start=job.start_year,
            end=job.end_year)
        job_set = None
        for s in self.job_sets:
            if s.set_number == job.year_set:
                job_set = s
                break

        # First check that we have the expected output
        if not job.postvalidate():
            message = 'ERROR: {job}-{start:04d}-{end:04d} does not have expected output'.format(
                job=job.type,
                start=job.start_year,
                end=job.end_year)
            self.event_list.push(
                message=message,
                data=job)
            print message
            logging.error(message)
            job.status = JobStatus.FAILED
            job_set.status = SetStatus.FAILED
            return

        # The job completed and has expected output
        done = True if job_set else False
        if done:
            for j in job_set.jobs:
                if j.status != JobStatus.COMPLETED:
                    done = False
                    break
        if done:
            job_set.status = SetStatus.COMPLETED

        # Finally host the files
        if job.type == 'aprime_diags':
            # TODO: Get aprime working
            pass
            # img_dir = 'coupled_diagnostics_{casename}-obs'.format(
            #     casename=job.config.get('test_casename'))
            # img_src = os.path.join(
            #     job.config.get('coupled_project_dir'),
            #     img_dir)
            # setup_local_hosting(job, event_list, img_src)
            # msg = '{job} hosted at {url}/index.html'.format(
            #     url=url,
            #     job=job.type)
            # logging.info(msg)
        elif job.type == 'amwg':
            img_dir = '{start:04d}-{end:04d}{casename}-obs'.format(
                start=job.config.get('start_year'),
                end=job.config.get('end_year'),
                casename=job.config.get('test_casename'))
            head, _ = os.path.split(job.config.get('test_path_diag'))
            img_src = os.path.join(head, img_dir)
            self.setup_local_hosting(job, img_src)
            msg = '{job} hosted at {url}/index.html'.format(
                url=job.config.get('host_url'),
                job=job.type)
            print msg
            logging.info(msg)
        elif job.type == 'e3sm_diags':
            img_src = job.config.get('results_dir')
            self.setup_local_hosting(job, img_src)
            msg = '{job} hosted at {url}/viewer/index.html'.format(
                url=job.config.get('host_url'),
                job=job.type)
            print msg
            logging.info(msg)

    def setup_local_hosting(self, job, img_src):
        """
        Sets up the local directory for hosting diagnostic output
        """
        msg = 'Setting up local hosting for {}'.format(job.type)
        self.event_list.push(
            message=msg,
            data=job)
        logging.info(msg)

        host_dir = job.config.get('web_dir')
        url = job.config.get('host_url')
        if os.path.exists(job.config.get('web_dir')):
            new_id = time.strftime("%Y-%m-%d-%I-%M")
            host_dir += '_' + new_id
            url += '_' + new_id
            job.config['host_url'] = url
        if not os.path.exists(img_src):
            msg = '{job} hosting failed, no image source at {path}'.format(
                job=job.type,
                path=img_src)
            logging.error(msg)
            return
        try:
            msg = 'copying images from {src} to {dst}'.format(
                src=img_src, dst=host_dir)
            logging.info(msg)
            copytree(src=img_src, dst=host_dir)
            
            while True:
                try:
                    p = Popen(['chmod', '-R', '0755', host_dir])
                except:
                    sleep(1)
                else:
                    break
            out, err = p.communicate()
            head, _ = os.path.split(host_dir)
            os.chmod(head, 0755)
            head, _ = os.path.split(head)
            os.chmod(head, 0755)
        except Exception as e:
            from lib.util import print_debug
            print_debug(e)
            msg = 'Error copying {0} to host directory {1}'.format(
                job.type, host_dir)
            self.event_list.push(
                message=msg,
                data=job)
            return

    def is_all_done(self):
        """
        Check if all job_sets are done, and all processing has been completed

        return -1 if still running
        return 0 if a jobset failed
        return 1 if all complete
        """

        # First check for pending jobs
        for job_set in self.job_sets:
            if job_set.status != SetStatus.COMPLETED \
                    and job_set.status != SetStatus.FAILED:
                return -1
        # all job sets are either complete or failed
        for job_set in self.job_sets:
            if job_set.status != SetStatus.COMPLETED:
                return 0
        return 1

    @property
    def dryrun(self):
        return self._dryrun

    @dryrun.setter
    def dryrun(self, _dryrun):
        self._dryrun = _dryrun
