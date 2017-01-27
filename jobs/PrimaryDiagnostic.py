import os

from uuid import uuid4
from util import render
from pprint import pformat

class PrimaryDiagnostic(object):
    def __init__(self, config):
        """
        Setup class attributes
        """
        self.inputs = {
            'coupled_project_dir': '',
            'test_casename': '',
            'test_native_res': '',
            'test_archive_dir': '',
            'test_begin_yr_climo': '',
            'test_end_yr_climo': '',
            'test_begin_yr_ts': '',
            'test_end_yr_ts': '',
            'ref_case': '',
            'ref_archive_dir': '',
            'mpas_meshfile': '',
            'mpas_remapfile': '',
            'pop_remapfile': '',
            'remap_files_dir': '',
            'GPCP_regrid_wgt_file': '',
            'CERES_EBAF_regrid_wgt_file': '',
            'ERS_regrid_wgt_file': '',
            'coupled_home_directory': '',
            'coupled_template_path': '',
            'rendered_output_path': '',
            'obs_ocndir': '',
            'obs_seaicedir': '',
            'obs_sstdir': '',
            'obs_iceareaNH': '',
            'obs_iceareaSH': '',
            'obs_icevolNH': '',
            'obs_icevolSH': ''
        }
        self.config = {}
        self.status = 'unvalidated'
        self.type = 'primary_diag'
        self.outputs = {}
        self.uuid = uuid4().hex
        self.job_id = 0
        self.depends_on = []
        self.prevalidate(config)
        print "init"


    def __str__(self):
        return pformat({
            'type': self.type,
            'config': self.config,
            'status': self.status,
            'depends_on': self.depends_on,
            'uuid': self.uuid,
            'job_id': self.job_id
        }, indent=4)

    def prevalidate(self, config):
        """
        Iterate over given config dictionary making sure all the inputs are set
        and rejecting any inputs that arent in the input dict
        """
        self.config = config
        self.depends_on = config.get('depends_on')
        self.status = 'valid'

    def postvalidate(self):
        """
        Check that what the job was supposed to do actually happened
        """
        print 'postvalidate'

    def execute(self):
        """
        Perform the actual work
        """
        render(
            variables=self.config,
            input_path=self.config.get('coupled_template_path'),
            output_path=self.config.get('rendered_output_path'),
            delimiter='%%')
        return
        #os.chdir(self.config.get('coupled_home_directory'))

