import sys
from setuptools import find_packages, setup

data_files = [(sys.prefix + '/share/acme_workflow/resources', [
                    'resources/acme_diags_template.py',
                    'resources/amwg_template.csh',
                    'resources/config_template.json',
                    'resources/run_AIMS_template.csh'])]

setup(
    name="acme_workflow",
    version="0.3.0",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="ACME Automated Workflow.",
    scripts=["workflow.py"],
    packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test"]),
    data_files=data_files
)