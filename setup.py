import sys
from setuptools import find_packages, setup

data_files = [('resources', [
                    'acme_diags_template.py',
                    'amwg_template.csh',
                    'config_template.json',
                    'run_AIMS_template.csh'])]

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