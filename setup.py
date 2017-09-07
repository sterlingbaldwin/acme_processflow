import sys
from setuptools import find_packages, setup

setup(
    name="acme_workflow",
    version="0.3.0",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="ACME Automated Workflow.",
    scripts=["workflow.py"],
    packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test"])
)