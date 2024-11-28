from setuptools import setup, find_packages

setup(
    name="personal-health-etl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-storage>=2.14.0",
        "google-cloud-bigquery>=3.14.1",
        "pandas>=2.2.0",
        "requests>=2.31.0",
        "pyyaml>=6.0.1",
    ],
)
