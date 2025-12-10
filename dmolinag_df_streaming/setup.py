from setuptools import setup, find_packages

setup(
    name="fif-sfa-dataflow",
    version="1.2.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "apache-beam[gcp]==2.69.0",
        "protobuf==4.25.3",
        "grpcio==1.62.2",
        "google-auth==2.41.1",
        "google-cloud-logging==3.9.0",
        "google-cloud-bigquery==3.25.0",
        "google-cloud-core==2.4.1",
        "google-cloud-dataflow-client==0.8.10",
        "google-cloud-secret-manager==2.20.2",
        "pycryptodomex==3.23.0",
    ]
)