from setuptools import setup, find_packages

setup(
    name='sales_data_transform',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
    ],
)