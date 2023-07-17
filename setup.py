#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-marketingcloud',
    version='1.0.0',
    description='Singer.io tap for extracting data from the Salesforce Marketingcloud API',
    author='Data Innovation',
    url='https://datainnovation.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap-marketingcloud'],
    install_requires=[
        'funcy==2.0',
        'singer-python==5.13.0',
        'python-dateutil==2.8.0',
        'voluptuous==0.13',
        'Salesforce-FuelSDK-DI @ https://test-files.pythonhosted.org/packages/de/33/c6dfb4b9a3331486d898c951929a88ab5548af6f69514f34b8f2e0c74e61/Salesforce_FuelSDK_DI-2.0.3-py3-none-any.whl'
    ],
    extras_require={
        'test': [
            'pylint==2.10.2',
            'astroid==2.7.3',
            'nose'
        ],
        'dev': [
            'ipdb==0.11'
        ]
    },
    entry_points='''
    [console_scripts]
    tap-marketingcloud=tap_marketingcloud:main
    ''',
    packages=find_packages(),
    package_data={
        'tap_marketingcloud': ['schemas/*.json']
    }
)
