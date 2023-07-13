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
        'funcy==1.9.1',
        'singer-python==5.12.1',
        'python-dateutil==2.6.0',
        'voluptuous==0.10.5',
        'Salesforce-FuelSDK==1.3.0'
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
