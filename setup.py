#!/usr/bin/env python

from setuptools import setup

setup(name='tap-hubspot',
      version='1.0.1',
      description='Singer.io tap for extracting data from the Hubspot API',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_hubspot'],
      install_requires=[
        'backoff==1.8.0',
        'ratelimit==2.2.1',
        'requests==2.23.0',
        'singer-python==5.9.0'
      ],
      entry_points='''
          [console_scripts]
          tap-hubspot=tap_hubspot:main
      ''',
      packages=['tap_hubspot'],
      package_data = {
          'tap_hubspot': ['schemas/*.json'],
      }
)
