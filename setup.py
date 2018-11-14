#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

from dynotool import __version__
from setuptools import setup, find_packages


PROJECT_URL = "http://www.cloudzero.com"
doclink = "Please visit {}.".format(PROJECT_URL)

setup(
    name='cloudzero-dyn-o-tool',
    version=__version__,
    description='Tools for easier living with DynamoDB',
    long_description=doclink,
    author='CloudZero',
    author_email='support@cloudzero.com',
    url=PROJECT_URL,
    packages=find_packages(),
    entry_points={
        'console_scripts': ['dynotool=dynotool.main:main']
    },
    package_data={'dynotool': ['data/*']},
    include_package_data=True,
    install_requires=[
        'docopt>=0.6.2',
        'boto3>=1.5.6',
        'botocore>=1.8.20',
        'simplejson>=3.13.2'
    ],
    license="MIT",
    zip_safe=False,
    keywords='CloudZero DynamoDB',
    platforms=['MacOS', 'Unix'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Unix'
    ],
)
