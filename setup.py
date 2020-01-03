#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

from dynotool import __version__
import setuptools


PROJECT_URL = "https://github.com/Cloudzero/dynotool"
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='dyn-o-tool',
    version=__version__,
    description='Tools for better living with DynamoDB',
    long_description=long_description,
    author='CloudZero',
    author_email='support@cloudzero.com',
    url=PROJECT_URL,
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': ['dynotool=dynotool.main:main']
    },
    package_data={'dynotool': ['data/*']},
    include_package_data=True,
    install_requires=[
        'docopt>=0.6.2',
        'boto3>=1.10.46',
        'simplejson>=3.16.0'
    ],
    license="MIT",
    zip_safe=False,
    keywords='CloudZero DynamoDB',
    platforms=['MacOS', 'Unix'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Unix'
    ],
    python_requires='>=3.6'
)
