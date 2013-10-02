#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from distutils.core import setup
from snakebite.version import version

setup(
    name='snakebite',
    version=version(),
    author=u'Wouter de Bie',
    author_email='wouter@spotify.com',
    description='Pure Python HDFS client',
    url='http://github.com/spotify/snakebite',
    packages=['snakebite', 'snakebite.protobuf'],
    scripts=['bin/snakebite'],
    license='Apache License 2.0',
    keywords='hadoop protobuf hdfs'.split(),
    classifiers=[
        'Topic :: Utilities',
        'Programming Language :: Python',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Environment :: Other Environment'
    ],
    data_files=[
        ('etc/bash_completion.d', ['scripts/snakebite-completion.bash']),
        ('', ['LICENSE'])
    ],
    install_requires=[
        'protobuf>2.4.1',
        'argparse'
    ]
)
