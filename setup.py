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

try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand
except ImportError:
    from distutils.core import setup
    from distutils.cmd import Command as TestCommand

from snakebite.version import version

import sys

class Tox(TestCommand):
    user_options = [('tox-args=', None, "Arguments to pass to tox")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = ''
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(args=self.tox_args.split())
        sys.exit(errno)

install_requires = [
    'protobuf>2.4.1',
    'argparse']

extras_require = {
    'kerberos': [
        'python-krbV',
        'sasl']
}

tests_require = [
    'tox',
    'virtualenv>=1.11.2']

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
    install_requires=install_requires,
    extras_require=extras_require,
    tests_require=tests_require,
    cmdclass={'test': Tox}
)
