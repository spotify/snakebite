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
import unittest2
import os
import pwd

from snakebite.commandlineparser import CommandLineParser


class CliTest(unittest2.TestCase):

    def setUp(self):
        self.parser = CommandLineParser()
        self.default_dir = os.path.join("/user", pwd.getpwuid(os.getuid())[0])

    def test_general_options(self):
        parser = self.parser

        output = parser.parse('ls some_folder'.split())
        self.assertFalse(output.debug)
        self.assertFalse(output.human)
        self.assertFalse(output.json)
        self.assertEqual(output.namenode, None)
        self.assertEqual(output.port, None)

        #each option
        output = parser.parse('-D ls some_folder'.split())
        self.assertTrue(output.debug)
        output = parser.parse('--debug ls some_folder'.split())
        self.assertTrue(output.debug)

        output = parser.parse('-H ls some_folder'.split())
        self.assertTrue(output.human)
        output = parser.parse('--human ls some_folder'.split())
        self.assertTrue(output.human)

        output = parser.parse('-j ls some_folder'.split())
        self.assertTrue(output.json)
        output = parser.parse('--json ls some_folder'.split())
        self.assertTrue(output.json)

        output = parser.parse('-n 100 ls some_folder'.split())  # what are typical values for namenodes?
        self.assertEqual(output.namenode, 100)
        output = parser.parse('--namenode 100 ls some_folder'.split())
        self.assertEqual(output.namenode, 100)

        output = parser.parse('-p 1234 ls some_folder'.split())
        self.assertEqual(output.port, 1234)
        output = parser.parse('--port 1234 ls some_folder'.split())
        self.assertEqual(output.port, 1234)

        output = parser.parse('-V 4 ls some_folder'.split())
        self.assertEqual(output.version, 4)
        output = parser.parse('--version 4 ls some_folder'.split())
        self.assertEqual(output.version, 4)

        output = parser.parse('-u alfred ls some_folder'.split())
        self.assertEqual(output.user, "alfred")
        output = parser.parse('--user alfred ls some_folder'.split())
        self.assertEqual(output.user, "alfred")
        output = parser.parse('ls some_folder'.split())
        self.assertEqual(output.user, None)

        #all options
        output = parser.parse('-D -H -j -n 100 -p 1234 -V 4 -u alfred ls some_folder'.split())
        self.assertTrue(output.debug)
        self.assertTrue(output.human)
        self.assertTrue(output.json)
        self.assertEqual(output.namenode, 100)
        self.assertEqual(output.port, 1234)
        self.assertEqual(output.version, 4)
        self.assertEqual(output.user, 'alfred')

        #options in illegal position
        with self.assertRaises(SystemExit):
            parser.parse('ls -D some_folder'.split())
        with self.assertRaises(SystemExit):
            parser.parse('ls some_folder -D'.split())

    def test_ls(self):
        parser = self.parser

        #no dir
        output = parser.parse('ls'.split())
        self.assertEqual(output.command, 'ls')
        self.assertEqual(output.dir, [self.default_dir])

        #one dir
        output = parser.parse('ls some_dir'.split())
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('ls dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

        #specific commands
        output = parser.parse('ls -d -R -s some_dir'.split())
        self.assertTrue(output.directory)
        self.assertTrue(output.recurse)
        self.assertTrue(output.summary)
        self.assertEqual(output.dir, ['some_dir'])

    def test_mkdir(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('mkdir'.split())

        #one dir
        output = parser.parse('mkdir some_dir'.split())
        self.assertEqual(output.command, 'mkdir')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('mkdir dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_mkdirp(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('mkdirp'.split())

        #one dir
        output = parser.parse('mkdirp some_dir'.split())
        self.assertEqual(output.command, 'mkdirp')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('mkdirp dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_chown(self):
        parser = self.parser

        #no dir and/or no owner
        with self.assertRaises(SystemExit):
            parser.parse('chown'.split())
        with self.assertRaises(SystemExit):
            parser.parse('chown owner_or_dir'.split())

        #one dir
        output = parser.parse('chown root some_dir'.split())
        self.assertEqual(output.command, 'chown')
        self.assertEqual(output.dir, ['some_dir'])
        self.assertEqual(output.single_arg, 'root')

        #multiple dirs
        output = parser.parse('chown root dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])
        self.assertEqual(output.single_arg, 'root')

        #recursive
        output = parser.parse('chown -R root some_dir'.split())
        self.assertTrue(output.recurse)

    def test_chmod(self):
        parser = self.parser

        #no dir and/or no mode
        with self.assertRaises(SystemExit):
            parser.parse('chmod'.split())
        with self.assertRaises(SystemExit):
            parser.parse('chmod mode_or_dir'.split())

        #one dir
        output = parser.parse('chmod 664 some_dir'.split())
        self.assertEqual(output.command, 'chmod')
        self.assertEqual(output.dir, ['some_dir'])
        self.assertEqual(output.single_int_arg, 664)

        #wrong type for mode argument
        with self.assertRaises(SystemExit):
            parser.parse('chmod not_an_int some_dir'.split())

        #multiple dirs
        output = parser.parse('chmod 664 dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])
        self.assertEqual(output.single_int_arg, 664)

        #recursive
        output = parser.parse('chmod -R 664 some_dir'.split())
        self.assertTrue(output.recurse)

    def test_chgrp(self):
        parser = self.parser

        #no dir and/or no group
        with self.assertRaises(SystemExit):
            parser.parse('chgrp'.split())
        with self.assertRaises(SystemExit):
            parser.parse('chgrp group_or_dir'.split())

        #one dir
        output = parser.parse('chgrp group some_dir'.split())
        self.assertEqual(output.command, 'chgrp')
        self.assertEqual(output.dir, ['some_dir'])
        self.assertEqual(output.single_arg, 'group')

        #multiple dirs
        output = parser.parse('chgrp group dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])
        self.assertEqual(output.single_arg, 'group')

        #recursive
        output = parser.parse('chgrp -R group some_dir'.split())
        self.assertTrue(output.recurse)

    def test_count(self):
        parser = self.parser

        #no dir
        output = parser.parse('count'.split())
        self.assertEqual(output.command, 'count')
        self.assertEqual(output.dir, [self.default_dir])

        #one dir
        output = parser.parse('count some_dir'.split())
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('count dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_df(self):
        parser = self.parser

        #no dir
        output = parser.parse('df'.split())
        self.assertEqual(output.command, 'df')

        with self.assertRaises(SystemExit):
            parser.parse('df some_additional_argument'.split())

    def test_du(self):
        parser = self.parser

        #no dir
        output = parser.parse('du'.split())
        self.assertEqual(output.command, 'du')
        self.assertEqual(output.dir, [self.default_dir])

        #one dir
        output = parser.parse('du some_dir'.split())
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('du dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

        #summary
        output = parser.parse('du -s some_dir'.split())
        self.assertTrue(output.summary)

    def test_mv(self):
        parser = self.parser

        #no source and/or no destination
        with self.assertRaises(SystemExit):
            parser.parse('mv'.split())
        with self.assertRaises(SystemExit):
            parser.parse('mv src_or_dest'.split())

        #one source
        output = parser.parse('mv source some_dest'.split())
        self.assertEqual(output.command, 'mv')
        self.assertEqual(output.dir, ['source'])
        self.assertEqual(output.single_arg, 'some_dest')

        #multiple sources
        output = parser.parse('mv source1 source2 source3 some_dest'.split())
        self.assertEqual(output.dir, ['source1', 'source2', 'source3'])
        self.assertEqual(output.single_arg, 'some_dest')

    def test_rm(self):
        parser = self.parser

        #no dir and/or no group
        with self.assertRaises(SystemExit):
            parser.parse('rm'.split())

        #one dir
        output = parser.parse('rm some_dir'.split())
        self.assertEqual(output.command, 'rm')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('rm dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

        #recursive
        output = parser.parse('rm -R some_dir'.split())
        self.assertTrue(output.recurse)

    def test_touchz(self):
        parser = self.parser

        #no dir and/or no group
        with self.assertRaises(SystemExit):
            parser.parse('touchz'.split())

        #one dir
        output = parser.parse('touchz some_dir'.split())
        self.assertEqual(output.command, 'touchz')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('touchz dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_serverdefaults(self):
        parser = self.parser

        #no arg
        output = parser.parse('serverdefaults'.split())
        self.assertEqual(output.command, 'serverdefaults')

        #too many args
        with self.assertRaises(SystemExit):
            parser.parse('serverdefaults some_additional_argument'.split())

    def test_rmdir(self):
        parser = self.parser

        #no dir and/or no group
        with self.assertRaises(SystemExit):
            parser.parse('rmdir'.split())

        #one dir
        output = parser.parse('rmdir some_dir'.split())
        self.assertEqual(output.command, 'rmdir')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('rmdir dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_setrep(self):
        parser = self.parser

        #no dir and/or no replication factor
        with self.assertRaises(SystemExit):
            parser.parse('setrep'.split())
        with self.assertRaises(SystemExit):
            parser.parse('setrep some_dir'.split())
        with self.assertRaises(SystemExit):
            parser.parse('setrep 3'.split())

        #one dir
        output = parser.parse('setrep 3 some_dir'.split())
        self.assertEqual(output.command, 'setrep')
        self.assertEqual(output.dir, ['some_dir'])
        self.assertEqual(output.single_int_arg, 3)

        #wrong type for mode argument
        with self.assertRaises(SystemExit):
            parser.parse('setrep not_an_int some_dir'.split())

        #multiple dirs
        output = parser.parse('setrep 3 dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])
        self.assertEqual(output.single_int_arg, 3)

        #recursive
        output = parser.parse('setrep -R 3 some_dir'.split())
        self.assertTrue(output.recurse)

    def test_usage(self):
        parser = self.parser

        #no command
        output = parser.parse('usage'.split())
        self.assertEqual(output.command, 'usage')

        #one dir
        output = parser.parse('usage some_cmd'.split())
        self.assertEqual(output.command, 'usage')
        self.assertEqual(output.arg, ['some_cmd'])

        #multiple dirs
        output = parser.parse('usage cmd1 cmd2 cmd3'.split())
        self.assertEqual(output.arg, ['cmd1', 'cmd2', 'cmd3'])

    def test_stat(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('stat'.split())

        #one dir
        output = parser.parse('stat some_dir'.split())
        self.assertEqual(output.command, 'stat')
        self.assertEqual(output.dir, ['some_dir'])

        #multiple dirs
        output = parser.parse('stat dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

    def test_test(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('test'.split())

        #one dir
        output = parser.parse('test some_dir'.split())
        self.assertEqual(output.command, 'test')
        self.assertEqual(output.single_arg, 'some_dir')

        #multiple dirs
        with self.assertRaises(SystemExit):
            parser.parse('test dir1 dir2 dir3'.split())

        #specific commands
        output = parser.parse('test -d -z -e some_dir'.split())
        self.assertTrue(output.directory)
        self.assertTrue(output.zero)
        self.assertTrue(output.exists)
        self.assertEqual(output.single_arg, 'some_dir')
