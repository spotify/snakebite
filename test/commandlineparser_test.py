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
import json
import sys
import traceback

from mock import MagicMock, patch, mock_open

from snakebite.config import HDFSConfig
from snakebite.commandlineparser import Commands, CommandLineParser
from snakebite.namenode import Namenode

from config_test import ConfigTest

class CommandLineParserTest(unittest2.TestCase):

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

        output = parser.parse('-j ls some_folder'.split())
        self.assertTrue(output.json)
        output = parser.parse('--json ls some_folder'.split())
        self.assertTrue(output.json)

        output = parser.parse('-n namenode_fqdn ls some_folder'.split())  # what are typical values for namenodes?
        self.assertEqual(output.namenode, "namenode_fqdn")
        output = parser.parse('--namenode namenode_fqdn ls some_folder'.split())
        self.assertEqual(output.namenode, "namenode_fqdn")

        output = parser.parse('-p 1234 ls some_folder'.split())
        self.assertEqual(output.port, 1234)
        output = parser.parse('--port 1234 ls some_folder'.split())
        self.assertEqual(output.port, 1234)

        output = parser.parse('-V 4 ls some_folder'.split())
        self.assertEqual(output.version, 4)
        output = parser.parse('--version 4 ls some_folder'.split())
        self.assertEqual(output.version, 4)

        #all options
        output = parser.parse('-D -j -n namenode_fqdn -p 1234 -V 4 ls some_folder'.split())
        self.assertTrue(output.debug)
        self.assertTrue(output.json)
        self.assertEqual(output.namenode, "namenode_fqdn")
        self.assertEqual(output.port, 1234)
        self.assertEqual(output.version, 4)

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
        output = parser.parse('ls -d -R -s -h some_dir'.split())
        self.assertTrue(output.directory)
        self.assertTrue(output.recurse)
        self.assertTrue(output.summary)
        self.assertTrue(output.human)
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

        # Human output
        output = parser.parse('count -h dir1 dir2 dir3'.split())
        self.assertTrue(output.human)

    def test_df(self):
        parser = self.parser

        #no dir
        output = parser.parse('df'.split())
        self.assertEqual(output.command, 'df')

        # Human output
        output = parser.parse('df -h'.split())
        self.assertEqual(output.command, 'df')
        self.assertTrue(output.human)

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

        #human
        output = parser.parse('du -h some_dir'.split())
        self.assertTrue(output.human)

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

        #skiptrash
        output = parser.parse('rm -S some_dir'.split())
        self.assertTrue(output.skiptrash)

        #skiptrash
        output = parser.parse('rm --skiptrash some_dir'.split())
        self.assertTrue(output.skiptrash)

        #usetrash
        output = parser.parse('rm -T some_dir'.split())
        self.assertTrue(output.usetrash)

        #usetrash
        output =parser.parse('rm --usetrash some_dir'.split())
        self.assertTrue(output.usetrash)

        #usetrash & skiptrash
        output = parser.parse('rm --usetrash --skiptrash some_dir'.split())
        self.assertTrue(output.usetrash)
        self.assertTrue(output.skiptrash)

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

    def test_cat(self):
        parser = self.parser

        #no path
        with self.assertRaises(SystemExit):
            parser.parse('cat'.split())

        #one path
        output = parser.parse('cat some_file'.split())
        self.assertEqual(output.command, 'cat')
        self.assertEqual(output.dir, ['some_file'])

        #multiple paths
        output = parser.parse('cat dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

        #specific commands
        output = parser.parse('cat -checkcrc dir1 dir2'.split())
        self.assertEqual(output.checkcrc, True)

    def test_copyFromLocal(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('copyFromLocal'.split())

        #one dir
        with self.assertRaises(SystemExit):
            parser.parse('copyFromLocal some_dir'.split())

        #two dirs
        output = parser.parse('copyFromLocal dir1 dir2'.split())
        self.assertEqual(output.dir, ['dir1'])
        self.assertEqual(output.single_arg, 'dir2')

    def test_copyToLocal(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('copyToLocal'.split())

        #one dir
        with self.assertRaises(SystemExit):
            parser.parse('copyToLocal some_dir'.split())

        #two dirs
        output = parser.parse('copyToLocal dir1 dir2'.split())
        self.assertEqual(output.dir, ['dir1'])
        self.assertEqual(output.single_arg, 'dir2')
        self.assertEqual(output.checkcrc, False)

        #specific commands
        output = parser.parse('copyToLocal -checkcrc dir1 dir2'.split())
        self.assertEqual(output.checkcrc, True)

    def test_cp(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('cp'.split())

        #one dir
        with self.assertRaises(SystemExit):
            parser.parse('cp some_dir'.split())

        #multiple dirs
        output = parser.parse('cp dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2'])
        self.assertEqual(output.single_arg, 'dir3')

    def test_get(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('get'.split())

        #one dir
        with self.assertRaises(SystemExit):
            parser.parse('get some_dir'.split())

        #multiple dirs
        output = parser.parse('get dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2'])
        self.assertEqual(output.single_arg, 'dir3')

        #specific commands
        output = parser.parse('get -checkcrc dir1 dir2'.split())
        self.assertEqual(output.checkcrc, True)

    def test_getmerge(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('getmerge'.split())

        #one dir
        with self.assertRaises(SystemExit):
            parser.parse('getmerge some_dir'.split())

        #two dirs
        output = parser.parse('getmerge dir1 dir2'.split())
        self.assertEqual(output.src_dst[0], 'dir1')
        self.assertEqual(output.src_dst[1], 'dir2')

        #multiple dirs
        with self.assertRaises(SystemExit):
            parser.parse('getmerge dir1 dir2 dir3'.split())

    # def test_put(self):
    #     parser = self.parser

    #     #no dir
    #     with self.assertRaises(SystemExit):
    #         parser.parse('put'.split())

    #     #one dir
    #     with self.assertRaises(SystemExit):
    #         parser.parse('put some_dir'.split())

    #     #multiple dirs
    #     output = parser.parse('put dir1 dir2 dir3'.split())
    #     self.assertEqual(output.dir, ['dir1', 'dir2'])
    #     self.assertEqual(output.single_arg, 'dir3')

    def test_tail(self):
        parser = self.parser

        #no dir
        with self.assertRaises(SystemExit):
            parser.parse('tail'.split())

        #one dir
        output = parser.parse('tail some_dir'.split())
        self.assertEqual(output.single_arg, 'some_dir')

        #multiple dirs
        with self.assertRaises(SystemExit):
            parser.parse('tail dir1 dir2'.split())

        #specific commands
        output = parser.parse('tail -f some_dir'.split())
        self.assertTrue(output.append)

    def test_text(self):
        parser = self.parser

        #no path
        with self.assertRaises(SystemExit):
            parser.parse('text'.split())

        #one path
        output = parser.parse('text some_file'.split())
        self.assertEqual(output.command, 'text')
        self.assertEqual(output.dir, ['some_file'])

        #multiple paths
        output = parser.parse('text dir1 dir2 dir3'.split())
        self.assertEqual(output.dir, ['dir1', 'dir2', 'dir3'])

        #specific commands
        output = parser.parse('text -checkcrc dir1 dir2'.split())
        self.assertEqual(output.checkcrc, True)



class MockParseArgs(object):
    # dir is a list of directories
    def __init__(self, dir=[],
                single_arg=None,
                command=None,
                namenode=None,
                port=None,
                usetrash=False,
                skiptrash=False):
        self.dir = dir
        self.single_arg = single_arg
        self.command = command
        self.namenode = namenode
        self.port = port
        self.usetrash = usetrash
        self.skiptrash = skiptrash

    def __contains__(self, b):
        return b in self.__dict__

class CommandLineParserInternalConfigTest(unittest2.TestCase):
    def setUp(self):
        self.parser = CommandLineParser()
        self.default_dir = os.path.join("/user", pwd.getpwuid(os.getuid())[0])


    def assert_namenode_spec(self, host, port, version=None):
        self.assertEqual(self.parser.args.namenode, host)
        self.assertEqual(self.parser.args.port, port)
        if version:
            self.assertEqual(self.parser.args.version, version)

    def assert_namenodes_spec(self, host, port, version=None):
        for namenode in self.parser.namenodes:
            try:
                self.assertEqual(namenode.host, host)
                self.assertEqual(namenode.port, port)
                if version:
                    self.assertEqual(namenode.version, version)
            except AssertionError:
                continue
            # There was no AssertError -> we found our NN
            return
        self.fail("NN not found in namenodes")


    def test_cl_config_conflicted(self):

        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav",
                                              "hdfs://foobar2:50070/user/rav"])
        with self.assertRaises(SystemExit):
            self.parser.read_config()


        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50071/user/rav",
                                              "hdfs://foobar:50070/user/rav"])
        with self.assertRaises(SystemExit):
            self.parser.read_config()

        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50072/user/rav",
                                              "hdfs://foobar2:50070/user/rav"])
        with self.assertRaises(SystemExit):
            self.parser.read_config()

        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav",
                                              "hdfs://foobar:50070/user/rav"],
                                         single_arg="hdfs://foobar2:50070/user/rav",
                                         command="mv")
        with self.assertRaises(SystemExit):
            self.parser.read_config()

    def test_cl_config_simple(self):
        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav",
                                              "hdfs://foobar:50070/user/rav2"])

        self.parser.read_config()
        self.assert_namenode_spec("foobar", 50070)
        self.assert_namenodes_spec("foobar", 50070)

        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav",
                                              "hdfs://foobar:50070/user/rav2"],
                                         single_arg="hdfs://foobar:50070/user/rav",
                                         command="mv")
        self.parser.read_config()
        self.assert_namenode_spec("foobar", 50070)
        self.assert_namenodes_spec("foobar", 50070)

    def test_cl_config_reduce_paths(self):
        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav",
                                              "hdfs://foobar:50070/user/rav2"],
                                         single_arg="hdfs://foobar:50070/user/rav3",
                                         command="mv")
        self.parser.init()
        self.assert_namenode_spec("foobar", 50070)
        self.assertIn("/user/rav", self.parser.args.dir)
        self.assertIn("/user/rav2", self.parser.args.dir)
        self.assertEqual(self.parser.args.single_arg, "/user/rav3")

    import snakebite.config
    @patch.object(snakebite.config.HDFSConfig, 'get_external_config')
    @patch("snakebite.commandlineparser.CommandLineParser._read_config_snakebiterc", return_value=None)
    def test_config_no_config(self, config_mock, read_config_mock):
        hadoop_home = None
        config_mock.return_value = []
        if os.environ.get("HADOOP_HOME"):
            hadoop_home = os.environ["HADOOP_HOME"]
            del os.environ["HADOOP_HOME"]
        self.parser.args = MockParseArgs()
        with self.assertRaises(SystemExit):
            self.parser.read_config()

        if hadoop_home:
            os.environ["HADOOP_HOME"] = hadoop_home

        self.assert_namenode_spec(None, None)


    valid_snake_one_rc = {"namenode": "foobar", "version": 9, "port": 54310}
    valid_snake_ha_rc = [{"namenode": "foobar", "version": 9, "port": 54310},
                         {"namenode": "foobar2", "version": 9, "port": 54310}]

    invalid_snake_rc = "hdfs://foobar:54310"

    @patch("os.path.exists")
    def test_read_config_snakebiterc_one_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_one_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", 54310, 9)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_ha_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_ha_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", 54310, 9)
            self.assert_namenodes_spec("foobar2", 54310, 9)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_invalid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.invalid_snake_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            with self.assertRaises(SystemExit):
                self.parser.read_config()

    valid_snake_noport_one_rc = {"namenode": "foobar", "version": 11}
    valid_snake_noport_ha_rc = [{"namenode": "foobar", "version": 100},
                                {"namenode": "foobar2", "version": 100}]

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_one_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_one_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", Namenode.DEFAULT_PORT, 11)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_ha_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_ha_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", Namenode.DEFAULT_PORT, 100)
            self.assert_namenodes_spec("foobar2", Namenode.DEFAULT_PORT, 100)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)


    valid_snake_noport_nov_one_rc = {"namenode": "foobar"}
    valid_snake_noport_nov_ha_rc = [{"namenode": "foobar"},
                                    {"namenode": "foobar2"}]

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_nov_one_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_nov_one_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", Namenode.DEFAULT_PORT, Namenode.DEFAULT_VERSION)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_nov_ha_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_nov_ha_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", Namenode.DEFAULT_PORT, Namenode.DEFAULT_VERSION)
            self.assert_namenodes_spec("foobar2", Namenode.DEFAULT_PORT, Namenode.DEFAULT_VERSION)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    valid_snake_noport_mix_rc = [{"namenode": "foobar", "version": 100},
                                 {"namenode": "foobar2", "port": 66}]

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_mix_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_mix_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", Namenode.DEFAULT_PORT, 100)
            self.assert_namenodes_spec("foobar2", 66, Namenode.DEFAULT_VERSION)
            self.assertEquals(self.parser.args.usetrash, HDFSConfig.use_trash)

    valid_snake_one_rc_v2 = {
                                "config_version": 2,
                                "use_trash": False,
                                "namenodes": [
                                    {"host": "foobar3", "version": 9, "port": 54310}
                                ]
                            }

    valid_snake_ha_rc_v2 = {
                                "config_version": 2,
                                "use_trash": True,
                                "namenodes": [
                                    {"host": "foobar4", "version": 9, "port": 54310},
                                    {"host": "foobar5", "version": 9, "port": 54310}
                                ]
                            }

    invalid_snake_rc_v2 = "hdfs://foobar:54310"

    @patch("os.path.exists")
    def test_read_config_snakebiterc_one_valid_v2(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_one_rc_v2))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assertFalse(self.parser.args.usetrash)
            self.assert_namenodes_spec("foobar3", 54310, 9)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_ha_valid_v2(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_ha_rc_v2))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assertTrue(self.parser.args.usetrash)
            self.assert_namenodes_spec("foobar4", 54310, 9)
            self.assert_namenodes_spec("foobar5", 54310, 9)


    @patch("os.path.exists")
    def test_read_config_snakebiterc_invalid_v2(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.invalid_snake_rc_v2))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            with self.assertRaises(SystemExit):
                self.parser.read_config()


    valid_snake_noport_one_rc_v2 = {
                                    "config_version": 2,
                                    "use_trash": False,
                                    "namenodes": [
                                        {"host": "foobar3", "version": 9}
                                    ]
                                   }

    valid_snake_mix_ha_rc_v2 = {
                                   "config_version": 2,
                                   "use_trash": True,
                                   "namenodes": [
                                        {"host": "foobar4", "version": 100},
                                        {"host": "foobar5", "port": 54310}
                                    ]
                                  }

    @patch("os.path.exists")
    def test_read_config_snakebiterc_noport_one_valid_v2(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_noport_one_rc_v2))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assertFalse(self.parser.args.usetrash)
            self.assert_namenodes_spec("foobar3", Namenode.DEFAULT_PORT, 9)

    @patch("os.path.exists")
    def test_read_config_snakebiterc_mix_ha_valid_v2(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_mix_ha_rc_v2))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs()
            self.parser.read_config()
            self.assertTrue(self.parser.args.usetrash)
            self.assert_namenodes_spec("foobar4", Namenode.DEFAULT_PORT, 100)
            self.assert_namenodes_spec("foobar5", 54310, Namenode.DEFAULT_VERSION)


    def test_cl_default_port(self):
        self.parser.args = MockParseArgs(dir=["hdfs://foobar/user/rav"],
                                         single_arg="hdfs://foobar/user/rav",
                                         command="mv")
        self.parser.read_config()
        self.assert_namenode_spec("foobar", Namenode.DEFAULT_PORT)

    def test_cl_trash_setting_preserved_after_cl_config(self):
        # no snakebiterc
        # read config from CL
        self.parser.args = MockParseArgs(dir=["hdfs://foobar:50070/user/rav"],
                                         skiptrash=True,
                                         command="rm")
        self.parser.read_config()
        self.assert_namenode_spec("foobar", 50070)
        self.assert_namenodes_spec("foobar", 50070)
        self.assertEquals(self.parser.args.skiptrash, True)

    def _revert_hdfs_try_paths(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = ConfigTest.original_hdfs_try_path
        HDFSConfig.core_try_paths = ConfigTest.original_core_try_path

    @patch("os.path.exists")
    def test_cl_trash_setting_preserved_after_snakebiterc_one_valid(self, exists_mock):
        m = mock_open(read_data=json.dumps(self.valid_snake_one_rc))

        with patch("snakebite.commandlineparser.open", m, create=True):
            self.parser.args = MockParseArgs(usetrash=True)
            self.parser.read_config()
            self.assert_namenodes_spec("foobar", 54310, 9)
            self.assertTrue(self.parser.args.usetrash)


    @patch('os.environ.get')
    def test_cl_usetrash_setting_preserved_after_external_nontrash_config(self, environ_get):
        environ_get.return_value = False
        # no snakebiterc
        # read external config (hdfs-site, core-site)
        self.parser.args = MockParseArgs(dir=["/user/rav/test"],
                                         usetrash=True,
                                         command="rm")
        try:
            HDFSConfig.core_try_paths = (ConfigTest.get_config_path('ha-core-site.xml'),)
            HDFSConfig.hdfs_try_paths = (ConfigTest.get_config_path('ha-noport-hdfs-site.xml'),)
            self.parser.init()

            self.assertTrue(self.parser.args.usetrash)
            self.assertTrue(self.parser.client.use_trash)
        finally:
            self._revert_hdfs_try_paths()

    @patch('os.environ.get')
    def test_cl_skiptrash_setting_preserved_after_external_nontrash_config(self, environ_get):
        environ_get.return_value = False
        # no snakebiterc
        # read external config (hdfs-site, core-site)
        self.parser.args = MockParseArgs(dir=["/user/rav/test"],
                                         skiptrash=True,
                                         usetrash=True,
                                         command="rm")
        try:
            HDFSConfig.core_try_paths = (ConfigTest.get_config_path('ha-core-site.xml'),)
            HDFSConfig.hdfs_try_paths = (ConfigTest.get_config_path('ha-noport-hdfs-site.xml'),)
            self.parser.init()

            self.assertTrue(self.parser.args.skiptrash)
            self.assertTrue(self.parser.args.usetrash)
            self.assertFalse(self.parser.client.use_trash)
        finally:
            self._revert_hdfs_try_paths()



class CommandLineParserExecuteTest(unittest2.TestCase):
    def test_execute_does_not_swallow_tracebacks(self):
        with patch.dict(Commands.methods, clear=True):
            @CommandLineParser.command.im_func()
            def boom(*args, **kwargs):
                def subboom():
                    raise IndexError("Boom!")
                subboom()

            parser = CommandLineParser()
            parser.parse(["boom"])

            try:
                parser.execute()
            except IndexError:
                _, _, exc_traceback = sys.exc_info()
                self.assertIn(
                    "subboom()\n",
                    traceback.format_exc(),
                    msg="Lost some stack frames when re-raising!",
                )
            else:
                self.fail("execute() should have raised an IndexError!")
