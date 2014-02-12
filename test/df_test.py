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
import re
import unittest2

from minicluster_testbase import MiniClusterTestBase

from snakebite.formatter import format_fs_stats

class DfTest(MiniClusterTestBase):

    def test_df(self):
        client_output = self.client.df()
        expected_output = self.cluster.df("/").split("\n")[1]

        (filesystem, capacity, used, remaining, pct) = re.split("\s+", expected_output)

        self.assertEqual(filesystem, client_output["filesystem"])
        self.assertEqual(long(capacity), client_output["capacity"])
        self.assertEqual(long(used), client_output["used"])

class StatsMock(dict):
    def __init__(self, capacity,
            used,
            remaining,
            under_replicated,
            corrupted_blocks,
            missing_blocks,
            filesystem):
        super(StatsMock, self).__init__({"capacity": capacity,
            "used":     used,
            "remaining": remaining,
            "under_replicated": under_replicated,
            "corrupted_blocks": corrupted_blocks,
            "missing_blocks": missing_blocks,
            "filesystem": filesystem})

class DfFormatTest(unittest2.TestCase):
  
    def test_middle(self):
        fake = StatsMock(100, 50, 50, 0, 0, 0, "foobar.com")
        output = format_fs_stats(fake).next().split('\n')
        stats = output[1].split()
        self.assertEqual(stats[4], "50.00%")
        self.assertEqual(stats[0], "foobar.com")

    def test_frag(self):
        fake = StatsMock(312432, 23423, 289009, 0, 0, 0, "foobar.com")
        output = format_fs_stats(fake).next().split('\n')
        stats = output[1].split()
        self.assertEqual(stats[4], "7.50%")
 
    def test_zero_size(self):
        fake = StatsMock(0, 0, 0, 0, 0, 0, "foobar.com")
        output = format_fs_stats(fake).next().split('\n')
        stats = output[1].split()
        self.assertEqual(stats[4], "0.00%")

    def test_corrupted_zero_size(self):
        fake = StatsMock(0, 50, 50, 0, 0, 0, "foobar.com")
        output = format_fs_stats(fake).next().split('\n')
        stats = output[1].split()
        self.assertEqual(stats[4], "0.00%")

    def test_full_size(self):
        fake = StatsMock(50, 50, 0, 0, 0, 0, "foobar.com")
        output = format_fs_stats(fake).next().split('\n')
        stats = output[1].split()
        self.assertEqual(stats[4], "100.00%")


 
