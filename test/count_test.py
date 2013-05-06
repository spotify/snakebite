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
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException
from minicluster_testbase import MiniClusterTestBase


class CountTest(MiniClusterTestBase):
    def test_count_path(self):
        client_output = sorted(self.client.count(["/"]), key=lambda node: node['path'])
        expected_output = sorted(self.cluster.count(["/"]), key=lambda node: node['path'])
        self.assertEqual(len(client_output), len(client_output))
        for i, expected_node in enumerate(expected_output):
            client_node = client_output[i]
            for key in ['path', 'length', 'directoryCount', 'fileCount']:
                self.assertEqual(client_node[key], expected_node[key])

    def test_count_multi(self):
        client_output = sorted(self.client.count(["/", "/dir1"]), key=lambda node: node['path'])
        expected_output = sorted(self.cluster.count(["/", "/dir1"]), key=lambda node: node['path'])
        self.assertEqual(len(client_output), len(client_output))
        for i, expected_node in enumerate(expected_output):
            client_node = client_output[i]
            for key in ['path', 'length', 'directoryCount', 'fileCount']:
                self.assertEqual(client_node[key], expected_node[key])

    def test_unknown_file(self):
        result = self.client.count(['/doesnotexist'])
        self.assertRaises(FileNotFoundException, result.next)

    def test_invalid_input(self):
        result = self.client.count('/stringpath')
        self.assertRaises(InvalidInputException, result.next)
