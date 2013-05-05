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
from spotify.snakebite.errors import FileNotFoundException
from spotify.snakebite.errors import InvalidInputException
from minicluster_testbase import MiniClusterTestBase
from util import assertDu


class DfTest(MiniClusterTestBase):

    def test_onepath(self):
        client_output = sorted(self.client.du(['/']), key=lambda node: node['path'])
        expected_output = sorted(self.cluster.du('/'), key=lambda node: node['path'])
        assertDu(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_multipath(self):
        client_output = sorted(self.client.du(['/', '/dir1']), key=lambda node: node['path'])
        expected_output = sorted(self.cluster.du(['/', '/dir1']), key=lambda node: node['path'])
        assertDu(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_toplevel(self):
        client_output = sorted(self.client.du(['/'], include_toplevel=True, include_children=False), key=lambda node: node['path'])
        expected_output = sorted(self.cluster.du(['/'], ['-s']), key=lambda node: node['path'])
        self.assertEqual(len(client_output), 1)
        assertDu(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_unknown_file(self):
        self.assertRaises(FileNotFoundException, self.client.du, ['/nonexistent'])

    def test_invalid_input(self):
        self.assertRaises(InvalidInputException, self.client.du, '/stringpath')
