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
from util import assertListings
from minicluster_testbase import MiniClusterTestBase
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException


class ListTest(MiniClusterTestBase):

    def test_toplevel_root(self):
        expected_output = self.cluster.ls(['/'])
        client_output = list(self.client.ls(['/']))
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_toplevel_dir(self):
        client_output = list(self.client.ls(['/dir1'], include_toplevel=True, include_children=False))
        self.assertEqual(len(client_output), 1)
        self.assertEqual(client_output[0]['file_type'], 'd')
        self.assertEqual(client_output[0]['length'], 0)

    def test_zerofile(self):
        client_output = list(self.client.ls(['/zerofile'], include_toplevel=True, include_children=False))
        self.assertEqual(len(client_output), 1)
        self.assertEqual(client_output[0]['file_type'], 'f')
        self.assertEqual(client_output[0]['length'], 0)

    def test_file_even_if_toplevel_is_false(self):
        client_output = list(self.client.ls(['/zerofile']))
        self.assertEqual(len(client_output), 1)
        self.assertEqual(client_output[0]['file_type'], 'f')
        self.assertEqual(client_output[0]['length'], 0)

    def test_root_incl_toplevel(self):
        expected_output = self.cluster.ls(['/'])
        client_output = list(self.client.ls(['/'], include_toplevel=True, include_children=True))
        self.assertEqual(len(client_output), len(expected_output) + 1)

    def test_root_recursive(self):
        expected_output = self.cluster.ls(['/'], ['-R'])
        client_output = list(self.client.ls(['/'], include_toplevel=False, recurse=True))
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_multiple_files(self):
        client_output = self.client.ls(['/zerofile', '/dir1'], include_toplevel=True, include_children=False)
        client_output = sorted(client_output, key=lambda node: node['path'])
        self.assertEqual(len(client_output), 2)
        self.assertEqual(client_output[0]['path'], '/dir1')
        self.assertEqual(client_output[1]['path'], '/zerofile')

    def test_glob(self):
        expected_output = self.cluster.ls(['/b*'])
        client_output = list(self.client.ls(['/b*']))
        self.assertTrue(len(client_output) > 1)
        self.assertTrue(len(expected_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/{foo,bar}'])
        client_output = list(self.client.ls(['/{foo,bar}']))
        self.assertTrue(len(client_output) > 1)
        self.assertTrue(len(expected_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/[fb]*/*/*/qux'])
        client_output = list(self.client.ls(['/[fb]*/*/*/qux']))
        self.assertTrue(len(client_output) > 1)
        self.assertTrue(len(expected_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/{foo,bar}/*/*/qux'])
        client_output = list(self.client.ls(['/{foo,bar}/*/*/qux']))
        self.assertTrue(len(client_output) > 1)
        self.assertTrue(len(expected_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/log/service/2013-05-17/*/Message10'])
        client_output = list(self.client.ls(['/log/service/2013-05-17/*/Message10']))
        self.assertTrue(len(expected_output) > 1)
        self.assertTrue(len(client_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/log/service/2013-05-17/*/Message10/'])
        client_output = list(self.client.ls(['/log/service/2013-05-17/*/Message10/']))
        self.assertTrue(len(expected_output) > 1)
        self.assertTrue(len(client_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

        expected_output = self.cluster.ls(['/log/service/2013-05-17/*/Message10/*'])
        client_output = list(self.client.ls(['/log/service/2013-05-17/*/Message10/*']))
        self.assertTrue(len(expected_output) > 1)
        self.assertTrue(len(client_output) > 1)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_path_cleanup(self):
        expected_output = self.cluster.ls(['/log/service/..'])
        client_output = list(self.client.ls(['/log/service/..']))
        self.assertTrue(len(expected_output) > 0)
        self.assertTrue(len(client_output) > 0)
        assertListings(expected_output, client_output, self.assertEqual, self.assertEqual)

    def test_unknown_file(self):
        result = self.client.ls(['/doesnotexist'])
        self.assertRaises(FileNotFoundException, result.next)

    def test_invalid_input(self):
        result = self.client.ls('/stringpath')
        self.assertRaises(InvalidInputException, result.next)
