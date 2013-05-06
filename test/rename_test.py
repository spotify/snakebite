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


class RenameTest(MiniClusterTestBase):
    def test_rename_file(self):
        self.client.rename(['/zerofile'], '/zerofile2')
        expected_output = self.client.ls(['/zerofile2'], include_toplevel=True)
        self.assertEqual(len(expected_output), 1)
        self.assertEqual(expected_output[0]['path'], '/zerofile2')
        self.assertRaises(FileNotFoundException, self.client.ls, ['/zerofile'])

    def test_rename_multi(self):
        self.client.rename(['/test1', '/test2'], '/dir1')
        expected_output = self.client.ls(['/dir1'])
        paths = set([node["path"] for node in expected_output])
        for path in ['/dir1/test1', '/dir1/test2']:
            self.assertTrue(path in paths)

    def test_unknown_file(self):
        self.assertRaises(FileNotFoundException, self.client.rename, ['/doesnotexist'], '/somewhereelse')

    def test_invalid_input(self):
            self.assertRaises(InvalidInputException, self.client.rename, '/stringpath', '777')
