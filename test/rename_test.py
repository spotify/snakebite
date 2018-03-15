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
from __future__ import print_function
from __future__ import absolute_import
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException
from .minicluster_testbase import MiniClusterTestBase


class RenameTest(MiniClusterTestBase):
    def test_rename_file(self):
        print(list(self.client.rename(['/zerofile'], '/zerofile2')))
        expected_output = list(self.client.ls(['/zerofile2'], include_toplevel=True))
        self.assertEqual(len(expected_output), 1)
        self.assertEqual(expected_output[0]['path'], '/zerofile2')
        result = self.client.ls(['/zerofile'])
        self.assertRaises(FileNotFoundException, result.next)

    def test_rename_multi(self):
        list(self.client.rename(['/test1', '/test2'], '/dir1'))
        expected_output = self.client.ls(['/dir1'])
        paths = set([node["path"] for node in expected_output])
        for path in ['/dir1/test1', '/dir1/test2']:
            self.assertTrue(path in paths)

    def test_unknown_file(self):
        result = self.client.rename(['/doesnotexist'], '/somewhereelse')
        self.assertRaises(FileNotFoundException, result.next)

    def test_invalid_input(self):
        result = self.client.rename('/stringpath', '777')
        self.assertRaises(InvalidInputException, result.next)

    def test_rename_multi_with_trailing_slash(self):
       list(self.client.rename(['/test3', '/test4'], '/dir1/'))
       expected_output = self.client.ls(['/dir1'])
       paths = set([node["path"] for node in expected_output])
       for path in ['/dir1/test3', '/dir1/test4']:
           self.assertTrue(path in paths)
