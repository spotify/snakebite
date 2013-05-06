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


class DeleteTest(MiniClusterTestBase):
    def test_delete_file(self):
        before_state = set([node['path'] for node in self.client.ls(['/'])])
        self.client.delete(['/zerofile'])
        after_state = set([node['path'] for node in self.client.ls(['/'])])
        self.assertEqual(len(after_state), len(before_state) - 1)
        self.assertFalse('/zerofile' in after_state)

    def test_delete_multi(self):
        before_state = set([node['path'] for node in self.client.ls(['/'])])
        self.client.delete(['/test1', '/test2'])
        after_state = set([node['path'] for node in self.client.ls(['/'])])
        self.assertEqual(len(after_state), len(before_state) - 2)
        self.assertFalse('/test1' in after_state or '/test2' in after_state)

    def test_unknown_file(self):
        self.assertRaises(FileNotFoundException, self.client.delete, ['/doesnotexist'])

    def test_invalid_input(self):
        self.assertRaises(InvalidInputException, self.client.delete, '/stringpath')

    def test_recurse(self):
        self.client.delete(['/foo'], recurse=True)
        client_output = self.client.ls(['/'])
        paths = [node['path'] for node in client_output]
        self.assertFalse('/foo' in paths)

    def test_glob(self):
        self.client.delete(['/ba*'], recurse=True)
        client_output = self.client.ls(['/'])
        paths = [node['path'] for node in client_output]
        self.assertFalse('/bar' in paths)
