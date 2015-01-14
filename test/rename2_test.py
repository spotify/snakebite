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
from snakebite.errors import FileAlreadyExistsException
from minicluster_testbase import MiniClusterTestBase


class Rename2Test(MiniClusterTestBase):

    def setUp(self):
        super(Rename2Test, self).setUp()
        list(self.client.delete(['*'], recurse=True))

    def test_rename2(self):
        list(self.client.mkdir(['a', 'b'], create_parent=True))
        list(self.client.rename2('a', 'dest'))
        move_b = lambda: list(self.client.rename2('b', 'dest'))
        self.assertRaises(FileAlreadyExistsException, move_b)

    def test_never_overwrite_nonempty(self):
        list(self.client.mkdir(['a', 'b'], create_parent=True))
        list(self.client.touchz(['a/file_1', 'b/file_2']))
        list(self.client.rename2('a', 'dest'))
        move_b = lambda: list(self.client.rename2('b', 'dest', overwriteDest=True))
        self.assertRaises(FileAlreadyExistsException, move_b)

    def test_can_overwrite_empty(self):
        list(self.client.mkdir(['a', 'b'], create_parent=True))
        list(self.client.rename2('a', 'dest'))
        list(self.client.rename2('b', 'dest', overwriteDest=True))

    def test_with_files(self):
        list(self.client.touchz(['x', 'y']))
        move_x = lambda: list(self.client.rename2('x', 'y'))
        self.assertRaises(FileAlreadyExistsException, move_x)
        list(self.client.rename2('x', 'y', overwriteDest=True))
