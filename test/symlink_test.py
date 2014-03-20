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


class SymlinkTest(MiniClusterTestBase):
    def test_create_symlink(self):
        self.client.symlink('/zerofile', '/zerofile2')
        file_result = list(self.client.ls(['/zerofile']))
        self.assertEqual(len(file_result), 1)
        link_result = list(self.client.ls(['/zerofile2']))
        self.assertEqual(len(link_result), 1)

    def test_create_symlink_dir(self):
        self.client.symlink('/dir2', '/dir2link')
        path_result = list(self.client.ls(['/dir2']))
        self.assertEqual(len(path_result), 1)
        self.assertTrue('/dir2/dir3' in [node['path'] for node in path_result])
        link_result = list(self.client.ls(['/dir2link']))
        self.assertEqual(len(link_result), 1)
        self.assertTrue('/dir2/dir3' in [node['path'] for node in link_result])
