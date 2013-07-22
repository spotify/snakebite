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
from minicluster_testbase import MiniClusterTestBase

import os
import shutil


class GetmergeTest(MiniClusterTestBase):
    def tearDown(self):
        shutil.rmtree('test/testfiles/temp_testfiles')

    def test_getmerge_file(self):
        os.mkdir('test/testfiles/temp_testfiles')
        self.cluster.getmerge('/test3', 'test/testfiles/temp_testfiles/expected')
        expected_output = self._read_file('test/testfiles/temp_testfiles/expected')
        client_output = self.client.getmerge('/test3', 'test/testfiles/temp_testfiles/client')
        self.assertEqual(expected_output, client_output)

    def test_getmerge_directory(self):
        os.mkdir('test/testfiles/temp_testfiles')
        self.cluster.getmerge('/dir2/dir3', 'test/testfiles/temp_testfiles/expected')
        expected_output = self._read_file('test/testfiles/temp_testfiles/expected')
        client_output = self.client.getmerge('/dir2/dir3', 'test/testfiles/temp_testfiles/client')
        self.assertEqual(expected_output, client_output)

    def test_getmerge_directory_nl(self):
        os.mkdir('test/testfiles/temp_testfiles')
        self.cluster.getmerge('/dir2/dir3', 'test/testfiles/temp_testfiles/expected', extra_args=['-nl'])
        expected_output = self._read_file('test/testfiles/temp_testfiles/expected')
        client_output = self.client.getmerge('/dir2/dir3', 'test/testfiles/temp_testfiles/client', nl=True)
        self.assertEqual(expected_output, client_output)

    def test_getmerge_directory_tree(self):
        # Should only merge files in the specified directory level (don't recurse)
        os.mkdir('test/testfiles/temp_testfiles')
        self.cluster.getmerge('/dir2', 'test/testfiles/temp_testfiles/expected')
        expected_output = self._read_file('test/testfiles/temp_testfiles/expected')
        client_output = self.client.getmerge('/dir2', 'test/testfiles/temp_testfiles/client')
        self.assertEqual(expected_output, client_output)

    def _read_file(self, file):
        f = open(file, 'r')
        data = f.read()
        f.close()
        return data
