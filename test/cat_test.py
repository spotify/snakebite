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


class CatTest(MiniClusterTestBase):

    def test_cat_file_on_1_block(self):  # Size < 1 block
        expected_output = self.cluster.cat('/test3')
        client_output = list(self.client.cat(['/test3']))[0]
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_2_blocks(self):  # 1 < size < 2 blocks
        self._write_to_test_cluster('/test1', 200, '/temp_test')  # 677,972 * 200 = 135,594,400 bytes

        client_output = list(self.client.cat(['/temp_test']))[0]
        expected_output = self.cluster.cat('/temp_test')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_3_blocks(self):  # 2 < size < 3 blocks
        self._write_to_test_cluster('/test1', 400, '/temp_test2')  # 677,972 * 400 = 271,188,800 bytes

        client_output = list(self.client.cat(['/temp_test2']))[0]
        expected_output = self.cluster.cat('/temp_test2')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_exactly_1_block(self):  # Size == 1 block
        self._write_to_test_cluster('/test3', 131072, '/temp_test3')  # 1024 * 131072 = 134,217,728 (default block size)

        client_output = list(self.client.cat(['/temp_test3']))[0]
        expected_output = self.cluster.cat('/temp_test3')
        self.assertEqual(expected_output, client_output)

    def _write_to_test_cluster(self, testfile, times, dst):
        testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
        f = open(''.join([testfiles_path, testfile]))
        p = self.cluster.put_subprocess('-', dst)
        for _ in xrange(times):
            f.seek(0)
            for line in f.readlines():
                print >> p.stdin, line
        p.communicate()
