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
        client_output = ''
        for file_to_read in self.client.cat(['/test3']):
            for data in file_to_read:
                client_output += data
        expected_output = self.cluster.cat('/test3')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_1_block_checkcrc(self):  # Size < 1 block
        client_output = ''
        for file_to_read in self.client.cat(['/test3'], check_crc=True):
            for data in file_to_read:
                client_output += data
        expected_output = self.cluster.cat('/test3')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_2_blocks(self):  # 1 < size < 2 blocks
        self._write_to_test_cluster('/test1', 10, '/temp_test', 4 * 1024 * 1024)
        # 6.77972 MB of test data, with 4MB block size gives 2 blocks

        client_output = ''
        for file_to_read in self.client.cat(['/temp_test']):
            for data in file_to_read:
                client_output += data
        expected_output = self.cluster.cat('/temp_test')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_3_blocks(self):  # 2 < size < 3 blocks
        # to limit size on test data, let's change default block size
        self._write_to_test_cluster('/test1', 10, '/temp_test2', 3 * 1024 * 1024)
        # size of the test data will be 677,972 * 10 = 6.77972 MB, and with
        #block size 3 MB, it gives as 3 blocks

        client_output = ''
        for file_to_read in self.client.cat(['/temp_test2']):
            for data in file_to_read:
                client_output += data
        expected_output = self.cluster.cat('/temp_test2')
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_exactly_1_block(self):  # Size == 1 block
        self._write_to_test_cluster('/test3', 1024, '/temp_test3', 1 * 1024 * 1024)
        # test3 is 1024 bytes, write it 1024 times to get 1MB of test data
        # set block size to 1MB to get exactly one block

        client_output = ''
        for file_to_read in self.client.cat(['/temp_test3']):
            for data in file_to_read:
                client_output += data
        expected_output = self.cluster.cat('/temp_test3')
        self.assertEqual(expected_output, client_output)

    def _write_to_test_cluster(self, testfile, times, dst, block_size=134217728):
        testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
        f = open(''.join([testfiles_path, testfile]))
        p = self.cluster.put_subprocess('-', dst, block_size)
        for _ in xrange(times):
            f.seek(0)
            for line in f.readlines():
                print >> p.stdin, line
        p.communicate()
