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


class CatTest(MiniClusterTestBase):
    def test_cat_file_on_1_block(self):  # Size < 1 block
        expected_output = self.cluster.cat('/test3')
        client_output = list(self.client.cat(['/test3']))[0]
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_2_blocks(self):  # 1 < size < 2 blocks
        expected_output = self.cluster.cat('/test4')
        client_output = list(self.client.cat(['/test4']))[0]
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_3_blocks(self):  # 2 < size < 3 blocks
        expected_output = self.cluster.cat('/test5')
        client_output = list(self.client.cat(['/test5']))[0]
        self.assertEqual(expected_output, client_output)

    def test_cat_file_on_exactly_1_block(self):  # 2 < size < 3 blocks
        expected_output = self.cluster.cat('/test6')
        client_output = list(self.client.cat(['/test6']))[0]
        self.assertEqual(expected_output, client_output)
