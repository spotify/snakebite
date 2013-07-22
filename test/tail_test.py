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


class TailTest(MiniClusterTestBase):
    def test_tail_on_one_block(self):
        expected_output = self.cluster.tail('/test1')
        client_output = list(self.client.tail('/test1'))[0]
        self.assertEqual(expected_output, client_output)

    def test_tail_over_two_blocks(self):
        expected_output = self.cluster.tail('/test4')
        client_output = list(self.client.tail('/test4'))[0]
        self.assertEqual(expected_output, client_output)

    def test_tail_on_file_smaller_than_1KB(self):
        expected_output = self.cluster.tail('/test3')
        client_output = list(self.client.tail('/test3'))[0]
        self.assertEqual(expected_output, client_output)
