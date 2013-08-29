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


class TailTest(MiniClusterTestBase):

    def test_tail_on_one_block(self):
        expected_output = self.cluster.tail('/test1')
        client_output = list(self.client.tail('/test1'))[0]
        self.assertEqual(expected_output, client_output)

    def test_tail_on_file_smaller_than_1KB(self):
        p = self.cluster.put_subprocess('-', '/temp_test')
        print >> p.stdin, "just a couple of bytes"
        p.communicate()

        expected_output = self.cluster.tail('/temp_test')
        client_output = list(self.client.tail('/temp_test'))[0]
        self.assertEqual(expected_output, client_output)

    def test_tail_over_two_blocks(self):  # Last KB of file spans 2 blocks.
        testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
        f = open('%s/test3' % testfiles_path)

        p = self.cluster.put_subprocess('-', '/temp_test2')
        for _ in xrange(131072):  # 1024 * 131072 = 134,217,728 (default block size)
            f.seek(0)
            for line in f.readlines():
                print >> p.stdin, line
        print >> p.stdin, "some extra bytes to exceed one blocksize"  # +40
        p.communicate()

        expected_output = self.cluster.tail('/temp_test2')
        client_output = list(self.client.tail('/temp_test2'))[0]
        self.assertEqual(expected_output, client_output)
