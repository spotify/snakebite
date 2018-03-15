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
from .minicluster_testbase import MiniClusterTestBase
import os
import random
from six.moves import xrange


class TailTest(MiniClusterTestBase):

    # Test cases

    def test_tail_on_one_block(self):
        self._compare_files('/test1')

    def test_tail_on_file_smaller_than_1KB(self):
        path = '/temp_test'
        p = self.cluster.put_subprocess('-', path)
        print("just a couple of bytes", file=p.stdin)
        p.communicate()

        self._compare_files(path)

    def test_tail_over_two_blocks(self):  # Last KB of file spans 2 blocks.
        path = '/temp_test2'
        self._generate_file_over_two_blocks(path)
        self._compare_files(path)

    def test_with_tail_length(self):
        self._compare_files('/test1', True)

    def test_with_tail_length_over_two_blocks(self):  # Last KB of file spans 2 blocks.
        path = '/temp_test3'
        self._generate_file_over_two_blocks(path)
        self._compare_files(path, True, 40)

    # Helper Methods:

    def _compare_files(self, path, random_tail = False, minimal_tail_length = 1):
        output = self.cluster.tail(path)
        tail_length = 1024  # The default tail length

        if random_tail:
            tail_length = random.randint(minimal_tail_length, len(output))

        expected_output = output[-1 * tail_length:]
        client_output = list(self.client.tail(path, tail_length))[0]
        self.assertEqual(expected_output, client_output)

    def _generate_file_over_two_blocks(self, path):
        f = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'testfiles', 'test3'))

        p = self.cluster.put_subprocess('-', path)
        for _ in xrange(131072):  # 1024 * 131072 = 134,217,728 (default block size)
            f.seek(0)
            for line in f.readlines():
                print(line, file=p.stdin)
        print('some extra bytes to exceed one blocksize', file=p.stdin)  # +40
        p.communicate()
