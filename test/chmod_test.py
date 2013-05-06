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
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException


class ChmodTest(MiniClusterTestBase):

    def test_onepath(self):
        self.client.chmod(['/dir1'], 0777)
        client_output = self.client.ls(['/dir1'], include_toplevel=True, include_children=False)
        self.assertEqual(client_output[0]["permission"], 511)

    def test_multipath(self):
        self.client.chmod(['/dir1', '/zerofile'], 0700)
        client_output = self.client.ls(['/dir1', '/zerofile'], include_toplevel=True, include_children=False)
        for node in client_output:
            if node["file_type"] == "d":
                self.assertEqual(node["permission"], 448)
            else:
                self.assertEqual(node["permission"], 384)

    def test_recursive(self):
        self.client.chmod(['/'], 0770, recurse=True)
        expected_output = self.cluster.ls(["/"], ["-R"])
        for node in expected_output:
            if node["file_type"] == "d":
                self.assertEqual(node["permission"], 504)
            else:
                self.assertEqual(node["permission"], 432)

    def test_unknown_file(self):
        self.assertRaises(FileNotFoundException, self.client.chmod, ['/nonexistent'], 0777, recurse=True)

    def test_invalid_input(self):
            self.assertRaises(InvalidInputException, self.client.chmod, '/stringpath', 777)
