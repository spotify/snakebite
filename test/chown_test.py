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


class ChownTest(MiniClusterTestBase):

    def test_onepath(self):
        list(self.client.chown(['/dir1'], "onepathowner"))
        client_output = list(self.client.ls(['/dir1'], include_toplevel=True, include_children=False))
        self.assertEqual(client_output[0]["owner"], "onepathowner")

    def test_multipath(self):
        list(self.client.chown(['/dir1', '/zerofile'], "multipathowner"))
        client_output = self.client.ls(['/dir1', '/zerofile'], include_toplevel=True, include_children=False)
        for node in client_output:
            self.assertEqual(node["owner"], "multipathowner")

    def test_recursive(self):
        list(self.client.chown(['/'], 'recursiveowner', recurse=True))
        expected_output = self.cluster.ls(["/"], ["-R"])
        for node in expected_output:
            self.assertEqual(node["owner"], "recursiveowner")

    def test_unknown_file(self):
        result = self.client.chown(['/nonexistent'], 'myGroup', recurse=True)
        self.assertRaises(FileNotFoundException, result.next)

    def test_user_group(self):
        list(self.client.chown(['/dir1'], "myUser:myGroup"))
        client_output = list(self.client.ls(['/dir1'], include_toplevel=True, include_children=False))
        self.assertEqual(client_output[0]["owner"], "myUser")
        self.assertEqual(client_output[0]["group"], "myGroup")

    def test_group(self):
        client_output = list(self.client.ls(['/dir1'], include_toplevel=True, include_children=False))
        expected_owner = client_output[0]["owner"]
        list(self.client.chown(['/dir1'], ":mySuperGroup"))
        client_output = list(self.client.ls(['/dir1'], include_toplevel=True, include_children=False))
        self.assertEqual(client_output[0]["owner"], expected_owner)
        self.assertEqual(client_output[0]["group"], "mySuperGroup")

    def test_invalid_input(self):
        result = self.client.chown('/stringpath', 'myGroup')
        self.assertRaises(InvalidInputException, result.next)
