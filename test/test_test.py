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


class TestTest(MiniClusterTestBase):
    def test_exists(self):
        result = self.client.test('/zerofile', exists=True)
        self.assertTrue(result)

    def test_not_exist(self):
        result = self.client.test('/zerofile-foo', exists=True)
        self.assertFalse(result)

    def test_dir_exists(self):
        result = self.client.test('/dir1', exists=True, directory=True)
        self.assertTrue(result)

    def test_dir_not_exists(self):
        result = self.client.test('/dir1337', exists=True, directory=True)
        self.assertFalse(result)

    def test_glob(self):
        result = self.client.test('/foo/bar/baz/*', exists=True)
        self.assertTrue(result)

    def test_glob_not_exists(self):
        result = self.client.test('/foo/bar/flep/*', exists=True)
        self.assertFalse(result)

    def test_zero(self):
        result = self.client.test('/zerofile', exists=True, zero_length=True)
        self.assertTrue(result)
