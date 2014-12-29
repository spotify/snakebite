# -*- coding: utf-8 -*-
# Copyright (c) 2014 Spotify AB
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

from snakebite.errors import DirectoryException, FileException
from minicluster_testbase import MiniClusterTestBase

class TouchZTest(MiniClusterTestBase):
    counter = 0

    @classmethod
    def get_fresh_filename(cls, prefix="/"):
        cls.counter+=1
        return "%stouchz_foobar%d" % (prefix, cls.counter) 

    def test_touchz_single(self):
        f1 = self.get_fresh_filename()
        result = all([ r['result'] for r in self.client.touchz([f1]) ])
        self.assertTrue(result)
        self.assertTrue(self.cluster.is_zero_bytes_file(f1))

    def test_touchz_multiple(self):
        f1, f2, f3 = self.get_fresh_filename(), self.get_fresh_filename(), self.get_fresh_filename()
        result = all([ r['result'] for r in
            self.client.touchz([f1, f2, f3]) ])
        self.assertTrue(result)
        self.assertTrue(self.cluster.is_zero_bytes_file(f1))
        self.assertTrue(self.cluster.is_zero_bytes_file(f2))
        self.assertTrue(self.cluster.is_zero_bytes_file(f3))

    def test_touchz_dir_doesnt_exists(self):
        f1 = self.get_fresh_filename(prefix="/big/data/lake/dir/")
        self.assertRaises(DirectoryException, all, self.client.touchz([f1]))
        self.assertFalse(self.cluster.is_zero_bytes_file(f1))

    def test_touchz_file_already_exists(self):
        f1 = self.get_fresh_filename()
        result = all([ r['result'] for r in self.client.touchz([f1]) ])
        self.assertTrue(result)
        self.assertTrue(self.cluster.is_zero_bytes_file(f1))
        result = all([ r['result'] for r in self.client.touchz([f1]) ])
        self.assertFalse(result)
