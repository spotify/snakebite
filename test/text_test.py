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


class TextTest(MiniClusterTestBase):
    def test_text_gzip(self):
        expected_output = self.cluster.text('/zipped/test1.gz')
        client_output = list(self.client.text(['/zipped/test1.gz']))[0]
        self.assertEqual(expected_output, client_output)

    def test_text_bzip2(self):
        expected_output = self.cluster.text('/zipped/test1.bz2')
        client_output = list(self.client.text(['/zipped/test1.bz2']))[0]
        self.assertEqual(expected_output, client_output)
