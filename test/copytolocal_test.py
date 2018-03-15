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
from __future__ import absolute_import
import os
import shutil

from .minicluster_testbase import MiniClusterTestBase

TESTFILES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles/temp_testfiles")

class CopyToLocalTest(MiniClusterTestBase):

    def tearDown(self):
        shutil.rmtree(TESTFILES_PATH)

    def test_copyToLocal_file_content(self):
        os.mkdir(TESTFILES_PATH)
        target_dir = "%s/file1" % TESTFILES_PATH

        expected_content = self.cluster.cat('/test3')
        for result in self.client.copyToLocal(['/test3'], target_dir):
            self.assertEqual(result['path'], target_dir)

            client_content = self._read_file(target_dir)
            self.assertEqual(client_content, expected_content)

            self.assertEqual(result['result'], True)

    def test_copyToLocal_conflicting_names(self):
        os.mkdir(TESTFILES_PATH)
        target_dir = "%s/file2" % TESTFILES_PATH

        self.cluster.copyToLocal('/test3', target_dir)
        for result in self.client.copyToLocal(['/test3'], target_dir):
            self.assertEqual(result['error'], "file exists")
            self.assertEqual(result['path'], target_dir)
            self.assertEqual(result['result'], False)

    def test_copyToLocal_directory_structure(self):
        test_dir = '%s/actual' % TESTFILES_PATH
        expected_dir = '%s/expected' % TESTFILES_PATH
        os.mkdir(TESTFILES_PATH)
        os.mkdir(expected_dir)
        os.mkdir(test_dir)
        expected_dir_structure = []
        test_dir_structure = []

        self.cluster.copyToLocal('/bar', expected_dir)

        for result in self.client.copyToLocal(['/bar'], test_dir):
            self.assertEqual(result['result'], True)

        for path, dirs, files in os.walk(expected_dir):
            expected_dir_structure.append(path.replace('/expected', "", 1))
            for f in files:
                f = "%s/%s" % (path, f)
                data = self._read_file(f)
                expected_dir_structure.append((f.replace('/expected', "", 1), data))

        for path, dirs, files in os.walk(test_dir):
            test_dir_structure.append(path.replace('/actual', "", 1))
            for f in files:
                f = "%s/%s" % (path, f)
                data = self._read_file(f)
                test_dir_structure.append((f.replace('/actual', "", 1), data))

        self.assertEqual(expected_dir_structure, test_dir_structure)

    def test_copyToLocal_relative_directory_structure(self):
        test_dir = '%s/relative_actual' % TESTFILES_PATH
        test_dir = os.path.relpath(test_dir)
        expected_dir = '%s/relative_expected' % TESTFILES_PATH
        os.mkdir(TESTFILES_PATH)
        os.mkdir(expected_dir)
        os.mkdir(test_dir)
        expected_dir_structure = []
        test_dir_structure = []

        self.cluster.copyToLocal('/bar/baz', expected_dir)

        for result in self.client.copyToLocal(['/bar/baz'], test_dir):
            self.assertEqual(result['result'], True)

        for path, dirs, files in os.walk(expected_dir):
            expected_dir_structure.append(path.replace('/relative_expected', "", 1))
            for f in files:
                f = "%s/%s" % (path, f)
                data = self._read_file(f)
                expected_dir_structure.append((f.replace('/relative_expected', "", 1), data))

        for path, dirs, files in os.walk(os.path.abspath(test_dir)):
            test_dir_structure.append(path.replace('/relative_actual', "", 1))
            for f in files:
                f = "%s/%s" % (path, f)
                data = self._read_file(f)
                test_dir_structure.append((f.replace('/relative_actual', "", 1), data))

        self.assertEqual(expected_dir_structure, test_dir_structure)


    def _read_file(self, file):
        f = open(file, 'r')
        data = f.read()
        f.close()
        return data
