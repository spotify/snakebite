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
import os
import shutil

from minicluster_testbase import MiniClusterTestBase


class CopyToLocalTest(MiniClusterTestBase):

    def tearDown(self):
        shutil.rmtree('test/testfiles/temp_testfiles')

    def test_copyToLocal_file_content(self):
        os.mkdir('test/testfiles/temp_testfiles')

        test3_content = self.cluster.cat('/test3')
        for result in self.client.copyToLocal(['/test3'], 'test/testfiles/temp_testfiles/file1'):
            self.assertEqual(result['path'], "test/testfiles/temp_testfiles/file1")
            self.assertEqual(result['response'], test3_content)
            self.assertEqual(result['result'], True)

    def test_copyToLocal_conflicting_names(self):
        os.mkdir('test/testfiles/temp_testfiles')
        target_dir = 'test/testfiles/temp_testfiles/file2'

        self.cluster.copyToLocal('/test3', target_dir)
        for result in self.client.copyToLocal(['/test3'], target_dir):
            self.assertEqual(result['error'], "file exists")
            self.assertEqual(result['path'], target_dir)
            self.assertEqual(result['result'], False)

    def test_copyToLocal_directory_structure(self):
        base_dir ='test/testfiles/temp_testfiles'
        test_dir = base_dir + '/actual'
        expected_dir = base_dir + '/expected' 
        os.mkdir(base_dir)
        os.mkdir(expected_dir)
        os.mkdir(test_dir)
        expected_dir_structure = []
        test_dir_structure = []

        test3 = self.cluster.copyToLocal('/bar', expected_dir)

        for result in self.client.copyToLocal(['/bar'], test_dir):
        	self.assertEqual(result['result'], True)

        for path, dirs, files in os.walk(expected_dir):
            expected_dir_structure.append(path.replace('/expected', "", 1))
            for f in files:
                f = path+"/"+f
                data = self._read_file(f)
                expected_dir_structure.append((f.replace('/expected', "", 1), data))

        for path, dirs, files in os.walk(test_dir):
            test_dir_structure.append(path.replace('/actual', "", 1))
            for f in files:
                f = path+"/"+f
                data = self._read_file(f)
                test_dir_structure.append((f.replace('/actual', "", 1), data))

        self.assertEqual(expected_dir_structure, test_dir_structure)


    def _read_file(self, file):
        f = open(file, 'r')
        data = f.read()
        f.close()
        return data