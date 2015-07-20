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
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException
from snakebite.platformutils import get_current_username
from minicluster_testbase import MiniClusterTestBase

import os
import re

class DeleteTest(MiniClusterTestBase):
    def test_delete_file(self):
        before_state = set([node['path'] for node in self.client.ls(['/'])])
        list(self.client.delete(['/zerofile']))
        after_state = set([node['path'] for node in self.client.ls(['/'])])
        self.assertEqual(len(after_state), len(before_state) - 1)
        self.assertFalse('/zerofile' in after_state)

    def test_delete_multi(self):
        before_state = set([node['path'] for node in self.client.ls(['/'])])
        list(self.client.delete(['/test1', '/test2']))
        after_state = set([node['path'] for node in self.client.ls(['/'])])
        self.assertEqual(len(after_state), len(before_state) - 2)
        self.assertFalse('/test1' in after_state or '/test2' in after_state)

    def test_unknown_file(self):
        result = self.client.delete(['/doesnotexist'])
        self.assertRaises(FileNotFoundException, result.next)

    def test_invalid_input(self):
        result = self.client.delete('/stringpath')
        self.assertRaises(InvalidInputException, result.next)

    def test_recurse(self):
        list(self.client.delete(['/foo'], recurse=True))
        client_output = self.client.ls(['/'])
        paths = [node['path'] for node in client_output]
        self.assertFalse('/foo' in paths)

    def test_glob(self):
        list(self.client.delete(['/ba*'], recurse=True))
        client_output = self.client.ls(['/'])
        paths = [node['path'] for node in client_output]
        self.assertFalse('/bar' in paths)

class DeleteWithTrashTest(MiniClusterTestBase):
    def setUp(self):
        super(DeleteWithTrashTest, self).setUp()
        self.client.use_trash = True
        self.username = get_current_username()
        self.trash_location = "/user/%s/.Trash/Current" % self.username

    def assertNotExists(self, location_under_test):
        self.assertFalse(self.client.test(location_under_test, exists=True))

    def assertExists(self, location_under_test):
        self.assertTrue(self.client.test(location_under_test, exists=True))

    def assertTrashExists(self):
        list(self.client.ls([self.trash_location]))

    def assertInTrash(self, location_under_test):
        self.assertTrashExists()
        trash_location = "%s%s" % (self.trash_location, location_under_test)
        self.assertTrue(self.client.test(trash_location, exists=True))

    def assertNotInTrash(self, location_under_test):
        self.assertTrashExists()
        trash_location = "%s%s" % (self.trash_location, location_under_test)
        self.assertFalse(self.client.test(trash_location, exists=True))

    def assertSecondaryTrash(self, location_under_test):
        client_output = self.client.ls([self.trash_location])
        regex = r"%s/bar\d{13}" % self.trash_location
        augmented_path = [n['path'] for n in client_output if re.match(regex, n['path'])][0]

        trash_contents = set([node['path'] for node in self.client.ls([augmented_path])])
        self.assertTrue(location_under_test % augmented_path in trash_contents)

    def test_delete_file(self):
        location_under_test = '/zerofile'
        list(self.client.delete([location_under_test]))
        self.assertNotExists(location_under_test)
        self.assertInTrash(location_under_test)

    def test_delete_multi(self):
        locations_under_test = ['/test1', '/test2']
        list(self.client.delete(locations_under_test))
        for location_under_test in locations_under_test:
            self.assertNotExists(location_under_test)
            self.assertInTrash(location_under_test)

    def test_unknown_file(self):
        result = self.client.delete(['/doesnotexist'])
        self.assertRaises(FileNotFoundException, result.next)

    def test_invalid_input(self):
        result = self.client.delete('/stringpath')
        self.assertRaises(InvalidInputException, result.next)

    def test_recurse(self):
        location_under_test = '/foo'
        list(self.client.delete(['/foo'], recurse=True))
        self.assertNotExists(location_under_test)
        self.assertInTrash(location_under_test)

    def test_subdir(self):
        location_under_test = "/bar/baz"

        list(self.client.delete([location_under_test], recurse=True))

        # Check if /bar still exists, but also that /user/<myuser>/.Trash/Current/bar has been created
        self.assertExists(os.path.dirname(location_under_test))
        self.assertInTrash(os.path.dirname(location_under_test))

        self.assertNotExists(location_under_test)
        self.assertInTrash(location_under_test)

        # Remove /bar and see if a 2nd version was created
        location_under_test = "/bar"
        list(self.client.delete([location_under_test], recurse=True))
        self.assertNotExists(location_under_test)

        client_output = self.client.ls([self.trash_location])
        regex = r"%s/bar\d{13}" % self.trash_location
        augmented_path = [n['path'] for n in client_output if re.match(regex, n['path'])][0]
        self.assertExists("%s%s" % (augmented_path, "/foo"))

    def test_glob(self):
        list(self.client.delete(['/zipped/*'], recurse=True))
        self.assertNotExists('/zipped/test1.gz')
        self.assertNotExists('/zipped/test1.bz2')
        self.assertInTrash('/zipped/test1.gz')
        self.assertInTrash('/zipped/test1.bz2')

    def test_path_in_trash(self):
        location_under_test = '/test3'
        list(self.client.delete([location_under_test]))
        self.assertInTrash(location_under_test)
        list(self.client.delete(["%s%s" % (self.trash_location, location_under_test)]))
        self.assertNotInTrash(location_under_test)

    def test_delete_trash_parent(self):
        list(self.client.delete(['/test4']))
        try_path = [os.path.dirname(os.path.dirname(self.trash_location))]

        with self.assertRaises(Exception):
            list(self.client.delete(try_path))

