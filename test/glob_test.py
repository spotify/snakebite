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
import unittest2
import spotify.snakebite.glob as glob


class GlobTest(unittest2.TestCase):
    def test_path_expansion(self):
        paths = ['/foo/bar', '/foo/{bar,baz}', '/foo/{bar,baz}/{quz,quux}', '/foo/{bar,baz}/{quz,quux}/{aap}']
        expected = ['/foo/bar', '/foo/bar', '/foo/baz', '/foo/bar/quz', '/foo/bar/quux', '/foo/baz/quz', '/foo/baz/quux', '/foo/bar/quz/aap', '/foo/bar/quux/aap', '/foo/baz/quz/aap', '/foo/baz/quux/aap']
        new_paths = glob.expandPaths(paths)
        self.assertEqual(new_paths, expected)
