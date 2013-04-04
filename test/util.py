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
import datetime


def assertListings(expected, actual, len_method, content_method):
    # Sort both listings by path name
    expected = sorted(expected, key=lambda node: node["path"])
    actual = sorted(actual, key=lambda node: node["path"])

    # Only test for the following attributes, since CLI hadoop doesn't provide all
    # attributes
    test_attributes = ['path', 'permission', 'block_replication', 'owner', 'group',
                   'length', 'modification_time', 'file_type']

    # Assert the length of both listings with the len_method
    len_method(len(actual), len(expected))

    # Assert both listings with the content_method
    for i, expected_node in enumerate(expected):
        client_node = actual[i]
        for attribute in test_attributes:
            # Modification times from CLI hadoop have less granularity,
            # so we transform both listings to an equal format
            if attribute == 'modification_time':
                client_time = datetime.datetime.fromtimestamp(client_node[attribute] / 1000).strftime("%Y%d%m%H%%M")
                expected_time = datetime.datetime.fromtimestamp(expected_node[attribute]).strftime("%Y%d%m%H%%M")
                content_method(expected_time, client_time)
            else:
                content_method(expected_node[attribute], client_node[attribute])


def assertDu(expected, actual, len_method, content_method):
    len_method(len(expected), len(actual))
    for i, expected_node in enumerate(expected):
        client_node = actual[i]
        content_method(expected_node, client_node)
