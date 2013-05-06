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

import re
import itertools


def expand_paths(paths):
    ''' Expand paths like /foo/{bar,baz} becomes /foo/bar, /foo/bar'''
    result = []
    exp = re.compile("{(.*?)}")
    for path in paths:
        m = exp.findall(path)
        if not m:
            result.append(path)
        else:
            x = [item.split(",") for item in m]
            template = re.sub("{.*?}", "%s", path)
            product = list(itertools.product(*x))
            for s in product:
                result.append(template % s)
    return result

magick_check = re.compile('[*?[{}]')


def has_magic(s):
    return magick_check.search(s) is not None
