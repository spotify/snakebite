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
from __future__ import division

import datetime
import stat
import json
import binascii
import os.path


def _octal_to_perm(octal):
    perms = list("-" * 9)
    if octal & stat.S_IRUSR:
        perms[0] = "r"
    if octal & stat.S_IWUSR:
        perms[1] = "w"
    if octal & stat.S_IXUSR:
        perms[2] = "x"
    if octal & stat.S_IRGRP:
        perms[3] = "r"
    if octal & stat.S_IWGRP:
        perms[4] = "w"
    if octal & stat.S_IXGRP:
        perms[5] = "x"
    if octal & stat.S_IROTH:
        perms[6] = "r"
    if octal & stat.S_IWOTH:
        perms[7] = "w"
    if octal & stat.S_IXOTH:
        perms[8] = "x"
    return "".join(perms)


def _sizeof_fmt(num):
    for x in ['', 'k', 'M', 'G', 'T', 'P']:
        if num < 1024.0:
            if x == '':
                return num
            else:
                return "%3.1f%s" % (num, x)
        num /= 1024.0


def format_column(col, node, human_readable):
    value = node.get(col)

    if col == "file_type":
        if value == "f":
            return "-"
        else:
            return value
    elif col == "permission":
        return _octal_to_perm(value)
    elif col == "modification_time":
        timestamp = datetime.datetime.fromtimestamp(value // 1000)
        return timestamp.strftime('%Y-%m-%d %H:%M')
    elif col == "block_replication":
        if node["file_type"] == "f":
            return value
        else:
            return "-"
    elif col == "length":
        if human_readable:
            return _sizeof_fmt(int(value))
        else:
            return value
    else:
        return value


def format_listing(listing, json_output=False, human_readable=False, recursive=False, summary=False):
    if json_output:
        for node in listing:
            yield json.dumps(node)
    else:
        nodes = []
        last_dir = None
        try:
            while True:
                node = listing.next()
                dir_name = os.path.dirname(node['path'])
                if dir_name != last_dir:
                    if last_dir:
                        yield _create_dir_listing(nodes, human_readable, recursive, summary)
                    last_dir = dir_name
                    nodes = []
                nodes.append(node)

        except StopIteration:
            yield _create_dir_listing(nodes, human_readable, recursive, summary)


def _create_dir_listing(nodes, human_readable, recursive, summary):
    ret = []
    if not recursive and not summary:
        ret.append("Found %d items" % len(nodes))

    if summary:
        for node in nodes:
            path = node['path']
            if node['file_type'] == "d":
                path += "/"
            ret.append(path)
    else:
        columns = ['file_type', 'permission', 'block_replication', 'owner', 'group', 'length', 'modification_time', 'path']

        max_len = max([len(str(node.get('length'))) for node in nodes] + [10])
        max_owner = max([len(str(node.get('owner'))) for node in nodes] + [10])
        max_group = max([len(str(node.get('group'))) for node in nodes] + [10])
        templ = "%%s%%s %%3s %%-%ds %%-%ds %%%ds %%s %%s" % (max_owner, max_group, max_len)
        for node in nodes:
            cols = [str(format_column(col, node, human_readable)) for col in columns]
            ret.append(templ % tuple(cols))

    return "\n".join(ret)


def format_results(results, json_output=False):
    if json_output:
        for result in results:
            yield json.dumps(result)
    else:
        for r in results:
            if r['result']:
                if r.get('message'):
                    yield "OK: %s %s" % (r.get('path'), r.get('message'))
                else:
                    yield "OK: %s" % r.get('path')
            else:
                yield "ERROR: %s (reason: %s)" % (r.get('path'), r.get('error', ''))


def format_counts(results, json_output=False, human_readable=False):
    if json_output:
        for result in results:
            yield json.dumps(result)
    else:
        for result in results:
            # lenght is space before replication
            length = result.get('length')
            if human_readable:
                length = _sizeof_fmt(int(length))

            yield "%12s %12s %18s %s" % (result.get('directoryCount'),
                                                result.get('fileCount'),
                                                length,
                                                result.get('path'))


def format_fs_stats(result, json_output=False, human_readable=False):
    if json_output:
        yield json.dumps(result)
    else:
        fs = result['filesystem']
        size = result['capacity']
        used = result['used']
        avail = result['remaining']
        if size == 0:
            pct_used = "0.00"
        else:
            pct_used = (float(used) / float(size)) * 100.0
            pct_used = "%.2f" % pct_used

        if human_readable:
            size = _sizeof_fmt(int(size))
            used = _sizeof_fmt(int(used))
            avail = _sizeof_fmt(int(avail))

        tmpl = "%%-%ds  %%%ds  %%%ds  %%%ds  %%%ds%%%%" % (max(len(str(fs)), len('Filesystem')),
                                                           max(len(str(size)), len('Size')),
                                                           max(len(str(used)), len('Used')),
                                                           max(len(str(avail)), len('Available')),
                                                           max(len(str(pct_used)), len('Use%')))

        header = tmpl % ('Filesystem', 'Size', 'Used', 'Available', 'Use')
        data = tmpl % (fs, size, used, avail, pct_used)
        yield "%s\n%s" % (header, data)


def format_du(listing, json_output=False, human_readable=False):
    if json_output:
        for result in listing:
            yield json.dumps(result)
    else:
        # for result in listing:
        #     if human_readable:
        #         result['lenght'] =  _sizeof_fmt(result['length'])
        #     yield "%s %s" % (result['length'], result['path'])

        nodes = []
        last_dir = None
        try:
            while True:
                node = listing.next()
                dir_name = os.path.dirname(node['path'])
                if dir_name != last_dir:
                    if last_dir:
                        yield _create_count_listing(nodes, human_readable)
                    last_dir = dir_name
                    nodes = []
                nodes.append(node)
        except StopIteration:
            yield _create_count_listing(nodes, human_readable)


def _create_count_listing(nodes, human_readable):
    ret = []
    if human_readable:
        for node in nodes:
            node['length'] = _sizeof_fmt(node['length'])
    max_len = max([len(str(r['length'])) for r in nodes])
    templ = "%%-%ds  %%s" % max_len
    for node in nodes:
        ret.append(templ % (node['length'], node['path']))
    return "\n".join(ret)

def _format_permission(decimal_permissions):
    """ formats a decimal representation of UNIX-style permissions,
        removing leading 0 for octal numbers with length > 4,
        into a string representation """
    return "{0:04o}".format(decimal_permissions)[-4:]

def format_stat(results, json_output=False):
    ret = results
    # By default snakebite returns permissions in decimal.
    if ret.has_key('permission'):
        ret['permission'] = _format_permission(ret['permission'])
    if json_output:
        return json.dumps(ret)
    return "\n".join(["%-20s\t%s" % (k, v) for k, v in sorted(ret.iteritems())])


def format_bytes(bytes):
    ascii = binascii.b2a_hex(bytes)
    byte_array = [ascii[i:i + 2] for i in range(0, len(ascii), 2)]
    return  "%s (len: %d)"% (" ".join(byte_array), len(byte_array))
