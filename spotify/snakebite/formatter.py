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
import stat
import json
import binascii


def _octalToPerm(octal):
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
    for x in ['', 'k', 'm', 'g', 't']:
        if num < 1024.0:
            if x == '':
                return num
            else:
                return "%3.1f%s" % (num, x)
        num /= 1024.0


def formatColumn(col, node, human_readable):
    value = node.get(col)

    if col == "file_type":
        if value == "f":
            return "-"
        else:
            return value
    elif col == "permission":
        return _octalToPerm(value)
    elif col == "modification_time":
        timestamp = datetime.datetime.fromtimestamp(value / 1000)
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


def formatListing(listing, json_output=False, human_readable=False):
    if json_output:
        return json.dumps(listing)

    ret = []
    ret.append("Found %d items" % len(listing))
    columns = ['file_type', 'permission', 'block_replication', 'owner', 'group', 'length', 'modification_time', 'path']

    max_len = max([len(str(node.get('length'))) for node in listing] + [10])
    templ = "%%s%%s %%3s %%-12s %%-12s %%%ds %%s %%s" % max_len
    for node in listing:
        cols = [str(formatColumn(col, node, human_readable)) for col in columns]
        ret.append(templ % tuple(cols))

    return "\n".join(ret)


def formatResults(results, json_output=False):
    if json_output:
        return json.dumps(results)

    ret = []
    max_len = max(len(r.get('path')) for r in results)
    templ = "%%-%ds %%-6s %%s" % max_len
    for r in results:
        if r['result']:
            result = "OK"
        else:
            result = "ERROR:"
        if r.get('error'):
            error = r['error']
        else:
            error = ""

        ret.append(templ % (r.get('path'), result, error))

    return "\n".join(ret)


def formatCounts(results, json_output=False):
    if json_output:
        return json.dumps(results)

    ret = []
    for result in results:
        ret.append("%12s %12s %18s %s" % (result.get('directoryCount'),
                                            result.get('fileCount'),
                                            result.get('spaceConsumed'),
                                            result.get('path')))
    return "\n".join(ret)


def formatFsStats(results, json_output=False, human_readable=False):
    r = results[0]
    if json_output:
        return json.dumps(r)

    fs = r['filesystem']
    size = r['capacity']
    used = r['used']
    avail = r['remaining']
    if avail == 0:
        pct_used = 0
    else:
        pct_used = str((used / avail) * 100)

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
    return "%s\n%s" % (header, data)


def formatDu(results, json_output=False, human_readable=False):
    ret = []
    if json_output:
            return json.dumps(results)
    if human_readable:
        for result in results:
            result['length'] = _sizeof_fmt(result['length'])
    max_len = max([len(str(r['length'])) for r in results])
    templ = "%%-%ds  %%s" % max_len
    for result in results:
        ret.append(templ % (result['length'], result['path']))
    return "\n".join(ret)


def formatStat(results, json_output=False):
    ret = []
    if json_output:
        return json.dumps(results)
    for result in results:
        ret.append(str(result))
    return "\n".join(ret)

def format_bytes(bytes):
    ascii = binascii.b2a_hex(bytes)
    return " ".join([ascii[i:i + 2] for i in range(0, len(ascii), 2)])
