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
import subprocess
import select
import re
import unittest2
import datetime

from spotify.snakebite.client import Client


class MiniCluster(object):
    def __init__(self, testfiles_path, jar_search_paths=[]):
        self._testfiles_path = testfiles_path
        self._hadoop_home = os.environ['HADOOP_HOME']
        self._jobclient_jar = os.environ.get('HADOOP_JOBCLIENT_JAR')
        self._hadoop_cmd = "%s/bin/hadoop" % self._hadoop_home
        self._startMiniCluster()
        self.host = "localhost"
        self.port = self._getNameNodePort()
        self.hdfs_url = "hdfs://%s:%d" % (self.host, self.port)

    def terminate(self):
        self.hdfs.terminate()

    def put(self, src, dst):
        src = "%s%s" % (self._testfiles_path, src)
        return self._runCmd([self._hadoop_cmd, 'fs', '-put', src, self._full_hdfs_path(dst)])

    def ls(self, src, extra_args=[]):
        src = [self._full_hdfs_path(x) for x in src]
        output = self._runCmd([self._hadoop_cmd, 'fs', '-ls'] + extra_args + src)
        return self._transformLsOutput(output, self.hdfs_url)

    def mkdir(self, src, extra_args=[]):
        return self._runCmd([self._hadoop_cmd, 'fs', '-mkdir'] + extra_args + [self._full_hdfs_path(src)])

    def df(self, src):
        return self._runCmd([self._hadoop_cmd, 'fs', '-df', self._full_hdfs_path(src)])

    def du(self, src, extra_args=[]):
        src = [self._full_hdfs_path(x) for x in src]
        return self._transformDuOutput(self._runCmd([self._hadoop_cmd, 'fs', '-du'] + extra_args + src), self.hdfs_url)

    def count(self, src):
        src = [self._full_hdfs_path(x) for x in src]
        return self._transformCountOutput(self._runCmd([self._hadoop_cmd, 'fs', '-count'] + src), self.hdfs_url)

    def _runCmd(self, cmd):
        print cmd
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return p.communicate()[0]

    def _full_hdfs_path(self, src):
        return "%s%s" % (self.hdfs_url, src)

    def _findMiniClusterJar(self, path):
        for dirpath, dirnames, filenames in os.walk(path):
            for files in filenames:
                if re.match(".*hadoop-mapreduce-client-jobclient.+-tests.jar", files):
                    return os.path.join(dirpath, files)

    def _startMiniCluster(self):
        if self._jobclient_jar:
            hadoop_jar = self._jobclient_jar
        else:
            hadoop_jar = self._findMiniClusterJar(self._hadoop_home)
        if not hadoop_jar:
            raise Exception("No hadoop jobclient test jar found")
        self.hdfs = subprocess.Popen([self._hadoop_cmd,
                                      'jar',
                                      hadoop_jar,
                                      'minicluster', '-nomr', '-format'],
                                      bufsize=0,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _getNameNodePort(self):
        port_found = False
        while self.hdfs.poll() is None and not port_found:
            rlist, wlist, xlist = select.select([self.hdfs.stderr, self.hdfs.stdout], [], [])
            for f in rlist:
                line = f.readline()
                print line,
                m = re.match(".*Started MiniDFSCluster -- namenode on port (\d+).*", line)
                if m:
                    return int(m.group(1))

    def _transformLsOutput(self, i, base_path):
        result = []
        for line in i.split("\n"):
            if not line or line.startswith("Found"):
                continue

            (perms, replication, owner, group, length, date, time, path) = re.split("\s+", line)
            node = {}

            if replication == '-':
                replication = 0

            node['permission'] = self._perms_to_int(perms)
            node['block_replication'] = int(replication)
            node['owner'] = owner
            node['group'] = group
            node['length'] = int(length)
            dt = "%s %s" % (date, time)
            node['modification_time'] = long(datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M').strftime('%s'))
            node['path'] = path.replace(base_path, '')
            node['file_type'] = self._getFileType(perms[0])
            result.append(node)
        return result

    def _transformDuOutput(self, i, base_path):
        result = []
        for line in i.split("\n"):
            if line:
                (length, path) = re.split("\s+", line)
                result.append({"path": path.replace(base_path, ""), "length": long(length)})
        return result

    def _transformCountOutput(self, i, base_path):
        result = []
        for line in i.split("\n"):
            if line:
                (_, dir_count, file_count, length, path) = re.split("\s+", line)
                result.append({"path": path.replace(base_path, ""), "length": long(length),
                               "directoryCount": long(dir_count), "fileCount": long(file_count)})
        return result

    def _getFileType(self, i):
        if i == "-":
            return "f"
        else:
            return i

    def _perms_to_int(self, perms):
        s = ""
        for x in perms[1:]:
            if x == "-":
                s += "0"
            else:
                s += "1"
        octal = "%d%d%d" % (int(s[0:3], 2), int(s[3:6], 2), int(s[6:9], 2))
        return int(octal, 8)


class MiniClusterTestBase(unittest2.TestCase):

    cluster = None

    @classmethod
    def setupClass(cls):
        if not cls.cluster:
            testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
            cls.cluster = MiniCluster(testfiles_path)
            cls.cluster.put("/test1", "/test1")
            cls.cluster.put("/test1", "/test2")
            cls.cluster.put("/zerofile", "/")
            cls.cluster.mkdir("/dir1")
            cls.cluster.put("/zerofile", "/dir1")
            cls.cluster.mkdir("/foo/bar/baz", ['-p'])
            cls.cluster.put("/zerofile", "/foo/bar/baz/qux")
            cls.cluster.mkdir("/bar/baz/foo", ['-p'])
            cls.cluster.put("/zerofile", "/bar/baz/foo/qux")
            cls.cluster.mkdir("/bar/foo/baz", ['-p'])
            cls.cluster.put("/zerofile", "/bar/foo/baz/qux")

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.terminate()

    def setUp(self):
        self.cluster = self.__class__.cluster
        self.client = Client(self.cluster.host, self.cluster.port)
