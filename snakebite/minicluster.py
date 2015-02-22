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
import datetime


class MiniCluster(object):
    ''' Class that spawns a hadoop mini cluster and wrap hadoop functionality

    This class requires the ``HADOOP_HOME`` environment variable to be set to run the ``hadoop`` command.
    It will search ``HADOOP_HOME`` for ``hadoop-mapreduce-client-jobclient<version>-tests.jar``, but the
    location of this jar can also be supplied by the ``HADOOP_JOBCLIENT_JAR`` environment variable.

    Since the current minicluster interface doesn't provide for specifying the namenode post number, and
    chooses a random one, this class parses the output from the minicluster to find the port numer.

    All supplied methods (like :py:func:`put`, :py:func:`ls`, etc) use the hadoop command to perform operations, and not
    the snakebite client, since this is used for testing snakebite itself.

    All methods return a list of maps that are snakebite compatible.

    Example without :mod:`snakebite.client <client>`

    >>> from snakebite.minicluster import MiniCluster
    >>> cluster = MiniCluster("/path/to/test/files")
    >>> ls_output = cluster.ls(["/"])

    Example with :mod:`snakebite.client <client>`

    >>> from snakebite.minicluster import MiniCluster
    >>> from snakebite.client import Client
    >>> cluster = MiniCluster("/path/to/test/files")
    >>> client = Client('localhost', cluster.port)
    >>> ls_output = client.ls(["/"])

    Just as the snakebite client, the cluster methods take a list of strings as paths. Wherever a method
    takes ``extra_args``, normal hadoop command arguments can be given (like -r, -f, etc).

    More info can be found at http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CLIMiniCluster.html

    .. note:: A minicluster will be started at instantiation
    .. note:: Not all hadoop commands have been implemented, only the ones that
              were necessary for testing the snakebite client, but please feel free to add them
    '''
    def __init__(self, testfiles_path, start_cluster=True, nnport=None):
        '''
        :param testfiles_path: Local path where test files can be found. Mainly used for ``put()``
        :type testfiles_path: string
        :param start_cluster: start a MiniCluster on initialization. If False, this class will act as an interface to the ``hadoop fs`` command
        :type start_cluster: boolean
        '''
        self._testfiles_path = testfiles_path
        self._hadoop_home = os.environ['HADOOP_HOME']
        self._jobclient_jar = os.environ.get('HADOOP_JOBCLIENT_JAR')
        self._hadoop_cmd = "%s/bin/hadoop" % self._hadoop_home
        if start_cluster:
            self._start_mini_cluster(nnport)
            self.host = "localhost"
            self.port = self._get_namenode_port()
            self.hdfs_url = "hdfs://%s:%d" % (self.host, self.port)
        else:
            self.hdfs_url = "hdfs://"

    def terminate(self):
        ''' Terminate the cluster

        Since the minicluster is started as a subprocess, this method has to be called explicitely when
        your program ends.
        '''
        self.hdfs.terminate()

    def put(self, src, dst):
        '''Upload a file to HDFS

        This will take a file from the ``testfiles_path`` supplied in the constuctor.
        '''
        src = "%s%s" % (self._testfiles_path, src)
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-put', src, self._full_hdfs_path(dst)], True)

    def put_subprocess(self, src, dst, block_size=134217728, text=True):  # This is used for testing with large files.
        block_size_flag = "-Ddfs.block.size=%s" % str(block_size)
        cmd = [self._hadoop_cmd, 'fs', block_size_flag, '-put', src, self._full_hdfs_path(dst)]
        return subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, universal_newlines=text)

    def exists(self, path):
        """Return True if <src> exists, False if doesn't"""
        return self._getReturnCodeCmd([self._hadoop_cmd, 'fs', '-test', '-e', path]) == 0

    def is_directory(self, path):
        """Return True if <path> is a directory, False if it's NOT a directory"""
        return self._getReturnCodeCmd([self._hadoop_cmd, 'fs', '-test', '-d', self._full_hdfs_path(path)]) == 0

    def is_files(self, path):
        """Return True if <path> is a file, False if it's NOT a file"""
        return self._getReturnCodeCmd([self._hadoop_cmd, 'fs', '-test', '-f', self._full_hdfs_path(path)]) == 0

    def is_greater_then_zero_bytes(self, path):
        """Return True if file <path> is greater than zero bytes in size, False otherwise"""
        return self._getReturnCodeCmd([self._hadoop_cmd, 'fs', '-test', '-s', self._full_hdfs_path(path)]) == 0

    def is_zero_bytes_file(self, path):
        """Return True if file <path> is zero bytes in size, else return False"""
        return self._getReturnCodeCmd([self._hadoop_cmd, 'fs', '-test', '-z', self._full_hdfs_path(path)]) == 0

    def ls(self, src, extra_args=[]):
        '''List files in a directory'''
        src = [self._full_hdfs_path(x) for x in src]
        output = self._getStdOutCmd([self._hadoop_cmd, 'fs', '-ls'] + extra_args + src, True)
        return self._transform_ls_output(output, self.hdfs_url)

    def mkdir(self, src, extra_args=[]):
        '''Create a directory'''
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-mkdir'] + extra_args + [self._full_hdfs_path(src)], True)

    def df(self, src):
        '''Perform ``df`` on a path'''
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-df', self._full_hdfs_path(src)], True)

    def du(self, src, extra_args=[]):
        '''Perform ``du`` on a path'''
        src = [self._full_hdfs_path(x) for x in src]
        return self._transform_du_output(self._getStdOutCmd([self._hadoop_cmd, 'fs', '-du'] + extra_args + src, True), self.hdfs_url)

    def count(self, src):
        '''Perform ``count`` on a path'''
        src = [self._full_hdfs_path(x) for x in src]
        return self._transform_count_output(self._getStdOutCmd([self._hadoop_cmd, 'fs', '-count'] + src, True), self.hdfs_url)

    def cat(self, src, extra_args=[], text=False):
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-cat'] + extra_args + [self._full_hdfs_path(src)], text)

    def copyToLocal(self, src, dst, extra_args=[]):
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-copyToLocal'] + extra_args + [self._full_hdfs_path(src), dst], True)

    def getmerge(self, src, dst, extra_args=[]):
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-getmerge'] + extra_args + [self._full_hdfs_path(src), dst], True)

    def tail(self, src, extra_args=[], text=False):
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-tail'] + extra_args + [self._full_hdfs_path(src)], text)

    def text(self, src):
        return self._getStdOutCmd([self._hadoop_cmd, 'fs', '-text', self._full_hdfs_path(src)], True)

    def _getReturnCodeCmd(self, cmd):
        proc = self._getCmdProcess(cmd, True)
        print(proc.communicate())
        return proc.wait()

    def _getCmdProcess(self, cmd, text=False):
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=text)

    def _getStdOutCmd(self, cmd, text=False):
        return self._getCmdProcess(cmd, text).communicate()[0]

    def _full_hdfs_path(self, src):
        return "%s%s" % (self.hdfs_url, src)

    def _find_mini_cluster_jar(self, path):
        for dirpath, dirnames, filenames in os.walk(path):
            for files in filenames:
                if re.match(".*hadoop-mapreduce-client-jobclient.+-tests.jar", files):
                    return os.path.join(dirpath, files)

    def _start_mini_cluster(self, nnport=None):
        if self._jobclient_jar:
            hadoop_jar = self._jobclient_jar
        else:
            hadoop_jar = self._find_mini_cluster_jar(self._hadoop_home)
        if not hadoop_jar:
            raise Exception("No hadoop jobclient test jar found")
        cmd = [self._hadoop_cmd, 'jar', hadoop_jar,
               'minicluster', '-nomr', '-format']
        if nnport:
            cmd.extend(['-nnport', "%s" % nnport])
        self.hdfs = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, universal_newlines=True)

    def _get_namenode_port(self):
        while self.hdfs.poll() is None:
            rlist, wlist, xlist = select.select([self.hdfs.stderr, self.hdfs.stdout], [], [])
            for f in rlist:
                line = f.readline()
                print(line,)
                m = re.match(".*Started MiniDFSCluster -- namenode on port (\d+).*", line)
                if m:
                    return int(m.group(1))

    def _transform_ls_output(self, i, base_path):
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
            node['file_type'] = self._get_file_type(perms[0])
            result.append(node)
        return result

    def _transform_du_output(self, i, base_path):
        result = []
        for line in i.split("\n"):
            if line:
                fields = re.split("\s+", line)
                if len(fields) == 3:
                    (length, space_consumed, path) = re.split("\s+", line)
                elif len(fields) == 2:
                    (length, path) = re.split("\s+", line)
                else:
                    raise ValueError("Result of du operation should contain 2"
                                     " or 3 field, but there's %d fields"
                                     % len(fields))
                result.append({"path": path.replace(base_path, ""),
                               "length": long(length)})
        return result

    def _transform_count_output(self, i, base_path):
        result = []
        for line in i.split("\n"):
            if line:
                (_, dir_count, file_count, length, path) = re.split("\s+", line)
                result.append({"path": path.replace(base_path, ""), "length": long(length),
                               "directoryCount": long(dir_count), "fileCount": long(file_count)})
        return result

    def _get_file_type(self, i):
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
