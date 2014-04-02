import unittest2
import os
import time
from snakebite.minicluster import MiniCluster
from snakebite.client import Client


class MiniClusterTestBase(unittest2.TestCase):

    cluster = None

    @classmethod
    def setupClass(cls):
        if not cls.cluster:
            testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
            cls.cluster = MiniCluster(testfiles_path)
            cls.cluster.put("/test1", "/test1")
            cls.cluster.put("/test1", "/test2")
            cls.cluster.put("/test3", "/test3") #1024 bytes

            cls.cluster.mkdir("/zipped")
            cls.cluster.put("/zipped/test1.gz", "/zipped")
            cls.cluster.put("/zipped/test1.bz2", "/zipped")

            cls.cluster.put("/zerofile", "/")

            cls.cluster.mkdir("/dir1")
            cls.cluster.put("/zerofile", "/dir1")
            cls.cluster.mkdir("/dir2")
            cls.cluster.mkdir("/dir2/dir3")
            cls.cluster.put("/test1", "/dir2/dir3")
            cls.cluster.put("/test3", "/dir2/dir3")

            cls.cluster.mkdir("/foo/bar/baz", ['-p'])
            cls.cluster.put("/zerofile", "/foo/bar/baz/qux")
            cls.cluster.mkdir("/bar/baz/foo", ['-p'])
            cls.cluster.put("/zerofile", "/bar/baz/foo/qux")
            cls.cluster.mkdir("/bar/foo/baz", ['-p'])
            cls.cluster.put("/zerofile", "/bar/foo/baz/qux")
            cls.cluster.put("/log", "/")

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.terminate()

    def setUp(self):
        version = os.environ.get("HADOOP_PROTOCOL_VER", 7)
        self.cluster = self.__class__.cluster
        self.client = Client(self.cluster.host, self.cluster.port, int(version))

if __name__ == '__main__':
    try:
        MiniClusterTestBase.setupClass()
        while True:
            time.sleep(5)
    finally:
        MiniClusterTestBase.cluster.terminate()
