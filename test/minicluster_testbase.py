import unittest2
import os
from spotify.snakebite.minicluster import MiniCluster
from spotify.snakebite.client import Client


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
