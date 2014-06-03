import unittest2
import inspect
from mock import patch

from snakebite.client import HAClient, AutoConfigClient
from snakebite.config import HDFSConfig
from snakebite.errors import OutOfNNException

class ClientTest(unittest2.TestCase):
    original_hdfs_try_path = set(HDFSConfig.hdfs_try_paths)
    original_core_try_path = set(HDFSConfig.core_try_paths)

    def setUp(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = self.original_hdfs_try_path
        HDFSConfig.core_try_paths = self.original_core_try_path


    def test_wrapped_methods(self):
        public_methods = [(name, method) for name, method in inspect.getmembers(HAClient, inspect.ismethod) if not name.startswith("_")]
        self.assertGreater(len(public_methods), 0)
        wrapped_methods = [str(method) for name, method in public_methods if ".wrapped" in str(method)]
        self.assertEqual(len(public_methods), len(wrapped_methods))

    def test_empty_namenodes_haclient(self):
        namenodes = ()
        self.assertRaises(OutOfNNException, HAClient, namenodes)

    @patch('os.environ.get')
    def test_empty_namenodes_autoclient(self, environ_get):
        #Make sure we will find no namenodes:
        environ_get.return_value = False
        HDFSConfig.hdfs_try_paths = ()
        HDFSConfig.core_try_paths = ()
        self.assertRaises(OutOfNNException, AutoConfigClient)
