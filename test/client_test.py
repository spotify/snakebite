import unittest2
import inspect
import errno
import socket
from mock import patch, Mock

from snakebite.client import HAClient, AutoConfigClient, Client
from snakebite.config import HDFSConfig
from snakebite.namenode import Namenode
from snakebite.errors import OutOfNNException, RequestError

class ClientTest(unittest2.TestCase):
    original_hdfs_try_path = HDFSConfig.hdfs_try_paths
    original_core_try_path = HDFSConfig.core_try_paths

    def setUp(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = self.original_hdfs_try_path
        HDFSConfig.core_try_paths = self.original_core_try_path

    def test_ha_client_econnrefused_socket_error(self):
        e = socket.error
        e.errno = errno.ECONNREFUSED
        mocked_client_cat = Mock(side_effect=e)
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        ha_client.cat = HAClient._ha_gen_method(mocked_client_cat)
        cat_result_gen = ha_client.cat(ha_client, ['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    def test_ha_client_ehostunreach_socket_error(self):
        e = socket.error
        e.errno = errno.EHOSTUNREACH
        mocked_client_cat = Mock(side_effect=e)
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        ha_client.cat = HAClient._ha_gen_method(mocked_client_cat)
        cat_result_gen = ha_client.cat(ha_client, ['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    def test_ha_client_socket_timeout(self):
        e = socket.timeout
        mocked_client_cat = Mock(side_effect=e)
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        ha_client.cat = HAClient._ha_gen_method(mocked_client_cat)
        cat_result_gen = ha_client.cat(ha_client, ['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    def test_ha_client_standby_errror(self):
        e = RequestError("org.apache.hadoop.ipc.StandbyException foo bar")
        mocked_client_cat = Mock(side_effect=e)
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        ha_client.cat = HAClient._ha_gen_method(mocked_client_cat)
        cat_result_gen = ha_client.cat(ha_client, ['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

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
