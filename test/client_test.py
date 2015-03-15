import unittest2
import inspect
import errno
import socket
from mock import patch, Mock

from snakebite.client import HAClient, AutoConfigClient, Client
import snakebite.protobuf.ClientNamenodeProtocol_pb2 as client_proto
from snakebite.service import AsyncHARpcService
from snakebite.config import HDFSConfig
from snakebite.namenode import Namenode
from snakebite.errors import OutOfNNException, RequestError

import logging

logging.basicConfig(level=logging.DEBUG)

class ClientTest(unittest2.TestCase):
    original_hdfs_try_path = set(HDFSConfig.hdfs_try_paths)
    original_core_try_path = set(HDFSConfig.core_try_paths)

    def setUp(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = self.original_hdfs_try_path
        HDFSConfig.core_try_paths = self.original_core_try_path

    @patch('snakebite.service.RpcService.call')
    def test_ha_client_econnrefused_socket_error(self, rpc_call):
        e = socket.error
        e.errno = errno.ECONNREFUSED
        e.message = "errno.ECONNREFUSED"
        rpc_call.side_effect=e
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_ha_client_ehostunreach_socket_error(self, rpc_call):
        e = socket.error
        e.errno = errno.EHOSTUNREACH
        e.message = "errno.EHOSTUNREACH"
        rpc_call.side_effect=e
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_ha_client_socket_timeout(self, rpc_call):
        e = socket.timeout
        e.message = "socket.timeout"
        rpc_call.side_effect=e
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_ha_client_standby_errror(self, rpc_call):
        e = RequestError("org.apache.hadoop.ipc.StandbyException foo bar")
        rpc_call.side_effect=e
        ha_client = HAClient([Namenode("foo"), Namenode("bar")])
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_async_ha_client_econnrefused_socket_error(self, rpc_call):
        e = socket.error
        e.errno = errno.ECONNREFUSED
        e.message = "errno.ECONNREFUSED"
        rpc_call.side_effect=e
        nns = [Namenode("foo"), Namenode("bar")]
        service = AsyncHARpcService(client_proto.ClientNamenodeProtocol_Stub,
                                    nns)
        ha_client = HAClient(nns, service=service)
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_async_ha_client_ehostunreach_socket_error(self, rpc_call):
        e = socket.error
        e.errno = errno.EHOSTUNREACH
        e.message = "errno.EHOSTUNREACH"
        rpc_call.side_effect=e
        nns = [Namenode("foo"), Namenode("bar")]
        service = AsyncHARpcService(client_proto.ClientNamenodeProtocol_Stub,
                                    nns)
        ha_client = HAClient(nns, service=service)
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_async_ha_client_socket_timeout(self, rpc_call):
        e = socket.timeout
        e.message = "socket.timeout"
        rpc_call.side_effect=e
        nns = [Namenode("foo"), Namenode("bar")]
        service = AsyncHARpcService(client_proto.ClientNamenodeProtocol_Stub,
                                    nns)
        ha_client = HAClient(nns, service=service)
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

    @patch('snakebite.service.RpcService.call')
    def test_async_ha_client_standby_errror(self, rpc_call):
        e = RequestError("org.apache.hadoop.ipc.StandbyException foo bar")
        rpc_call.side_effect=e
        nns = [Namenode("foo"), Namenode("bar")]
        service = AsyncHARpcService(client_proto.ClientNamenodeProtocol_Stub,
                                    nns)
        ha_client = HAClient(nns, service=service)
        cat_result_gen = ha_client.cat(['foobar'])
        self.assertRaises(OutOfNNException, all, cat_result_gen)

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
