import unittest2
import os
import time

import os.path
import snakebite
from snakebite.config import HDFSConfig
from snakebite.client import AutoConfigClient
from mock import patch, mock_open

class ConfigTest(unittest2.TestCase):
    original_hdfs_try_path = set(HDFSConfig.hdfs_try_paths)
    original_core_try_path = set(HDFSConfig.core_try_paths)

    def setUp(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = self.original_hdfs_try_path
        HDFSConfig.core_try_paths = self.original_core_try_path

    @staticmethod
    def get_config_path(config_name):
        return os.path.abspath(os.path.join(snakebite.__file__, os.pardir, os.pardir, 'test/testconfig/conf/%s' % config_name))

    def _verify_hdfs_settings(self, config):
        self.assertEquals(len(config), 2)
        # assert first NN
        self.assertEqual('namenode1.mydomain', config[0]['namenode'])
        self.assertEqual(8888, config[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', config[1]['namenode'])
        self.assertEqual(8888, config[1]['port'])

    # namenodes in ha-port-hdfs-site.xml with no namespace (so all of them expected)
    def _verify_hdfs_settings_all(self, config):
        self.assertEquals(len(config), 3)
        # assert first NN
        self.assertEqual('namenode1.mydomain', config[0]['namenode'])
        self.assertEqual(8888, config[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', config[1]['namenode'])
        self.assertEqual(8888, config[1]['port'])
        # assert third NN
        self.assertEqual('namenode.other-domain', config[2]['namenode'])
        self.assertEqual(8888, config[2]['port'])

    def _verify_hdfs_noport_settings(self, config):
        self.assertEquals(len(config), 2)
        # assert first NN
        self.assertEqual('namenode1.mydomain', config[0]['namenode'])
        self.assertEqual(8020, config[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', config[1]['namenode'])
        self.assertEqual(8020, config[1]['port'])

    # namenodes in ha-port-hdfs-site.xml using namespace in ha-core-site.xml
    def _verify_hdfs_port_settings(self, config):
        self.assertEquals(len(config), 2)
        # assert first NN
        self.assertEqual('namenode1.mydomain', config[0]['namenode'])
        self.assertEqual(8888, config[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', config[1]['namenode'])
        self.assertEqual(8888, config[1]['port'])

    def test_read_hdfs_config_ha(self):
        hdfs_core_path = self.get_config_path('ha-port-hdfs-site.xml')
        conf = HDFSConfig.read_hadoop_config(hdfs_core_path)
        config = HDFSConfig.read_hdfs_config('', conf, '', [])
        self._verify_hdfs_settings_all(config)

    def test_read_hdfs_port_config_ha(self):
        hdfs_core_path = self.get_config_path('ha-port-hdfs-site.xml')
        conf = HDFSConfig.read_hadoop_config(hdfs_core_path)
        config = HDFSConfig.read_hdfs_config('', conf, 'testha', ['namenode1-mydomain', 'namenode2-mydomain'])
        self._verify_hdfs_port_settings(config)

    def test_read_core_config_ha(self):
        core_site_path = self.get_config_path('ha-core-site.xml')
        config = HDFSConfig.read_core_config(core_site_path)
        self.assertEquals(len(config), 1)
        self.assertEquals('testha', config[0]['namenode'])
        self.assertEquals(8020, config[0]['port'])
        self.assertFalse(HDFSConfig.use_trash)

    @patch('os.environ.get')
    def test_read_config_ha_with_ports(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-port-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_settings(config)

    @patch('os.environ.get')
    def test_read_config_non_ha_with_ports(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('non-ha-port-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = ()
        config = HDFSConfig.get_external_config()

        self.assertEquals(len(config), 1)
        self.assertEquals(config[0]['namenode'], 'testhost.net')
        self.assertEquals(config[0]['port'], 8888)
        self.assertFalse(HDFSConfig.use_trash)

    @patch('os.environ.get')
    def test_ha_without_ports(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_noport_settings(config)

    @patch('os.environ.get')
    def test_ha_config_trash_in_core(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('core-with-trash.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_noport_settings(config)
        self.assertTrue(HDFSConfig.use_trash)

    @patch('os.environ.get')
    def test_ha_config_trash_in_hdfs(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-trash-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_noport_settings(config)
        self.assertTrue(HDFSConfig.use_trash)

    @patch('os.environ.get')
    def test_autoconfig_client_trash_true(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-trash-hdfs-site.xml'),)
        client = AutoConfigClient()
        self.assertTrue(client.use_trash)

    @patch('os.environ.get')
    def test_autoconfig_client_trash_false(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-hdfs-site.xml'),)
        client = AutoConfigClient()
        self.assertFalse(client.use_trash)
