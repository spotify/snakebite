import unittest2
import os
import time

import os.path
import snakebite
from snakebite.config import HDFSConfig
from snakebite.client import AutoConfigClient
from mock import patch, mock_open

class ConfigTest(unittest2.TestCase):
    original_hdfs_try_path = HDFSConfig.hdfs_try_paths
    original_core_try_path = HDFSConfig.core_try_paths

    def setUp(self):
        # Make sure HDFSConfig is in vanilla state
        HDFSConfig.use_trash = False
        HDFSConfig.hdfs_try_paths = self.original_hdfs_try_path
        HDFSConfig.core_try_paths = self.original_core_try_path

    @staticmethod
    def get_config_path(config_name):
        return os.path.abspath(os.path.join(snakebite.__file__, os.pardir, os.pardir, 'test/testconfig/conf/%s' % config_name))

    def _verify_hdfs_settings(self, config):
        namenodes = config['namenodes']
        self.assertEquals(len(namenodes), 2)
        # assert first NN
        self.assertEqual('namenode1.mydomain', namenodes[0]['namenode'])
        self.assertEqual(8888, namenodes[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', namenodes[1]['namenode'])
        self.assertEqual(8888, namenodes[1]['port'])

    def _verify_hdfs_noport_settings(self, config):
        namenodes = config['namenodes']
        self.assertEquals(len(namenodes), 2)
        # assert first NN
        self.assertEqual('namenode1.mydomain', namenodes[0]['namenode'])
        self.assertEqual(8020, namenodes[0]['port'])
        # assert second NN
        self.assertEqual('namenode2.mydomain', namenodes[1]['namenode'])
        self.assertEqual(8020, namenodes[1]['port'])

    def test_read_hdfs_config_ha(self):
        hdfs_site_path = self.get_config_path('ha-port-hdfs-site.xml')
        config = HDFSConfig.read_hdfs_config(hdfs_site_path, 'testha')
        self._verify_hdfs_settings(config)

    def test_read_core_config_ha(self):
        core_site_path = self.get_config_path('ha-core-site.xml')
        config = HDFSConfig.read_core_config(core_site_path)
        namenodes = config['namenodes']
        self.assertEquals(len(namenodes), 1)
        self.assertEquals('testha', namenodes[0]['namenode'])
        self.assertEquals(8020, namenodes[0]['port'])

    def test_read_core_config_emr(self):
        core_site_path = self.get_config_path('emr-core-site.xml')
        config = HDFSConfig.read_core_config(core_site_path)
        namenodes = config['namenodes']
        self.assertEquals(len(namenodes), 1)
        self.assertEquals('testha', namenodes[0]['namenode'])
        self.assertEquals(8020, namenodes[0]['port'])

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

        namenodes = config['namenodes']
        self.assertEquals(len(namenodes), 1)
        self.assertEquals(namenodes[0]['namenode'], 'testhost.net')
        self.assertEquals(namenodes[0]['port'], 8888)
        self.assertFalse(config['use_trash'])

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
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-with-trash.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_noport_settings(config)
        self.assertTrue(config['use_trash'])

    @patch('os.environ.get')
    def test_ha_config_trash_in_hdfs(self, environ_get):
        environ_get.return_value = False
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-noport-trash-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_noport_settings(config)
        self.assertTrue(config['use_trash'])

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

    def test_retry_configs(self):
        conf_path = self.get_config_path('ha-retry-hdfs-site.xml')
        config = HDFSConfig.read_hdfs_config(conf_path)
        self.assertEquals(config['client_retries'], 5)
        self.assertEquals(config['client_sleep_base_millis'], 400)
        self.assertEquals(config['client_sleep_max_millis'], 14000)
        self.assertEquals(config['socket_timeout_millis'], 25000)
        self.assertEquals(config['failover_max_attempts'], 7)

    def test_use_datanode_hostname_configs(self):
        conf_path = self.get_config_path('use-datanode-hostname-hdfs-site.xml')
        config = HDFSConfig.read_hdfs_config(conf_path)
        self.assertTrue(config['use_datanode_hostname'])

    def test_ha_multi(self):
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-multi-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self._verify_hdfs_settings(config)

    def test_ha_multi_missing_nameservices(self):
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-multi-no-nameservices-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self.assertEquals(config['namenodes'], [{'namenode': 'testha', 'port': 8020}])

    def test_ha_multi_bad_logical_nn_mapping(self):
        HDFSConfig.core_try_paths = (self.get_config_path('ha-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-multi-bad-nn-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        self.assertEquals(config['namenodes'], [{'namenode': 'testha', 'port': 8020}])

    def test_ha_multi_missing_default_fs(self):
        HDFSConfig.core_try_paths = (self.get_config_path('ha-no-default-fs-core-site.xml'),)
        HDFSConfig.hdfs_try_paths = (self.get_config_path('ha-multi-hdfs-site.xml'),)
        config = HDFSConfig.get_external_config()

        print config
        self.assertEquals(config['namenodes'], [])
