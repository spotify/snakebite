import unittest2
import os
import time

from snakebite.config import HDFSConfig

class ConfigTest(unittest2.TestCase):

    def test_read_hdfs_config_ha(self):
       config = HDFSConfig.read_hdfs_config('test/testconfig/conf/hdfs-site.xml')
       # assert first NN
       self.assertEqual('namenode1.mydomain', config[0]['namenode'])
       self.assertEqual(8020, config[0]['port'])
       # assert second NN
       self.assertEqual('namenode2.mydomain', config[1]['namenode'])
       self.assertEqual(8020, config[1]['port'])
