import os
import logging
import xml.etree.ElementTree as ET
from urlparse import urlparse

from namenode import Namenode

log = logging.getLogger(__name__)


class HDFSConfig(object):
    use_trash = False
    use_sasl = False

    @classmethod
    def get_config_from_env(cls):
        """
        .. deprecated:: 2.5.3
        Gets configuration out of environment.

        Returns list of dicts - list of namenode representations
        """
        core_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'core-site.xml')
        configs = cls.read_core_config(core_path)

        hdfs_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'hdfs-site.xml')
        tmp_config = cls.read_hdfs_config(hdfs_path)

        if tmp_config:
            # if config exists in hdfs - it's HA config, update configs
            configs = tmp_config

        if not configs:
            raise Exception("No config found in %s nor in %s" % (core_path, hdfs_path))

        return configs

    @staticmethod
    def read_hadoop_config(hdfs_conf_path):
        if os.path.exists(hdfs_conf_path):
            try:
                tree = ET.parse(hdfs_conf_path)
            except:
                log.error("Unable to parse %s" % hdfs_conf_path)
                return
            root = tree.getroot()
            for p in root.findall("./property"):
                yield p

    @classmethod
    def read_core_config(cls, core_site_path):
        config = []
        for property in cls.read_hadoop_config(core_site_path):

            # fs.default.name is the key name for the file system on EMR clusters
            if property.findall('name')[0].text in ('fs.defaultFS', 'fs.default.name'):
                parse_result = urlparse(property.findall('value')[0].text)
                log.debug("Got namenode '%s' from %s" % (parse_result.geturl(), core_site_path))

                config.append({"namenode": parse_result.hostname,
                               "port": parse_result.port if parse_result.port
                                                         else Namenode.DEFAULT_PORT})

            if property.findall('name')[0].text == 'fs.trash.interval':
                cls.use_trash = True

            if property.findall('name')[0].text == 'hadoop.security.authentication':
                log.debug("Got hadoop.security.authentication '%s'" % (property.findall('value')[0].text))
                if property.findall('value')[0].text == 'kerberos':
                    cls.use_sasl = True
                else:
                    cls.use_sasl = False
 
        return config

    @classmethod
    def read_hdfs_config(cls, hdfs_site_path):
        configs = []
        for property in cls.read_hadoop_config(hdfs_site_path):
            if property.findall('name')[0].text.startswith("dfs.namenode.rpc-address"):
                parse_result = urlparse("//" + property.findall('value')[0].text)
                log.debug("Got namenode '%s' from %s" % (parse_result.geturl(), hdfs_site_path))
                configs.append({"namenode": parse_result.hostname,
                                "port": parse_result.port if parse_result.port
                                                          else Namenode.DEFAULT_PORT})

            if property.findall('name')[0].text == 'fs.trash.interval':
                cls.use_trash = True

        return configs

    core_try_paths = ('/etc/hadoop/conf/core-site.xml',
                      '/usr/local/etc/hadoop/conf/core-site.xml',
                      '/usr/local/hadoop/conf/core-site.xml')

    hdfs_try_paths = ('/etc/hadoop/conf/hdfs-site.xml',
                      '/usr/local/etc/hadoop/conf/hdfs-site.xml',
                      '/usr/local/hadoop/conf/hdfs-site.xml')

    @classmethod
    def get_external_config(cls):
        if os.environ.get('HADOOP_HOME'):
            hdfs_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'hdfs-site.xml')
            cls.hdfs_try_paths = (hdfs_path,) + cls.hdfs_try_paths
            core_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'core-site.xml')
            cls.core_try_paths = (core_path,) + cls.core_try_paths
        if os.environ.get('HADOOP_CONF_DIR'):
            hdfs_path = os.path.join(os.environ['HADOOP_CONF_DIR'], 'hdfs-site.xml')
            cls.hdfs_try_paths = (hdfs_path,) + cls.hdfs_try_paths
            core_path = os.path.join(os.environ['HADOOP_CONF_DIR'], 'core-site.xml')
            cls.core_try_paths = (core_path,) + cls.core_try_paths

        # Try to find other paths
        configs = []
        for core_conf_path in cls.core_try_paths:
            configs = cls.read_core_config(core_conf_path)
            if configs:
                break

        for hdfs_conf_path in cls.hdfs_try_paths:
            tmp_config = cls.read_hdfs_config(hdfs_conf_path)
            if tmp_config:
                # if there is hdfs-site data available return it
                return tmp_config

        return configs
