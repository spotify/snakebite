import os
import logging
import xml.etree.ElementTree as ET

from urlparse import urlparse
from namenode import Namenode

log = logging.getLogger(__name__)

class HDFSConfig(object):
    use_trash = False

    @staticmethod
    def read_hadoop_config(hdfs_conf_path):
        config_entries = []
        if os.path.exists(hdfs_conf_path):
            try:
                tree = ET.parse(hdfs_conf_path)
            except:
                log.error("Unable to parse %s" % hdfs_conf_path)
                return config_entries
            root = tree.getroot()
            config_entries.extend(root.findall("./property"))
        return config_entries

    @classmethod
    def read_core_config(cls, core_site_path):
        conf = cls.read_hadoop_config(core_site_path)
        configs = []
        for property in conf:
            if property.findall('name')[0].text == 'fs.defaultFS':
                parse_result = urlparse(property.findall('value')[0].text)
                configs.append(cls.get_namenode(parse_result))

            cls.set_trash_mode(property)

        return configs

    @classmethod
    def read_hdfs_config(cls, config_path, conf, environment, environment_suffixes):
        configs = []
        for property in conf:
            if cls.is_namenode_host(property, environment, environment_suffixes):
                parse_result = urlparse("//" + property.findall('value')[0].text)
                log.debug("Got namenode '%s' from %s" % (parse_result.geturl(), config_path))
                configs.append(cls.get_namenode(parse_result))

            cls.set_trash_mode(property)

        return configs

    @classmethod
    def is_namenode_host(cls, property, environment, environment_suffixes):
        name = property.findall('name')[0].text
        if not environment_suffixes:
            return name.startswith("dfs.namenode.rpc-address")
        for suffix in environment_suffixes:
            if name.startswith("dfs.namenode.rpc-address." + environment + "." + suffix):
                return True
        return False

    @classmethod
    def get_namenode(cls, parse_result):
        return {
            "namenode": parse_result.hostname,
            "port": parse_result.port if parse_result.port else Namenode.DEFAULT_PORT
        }

    @classmethod
    def set_trash_mode(cls, property):
        if property.findall('name')[0].text == 'fs.trash.interval':
                cls.use_trash = True

    @classmethod
    def get_environment(cls, core_config):
        for config in core_config:
            environment_name = config['namenode']
            if environment_name:
                return environment_name

    @classmethod
    def get_environment_suffixes(cls, environment, hadoop_config):
        for property in hadoop_config:
            if property.findall('name')[0].text == 'dfs.ha.namenodes.' + environment:
                return property.findall('value')[0].text.split(',')


    core_try_paths = ('/etc/hadoop/conf/core-site.xml',
                      '/usr/local/etc/hadoop/conf/core-site.xml',
                      '/usr/local/hadoop/conf/core-site.xml')

    hdfs_try_paths = ('/etc/hadoop/conf/hdfs-site.xml',
                      '/usr/local/etc/hadoop/conf/hdfs-site.xml',
                      '/usr/local/hadoop/conf/hdfs-site.xml')

    @classmethod
    def get_external_config(cls):
        if os.environ.get('HADOOP_HOME'):
            core_paths = [os.path.join(os.environ['HADOOP_HOME'], 'conf', 'core-site.xml')]
            hdfs_paths = [os.path.join(os.environ['HADOOP_HOME'], 'conf', 'hdfs-site.xml')]
        else:
            # Try to find other paths
            core_paths = cls.core_try_paths
            hdfs_paths = cls.hdfs_try_paths

        configs = []
        for core_conf_path in core_paths:
            configs = cls.read_core_config(core_conf_path)
            if configs:
                break

        environment = cls.get_environment(configs)

        for hdfs_conf_path in hdfs_paths:
            hadoop_config = cls.read_hadoop_config(hdfs_conf_path)
            environment_suffixes = cls.get_environment_suffixes(environment, hadoop_config)
            tmp_config = cls.read_hdfs_config(hdfs_conf_path, hadoop_config, environment, environment_suffixes)
            if tmp_config:
                # if there is hdfs-site data available return it
                return tmp_config

        if not configs:
            raise Exception("No configs found")

        return configs
