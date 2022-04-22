import os
import logging
import xml.etree.ElementTree as ET
from urlparse import urlparse

from namenode import Namenode

log = logging.getLogger(__name__)


class HDFSConfig(object):
    @classmethod
    def get_config_from_env(cls):
        """
        .. deprecated:: 2.5.3
        Gets configuration out of environment.

        Returns list of dicts - list of namenode representations
        """
        core_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'core-site.xml')
        core_configs = cls.read_core_config(core_path)

        maybe_ha_name = None
        if len(core_configs.get('namenodes', [])) == 1:
            # We may have gotten an HA config from core-site.xml, try
            # to use it to resolve HA names
            maybe_ha_name = core_configs.get('namenodes')[0].get('namenode')

        hdfs_path = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'hdfs-site.xml')
        # May have interpreted defaultFS as a NN, pass this to
        # read_hdfs_config to try to resolve an HA configuration
        hdfs_configs = cls.read_hdfs_config(hdfs_path, maybe_ha_name)

        if (not core_configs) and (not hdfs_configs):
            raise Exception("No config found in %s nor in %s" % (core_path, hdfs_path))

        configs = {
            'use_trash': hdfs_configs.get('use_trash', core_configs.get('use_trash', False)),
            'use_sasl': core_configs.get('use_sasl', False),
            'hdfs_namenode_principal': hdfs_configs.get('hdfs_namenode_principal', None),
            'namenodes': hdfs_configs.get('namenodes', []) or core_configs.get('namenodes', [])
        }

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
        configs = {}

        namenodes = []
        for property in cls.read_hadoop_config(core_site_path):

            # fs.default.name is the key name for the file system on EMR clusters
            if property.findall('name')[0].text in ('fs.defaultFS', 'fs.default.name'):
                parse_result = urlparse(property.findall('value')[0].text)
                log.debug("Got namenode '%s' from %s" % (parse_result.geturl(), core_site_path))

                namenodes.append({"namenode": parse_result.hostname,
                               "port": parse_result.port if parse_result.port
                                                         else Namenode.DEFAULT_PORT})

            if property.findall('name')[0].text == 'fs.trash.interval':
                configs['use_trash'] = True

            if property.findall('name')[0].text == 'hadoop.security.authentication':
                log.debug("Got hadoop.security.authentication '%s'" % (property.findall('value')[0].text))
                if property.findall('value')[0].text == 'kerberos':
                    configs['use_sasl'] = True
                else:
                    configs['use_sasl'] = False

        if namenodes:
            configs['namenodes'] = namenodes

        return configs

    @classmethod
    def read_hdfs_config(cls, hdfs_site_path, maybe_ha_name=None):
        configs = {}
        ha_configs = cls.read_hdfs_ha_configs(hdfs_site_path)

        namenodes = []
        for property in cls.read_hadoop_config(hdfs_site_path):
            if property.findall('name')[0].text.startswith('dfs.namenode.rpc-address'):
                prop_name = property.findall('name')[0].text
                parse_result = urlparse("//" + property.findall('value')[0].text)

                if (prop_name == 'dfs.namenode.rpc-address' or
                    cls.valid_ha_namenode(maybe_ha_name,
                                          ha_configs,
                                          parse_result.geturl(),
                                          hdfs_site_path,
                                          prop_name)):
                    log.debug("Got namenode '%s' from %s" % (parse_result.geturl(), hdfs_site_path))
                    namenodes.append({"namenode": parse_result.hostname,
                                    "port": parse_result.port if parse_result.port
                                    else Namenode.DEFAULT_PORT})

            if property.findall('name')[0].text == 'fs.trash.interval':
                configs['use_trash'] = True

            if property.findall('name')[0].text == 'dfs.namenode.kerberos.principal':
                log.debug("hdfs principal found: '%s'" % (property.findall('value')[0].text))
                configs['hdfs_namenode_principal'] = property.findall('value')[0].text

            if property.findall('name')[0].text == 'dfs.client.retry.max.attempts':
                configs['client_retries'] = int(property.findall('value')[0].text)

            if property.findall('name')[0].text == 'dfs.client.socket-timeout':
                configs['socket_timeout_millis'] = int(property.findall('value')[0].text)

            if property.findall('name')[0].text == 'dfs.client.failover.sleep.base.millis':
                configs['client_sleep_base_millis'] = int(property.findall('value')[0].text)

            if property.findall('name')[0].text == 'dfs.client.failover.sleep.max.millis':
                configs['client_sleep_max_millis'] = int(property.findall('value')[0].text)

            if property.findall('name')[0].text == 'dfs.client.failover.max.attempts':
                configs['failover_max_attempts'] = int(property.findall('value')[0].text)

            if property.findall('name')[0].text == 'dfs.client.use.datanode.hostname':
                configs['use_datanode_hostname'] = bool(property.findall('value')[0].text)

        if namenodes:
            configs['namenodes'] = namenodes

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
        core_configs = {}
        for core_conf_path in cls.core_try_paths:
            core_configs = cls.read_core_config(core_conf_path)
            if core_configs:
                break

        hdfs_configs = {}
        for hdfs_conf_path in cls.hdfs_try_paths:
            if len(core_configs.get('namenodes', [])) == 1:
                hdfs_configs = cls.read_hdfs_config(hdfs_conf_path,
                                                    core_configs.get('namenodes')[0].get('namenode'))
            else:
                hdfs_configs = cls.read_hdfs_config(hdfs_conf_path)
            if hdfs_configs:
                break

        configs = {
            'use_trash': hdfs_configs.get('use_trash', core_configs.get('use_trash', False)),
            'use_sasl': core_configs.get('use_sasl', False),
            'hdfs_namenode_principal': hdfs_configs.get('hdfs_namenode_principal', None),
            'namenodes': hdfs_configs.get('namenodes', []) or core_configs.get('namenodes', []),
            'client_retries' : hdfs_configs.get('client_retries', 10),
            'client_sleep_base_millis' : hdfs_configs.get('client_sleep_base_millis', 500),
            'client_sleep_max_millis' : hdfs_configs.get('client_sleep_max_millis', 15000),
            'socket_timeout_millis' : hdfs_configs.get('socket_timeout_millis', 60000),
            'failover_max_attempts' : hdfs_configs.get('failover_max_attempts', 15),
            'use_datanode_hostname' : hdfs_configs.get('use_datanode_hostname', False)
        }

        return configs

    @classmethod
    def valid_ha_namenode(cls, maybe_ha_name, ha_configs, nn_url, hdfs_site_path, name):
        name_parts = name.split('.')

        if len(name_parts) != 5:
            log.debug("Could not parse cluster name from %s, skipping %s from %s" %
                      (name,
                       nn_url,
                       hdfs_site_path))
            return False

        cluster = name_parts[-2]
        if cluster != maybe_ha_name:
            log.debug("Skipping %s from %s, becuause it does not belong to our active cluster %s" %
                          (nn_url, hdfs_site_path, maybe_ha_name))
            return False

        if cluster not in ha_configs.get('clusters', []):
            log.debug("Skipping %s from %s, becuause it is no in the configured cluster list: %s" %
                          (nn_url, hdfs_site_path, ha_configs.get('clusters')))
            return False

        logical_namenode = name_parts[-1]
        cluster_logical_namenodes = (ha_configs
                                     .get('logical_namenodes', {})
                                     .get(cluster, []))
        if logical_namenode not in cluster_logical_namenodes:
            log.debug("Could not find logical mapping for %s in cluster %s from %s, skipping" %
                          (nn_url,
                           cluster,
                           hdfs_site_path))
            return False
        return True

    @classmethod
    def read_hdfs_ha_configs(cls, hdfs_site_path):
        ha_configs = {}
        for property in cls.read_hadoop_config(hdfs_site_path):
            name = property.findall('name')[0].text
            value = property.findall('value')[0].text

            if name == 'dfs.nameservices':
                ha_configs['clusters'] = value.split(',')

            if name.startswith('dfs.ha.namenodes'):
                name_parts = property.findall('name')[0].text.split('.')
                if len(name_parts) != 4:
                    log.debug("Could not parse cluster name from %s, skipping" %
                              (property.findall('name')[0].text))
                    continue
                if 'logical_namenodes' not in ha_configs:
                    ha_configs['logical_namenodes'] = {}
                cluster = name_parts[-1]
                ha_configs['logical_namenodes'][cluster] = value.split(',')
        return ha_configs
