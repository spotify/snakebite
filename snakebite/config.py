import os
import xml.etree.ElementTree as ET

from urlparse import urlparse

def get_config_from_env():
    hdfs_conf = os.path.join(os.environ['HADOOP_HOME'], 'conf', 'core-site.xml')
    config = read_hadoop_config(hdfs_conf)
    if config:
        return config
    else:
        raise Exception("No config found in %s" % hdfs_conf)


def read_hadoop_config(hdfs_conf):
    if os.path.exists(hdfs_conf):
        tree = ET.parse(hdfs_conf)
        root = tree.getroot()
        for p in root.findall("./property"):
            if p.findall('name')[0].text == 'fs.defaultFS':
                parse_result = urlparse(p.findall('value')[0].text)
                return (parse_result.hostname, parse_result.port)