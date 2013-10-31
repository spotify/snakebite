#!/usr/bin/env bash
export HADOOP_HOME=/tmp/hadoop

# Fix /etc/hosts so Java will not break
echo "Fixing /etc/hosts"
sudo sed -e "s/^127.0.0.1.*/127.0.0.1 localhost $(hostname)/" --in-place /etc/hosts

mkdir $HADOOP_HOME

if [ $HADOOP_DISTRO = "cdh" ]; then
    URL="http://archive.cloudera.com/cdh4/cdh/4/hadoop-2.0.0-cdh4.2.1.tar.gz"
    export HADOOP_PROTOCOL_VER=7
elif [ $HADOOP_DISTRO = "hdp" ]; then
    URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0/tars/hadoop-2.2.0.2.0.6.0-76.tar.gz"
    export HADOOP_PROTOCOL_VER=9
else
    echo "No HADOOP_DISTRO specified"
    exit 1
fi

echo "Downloading Hadoop from $URL"
wget $URL -O hadoop.tar.gz

echo "Extracting hadoop into $HADOOP_HOME"
tar zxf hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME

echo "Running apt-get update"
sudo apt-get update -qq