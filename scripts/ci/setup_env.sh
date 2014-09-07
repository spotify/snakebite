#!/usr/bin/env bash

HADOOP_DISTRO=${HADOOP_DISTRO:-"hdp"}
HADOOP_HOME=/tmp/hadoop-${HADOOP_DISTRO}

mkdir -p $HADOOP_HOME

if [ $HADOOP_DISTRO = "cdh" ]; then
    URL="http://archive.cloudera.com/cdh5/cdh/5/hadoop-latest.tar.gz"
elif [ $HADOOP_DISTRO = "hdp" ]; then
    URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0/tars/hadoop-2.2.0.2.0.6.0-76.tar.gz"
else
    echo "No HADOOP_DISTRO specified"
    exit 1
fi

echo "Downloading Hadoop from $URL to ${HADOOP_HOME}/hadoop.tar.gz"
curl -z ${HADOOP_HOME}/hadoop.tar.gz -o ${HADOOP_HOME}/hadoop.tar.gz -L $URL

if [ $? != 0 ]; then
    echo "Failed to download Hadoop from $URL - abort" >&2
    exit 1
fi

echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
tar zxf ${HADOOP_HOME}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME

if [ $? != 0 ]; then
    echo "Failed to extract Hadoop from ${HADOOP_HOME}/hadoop.tar.gz to ${HADOOP_HOME} - abort" >&2
    exit 1
fi
