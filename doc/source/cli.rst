**********
CLI client
**********
A command line interface for HDFS using :mod:`snakebite.client <client>`.

Config
======

Snakebite CLI can accept configuration in a couple of different ways,
but there's strict priority for each of them.
List of methods, in priority order:

1. via path in command line - eg: ``hdfs://namenode_host:port/path``
2. via ``-n``, ``-p``, ``-V`` flags in command line
3. via ``~/.snakebiterc`` file
4. via ``/etc/snakebiterc`` file
5. via ``$HADOOP_HOME/core-site.xml`` and/or ``$HADOOP_HOME/hdfs-site.xml`` files
6. via ``core-site.xml`` and/or ``hdfs-site.xml`` in default locations

More about methods from 3 to 6 below.

Config files
^^^^^^^^^^^^

Snakebite config can exist in ``~/.snakebiterc`` - per system user, or in
``/etc/snakebiterc`` - system wide config.

A config looks like:

::

  {
      "config_version": 2,
      "skiptrash": true,
      "namenodes": [
          {"host": "mynamenode1", "port": 8020, "version": 9},
          {"host": "mynamenode2", "port": 8020, "version": 9}
      ]
  }


The version property denotes the protocol version used. CDH 4.1.3 uses protocol 7, while
HDP 2.0 uses protocol 9. Snakebite defaults to 9. Default port of namenode is 8020.
Default value of ``skiptrash`` is ``true``.

Hadoop config files
^^^^^^^^^^^^^^^^^^^

Last two methods of providing config for snakebite is through hadoop config files.
If either ``HADOOP_HOME`` or ``HADOOP_CONF_DIR`` environment variable is set, snakebite will try to find ``core-site.xml``
and/or ``hdfs-site.xml`` files in ``$HADOOP_HOME/conf`` or ``$HADOOP_CONF_DIR`` directory. If ``HADOOP_HOME`` or ``HADOOP_CONF_DIR`` is not set,
snakebite will try to find those files in a couple of default hadoop config locations:

* /etc/hadoop/conf/core-site.xml
* /usr/local/etc/hadoop/conf/core-site.xml
* /usr/local/hadoop/conf/core-site.xml
* /etc/hadoop/conf/hdfs-site.xml
* /usr/local/etc/hadoop/conf/hdfs-site.xml
* /usr/local/hadoop/conf/hdfs-site.xml

Bash completion
===============

Snakebite CLI comes with bash completion file in /scripts. If snakebite is installed
via debian package it will install completion file automatically. But if snakebite
is installed via pip/setup.py it will not do that, as it would requite write access
in /etc (usually root), in that case it's required to install completion script manually.

Usage
=====
::

    snakebite [general options] cmd [arguments]
    general options:
      -D --debug                     Show debug information
      -V --version                   Hadoop protocol version (default:9)
      -h --help                      show help
      -j --json                      JSON output
      -n --namenode                  namenode host
      -p --port                      namenode RPC port (default: 8020)
      -v --ver                       Display snakebite version

    commands:
      cat [paths]                    copy source paths to stdout
      chgrp <grp> [paths]            change group
      chmod <mode> [paths]           change file mode (octal)
      chown <owner:grp> [paths]      change owner
      copyToLocal [paths] dst        copy paths to local file system destination
      count [paths]                  display stats for paths
      df                             display fs stats
      du [paths]                     display disk usage statistics
      get file dst                   copy files to local file system destination
      getmerge dir dst               concatenates files in source dir into destination local file
      ls [paths]                     list a path
      mkdir [paths]                  create directories
      mkdirp [paths]                 create directories and their parents
      mv [paths] dst                 move paths to destination
      rm [paths]                     remove paths
      rmdir [dirs]                   delete a directory
      serverdefaults                 show server information
      setrep <rep> [paths]           set replication factor
      stat [paths]                   stat information
      tail path                      display last kilobyte of the file to stdout
      test path                      test a path
      text path [paths]              output file in text format
      touchz [paths]                 creates a file of zero length
      usage <cmd>                    show cmd usage

    to see command-specific options use: snakebite [cmd] --help
