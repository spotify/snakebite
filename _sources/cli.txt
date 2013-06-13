CLI client
==========
A command line interface for HDFS using :mod:`snakebite.client <client>`.

The CLI client first tries parse the path and in case it's in the form
``hdfs://namenode:port/path`` it will use that configuration.
Otherwise it will use -n and -p command line arguments.
If the previous aren't set it tries to read the config from ``~/.snakebiterc`` and
if that doesn't exist, it will check ``$HADOOP_HOME/core-site.xml`` and create a
``~/.snakebiterc`` from that.

A config looks like

::

  {
    "namenode": "<host/ip>",
    "port": 54310,
    "version": 7
  }

The version property denotes the protocol version used. CDH 4.1.3 uses protocol 7, while 
HDP 2.0 uses protocol 8. Snakebite defaults to 7.

Snakebite cli comes with bash completion inf /scripts.

Usage
=====
::

    Usage: snakebite [options] cmd [args]

    Options:
      -h, --help            show this help message and exit
      -D, --debug           Show debug information
      -j, --json            JSON output
      -n NAMENODE, --namenode=NAMENODE
                            namenode host
      -V VERSION, --version=VERSION
                            Hadoop protocol version (default:8
      -p PORT, --port=PORT  namenode RPC port
      -R, --recurse         recurse into subdirectories
      -d, --directory       show only the path and no children / check if path is
                            a dir
      -H, --human           human readable output
      -s, --summary         print summarized output
      -z, --zero            check for zero length
      -e, --exists          check if file exists

    Commands:
      chgrp <grp> [paths]            change group
      chmod <mode> [paths]           change file mode (octal)
      chown <owner:grp> [paths]      change owner
      count [paths]                  display stats for paths
      df                             display fs stats
      du [paths]                     display disk usage statistics
      ls [path]                      list a path
      mkdir [paths]                  create directories
      mkdirp [paths]                 create directories and their parents
      mv [paths] dst                 move paths to destination
      rm [paths]                     remove paths
      rmdir [dirs]                   delete a directory
      serverdefaults                 show server information
      setrep <rep> [paths]           set replication factor
      stat [paths]                   stat information
      test path                      test a path
      touchz [paths]                 creates a file of zero length
      usage <cmd>                    show cmd usage