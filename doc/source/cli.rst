CLI client
==========
A command line interface for HDFS using :mod:`snakebite.client <client>`.

The CLI client tries to read the config from ~/.snakebiterc and if that doesn't
exist, it will check $HADOOP_HOME/core-site.xml and create a ~/.snakebiterc from that.

A config looks like

::

  {
    "namenode": "<host/ip>",
    "port": 54310
  }

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
                            namenode host (default: localhost)
      -p PORT, --port=PORT  namenode RPC port (default: 54310)
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