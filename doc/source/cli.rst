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

    snakebite [general options] cmd [arguments]
    general options:
      -D --debug                     Show debug information
      -V --version                   Hadoop protocol version (default:8)
      -j --json                      JSON output
      -n --namenode                  namenode host
      -p --port                      namenode RPC port

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