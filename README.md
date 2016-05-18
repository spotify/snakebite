![Snakebite mini logo](https://github.com/spotify/snakebite/blob/master/doc/logo/logo-mini-typo.png)
---

Snakebite is a python library that provides a pure python HDFS client
and a wrapper around Hadoops minicluster. The client uses protobuf for
communicating with the NameNode and comes in the form of a library and a
command line interface. Currently, the snakebite client supports most
actions that involve the Namenode and reading data from DataNodes.

*Note:* all methods that read data from a data node are able to check
the CRC during transfer, but this is disabled by default because of
performance reasons. This is the opposite behaviour from the stock
Hadoop client.

Snakebite requires python2 (python3 is not supported yet) and
python-protobuf 2.4.1 or higher.

Snakebite 1.3.x has been tested mainly against Cloudera CDH4.1.3 (hadoop
2.0.0) in production. Tests pass on HortonWorks HDP 2.0.3.22-alpha
(protocol versions 7 and 8)

Snakebite 2.x has been tested on Hortonworks HDP2.0 and CDH5 Beta and
ONLY supports Hadoop 2.2.0 and up (protocol version 9)!

Installing
==========

Snakebite releases are available through pypi at
<https://pypi.python.org/pypi/snakebite/>

To install snakebite run:

`pip install snakebite`

To install snakebite 2.x with Kerberos/SASL support, make sure you can
install python-krbV (<https://fedorahosted.org/python-krbV/>) and then
run:

`pip install "snakebite[kerberos]"`

Since the older version of snakebite (1.3.x) supports Hadoop 1.0 (instead of Hadoop 2), you
might want to install an older version by running:

`pip install -I snakebite==1.3.x`

Note that the 1.3 branch is unmaintained and doesn't include any of the fixes in the 2.x branch.

Documentation
=============

More information and documentation can be found at http://snakebite.readthedocs.org/en/latest/

Development
===========

Make sure to read about development
[here](http://snakebite.readthedocs.org/en/latest/development.html) and about
testing over [here](http://snakebite.readthedocs.org/en/latest/testing.html),
hack and come back with a pull requests &lt;3

Travis CI status: [![Travis](https://api.travis-ci.org/spotify/snakebite.png)](https://travis-ci.org/spotify/snakebite)
[![Join the chat at https://gitter.im/spotify/snakebite](https://badges.gitter.im/spotify/snakebite.svg)](https://gitter.im/spotify/snakebite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Copyright 2013-2016 Spotify AB
