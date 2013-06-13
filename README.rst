=========
Snakebite
=========
Snakebite is a python library that provides a pure python HDFS client and a wrapper around Hadoops minicluster. 
The client uses protobuf for communicating with the NameNode and comes in the form of a library and a command line interface.
Currently, the snakebite client only supports actions that only involve the Namenode.

Snakebite requires python2 (python3 is not supported yet) and python-protobuf 2.4.1 or higher.

Snakebite has been tested mainly against Cloudera CDH4.1.3 (hadoop 2.0.0) in production. Tests pass on HortonWorks HDP 2.0.3.22-alpha

The quickest way to install snakebite is to run:

  pip install snakebite

For more information and documentation can be found at http://spotify.github.io/snakebite/

Travis CI status: 

.. image:: https://api.travis-ci.org/spotify/snakebite.png

Copyright 2013 Spotify AB
