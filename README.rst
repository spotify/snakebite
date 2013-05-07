=========
Snakebite
=========
Snakebite is a python library that provides a pure python HDFS client and a wrapper around Hadoops minicluster. 
The client uses protobuf for communicating with the NameNode and comes in the form of a library and a command line interface.
Currently, the snakebite client only supports actions that only involve the Namenode.

Snakebite client uses python-protobuf 2.3, since that's what is available at Spotify.

Snakebite has been tested against Cloudera CDH4.1.3 (hadoop 2.0.0)

For more information and documentation can be found at http://spotify.github.io/snakebite/

Copyright 2013 Spotify AB
