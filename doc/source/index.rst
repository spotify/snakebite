#######################
Snakebite documentation
#######################
Snakebite is a python package that provides:

.. toctree::
   :hidden:

   client
   cli
   development
   testing
   minicluster
   hadoop_rpc


* :doc:`A pure python HDFS client library that uses protobuf messages over Hadoop RPC to communicate with HDFS. <client>`
* :doc:`A command line interface (CLI) for HDFS that uses the pure python client library. <cli>`
* :doc:`A hadoop minicluster wrapper. <minicluster>`
* :doc:`Hadoop RPC specification. <hadoop_rpc>`

Background
==========
Since the 'normal' Hadoop HDFS client (``hadoop fs``) is written in Java and has
a lot of dependencies on Hadoop jars, startup times are quite high (> 3 secs).
This isn't ideal for integrating Hadoop commands in python projects.

At Spotify we use the `luigi job scheduler <http://github.com/spotify/luigi>`_
that relies on doing a lot of existence checks and moving data around in HDFS.
And since calling ``hadoop`` from python is expensive, we decided to write a
pure python HDFS client that only relies on protobuf. The current
:mod:`snakebite.client <client>` library uses protobuf messages and
implements the Hadoop RPC protocol for talking to the NameNode.

During development, we needed to verify :mod:`snakebite.client <client>`
behavior against the real client and for that we implemented a :mod:`minicluster`
that wraps a Hadoop Java mini cluster. Obviously this :mod:`minicluster` can be
used in different projects, so we made it a part of snakebite.

And since it's nice to have a CLI that uses :mod:`snakebite.client <client>`
we've implemented a :doc:`cli` as well.

.. warning:: all methods that read data from a data node are able to check the
   CRC during transfer, but this is disabled by default because of performance
   reasons. This is the opposite behaviour from the stock Hadoop client.

LICENSE
=======
Copyright (c) 2013 - 2014 Spotify AB

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

Code in :mod:`channel`, :mod:`logger` and :mod:`service` was borrowed from https://code.google.com/p/protobuf-socket-rpc/ and
carries it's respective license.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

