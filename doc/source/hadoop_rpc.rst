Hadoop RPC protocol description
===============================

Snakebite currently implements the following protocol in
:class:`snakebite.channel.SocketRpcChannel` to communicate with the NameNode.

=============
Connection
=============
The Hadoop RPC protocol works as described below. On connection, headers are
sent to setup a session. After that, multiple requests can be sent within the session.

+----------------------------------+----------------+--------------------------------------+
| Function                         | Type           | Default                              |
+==================================+================+======================================+
| Header                           | :class:`bytes` | "hrpc"                               |
+----------------------------------+----------------+--------------------------------------+
| Version                          | :class:`uint8` | 7                                    |
+----------------------------------+----------------+--------------------------------------+
| Auth method                      | :class:`uint8` | 80 (Auth method :class:`SIMPLE`)     |
+----------------------------------+----------------+--------------------------------------+
| Serialization type               | :class:`uint8` | 0 (:class:`protobuf`)                |
+----------------------------------+----------------+--------------------------------------+
| IpcConnectionContextProto length | :class:`uint32`|                                      |
+----------------------------------+----------------+--------------------------------------+
| IpcConnectionContextProto        | :class:`bytes` |                                      |
+----------------------------------+----------------+--------------------------------------+

==================
Sending messages
==================

When sending a message, the following is sent to the sever:

+----------------------------------+-----------------------------------------+
| Function                         | Type                                    |
+==================================+=========================================+
| Length of the next two parts     | :class:`uint32`                         |
+----------------------------------+-----------------------------------------+
| RpcPayloadHeaderProto length     | :class:`varint`                         |
+----------------------------------+-----------------------------------------+
| RpcPayloadHeaderProto            | :class:`protobuf serialized message`    |
+----------------------------------+-----------------------------------------+
| HadoopRpcRequestProto length     | :class:`varint`                         |
+----------------------------------+-----------------------------------------+
| HadoopRpcRequestProto            | :class:`protobuf serialized message`    |
+----------------------------------+-----------------------------------------+

:class:`varint` is a `Protocol Buffer variable int <https://developers.google.com/protocol-buffers/docs/encoding#varints>`_. 

.. note::
    The Java protobuf implementation uses :class:`writeToDelimited` to prepend
    the message with their lenght, but the python implementation doesn't implement
    such a method (yet).

Next to an :class:`rpcKind` (snakebites default is :class:`RPC_PROTOCOL_BUFFER`),
an :class:`rpcOp` (snakebites default is :class:`RPC_FINAL_PAYLOAD`), the
:class:`RpcPayloadHeaderProto` message defines a :class:`callId` that is added
in the RPC response (described below).

The :class:`HadoopRpcRequestProto` contains a :class:`methodName` field that defines
what server method is called and a has a property :class:`request` that contains the
serialized actual request message.

====================
Receiving messages
====================

After a message is sent, the response can be read in the following way:

+----------------------------------------------+-----------------+
| Function                                     | Type            |
+==============================================+=================+
| Length of the RpcResponseHeaderProto         | :class:`varint` |
+----------------------------------------------+-----------------+
| RpcResponseHeaderProto                       | :class:`bytes`  |
+----------------------------------------------+-----------------+
| Length of the RPC response                   | :class:`uint32` |
+----------------------------------------------+-----------------+
| Serialized RPC response                      | :class:`bytes`  |
+----------------------------------------------+-----------------+

The :class:`RpcResponseHeaderProto` contains the :class:`callId` of the request
and a status field. The status can be :class:`SUCCESS`, :class:`ERROR` or 
:class:`FAILURE`. In case :class:`SUCCESS` the rest of response is a complete
protobuf response.

In case of :class:`ERROR`, the response looks like follows:

+----------------------------------------+-----------------------+
| Function                               | Type                  |
+========================================+=======================+
| Length of the RpcResponseHeaderProto   | :class:`varint`       |
+----------------------------------------+-----------------------+
| RpcResponseHeaderProto                 | :class:`bytes`        |
+----------------------------------------+-----------------------+
| Length of the RPC response             | :class:`uint32`       |
+----------------------------------------+-----------------------+
| Length of the Exeption class name      | :class:`uint32`       |
+----------------------------------------+-----------------------+
| Exception class name                   | :class:`utf-8 string` |
+----------------------------------------+-----------------------+
| Length of the stack trace              | :class:`uint32`       |
+----------------------------------------+-----------------------+
| Stack trace                            | :class:`utf-8 string` |
+----------------------------------------+-----------------------+
