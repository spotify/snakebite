Hadoop RPC protocol description
===============================

Snakebite currently implements the following protocol in
:py:data:`snakebite.channel.SocketRpcChannel` to communicate with the NameNode.

=============
Connection
=============
The Hadoop RPC protocol works as described below. On connection, headers are
sent to setup a session. After that, multiple requests can be sent within the session.

+----------------------------------+------------------+----------------------------------------+
| Function                         | Type             | Default                                |
+==================================+==================+========================================+
| Header                           | :py:data:`bytes` | "hrpc"                                 |
+----------------------------------+------------------+----------------------------------------+
| Version                          | :py:data:`uint8` | 7                                      |
+----------------------------------+------------------+----------------------------------------+
| Auth method                      | :py:data:`uint8` | 80 (Auth method :py:data:`SIMPLE`)     |
+----------------------------------+------------------+----------------------------------------+
| Serialization type               | :py:data:`uint8` | 0 (:py:data:`protobuf`)                |
+----------------------------------+------------------+----------------------------------------+
| IpcConnectionContextProto length | :py:data:`uint32`|                                        |
+----------------------------------+------------------+----------------------------------------+
| IpcConnectionContextProto        | :py:data:`bytes` |                                        |
+----------------------------------+------------------+----------------------------------------+

==================
Sending messages
==================

When sending a message, the following is sent to the sever:

+----------------------------------+-----------------------------------------+
| Function                         | Type                                    |
+==================================+=========================================+
| Length of the next two parts     | :py:data:`uint32`                       |
+----------------------------------+-----------------------------------------+
| RpcPayloadHeaderProto length     | :py:data:`varint`                       |
+----------------------------------+-----------------------------------------+
| RpcPayloadHeaderProto            | :py:data:`protobuf serialized message`  |
+----------------------------------+-----------------------------------------+
| HadoopRpcRequestProto length     | :py:data:`varint`                       |
+----------------------------------+-----------------------------------------+
| HadoopRpcRequestProto            | :py:data:`protobuf serialized message`  |
+----------------------------------+-----------------------------------------+

:py:data:`varint` is a `Protocol Buffer variable int <https://developers.google.com/protocol-buffers/docs/encoding#varints>`_. 

.. note::
    The Java protobuf implementation uses :py:data:`writeToDelimited` to prepend
    the message with their lenght, but the python implementation doesn't implement
    such a method (yet).

Next to an :py:data:`rpcKind` (snakebites default is :py:data:`RPC_PROTOCOL_BUFFER`),
an :py:data:`rpcOp` (snakebites default is :py:data:`RPC_FINAL_PAYLOAD`), the
:py:data:`RpcPayloadHeaderProto` message defines a :py:data:`callId` that is added
in the RPC response (described below).

The :py:data:`HadoopRpcRequestProto` contains a :py:data:`methodName` field that defines
what server method is called and a has a property :py:data:`request` that contains the
serialized actual request message.

====================
Receiving messages
====================

After a message is sent, the response can be read in the following way:

+----------------------------------------------+-------------------+
| Function                                     | Type              |
+==============================================+===================+
| Length of the RpcResponseHeaderProto         | :py:data:`varint` |
+----------------------------------------------+-------------------+
| RpcResponseHeaderProto                       | :py:data:`bytes`  |
+----------------------------------------------+-------------------+
| Length of the RPC response                   | :py:data:`uint32` |
+----------------------------------------------+-------------------+
| Serialized RPC response                      | :py:data:`bytes`  |
+----------------------------------------------+-------------------+

The :py:data:`RpcResponseHeaderProto` contains the :py:data:`callId` of the request
and a status field. The status can be :py:data:`SUCCESS`, :py:data:`ERROR` or 
:py:data:`FAILURE`. In case :py:data:`SUCCESS` the rest of response is a complete
protobuf response.

In case of :py:data:`ERROR`, the response looks like follows:

+----------------------------------------+-------------------------+
| Function                               | Type                    |
+========================================+=========================+
| Length of the RpcResponseHeaderProto   | :py:data:`varint`       |
+----------------------------------------+-------------------------+
| RpcResponseHeaderProto                 | :py:data:`bytes`        |
+----------------------------------------+-------------------------+
| Length of the RPC response             | :py:data:`uint32`       |
+----------------------------------------+-------------------------+
| Length of the Exeption class name      | :py:data:`uint32`       |
+----------------------------------------+-------------------------+
| Exception class name                   | :py:data:`utf-8 string` |
+----------------------------------------+-------------------------+
| Length of the stack trace              | :py:data:`uint32`       |
+----------------------------------------+-------------------------+
| Stack trace                            | :py:data:`utf-8 string` |
+----------------------------------------+-------------------------+
