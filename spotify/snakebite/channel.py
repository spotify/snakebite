# -*- coding: utf-8 -*-
# Copyright (c) 2009 Las Cumbres Observatory (www.lcogt.net)
# Copyright (c) 2010 Jan Dittberner
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
'''
channel.py - Socket implementation of Google's Protocol Buffers RPC
service interface.

This package contains classes providing a socket implementation of the
RPCChannel abstract class.

Original Authors: Martin Norbury (mnorbury@lcogt.net)
         Eric Saunders (esaunders@lcogt.net)
         Jan Dittberner (jan@dittberner.info)

May 2009, Nov 2010

Modified for snakebite: Wouter de Bie (wouter@spotify.com)

May 2012

'''

# Standard library imports
import socket
import os

# Third party imports
import google.protobuf.service as service
#from error import RpcError

# Protobuf imports
import spotify.snakebite.protobuf.RpcPayloadHeader_pb2 as rpcheaderproto
import spotify.snakebite.protobuf.IpcConnectionContext_pb2 as connectionContext
import spotify.snakebite.protobuf.hadoop_rpc_pb2 as hadoop_rpc

from spotify.snakebite.formatter import format_bytes
from spotify.snakebite.errors import RequestError

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

# Module imports

import logger
import struct

# Configure package logging
log = logger.getLogger(__name__)


class RpcBufferedReader(object):
    '''Class that wraps a socket and provides some utility methods for reading
    and rewinding of the buffer. This comes in handy when reading protobuf varints.
    '''
    def __init__(self, socket):
        self.socket = socket
        self.buffer = ""
        self.bytes_read = 0

    def read(self, n):
        '''Reads n bytes from the buffer. This will overwrite the internal buffer.'''
        self.buffer = self.socket.recv(n)
        self.bytes_read += n
        log.debug("Bytes read: %d, total: %d" % (n, self.bytes_read))
        return self.buffer

    def rewind(self, pos):
        '''Rewinds the current buffer to a position. Needed for reading varints,
        because we might read bytes that belong to the stream after the varint.
        '''
        self.buffer = self.buffer[pos:]
        log.debug("Reset buffer to pos %d" % pos)

    def read_more(self, n):
        '''Appends the current buffer with reading n bytes.'''
        self.buffer += self.socket.recv(n)
        self.bytes_read += n
        log.debug("More bytes read: %d, total: %d" % (n, self.bytes_read))
        return self.buffer

    def bytes_read(self):
        '''Returns amount of bytes read.'''
        return self.bytes_read

    def buffer_lenght(self):
        '''Returns the length of the current buffer.'''
        return len(self.buffer)


class SocketRpcChannel(service.RpcChannel):
    '''Socket implementation of an RpcChannel.
    '''

    def __init__(self, host, port):
        '''SocketRpcChannel to connect to a socket server on a user defined port.'''
        self.host = host
        self.port = port
        self.sock = None
        self.call_id = 0

    def validateRequest(self, request):
        '''Validate the client request against the protocol file.'''

        # Check the request is correctly initialized
        if not request.IsInitialized():
            print dir(request)
            raise Exception("Client request (%s) is missing mandatory fields" % type(request))

    def openSocket(self, host, port, context):
        '''Open a socket connection to a given host and port and writes the Hadoop header
        The Hadoop RPC protocol looks like this when creating a connection:

        +---------------------------------------------------------------------+
        |  Header, 4 bytes ("hrpc")                                           |
        +---------------------------------------------------------------------+
        |  Version, 1 byte (verion 7)                                         |
        +---------------------------------------------------------------------+
        |  Auth method, 1 byte (Auth method SIMPLE = 80)                      |
        +---------------------------------------------------------------------+
        |  Serialization type, 1 byte (Protobuf = 0)                          |
        +---------------------------------------------------------------------+
        |  Length of the IpcConnectionContextProto (4 bytes/32 bit int)       |
        +---------------------------------------------------------------------+
        |  Serialized IpcConnectionContextProto                               |
        +---------------------------------------------------------------------+
        '''

        log.debug("############## CONNECTING ##############")
        # Open socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect socket to server - defined by host and port arguments
        self.sock.connect((host, port))

        # Send RPC headers
        self.sock.send("hrpc")                                 # header
        self.sock.send(struct.pack('B', 7))                    # version
        self.sock.send(struct.pack('B', 80))                   # auth method
        self.sock.send(struct.pack('B', 0))                    # serialization type (protobuf = 0)

        self.sock.send(struct.pack('!I', len(context)))        # length of connection context (32bit int)
        self.sock.send(context)                                # connection context

    def createRpcRequest(self, method, request):
        '''Wraps the user's request in an HadoopRpcRequestProto message and serializes it delimited.'''
        s_request = request.SerializeToString()
        self.logProtobufMessage("Protobuf message", request)
        log.debug("Protobuf message bytes (%d): %s" % (len(s_request), format_bytes(s_request)))
        rpcRequest = hadoop_rpc.HadoopRpcRequestProto()
        rpcRequest.methodName = method.name
        rpcRequest.request = s_request
        rpcRequest.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        rpcRequest.clientProtocolVersion = 1L

        # Serialize delimited
        s_rpcRequest = rpcRequest.SerializeToString()
        self.logProtobufMessage("RpcRequest (len: %d)" % len(s_rpcRequest), rpcRequest)
        return encoder._VarintBytes(len(s_rpcRequest)) + s_rpcRequest

    def createRpcHeader(self):
        '''Creates and serializes a delimited RpcPayloadHeaderProto message.'''
        rpcheader = rpcheaderproto.RpcPayloadHeaderProto()
        rpcheader.rpcKind = 2  # rpcheaderproto.RpcKindProto.Value('RPC_PROTOCOL_BUFFER')
        rpcheader.rpcOp = 0  # rpcheaderproto.RpcPayloadOperationProto.Value('RPC_FINAL_PAYLOAD')
        rpcheader.callId = self.call_id
        self.call_id += 1

        # Serialize delimited
        s_rpcHeader = rpcheader.SerializeToString()
        self.logProtobufMessage("RpcPayloadHeader (len: %d)" % (len(s_rpcHeader)), rpcheader)
        return encoder._VarintBytes(len(s_rpcHeader)) + s_rpcHeader

    def createConnectionContext(self):
        '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
        context = connectionContext.IpcConnectionContextProto()
        context.userInfo.effectiveUser = os.getlogin()
        context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        s_context = context.SerializeToString()
        self.logProtobufMessage("RequestContext (len: %d)" % len(s_context), context)
        return s_context

    def sendRpcMessage(self, rpcHeader, rpcRequest):
        '''Sends a Hadoop RPC request to the NameNode.

        The IpcConnectionContextProto, RpcPayloadHeaderProto and HadoopRpcRequestProto
        should already be serialized in the right way (delimited or not) before
        they are passed in this method.

        The Hadoop RPC protocol looks like this for sending requests:

        When sending requests
        +---------------------------------------------------------------------+
        |  Length of the next two parts (4 bytes/32 bit int)                  |
        +---------------------------------------------------------------------+
        |  Delimited serialized RpcPayloadHeaderProto (varint len + header)   |
        +---------------------------------------------------------------------+
        |  Delimited serialized HadoopRpcRequestProto (varint len + request)  |
        +---------------------------------------------------------------------+
        '''
        log.debug("############## SENDING ##############")

        length = len(rpcHeader) + len(rpcRequest)
        log.debug("Header + payload len: %d" % length)
        self.sock.send(struct.pack('!I', length))                  # length of header + request (32bit int)
        self.sock.send(rpcHeader)                                  # payload header
        self.sock.send(rpcRequest)                                 # rpc request

    def logProtobufMessage(self, header, message):
        log.debug("%s:\n\n\033[92m%s\033[0m" % (header, message))

    def recvRpcMessage(self):
        '''Handle reading an RPC reply from the server. This is done by wrapping the
        socket in a RcpBufferedReader that allows for rewinding of the buffer stream.
        '''
        log.debug("############## RECVING ##############")
        byte_stream = RpcBufferedReader(self.sock)
        return byte_stream

    def getDelimitedMessageBytes(self, byte_stream):
        ''' Parse a delimited protobuf message. This is done by first getting a protobuf varint from
        the stream that represents the length of the message, then reading that amount of
        from the message and then parse it.
        Since the int can be represented as max 4 bytes, first get 4 bytes and try to decode.
        The decoder returns the value and the position where the value was found, so we need
        to rewind the buffer to the position, because the remaining bytes belong to the message
        after.
        '''

        (length, pos) = decoder._DecodeVarint32(byte_stream.read(4), 0)
        log.debug("Delimited message length (pos %d): %d" % (pos, length))

        byte_stream.rewind(pos)
        message_bytes = byte_stream.read_more(length - byte_stream.buffer_lenght())
        log.debug("Delimited message bytes (%d): %s" % (len(message_bytes), format_bytes(message_bytes)))
        return message_bytes

    def getLength(self, byte_stream):
        ''' In Hadoop protobuf RPC, some parts of the stream are delimited with protobuf varint,
        while others are delimited with 4 byte integers. This reads 4 bytes from the byte stream
        and retruns the length of the delimited part that follows, by unpacking the 4 bytes
        and returning the first element from a tuple. The tuple that is returned from struc.unpack()
        only contains one element.
        '''
        length = struct.unpack("!i", byte_stream.read(4))[0]
        log.debug("4 bytes delimited part length: %d" % length)
        return length

    def parseResponse(self, byte_stream, response_class):
        '''Parses a Hadoop RPC response.

        The RpcResponseHeaderProto contains a status field that marks SUCCESS or ERROR.
        The Hadoop RPC protocol looks like the diagram below for receiving SUCCESS requests.
        +-----------------------------------------------------------+
        |  Delimited serialized RpcResponseHeaderProto              |
        +-----------------------------------------------------------+
        |  Length of the RPC resonse (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Serialized RPC response                                  |
        +-----------------------------------------------------------+

        The Hadoop RPC protocol looks like the diagram below for receiving ERROR requests.
        +-----------------------------------------------------------+
        |  Delimited serialized RpcResponseHeaderProto              |
        +-----------------------------------------------------------+
        |  Length of the RPC resonse (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Length of the Exeption class name (4 bytes/32 bit int)   |
        +-----------------------------------------------------------+
        |  Exception class name string                              |
        +-----------------------------------------------------------+
        |  Length of the stack trace (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Stack trace string                                       |
        +-----------------------------------------------------------+

        If the lenght of the strings is -1, the strings are null
        '''

        log.debug("############## PARSING ##############")
        log.debug("Payload class: %s" % response_class)

        log.debug("---- Parsing header ----")
        header_bytes = self.getDelimitedMessageBytes(byte_stream)
        header = rpcheaderproto.RpcResponseHeaderProto()
        header.ParseFromString(header_bytes)
        self.logProtobufMessage("Response header", header)

        if header.status == 0:  # rpcheaderproto.RpcStatusProto.Value('SUCCESS')
            log.debug("---- Parsing response ----")
            response = response_class()
            response_length = self.getLength(byte_stream)

            if response_length == 0:
                return

            response_bytes = byte_stream.read(response_length)
            log.debug("Response bytes (%d): %s" % (len(response_bytes), format_bytes(response_bytes)))

            response.ParseFromString(response_bytes)
            self.logProtobufMessage("Response", response)
            return response

        elif header.status == 1:  # rpcheaderproto.RpcStatusProto.Value('ERROR')
            length = self.getLength(byte_stream)
            log.debug("Class name length: %d" % (length))
            if length == -1:
                class_name = None
            else:
                class_name = byte_stream.read(length)
                log.debug("Class name (%d): %s" % (len(class_name), class_name))

            length = self.getLength(byte_stream)
            log.debug("Stack trace length: %d" % (length))
            if length == -1:
                stack_trace = None
            else:
                stack_trace = byte_stream.read(length)
                log.debug("Stack trace (%d): %s" % (len(stack_trace), stack_trace))

            stack_trace_msg = stack_trace.split("\n")[0]
            log.debug(stack_trace_msg)

            raise RequestError(stack_trace_msg)

    def closeSocket(self):
        '''Closes the socket and resets the channel.'''
        log.debug("Closing socket")
        if self.sock:
            try:
                self.sock.close()
            except:
                pass

            self.sock = None

    def CallMethod(self, method, controller, request, response_class, done):
        '''Call the RPC method.
        '''
        try:
            self.validateRequest(request)

            if not self.sock:
                context = self.createConnectionContext()
                self.openSocket(self.host, self.port, context)

            # Create serialized rpcHeader, context and rpcRequest
            rpcHeader = self.createRpcHeader()
            rpcRequest = self.createRpcRequest(method, request)

            self.sendRpcMessage(rpcHeader, rpcRequest)

            byte_stream = self.recvRpcMessage()
            return self.parseResponse(byte_stream, response_class)
        except RequestError, e:  # Raise a request error, but don't close the socket
            raise e
        except Exception, e:  # All other errors close the socket
            self.closeSocket()
            raise e


class SocketRpcController(service.RpcController):
    ''' RpcController implementation to be used by the SocketRpcChannel class.

    The RpcController is used to mediate a single method call.
    '''

    def __init__(self):
        '''Constructor which initializes the controller's state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def handleError(self, error_code, message):
        '''Log and set the controller state.'''
        self._fail = True
        self.reason = error_code
        self._error = message

    def reset(self):
        '''Resets the controller i.e. clears the error state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def failed(self):
        '''Returns True if the controller is in a failed state.'''
        return self._fail

    def error(self):
        return self._error
