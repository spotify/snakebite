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
import pwd
import math

# Third party imports
from google.protobuf.service import RpcChannel

# Protobuf imports
import snakebite.protobuf.RpcPayloadHeader_pb2 as rpcheaderproto
import snakebite.protobuf.IpcConnectionContext_pb2 as connectionContext
import snakebite.protobuf.datatransfer_pb2 as datatransfer_proto
import snakebite.protobuf.hadoop_rpc_pb2 as hadoop_rpc

from snakebite.formatter import format_bytes
from snakebite.errors import RequestError
from snakebite.crc32c import crc

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

# Module imports

import logger
import struct

# Configure package logging
log = logger.getLogger(__name__)


def log_protobuf_message(header, message):
    log.debug("%s:\n\n\033[92m%s\033[0m" % (header, message))


def get_delimited_message_bytes(byte_stream):
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

    byte_stream.rewind(4 - pos)
    message_bytes = byte_stream.read(length)
    log.debug("Delimited message bytes (%d): %s" % (len(message_bytes), format_bytes(message_bytes)))
    return message_bytes


class RpcBufferedReader(object):
    '''Class that wraps a socket and provides some utility methods for reading
    and rewinding of the buffer. This comes in handy when reading protobuf varints.
    '''
    MAX_READ_ATTEMPTS = 100

    def __init__(self, socket):
        self.socket = socket
        self.reset()

    def read(self, n):
        '''Reads n bytes into the internal buffer'''
        bytes_wanted = n - self.buffer_length + self.pos + 1
        if bytes_wanted > 0:
            self._buffer_bytes(bytes_wanted)

        end_pos = self.pos + n
        ret = self.buffer[self.pos + 1:end_pos + 1]
        self.pos = end_pos
        return ret

    def _buffer_bytes(self, n):
        to_read = n
        for _ in xrange(self.MAX_READ_ATTEMPTS):
            bytes_read = self.socket.recv(to_read)
            self.buffer += bytes_read
            to_read -= len(bytes_read)
            if to_read == 0:
                log.debug("Bytes read: %d, total: %d" % (len(bytes_read), self.buffer_length))
                return n
        if len(bytes_read) < n:
            raise Exception("RpcBufferedReader only managed to read %s out of %s bytes" % (len(bytes_read), n))

    def rewind(self, places):
        '''Rewinds the current buffer to a position. Needed for reading varints,
        because we might read bytes that belong to the stream after the varint.
        '''
        log.debug("Rewinding pos %d with %d places" % (self.pos, places))
        self.pos -= places
        log.debug("Reset buffer to pos %d" % self.pos)

    def reset(self):
        self.buffer = ""
        self.pos = -1  # position of last byte read

    @property
    def buffer_length(self):
        '''Returns the length of the current buffer.'''
        return len(self.buffer)


class SocketRpcChannel(RpcChannel):
    ERROR_BYTES = 18446744073709551615L

    '''Socket implementation of an RpcChannel.
    '''

    def __init__(self, host, port, version):
        '''SocketRpcChannel to connect to a socket server on a user defined port.'''
        self.host = host
        self.port = port
        self.sock = None
        self.call_id = 0
        self.version = version

    def validate_request(self, request):
        '''Validate the client request against the protocol file.'''

        # Check the request is correctly initialized
        if not request.IsInitialized():
            raise Exception("Client request (%s) is missing mandatory fields" % type(request))

    def open_socket(self, host, port, context):
        '''Open a socket connection to a given host and port and writes the Hadoop header
        The Hadoop RPC protocol looks like this when creating a connection:

        +---------------------------------------------------------------------+
        |  Header, 4 bytes ("hrpc")                                           |
        +---------------------------------------------------------------------+
        |  Version, 1 byte (default verion 7)                                 |
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
        self.sock.send(struct.pack('B', self.version))         # version
        self.sock.send(struct.pack('B', 80))                   # auth method
        self.sock.send(struct.pack('B', 0))                    # serialization type (protobuf = 0)

        self.sock.send(struct.pack('!I', len(context)))        # length of connection context (32bit int)
        self.sock.send(context)                                # connection context

    def create_rpc_request(self, method, request):
        '''Wraps the user's request in an HadoopRpcRequestProto message and serializes it delimited.'''
        s_request = request.SerializeToString()
        log_protobuf_message("Protobuf message", request)
        log.debug("Protobuf message bytes (%d): %s" % (len(s_request), format_bytes(s_request)))
        rpcRequest = hadoop_rpc.HadoopRpcRequestProto()
        rpcRequest.methodName = method.name
        rpcRequest.request = s_request
        rpcRequest.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        rpcRequest.clientProtocolVersion = 1L

        # Serialize delimited
        s_rpcRequest = rpcRequest.SerializeToString()
        log_protobuf_message("RpcRequest (len: %d)" % len(s_rpcRequest), rpcRequest)
        return encoder._VarintBytes(len(s_rpcRequest)) + s_rpcRequest

    def create_rpc_header(self):
        '''Creates and serializes a delimited RpcPayloadHeaderProto message.'''
        rpcheader = rpcheaderproto.RpcPayloadHeaderProto()
        rpcheader.rpcKind = 2  # rpcheaderproto.RpcKindProto.Value('RPC_PROTOCOL_BUFFER')
        rpcheader.rpcOp = 0  # rpcheaderproto.RpcPayloadOperationProto.Value('RPC_FINAL_PAYLOAD')
        rpcheader.callId = self.call_id
        self.call_id += 1

        # Serialize delimited
        s_rpcHeader = rpcheader.SerializeToString()
        log_protobuf_message("RpcPayloadHeader (len: %d)" % (len(s_rpcHeader)), rpcheader)
        return encoder._VarintBytes(len(s_rpcHeader)) + s_rpcHeader

    def create_connection_context(self):
        '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
        context = connectionContext.IpcConnectionContextProto()
        context.userInfo.effectiveUser = pwd.getpwuid(os.getuid())[0]
        context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        s_context = context.SerializeToString()
        log_protobuf_message("RequestContext (len: %d)" % len(s_context), context)
        return s_context

    def send_rpc_message(self, rpcHeader, rpcRequest):
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

    def recv_rpc_message(self):
        '''Handle reading an RPC reply from the server. This is done by wrapping the
        socket in a RcpBufferedReader that allows for rewinding of the buffer stream.
        '''
        log.debug("############## RECVING ##############")
        byte_stream = RpcBufferedReader(self.sock)
        return byte_stream

    def get_length(self, byte_stream):
        ''' In Hadoop protobuf RPC, some parts of the stream are delimited with protobuf varint,
        while others are delimited with 4 byte integers. This reads 4 bytes from the byte stream
        and retruns the length of the delimited part that follows, by unpacking the 4 bytes
        and returning the first element from a tuple. The tuple that is returned from struc.unpack()
        only contains one element.
        '''
        length = struct.unpack("!i", byte_stream.read(4))[0]
        log.debug("4 bytes delimited part length: %d" % length)
        return length

    def parse_response(self, byte_stream, response_class):
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

        If the length of the strings is -1, the strings are null
        '''

        log.debug("############## PARSING ##############")
        log.debug("Payload class: %s" % response_class)

        # Let's see if we deal with an error on protocol level
        check = struct.unpack("!Q", byte_stream.read(8))[0]
        if check == self.ERROR_BYTES:
            self.handle_error(byte_stream)

        byte_stream.rewind(8)
        log.debug("---- Parsing header ----")
        header_bytes = get_delimited_message_bytes(byte_stream)
        header = rpcheaderproto.RpcResponseHeaderProto()
        header.ParseFromString(header_bytes)
        log_protobuf_message("Response header", header)

        if header.status == 0:  # rpcheaderproto.RpcStatusProto.Value('SUCCESS')
            log.debug("---- Parsing response ----")
            response = response_class()
            response_length = self.get_length(byte_stream)

            if response_length == 0:
                return

            response_bytes = byte_stream.read(response_length)
            log.debug("Response bytes (%d): %s" % (len(response_bytes), format_bytes(response_bytes)))

            response.ParseFromString(response_bytes)
            log_protobuf_message("Response", response)
            return response

        elif header.status == 1:  # rpcheaderproto.RpcStatusProto.Value('ERROR')
            self.handle_error(byte_stream)

    def handle_error(self, byte_stream):
        '''Handle errors'''
        length = self.get_length(byte_stream)
        log.debug("Class name length: %d" % (length))
        if length == -1:
            class_name = None
        else:
            class_name = byte_stream.read(length)
            log.debug("Class name (%d): %s" % (len(class_name), class_name))

        length = self.get_length(byte_stream)
        log.debug("Stack trace length: %d" % (length))
        if length == -1:
            stack_trace = None
        else:
            stack_trace = byte_stream.read(length)
            log.debug("Stack trace (%d): %s" % (len(stack_trace), stack_trace))

        stack_trace_msg = stack_trace.split("\n")[0]
        log.debug(stack_trace_msg)

        raise RequestError(stack_trace_msg)

    def close_socket(self):
        '''Closes the socket and resets the channel.'''
        log.debug("Closing socket")
        if self.sock:
            try:
                self.sock.close()
            except:
                pass

            self.sock = None

    def CallMethod(self, method, controller, request, response_class, done):
        '''Call the RPC method. The naming doesn't confirm PEP8, since it's
        a method called by protobuf
        '''
        try:
            self.validate_request(request)

            if not self.sock:
                context = self.create_connection_context()
                self.open_socket(self.host, self.port, context)

            # Create serialized rpcHeader, context and rpcRequest
            rpcHeader = self.create_rpc_header()
            rpcRequest = self.create_rpc_request(method, request)

            self.send_rpc_message(rpcHeader, rpcRequest)

            byte_stream = self.recv_rpc_message()
            return self.parse_response(byte_stream, response_class)
        except RequestError:  # Raise a request error, but don't close the socket
            raise
        except Exception:  # All other errors close the socket
            self.close_socket()
            raise


class DataXceiverChannel(object):
    # For internal reading: should be larger than bytes_per_chunk
    LOAD_SIZE = 16000.0

    # Op codes
    WRITE_BLOCK = 80
    READ_BLOCK = 81
    READ_METADATA = 82
    REPLACE_BLOCK = 83
    COPY_BLOCK = 84
    BLOCK_CHECKSUM = 85
    TRANSFER_BLOCK = 86

    # Checksum types
    CHECKSUM_NULL = 0
    CHECKSUM_CRC32 = 1
    CHECKSUM_CRC32C = 2
    CHECKSUM_DEFAULT = 3
    CHECKSUM_MIXED = 4

    MAX_READ_ATTEMPTS = 100

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        try:
            self.sock.connect((self.host, self.port))
            log.debug("%s connected to DataNode" % self)
            return True
        except Exception:
            log.debug("%s connection to DataNode failed" % self)
            return False

    def _close_socket(self):
        self.sock.close()

    def _read_bytes(self, n, depth=0):
        if depth > self.MAX_READ_ATTEMPTS:
            raise Exception("Tried to read %d more bytes, but failed after %d attempts" % (n, self.MAX_READ_ATTEMPTS))

        bytes = self.sock.recv(n)
        if len(bytes) < n:
            left = n - len(bytes)
            depth += 1
            bytes += self._read_bytes(left, depth)
        return bytes

    def readBlock(self, length, pool_id, block_id, generation_stamp, offset, check_crc):
        '''Send a read request to given block. If we receive a successful response,
        we start reading packets.

        Send read request:
        +---------------------------------------------------------------------+
        |  Data Transfer Protocol Version, 2 bytes                            |
        +---------------------------------------------------------------------+
        |  Op code, 1 byte (READ_BLOCK = 81)                                  |
        +---------------------------------------------------------------------+
        |  Delimited serialized OpReadBlockProto (varint len + request)       |
        +---------------------------------------------------------------------+

        Receive response:
        +---------------------------------------------------------------------+
        |  Delimited BlockOpResponseProto (varint len + response)             |
        +---------------------------------------------------------------------+

        Start reading packets. Each packet has the following structure:
        +---------------------------------------------------------------------+
        |  Packet length (4 bytes/32 bit int)                                 |
        +---------------------------------------------------------------------+
        |  Serialized size of header, 2 bytes                                 |
        +---------------------------------------------------------------------+
        |  Packet Header Proto                                                |
        +---------------------------------------------------------------------+
        |  x checksums, 4 bytes each                                          |
        +---------------------------------------------------------------------+
        |  x chunks of payload data                                           |
        +---------------------------------------------------------------------+

        '''
        log.debug("%s sending readBlock request" % self)

        # Send version and opcode
        self.sock.send(struct.pack('>h', 28))
        self.sock.send(struct.pack('b', self.READ_BLOCK))
        length = length - offset

        # Create and send OpReadBlockProto message
        request = datatransfer_proto.OpReadBlockProto()
        request.offset = offset
        request.len = length
        header = request.header
        header.clientName = "snakebite"
        base_header = header.baseHeader
        block = base_header.block
        block.poolId = pool_id
        block.blockId = block_id
        block.generationStamp = generation_stamp
        s_request = request.SerializeToString()
        log_protobuf_message("OpReadBlockProto:", request)
        delimited_request = encoder._VarintBytes(len(s_request)) + s_request
        self.sock.send(delimited_request)

        byte_stream = RpcBufferedReader(self.sock)
        block_op_response_bytes = get_delimited_message_bytes(byte_stream)

        block_op_response = datatransfer_proto.BlockOpResponseProto()
        block_op_response.ParseFromString(block_op_response_bytes)
        log_protobuf_message("BlockOpResponseProto", block_op_response)

        checksum_type = block_op_response.readOpChecksumInfo.checksum.type
        bytes_per_chunk = block_op_response.readOpChecksumInfo.checksum.bytesPerChecksum
        log.debug("Checksum type: %s, bytesPerChecksum: %s" % (checksum_type, bytes_per_chunk))
        if checksum_type in [self.CHECKSUM_CRC32C, self.CHECKSUM_CRC32]:
            checksum_len = 4
        else:
            raise Exception("Checksum type %s not implemented" % checksum_type)

        total_read = 0
        if block_op_response.status == 0:  # datatransfer_proto.Status.Value('SUCCESS')
            while total_read < length:
                log.debug("== Reading next packet")

                packet_len = struct.unpack("!I", byte_stream.read(4))[0]
                log.debug("Packet length: %s", packet_len)

                serialized_size = struct.unpack("!H", byte_stream.read(2))[0]
                log.debug("Serialized size: %s", serialized_size)

                packet_header_bytes = byte_stream.read(serialized_size)
                packet_header = datatransfer_proto.PacketHeaderProto()
                packet_header.ParseFromString(packet_header_bytes)
                log_protobuf_message("PacketHeaderProto", packet_header)

                data_len = packet_header.dataLen

                chunks_per_packet = int((data_len + bytes_per_chunk - 1) / bytes_per_chunk)
                log.debug("Nr of chunks: %d", chunks_per_packet)

                data_len = packet_len - 4 - chunks_per_packet * checksum_len
                log.debug("Payload len: %d", data_len)

                byte_stream.reset()

                # Collect checksums
                if check_crc:
                    checksums = []
                    for _ in xrange(0, chunks_per_packet):
                        checksum = self._read_bytes(checksum_len)
                        checksum = struct.unpack("!I", checksum)[0]
                        checksums.append(checksum)
                else:
                    self._read_bytes(checksum_len * chunks_per_packet)

                # We use a fixed size buffer (a "load") to read only a couple of chunks at once. 
                bytes_per_load = self.LOAD_SIZE - (self.LOAD_SIZE % bytes_per_chunk)
                chunks_per_load = int(bytes_per_load / bytes_per_chunk)
                loads_per_packet = int(math.ceil(bytes_per_chunk * chunks_per_packet / bytes_per_load))

                read_on_packet = 0
                for i in range(loads_per_packet):
                    load = ''
                    for j in range(chunks_per_load):
                        log.debug("Reading chunk %s in load %s:", j, i)
                        bytes_to_read = min(bytes_per_chunk, data_len - read_on_packet)
                        chunk = self._read_bytes(bytes_to_read)
                        if check_crc:
                            checksum_index = i * chunks_per_load + j
                            if checksum_index < len(checksums) and crc(chunk) != checksums[checksum_index]:
                                raise Exception("Checksum doesn't match")
                        load += chunk
                        total_read += len(chunk)
                        read_on_packet += len(chunk)
                    yield load
           
            # Send ClientReadStatusProto message confirming successful read
            request = datatransfer_proto.ClientReadStatusProto()
            request.status = 0  # SUCCESS
            s_request = request.SerializeToString()
            log_protobuf_message("ClientReadStatusProto:", request)
            delimited_request = encoder._VarintBytes(len(s_request)) + s_request
            self.sock.send(delimited_request)
            self._close_socket()

    def __repr__(self):
        return "DataXceiverChannel<%s:%d>" % (self.host, self.port)
