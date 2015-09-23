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
import math

# Third party imports
from google.protobuf.service import RpcChannel

# Protobuf imports
from snakebite.protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from snakebite.protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto
from snakebite.protobuf.ProtobufRpcEngine_pb2 import RequestHeaderProto
from snakebite.protobuf.datatransfer_pb2 import OpReadBlockProto, BlockOpResponseProto, PacketHeaderProto, ClientReadStatusProto

from snakebite.platformutils import get_current_username
from snakebite.formatter import format_bytes
from snakebite.errors import RequestError
from snakebite.crc32c import crc

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

# Module imports

import logger
import logging
import struct
import uuid

# Configure package logging
log = logger.getLogger(__name__)


def log_protobuf_message(header, message):
    log.debug("%s:\n\n\033[92m%s\033[0m" % (header, message))


def get_delimited_message_bytes(byte_stream, nr=4):
    ''' Parse a delimited protobuf message. This is done by first getting a protobuf varint from
    the stream that represents the length of the message, then reading that amount of
    from the message and then parse it.
    Since the int can be represented as max 4 bytes, first get 4 bytes and try to decode.
    The decoder returns the value and the position where the value was found, so we need
    to rewind the buffer to the position, because the remaining bytes belong to the message
    after.
    '''

    (length, pos) = decoder._DecodeVarint32(byte_stream.read(nr), 0)
    if log.getEffectiveLevel() == logging.DEBUG:
        log.debug("Delimited message length (pos %d): %d" % (pos, length))

    delimiter_bytes = nr - pos

    byte_stream.rewind(delimiter_bytes)
    message_bytes = byte_stream.read(length)
    if log.getEffectiveLevel() == logging.DEBUG:
        log.debug("Delimited message bytes (%d): %s" % (len(message_bytes), format_bytes(message_bytes)))

    total_len = length + pos
    return (total_len, message_bytes)


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
    RPC_HEADER = "hrpc"
    RPC_SERVICE_CLASS = 0x00
    AUTH_PROTOCOL_NONE = 0x00
    AUTH_PROTOCOL_SASL = 0xDF
    RPC_PROTOCOL_BUFFFER = 0x02


    '''Socket implementation of an RpcChannel.
    '''

    def __init__(self, host, port, version, effective_user=None, use_sasl=False):
        '''SocketRpcChannel to connect to a socket server on a user defined port.
           It possible to define version and effective user for the communication.'''
        self.host = host
        self.port = port
        self.sock = None
        self.call_id = -3  # First time (when the connection context is sent, the call_id should be -3, otherwise start with 0 and increment)
        self.version = version
        self.client_id = str(uuid.uuid4())
        self.use_sasl = use_sasl
        if self.use_sasl:
            try:
                # Only import if SASL is enabled
                from snakebite.rpc_sasl import SaslRpcClient
                from snakebite.kerberos import Kerberos
            except ImportError:
                raise Exception("Kerberos libs not found. Please install snakebite using 'pip install snakebite[kerberos]'")

            kerberos = Kerberos()
            self.effective_user = effective_user or kerberos.user_principal().name
        else: 
            self.effective_user = effective_user or get_current_username()

    def validate_request(self, request):
        '''Validate the client request against the protocol file.'''

        # Check the request is correctly initialized
        if not request.IsInitialized():
            raise Exception("Client request (%s) is missing mandatory fields" % type(request))

    def get_connection(self, host, port):
        '''Open a socket connection to a given host and port and writes the Hadoop header
        The Hadoop RPC protocol looks like this when creating a connection:

        +---------------------------------------------------------------------+
        |  Header, 4 bytes ("hrpc")                                           |
        +---------------------------------------------------------------------+
        |  Version, 1 byte (default verion 9)                                 |
        +---------------------------------------------------------------------+
        |  RPC service class, 1 byte (0x00)                                   |
        +---------------------------------------------------------------------+
        |  Auth protocol, 1 byte (Auth method None = 0)                       |
        +---------------------------------------------------------------------+
        |  Length of the RpcRequestHeaderProto  + length of the               |
        |  of the IpcConnectionContextProto (4 bytes/32 bit int)              |
        +---------------------------------------------------------------------+
        |  Serialized delimited RpcRequestHeaderProto                         |
        +---------------------------------------------------------------------+
        |  Serialized delimited IpcConnectionContextProto                     |
        +---------------------------------------------------------------------+
        '''

        log.debug("############## CONNECTING ##############")
        # Open socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(10)
        # Connect socket to server - defined by host and port arguments
        self.sock.connect((host, port))

        # Send RPC headers
        self.write(self.RPC_HEADER)                             # header
        self.write(struct.pack('B', self.version))              # version
        self.write(struct.pack('B', self.RPC_SERVICE_CLASS))    # RPC service class
        if self.use_sasl:
            self.write(struct.pack('B', self.AUTH_PROTOCOL_SASL))   # serialization type (protobuf = 0xDF)
        else:
            self.write(struct.pack('B', self.AUTH_PROTOCOL_NONE))   # serialization type (protobuf = 0)

        if self.use_sasl:
            sasl = SaslRpcClient(self)
            sasl_connected = sasl.connect()
            if not sasl_connected:
                raise Exception("SASL is configured, but cannot get connected")

        rpc_header = self.create_rpc_request_header()
        context = self.create_connection_context()

        header_length = len(rpc_header) + encoder._VarintSize(len(rpc_header)) +len(context) + encoder._VarintSize(len(context))

        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("Header length: %s (%s)" % (header_length, format_bytes(struct.pack('!I', header_length))))

        self.write(struct.pack('!I', header_length))

        self.write_delimited(rpc_header)
        self.write_delimited(context)
    
    def write(self, data):
        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("Sending: %s", format_bytes(data))
        self.sock.send(data)

    def write_delimited(self, data):
        self.write(encoder._VarintBytes(len(data)))
        self.write(data)

    def create_rpc_request_header(self):
        '''Creates and serializes a delimited RpcRequestHeaderProto message.'''
        rpcheader = RpcRequestHeaderProto()
        rpcheader.rpcKind = 2  # rpcheaderproto.RpcKindProto.Value('RPC_PROTOCOL_BUFFER')
        rpcheader.rpcOp = 0  # rpcheaderproto.RpcPayloadOperationProto.Value('RPC_FINAL_PACKET')
        rpcheader.callId = self.call_id
        rpcheader.retryCount = -1
        rpcheader.clientId = self.client_id[0:16]

        if self.call_id == -3:
            self.call_id = 0
        else:
            self.call_id += 1

        # Serialize delimited
        s_rpcHeader = rpcheader.SerializeToString()
        log_protobuf_message("RpcRequestHeaderProto (len: %d)" % (len(s_rpcHeader)), rpcheader)
        return s_rpcHeader

    def create_connection_context(self):
        '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
        context = IpcConnectionContextProto()
        context.userInfo.effectiveUser = self.effective_user
        context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"

        s_context = context.SerializeToString()
        log_protobuf_message("RequestContext (len: %d)" % len(s_context), context)
        return s_context

    def send_rpc_message(self, method, request):
        '''Sends a Hadoop RPC request to the NameNode.

        The IpcConnectionContextProto, RpcPayloadHeaderProto and HadoopRpcRequestProto
        should already be serialized in the right way (delimited or not) before
        they are passed in this method.

        The Hadoop RPC protocol looks like this for sending requests:

        When sending requests
        +---------------------------------------------------------------------+
        |  Length of the next three parts (4 bytes/32 bit int)                |
        +---------------------------------------------------------------------+
        |  Delimited serialized RpcRequestHeaderProto (varint len + header)   |
        +---------------------------------------------------------------------+
        |  Delimited serialized RequestHeaderProto (varint len + header)      |
        +---------------------------------------------------------------------+
        |  Delimited serialized Request (varint len + request)                |
        +---------------------------------------------------------------------+
        '''
        log.debug("############## SENDING ##############")

        #0. RpcRequestHeaderProto
        rpc_request_header = self.create_rpc_request_header()
        #1. RequestHeaderProto
        request_header = self.create_request_header(method)
        #2. Param
        param = request.SerializeToString()
        if log.getEffectiveLevel() == logging.DEBUG:
            log_protobuf_message("Request", request)

        rpc_message_length = len(rpc_request_header) + encoder._VarintSize(len(rpc_request_header)) + \
                             len(request_header) + encoder._VarintSize(len(request_header)) + \
                             len(param) + encoder._VarintSize(len(param))

        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("RPC message length: %s (%s)" % (rpc_message_length, format_bytes(struct.pack('!I', rpc_message_length))))
        self.write(struct.pack('!I', rpc_message_length))

        self.write_delimited(rpc_request_header)
        self.write_delimited(request_header)
        self.write_delimited(param)

    def create_request_header(self, method):
        header = RequestHeaderProto()
        header.methodName = method.name
        header.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
        header.clientProtocolVersion = 1

        s_header = header.SerializeToString()
        log_protobuf_message("RequestHeaderProto (len: %d)" % len(s_header), header)
        return s_header

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
        |  Length of the RPC resonse (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Delimited serialized RpcResponseHeaderProto              |
        +-----------------------------------------------------------+
        |  Serialized delimited RPC response                        |
        +-----------------------------------------------------------+

        In case of an error, the header status is set to ERROR and the error fields are set.
        '''

        log.debug("############## PARSING ##############")
        log.debug("Payload class: %s" % response_class)

        # Read first 4 bytes to get the total length
        len_bytes = byte_stream.read(4)
        total_length = struct.unpack("!I", len_bytes)[0]
        log.debug("Total response length: %s" % total_length)

        header = RpcResponseHeaderProto()
        (header_len, header_bytes) = get_delimited_message_bytes(byte_stream)

        log.debug("Header read %d" % header_len)
        header.ParseFromString(header_bytes)
        log_protobuf_message("RpcResponseHeaderProto", header)

        if header.status == 0:
            log.debug("header: %s, total: %s" % (header_len, total_length))
            if header_len >= total_length:
                return
            response = response_class()
            response_bytes = get_delimited_message_bytes(byte_stream, total_length - header_len)[1]
            if len(response_bytes) > 0:
                response.ParseFromString(response_bytes)
                if log.getEffectiveLevel() == logging.DEBUG:
                    log_protobuf_message("Response", response)
                return response
        else:
            self.handle_error(header)

    def handle_error(self, header):
        raise RequestError("\n".join([header.exceptionClassName, header.errorMsg]))

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
                self.get_connection(self.host, self.port)

            self.send_rpc_message(method, request)

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

    def write(self, data):
        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("Sending: %s", format_bytes(data))
        self.sock.send(data)

    def write_delimited(self, data):
        self.write(encoder._VarintBytes(len(data)))
        self.write(data)

    def readBlock(self, length, pool_id, block_id, generation_stamp, offset, block_token, check_crc):
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
        request = OpReadBlockProto()
        request.offset = offset
        request.len = length
        header = request.header
        header.clientName = "snakebite"
        base_header = header.baseHeader
        # TokenProto
        token = base_header.token
        token.identifier = block_token.identifier
        token.password = block_token.password
        token.kind = block_token.kind
        token.service = block_token.service
        # ExtendedBlockProto
        block = base_header.block
        block.poolId = pool_id
        block.blockId = block_id
        block.generationStamp = generation_stamp
        s_request = request.SerializeToString()
        log_protobuf_message("OpReadBlockProto:", request)
        self.write_delimited(s_request)

        byte_stream = RpcBufferedReader(self.sock)
        block_op_response_bytes = get_delimited_message_bytes(byte_stream)[1]

        block_op_response = BlockOpResponseProto()
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
                packet_header = PacketHeaderProto()
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
            request = ClientReadStatusProto()
            request.status = 0  # SUCCESS
            log_protobuf_message("ClientReadStatusProto:", request)
            s_request = request.SerializeToString()
            self.write_delimited(s_request)
            self._close_socket()

    def __repr__(self):
        return "DataXceiverChannel<%s:%d>" % (self.host, self.port)
