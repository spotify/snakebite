# -*- coding: utf-8 -*-
# Copyright (c) 2015 Bolke de Bruin
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
rpc_sasl.py - Implementation of SASL on top of Hadoop RPC.

This package contains a class providing a SASL authentication implementation
using Hadoop RPC as a transport. It was inspired the Hadoop Java classes.

May 2015

Bolke de Bruin (bolke@xs4all.nl)

'''

import struct
import sasl

from snakebite.protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto
import google.protobuf.internal.encoder as encoder

import logger

# Configure package logging
log = logger.getLogger(__name__)

def log_protobuf_message(header, message):
    log.debug("%s:\n\n\033[92m%s\033[0m" % (header, message))

class SaslRpcClient:
    def __init__(self, sasl_client_factory, mechanism, trans):
        self.sasl_client_factory = sasl_client_factory
        self.sasl = None
        self.mechanism = mechanism
        self._trans = trans

    def send_sasl_message(self, message):
        rpcheader = RpcRequestHeaderProto()
        rpcheader.rpcKind = 2 # RPC_PROTOCOL_BUFFER
        rpcheader.rpcOp = 0
        rpcheader.callId = -33 # SASL
        rpcheader.retryCount = -1
        rpcheader.clientId = b""

        s_rpcheader = rpcheader.SerializeToString()
        s_message = message.SerializeToString()
        
        header_length = len(s_rpcheader) + encoder._VarintSize(len(s_rpcheader)) + len(s_message) + encoder._VarintSize(len(s_message)) 

        self._trans.write(struct.pack('!I', header_length))
        self._trans.write_delimited(s_rpcheader)
        self._trans.write_delimited(s_message)

        log_protobuf_message("Send out", message)

    def recv_sasl_message(self):
        bytestream = self._trans.recv_rpc_message()
        sasl_response = self._trans.parse_response(bytestream, RpcSaslProto)

        return sasl_response

    def sasl_connect(self):
        negotiate = RpcSaslProto()
        negotiate.state = 1
        self.send_sasl_message(negotiate)

        self.sasl = self.sasl_client_factory()
        ret, chosen_mech, initial_response = self.sasl.start("TOKEN,GSSAPI")
        log.debug("Chosen mech: %s" % chosen_mech)
        log.debug("Initial response: %s" % initial_response)

        # do while true
        while True:
          res = self.recv_sasl_message()
          # TODO: check mechanisms
          if res.state == 1:
            #res_token = b""
            initiate = RpcSaslProto()
            initiate.state = 2
            initiate.token = initial_response

            #auth_method = RpcSaslProto.SaslAuth()
            auth_method = initiate.auths.add()
            auth_method.mechanism = "GSSAPI"
            auth_method.method = "KERBEROS"
            auth_method.protocol = "hdfs"
            auth_method.serverId = "master01.paymentslab.int"

            #initiate.auths.append(auth_method) 
            self.send_sasl_message(initiate)
            continue
           
          if res.state == 3:
            res_token = self.sasl_evaluate_token(res)
            response = RpcSaslProto()
            response.token = res_token
            response.state = 4
            self.send_sasl_message(response)
            continue

          if res.state == 0:
            return True

    def sasl_evaluate_token(self, sasl_response):
        ret, response = self.sasl.step(sasl_response.token)
        if not ret:
          raise Exception("Bad SASL results: %s" % (self.sasl.getError()))

        return response 

    def select
