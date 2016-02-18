# -*- coding: utf-8 -*-
# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from snakebite.channel import SocketRpcChannel
import google.protobuf.service as service


class RpcService(object):
    def __init__(self, service_stub_class, port, host, hadoop_version, effective_user=None,
                 use_sasl=False, hdfs_namenode_principal=None, sock_connect_timeout=10000,
                 sock_request_timeout=10000):
        self.service_stub_class = service_stub_class
        self.port = port
        self.host = host

        # Setup the RPC channel
        self.channel = SocketRpcChannel(host=self.host, port=self.port, version=hadoop_version,
                                        effective_user=effective_user, use_sasl=use_sasl,
                                        hdfs_namenode_principal=hdfs_namenode_principal,
                                        sock_connect_timeout=sock_connect_timeout,
                                        sock_request_timeout=sock_request_timeout,)
        self.service = self.service_stub_class(self.channel)

        # go through service_stub methods and add a wrapper function to
        # this object that will call the method
        for method in service_stub_class.GetDescriptor().methods:
            # Add service methods to the this object
            rpc = lambda request, service=self, method=method.name: service.call(service_stub_class.__dict__[method], request)

            self.__dict__[method.name] = rpc

    def call(self, method, request):
        controller = SocketRpcController()
        return method(self.service, controller, request)


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
