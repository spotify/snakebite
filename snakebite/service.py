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
from snakebite.channel import SocketRpcController


class RpcService(object):

    def __init__(self, service_stub_class, port, host):
        self.service_stub_class = service_stub_class
        self.port = port
        self.host = host

        # Setup the RPC channel
        self.channel = SocketRpcChannel(host=self.host, port=self.port)
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
