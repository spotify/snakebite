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


class ConnectionFailureException(Exception):
    def __init__(self, msg):
        super(ConnectionFailureException, self).__init__(msg)


class DirectoryException(Exception):
    def __init__(self, msg):
        super(DirectoryException, self).__init__(msg)


class FileAlreadyExistsException(Exception):
    def __init__(self, msg):
        super(FileAlreadyExistsException, self).__init__(msg)


class FileException(Exception):
    def __init__(self, msg):
        super(FileException, self).__init__(msg)


class FileNotFoundException(Exception):
    def __init__(self, msg):
        super(FileNotFoundException, self).__init__(msg)


class InvalidInputException(Exception):
    def __init__(self, msg):
        super(InvalidInputException, self).__init__(msg)


class OutOfNNException(Exception):
    def __init__(self, msg):
        super(OutOfNNException, self).__init__(msg)


class RequestError(Exception):
    def __init__(self, msg):
        super(RequestError, self).__init__(msg)
