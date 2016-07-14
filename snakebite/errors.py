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


class SnakebiteException(Exception):
    """
    Common base class for all snakebite exceptions.
    """
    pass


class FatalException(SnakebiteException):
    """
    FatalException indicates that retry of current operation alone would have the same effect.
    """
    pass


class TransientException(SnakebiteException):
    """
    TransientException indicates that retry of current operation could help.
    """
    pass


class ConnectionFailureException(TransientException):
    def __init__(self, msg):
        super(ConnectionFailureException, self).__init__(msg)


class DirectoryException(FatalException):
    def __init__(self, msg):
        super(DirectoryException, self).__init__(msg)


class FileAlreadyExistsException(FatalException):
    def __init__(self, msg):
        super(FileAlreadyExistsException, self).__init__(msg)


class FileException(FatalException):
    def __init__(self, msg):
        super(FileException, self).__init__(msg)


class FileNotFoundException(FatalException):
    def __init__(self, msg):
        super(FileNotFoundException, self).__init__(msg)


class InvalidInputException(FatalException):
    def __init__(self, msg):
        super(InvalidInputException, self).__init__(msg)


class OutOfNNException(TransientException):
    def __init__(self, msg):
        super(OutOfNNException, self).__init__(msg)


class RequestError(TransientException):
    """
    Note: request error could be transient and could be fatal, depending on underlying error.
    """
    def __init__(self, msg):
        super(RequestError, self).__init__(msg)
