# -*- coding: utf-8 -*-
# Copyright (c) 2009 Las Cumbres Observatory (www.lcogt.net)
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

'''protobuf/logger.py - Module for configuring the package level logging.

This module contains a convenience function for creating and retrieving a
logger with a given name. In addition a Null handler is added to the logger
to prevent client software not implementing the logging package from
receiving "No handler" error messages.

Authors: Martin Norbury (mnorbury@lcogt.net)
         Eric Saunders (esaunders@lcogt.net)

May 2009
'''

# Standard library imports
import logging


class _NullHandler(logging.Handler):
    ''' NULL logging handler.

    A null logging handler to prevent clients that don't require the
    logging package from reporting no handlers found.
    '''

    def emit(self, record):
        ''' Override the emit function to do nothing. '''
        pass


def getLogger(name):
    ''' Create and return a logger with the specified name. '''

    # Create logger and add a default NULL handler
    log = logging.getLogger(name)
    log.addHandler(_NullHandler())

    return log
