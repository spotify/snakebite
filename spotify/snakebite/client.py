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

import spotify.snakebite.protobuf.ClientNamenodeProtocol_pb2 as client_proto
import spotify.snakebite.protobuf.hdfs_pb2 as hdfs_proto
import spotify.snakebite.glob as glob
from spotify.snakebite.errors import RequestError
from spotify.snakebite.service import RpcService
from spotify.snakebite.errors import FileNotFoundException
from spotify.snakebite.errors import DirectoryException
from spotify.snakebite.errors import FileException
from spotify.snakebite.errors import InvalidInputException

import logging
import os
import os.path
import re
import fnmatch

log = logging.getLogger(__name__)


class Client(object):
    ''' Class that acts as a client to HDFS.

    **Example:**

    >>> from spotify.snakebite.client import Client
    >>> client = Client("localhost", 54310)

    .. note::
        ``paths`` parameters in methods are often passed as lists, since operations can work on multiple
        paths. The return values of methods are always lists of results.

    .. note::
        Parameters like ``include_children``, ``include_toplevel`` and ``recurse`` are not used
        when paths contain globs.
    '''
    FILETYPES = {
        1: "d",
        2: "f",
        3: "s"
    }

    def __init__(self, host, port):
        '''
        :param host: Hostname or IP address of the NameNode
        :type host: string
        :param port: RPC Port of the NameNode
        :type port: int
        '''
        self.host = host
        self.port = port
        self.service_stub_class = client_proto.ClientNamenodeProtocol_Stub
        self.service = RpcService(self.service_stub_class, self.port, self.host)

    def ls(self, paths, recurse=False, include_toplevel=False, include_children=True):
        ''' Issues 'ls' command and returns a list of maps that contain fileinfo

        :param paths: Paths to list
        :type paths: list
        :param recurse: Recursive listing
        :type recurse: boolean
        :param include_toplevel: Include the given path in the listing. If the path is a file, include_toplevel is always True.
        :type include_toplevel: boolean
        :param include_children: Include child nodes in the listing.
        :type include_children: boolean
        :returns: list of dictionaries

        **Examples:**

        Directory listing

        >>> client.ls(["/"])
        [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317324982L, 'block_replication': 1, 'modification_time': 1367317325346L, 'length': 6783L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/Makefile'}, {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317325431L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/build'}, {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317326510L, 'block_replication': 1, 'modification_time': 1367317326522L, 'length': 100L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/index.asciidoc'}, {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317326628L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/source'}]

        File listing

        >>> client.ls(["/Makefile"])
        [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317324982L, 'block_replication': 1, 'modification_time': 1367317325346L, 'length': 6783L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/Makefile'}]

        Get directory information

        >>> client.ls(["/source"], include_toplevel=True, include_children=False)
        [{'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317326628L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/source'}]
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")

        return self._findItems(paths, self._handleLs,
                               include_toplevel=include_toplevel,
                               include_children=include_children,
                               recurse=recurse)

    LISTING_ATTRIBUTES = ['length', 'owner', 'group', 'block_replication',
                          'modification_time', 'access_time', 'blocksize']

    def _handleLs(self, path, node):
        ''' Handle every node received for an ls request'''
        entry = {}

        entry["file_type"] = self.FILETYPES[node.fileType]
        entry["permission"] = node.permission.perm
        entry["path"] = path

        for attribute in self.LISTING_ATTRIBUTES:
            entry[attribute] = node.__getattribute__(attribute)

        return entry

    def chmod(self, paths, mode, recurse=False):
        ''' Change the mode for paths. This returns a list of maps containing the resut of the operation.

        :param paths: List of paths to chmod
        :type paths: list
        :param mode: Octal mode (e.g. 0755)
        :type mode: int
        :param recurse: Recursive chmod
        :type recurse: boolean

        .. note:: The top level directory is always included when `recurse=True`'''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chmod: no path given")
        if not mode:
            raise InvalidInputException("chmod: no mode given")

        processor = lambda path, node, mode=mode: self._handleChmod(path, node, mode)
        return self._findItems(paths, processor, include_toplevel=True,
                               include_children=False, recurse=recurse)

    def _handleChmod(self, path, node, mode):
        request = client_proto.SetPermissionRequestProto()
        request.src = path
        request.permission.perm = mode
        self.service.setPermission(request)
        return {"result": True, "path": path}

    def chown(self, paths, owner, recurse=False):
        ''' Change the owner for paths. The owner can be specified as `user` or `user:group`

        :param paths: List of paths to chmod
        :type paths: list
        :param owner: New owner
        :type owner: string
        :param recurse: Recursive chown
        :type recurse: boolean

        This always include the toplevel when recursing.'''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chown: no path given")
        if not paths:
            raise InvalidInputException("chown: no owner given")

        processor = lambda path, node, owner=owner: self._handleChown(path, node, owner)
        return self._findItems(paths, processor, include_toplevel=True,
                               include_children=False, recurse=recurse)

    def _handleChown(self, path, node, owner):
        if ":" in owner:
            (owner, group) = owner.split(":")
        else:
            group = ""

        request = client_proto.SetOwnerRequestProto()
        request.src = path
        if owner:
            request.username = owner
        if group:
            request.groupname = group
        self.service.setOwner(request)
        return {"result": True, "path": path}

    def chgrp(self, paths, group, recurse=False):
        ''' Change the group of paths.

        :param paths: List of paths to chgrp
        :type paths: list
        :param group: New group
        :type mode: string
        :param recurse: Recursive chgrp
        :type recurse: boolean

        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chgrp: no paths given")
        if not group:
            raise InvalidInputException("chgrp: no group given")

        owner = ":%s" % group
        processor = lambda path, node, owner=owner: self._handleChown(path, node, owner)
        return self._findItems(paths, processor, include_toplevel=True,
                               include_children=False, recurse=recurse)

    def count(self, paths):
        ''' Count files in a path

        :param paths: List of paths to count
        :type paths: list

        **Examples:**

        >>> client.count(['/'])
        [{'spaceConsumed': 260185L, 'quota': 2147483647L, 'spaceQuota': 18446744073709551615L, 'length': 260185L, 'directoryCount': 9L, 'path': '/', 'fileCount': 34L}]

        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("count: no path given")

        processor = lambda path, node: self._handleCount(path, node)
        return self._findItems(paths, processor, include_toplevel=True,
                               include_children=False, recurse=False)

    COUNT_ATTRIBUTES = ['length', 'fileCount', 'directoryCount', 'quota', 'spaceConsumed', 'spaceQuota']

    def _handleCount(self, path, node):
        request = client_proto.GetContentSummaryRequestProto()
        request.path = path
        response = self.service.getContentSummary(request)
        entry = {"path": path}
        for attribute in self.COUNT_ATTRIBUTES:
            entry[attribute] = response.summary.__getattribute__(attribute)
        return entry

    def df(self):
        ''' Get FS information

        **Examples:**

        >>> client.df()
        [{'used': 491520L, 'capacity': 120137519104L, 'under_replicated': 0L, 'missing_blocks': 0L, 'filesystem': 'hdfs://localhost:54310', 'remaining': 19669295104L, 'corrupt_blocks': 0L}]
        '''
        processor = lambda path, node: self._handleDf(path, node)
        return self._findItems(['/'], processor, include_toplevel=True, include_children=False, recurse=False)

    def _handleDf(self, path, node):
        request = client_proto.GetFsStatusRequestProto()
        response = self.service.getFsStats(request)
        entry = {"filesystem": "hdfs://%s:%d" % (self.host, self.port)}
        for i in ['capacity', 'used', 'remaining', 'under_replicated',
                  'corrupt_blocks', 'missing_blocks']:
            entry[i] = response.__getattribute__(i)
        return entry

    def du(self, paths, include_toplevel=False, include_children=True):
        '''Returns size information for paths

        :param paths: Paths to du
        :type paths: list
        :param include_toplevel: Include the given path in the result. If the path is a file, include_toplevel is always True.
        :type include_toplevel: boolean
        :param include_children: Include child nodes in the result.
        :type include_children: boolean
        :returns: list of dictionaries

        **Examples:**

        Children:

        >>> client.du(['/'])
        [{'path': '/Makefile', 'length': 6783L}, {'path': '/build', 'length': 244778L}, {'path': '/index.asciidoc', 'length': 100L}, {'path': '/source', 'length': 8524L}]

        Directory only:

        >>> client.du(['/'], include_toplevel=True, include_children=False)
        [{'path': '/', 'length': 260185L}]

        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("du: no path given")

        processor = lambda path, node: self._handleDu(path, node)
        return self._findItems(paths, processor, include_toplevel=include_toplevel,
                               include_children=include_children, recurse=False)

    def _handleDu(self, path, node):
        request = client_proto.GetContentSummaryRequestProto()
        request.path = path
        response = self.service.getContentSummary(request)
        return {"path": path, "length": response.summary.length}

    def rename(self, paths, dst):
        ''' Rename (move) path(s) to a destination

        :param paths: Source paths
        :type paths: list
        :param dst: destination
        :type dst: string
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("rename: no path given")
        if not dst:
            raise InvalidInputException("rename: no destination given")

        processor = lambda path, node, dst=dst: self._handleRename(path, node, dst)
        return self._findItems(paths, processor, include_toplevel=True)

    def _handleRename(self, path, node, dst):
        request = client_proto.RenameRequestProto()
        request.src = path
        request.dst = dst
        response = self.service.rename(request)
        return {"path": path, "result": response.result}

    def delete(self, paths, recurse=False):
        ''' Delete paths

        :param paths: Paths to delete
        :type paths: list
        :param recurse: Recursive delete (use with care!)
        :type recurse: boolean
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("delete: no path given")

        processor = lambda path, node, recurse=recurse: self._handleDelete(path, node, recurse)
        return self._findItems(paths, processor, include_toplevel=True)

    def _handleDelete(self, path, node, recurse):
        if (self._isDir(node) and not recurse):
            raise DirectoryException("rm: `%s': Is a directory" % path)

        # None might be passed in for recurse
        if not recurse:
            recurse = False

        request = client_proto.DeleteRequestProto()
        request.src = path
        request.recursive = recurse
        response = self.service.delete(request)
        return {"path": path, "result": response.result}

    def rmdir(self, paths):
        ''' Delete a directory

        :param paths: Paths to delete
        :type paths: list

        .. note: directories have to be empty.
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("rmdir: no path given")

        processor = lambda path, node: self._handleRmdir(path, node)
        return self._findItems(paths, processor, include_toplevel=True)

    def _handleRmdir(self, path, node):
        if not self._isDir(node):
            raise DirectoryException("rmdir: `%s': Is not a directory" % path)

        # Check if the directory is empty
        files = self.ls([path])
        if len(files) > 0:
            raise DirectoryException("rmdir: `%s': Directory is not empty" % path)

        return self._handleDelete(path, node, recurse=True)

    def touchz(self, paths, replication=None, blocksize=None):
        ''' Create a zero length file or updates the timestamp on a zero length file

        :param paths: Paths
        :type paths: list
        :param replication: Replication factor
        :type recurse: int
        :param blocksize: Block size (in bytes) of the newly created file
        :type blocksize: int
        '''

        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("touchz: no path given")

        # Let's get the blocksize and replication from the server defaults
        # provided by the namenode if they are not specified
        if not replication or blocksize:
            defaults = self.serverdefaults()

        if not replication:
            replication = defaults['replication']
        if not blocksize:
            blocksize = defaults['blockSize']

        processor = lambda path, node, replication=replication, blocksize=blocksize: self._handleTouchz(path, node, replication, blocksize)
        return self._findItems(paths, processor, include_toplevel=True, check_nonexistence=True)

    def _handleTouchz(self, path, node, replication, blocksize):
        # Item already exists
        if node:
            if node.length != 0:
                raise FileException("touchz: `%s': Not a zero-length file" % path)
            if self._isDir(node):
                raise DirectoryException("touchz: `%s': Is a directory" % path)

            response = self._createFile(path, replication, blocksize, overwrite=True)
        else:
            # Check if the parent directory exists
            parent = self._getFileInfo(os.path.dirname(path))
            if not parent:
                raise DirectoryException("touchz: `%s': No such file or directory" % path)
            else:
                response = self._createFile(path, replication, blocksize, overwrite=False)
        return {"path": path, "result": response.result}

    def setrep(self, paths, replication, recurse=False):
        ''' Set the replication factor for paths

        :param paths: Paths
        :type paths: list
        :param replication: Replication factor
        :type recurse: int
        :param recurse: Apply replication factor recursive
        :type recurse: boolean
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("setrep: no path given")
        if not replication:
            raise InvalidInputException("setrep: no replication given")

        processor = lambda path, node, replication=replication: self._handleSetrep(path, node, replication)
        return self._findItems(paths, processor, include_toplevel=True,
                               include_children=False, recurse=recurse)

    def _handleSetrep(self, path, node, replication):
        if not self._isDir(node):
            request = client_proto.SetReplicationRequestProto()
            request.src = path
            request.replication = replication
            response = self.service.setReplication(request)
            return {"result": response.result, "path": path}

    def _createFile(self, path, replication, blocksize, overwrite):
        if overwrite:
            createFlag = 0x02
        else:
            createFlag = 0x01

        # Issue a CreateRequestProto
        request = client_proto.CreateRequestProto()
        request.src = path
        request.masked.perm = 0644
        request.clientName = "snakebite"
        request.createFlag = createFlag
        request.createParent = False
        request.replication = replication
        request.blockSize = blocksize

        # The response doesn't contain anything
        self.service.create(request)

        # Issue a CompleteRequestProto
        request = client_proto.CompleteRequestProto()
        request.src = path
        request.clientName = "snakebite"

        return self.service.complete(request)

    def stat(self, paths):
        ''' Stat a fileCount

        :param paths: Path
        :type paths: string

        **Example:**

        >>> client.stat(['/index.asciidoc'])
        [{'blocksize': 134217728L, 'owner': u'wouter', 'length': 100L, 'access_time': 1367317326510L, 'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'path': '/index.asciidoc', 'modification_time': 1367317326522L, 'block_replication': 1}]
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("stat: no path given")

        processor = lambda path, node: self._handleStat(path, node)
        return self._findItems(paths, processor, include_toplevel=True)

    def _handleStat(self, path, node):
        return {"path": path,
                "file_type": self.FILETYPES[node.fileType],
                "length": node.length,
                "permission": node.permission.perm,
                "owner": node.owner,
                "group": node.group,
                "modification_time": node.modification_time,
                "access_time": node.access_time,
                "block_replication": node.block_replication,
                "blocksize": node.blocksize}

    def test(self, path, exists=False, directory=False, zero_length=False):
        '''Test if a paht exist, is a directory or has zero length

        :param path: Path to test
        :type path: string
        :param exists: Check if the path exists
        :type exists: boolean
        :param directory: Check if the path exists
        :type exists: boolean
        :param zero_length: Check if the path is zero-length
        :type zero_length: boolean

        .. note:: directory and zero lenght are AND'd.
        '''
        if not type(path) == type(""):
            raise InvalidInputException("Path should be a string")
        if not path:
            raise InvalidInputException("test: no path given")

        processor = lambda path, node, exists=exists, directory=directory, zero_length=zero_length: self._handleTest(path, node, exists, directory, zero_length)
        try:
            return self._findItems([path], processor, include_toplevel=True)[0]
        except FileNotFoundException, e:
            if exists:
                return False
            else:
                raise e

    def _handleTest(self, path, node, exists, directory, zero_length):
        return self._is_directory(directory, node) and self._is_zero_length(zero_length, node)

    def mkdir(self, paths, create_parent=False, mode=0755):
        ''' Create a directoryCount

        :param paths: Paths to create
        :type paths: list of strings
        :param create_parent: Also create the parent directories
        :type create_parent: boolean
        :param mode: Mode the directory should be created with
        :type mode: int
        '''
        if not type(paths) == type([]):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("mkdirs: no path given")

        creations = []
        for path in paths:
            orig_path = path
            if not path.startswith("/"):
                path = self._joinUserPath(path)

            fileinfo = self._getFileInfo(path)
            if not fileinfo:
                try:
                    request = client_proto.MkdirsRequestProto()
                    request.src = path
                    request.masked.perm = mode
                    request.createParent = create_parent
                    response = self.service.mkdirs(request)
                    creations.append({"path": orig_path, "result": response.result})
                except RequestError, e:
                    creations.append({"path": orig_path, "result": False, "error": str(e)})
            else:
                creations.append({"path": path, "result": False, "error": "mkdir: `%s': File exists" % orig_path})
        return creations

    def serverdefaults(self):
        '''Get server defaults

        **Example:**

        >>> client.serverdefaults()
        {'writePacketSize': 65536, 'fileBufferSize': 4096, 'replication': 1, 'bytesPerChecksum': 512, 'trashInterval': 0L, 'blockSize': 134217728L, 'encryptDataTransfer': False, 'checksumType': 2}
        '''
        request = client_proto.GetServerDefaultsRequestProto()
        response = self.service.getServerDefaults(request).serverDefaults
        return {'blockSize': response.blockSize, 'bytesPerChecksum': response.bytesPerChecksum,
                'writePacketSize': response.writePacketSize, 'replication': response.replication,
                'fileBufferSize': response.fileBufferSize, 'encryptDataTransfer': response.encryptDataTransfer,
                'trashInterval': response.trashInterval, 'checksumType': response.checksumType}

    def _is_directory(self, should_check, node):
        if not should_check:
            return True
        return self._isDir(node)

    def _is_zero_length(self, should_check, node):
        if not should_check:
            return True
        return node.length == 0

    def _getFullPath(self, path, node):
        if node.path:
            return os.path.join(path, node.path)
        else:
            return path

    def _findItems(self, paths, processor, include_toplevel=False, include_children=False, recurse=False, check_nonexistence=False):
        ''' Request file info from the NameNode and call the processor on the node(s) returned

        :param paths:
            A list of paths that need to be processed
        :param processor:
            Method that is called on an node. Method signature should be foo(path, node). For additional
            (static) params, use a lambda.
        :param include_toplevel:
            Boolean to enable the inclusion of the first node found.
            Example: listing a directory should not include the toplevel, but chmod should
            only operate on the path that is input, so it should include the toplevel.
        :param include_children:
            Include children (when the path is a directory) in processing. Recurse will always
            include children.
            Example: listing a directory should include children, but chmod shouldn't.
        :param recurse:
            Recurse into children if they are directories.
        '''
        collection = []

        if not paths:
            paths = [os.path.join("/user", os.getlogin())]

        # Expand paths if necessary (/foo/{bar,baz} --> ['/foo/bar', '/foo/baz'])
        paths = glob.expandPaths(paths)

        for path in paths:
            if not path.startswith("/"):
                path = self._joinUserPath(path)

            log.debug("Trying to find path %s" % path)

            if glob.has_magic(path):
                log.debug("Dealing with globs in %s" % path)
                collection += self._globFind(path, processor, include_toplevel)
            else:
                fileinfo = self._getFileInfo(path)
                if not fileinfo and not check_nonexistence:
                    raise FileNotFoundException("`%s': No such file or directory" % path)
                elif not fileinfo and check_nonexistence:
                    collection.append(processor(path, None))
                    return collection

                if include_toplevel or not self._isDir(fileinfo.fs):
                    # Construct the full path before processing
                    full_path = self._getFullPath(path, fileinfo.fs)
                    log.debug("Added %s to to result set" % full_path)
                    entry = processor(full_path, fileinfo.fs)
                    collection.append(entry)

                if self._isDir(fileinfo.fs) and (include_children or recurse):
                    listing = self._getDirListing(path)
                    for node in listing.dirList.partialListing:
                        full_path = self._getFullPath(path, node)
                        entry = processor(full_path, node)
                        collection.append(entry)

                        # Recurse into directories
                        if recurse and self._isDir(node):
                            # Construct the full path before processing
                            full_path = os.path.join(path, node.path)
                            collection += self._findItems([full_path],
                                                          processor,
                                                          include_toplevel=False,
                                                          include_children=False,
                                                          recurse=recurse)

        return collection

    def _getDirListing(self, path):
        request = client_proto.GetListingRequestProto()
        request.src = path
        request.startAfter = ''
        request.needLocation = False
        return self.service.getListing(request)

    def _globFind(self, path, processor, include_toplevel):
        '''Handle globs in paths.
        This is done by listing the directory before a glob and checking which
        node matches the initial glob. If there are more globs in the path,
        we don't add the found children to the result, but traverse into paths
        that did have a match.
        '''
        collection = []
        # Split path elements and check where the first occurence of magic is
        path_elements = path.split("/")
        for i, element in enumerate(path_elements):
            if glob.has_magic(element):
                first_magic = i
                break

        # Create path that we check first to get a listing we match all children
        # against. If the 2nd path element is a glob, we need to check "/", and
        # we hardcode that, since "/".join(['']) doesn't return "/"
        if first_magic == 1:
            check_path = "/"
        else:
            check_path = "/".join(path_elements[:first_magic])

        # Path that we need to match against
        match_path = "/".join(path_elements[:first_magic + 1])

        # Rest of the unmatched path. In case the rest is only one element long
        # we prepend it with "/", since "/".join(['x']) doesn't return "/x"
        rest_elements = path_elements[first_magic + 1:]
        if len(rest_elements) == 1:
            rest = "/" + rest_elements[0]
        else:
            rest = "/".join(rest_elements)

        # Check if the path exists and that it's a directory (which it should..)
        fileinfo = self._getFileInfo(check_path)
        if fileinfo and self._isDir(fileinfo.fs):
            # List all child nodes and match them agains the glob
            listing = self._getDirListing(check_path)
            for node in listing.dirList.partialListing:
                full_path = self._getFullPath(check_path, node)
                if fnmatch.fnmatch(full_path, match_path):
                    # If we have a match, but need to go deeper, we recurse
                    if rest and glob.has_magic(rest):
                        traverse_path = "/".join([full_path, rest])
                        collection += self._globFind(traverse_path, processor, include_toplevel)
                    else:
                        # If the matching node is a directory, we list the directory
                        # This is what the hadoop client does at least.
                        if self._isDir(node):
                            if include_toplevel:
                                entry = processor(full_path, node)
                                collection.append(entry)
                                
                            fp = self._getFullPath(check_path, node)
                            dir_list = self._getDirListing(fp)
                            for n in dir_list.dirList.partialListing:
                                full_child_path = self._getFullPath(fp, n)
                                entry = processor(full_child_path, n)
                                collection.append(entry)
                        else:
                            entry = processor(full_path, node)
                            collection.append(entry)

        return collection

    def _isDir(self, entry):
        return self.FILETYPES.get(entry.fileType) == "d"

    def _getFileInfo(self, path):
        request = client_proto.GetFileInfoRequestProto()
        request.src = path

        return self.service.getFileInfo(request)

    def _joinUserPath(self, path):
        return os.path.join("/user", os.getlogin(), path)
