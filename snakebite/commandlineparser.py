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

import argparse
import sys
import os
import pwd
import json
from urlparse import urlparse

from snakebite.client import HAClient
from snakebite.errors import FileNotFoundException
from snakebite.errors import DirectoryException
from snakebite.errors import FileException
from snakebite.errors import RequestError
from snakebite.formatter import format_listing
from snakebite.formatter import format_results
from snakebite.formatter import format_counts
from snakebite.formatter import format_fs_stats
from snakebite.formatter import format_stat
from snakebite.formatter import format_du
from snakebite.config import HDFSConfig
from snakebite.version import version
from snakebite.namenode import Namenode


def exitError(error):
    if isinstance(error, FileNotFoundException) or \
       isinstance(error, DirectoryException) or \
       isinstance(error, FileException):
        print str(error)
    elif isinstance(error, RequestError):
        print "Request error: %s" % str(error)
    else:
        raise error
    sys.exit(-1)


def command(args="", descr="", allowed_opts="", visible=True):
    def wrap(f):
        Commands.methods[f.func_name] = {"method": f,
                                         "args": args,
                                         "descr": descr,
                                         "allowed_opts": allowed_opts,
                                         "visible": visible}
    return wrap


class Commands(object):
    methods = {}


class ArgumentParserError(Exception):

    def __init__(self, message, error_message, prog, stdout=None, stderr=None, error_code=None):
        Exception.__init__(self, message, stdout, stderr)
        self.message = message
        self.error_message = error_message
        self.prog = prog


class Parser(argparse.ArgumentParser):
    def print_help(self):
        print ''.join([self.usage, self.epilog])

    def error(self, message):  # Override error message to show custom help.
        raise ArgumentParserError("SystemExit", message, self.prog)


class CommandLineParser(object):

    GENERIC_OPTS = {'D': {"short": '-D',
                          "long": '--debug',
                          "help": 'Show debug information',
                          "action": 'store_true'},
                    'j': {"short": '-j',
                          "long": '--json',
                          "help": 'JSON output',
                          "action": 'store_true'},
                    'n': {"short": '-n',
                          "long": '--namenode',
                          "help": 'namenode host',
                          "type": str},
                    'V': {"short": '-V',
                          "long": '--version',
                          "help": 'Hadoop protocol version (default:%d)' % Namenode.DEFAULT_VERSION,
                          "default": Namenode.DEFAULT_VERSION,
                          "type": float},
                    'p': {"short": '-p',
                          "long": '--port',
                          "help": 'namenode RPC port',
                          "type": int},
                    'h': {"short": '-h',
                          "long": '--help',
                          "help": 'show help',
                          "type": int},
                    'v': {"short": '-v',
                          "long": '--ver',
                          "help": 'Display snakebite version',
                          "type": int}
                    }

    SUB_OPTS = {'R': {"short": '-R',
                      "long": '--recurse',
                      "help": 'recurse into subdirectories',
                      "action": 'store_true'},
                'd': {"short": '-d',
                      "long": '--directory',
                      "help": 'show only the path and no children / check if path is a dir',
                      "action": 'store_true'},
                's': {"short": '-s',
                      "long": '--summary',
                      "help": 'print summarized output',
                      "action": 'store_true'},
                'z': {"short": '-z',
                      "long": '--zero',
                      "help": 'check for zero length',
                      "action": 'store_true'},
                'e': {"short": '-e',
                      "long": '--exists',
                      "help": 'check if file exists',
                      "action": 'store_true'},
                'checkcrc': {"short": '-checkcrc',
                             "long": "--checkcrc",
                             "help": 'check Crc',
                             "action": 'store_true'},
                'f': {"short": '-f',
                      "long": "--append",
                      "help": 'show appended data as the file grows',
                      "action": 'store_true'},
                'nl': {"short": '-nl',
                       "long": "--newline",
                       "help": 'add a newline character at the end of each file.',
                       "action": 'store_true'},
                'h': {"short": '-h',
                      "long": '--human',
                      "help": 'human readable output',
                      "action": 'store_true'}
                }

    def __init__(self):
        usage = "snakebite [general options] cmd [arguments]"
        epilog = "\ngeneral options:\n"
        epilog += "\n".join(sorted(["  %-30s %s" % ("%s %s" % (v['short'], v['long']), v['help']) for k, v in self.GENERIC_OPTS.iteritems()]))
        epilog += "\n\ncommands:\n"
        epilog += "\n".join(sorted(["  %-30s %s" % ("%s %s" % (k, v['args']), v['descr']) for k, v in Commands.methods.iteritems() if v['visible']]))
        epilog += "\n\nto see command-specific options use: snakebite [cmd] --help"

        self.parser = Parser(usage=usage, epilog=epilog, formatter_class=argparse.RawTextHelpFormatter, add_help=False)
        self._build_parent_parser()
        self._add_subparsers()
        self.namenodes = []

    def _build_parent_parser(self):
        #general options
        for opt_name, opt_data in self.GENERIC_OPTS.iteritems():
            if 'action' in opt_data:
                self.parser.add_argument(opt_data['short'], opt_data['long'], help=opt_data['help'], action=opt_data['action'])
            else:
                if 'default' in opt_data:
                    self.parser.add_argument(opt_data['short'], opt_data['long'], help=opt_data['help'], type=opt_data['type'], default=opt_data['default'])
                else:
                    self.parser.add_argument(opt_data['short'], opt_data['long'], help=opt_data['help'], type=opt_data['type'])

    def _add_subparsers(self):
        default_dir = os.path.join("/user", pwd.getpwuid(os.getuid())[0])

        #sub-options
        arg_parsers = {}
        for opt_name, opt_data in self.SUB_OPTS.iteritems():
            arg_parsers[opt_name] = argparse.ArgumentParser(add_help=False)
            arg_parsers[opt_name].add_argument(opt_data['short'], opt_data['long'], help=opt_data['help'],
                                               action=opt_data['action'])

        subcommand_help_parser = argparse.ArgumentParser(add_help=False)
        subcommand_help_parser.add_argument('-H', '--help', action='store_true')

        # NOTE: args and dirs are logically equivalent except for default val.
        # Difference in naming gives more valuable error/help output.

        # 0 or more dirs
        positional_arg_parsers = {}
        positional_arg_parsers['[dirs]'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['[dirs]'].add_argument('dir', nargs='*', default=[default_dir], help="[dirs]")

        # 1 or more dirs
        positional_arg_parsers['dir [dirs]'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['dir [dirs]'].add_argument('dir', nargs='+', default=[default_dir], help="dir [dirs]")

        # 2 dirs
        positional_arg_parsers['src dst'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['src dst'].add_argument('src_dst', nargs=2, default=[default_dir], help="src dst")

        # 1 or more args
        positional_arg_parsers['[args]'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['[args]'].add_argument('arg', nargs='*', help="[args]")

        # 1 arg
        positional_arg_parsers['arg'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['arg'].add_argument('single_arg', default=default_dir, help="arg")

        # 1 (integer) arg
        positional_arg_parsers['(int) arg'] = argparse.ArgumentParser(add_help=False)
        positional_arg_parsers['(int) arg'].add_argument('single_int_arg', default='0', help="(integer) arg",
                                                         type=int)

        subparsers = self.parser.add_subparsers()
        for cmd_name, cmd_info in Commands.methods.iteritems():
            parents = [arg_parsers[opt] for opt in cmd_info['allowed_opts'] if opt in arg_parsers]
            parents += [subcommand_help_parser]
            if 'req_args' in cmd_info and not cmd_info['req_args'] is None:
                parents += [positional_arg_parsers[arg] for arg in cmd_info['req_args']]
            command_parser = subparsers.add_parser(cmd_name, add_help=False, parents=parents)
            command_parser.set_defaults(command=cmd_name)

    def read_config(self):

        # Try to retrieve namenode config from within CL arguments
        if self._read_config_cl():
            return

        ''' Try to read the config from ~/.snakebiterc and if that doesn't exist,
        check $HADOOP_HOME/core-site.xml and $HADOOP_HOME/hdfs-site.xml
        and create a ~/.snakebiterc from that.
        '''
        config_file = os.path.join(os.path.expanduser('~'), '.snakebiterc')

        if os.path.exists(config_file):
            #if ~/.snakebiterc exists - read config from it
            self._read_config_snakebiterc()
        else:
            # Try to read the configuration for HDFS configuration files
            configs = HDFSConfig.get_external_config()

            # if configs exist and contain something
            if configs:
                for config in configs:
                    nn = Namenode(config['namenode'],
                                  config['port'])
                    self.namenodes.append(nn)

                # Save retrieved configuration to snakebite config file
                self._write_hadoop_config(config_file)

        if len(self.namenodes):
            return
        else:
            print "No ~/.snakebiterc found, no HADOOP_HOME set and no -n and -p provided"
            print "Tried to find core-site.xml in:"
            for core_conf_path in HDFSConfig.core_try_paths:
                print " - %s" % core_conf_path
            print "Tried to find hdfs-site.xml in:"
            for hdfs_conf_path in HDFSConfig.hdfs_try_paths:
                print " - %s" % hdfs_conf_path
            print "\nYou can manually create ~/.snakebiterc with the following content:"
            print '{"namenode": "ip/hostname", "port": 54310, "version": %d}' % Namenode.DEFAULT_VERSION
            sys.exit(1)

    def _read_config_snakebiterc(self):
        with open(os.path.join(os.path.expanduser('~'), '.snakebiterc')) as config_file:
            configs = json.load(config_file)

        if isinstance(configs, list):
            # config is a list of namenode(s) - possibly HA
            for config in configs:
                nn = Namenode(config['namenode'],
                              config['port'],
                              config.get('version', Namenode.DEFAULT_VERSION))
                self.namenodes.append(nn)
        elif isinstance(configs, dict):
             # config is a single namenode - no HA
            self.namenodes.append(Namenode(configs['namenode'],
                                           configs['port'],
                                           configs.get('version', Namenode.DEFAULT_VERSION)))
        else:
            print "Config retrieved from ~/.snakebiterc is corrupted! Remove it!"
            sys.exit(1)

    def _read_config_cl(self):
        ''' Check if any directory arguments contain hdfs://'''
        if self.args and 'dir' in self.args:
            dirs_to_check = list(self.args.dir)
            if self.args.command == 'mv':
                dirs_to_check.append(self.args.single_arg)
            for directory in dirs_to_check:
                if 'hdfs://' in directory:
                    parse_result = urlparse(directory)

                    if not self.args.namenode is None and not self.args.port is None and (self.args.port != parse_result.port or self.args.namenode != parse_result.hostname):
                        print "error: conflicting nodenames or ports"
                        sys.exit(-1)
                    else:
                        self.args.namenode = parse_result.hostname
                        self.args.port = parse_result.port

                        if directory in self.args.dir:
                            self.args.dir.remove(directory)
                            self.args.dir.append(parse_result.path)
                        else:
                            self.args.single_arg = parse_result.path

        if self.args.namenode and self.args.port:
            # If namenode config found based on arguments, save namenode
            self.namenodes.append(Namenode(self.args.namenode, self.args.port))
            return True
        else:
            return False

    def _write_hadoop_config(self, config_file_path):
        # Write config to file
        with open(config_file_path, "w") as config_file:
            config = []
            for namenode in self.namenodes:
                config.append(namenode.toDict())

            if len(config) > 1:
                #If many NNs use list syntax
                pretty_config = json.dumps(config).split('},')
                config_file.write('},\n'.join(pretty_config))
            elif len(config) == 1:
                #If just one NN use old syntax
                config_file.write(json.dumps(config[0]))
            else:
                print "Try to write configuration without NameNode configuration!"
                sys.exit(-1)

    def parse(self, non_cli_input=None):  # Allow input for testing purposes
        if not sys.argv[1:] and not non_cli_input:
            self.parser.print_help()
            sys.exit(-1)

        try:
            args = self.parser.parse_args(non_cli_input)
        except ArgumentParserError, error:
            if "-h" in sys.argv or "--help" in sys.argv:  # non cli input?
                commands = [cmd for (cmd, description) in Commands.methods.iteritems() if description['visible'] is True]
                command = error.prog.split()[-1]
                if command in commands:
                    self.usage_helper(command)
                else:
                    self.parser.print_help()
                self.parser.exit(2)
            elif "-v" in sys.argv or "--ver" in sys.argv:
                print version()
                self.parser.exit(0)
            else:
                self.parser.print_usage(sys.stderr)
                self.parser.exit(2, 'error: %s. Use -h for help.\n' % (error.error_message))

        self.cmd = args.command
        self.args = args
        return self.args

    def setup_client(self):
        self.client = HAClient(self.namenodes)

    def execute(self):
        if self.args.help:
            #if 'ls -H' is called, execute 'usage ls'
            self.args.arg = [self.cmd]
            return Commands.methods['usage']['method'](self)
        if not Commands.methods.get(self.cmd):
            self.parser.print_help()
            sys.exit(-1)
        try:
            return Commands.methods[self.cmd]['method'](self)
        except Exception, e:
            exitError(e)

    def command(args="", descr="", allowed_opts="", visible=True, req_args=None):
        def wrap(f):
            Commands.methods[f.func_name] = {"method": f,
                                             "args": args,
                                             "descr": descr,
                                             "allowed_opts": allowed_opts,
                                             "visible": visible,
                                             "req_args": req_args}
        return wrap

    @command(visible=False)
    def commands(self):
        print "\n".join(sorted([k for k, v in Commands.methods.iteritems() if v['visible']]))

    @command(args="[path]", descr="Used for command line completion", visible=False, req_args=['[dirs]'])
    def complete(self):
        self.args.summary = True
        self.args.directory = False
        self.args.recurse = False
        self.args.human = False
        try:
            for line in self._listing():
                print line.replace(" ", "\\\\ ")
        except FileNotFoundException:
            pass

    @command(args="[paths]", descr="list a path", allowed_opts=["d", "R", "s", "h"], req_args=['[dirs]'])
    def ls(self):
        for line in self._listing():
            print line

    def _listing(self):
        # Mimicking hadoop client behaviour
        if self.args.directory:
            include_children = False
            recurse = False
            include_toplevel = True
        else:
            include_children = True
            include_toplevel = False
            recurse = self.args.recurse

        listing = self.client.ls(self.args.dir, recurse=recurse,
                                 include_toplevel=include_toplevel,
                                 include_children=include_children)

        for line in format_listing(listing, json_output=self.args.json,
                                   human_readable=self.args.human,
                                   recursive=recurse,
                                   summary=self.args.summary):
            yield line

    @command(args="[paths]", descr="create directories", req_args=['dir [dirs]'])
    def mkdir(self):
        creations = self.client.mkdir(self.args.dir)
        for line in format_results(creations, json_output=self.args.json):
            print line

    @command(args="[paths]", descr="create directories and their parents", req_args=['dir [dirs]'])
    def mkdirp(self):
        creations = self.client.mkdir(self.args.dir, create_parent=True)
        for line in format_results(creations, json_output=self.args.json):
            print line

    @command(args="<owner:grp> [paths]", descr="change owner", allowed_opts=["R"], req_args=['arg', 'dir [dirs]'])
    def chown(self):
        owner = self.args.single_arg
        try:
            mods = self.client.chown(self.args.dir, owner, recurse=self.args.recurse)
            for line in format_results(mods, json_output=self.args.json):
                print line
        except FileNotFoundException, e:
            exitError(e)

    @command(args="<mode> [paths]", descr="change file mode (octal)", allowed_opts=["R"], req_args=['(int) arg', 'dir [dirs]'])
    def chmod(self):
        mode = int(str(self.args.single_int_arg), 8)
        mods = self.client.chmod(self.args.dir, mode, recurse=self.args.recurse)
        for line in format_results(mods, json_output=self.args.json):
            print line

    @command(args="<grp> [paths]", descr="change group", allowed_opts=["R"], req_args=['arg', 'dir [dirs]'])
    def chgrp(self):
        grp = self.args.single_arg
        mods = self.client.chgrp(self.args.dir, grp, recurse=self.args.recurse)
        for line in format_results(mods, json_output=self.args.json):
            print line

    @command(args="[paths]", descr="display stats for paths", allowed_opts=['h'], req_args=['[dirs]'])
    def count(self):
        counts = self.client.count(self.args.dir)
        for line in format_counts(counts, json_output=self.args.json,
                                  human_readable=self.args.human):
            print line

    @command(args="", descr="display fs stats", allowed_opts=['h'])
    def df(self):
        result = self.client.df()
        for line in format_fs_stats(result, json_output=self.args.json,
                                    human_readable=self.args.human):
            print line

    @command(args="[paths]", descr="display disk usage statistics", allowed_opts=["s", "h"], req_args=['[dirs]'])
    def du(self):
        if self.args.summary:
            include_children = False
            include_toplevel = True
        else:
            include_children = True
            include_toplevel = False
        result = self.client.du(self.args.dir, include_toplevel=include_toplevel, include_children=include_children)
        for line in format_du(result, json_output=self.args.json, human_readable=self.args.human):
            print line

    @command(args="[paths] dst", descr="move paths to destination", req_args=['dir [dirs]', 'arg'])
    def mv(self):
        paths = self.args.dir
        dst = self.args.single_arg
        result = self.client.rename(paths, dst)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="[paths]", descr="remove paths", allowed_opts=["R"], req_args=['dir [dirs]'])
    def rm(self):
        result = self.client.delete(self.args.dir, recurse=self.args.recurse)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="[paths]", descr="creates a file of zero length", req_args=['dir [dirs]'])
    def touchz(self):
        result = self.client.touchz(self.args.dir)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="", descr="show server information")
    def serverdefaults(self):
        print self.client.serverdefaults()

    @command(args="[dirs]", descr="delete a directory", req_args=['dir [dirs]'])
    def rmdir(self):
        result = self.client.rmdir(self.args.dir)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="<rep> [paths]", descr="set replication factor", allowed_opts=['R'], req_args=['(int) arg', 'dir [dirs]'])
    def setrep(self):
        rep_factor = int(self.args.single_int_arg)
        result = self.client.setrep(self.args.dir, rep_factor, recurse=self.args.recurse)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="<cmd>", descr="show cmd usage", req_args=['[args]'])
    def usage(self):
        if not 'arg' in self.args or self.args.arg == []:
            self.parser.print_help()
            sys.exit(-1)

        for sub_cmd in self.args.arg:
            self.usage_helper(sub_cmd)

    def usage_helper(self, command):
        cmd_entry = Commands.methods.get(command)
        if not cmd_entry:
            self.parser.print_help()
            sys.exit(-1)
        cmd_args = []
        cmd_descriptions = "\ncommand options: \n"
        allowed_opts = cmd_entry.get('allowed_opts')
        if allowed_opts:
            cmd_args += ["[-%s]" % o for o in allowed_opts]
            cmd_descriptions += "\n".join(sorted([" %-30s %s" % ("%s %s" % (self.SUB_OPTS[o]['short'], self.SUB_OPTS[o]['long']), self.SUB_OPTS[o]['help']) for o in allowed_opts]))
        args = cmd_entry.get('args')
        if args:
            cmd_args.append(args)

        print "usage: snakebite [general options] %s %s" % (command, " ".join(cmd_args))

        general_opts = "\ngeneral options:\n"
        general_opts += "\n".join(sorted(["  %-30s %s" % ("%s %s" % (v['short'], v['long']), v['help']) for k, v in self.GENERIC_OPTS.iteritems()]))
        print general_opts

        if allowed_opts:
            print cmd_descriptions

    @command(args="[paths]", descr="stat information", req_args=['dir [dirs]'])
    def stat(self):
        print format_stat(self.client.stat(self.args.dir), json_output=self.args.json)

    @command(args="path", descr="test a path", allowed_opts=['d', 'z', 'e'], req_args=['arg'])
    def test(self):
        path = self.args.single_arg
        if self.client.test(path, exists=self.args.exists, directory=self.args.directory, zero_length=self.args.zero):
            sys.exit(0)
        else:
            sys.exit(1)

    @command(args="[paths]", descr="copy source paths to stdout", allowed_opts=['checkcrc'], req_args=['dir [dirs]'])
    def cat(self):
        for file_to_read in self.client.cat(self.args.dir, check_crc=self.args.checkcrc):
            for load in file_to_read:
                sys.stdout.write(load)

    @command(args="path dst", descr="copy local file reference to destination", req_args=['dir [dirs]', 'arg'], visible=False)
    def copyFromLocal(self):
        src = self.args.dir
        dst = self.args.single_arg
        result = self.client.copyFromLocal(src, dst)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="[paths] dst", descr="copy paths to local file system destination", allowed_opts=['checkcrc'], req_args=['dir [dirs]', 'arg'])
    def copyToLocal(self):
        paths = self.args.dir
        dst = self.args.single_arg
        result = self.client.copyToLocal(paths, dst, check_crc=self.args.checkcrc)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="[paths] dst", descr="copy files from source to destination", allowed_opts=['checkcrc'], req_args=['dir [dirs]', 'arg'], visible=False)
    def cp(self):
        paths = self.args.dir
        dst = self.args.single_arg
        result = self.client.cp(paths, dst, checkcrc=self.args.checkcrc)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="file dst", descr="copy files to local file system destination", allowed_opts=['checkcrc'], req_args=['dir [dirs]', 'arg'])
    def get(self):
        paths = self.args.dir
        dst = self.args.single_arg
        result = self.client.copyToLocal(paths, dst, check_crc=self.args.checkcrc)
        for line in format_results(result, json_output=self.args.json):
            print line

    @command(args="dir dst", descr="concatenates files in source dir into destination local file", allowed_opts=['nl'], req_args=['src dst'])
    def getmerge(self):
        source = self.args.src_dst[0]
        dst = self.args.src_dst[1]
        result = self.client.getmerge(source, dst, newline=self.args.newline)
        for line in format_results(result, json_output=self.args.json):
            print line

    # @command(args="[paths] dst", descr="copy sources from local file system to destination", req_args=['dir [dirs]', 'arg'])
    # def put(self):
    #     paths = self.args.dir
    #     dst = self.args.single_arg
    #     result = self.client.put(paths, dst)
    #     for line in format_results(result, json_output=self.args.json):
    #         print line

    @command(args="path", descr="display last kilobyte of the file to stdout", allowed_opts=['f'], req_args=['arg'])
    def tail(self):
        path = self.args.single_arg
        result = self.client.tail(path, append=self.args.append)
        for line in result:
            print line

    @command(args="path [paths]", descr="output file in text format", allowed_opts=['checkcrc'], req_args=['dir [dirs]'])
    def text(self):
        paths = self.args.dir
        result = self.client.text(paths)
        for line in result:
            print line
