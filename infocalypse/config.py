""" Helper class used to persist stored state for Infocalypse
    mercurial extension.

    Copyright (C) 2009 Darrell Karbott

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public
    License as published by the Free Software Foundation; either
    version 2.0 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

    Author: djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks
"""


import os
import sys

from fcpclient import get_usk_hash, is_usk_file, get_version, \
     get_usk_for_usk_version
from knownrepos import DEFAULT_TRUST, DEFAULT_GROUPS, \
     DEFAULT_NOTIFICATION_GROUP
from mercurial import util
from ConfigParser import ConfigParser

if sys.platform == 'win32':
    CFG_NAME = 'infocalypse.ini'
else:
    CFG_NAME = '.infocalypse'

DEFAULT_CFG_PATH = '~/%s' % CFG_NAME

def normalize(usk_or_id):
    """ Returns a USK hash. """
    if usk_or_id.startswith('USK'):
        usk_or_id = get_usk_hash(usk_or_id)
    return usk_or_id

def norm_path(dir_name):
    """ Get a canonical path rep which works on Windows. """
    dir_name = os.path.normcase(os.path.abspath(dir_name))
    # Hack to deal with the fact that ConfigParser parsing
    # chokes on ':' in option values.
    # Required for Windows. Should be a harmless NOP on *nix.
    split = os.path.splitdrive(dir_name)
    fixed = split[0].replace(':', '') + split[1]
    return fixed

# Eventually set state from fms feed. i.e. latest repo updates.
class Config:
    """ Persisted state used by the Infocalypse mercurial extension. """
    def __init__(self):
        # repo_id -> version map
        self.version_table = {}
        # repo_dir -> request usk map
        self.request_usks = {}
        # repo_id -> insert uri map
        self.insert_usks = {}
        # fms_id -> (usk_hash, ...) map
        self.fmsread_trust_map = {}
        self.fmsread_groups = ()

        self.file_name = None

        # Use a dict instead of members to avoid pylint R0902.
        self.defaults = {}
        self.defaults['HOST'] = '127.0.0.1'
        self.defaults['PORT'] = 9481
        self.defaults['TMP_DIR'] = None
        self.defaults['DEFAULT_PRIVATE_KEY'] = None

        self.defaults['FMS_HOST'] = '127.0.0.1'
        self.defaults['FMS_PORT'] = 1119
        self.defaults['FMS_ID'] = None # User must set this in config.
        self.defaults['FMSNOTIFY_GROUP'] = DEFAULT_NOTIFICATION_GROUP

    def get_index(self, usk_or_id):
        """ Returns the highest known USK version for a USK or None. """
        return self.version_table.get(normalize(usk_or_id))

    def update_index(self, usk_or_id, index):
        """ Update the stored index value for a USK. """
        usk_or_id = normalize(usk_or_id)
        prev = self.get_index(usk_or_id)
        index = abs(index)
        if not prev is None and index < prev:
            print "update_index -- exiting, new value is lower %i %i %s" % \
                  (prev, index, usk_or_id)
            return
        self.version_table[usk_or_id] = index

    def update_dir(self, repo_dir, usk):
        """ Updated the repo USK used pull changes into repo_dir. """
        assert is_usk_file(usk)
        repo_dir = norm_path(repo_dir)
        self.request_usks[repo_dir] = usk

    def get_request_uri(self, for_dir):
        """ Get the repo USK used to pull changes into for_dir or None. """
        uri = self.request_usks.get(norm_path(for_dir))
        if uri is None:
            return None
        version = self.get_index(uri)
        if not version is None:
            if version > get_version(uri):
                uri = get_usk_for_usk_version(uri, version)
        return uri

    def get_insert_uri(self, for_usk_or_id):
        """ Get the insert USK for the request USK or None. """
        uri = self.insert_usks.get(normalize(for_usk_or_id))
        if uri is None:
            return None
        version = self.get_index(for_usk_or_id)
        if not version is None:
            if version > get_version(uri):
                uri = get_usk_for_usk_version(uri, version)

        return uri

    def set_insert_uri(self, for_usk_or_id, insert_usk):
        """ Set the insert USK associated with the request USK. """
        self.insert_usks[normalize(for_usk_or_id)] = insert_usk

    # Hmmm... really nescessary?
    def get_dir_insert_uri(self, repo_dir):
        """ Return the insert USK for repo_dir or None. """
        request_uri = self.request_usks.get(norm_path(repo_dir))
        if request_uri is None:
            return None
        return self.get_insert_uri(request_uri)

    # MY_KEY/foobar -- i.e. to insert
    # MIRROR/freenet -- i.e. to request
    #def get_key_alias(self, alias, is_public):
    #    pass

    @classmethod
    def update_defaults(cls, parser, cfg):
        """ INTERNAL: Helper function to simplify from_file. """
        if parser.has_section('default'):
            if parser.has_option('default','host'):
                cfg.defaults['HOST'] = parser.get('default','host')
            if parser.has_option('default','port'):
                cfg.defaults['PORT'] = parser.getint('default','port')
            if parser.has_option('default','tmp_dir'):
                cfg.defaults['TMP_DIR'] = parser.get('default', 'tmp_dir')
            if parser.has_option('default','default_private_key'):
                cfg.defaults['DEFAULT_PRIVATE_KEY'] = (
                    parser.get('default','default_private_key'))

            if parser.has_option('default','fms_host'):
                cfg.defaults['FMS_HOST'] = parser.get('default','fms_host')
            if parser.has_option('default','fms_port'):
                cfg.defaults['FMS_PORT'] = parser.getint('default','fms_port')
            if parser.has_option('default','fms_id'):
                cfg.defaults['FMS_ID'] = parser.get('default','fms_id')
            if parser.has_option('default','fmsnotify_group'):
                cfg.defaults['FMSNOTIFY_GROUP'] = parser.get('default',
                                                             'fmsnotify_group')
            if parser.has_option('default','fmsread_groups'):
                cfg.fmsread_groups = (parser.get('default','fmsread_groups').
                                      strip().split('|'))
            else:
                cfg.fmsread_groups = DEFAULT_GROUPS

    @classmethod
    def from_file(cls, file_name):
        """ Make a Config from a file. """
        file_name = os.path.expanduser(file_name)
        parser = ConfigParser()
        parser.read(file_name)
        cfg = Config()
        if parser.has_section('index_values'):
            for repo_id in parser.options('index_values'):
                cfg.version_table[repo_id] = (
                    parser.getint('index_values', repo_id))
        if parser.has_section('request_usks'):
            for repo_dir in parser.options('request_usks'):
                cfg.request_usks[repo_dir] = parser.get('request_usks',
                                                        repo_dir)
        if parser.has_section('insert_usks'):
            for repo_id in parser.options('insert_usks'):
                cfg.insert_usks[repo_id] = parser.get('insert_usks', repo_id)

        # ignored = fms_id|usk_hash|usk_hash|...
        if parser.has_section('fmsread_trust_map'):
            for ordinal in parser.options('fmsread_trust_map'):
                fields = parser.get('fmsread_trust_map',
                                    ordinal).strip().split('|')
                # REDFLAG: better validation for fms_id, hashes?
                if fields[0].find('@') == -1:
                    raise ValueError("%s doesn't look like an fms id." %
                                     fields[0])
                if len(fields) < 2:
                    raise ValueError("No USK hashes for fms id: %s?" %
                                     fields[0])
                cfg.fmsread_trust_map[fields[0]] = tuple(fields[1:])
        else:
            cfg.fmsread_trust_map = DEFAULT_TRUST

        Config.update_defaults(parser, cfg)

        cfg.file_name = file_name
        return cfg

    @classmethod
    def from_ui(cls, ui_):
        """ Make a Config from a ui.

            This checks the [infocalypse] section of the user's
            .hgrc for a cfg_file entry, and creates a Config from
            that file.

            If there's no [infocalypse] section, a Config is
            created from the default file."""

        file_name = ui_.config('infocalypse', 'cfg_file', None)
        if file_name is None:
            file_name = os.path.expanduser(DEFAULT_CFG_PATH)
        if not os.path.exists(file_name):
            ui_.warn("Couldn't read config file: %s\n" % file_name)
            raise util.Abort("Run fn-setup.\n")
        return Config.from_file(file_name)

    @classmethod
    def to_file(cls, cfg, file_name=None):
        """ Writes a Config to a file. """
        if file_name is None:
            if cfg.file_name is None:
                file_name = os.path.expanduser(DEFAULT_CFG_PATH)
            else:
                file_name = cfg.file_name
        file_name = os.path.expanduser(file_name)
        parser = ConfigParser()
        parser.add_section('default')
        parser.set('default', 'host', cfg.defaults['HOST'])
        parser.set('default', 'port', cfg.defaults['PORT'])
        parser.set('default', 'tmp_dir', cfg.defaults['TMP_DIR'])
        parser.set('default', 'default_private_key',
                   cfg.defaults['DEFAULT_PRIVATE_KEY'])

        parser.set('default', 'fms_host', cfg.defaults['FMS_HOST'])
        parser.set('default', 'fms_port', cfg.defaults['FMS_PORT'])
        parser.set('default', 'fms_id', cfg.defaults['FMS_ID'])
        parser.set('default', 'fmsnotify_group',
                   cfg.defaults['FMSNOTIFY_GROUP'])
        parser.set('default', 'fmsread_groups', '|'.join(cfg.fmsread_groups))

        parser.add_section('index_values')
        for repo_id in cfg.version_table:
            parser.set('index_values', repo_id, cfg.version_table[repo_id])
        parser.add_section('request_usks')
        for repo_dir in cfg.request_usks:
            parser.set('request_usks', repo_dir, cfg.request_usks[repo_dir])
        parser.add_section('insert_usks')
        for repo_id in cfg.insert_usks:
            parser.set('insert_usks', repo_id, cfg.insert_usks[repo_id])
        parser.add_section('fmsread_trust_map')
        for index, fms_id in enumerate(cfg.fmsread_trust_map):
            entry = cfg.fmsread_trust_map[fms_id]
            assert len(entry) > 0
            parser.set('fmsread_trust_map', str(index),
                       fms_id + '|' + '|'.join(entry))

        out_file = open(file_name, 'wb')
        try:
            parser.write(out_file)
        finally:
            out_file.close()
