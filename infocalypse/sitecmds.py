""" Implementation of commands to insert freesites.

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

from ConfigParser import ConfigParser

from mercurial import util

from fcpconnection import FCPError
from fcpclient import FCPClient, get_file_infos, set_index_file

def write_default_config(ui_, repo):
    """ Write a default freesite.cfg file into the repository root dir. """
    file_name = os.path.join(repo.root, 'freesite.cfg')

    if os.path.exists(file_name):
        raise util.Abort("Already exists: %s" % file_name)

    out_file = open(file_name, 'w')
    try:
        out_file.write("""[default]
# Human readable site name.
site_name = default
# Directory to insert from relative to the repository root.
site_dir = site_root
# Optional external file to load the site key from, relative
# to the directory your .infocalypse/infocalypse.ini file
# is stored in. This file should contain ONLY the SSK insert
# key up to the first slash.
#
# If this value is not set the insert SSK for the repo is
# used.
#site_key_file = example_freesite_key.txt
#
# Optional file to display by default.  If this is not
# set index.html is used.
#default_file = index.html
""")
    finally:
        out_file.close()

    ui_.status('Created config file:\n%s\n' % file_name)
    ui_.status('You probably want to edit at least the site_name.\n')

def read_freesite_cfg(ui_, repo, params, stored_cfg):
    """ Read param out of the freesite.cfg file. """
    cfg_file = os.path.join(repo.root, 'freesite.cfg')

    ui_.status('Using config file:\n%s\n' % cfg_file)
    if not os.path.exists(cfg_file):
        ui_.warn("Can't read: %s\n" % cfg_file)
        raise util.Abort("Use --createconfig to create freesite.cfg")

    parser = ConfigParser()
    parser.read(cfg_file)
    if not parser.has_section('default'):
        raise util.Abort("Can't read default section of config file?")

    params['SITE_NAME'] = parser.get('default', 'site_name')
    params['SITE_DIR'] = parser.get('default', 'site_dir')
    if parser.has_option('default','default_file'):
        params['SITE_DEFAULT_FILE'] = parser.get('default', 'default_file')
    else:
        params['SITE_DEFAULT_FILE'] = 'index.html'

    if params.get('SITE_KEY'):
        return # key set on command line

    if not parser.has_option('default','site_key_file'):
        params['SITE_KEY'] = ''
        return # Will use the insert SSK for the repo.

    key_file = parser.get('default', 'site_key_file', 'default')
    if key_file == 'default':
        ui_.status('Using repo insert key as site key.\n')
        params['SITE_KEY'] = 'default'
        return # Use the insert SSK for the repo.
    try:
        # Read private key from specified key file relative
        # to the directory the .infocalypse config file is stored in.
        key_file = os.path.join(os.path.dirname(stored_cfg.file_name),
                                key_file)
        ui_.status('Reading site key from:\n%s\n' % key_file)
        params['SITE_KEY'] = open(key_file, 'rb').read().strip()
    except IOError:
        raise util.Abort("Couldn't read site key from: %s" % key_file)

    if not params['SITE_KEY'].startswith('SSK@'):
        raise util.Abort("Stored site key not an SSK?")

def get_insert_uri(params):
    """ Helper function builds the insert URI. """
    if params['SITE_KEY'] == 'CHK@':
        return 'CHK@/'
    return '%s/%s-%i/' % (params['SITE_KEY'],
                          params['SITE_NAME'], params['SITE_INDEX'])

# Convert SSK to USK so n00b5 don't phr34k out.
def show_request_uri(ui_, params, uri):
    """ Helper function to print the request URI."""
    if uri.startswith('SSK@'):
        request_uri = 'U%s/%s/%i/' % (uri.split('/')[0][1:],
                                      params['SITE_NAME'],
                                      params['SITE_INDEX'])
    else:
        request_uri = uri
    ui_.status('RequestURI:\n%s\n' % request_uri)

def execute_putsite(ui_, repo, params):
    """ Run the putsite command. """
    def progress(dummy, msg):
        """ Message callback which writes to the hg ui instance."""

        if msg[0] == 'SimpleProgress':
            ui_.status("Progress: (%s/%s/%s)\n" % (msg[1]['Succeeded'],
                                                   msg[1]['Required'],
                                                   msg[1]['Total']))
        else:
            ui_.status("Progress: %s\n" % msg[0])


    if params.get('SITE_CREATE_CONFIG', False):
        write_default_config(ui_, repo)
        return

    # Remove trailing /
    params['SITE_KEY'] = params['SITE_KEY'].split('/')[0].strip()
    insert_uri = get_insert_uri(params)
    site_root = os.path.join(repo.root, params['SITE_DIR'])

    ui_.status('Default file: %s\n' % params['SITE_DEFAULT_FILE'])
    ui_.status('Reading files from:\n%s\n' % site_root)

    infos = get_file_infos(site_root)

    try:
        set_index_file(infos, params['SITE_DEFAULT_FILE'])
    except ValueError:
        raise util.Abort("Couldn't read %s" % params['SITE_DEFAULT_FILE'])

    ui_.status('--- files ---\n')

    for info in infos:
        ui_.status('%s %s\n' % (info[0], info[1]))
    ui_.status('---\n')

    if params['DRYRUN']:
        ui_.status('Would have inserted to:\n%s\n' % insert_uri)
        ui_.status('But --dryrun was set.')
        return

    client = FCPClient.connect(params['FCP_HOST'],
                               params['FCP_PORT'])
    client.in_params.default_fcp_params['DontCompress'] = False
    client.message_callback = progress
    try:
        ui_.status('Inserting to:\n%s\n' % insert_uri)
        try:
            request_uri = client.put_complex_dir(insert_uri, infos)[1]['URI']
            show_request_uri(ui_, params, request_uri)
        except FCPError, err:
            if err.is_code(9): # magick number for collision
                ui_.warn('An update was already inserted on that index.\n'
                         + 'Set a later index with --index and try again.\n')
                raise util.Abort("Key collision.")
            else:
                ui_.warn(str(err) + '\n')
                raise util.Abort("FCP Error")
    finally:
        client.close()

MSG_FMT = """InsertURI:
%s
RequestURI:
%s

This is what you need to put in a site_key_file file:
%s
"""

def execute_genkey(ui_, params):
    """ Run the genkey command. """
    client = FCPClient.connect(params['FCP_HOST'],
                               params['FCP_PORT'])

    client.message_callback = lambda x, y : None # silence.
    resp = client.generate_ssk()
    ui_.status(MSG_FMT % (resp[1]['InsertURI'], resp[1]['RequestURI'],
                          resp[1]['InsertURI'].split('/')[0] +'/'))
