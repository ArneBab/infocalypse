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
import shutil

from mercurial import util

from fcpconnection import FCPError
from fcpclient import FCPClient, get_file_infos, set_index_file

# HACK
from pathhacks import add_parallel_sys_path
add_parallel_sys_path('fniki')

import piki

# REDFLAG: DCI deal with loading hacks for config
from config import write_default_config

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

def dump_wiki_html(wiki_root, staging_dir):
    """ Dump the wiki as flat directory of html.

        wiki_root is the directory containing the wikitext and www dirs.
        staging_dir MUST contain the substring 'deletable'.
    """

    # i.e. so you can't delete your home directory by mistake.
    if not staging_dir.find("deletable"):
        raise ValueError("staging dir name must contain 'deletable'")

    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)
    assert not os.path.exists(staging_dir)

    os.makedirs(staging_dir)

    # REDFLAG: DCI, should be piki.
    piki.dump(staging_dir, wiki_root)

TMP_DUMP_DIR = '_tmp_wiki_html_deletable'
# Hmmmm... broken out to appease pylint
def do_freenet_insert(ui_, repo, params, insert_uri, progress_func):
    """ INTERNAL: Helper does the actual insert. """
    default_mime_type = "text/plain" # put_complex_dir() default. Hmmmm.
    if not params['ISWIKI']:
        site_root = os.path.join(repo.root, params['SITE_DIR'])
    else:
        # REDFLAG: DCI temp file cleanup on exception

        # Because wiki html files have no extension to guess from.
        default_mime_type = 'text/html'

        ui_.status("Dumping wiki as HTML...\n")
        site_root = os.path.join(params['TMP_DIR'], TMP_DUMP_DIR)
        dump_wiki_html(os.path.join(repo.root, params['WIKI_ROOT']),
                       site_root)

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
        ui_.status('But --dryrun was set.\n')
        return

    client = FCPClient.connect(params['FCP_HOST'],
                               params['FCP_PORT'])
    client.in_params.default_fcp_params['DontCompress'] = False
    client.message_callback = progress_func
    try:
        ui_.status('Inserting to:\n%s\n' % insert_uri)
        try:
            request_uri = client.put_complex_dir(insert_uri, infos,
                                                 default_mime_type)[1]['URI']
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
    try:
        do_freenet_insert(ui_, repo, params,
                          get_insert_uri(params),
                          progress)
    finally:
        tmp_dump = os.path.join(params['TMP_DIR'], TMP_DUMP_DIR)
        if os.path.exists(tmp_dump):
            # REDFLAG: DCI, failure here is horrible.
            # i.e. untrusted unencrypted data on your disk
            shutil.rmtree(tmp_dump)

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
