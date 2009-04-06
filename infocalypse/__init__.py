"""Redundant incrementally updateable repositories in Freenet.

Copyright (C) 2009 Darrell Karbott
License: GPL 2 (or later)

This extension provides commands to create and maintain
incrementally updateable mercurial repositories in
Freenet.

REQURIEMENTS:
You MUST be able to connect to a running Freenet node
in order to use this extension.

For more information on Freenet see:
http://freenetproject.org/

ADDING THE EXTENSION:
Add the following to your .hgrc/mercurial.ini file.

   # .hgrc snippet
   [extensions]
   infocalypse = /path/to/infocalypse_dir

Where infocalypse_dir is the directory containing
this file.

SETUP:
Run hg fn-setup once to create the extension's config file.

By default, it will write the configuration to:
~/.infocalypse on *nix and ~/infocalypse.ini on
Windows.

If you want to put the config file in a different
location set the cfg_file option in the
[infocalypse] section of your
.hgrc/mercurial.ini file *before* running setup.

Example .hgrc entry:

# Snip, from .hgrc
[infocalypse]
cfg_file = /mnt/usbkey/s3kr1t/infocalypse.cfg

The default temp file dirctory is set to:
~/infocalypse_tmp. It will be created if
it doesn't exist.

Set the --tmpdir option to use a different
value.

The config file contains your default
private key and cached information about
what repositories you insert/retrieve.

It's a good idea to keep it on
a removable drive for maximum security.

EXAMPLES:

hg fn-create --uri USK@/test.R1/0

Inserts the local hg repository into a new
USK in Freenet, using the private key in your
config file.  You can use a full insert URI
value if you want.

hg fn-push --uri USK@/test.R1/0

Pushes incremental changes from the local
directory into the existing repository.

You can ommit the --uri argument when
you run from the same directory the fn-create
was run in because the insert key -> dir
mapping is saved in the config file.

Go to a different directory do an hg init and type:

hg fn-pull --uri <request uri from steps above>

to pull from the repository in Freenet.

The request uri -> dir mapping is saved after
the first pull, so you can ommit the --uri
argument for subsequent fn-pull invocations.

hg fn-reinsert

will re-insert the bundles for the repository
that was last pulled into the directory.  If
you have the insert uri the top level key(s)
will also be re-inserted.


HINTS:
The -q, -v and --debug verbosity options are
supported.

Top level URIs ending in '.R1' are inserted redundantly.

CONTACT:
djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks
Post to freenet group on FMS.

"""

# Copyright (C) 2009 Darrell Karbott
#
# Author: djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks

# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; either
# version 2.0 of the License, or (at your option) any later version.

# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import os

from mercurial import commands, util

from infcmds import get_config_info, execute_create, execute_pull, \
     execute_push, execute_setup, execute_copy, execute_reinsert

def set_target_version(ui_, repo, opts, params, msg_fmt):
    """ INTERNAL: Update TARGET_VERSION in params. """

    revs = opts.get('rev') or None
    if not revs is None:
        if len(revs) > 1:
            raise util.Abort("Only a single -r version is supported. ")
        rev = revs[0]
        ctx = repo[rev] # Fail if we don't have the rev.
        params['TO_VERSION'] = rev
        if ctx != repo['tip']:
            ui_.status(msg_fmt % rev)

def infocalypse_create(ui_, repo, **opts):
    """ Create a new Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)

    insert_uri = opts['uri']
    if insert_uri == '':
        # REDFLAG: fix parameter definition so that it is required?
        ui_.warn("Please set the insert URI with --uri.\n")
        return

    set_target_version(ui_, repo, opts, params,
                       "Only inserting to version: %s\n")
    params['INSERT_URI'] = insert_uri
    execute_create(ui_, repo, params, stored_cfg)

def infocalypse_copy(ui_, repo, **opts):
    """ Copy an Infocalypse repository to a new URI. """
    params, stored_cfg = get_config_info(ui_, opts)

    insert_uri = opts['inserturi']
    if insert_uri == '':
        # REDFLAG: fix parameter definition so that it is required?
        ui_.warn("Please set the insert URI with --inserturi.\n")
        return

    request_uri = opts['requesturi']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --requesturi option.\n")
            return
    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    execute_copy(ui_, repo, params, stored_cfg)

def infocalypse_reinsert(ui_, repo, **opts):
    """ Reinsert the current version of an Infocalypse repository. """
    params, stored_cfg = get_config_info(ui_, opts)

    request_uri = stored_cfg.get_request_uri(repo.root)
    if not request_uri:
        ui_.warn("There is no stored request URI for this repo.\n"
                 "Do a fn-pull from a repository USK and try again.\n")
        return

    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri:
        ui_.status("No insert URI. Will skip re-insert "
                   +"of top key.\n")
        insert_uri = None

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    execute_reinsert(ui_, repo, params, stored_cfg)

def infocalypse_pull(ui_, repo, **opts):
    """ Pull from an Infocalypse repository in Freenet.
     """
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    params['REQUEST_URI'] = request_uri
    # Hmmmm... can't really implement rev.
    execute_pull(ui_, repo, params, stored_cfg)

def infocalypse_push(ui_, repo, **opts):
    """ Push to an Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)
    insert_uri = opts['uri']
    if insert_uri == '':
        insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this repo.\n"
                    "Please set one with the --uri option.\n")
            return

    set_target_version(ui_, repo, opts, params,
                       "Only pushing to version: %s\n")
    params['INSERT_URI'] = insert_uri
    #if opts['requesturi'] != '':
    #    # DOESN'T search the insert uri index.
    #    ui_.status(("Copying from:\n%s\nTo:\n%s\n\nThis is an "
    #                + "advanced feature. "
    #                + "I hope you know what you're doing.\n") %
    #               (opts['requesturi'], insert_uri))
    #    params['REQUEST_URI'] = opts['requesturi']

    execute_push(ui_, repo, params, stored_cfg)

def infocalypse_setup(ui_, **opts):
    """ Setup the extension for use for the first time. """

    execute_setup(ui_,
                  opts['fcphost'],
                  opts['fcpport'],
                  opts['tmpdir'])

# Can't use None as a default? Means "takes no argument'?
FCP_OPTS = [('', 'fcphost', '', 'fcp host'),
            ('', 'fcpport', 0, 'fcp port'),
]

AGGRESSIVE_OPT = [('', 'aggressive', None, 'aggressively search for the '
                   + 'latest USK index'),]

# Allow mercurial naming convention for command table.
# pylint: disable-msg=C0103
cmdtable = {
    "fn-pull": (infocalypse_pull,
                [('', 'uri', '', 'request URI to pull from'),]
                + FCP_OPTS
                + AGGRESSIVE_OPT,
                "[options]"),

    "fn-push": (infocalypse_push,
                [('', 'uri', '', 'insert URI to push to'),
                 # Buggy. Not well thought out.
                 #('', 'requesturi', '', 'optional request URI to copy'),
                 ('r', 'rev', [],'maximum rev to push'),]
                + FCP_OPTS
                + AGGRESSIVE_OPT,
                "[options]"),

    "fn-create": (infocalypse_create,
                  [('', 'uri', '', 'insert URI to create on'),
                   ('r', 'rev', [],'maximum rev to push'),]
                  + FCP_OPTS,
                "[options]"),
    "fn-copy": (infocalypse_copy,
                [('', 'requesturi', '', 'request URI to copy from'),
                 ('', 'inserturi', '', 'insert URI to copy to'), ]
                + FCP_OPTS,
                "[options]"),

    "fn-reinsert": (infocalypse_reinsert,
                    FCP_OPTS,
                    "[options]"),

    "fn-setup": (infocalypse_setup,
                 [('', 'tmpdir', '~/infocalypse_tmp', 'temp directory'),]
                 + FCP_OPTS,
                "[options]"),
    }


commands.norepo += ' fn-setup'

