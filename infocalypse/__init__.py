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

To use the (optional) fn-fmsread and fn-fmsnotify commands
you must be able to connect to a running FMS
server.

For more information on FMS see:
USK@0npnMrqZNKRCRoGojZV93UNHCMN-6UU3rRSAmP6jNLE,
   ~BG-edFtdCC1cSH4O3BWdeIYa8Sw5DfyrSV-TKdO5ec,AQACAAE/fms/98/

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

USAGE EXAMPLES:

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


RE-REINSERTING AND "SPONSORING" REPOS:

hg fn-reinsert

will re-insert the bundles for the repository
that was last pulled into the directory.

The exact behavior is determined by the
level argument.

level:
1 - re-inserts the top key(s)
2 - re-inserts the top keys(s), graphs(s) and
    the most recent update.
3 - re-inserts the top keys(s), graphs(s) and
    all keys required to bootstrap the repo.
    This is the default level.
4 - adds redundancy for big (>7Mb) updates.
5 - re-inserts existing redundant big updates.

Levels 1 and 4 require that you have the private
key for the repository. For other levels, the
top key insert is skipped if you don't have
the private key.

WARNING:
DO NOT use fn-reinsert if you're concerned about
correlation attacks. The risk is on the order
of re-inserting a freesite, but may be
worse if you use redundant
(i.e. USK@<line noise>/name.R1/0) top keys.

REPOSITORY UPDATE NOTIFICATIONS VIA FMS:
hg fn-fmsread

with no arguments reads the latest repo USK indexes from
fms and updates the locally cached values.

There's a trust map in the config file which
determines which fms ids can update which repositories.

The format is:
<number> = <fms_id>|<usk_hash0>|<usk_hash1>| ... |<usk_hashn>

You can get the repository hash for a repo by running
fn-info in the directory where you have fn-pull'ed it.

You MUST manually update the trust map to enable
index updating for repos other than the one
this code lives in (be68e8feccdd).

# Example .infocalypse snippet
[fmsread_trust_map]
1 = test0@adnT6a9yUSEWe5p8J-O1i8rJCDPqccY~dVvAmtMuC9Q|55833b3e6419
0 = djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks|be68e8feccdd|5582404a9124
2 = test1@SH1BCHw-47oD9~B56SkijxfE35M9XUvqXLX1aYyZNyA|fab7c8bd2fc3

hg fn-fmsread --list

Displays announced repositories from fms ids that appear in
the trust map.

hg fn-fmsread --listall

Displays all announced repositories including ones from unknown
fms ids.

hg fn-fmsnotify

Posts update notifications to fms.

You MUST set the fms_id value in the config file
to your fms id for this to work. You only need the
part before the '@'.

# Example .infocalypse snippet
fms_id = djk

Use --dryrun to double check before sending the actual
fms message.

Use --announce at least once if you want your USK to
show up in the fmsread --listall list.

By default notifications are written to and read
from the infocalypse.notify fms group.

The read and write groups can be changed by editing
the following variables in the config file:

fmsnotify_group = <group>
fmsread_groups = <group0>[|<group1>|...]

The fms_host and fms_port variables allow you
to specify the fms host and port if you run
fms on a non-standard host/port.

fms can have pretty high latency. Be patient. It may
take hours (sometimes a day!) for your notification
to appear.  Don't send lots of redundant notifications.

HINTS:
The -q, -v and --debug verbosity options are
supported.

Top level URIs ending in '.R1' are inserted redundantly.
Don't use this if you are worried about correlation
attacks.

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

from binascii import hexlify
from mercurial import commands, util

from infcmds import get_config_info, execute_create, execute_pull, \
     execute_push, execute_setup, execute_copy, execute_reinsert, \
     execute_info

from fmscmds import execute_fmsread, execute_fmsnotify

def set_target_version(ui_, repo, opts, params, msg_fmt):
    """ INTERNAL: Update TARGET_VERSION in params. """

    revs = opts.get('rev') or None
    if not revs is None:
        for rev in revs:
            repo.changectx(rev) # Fail if we don't have the rev.

        params['TO_VERSIONS'] = tuple(revs)
        ui_.status(msg_fmt % ' '.join([ver[:12] for ver in revs]))
    else:
        # REDFLAG: get rid of default versions arguments?
        params['TO_VERSIONS'] = tuple([hexlify(head) for head in repo.heads()])
        #print "set_target_version -- using all head"
        #print params['TO_VERSIONS']

def infocalypse_create(ui_, repo, **opts):
    """ Create a new Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)

    insert_uri = opts['uri']
    if insert_uri == '':
        # REDFLAG: fix parameter definition so that it is required?
        ui_.warn("Please set the insert URI with --uri.\n")
        return

    set_target_version(ui_, repo, opts, params,
                       "Only inserting to version(s): %s\n")
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

    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Do a fn-pull from a repository USK and try again.\n")
            return

    level = opts['level']
    if level < 1 or level > 5:
        ui_.warn("level must be 1,2,3,4 or 5.\n")
        return

    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri:
        if level == 1 or level == 4:
            ui_.warn(("You can't re-insert at level %i without the "
                     + "insert URI.\n") % level)
            return

        ui_.status("No insert URI. Will skip re-insert "
                   +"of top key.\n")
        insert_uri = None

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    params['REINSERT_LEVEL'] = level
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
                       "Only pushing to version(s): %s\n")
    params['INSERT_URI'] = insert_uri
    #if opts['requesturi'] != '':
    #    # DOESN'T search the insert uri index.
    #    ui_.status(("Copying from:\n%s\nTo:\n%s\n\nThis is an "
    #                + "advanced feature. "
    #                + "I hope you know what you're doing.\n") %
    #               (opts['requesturi'], insert_uri))
    #    params['REQUEST_URI'] = opts['requesturi']

    execute_push(ui_, repo, params, stored_cfg)

def infocalypse_info(ui_, repo, **opts):
    """ Display information about an Infocalypse repository.
     """
    # FCP not required. Hmmm... Hack
    opts['fcphost'] = ''
    opts['fcpport'] = 0
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    params['REQUEST_URI'] = request_uri
    execute_info(ui_, params, stored_cfg)

def infocalypse_fmsread(ui_, repo, **opts):
    """ Read repository update information from fms.
    """
    # FCP not required. Hmmm... Hack
    opts['fcphost'] = ''
    opts['fcpport'] = 0
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.status("There is no stored request URI for this repo.\n")
            request_uri = None

    if opts['listall']:
        params['FMSREAD'] = 'listall'
    elif opts['list']:
        params['FMSREAD'] = 'list'
    else:
        params['FMSREAD'] = 'update'
    params['DRYRUN'] = opts['dryrun']
    params['REQUEST_URI'] = request_uri
    execute_fmsread(ui_, params, stored_cfg)

def infocalypse_fmsnotify(ui_, repo, **opts):
    """ Post a msg with the current repository USK index to fms.
    """
    params, stored_cfg = get_config_info(ui_, opts)
    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri:
        ui_.warn("You can't notify because there's no stored "
                 + "insert URI for this repo.\n"
                 + "Run from the directory you inserted from.\n")
        return

    params['ANNOUNCE'] = opts['announce']
    params['DRYRUN'] = opts['dryrun']
    params['INSERT_URI'] = insert_uri
    execute_fmsnotify(ui_, repo, params, stored_cfg)

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
NOSEARCH_OPT = [('', 'nosearch', None, 'use USK version in URI'), ]
# Allow mercurial naming convention for command table.
# pylint: disable-msg=C0103

cmdtable = {
    "fn-pull": (infocalypse_pull,
                [('', 'uri', '', 'request URI to pull from'),]
                + FCP_OPTS
                + NOSEARCH_OPT
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
                + FCP_OPTS
                + NOSEARCH_OPT,
                "[options]"),

    "fn-reinsert": (infocalypse_reinsert,
                    [('', 'uri', '', 'request URI'),
                     ('', 'level', 3, 'how much to re-insert')]
                    + FCP_OPTS
                    + NOSEARCH_OPT,
                    "[options]"),

    "fn-info": (infocalypse_info,
                 [('', 'uri', '', 'request URI'),],
                "[options]"),


    "fn-fmsread": (infocalypse_fmsread,
                   [('', 'uri', '', 'request URI'),
                    ('', 'list', None, 'show repo USKs from trusted '
                     + 'fms identities'),
                    ('', 'listall', None, 'show all repo USKs'),
                    ('', 'dryrun', None, "don't update the index cache"),],
                   "[options]"),

    "fn-fmsnotify": (infocalypse_fmsnotify,
                     [('', 'dryrun', None, "don't send fms message"),
                     ('', 'announce', None, "include full URI update"), ]
                     + FCP_OPTS, # Needs to invert the insert uri
                     "[options]"),

    "fn-setup": (infocalypse_setup,
                 [('', 'tmpdir', '~/infocalypse_tmp', 'temp directory'),]
                 + FCP_OPTS,
                "[options]"),
    }


commands.norepo += ' fn-setup'

