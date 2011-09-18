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

To use the (optional, but highly recommended) fn-fmsread
and fn-fmsnotify commands you must be able to connect to
a running FMS server.

For more information on FMS see:
USK@0npnMrqZNKRCRoGojZV93UNHCMN-6UU3rRSAmP6jNLE,
   ~BG-edFtdCC1cSH4O3BWdeIYa8Sw5DfyrSV-TKdO5ec,AQACAAE/fms/128/

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
FMS and updates the locally cached values.

There's a trust map in the config file which
determines which FMS ids can update the index values
for which repositories. It is purely local and completely
separate from the trust values which appear in the
FMS web of trust.

The trust map is stored in the '[fmsread_trust_map]' section
of the config file.

The format is:
<number> = <fms_id>|<usk_hash0>|<usk_hash1>| ... |<usk_hashn>

The number value must be unique, but is ignored.

You can get the repository hash for a repo by running
fn-info in the directory where you have fn-pull'ed it
or with fn-fmsread --list[all] if it has been announced.

Here's an example trust map config entry:
# Example .infocalypse snippet
[fmsread_trust_map]
1 = test0@adnT6a9yUSEWe5p8J-O1i8rJCDPqccY~dVvAmtMuC9Q|55833b3e6419
0 = djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks|be68e8feccdd|5582404a9124
2 = test1@SH1BCHw-47oD9~B56SkijxfE35M9XUvqXLX1aYyZNyA|fab7c8bd2fc3

You MUST update the trust map to enable index updating for
repos other than the one this code lives in (c856b1653f0b).
You can edit the config file directly if you want.

However, the easiest way to update the trust map is by using the
--trust and --untrust options on fn-fmsread.

For example to trust falafel@IxVqeqM0LyYdTmYAf5z49SJZUxr7NtQkOqVYG0hvITw
to notify you about changes to the repository with repo hash 2220b02cf7ee,
type:

hg fn-fmsread --trust --hash 2220b02cf7ee \
   --fmsid falafel@IxVqeqM0LyYdTmYAf5z49SJZUxr7NtQkOqVYG0hvITw

And to stop trusting that FMS id for updates to 2220b02cf7ee, you would
type:

hg fn-fmsread --untrust --hash 2220b02cf7ee \
   --fmsid falafel@IxVqeqM0LyYdTmYAf5z49SJZUxr7NtQkOqVYG0hvITw

To show the trust map type:

hg fn-fmsread --showtrust

The command:

hg fn-fmsread --list

displays announced repositories from FMS ids that appear anywhere in
the trust map.

hg fn-fmsread --listall

Displays all announced repositories including ones from unknown
FMS ids.

You can use the --hash option with fn-pull to pull any repository
you see in the fn-read --list[all] lists by specifying the
repository hash.

e.g. to pull this code, cd to an empty directory and type:

hg init
hg fn-pull --hash c856b1653f0b --aggressive

The command:

hg fn-fmsnotify

posts update notifications to FMS.

You MUST set the fms_id value in the config file
to your FMS id for this to work. You only need the
part before the '@'.

# Example .infocalypse snippet
fms_id = djk

Use --dryrun to double check before sending the actual
FMS message.

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

FMS can have pretty high latency. Be patient. It may
take hours (sometimes a day!) for your notification
to appear.  Don't send lots of redundant notifications.

FREESITE INSERTION:
hg fn-putsite --index <n>

inserts a freesite based on the configuration in
the freesite.cfg file in the root of the repository.

Use:
hg fn-putsite --createconfig

to create a basic freesite.cfg file that you
can modify. Look at the comments in it for an
explanation of the supported parameters.

The default freesite.cfg file inserts using the
same private key as the repo and a site name
of 'default'. Editing the name is highly
recommended.

You can use --key CHK@ to insert a test version of
the site to a CHK key before writing to the USK.

Limitations:
o You MUST have fn-pushed the repo at least once
  in order to insert using the repo's private key.
  If you haven't fn-push'd you'll see this error:
  "You don't have the insert URI for this repo.
  Supply a private key with --key or fn-push the repo."
o Inserts *all* files in the site_dir directory in
  the freesite.cfg file.  Run with --dryrun to make
  sure that you aren't going to insert stuff you don't
  want too.
o You must manually specify the USK edition you want
  to insert on.  You will get a collision error
  if you specify an index that was already inserted.
o Don't use this for big sites.  It should be fine
  for notes on your project.  If you have lots of images
  or big binary files use a tool like jSite instead.
o Don't modify site files while the fn-putsite is
  running.

HINTS:
The -q, -v and --debug verbosity options are
supported.

Top level URIs ending in '.R1' are inserted redundantly.
Don't use this if you're worried about correlation
attacks.

If you see 'abort: Connection refused' when you run
fn-fmsread or fn-fmsnotify, check fms_host and
fms_port in the config file.

MORE DOCUMENTATION:
See doc/infocalypse_howto.html in the directory this
extension was installed into.

SOURCE CODE:
The authoritative repository for this code is hosted in Freenet.

hg fn-fmsread -v
hg fn-pull --debug --aggressive \\
--uri USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,\\
      AQACAAE/wiki_hacking.R1/20

NOTE: This repository has some other unmaintained and abandoned stuff in it.
      e.g. the pre jfniki python server based wiki code, python incremental archive stuff.


CONTACT:
djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks
Post to freenet group on FMS.

d kar bott at com cast dot net

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

from fmscmds import execute_fmsread, execute_fmsnotify, get_uri_from_hash, \
     execute_setupfms

from sitecmds import execute_putsite, execute_genkey
from wikicmds import execute_wiki, execute_wiki_apply
from arccmds import execute_arc_create, execute_arc_pull, execute_arc_push, \
     execute_arc_reinsert

from config import read_freesite_cfg
from validate import is_hex_string, is_fms_id

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

    if opts['hash']:
        # Use FMS to lookup the uri from the repo hash.
        if opts['uri'] != '':
            ui_.warn("Ignoring --uri because --hash is set!\n")
        if len(opts['hash']) != 1:
            raise util.Abort("Only one --hash value is allowed.")
        params['FMSREAD_HASH'] = opts['hash'][0]
        params['FMSREAD_ONLYTRUSTED'] = bool(opts['onlytrusted'])
        request_uri = get_uri_from_hash(ui_, repo, params, stored_cfg)
    else:
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
    execute_info(ui_, repo, params, stored_cfg)

def parse_trust_args(params, opts):
    """ INTERNAL: Helper function to parse  --hash and --fmsid. """
    if opts.get('hash', []) == []:
        raise util.Abort("Use --hash to set the USK hash.")
    if len(opts['hash']) != 1:
        raise util.Abort("Only one --hash value is allowed.")
    if not is_hex_string(opts['hash'][0]):
        raise util.Abort("[%s] doesn't look like a USK hash." %
                         opts['hash'][0])

    if opts.get('fmsid', []) == []:
        raise util.Abort("Use --fmsid to set the FMS id.")
    if len(opts['fmsid']) != 1:
        raise util.Abort("Only one --fmsid value is allowed.")
    if not is_fms_id(opts['fmsid'][0]):
        raise util.Abort("[%s] doesn't look like an FMS id."
                         % opts['fmsid'][0])

    params['FMSREAD_HASH'] = opts['hash'][0]
    params['FMSREAD_FMSID'] = opts['fmsid'][0]

def parse_fmsread_subcmd(params, opts):
    """ INTERNAL: Parse subcommand for fmsread."""
    if opts['listall']:
        params['FMSREAD'] = 'listall'
    elif opts['list']:
        params['FMSREAD'] = 'list'
    elif opts['showtrust']:
        params['FMSREAD'] = 'showtrust'
    elif opts['trust']:
        params['FMSREAD'] = 'trust'
        parse_trust_args(params, opts)
    elif opts['untrust']:
        params['FMSREAD'] = 'untrust'
        parse_trust_args(params, opts)
    else:
        params['FMSREAD'] = 'update'

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
    parse_fmsread_subcmd(params, opts)
    params['DRYRUN'] = opts['dryrun']
    params['REQUEST_URI'] = request_uri
    execute_fmsread(ui_, params, stored_cfg)

def infocalypse_fmsnotify(ui_, repo, **opts):
    """ Post a msg with the current repository USK index to fms.
    """
    params, stored_cfg = get_config_info(ui_, opts)
    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri and not (opts['submitbundle'] or
                               opts['submitwiki']):
        ui_.warn("You can't notify because there's no stored "
                 + "insert URI for this repo.\n"
                 + "Run from the directory you inserted from.\n")
        return

    params['ANNOUNCE'] = opts['announce']
    params['SUBMIT_BUNDLE'] = opts['submitbundle']
    params['SUBMIT_WIKI'] = opts['submitwiki']
    if params['SUBMIT_WIKI'] or params['SUBMIT_BUNDLE']:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n")
            raise util.Abort("No request URI.")
        params['REQUEST_URI'] = request_uri

    params['DRYRUN'] = opts['dryrun']
    params['INSERT_URI'] = insert_uri
    execute_fmsnotify(ui_, repo, params, stored_cfg)

MSG_BAD_INDEX = 'You must set --index to a value >= 0.'
def infocalypse_putsite(ui_, repo, **opts):
    """ Insert an update to a freesite.
    """

    if opts['createconfig']:
        if opts['wiki']:
            raise util.Abort("Use fn-wiki --createconfig.")
        params = {'SITE_CREATE_CONFIG':True}
        execute_putsite(ui_, repo, params)
        return

    params, stored_cfg = get_config_info(ui_, opts)
    if opts['key'] != '': # order important
        params['SITE_KEY'] = opts['key']
        if not (params['SITE_KEY'].startswith('SSK') or
                params['SITE_KEY'] == 'CHK@'):
            raise util.Abort("--key must be a valid SSK "
                             + "insert key or CHK@.")

    params['ISWIKI'] = opts['wiki']
    read_freesite_cfg(ui_, repo, params, stored_cfg)

    try:
        # --index not required for CHK@
        if not params['SITE_KEY'].startswith('CHK'):
            params['SITE_INDEX'] = int(opts['index'])
            if params['SITE_INDEX'] < 0:
                raise ValueError()
        else:
            params['SITE_INDEX'] = -1
    except ValueError:
        raise util.Abort(MSG_BAD_INDEX)
    except TypeError:
        raise util.Abort(MSG_BAD_INDEX)

    params['DRYRUN'] = opts['dryrun']

    if not params.get('SITE_KEY', None):
        insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
        if not insert_uri:
            ui_.warn("You don't have the insert URI for this repo.\n"
                     + "Supply a private key with --key or fn-push "
                     + "the repo.\n")
            return # REDFLAG: hmmm... abort?
        params['SITE_KEY'] = 'SSK' + insert_uri.split('/')[0][3:]

    execute_putsite(ui_, repo, params)

def infocalypse_wiki(ui_, repo, **opts):
    """ View and edit the current repository as a wiki. """
    if os.getcwd() != repo.root:
        raise util.Abort("You must be in the repository root directory.")

    subcmds = ('run', 'createconfig', 'apply')
    required = sum([bool(opts[cmd]) for cmd in subcmds])
    if required == 0:
        raise util.Abort("You must specify either --run, " +
                         "--createconfig, --apply")
    if required > 1:
        raise util.Abort("Use either --run, --createconfig, or --apply")

    if opts['apply'] != '':
        params, stored_cfg = get_config_info(ui_, opts)
        params['REQUEST_URI'] = opts['apply']
        execute_wiki_apply(ui_, repo, params, stored_cfg)
        return

    if opts['fcphost'] != '' or opts['fcpport'] != 0:
        raise util.Abort("--fcphost, --fcpport only for --apply")

    # hmmmm.... useless copy?
    params = {'WIKI' : [cmd for cmd in subcmds if opts[cmd]][0],
              'HTTP_PORT': opts['http_port'],
              'HTTP_BIND': opts['http_bind']}
    execute_wiki(ui_, repo, params)

def infocalypse_genkey(ui_, **opts):
    """ Print a new SSK key pair. """
    params, dummy = get_config_info(ui_, opts)
    execute_genkey(ui_, params)

def infocalypse_setup(ui_, **opts):
    """ Setup the extension for use for the first time. """

    execute_setup(ui_,
                  opts['fcphost'],
                  opts['fcpport'],
                  opts['tmpdir'])

    if not opts['nofms']:
        execute_setupfms(ui_, opts)
    else:
        ui_.status("Skipped FMS configuration because --nofms was set.\n")

def infocalypse_setupfms(ui_, **opts):
    """ Setup or modify the fms configuration. """
    # REQUIRES config file.
    execute_setupfms(ui_, opts)

#----------------------------------------------------------"
def do_archive_create(ui_, opts, params, stored_cfg):
    """ fn-archive --create."""
    insert_uri = opts['uri']
    if insert_uri == '':
        raise util.Abort("Please set the insert URI with --uri.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()
    execute_arc_create(ui_, params, stored_cfg)

def do_archive_push(ui_, opts, params, stored_cfg):
    """ fn-archive --push."""
    insert_uri = opts['uri']
    if insert_uri == '':
        insert_uri = (
            stored_cfg.get_dir_insert_uri(params['ARCHIVE_CACHE_DIR']))
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise util.Abort("No Insert URI.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()

    execute_arc_push(ui_, params, stored_cfg)

def do_archive_pull(ui_, opts, params, stored_cfg):
    """ fn-archive --pull."""
    request_uri = opts['uri']

    if request_uri == '':
        request_uri = (
            stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR']))
        if not request_uri:
            ui_.warn("There is no stored request URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise util.Abort("No request URI.")

    params['REQUEST_URI'] = request_uri
    params['TO_DIR'] = os.getcwd()
    execute_arc_pull(ui_,  params, stored_cfg)

ILLEGAL_FOR_REINSERT = ('uri', 'aggressive', 'nosearch')
def do_archive_reinsert(ui_, opts, params, stored_cfg):
    """ fn-archive --reinsert."""
    illegal = [value for value in ILLEGAL_FOR_REINSERT
               if value in opts and opts[value]]
    if illegal:
        raise util.Abort("--uri, --aggressive, --nosearch illegal " +
                         "for reinsert.")
    request_uri = stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR'])
    if request_uri is None:
        ui_.warn("There is no stored request URI for this archive.\n" +
                 "Run fn-archive --pull first!.\n")
        raise util.Abort(" No request URI, can't re-insert")

    insert_uri = stored_cfg.get_dir_insert_uri(params['ARCHIVE_CACHE_DIR'])
    params['REQUEST_URI'] = request_uri
    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd() # hmmm not used.
    params['REINSERT_LEVEL'] = 3
    execute_arc_reinsert(ui_, params, stored_cfg)

ARCHIVE_SUBCMDS = {'create':do_archive_create,
                   'push':do_archive_push,
                   'pull':do_archive_pull,
                   'reinsert':do_archive_reinsert}
ARCHIVE_CACHE_DIR = '.ARCHIVE_CACHE'
def infocalypse_archive(ui_, **opts):
    """ Commands to maintain a non-hg incremental archive."""
    subcmd = [value for value in ARCHIVE_SUBCMDS if opts[value]]
    if len(subcmd) > 1:
        raise util.Abort("--create, --pull, --push are mutally exclusive. " +
                         "Only specify one.")
    if len(subcmd) > 0:
        subcmd = subcmd[0]
    else:
        subcmd = "pull"

    params, stored_cfg = get_config_info(ui_, opts)
    params['ARCHIVE_CACHE_DIR'] = os.path.join(os.getcwd(), ARCHIVE_CACHE_DIR)

    if not subcmd in ARCHIVE_SUBCMDS:
        raise util.Abort("Unhandled subcommand: " + subcmd)

    # 2 qt?
    ARCHIVE_SUBCMDS[subcmd](ui_, opts, params, stored_cfg)

#----------------------------------------------------------"

# Can't use None as a default? Means "takes no argument'?
FCP_OPTS = [('', 'fcphost', '', 'fcp host'),
            ('', 'fcpport', 0, 'fcp port'),
]

FMS_OPTS = [('', 'fmshost', '', 'fms host'),
            ('', 'fmsport', 0, 'fms port'),
]


AGGRESSIVE_OPT = [('', 'aggressive', None, 'aggressively search for the '
                   + 'latest USK index'),]
NOSEARCH_OPT = [('', 'nosearch', None, 'use USK version in URI'), ]
# Allow mercurial naming convention for command table.
# pylint: disable-msg=C0103

cmdtable = {
    "fn-pull": (infocalypse_pull,
                [('', 'uri', '', 'request URI to pull from'),
                 ('', 'hash', [], 'repo hash of repository to pull from'),
                 ('', 'onlytrusted', None, 'only use repo announcements from '
                  + 'known users')]
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
                    ('', 'hash', [], 'repo hash to modify trust for'),
                    ('', 'fmsid', [], 'FMS id to modify trust for'),
                    ('', 'list', None, 'show repo USKs from trusted '
                     + 'fms identities'),
                    ('', 'listall', None, 'show all repo USKs'),
                    ('', 'showtrust', None, 'show the trust map'),
                    ('', 'trust', None, 'add an entry to the trust map'),
                    ('', 'untrust', None, 'remove an entry from the trust map'),
                    ('', 'dryrun', None, "don't update the index cache"),],
                   "[options]"),

    "fn-fmsnotify": (infocalypse_fmsnotify,
                     [('', 'dryrun', None, "don't send fms message"),
                     ('', 'announce', None, "include full URI update"),
                     ('', 'submitbundle', None, "insert patch bundle and " +
                      "send an fms notification"),
                      ('', 'submitwiki', None, "insert overlayed wiki " +
                       "changes and send an fms notification"),]
                     + FCP_OPTS, # Needs to invert the insert uri
                     "[options]"),

    "fn-putsite": (infocalypse_putsite,
                     [('', 'dryrun', None, "don't insert site"),
                     ('', 'index', -1, "edition to insert"),
                     ('', 'createconfig', None, "create default freesite.cfg"),
                      ('', 'wiki', None, "insert a wiki, requires fnwiki.cfg"),
                     ('', 'key', '', "private SSK to insert under"),]
                     + FCP_OPTS,
                     "[options]"),

    "fn-wiki": (infocalypse_wiki,
                [('', 'run', None, "start a local http server " +
                  "displaying a wiki"),
                 ('', 'createconfig', None, "create default fnwiki.cfg " +
                  "and skeleton wiki_root dir"),
                 ('', 'http_port', 8081, "port for http server"),
                 ('', 'http_bind', 'localhost', "interface http " +
                  "listens on, '' to listen on all"),
                 ('', 'apply', '', "apply changes to the wiki from the " +
                  "supplied Request URI ")] +
                FCP_OPTS,
                "[options]"),

    "fn-genkey": (infocalypse_genkey,
                  FCP_OPTS,
                  "[options]"),

    "fn-setup": (infocalypse_setup,
                 [('', 'tmpdir', '~/infocalypse_tmp', 'temp directory'),
                  ('', 'nofms', None, 'skip FMS configuration'),
                  ('', 'fmsid', '', "fmsid (only part before '@'!)"),
                  ('', 'timeout', 30, "fms socket timeout in seconds")]
                 + FCP_OPTS
                 + FMS_OPTS,
                "[options]"),

    "fn-setupfms": (infocalypse_setupfms,
                 [('', 'fmsid', '', "fmsid (only part before '@'!)"),
                  ('', 'timeout', 30, "fms socket timeout in seconds"),]
                 + FMS_OPTS,
                "[options]"),

    "fn-archive": (infocalypse_archive,
                  [('', 'uri', '', 'Request URI for --pull, Insert URI ' +
                    'for --create, --push'),
                   ('', 'create', None, 'Create a new archive using the ' +
                    'Insert URI --uri'),
                   ('', 'push', None, 'Push incremental updates into the ' +
                    'archive in Freenet'),
                   ('', 'pull', None, 'Pull incremental updates from the ' +
                    'archive in Freenet'),
                   ('', 'reinsert', None, 'Re-insert the entire archive. '),
                   ]
                   + FCP_OPTS
                   + NOSEARCH_OPT
                   + AGGRESSIVE_OPT,
                   "[options]"),
    }


commands.norepo += ' fn-setup'
commands.norepo += ' fn-setupfms'
commands.norepo += ' fn-genkey'
commands.norepo += ' fn-archive'
