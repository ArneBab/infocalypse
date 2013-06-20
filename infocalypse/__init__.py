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

hg fn-create --wot nickname/test.R1/0

Inserts the local hg repository into a new
USK in Freenet, using the private key in your
config file.  You can use a full insert URI
value if you want, or --wot for inserting
under a WoT ID. It takes enough of a WoT
nickname required to be unambiguous.

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

from commands import *

from binascii import hexlify
from mercurial import commands, util

#----------------------------------------------------------"

# Can't use None as a default? Means "takes no argument'?
FCP_OPTS = [('', 'fcphost', '', 'fcp host'),
            ('', 'fcpport', 0, 'fcp port'),
]

FMS_OPTS = [('', 'fmshost', '', 'fms host'),
            ('', 'fmsport', 0, 'fms port'),
]

WOT_OPTS = [('', 'truster', '', 'WoT identity to use when looking up others'),
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
                 ('', 'wot', '', 'WoT nick@key/repo to pull from'),
                 ('', 'onlytrusted', None, 'only use repo announcements from '
                  + 'known users')]
                + WOT_OPTS
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
                   ('', 'wot', '', 'WoT nickname to create on'),
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
                 + WOT_OPTS
                 + FCP_OPTS
                 + FMS_OPTS,
                "[options]"),

    "fn-setupfms": (infocalypse_setupfms,
                 [('', 'fmsid', '', "fmsid (only part before '@'!)"),
                  ('', 'timeout', 30, "fms socket timeout in seconds"),]
                 + FMS_OPTS,
                "[options]"),

    "fn-setupwot": (infocalypse_setupwot,
                    FCP_OPTS +
                    WOT_OPTS,
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
commands.norepo += ' fn-setupwot'
