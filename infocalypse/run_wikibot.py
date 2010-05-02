""" Set up and run a single wikibot instance.

    Uses *nix specific apis! Only tested on Linux.

    Copyright (C) 2009, 2010 Darrell Karbott

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

import errno
import os
import signal
import sys

from ConfigParser import ConfigParser

from fcpclient import FCPClient, get_usk_hash
from fcpconnection import FCPConnection, PolledSocket
from requestqueue import RequestRunner
from bundlecache import is_writable

from fmsstub import FMSStub

from fmsbot import FMSBotRunner, run_event_loops, make_bot_path
from wikibot import WikiBot

############################################################
# FCP info
FCP_HOST = '127.0.0.1'
FCP_PORT = 19481

# FMS info
FMS_HOST = '127.0.0.1'
FMS_PORT = 11119
# NOTE: fms id for bot is read from fnwiki.cfg.

# Latest known repo usk index
# MUST set this when starting for the first time or re-bootstrapping.
INDEX_HINT = 0

# vebosity of logging output (NOT FCP 'Verbosity')
VERBOSITY = 5

# Root directory for temporary files.
BASE_DIR = '/tmp/fnikibot'

# File containing the private SSK key.
# String is filled in with the usk hash for the wikitext repo.
#
# MUST match the public key for the wiki_repo_usk in the
# fnwiki.cfg file.
KEY_FILE_FMT = '~/wikibot_key_%s.txt'

# Set this True to post repo update notifications to
# infocalypse.notify.  You MUST set this for users to
# be able to see the bot's update notifications with
# the default configuration of fn-fmsread.
#
# BUT please don't set it True when testing, to avoid
# spewing garbage into the infocalypse.notify group.
POST_TO_INFOCALYPSE_NOTIFY = False

# Usually, you won't need to tweek parameters below this line.
#
# Additional configuration info is read from the fnwiki.cfg
# file for the wiki. See read_fniki_cfg.
#----------------------------------------------------------#

def read_fnwiki_cfg(cfg_file):
    """ Quick and dirty helper w/o hg deps. to read cfg file."""
    parser = ConfigParser()
    parser.read(cfg_file)
    if not parser.has_section('default'):
        raise IOError("Can't read default section of config file: %s"
                      % cfg_file)

    # Hmmm some param key strings are different than config.py.
    return {'WIKI_ROOT':parser.get('default', 'wiki_root'),
            'SITE_NAME':parser.get('default', 'site_name'),
            'SITE_DEFAULT_FILE':parser.get('default', 'default_file'),
            'FMS_GROUP':parser.get('default', 'wiki_group'),
            # Only the part before '@'.
            'FMS_ID':parser.get('default', 'wiki_server_id').split('@')[0],
            'WIKI_REPO_USK':parser.get('default', 'wiki_repo_usk')}

def get_dirs(base_dir, create=False):
    " Get, and optionally create the required working directories."
    ret = (os.path.join(base_dir, '__wikibot_tmp__'),
            os.path.join(base_dir, 'hgrepo'),
            os.path.join(base_dir, 'bot_storage'),
            os.path.join(os.path.join(base_dir, 'bot_storage'), # required?
                         '.hg'))

    if create:
        for value in ret:
            if os.path.exists(value):
                raise IOError("Directory already exists: %s" % value)
        print
        for value in ret:
            os.makedirs(value)
            if not is_writable(value):
                raise IOError("Couldn't write to: %s" % value)
            print "Created: %s" % value

        print
        print "You need to MANUALLY fn-pull the wikitext repo into:"
        print ret[1]

    else:
        for value in ret:
            if not is_writable(value):
                raise IOError("Directory doesn't exist or isn't writable: %s"
                              % value)
    return ret[:3]

# LATER: load from a config file
def get_params(base_dir):
    """ Return the parameters to run a WikiBot. """

    # Get working directories.
    (tmp_dir,         # MUST exist
     repo_dir,        # MUST exist and contain wikitext hg repo.
     bot_storage_dir, # MUST exist
     )  = get_dirs(base_dir)

    params = read_fnwiki_cfg(os.path.join(repo_dir, 'fnwiki.cfg'))

    # MUST contain SSK private key
    key_file = KEY_FILE_FMT % get_usk_hash(params['WIKI_REPO_USK'])
    print "Read insert key from: %s" % key_file

    # Load private key for the repo from a file..
    insert_ssk = open(os.path.expanduser(key_file), 'rb').read().strip()
    assert insert_ssk.startswith('SSK@')
    # Raw SSK insert key.
    insert_ssk = insert_ssk.split('/')[0].strip()

    # Make insert URI from request URI in config file.
    human = '/'.join(params['WIKI_REPO_USK'].split('/')[1:])
    insert_uri = 'U' + insert_ssk[1:] + '/' + human

    # Then invert the request_uri from it.
    print "Inverting public key from private one..."
    request_uri = FCPClient.connect(FCP_HOST, FCP_PORT). \
                  get_request_uri(insert_uri)
    print request_uri
    if get_usk_hash(request_uri) != get_usk_hash(params['WIKI_REPO_USK']):
        print "The insert SSK doesn't match WIKI_REPO_USK in fnwiki.cfg!"
        assert False

    # LATER: Name convention.
    # USK@/foo.wikitext.R1/0 -- wiki source
    # USK@/foo/0 -- freesite

    #print "Reading latest index from Freenet... This can take minutes."
    #index = prefetch_usk(FCPClient.connect(fcp_host, fcp_port),
    #                     request_uri)
    #insert_uri = get_usk_for_usk_version(insert_uri, index)
    #request_uri = get_usk_for_usk_version(request_uri, index) # needed?


    # Hmmmm... freesite index is read from 'I_<n>' tags in
    # repo. There is no way to set it.
    params.update({
        # FCP 2.0
        'MaxRetries':3,
        'PriorityClass':1,
        #'DontCompress':True,
        'Verbosity':1023, # MUST set this to get progress messages.

        # FCPConnection / RequestRunner
        'FCP_HOST':FCP_HOST,
        'FCP_PORT':FCP_PORT,
        'FCP_POLL_SECS':0.25,
        'N_CONCURRENT':4,
        'CANCEL_TIME_SECS': 15 * 60,

        # FMSBotRunner
        'FMS_HOST':FMS_HOST,
        'FMS_PORT':FMS_PORT,
        'FMS_POLL_SECS': 3 * 60,
        'BOT_STORAGE_DIR':bot_storage_dir,

        # WikiBot
        'FMS_NOTIFY_GROUP': ('infocalypse.notify' if POST_TO_INFOCALYPSE_NOTIFY
                             else ''),  # extra group to notify.
        'LATEST_INDEX':INDEX_HINT, # Just a hint, it is also stored in shelve db
        'SITE_KEY':insert_ssk,
        'INSERT_URI':insert_uri,
        'REQUEST_URI':request_uri,
        'VERBOSITY':VERBOSITY,
        'TMP_DIR':tmp_dir,
        'NO_SEARCH':False, # REQUIRED
        'USK_HASH':get_usk_hash(request_uri),
        'FNPUSH_COALESCE_SECS':60, # Time to wait before pushing
        'SITE_COALESCE_SECS':60, # Time to wait before inserting.
        'NOTIFY_COALESCE_SECS':60, # Time 2w8b4 sending fms repo update msg
        'COMMIT_COALESCE_SECS':-1, # Hack to force immediate commit
        'FMS_TRUST_CACHE_SECS': 1 * 60 * 60,
        'FMS_MIN_TRUST':55, # peer message trust
        'NONE_TRUST':49, # i.e. disable posting for 'None' peer msg trust
        'REPO_DIR':repo_dir,

        # Only uncomment for testing.
        #'MSG_SPOOL_DIR':'/tmp/fake_msgs',
        })

    return params

def run_wikibot(params):
    """ Setup an FMSBotRunner and run a single WikiBot instance in it. """

    # Setup RequestQueue for FCP requests.
    async_socket = PolledSocket(params['FCP_HOST'], params['FCP_PORT'])
    request_runner = RequestRunner(FCPConnection(async_socket, True),
                                   params['N_CONCURRENT'])

    # Setup FMSBotRunner to house the WikiBot.
    bot_runner = FMSBotRunner(params)
    if 'MSG_SPOOL_DIR' in params:
        print "READING MESSAGES FROM SPOOL DIR INSTEAD OF FMS!"

        # This table MUST map all short names to full fms_ids for
        # all message senders. MUST contain the bot fms_id.
        lut = {'djk':'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks'}
        assert params['FMS_ID'] in lut
        bot_runner.nntp = FMSStub(params['MSG_SPOOL_DIR'],
                                  params['FMS_GROUP'],
                                  lut)

    # Install a single WikiBot instance.
    wiki_bot = WikiBot('wikibot_' + params['USK_HASH'],
                       params, request_runner)
    bot_runner.register_bot(wiki_bot, (params['FMS_GROUP'], ))

    # Initialize the FMSBotRunner
    bot_runner.startup()

    # Run until there's an error on the FCP socket or
    # the FMSBotRunner shuts down.
    run_event_loops(bot_runner, request_runner,
                    params['FMS_POLL_SECS'],
                    params['FCP_POLL_SECS'],
                    wiki_bot.log) # Hmmm... (ab)use WikiBot log.



############################################################
# Use explict dispatch table in order to avoid conditional
# gook.
def cmd_setup(dummy):
    """ Setup the working directories used by the wikibot."""
    get_dirs(BASE_DIR, True)

def cmd_start(params):
    """ Start the bot. REQUIRES already setup."""
    run_wikibot(params)

def cmd_stop(params):
    """ Stop the bot."""
    try:
        pid = int(open(make_bot_path(params['BOT_STORAGE_DIR'],
                                 'wikibot_' + params['USK_HASH'],
                                 'pid'), 'rb').read().strip())

        print "Stopping, pid: %i..." % pid
        os.kill(pid, signal.SIGINT)
        os.waitpid(pid, 0)
        print "Stopped."
    except IOError: # no pid file
        print "Not running."
    except OSError, err:
        if err.errno ==  errno.ECHILD:
            # Process died before waitpid.
            print "Stopped."
        else:
            print "Failed: ", err

def cmd_status(params):
    """ Check if the bot is running."""

    print "wikibot_%s:" % params['USK_HASH']
    print "storage: %s" % params['BOT_STORAGE_DIR']

    # Attribution:
    # http://stackoverflow.com/questions/38056/how-do-you-check-in-linux-with- \
    #       python-if-a-process-is-still-running
    try:
        pid = int(open(make_bot_path(params['BOT_STORAGE_DIR'],
                                 'wikibot_' + params['USK_HASH'],
                                 'pid'), 'rb').read().strip())

        print "pid: %i" % pid
        os.kill(pid, 0)
        print "STATUS: Running"
    except IOError: # no pid file
        print "STATUS: Stopped"
    except OSError, err:
        if err.errno == errno.ESRCH:
            print "STATUS: Crashed!"
        elif err.errno == errno.EPERM:
            print "No permission to signal this process! Maybe run whoami?"
        else:
            print "Unknown error checking pid!"

def cmd_catchup(params):
    """ Rebuild local working files rebuilding IGNORING all
        submission messages.

        This is used to re-bootstrap the bot when the local database
        files have been lost or deleted.  e.g. moving the bot to
        a different machine.

        BUG: Doesn't restore the processed submission CHK list.
"""
    params['CATCH_UP'] = True
    params['FMS_POLL_SECS'] = 1
    run_wikibot(params)

def cmd_help(dummy):
    """ Print a help message."""

    print """USAGE:
run_wikibot.py <cmd>

where <cmd> is %s""" % (', '.join(DISPATCH_TABLE.keys()))

DISPATCH_TABLE = {"setup":cmd_setup,
                  "start":cmd_start,
                  "stop":cmd_stop,
                  "status":cmd_status,
                  "catchup":cmd_catchup,
                  "help":cmd_help}

############################################################

def main():
    """ CLI entry point."""
    cmd = sys.argv[1] if len(sys.argv) == 2 else 'help'
    try:
        parameters = (None if cmd == 'setup' or cmd == 'help'
                      else get_params(BASE_DIR))
    except IOError, err:
        print "FAILED: %s" % str(err)
        return

    DISPATCH_TABLE[cmd](parameters)

if __name__ == "__main__":
    main()
