""" Set up and run a single wikibot instance.

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

from fcpclient import FCPClient, get_usk_hash
from fcpconnection import FCPConnection, PolledSocket
from requestqueue import RequestRunner
from bundlecache import is_writable

from fmsstub import FMSStub

from fmsbot import FMSBotRunner, run_event_loops
from wikibot import WikiBot


def read_fnwiki_cfg(cfg_file):
    """ Quick and dirty helper w/o hg deps. to read cfg file."""
    parser = ConfigParser()
    parser.read(cfg_file)
    if not parser.has_section('default'):
        raise IOError("Can't read default section of config file?")

    # Hmmm some param key strings are different than config.py.
    return {'WIKI_ROOT':parser.get('default', 'wiki_root'),
            'SITE_NAME':parser.get('default', 'site_name'),
            'SITE_DEFAULT_FILE':parser.get('default', 'default_file'),
            'FMS_GROUP':parser.get('default', 'wiki_group'),
            # Only the part before '@'.
            'FMS_ID':parser.get('default', 'wiki_server_id').split('@')[0],
            'WIKI_REPO_USK':parser.get('default', 'wiki_repo_usk')}

# LATER: load from a config file
def get_params():
    """ Return the parameters to run a WikiBot. """

    # Directory containing all bot related stuff.
    base_dir = '/tmp/wikibots'

    # File containing the private SSK key.
    key_file_fmt = key_file = '~/wikibot_key_%s.txt'

    # FCP info
    fcp_host = '127.0.0.1'
    fcp_port = 9481

    # FMS info
    fms_host = '127.0.0.1'
    fms_port = 1119
    # NOTE: fms id for bot is read from fnwiki.cfg.

    # Latest known repo usk index
    index_hint = 0

    # vebosity of logging output (NOT FCP 'Verbosity')
    verbosity = 5

    # MUST exist
    tmp_dir = os.path.join(base_dir, '__wikibot_tmp__')
    # MUST exist and contain wikitext hg repo.
    repo_dir = os.path.join(base_dir, 'hgrepo')
    # MUST exist
    bot_storage_dir = os.path.join(base_dir, 'bot_storage')

    #----------------------------------------------------------#
    assert is_writable(tmp_dir)
    assert os.path.exists(os.path.join(repo_dir, '.hg'))

    params = read_fnwiki_cfg(os.path.join(repo_dir, 'fnwiki.cfg'))

    # MUST contain SSK private key
    key_file = key_file_fmt % get_usk_hash(params['WIKI_REPO_USK'])
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
    request_uri = FCPClient.connect(fcp_host, fcp_port). \
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
        'FCP_HOST':fcp_host,
        'FCP_PORT':fcp_port,
        'FCP_POLL_SECS':0.25,
        'N_CONCURRENT':4,
        'CANCEL_TIME_SECS': 7 * 60,

        # FMSBotRunner
        'FMS_HOST':fms_host,
        'FMS_PORT':fms_port,
        'FMS_POLL_SECS': 3 * 60,
        'BOT_STORAGE_DIR':bot_storage_dir,

        # WikiBot
        'FMS_NOTIFY_GROUP':'infocalypse.notify', # extra group to notify.
        'LATEST_INDEX':index_hint, # Just a hint, it is also stored in shelve db
        'SITE_KEY':insert_ssk,
        'INSERT_URI':insert_uri,
        'REQUEST_URI':request_uri,
        'VERBOSITY':verbosity,
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



if __name__ == "__main__":
    run_wikibot(get_params())
