""" Implementation of commands for Infocalypse mercurial extension.

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

# REDFLAG: cleanup exception handling
#          by converting socket.error to IOError in fcpconnection?
# REDFLAG: returning vs aborting. set system exit code.
import os
import socket
import time
from binascii import hexlify

from mercurial import util
from mercurial import commands

from fcpclient import parse_progress, is_usk, is_ssk, get_version, \
     get_usk_for_usk_version, FCPClient, is_usk_file, is_negative_usk
from fcpconnection import FCPConnection, PolledSocket, CONNECTION_STATES, \
     get_code, FCPError
from fcpmessage import PUT_FILE_DEF

from requestqueue import RequestRunner

from graph import UpdateGraph, get_heads, has_version
from bundlecache import BundleCache, is_writable, make_temp_file
from updatesm import UpdateStateMachine, QUIESCENT, FINISHING, REQUESTING_URI, \
     REQUESTING_GRAPH, REQUESTING_BUNDLES, INVERTING_URI, \
     REQUESTING_URI_4_INSERT, INSERTING_BUNDLES, INSERTING_GRAPH, \
     INSERTING_URI, FAILING, REQUESTING_URI_4_COPY, \
     REQUIRES_GRAPH_4_HEADS, REQUESTING_GRAPH_4_HEADS, \
     RUNNING_SINGLE_REQUEST, UpdateContext

from archivesm import ArchiveStateMachine, ArchiveUpdateContext

from statemachine import StatefulRequest

from config import Config, DEFAULT_CFG_PATH, FORMAT_VERSION, normalize

from knownrepos import DEFAULT_TRUST, DEFAULT_GROUPS

DEFAULT_PARAMS = {
    # FCP params
    'MaxRetries':3,
    'PriorityClass':1,
    'DontCompress':True, # hg bundles are already compressed.
    'Verbosity':1023, # MUST set this to get progress messages.
    #'GetCHKOnly':True, # REDFLAG: For testing only. Not sure this still works.

    # Non-FCP stuff
    'N_CONCURRENT':4, # Maximum number of concurrent FCP requests.
    'CANCEL_TIME_SECS': 20 * 60, # Bound request time.
    'POLL_SECS':0.25, # Time to sleep in the polling loop.

    # Testing HACKs
    #'TEST_DISABLE_GRAPH': True, # Disable reading the graph.
    #'TEST_DISABLE_UPDATES': True, # Don't update info in the top key.
    #'MSG_SPOOL_DIR':'/tmp/fake_msgs', # Stub out fms
    }

MSG_TABLE = {(QUIESCENT, REQUESTING_URI_4_INSERT)
             :"Requesting previous URI...",
             (QUIESCENT, REQUESTING_URI_4_COPY)
             :"Requesting URI to copy...",
             (REQUESTING_URI_4_INSERT,REQUESTING_GRAPH)
             :"Requesting previous graph...",
             (INSERTING_BUNDLES,INSERTING_GRAPH)
             :"Inserting updated graph...",
             (INSERTING_GRAPH, INSERTING_URI)
             :"Inserting URI...",
             (REQUESTING_URI_4_COPY, INSERTING_URI)
             :"Inserting copied URI...",
             (QUIESCENT, REQUESTING_URI)
             :"Fetching URI...",
             (REQUESTING_URI, REQUESTING_BUNDLES)
             :"Fetching bundles...",
             (REQUIRES_GRAPH_4_HEADS, REQUESTING_GRAPH_4_HEADS)
             :"Head list not in top key, fetching graph...",
             }

class UICallbacks:
    """ Display callback output with a ui instance. """
    def __init__(self, ui_):
        self.ui_ = ui_
        self.verbosity = 0

    def connection_state(self, dummy, state):
        """ FCPConnection.state_callback function which writes to a ui. """

        if self.verbosity < 2:
            return

        value = CONNECTION_STATES.get(state)
        if not value:
            value = "UNKNOWN"

        self.ui_.status("FCP connection [%s]\n" % value)

    def transition_callback(self, from_state, to_state):
        """ StateMachine transition callback that writes to a ui."""
        if self.verbosity < 1:
            return
        if self.verbosity > 2:
            self.ui_.status("[%s]->[%s]\n" % (from_state.name, to_state.name))
            return
        if to_state.name == FAILING:
            self.ui_.status("Cleaning up after failure...\n")
            return
        if to_state.name == FINISHING:
            self.ui_.status("Cleaning up...\n")
            return
        msg = MSG_TABLE.get((from_state.name, to_state.name))
        if not msg is None:
            self.ui_.status("%s\n" % msg)

    def monitor_callback(self, update_sm, client, msg):
        """ FCP message status callback which writes to a ui. """
        if self.verbosity < 2:
            return

        #prefix = update_sm.current_state.name
        prefix = ''
        if self.verbosity > 2:
            prefix = client.request_id()[:10] + ':'

        if hasattr(update_sm.current_state, 'pending') and self.verbosity > 1:
            prefix = ("{%i}:" % len(update_sm.runner.running)) + prefix

        if msg[0] == 'SimpleProgress':
            text = str(parse_progress(msg))
        elif msg[0] == 'URIGenerated':
            return # shows up twice
        #elif msg[0] == 'PutSuccessful':
        #    text = 'PutSuccessful:' + msg[1]['URI']
        elif msg[0] == 'ProtocolError':
            text = 'ProtocolError:' + str(msg)
        elif msg[0] == 'AllData':
             # Don't try to print raw data.
            text = 'AllData: length=%s' % msg[1].get('DataLength', '???')
        elif msg[0].find('Failed') != -1:
            code = get_code(msg) or -1
            redirect = ''
            if (code == 27 and 'RedirectURI' in msg[1]
                and is_usk(msg[1]['RedirectURI'])):
                redirect = ", redirected to version: %i" % \
                           get_version(msg[1]['RedirectURI'])

            text = "%s: code=%i%s" % (msg[0], code, redirect)
        else:
            text = msg[0]

        self.ui_.status("%s%s:%s\n" % (prefix, str(client.tag), text))
        # REDFLAG: re-add full dumping of FCP errors at debug level?
        #if msg[0].find('Failed') != -1 or msg[0].find('Error') != -1:
            #print  client.in_params.pretty()
            #print msg
            #print "FINISHED:" , bool(client.is_finished())

# Hmmmm... SUSPECT. Abuse of mercurial ui design intent.
# ISSUE: I don't just want to suppress/include output.
# I use this value to keep from running code which isn't
# required.
def get_verbosity(ui_):
    """ INTERNAL: Get the verbosity level from the state of a ui. """
    if ui_.debugflag:
        return 5 # Graph, candidates, canonical paths
    elif ui_.verbose:
        return 2 # FCP message status
    elif ui_.quiet:
        # Hmmm... still not 0 output
        return 0
    else:
        return 1 # No FCP message status

def get_config_info(ui_, opts):
    """ INTERNAL: Read configuration info out of the config file and
        or command line options. """

    cfg = Config.from_ui(ui_)
    if cfg.defaults['FORMAT_VERSION'] != FORMAT_VERSION:
        ui_.warn(('Updating config file: %s\n'
                  + 'From format version: %s\nTo format version: %s\n') %
                 (str(cfg.file_name),
                  cfg.defaults['FORMAT_VERSION'],
                  FORMAT_VERSION))

        # Hacks to clean up variables that were set wrong.
        if not cfg.fmsread_trust_map:
            ui_.warn('Set default trust map.\n')
            cfg.fmsread_trust_map = DEFAULT_TRUST.copy()
        if not cfg.fmsread_groups or cfg.fmsread_groups == ['', ]:
            ui_.warn('Set default fmsread groups.\n')
            cfg.fmsread_groups = DEFAULT_GROUPS
        Config.to_file(cfg)
        ui_.warn('Converted OK.\n')

    if opts.get('fcphost') != '':
        cfg.defaults['HOST'] = opts['fcphost']
    if opts.get('fcpport') != 0:
        cfg.defaults['PORT'] = opts['fcpport']

    params = DEFAULT_PARAMS.copy()
    params['FCP_HOST'] = cfg.defaults['HOST']
    params['FCP_PORT'] = cfg.defaults['PORT']
    params['TMP_DIR'] = cfg.defaults['TMP_DIR']
    params['VERBOSITY'] = get_verbosity(ui_)
    params['NO_SEARCH'] = (bool(opts.get('nosearch')) and
                           (opts.get('uri', None) or
                            opts.get('requesturi', None)))

    request_uri = opts.get('uri') or opts.get('requesturi')
    if bool(opts.get('nosearch')) and not request_uri:
        if opts.get('uri'):
            arg_name = 'uri'
        else:
            assert opts.get('requesturi')
            arg_name = 'requesturi'

        ui_.status('--nosearch ignored because --%s was not set.\n' % arg_name)
    params['AGGRESSIVE_SEARCH'] = (bool(opts.get('aggressive')) and
                                   not params['NO_SEARCH'])
    if bool(opts.get('aggressive')) and params['NO_SEARCH']:
        ui_.status('--aggressive ignored because --nosearch was set.\n')

    return (params, cfg)

# Hmmmm USK@/style_keys/0
def check_uri(ui_, uri):
    """ INTERNAL: Abort if uri is not supported. """
    if uri is None:
        return

    if is_usk(uri):
        if not is_usk_file(uri):
            ui_.status("Only file USKs are allowed."
                       + "\nMake sure the URI ends with '/<number>' "
                       + "with no trailing '/'.\n")
            raise util.Abort("Non-file USK %s\n" % uri)
        # Just fix it instead of doing B&H?
        if is_negative_usk(uri):
            ui_.status("Negative USK index values are not allowed."
                       + "\nUse --aggressive instead. \n")
            raise util.Abort("Negative USK %s\n" % uri)

def set_debug_vars(verbosity, params):
    """ Set debug dumping switch variables based on verbosity. """
    if verbosity > 2 and params.get('DUMP_GRAPH', None) is None:
        params['DUMP_GRAPH'] = True
    if verbosity > 3 and params.get('DUMP_UPDATE_EDGES', None) is None:
        params['DUMP_UPDATE_EDGES'] = True
    if verbosity > 4 and params.get('DUMP_CANONICAL_PATHS', None) is None:
        params['DUMP_CANONICAL_PATHS'] = True
    if verbosity > 4 and params.get('DUMP_URIS', None) is None:
        params['DUMP_URIS'] = True
    if verbosity > 4 and params.get('DUMP_TOP_KEY', None) is None:
        params['DUMP_TOP_KEY'] = True

# REDFLAG: remove store_cfg
def setup(ui_, repo, params, stored_cfg):
    """ INTERNAL: Setup to run an Infocalypse extension command. """
    # REDFLAG: choose another name. Confusion w/ fcp param
    # REDFLAG: add an hg param and get rid of this line.
    #params['VERBOSITY'] = 1

    check_uri(ui_, params.get('INSERT_URI'))
    check_uri(ui_, params.get('REQUEST_URI'))

    if not is_writable(os.path.expanduser(stored_cfg.defaults['TMP_DIR'])):
        raise util.Abort("Can't write to temp dir: %s\n"
                         % stored_cfg.defaults['TMP_DIR'])

    verbosity = params.get('VERBOSITY', 1)
    set_debug_vars(verbosity, params)

    callbacks = UICallbacks(ui_)
    callbacks.verbosity = verbosity

    if not repo is None:
        # BUG:? shouldn't this be reading TMP_DIR from stored_cfg
        cache = BundleCache(repo, ui_, params['TMP_DIR'])

    try:
        async_socket = PolledSocket(params['FCP_HOST'], params['FCP_PORT'])
        connection = FCPConnection(async_socket, True,
                                   callbacks.connection_state)
    except socket.error, err: # Not an IOError until 2.6.
        ui_.warn("Connection to FCP server [%s:%i] failed.\n"
                % (params['FCP_HOST'], params['FCP_PORT']))
        raise err
    except IOError, err:
        ui_.warn("Connection to FCP server [%s:%i] failed.\n"
                % (params['FCP_HOST'], params['FCP_PORT']))
        raise err

    runner = RequestRunner(connection, params['N_CONCURRENT'])

    if repo is None:
        # For incremental archives.
        ctx = ArchiveUpdateContext()
        update_sm = ArchiveStateMachine(runner, ctx)
    else:
        # For Infocalypse repositories
        ctx = UpdateContext(None)
        ctx.repo = repo
        ctx.ui_ = ui_
        ctx.bundle_cache = cache
        update_sm = UpdateStateMachine(runner, ctx)


    update_sm.params = params.copy()
    update_sm.transition_callback = callbacks.transition_callback
    update_sm.monitor_callback = callbacks.monitor_callback

    # Modify only after copy.
    update_sm.params['FREENET_BUILD'] = runner.connection.node_hello[1]['Build']

    return update_sm

def run_until_quiescent(update_sm, poll_secs, close_socket=True):
    """ Run the state machine until it reaches the QUIESCENT state. """
    runner = update_sm.runner
    assert not runner is None
    connection = runner.connection
    assert not connection is None
    raised = True
    try:
        while update_sm.current_state.name != QUIESCENT:
            # Poll the FCP Connection.
            try:
                if not connection.socket.poll():
                    print "run_until_quiescent -- poll returned False" 
                    # REDFLAG: jam into quiesent state?,
                    # CONNECTION_DROPPED state?
                    break
                # Indirectly nudge the state machine.
                update_sm.runner.kick()
            except socket.error: # Not an IOError until 2.6.
                update_sm.ctx.ui_.warn("Exiting because of an error on "
                                       + "the FCP socket.\n")
                raise
            except IOError:
                # REDLAG: better message.
                update_sm.ctx.ui_.warn("Exiting because of an IO error.\n")
                raise
            # Rest :-)
            time.sleep(poll_secs)
        raised = False
    finally:
        if raised or close_socket:
            update_sm.runner.connection.close()

def cleanup(update_sm):
    """ INTERNAL: Cleanup after running an Infocalypse command. """
    if update_sm is None:
        return

    if not update_sm.runner is None:
        update_sm.runner.connection.close()

    if not update_sm.ctx.bundle_cache is None:
        update_sm.ctx.bundle_cache.remove_files()

# This function needs cleanup.
# REDFLAG: better name. 0) inverts 1) updates indices from cached state.
# 2) key substitutions.
def do_key_setup(ui_, update_sm, params, stored_cfg):
    """ INTERNAL:  Handle inverting/updating keys before running a command."""
    insert_uri = params.get('INSERT_URI')
    if not insert_uri is None and insert_uri.startswith('USK@/'):
        insert_uri = ('USK'
                      + stored_cfg.defaults['DEFAULT_PRIVATE_KEY'][3:]
                      + insert_uri[5:])
        ui_.status("Filled in the insert URI using the default private key.\n")

    if insert_uri is None or not (is_usk(insert_uri) or is_ssk(insert_uri)):
        return (params.get('REQUEST_URI'), False)

    update_sm.start_inverting(insert_uri)
    run_until_quiescent(update_sm, params['POLL_SECS'], False)
    if update_sm.get_state(QUIESCENT).prev_state != INVERTING_URI:
        raise util.Abort("Couldn't invert private key:\n%s" % insert_uri)

    inverted_uri = update_sm.get_state(INVERTING_URI).get_request_uri()
    params['INVERTED_INSERT_URI'] = inverted_uri

    if is_usk(insert_uri):
        # Determine the highest known index for the insert uri.
        max_index = max(stored_cfg.get_index(inverted_uri),
                        get_version(insert_uri))

        # Update the insert uri to the latest known version.
        params['INSERT_URI'] = get_usk_for_usk_version(insert_uri,
                                                       max_index)

        # Update the inverted insert URI to the latest known version.
        params['INVERTED_INSERT_URI'] = get_usk_for_usk_version(
            inverted_uri,
            max_index)

    # Update the index of the request uri using the stored config.
    request_uri = params.get('REQUEST_URI')
    if not request_uri is None and is_usk(request_uri):
        assert not params['NO_SEARCH'] or not request_uri is None
        if not params['NO_SEARCH']:
            max_index = max(stored_cfg.get_index(request_uri),
                            get_version(request_uri))
            request_uri = get_usk_for_usk_version(request_uri, max_index)

        if (params['NO_SEARCH'] and
            # Force the insert URI down to the version in the request URI.
            usks_equal(request_uri, params['INVERTED_INSERT_URI'])):
            params['INVERTED_INSERT_URI'] = request_uri
            params['INSERT_URI'] = get_usk_for_usk_version(
                insert_uri,
                get_version(request_uri))

    # Skip key inversion if we already inverted the insert_uri.
    is_keypair = False
    if (request_uri is None and
        not params.get('INVERTED_INSERT_URI') is None):
        request_uri = params['INVERTED_INSERT_URI']
        is_keypair = True
    return (request_uri, is_keypair)

def handle_updating_config(repo, update_sm, params, stored_cfg,
                           is_pulling=False):
    """ INTERNAL: Write updates into the config file IFF the previous
        command succeeded. """
    if not is_pulling:
        if not update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            return

        if not is_usk_file(params['INSERT_URI']):
            return

        inverted_uri = params['INVERTED_INSERT_URI']

        # Cache the request_uri - insert_uri mapping.
        stored_cfg.set_insert_uri(inverted_uri, update_sm.ctx['INSERT_URI'])

        # Cache the updated index for the insert.
        version = get_version(update_sm.ctx['INSERT_URI'])
        stored_cfg.update_index(inverted_uri, version)
        stored_cfg.update_dir(repo.root, inverted_uri)

        # Hmmm... if we wanted to be clever we could update the request
        # uri too when it doesn't match the insert uri. Ok for now.
        # Only for usks and only on success.
        #print "UPDATED STORED CONFIG(0)"
        Config.to_file(stored_cfg)

    else:
        # Only finishing required. same. REDFLAG: look at this again
        if not update_sm.get_state(QUIESCENT).arrived_from((
            REQUESTING_BUNDLES, FINISHING)):
            return

        if not is_usk(params['REQUEST_URI']):
            return

        state = update_sm.get_state(REQUESTING_URI)
        updated_uri = state.get_latest_uri()
        version = get_version(updated_uri)
        stored_cfg.update_index(updated_uri, version)
        stored_cfg.update_dir(repo.root, updated_uri)
        #print "UPDATED STORED CONFIG(1)"
        Config.to_file(stored_cfg)

def is_redundant(uri):
    """ Return True if uri is a file USK and ends in '.R1',
        False otherwise. """
    if not is_usk_file(uri):
        return ''
    fields = uri.split('/')
    if not fields[-2].endswith('.R1'):
        return ''
    return 'Redundant '

############################################################
# User feedback? success, failure?
def execute_create(ui_, repo, params, stored_cfg):
    """ Run the create command. """
    update_sm = None
    try:
        update_sm = setup(ui_, repo, params, stored_cfg)
        # REDFLAG: Do better.
        # This call is not necessary, but I do it to set
        # 'INVERTED_INSERT_URI'. Write code to fish that
        # out of INSERTING_URI instead.
        do_key_setup(ui_, update_sm, params, stored_cfg)

        ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params['INSERT_URI']),
                                            params['INSERT_URI']))
        #ui_.status("Current tip: %s\n" % hex_version(repo)[:12])

        update_sm.start_inserting(UpdateGraph(),
                                  params.get('TO_VERSIONS', ('tip',)),
                                  params['INSERT_URI'])

        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Inserted to:\n%s\n" %
                       '\n'.join(update_sm.get_state(INSERTING_URI).
                                 get_request_uris()))
        else:
            ui_.status("Create failed.\n")

        handle_updating_config(repo, update_sm, params, stored_cfg)
    finally:
        cleanup(update_sm)

# REDFLAG: LATER: make this work without a repo?
def execute_copy(ui_, repo, params, stored_cfg):
    """ Run the copy command. """
    update_sm = None
    try:
        update_sm = setup(ui_, repo, params, stored_cfg)
        do_key_setup(ui_, update_sm, params, stored_cfg)

        ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params['INSERT_URI']),
                                            params['INSERT_URI']))
        update_sm.start_copying(params['REQUEST_URI'],
                                params['INSERT_URI'])

        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Copied to:\n%s\n" %
                       '\n'.join(update_sm.get_state(INSERTING_URI).
                                 get_request_uris()))
        else:
            ui_.status("Copy failed.\n")

        handle_updating_config(repo, update_sm, params, stored_cfg)
    finally:
        cleanup(update_sm)

def usks_equal(usk_a, usk_b):
    """ Returns True if the USKs are equal disregarding version. """
    return (get_usk_for_usk_version(usk_a, 0)
            == get_usk_for_usk_version(usk_b, 0))

LEVEL_MSGS = {
    1:"Re-inserting top key(s) and graph(s).",
    2:"Re-inserting top key(s) if possible, graph(s), latest update.",
    3:"Re-inserting top key(s) if possible, graph(s), all bootstrap CHKs.",
    4:"Inserting redundant keys for > 7Mb updates.",
    5:"Re-inserting redundant updates > 7Mb.",
    }

def execute_reinsert(ui_, repo, params, stored_cfg):
    """ Run the reinsert command. """
    update_sm = None
    try:
        update_sm = setup(ui_, repo, params, stored_cfg)
        request_uri, is_keypair = do_key_setup(ui_, update_sm,
                                               params, stored_cfg)
        params['REQUEST_URI'] = request_uri

        if not params['INSERT_URI'] is None:
            if (is_usk(params['INSERT_URI']) and
                (not is_usk(params['REQUEST_URI'])) or
                (not usks_equal(params['REQUEST_URI'],
                                params['INVERTED_INSERT_URI']))):
                raise util.Abort("Request URI doesn't match insert URI.")

            ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params[
                'INSERT_URI']),
                                                params['INSERT_URI']))
        ui_.status("%sRequest URI:\n%s\n" % (is_redundant(params[
            'REQUEST_URI']),
                                             params['REQUEST_URI']))

        ui_.status(LEVEL_MSGS[params['REINSERT_LEVEL']] + '\n')
        update_sm.start_reinserting(params['REQUEST_URI'],
                                    params['INSERT_URI'],
                                    is_keypair,
                                    params['REINSERT_LEVEL'])

        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Reinsert finished.\n")
        else:
            ui_.status("Reinsert failed.\n")

        # Don't need to update the config.
    finally:
        cleanup(update_sm)

def execute_push(ui_, repo, params, stored_cfg):
    """ Run the push command. """

    assert params.get('REQUEST_URI', None) is None
    update_sm = None
    try:
        update_sm = setup(ui_, repo, params, stored_cfg)
        request_uri, is_keypair = do_key_setup(ui_, update_sm, params,
                                               stored_cfg)

        ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params['INSERT_URI']),
                                            params['INSERT_URI']))
        #ui_.status("Current tip: %s\n" % hex_version(repo)[:12])

        update_sm.start_pushing(params['INSERT_URI'],
                                params.get('TO_VERSIONS', ('tip',)),
                                request_uri, # None is allowed
                                is_keypair)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Inserted to:\n%s\n" %
                       '\n'.join(update_sm.get_state(INSERTING_URI).
                                 get_request_uris()))
        else:
            extra = ''
            if update_sm.ctx.get('UP_TO_DATE', False):
                extra = '. Local changes already in Freenet'
            ui_.status("Push failed%s.\n" % extra)

        handle_updating_config(repo, update_sm, params, stored_cfg)
    finally:
        cleanup(update_sm)

def execute_pull(ui_, repo, params, stored_cfg):
    """ Run the pull command. """
    update_sm = None
    try:
        assert not params['REQUEST_URI'] is None
        if not params['NO_SEARCH'] and is_usk_file(params['REQUEST_URI']):
            index = stored_cfg.get_index(params['REQUEST_URI'])
            if not index is None:
                if index >= get_version(params['REQUEST_URI']):
                    # Update index to the latest known value
                    # for the --uri case.
                    params['REQUEST_URI'] = get_usk_for_usk_version(
                        params['REQUEST_URI'], index)
                else:
                    ui_.status(("Cached index [%i] < index in USK [%i].  "
                                + "Using the index from the USK.\n"
                                + "You're sure that index exists, right?\n") %
                               (index, get_version(params['REQUEST_URI'])))

        update_sm = setup(ui_, repo, params, stored_cfg)
        ui_.status("%sRequest URI:\n%s\n" % (is_redundant(params[
            'REQUEST_URI']),
                                             params['REQUEST_URI']))
        #ui_.status("Current tip: %s\n" % hex_version(repo)[:12])
        update_sm.start_pulling(params['REQUEST_URI'])
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Pulled from:\n%s\n" %
                       update_sm.get_state('REQUESTING_URI').
                       get_latest_uri())
            #ui_.status("New tip: %s\n" % hex_version(repo)[:12])
        else:
            ui_.status("Pull failed.\n")

        handle_updating_config(repo, update_sm, params, stored_cfg, True)
    finally:
        cleanup(update_sm)


# Note: doesn't close the socket, but its ok because cleanup() does.
def read_freenet_heads(params, update_sm, request_uri):
    """ Helper function reads the know heads from Freenet. """
    update_sm.start_requesting_heads(request_uri)
    run_until_quiescent(update_sm, params['POLL_SECS'], False)
    if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
        if update_sm.ctx.graph is None:
            # Heads are in the top key.
            top_key_tuple = update_sm.get_state(REQUIRES_GRAPH_4_HEADS).\
                            get_top_key_tuple()
            assert top_key_tuple[1][0][5] # heads list complete
            return top_key_tuple[1][0][2] # stored in first update

        else:
            # Have to pull the heads from the graph.
            assert not update_sm.ctx.graph is None
            return get_heads(update_sm.ctx.graph)

    raise util.Abort("Couldn't read heads from Freenet.")


NO_INFO_FMT = """There's no stored information about this USK.
USK hash: %s
"""

INFO_FMT = """USK hash: %s
Index   : %i

Trusted Notifiers:
%s

Request URI:
%s
Insert URI:
%s

Reading repo state from Freenet...
"""

def execute_info(ui_, repo, params, stored_cfg):
    """ Run the info command. """
    request_uri = params['REQUEST_URI']
    if request_uri is None or not is_usk_file(request_uri):
        ui_.status("Only works with USK file URIs.\n")
        return

    usk_hash = normalize(request_uri)
    max_index = stored_cfg.get_index(request_uri)
    if max_index is None:
        ui_.status(NO_INFO_FMT % usk_hash)
        return

    insert_uri = str(stored_cfg.get_insert_uri(usk_hash))

    # fix index
    request_uri = get_usk_for_usk_version(request_uri, max_index)

    trusted = stored_cfg.trusted_notifiers(usk_hash)
    if not trusted:
        trusted = '   None'
    else:
        trusted = '   ' + '\n   '.join(trusted)

    ui_.status(INFO_FMT %
               (usk_hash, max_index or -1, trusted, request_uri, insert_uri))

    update_sm = setup(ui_, repo, params, stored_cfg)
    try:
        ui_.status('Freenet head(s): %s\n' %
                   ' '.join([ver[:12] for ver in
                             read_freenet_heads(params, update_sm,
                                                request_uri)]))
    finally:
        cleanup(update_sm)


def setup_tmp_dir(ui_, tmp):
    """ INTERNAL: Setup the temp directory. """
    tmp = os.path.expanduser(tmp)

    # Create the tmp dir if nescessary.
    if not os.path.exists(tmp):
        try:
            os.makedirs(tmp)
        except os.error, err:
            # Will exit below.
            ui_.warn(err)
    return tmp


MSG_HGRC_SET = \
"""Read the config file name from the:

[infocalypse]
cfg_file = <filename>

section of your .hgrc (or mercurial.ini) file.

cfg_file: %s

"""

MSG_CFG_EXISTS = \
"""%s already exists!
Move it out of the way if you really
want to re-run setup.

Consider before deleting it. It may contain
the *only copy* of your private key.

If you're just trying to update the FMS configuration run:

hg fn-setupfms

instead.

"""
def execute_setup(ui_, host, port, tmp, cfg_file = None):
    """ Run the setup command. """
    def connection_failure(msg):
        """ INTERNAL: Display a warning string. """
        ui_.warn(msg)
        ui_.warn("It looks like your FCP host or port might be wrong.\n")
        ui_.warn("Set them with --fcphost and/or --fcpport and try again.\n")
        raise util.Abort("Connection to FCP server failed.")

    # Fix defaults.
    if host == '':
        host = '127.0.0.1'
    if port == 0:
        port = 9481

    if cfg_file is None:
        cfg_file = os.path.expanduser(DEFAULT_CFG_PATH)

    existing_name = ui_.config('infocalypse', 'cfg_file', None)
    if not existing_name is None:
        existing_name = os.path.expanduser(existing_name)
        ui_.status(MSG_HGRC_SET % existing_name)
        cfg_file = existing_name

    if os.path.exists(cfg_file):
        ui_.status(MSG_CFG_EXISTS % cfg_file)
        raise util.Abort("Refusing to modify existing configuration.")

    tmp = setup_tmp_dir(ui_, tmp)

    if not is_writable(tmp):
        raise util.Abort("Can't write to temp dir: %s\n" % tmp)

    # Test FCP connection.
    timeout_secs = 20
    connection = None
    default_private_key = None
    try:
        ui_.status("Testing FCP connection [%s:%i]...\n" % (host, port))

        connection = FCPConnection(PolledSocket(host, port))

        started = time.time()
        while (not connection.is_connected() and
               time.time() - started < timeout_secs):
            connection.socket.poll()
            time.sleep(.25)

        if not connection.is_connected():
            connection_failure(("\nGave up after waiting %i secs for an "
                               + "FCP NodeHello.\n") % timeout_secs)

        ui_.status("Looks good.\nGenerating a default private key...\n")

        # Hmmm... this waits on a socket. Will an ioerror cause an abort?
        # Lazy, but I've never seen this call fail except for IO reasons.
        client = FCPClient(connection)
        client.message_callback = lambda x, y:None # Disable chatty default.
        default_private_key = client.generate_ssk()[1]['InsertURI']

    except FCPError:
        # Protocol error.
        connection_failure("\nMaybe that's not an FCP server?\n")

    except socket.error: # Not an IOError until 2.6.
        # Horked.
        connection_failure("\nSocket level error.\n")

    except IOError:
        # Horked.
        connection_failure("\nSocket level error.\n")

    cfg = Config()
    cfg.defaults['HOST'] = host
    cfg.defaults['PORT'] = port
    cfg.defaults['TMP_DIR'] = tmp
    cfg.defaults['DEFAULT_PRIVATE_KEY'] = default_private_key
    Config.to_file(cfg, cfg_file)

    ui_.status("""\nFinished setting configuration.
FCP host: %s
FCP port: %i
Temp dir: %s
cfg file: %s

Default private key:
%s

The config file was successfully written!

""" % (host, port, tmp, cfg_file, default_private_key))


def create_patch_bundle(ui_, repo, freenet_heads, out_file):
    """ Creates an hg bundle file containing all the changesets
        later than freenet_heads. """

    freenet_heads = list(freenet_heads)
    freenet_heads.sort()
    # Make sure you have them all locally
    for head in freenet_heads:
        if not has_version(repo, head):
            raise util.Abort("The local repository isn't up to date. " +
                             "Run hg fn-pull.")

    heads = [hexlify(head) for head in repo.heads()]
    heads.sort()

    if freenet_heads == heads:
        raise util.Abort("All local changesets already in the repository " +
                         "in Freenet.")

    # Create a bundle using the freenet_heads as bases.
    ui_.pushbuffer()
    try:
        #print 'PARENTS:', freenet_heads
        #print 'HEADS:', heads
        commands.bundle(ui_, repo, out_file,
                        None, base=list(freenet_heads),
                        rev=heads)
    finally:
        ui_.popbuffer()

def execute_insert_patch(ui_, repo, params, stored_cfg):
    """ Create and hg bundle containing all changes not already in the
        infocalypse repo in Freenet and insert it to a CHK.

        Returns a machine readable patch notification message.
        """
    try:
        update_sm = setup(ui_, repo, params, stored_cfg)
        out_file = make_temp_file(update_sm.ctx.bundle_cache.base_dir)

        ui_.status("Reading repo state from Freenet...\n")
        freenet_heads = read_freenet_heads(params, update_sm,
                                           params['REQUEST_URI'])

        # This may eventually change to support other patch types.
        create_patch_bundle(ui_, repo, freenet_heads, out_file)

        # Make an FCP file insert request which will run on the
        # on the state machine.
        request = StatefulRequest(update_sm)
        request.tag = 'patch_bundle_insert'
        request.in_params.definition = PUT_FILE_DEF
        request.in_params.fcp_params = update_sm.params.copy()
        request.in_params.fcp_params['URI'] = 'CHK@'
        request.in_params.file_name = out_file
        request.in_params.send_data = True

        # Must do this here because file gets deleted.
        chk_len = os.path.getsize(out_file)

        ui_.status("Inserting %i byte patch bundle...\n" %
                   os.path.getsize(out_file))
        update_sm.start_single_request(request)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        freenet_heads = list(freenet_heads)
        freenet_heads.sort()
        heads = [hexlify(head) for head in repo.heads()]
        heads.sort()
        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            chk = update_sm.get_state(RUNNING_SINGLE_REQUEST).\
                  final_msg[1]['URI']
            ui_.status("Patch CHK:\n%s\n" %
                       chk)
            # ':', '|' not in freenet base64
            ret = ':'.join(('B', normalize(params['REQUEST_URI']), str(chk_len),
                            ':'.join([base[:12] for base in freenet_heads]),
                            '|', ':'.join([head[:12] for head in heads]), chk))

            ui_.status("\nNotification:\n%s\n" % ret
                        + '\n')
            return ret

        raise util.Abort("Patch CHK insert failed.")

    finally:
        # Cleans up out file.
        cleanup(update_sm)
