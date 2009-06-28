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

from mercurial import util

from fcpclient import parse_progress, is_usk, is_ssk, get_version, \
     get_usk_for_usk_version, FCPClient, is_usk_file, is_negative_usk

from fcpconnection import FCPConnection, PolledSocket, CONNECTION_STATES, \
     get_code, FCPError
from requestqueue import RequestRunner

from graph import UpdateGraph
from bundlecache import BundleCache, is_writable
from updatesm import UpdateStateMachine, QUIESCENT, FINISHING, REQUESTING_URI, \
     REQUESTING_GRAPH, REQUESTING_BUNDLES, INVERTING_URI, \
     REQUESTING_URI_4_INSERT, INSERTING_BUNDLES, INSERTING_GRAPH, \
     INSERTING_URI, FAILING, REQUESTING_URI_4_COPY, CANCELING, CleaningUp

from config import Config, DEFAULT_CFG_PATH, FORMAT_VERSION, normalize
from knownrepos import DEFAULT_TRUST, DEFAULT_GROUPS

DEFAULT_PARAMS = {
    # FCP params
    'MaxRetries':3,
    'PriorityClass':1,
    'DontCompress':True, # hg bundles are already compressed.
    'Verbosity':1023, # MUST set this to get progress messages.
    #'GetCHKOnly':True, # REDFLAG: DCI! remove

    # Non-FCP stuff
    'N_CONCURRENT':4, # Maximum number of concurrent FCP requests.
    'CANCEL_TIME_SECS': 10 * 60, # Bound request time.
    'POLL_SECS':0.25, # Time to sleep in the polling loop.
    #'TEST_DISABLE_GRAPH': True, # Disable reading the graph.
    #'TEST_DISABLE_UPDATES': True, # Don't update info in the top key.
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
        # REDFLAG: remove when 1209 comes out.
        if (msg[0] == 'PutFailed' and get_code(msg) == 9 and
            update_sm.params['FREENET_BUILD'] == '1208' and
            update_sm.ctx.get('REINSERT', 0) > 0):
            self.ui_.warn('There is a KNOWN BUG in 1208 which '
                          + 'causes code==9 failures for re-inserts.\n'
                          + 'The re-insert might actually have succeeded.\n'
                          + 'Who knows???\n')
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


def disable_cancel(updatesm, disable=True):
    """ INTERNAL: Hack to work around 1208 cancel kills FCP connection bug. """
    if disable:
        if not hasattr(updatesm.runner, 'old_cancel_request'):
            updatesm.runner.old_cancel_request = updatesm.runner.cancel_request
        msg = ("RequestRunner.cancel_request() disabled to work around "
               + "1208 bug\n")
        updatesm.runner.cancel_request = (
            lambda dummy : updatesm.ctx.ui_.status(msg))
    else:
        if hasattr(updatesm.runner, 'old_cancel_request'):
            updatesm.runner.cancel_request = updatesm.runner.old_cancel_request
            updatesm.ctx.ui_.status("Re-enabled canceling so that "
                                    + "shutdown works.\n")
class PatchedCleaningUp(CleaningUp):
    """ INTERNAL: 1208 bug work around to re-enable canceling. """
    def __init__(self, parent, name, finished_state):
        CleaningUp.__init__(self, parent, name, finished_state)

    def enter(self, from_state):
        """ Override to back out 1208 cancel hack. """
        disable_cancel(self.parent, False)
        CleaningUp.enter(self, from_state)

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

    callbacks = UICallbacks(ui_)
    callbacks.verbosity = verbosity

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
    update_sm = UpdateStateMachine(runner, repo, ui_, cache)
    update_sm.params = params.copy()
    update_sm.transition_callback = callbacks.transition_callback
    update_sm.monitor_callback = callbacks.monitor_callback

    # Modify only after copy.
    update_sm.params['FREENET_BUILD'] = runner.connection.node_hello[1]['Build']

    # REDFLAG: Hack to work around 1208 cancel bug. Remove.
    if update_sm.params['FREENET_BUILD'] == '1208':
        ui_.warn("DISABLING request canceling to work around 1208 FCP bug.\n"
                 "This may cause requests to hang. :-(\n\n")
        disable_cancel(update_sm)

        # Patch state machine to re-enable canceling on shutdown.
        #CANCELING:CleaningUp(self, CANCELING, QUIESCENT),
        #FAILING:CleaningUp(self, FAILING, QUIESCENT),
        #FINISHING:CleaningUp(self, FINISHING, QUIESCENT),
        update_sm.states[CANCELING] = PatchedCleaningUp(update_sm,
                                                        CANCELING, QUIESCENT)
        update_sm.states[FAILING] = PatchedCleaningUp(update_sm,
                                                      FAILING, QUIESCENT)
        update_sm.states[FINISHING] = PatchedCleaningUp(update_sm,
                                                        FINISHING, QUIESCENT)

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


# REDFLAG: move into fcpclient?
#def usks_equal(usk_a, usk_b):
#    assert is_usk(usk_a) and and is_usk(usk_b)
#    return (get_usk_for_usk_version(usk_a, 0) ==
#            get_usk_for_usk_version(usk_b, 0))

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
            ui_.status("Push failed.\n")

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
                # Update index to the latest known value
                # for the --uri case.
                params['REQUEST_URI'] = get_usk_for_usk_version(
                    params['REQUEST_URI'], index)

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

NO_INFO_FMT = """There's no stored information about that USK.
USK hash: %s
"""

INFO_FMT = """USK hash: %s
index   : %i

Request URI:
%s
Insert URI:
%s
"""

def execute_info(ui_, params, stored_cfg):
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

    ui_.status(INFO_FMT %
               (usk_hash, max_index or -1, request_uri, insert_uri))

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

"""

def execute_setup(ui_, host, port, tmp, cfg_file = None):
    """ Run the setup command. """
    def connection_failure(msg):
        """ INTERNAL: Display a warning string. """
        ui_.warn(msg)
        ui_.warn("It looks like your FCP host or port might be wrong.\n")
        ui_.warn("Set them with --fcphost and/or --fcpport and try again.\n")

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
        ui_.warn("Can't write to temp dir: %s\n" % tmp)
        return

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
            return

        ui_.status("Looks good.\nGenerating a default private key...\n")

        # Hmmm... this waits on a socket. Will an ioerror cause an abort?
        # Lazy, but I've never seen this call fail except for IO reasons.
        client = FCPClient(connection)
        client.message_callback = lambda x, y:None # Disable chatty default.
        default_private_key = client.generate_ssk()[1]['InsertURI']

    except FCPError:
        # Protocol error.
        connection_failure("\nMaybe that's not an FCP server?\n")
        return

    except socket.error: # Not an IOError until 2.6.
        # Horked.
        connection_failure("\nSocket level error.\n")
        return

    except IOError:
        # Horked.
        connection_failure("\nSocket level error.\n")
        return

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

""" % (host, port, tmp, cfg_file, default_private_key))

