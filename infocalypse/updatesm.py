""" Classes to asynchronously create, push and pull Infocalypse
    Freenet repositories.

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
import random
import time

from .fcpclient import get_ssk_for_usk_version, get_usk_for_usk_version, \
     is_usk, is_ssk, is_usk_file, get_version, get_negative_usk, \
     make_search_uris, make_frozen_uris, ssk_to_usk

from .fcpconnection import SUCCESS_MSGS
from .fcpmessage import GET_DEF, PUT_FILE_DEF, GET_REQUEST_URI_DEF

from .requestqueue import RequestQueue

from .chk import clear_control_bytes
from .bundlecache import make_temp_file, BundleException
from .graph import INSERT_NORMAL, INSERT_PADDED, INSERT_SALTED_METADATA, \
     INSERT_HUGE, FREENET_BLOCK_LEN, has_version, \
     pull_bundle, hex_version
from .graphutil import minimal_graph, graph_to_string, parse_graph
from .choose import get_top_key_updates

from .statemachine import StatefulRequest, RequestQueueState, StateMachine, \
     Quiescent, Canceling, RetryingRequestList, CandidateRequest, \
     DecisionState, RunningSingleRequest, require_state, delete_client_file

from .insertingbundles import InsertingBundles
from .requestingbundles import RequestingBundles

from . import topkey

HG_MIME_TYPE = b'application/mercurial-bundle'
HG_MIME_TYPE_FMT = HG_MIME_TYPE + b'_%i'

METADATA_MARKER = HG_MIME_TYPE + b'_'
PAD_BYTE = b'\xff'

MAX_SSK_LEN = 1024

class UpdateContextBase(dict):
    """ A class to hold inter-state data used while the state machine is
        running. """

    def __init__(self, parent):
        dict.__init__(self)

        # Parent state machine.
        self.parent = parent

        # Merurial state
        self.ui_ = None
        self.repo = None
        self.bundle_cache = None

        # Orphaned request handling hmmm...
        self.orphaned = {}

        # If this is True states can use the results of index searches on the
        # public key to update the private key.
        self[b'IS_KEYPAIR'] = False

        self[b'INSERT_URI'] = b'CHK@'
        self[b'REQUEST_URI'] = None

    def set_cancel_time(self, request):
        """ Sets the timeout on a QueueableRequest. """
        request.cancel_time_secs = time.time() \
                                   + self.parent.params['CANCEL_TIME_SECS']

    def orphan_requests(self, from_state):
        """ Give away requests that should be allowed to keep running. """
        if not hasattr(from_state, 'pending') or len(from_state.pending) == 0:
            return

        for tag in from_state.pending:
            request = from_state.pending[tag]
            request.tag = "orphaned_%s_%s" % (str(request.tag), from_state.name)
            assert not request.tag in self.orphaned
            self.orphaned[request.tag] = request
        from_state.pending.clear()


class UpdateContext(UpdateContextBase):
    """ A class to hold inter-state data used while the state machine is
        running. """

    def __init__(self, parent):
        UpdateContextBase.__init__(self, parent)

        self.graph = None
        self[b'TARGET_VERSIONS'] = None

    def has_versions(self, versions):
        """ Returns True if all versions are already in the hg repository,
            False otherwise. """
        if versions is None:
            return False # Allowed.

        assert (type(versions) == type((0, )) or
                type(versions) == type([0, ]))
        assert len(versions) > 0
        for version in versions:
            if not has_version(self.repo, version):
                return False
        return True

    def pull(self, file_name):
        """ Pulls an hg bundle file into the local repository. """
        self.ui_.pushbuffer() # Hmmm.. add param to make this optional?
        try:
            pull_bundle(self.repo, self.ui_, file_name)
        finally:
            self.ui_.popbuffer()

    # REDFLAG: get rid of tag arg?
    def make_splitfile_metadata_request(self, edge, tag):
        """ Makes a StatefulRequest for the Freenet metadata for the
            CHK corresponding to an edge in the update graph.

            Helper function used by InsertingBundles state.
        """
        request = StatefulRequest(self.parent)
        request.tag = tag
        # TRICKY: Clear control bytes to get the raw CHK contents,
        # disabling Freenet metadata handling.
        uri = clear_control_bytes(self.parent.ctx.graph.get_chk(edge))
        request.in_params.definition = GET_DEF
        request.in_params.fcp_params = self.parent.params.copy()
        request.in_params.fcp_params[b'URI'] = uri
        self.set_cancel_time(request)
        return request

    # From file (via bundle_cache).
    def make_edge_insert_request(self, edge, tag, salted_metadata_cache):
        """ Makes a StatefuleRequest to insert the hg bundle
            corresponding to an edge in the update graph.

            Helper function used by InsertingBundles state.
        """
        request = StatefulRequest(self.parent)
        request.tag = tag
        request.in_params.definition = PUT_FILE_DEF
        request.in_params.fcp_params = self.parent.params.copy()
        request.in_params.fcp_params[b'URI'] = b'CHK@'
        kind = self.graph.insert_type(edge)
        if kind == INSERT_SALTED_METADATA:
            #print "make_edge_insert_request -- salted"
            assert edge[2] == 1
            raw_bytes = salted_metadata_cache[(edge[0], edge[1], 0)]
            pos = raw_bytes.find(METADATA_MARKER)
            if pos == -1 or len(raw_bytes) < pos + len(METADATA_MARKER) + 1:
                raise Exception("Couldn't read marker string.")

            salted_pos = pos + len(METADATA_MARKER)
            old_salt = raw_bytes[salted_pos]
            if old_salt != b'0'[0]:
                raise Exception("Unexpected salt byte: %s" % old_salt)

            twiddled_bytes = raw_bytes[:salted_pos] + b'1' \
                             + raw_bytes[salted_pos + 1:]
            assert len(raw_bytes) == len(twiddled_bytes)

            request.in_params.send_data = twiddled_bytes
            self.set_cancel_time(request)
            return request

        assert (kind == INSERT_NORMAL or kind == INSERT_PADDED or
                kind == INSERT_HUGE)
        pad = (kind == INSERT_PADDED)
        #print "make_edge_insert_request -- from disk: pad"

        tmp_file, mime_type = self._get_bundle(edge, pad)
        request.in_params.file_name = tmp_file
        request.in_params.send_data = True
        if not mime_type is None:
            request.in_params.fcp_params[b'Metadata.ContentType'] = mime_type
        self.set_cancel_time(request)
        return request

    def _get_bundle(self, edge, pad):
        """ Returns a (temp_file, mime_type) tuple for the hg bundle
            file corresponding to edge. """
        original_len = self.graph.get_length(edge)
        expected_len = original_len
        if pad:
            expected_len += 1
        # Hmmmm... misuse of bundle cache dir?
        tmp_file = make_temp_file(self.parent.ctx.bundle_cache.base_dir)
        raised = False
        try:
            bundle = self.parent.ctx.bundle_cache.make_bundle(self.graph,
                                                              self.parent.ctx.
                                                              version_table,
                                                              edge[:2],
                                                              tmp_file)

            if bundle[0] != original_len:
                raise BundleException("Wrong size. Expected: %i. Got: %i"
                                      % (original_len, bundle[0]))
            assert bundle[0] == original_len
            if pad:
                out_file = open(tmp_file, 'ab')
                try:
                    out_file.seek(0, os.SEEK_END)
                    out_file.write(PAD_BYTE)
                finally:
                    out_file.close()

            assert expected_len == os.path.getsize(tmp_file)
            raised = False
        finally:
            if raised and os.path.exists(tmp_file):
                os.remove(tmp_file)

        if expected_len <= FREENET_BLOCK_LEN:
            mime_type = None
        else:
            assert edge[2] > -1 and edge[2] < 2
            mime_type = HG_MIME_TYPE_FMT % edge[2]

        return (tmp_file, mime_type)



class CleaningUp(Canceling):
    """ Cancel all pending requests including orphaned ones and wait
        for them to finish. """

    def __init__(self, parent, name, finished_state):
        Canceling.__init__(self, parent, name, finished_state)

    def enter(self, from_state):
        """ Override Cancel implementation to grab all orphaned requests."""
        # print(from_state.__dict__)
        self.parent.ctx.orphan_requests(from_state)
        self.pending.update(self.parent.ctx.orphaned)
        self.parent.ctx.orphaned.clear()
        # Hmmm... should be ok to recancel already canceled requests.
        for request in list(self.pending.values()):
            self.parent.runner.cancel_request(request)
        if len(self.pending) == 0:
            self.parent.transition(self.finished_state)

# candidate is:
#[uri, tries, is_insert, raw_data, mime_type, last_msg]
class StaticRequestList(RetryingRequestList):
    """ A base class for states which insert or fetch static lists
        of keys/to from Freenet.

        Candidates are tuples of the form:
        [uri, tries, is_insert, raw_data, mime_type, last_msg]
    """
    def __init__(self, parent, name, success_state, failure_state):
        RetryingRequestList.__init__(self, parent, name)
        self.success_state = success_state
        self.failure_state = failure_state
        self.ordered = [] # i.e. so you can deref candidates in order queued
        self.required_successes = 0
        # If this is True attemps all candidates before success.
        self.try_all = False

    def reset(self):
        """ Implementation of State virtual. """
        self.ordered = []
        self.required_successes = 0
        self.try_all = False
        RetryingRequestList.reset(self)

    def queue(self, candidate):
        """ Enqueue a new request. """
        #[uri, tries, is_insert, raw_data, mime_type, last_msg]
        assert candidate[1] == 0
        assert candidate[2] == True or candidate[2] == False
        assert candidate[5] == None
        self.current_candidates.insert(0, candidate)
        self.ordered.append(candidate)

    def should_retry(self, dummy_client, dummy_msg, candidate):
        """ Returns True if the request candidate should be retried,
            False otherwise. """
        # REDFLAG: rationalize parameter names
        # ATL == Above the Line
        max_retries = self.parent.params.get('MAX_ATL_RETRIES', 0)
        return candidate[1] > max_retries + 1

    # Override to provide better tags.
    # tags MUST uniquely map to candidates.
    # REDFLAG: O(n)
    def get_tag(self, candidate):
        """ Return a unique tag correspoinding the request candidate. """
        return self.ordered.index(candidate)

    def get_result(self, index):
        """ Returns the final FCP message for a request candidate or
            None if none is available. """
        return self.ordered[index][5]

    def candidate_done(self, client, msg, candidate):
        """ Implementation of RetryingRequestList virtual. """
        # Add candidate to next_candidates here to retry.
        # Add candidate to finished_candidates here if done.
        # Commented out to avoid pylint R0922
        #raise NotImplementedError()
        candidate[5] = msg
        if msg[0] in SUCCESS_MSGS:
            self.required_successes -= 1
        elif self.should_retry(client, msg, candidate):
            self.next_candidates.insert(0, candidate)
            return

        self.finished_candidates.append(candidate)
        if self.required_successes <= 0:
            if self.try_all:
                for candidate in self.ordered:
                    if candidate[5] is None:
                        # Wait for all to be tried
                        return
            self.parent.transition(self.success_state)
            return

        if self.is_stalled():
            self.parent.transition(self.failure_state)

    # Override for bigger data. This:
    # 0) Keeps all data in RAM
    # 1) Limits requests to 32K
    #[uri, tries, is_insert, raw_data, mime_type, last_msg]
    def make_request(self, candidate):
        """ Implementation of RetryingRequestList virtual. """
        request = CandidateRequest(self.parent)
        request.tag = self.get_tag(candidate)
        request.candidate = candidate
        request.in_params.fcp_params = self.parent.params.copy()
        request.in_params.fcp_params[b'URI'] = candidate[0]
        if candidate[2]:
            # Insert from raw data.
            request.in_params.definition = PUT_FILE_DEF
            if not candidate[4] is None:
                mime_type = candidate[4]
                request.in_params.fcp_params[b'Metadata.ContentType'] = mime_type
            request.in_params.send_data = candidate[3]
        else:
            # Request data
            request.in_params.definition = GET_DEF
            request.in_params.fcp_params[b'MaxSize'] = FREENET_BLOCK_LEN
            request.in_params.allowed_redirects = (
                self.parent.params.get('ALLOWED_REDIRECTS', 5))
        # Hmmmm...
        self.parent.ctx.set_cancel_time(request)
        candidate[1] += 1
        return request

class InsertingGraph(StaticRequestList):
    """ A state to insert the Infocalypse update graph into Freenet. """
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name,
                                   success_state, failure_state)
        self.working_graph = None

    def enter(self, from_state):
        """ Implementation of State virtual.

            This computes the minimal graph that will fit in a 32k
            block from the graph in the context and inserts it
            into two different Freenet CHK's.  Different headers
            are added before the graph data to get different
            CHKs.
        """
        require_state(from_state, INSERTING_BUNDLES)

        if self.parent.params.get('DUMP_GRAPH', False):
            self.parent.ctx.ui_.status(b"--- Updated Graph ---\n")
            self.parent.ctx.ui_.status(graph_to_string(self.parent.ctx.graph)
                                   + b'\n')

        # Create minimal graph that will fit in a 32k block.
        assert not self.parent.ctx.version_table is None
        self.working_graph = minimal_graph(self.parent.ctx.graph,
                                           self.parent.ctx.repo,
                                           self.parent.ctx.version_table,
                                           31*1024)
        if self.parent.params.get('DUMP_GRAPH', False):
            self.parent.ctx.ui_.status(b"--- Minimal Graph ---\n")
            self.parent.ctx.ui_.status(graph_to_string(self.working_graph)
                                       + b'\n---\n')

        # Make sure the string rep is small enough!
        graph_bytes = graph_to_string(self.working_graph)
        assert len(graph_bytes) <= 31 * 1024

        # Insert the graph twice for redundancy
        self.queue([b'CHK@', 0, True, b'#A\n' + graph_bytes, None, None])
        self.queue([b'CHK@', 0, True, b'#B\n' + graph_bytes, None, None])
        self.required_successes = 2

    def leave(self, to_state):
        """ Implementation of State virtual.

            This updates the graph in the context on success. """
        if to_state.name == self.success_state:
            # Update the graph in the context on success.
            self.parent.ctx.graph = self.working_graph
            self.working_graph = None

    def reset(self):
        """ Implementation of State virtual. """
        StaticRequestList.reset(self)
        self.working_graph = None

    # REDFLAG: cache value? not cheap
    def get_top_key_tuple(self):
        """ Get the python rep of the data required to insert a new URI
            with the updated graph CHK(s). """
        graph = self.parent.ctx.graph
        assert not graph is None

        # REDFLAG: graph redundancy hard coded to 2.
        chks = (self.get_result(0)[1][b'URI'], self.get_result(1)[1][b'URI'])

        # Slow.
        updates = get_top_key_updates(graph, self.parent.ctx.repo)

        # Head revs are more important because they allow us to
        # check whether the local repo is up to date.

        # Walk from the oldest to the newest update discarding
        # base revs, then head revs until the binary rep will
        # fit in an ssk.
        index = len(updates) - 1
        zorch_base = True
        while (len(topkey.top_key_tuple_to_bytes((chks, updates))) >= MAX_SSK_LEN
               and index >= 0):
            victim = list(updates[index])
            # djk20090628 -- There was a bad b_ug here until c47cb6a56d80 which
            #                caused revs not to be discarded. This resulted in
            #                topkey data which was larger than expected.
            #
            # Discard versions
            victim[1 + int(zorch_base)] = victim[1 + int(zorch_base)][:1]
            victim[4 + int(zorch_base)] = False
            updates[index] = tuple(victim)
            if not zorch_base:
                zorch_base = True
                index -= 1
                continue
            zorch_base = False

        assert len(topkey.top_key_tuple_to_bytes((chks, updates))) < MAX_SSK_LEN

        return (chks, updates)

def should_increment(state):
    """ INTERNAL: Returns True if the insert uri should be incremented,
        False otherwise. """
    level = state.parent.ctx.get(b'REINSERT', 0)
    assert level >= 0 and level <= 5
    return (level < 1 or level > 3) and level != 5

class InsertingUri(StaticRequestList):
    """ A state to insert the top level URI for an Infocalypse repository
        into Freenet."""
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                             failure_state)
        self.topkey_funcs = topkey
        self.cached_top_key_tuple = None

    def enter(self, from_state):
        """ Implementation of State virtual.

            This creates the binary rep for the top level key
            data and starts inserting it into Freenet.
        """
        if not hasattr(from_state, 'get_top_key_tuple'):
            raise Exception("Illegal Transition from: %s" % from_state.name)


        # DCI: Retest non-archive stuff!
        # Cache *before* the possible transition below.
        top_key_tuple = from_state.get_top_key_tuple()
        self.cached_top_key_tuple = top_key_tuple # hmmmm...

        if (self.parent.ctx[b'INSERT_URI'] is None
            and self.parent.ctx.get(b'REINSERT', 0) > 0):
            # Hmmmm... hackery to deal with reinsert w/o insert uri
            self.parent.transition(self.success_state)
            return

        assert not self.parent.ctx[b'INSERT_URI'] is None

        if self.parent.params.get('DUMP_TOP_KEY', False):
            self.topkey_funcs.dump_top_key_tuple(top_key_tuple,
                                                 self.parent.ctx.ui_.status)

        salt = {0:0x00, 1:0xff} # grrr.... less code.
        insert_uris = make_frozen_uris(self.parent.ctx[b'INSERT_URI'],
                                       should_increment(self))
        assert len(insert_uris) < 3
        for index, uri in enumerate(insert_uris):
            if self.parent.params.get('DUMP_URIS', False):
                self.parent.ctx.ui_.status(b"INSERT_URI: %s\n" % uri)
            self.queue([uri, 0, True,
                        self.topkey_funcs.top_key_tuple_to_bytes(top_key_tuple,
                                                                 salt[index]),
                        None, None])
        self.required_successes = len(insert_uris)

    def leave(self, to_state):
        """ Implementation of State virtual. """
        if to_state.name == self.success_state:
            # Hmmm... what about chks?
            # Update the index in the insert_uri on success
            if (should_increment(self) and
                is_usk(self.parent.ctx[b'INSERT_URI'])):
                version = get_version(self.parent.ctx[b'INSERT_URI']) + 1
                self.parent.ctx[b'INSERT_URI'] = (
                    get_usk_for_usk_version(self.parent.ctx[b'INSERT_URI'],
                                            version))
                if self.parent.params.get('DUMP_URIS', False):
                    self.parent.ctx.ui_.status((b"INSERT UPDATED INSERT "
                                               + b"URI:\n%s\n")
                                               % self.parent.ctx[b'INSERT_URI'])
    def get_request_uris(self):
        """ Return the inserted request uri(s). """
        ret = []
        was_usk = is_usk_file(self.parent.ctx[b'INSERT_URI'])
        for candidate in self.ordered:
            uri = candidate[5][1][b'URI']
            if is_ssk(uri) and was_usk:
                uri = ssk_to_usk(uri)
            ret.append(uri)
        return ret

    def get_top_key_tuple(self):
        """ Return the top key tuple that it inserted from. """
        return self.cached_top_key_tuple

class RequestingUri(StaticRequestList):
    """ A state to request the top level URI for an Infocalypse
        repository. """
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                                   failure_state)
        self.try_all = True # Hmmmm...

        # hmmmm... Does C module as namespace idiom really belong in Python?
        # Git'r done for now.
        self.topkey_funcs = topkey

    def enter(self, dummy):
        """ Implementation of State virtual. """
        #require_state(from_state, QUIESCENT)
        #print "REQUEST_URI:"
        #print self.parent.ctx[b'REQUEST_URI']

        request_uri = self.parent.ctx[b'REQUEST_URI']
        if (is_usk(request_uri) and
            self.parent.params.get('AGGRESSIVE_SEARCH', False)):
            request_uri = get_negative_usk(request_uri)

        if (is_usk(request_uri) and self.parent.params['NO_SEARCH']):
            request_uris = make_frozen_uris(request_uri, False)
            self.parent.ctx.ui_.status(b"Request URI index searching "
                                       + b"disabled.\n")
        else:
            request_uris = make_search_uris(request_uri)

        for uri in request_uris:
            #[uri, tries, is_insert, raw_data, mime_type, last_msg]
            if self.parent.params.get('DUMP_URIS', False):
                self.parent.ctx.ui_.status(b"REQUEST URI: %s\n" % uri)
            self.queue([uri, 0, False, None, None, None])

        self.required_successes = 1 #len(self.results) # Hmmm fix, but how

        # So we don't implictly favor one by requesting it first.
        random.shuffle(self.current_candidates)

    def leave(self, to_state):
        """ Implementation of State virtual. """
        if to_state.name == self.success_state:
            self.parent.ctx[b'REQUEST_URI'] = self.get_latest_uri()
            if is_usk(self.parent.ctx[b'REQUEST_URI']):
                self.parent.ctx.ui_.status(b"Current USK version: %i\n" %
                                       get_version(self.parent
                                                   .ctx[b'REQUEST_URI']))

            if (self.parent.ctx[b'IS_KEYPAIR'] and
                is_usk(self.parent.ctx[b'REQUEST_URI']) and # lose usk checks?
                is_usk(self.parent.ctx[b'INSERT_URI'])):
                version = get_version(self.parent.ctx[b'REQUEST_URI'])
                self.parent.ctx[b'INSERT_URI'] = (
                    get_usk_for_usk_version(self.parent.ctx[b'INSERT_URI'],
                                            version))
                #print "SEARCH UPDATED INSERT URI: ", \
                # self.parent.ctx[b'INSERT_URI']

            # Allow pending requests to run to completion.
            self.parent.ctx.orphan_requests(self)
            if self.parent.params.get('DUMP_TOP_KEY', False):
                self.topkey_funcs.dump_top_key_tuple(self.get_top_key_tuple(),
                                                     self.parent.ctx.ui_.status)

    def get_top_key_tuple(self):
        """ Get the python rep of the data in the URI. """
        top_key_tuple = None
        for candidate in self.ordered:
            result = candidate[5]
            if result is None or result[0] != b'AllData':
                continue
            top_key_tuple = self.topkey_funcs.bytes_to_top_key_tuple(result[2])[0]
            break
        assert not top_key_tuple is None
        return top_key_tuple

    def get_latest_uri(self):
        """ Returns the URI with the version part update if the URI is a USK."""
        if (is_usk(self.parent.ctx[b'REQUEST_URI']) and
            self.parent.params['NO_SEARCH']):
            return self.parent.ctx[b'REQUEST_URI']

        max_version = None
        for candidate in self.ordered:
            result = candidate[5]
            if result is None or result[0] != b'AllData':
                continue
            uri = result[1][b'URI']
            if not is_usk_file(uri):
                return uri
            
            max_version = max(
                (max_version if max_version is not None else -1),
                abs(get_version(uri)))
            break

        assert not max_version is None
        # The .R1 URI is queued first.
        assert (len(self.ordered) < 2 or
                self.ordered[0][0].find(b'.R1') != -1)
        return get_usk_for_usk_version(self.ordered[0][0],
                                       max_version)
class RequiresGraph(DecisionState):
    """ State which decides whether the graph data is required. """
    def __init__(self, parent, name, yes_state, no_state):
        DecisionState.__init__(self, parent, name)
        self.yes_state = yes_state
        self.no_state = no_state
        self.top_key_tuple = None

    def reset(self):
        """ Implementation of State virtual. """
        self.top_key_tuple = None

    def decide_next_state(self, from_state):
        """ Returns yes_state if the graph is required, no_state otherwise. """
        assert hasattr(from_state, 'get_top_key_tuple')
        self.top_key_tuple = from_state.get_top_key_tuple()
        if not self.top_key_tuple[1][0][5]:
            # The top key data doesn't contain the full head list for
            # the repository in Freenet, so we need to request the
            # graph.
            return self.yes_state
        return self.no_state

    def get_top_key_tuple(self):
        """ Return the cached top key tuple. """
        assert not self.top_key_tuple is None
        return self.top_key_tuple


class InvertingUri(RequestQueueState):
    """ A state to compute the request URI corresponding to a Freenet
        insert URI. """
    def __init__(self, parent, name, success_state, failure_state):
        RequestQueueState.__init__(self, parent, name)
        self.insert_uri = None
        self.queued = False
        self.msg = None
        self.success_state = success_state
        self.failure_state = failure_state
    def get_request_uri(self):
        """ Returns the request URI."""
        if self.msg is None or self.msg[0] != b'PutSuccessful':
            return None
        inverted = self.msg[1][b'URI']
        public = inverted[inverted.find(b'@') + 1: inverted.find(b'/')]
        return self.insert_uri[:self.insert_uri.find(b'@') + 1] + public \
               + self.insert_uri[self.insert_uri.find(b'/'):]

    def enter(self, dummy):
        """ Implementation of State virtual. """
        if self.insert_uri == None:
            self.insert_uri = self.parent.ctx[b'INSERT_URI']
        assert not self.insert_uri is None

    def leave(self, to_state):
        """ Implementation of State virtual.

            Sets the REQUEST_URI in the context on success.
        """
        if to_state.name == self.success_state:
            # Don't overwrite request_uri in the pushing from case.
            if self.parent.ctx[b'REQUEST_URI'] is None:
                self.parent.ctx[b'REQUEST_URI'] = self.get_request_uri()

    def reset(self):
        """ Implementation of State virtual. """
        self.insert_uri = None
        self.queued = False
        self.msg = None
        RequestQueueState.reset(self)

    def next_runnable(self):
        """ Implementation of RequestQueueState virtual. """
        if self.queued:
            return None
        self.queued = True

        uri = self.insert_uri
        if is_usk(uri):
            # Hack to keep freenet from doing a USK search.
            uri = get_ssk_for_usk_version(uri, 0)

        request = StatefulRequest(self.parent)
        request.in_params.definition = GET_REQUEST_URI_DEF
        request.in_params.fcp_params = {'URI': uri,
                                        b'MaxRetries': 1,
                                        b'PriorityClass':1,
                                        b'UploadFrom':b'direct',
                                        b'GetCHKOnly':True}
        request.in_params.send_data = b'@' * 9
        request.in_params.fcp_params[b'DataLength'] = (
            len(request.in_params.send_data))
        request.tag = b'only_invert' # Hmmmm...
        self.parent.ctx.set_cancel_time(request)
        return request

    def request_done(self, dummy_client, msg):
        """ Implementation of RequestQueueState virtual. """
        self.msg = msg
        if msg[0] == b'PutSuccessful':
            self.parent.transition(self.success_state)
            return
        self.parent.transition(self.failure_state)

class RequestingGraph(StaticRequestList):
    """ A state to request the update graph for an Infocalypse repository. """
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                                   failure_state)

    def enter(self, from_state):
        """ Implementation of State virtual. """

        assert hasattr(from_state, "get_top_key_tuple")
        top_key_tuple = from_state.get_top_key_tuple()

        #top_key_tuple = self.get_top_key_tuple() REDFLAG: remove
        #print "TOP_KEY_TUPLE", top_key_tuple
        #[uri, tries, is_insert, raw_data, mime_type, last_msg]
        for uri in top_key_tuple[0]:
            self.queue([uri, 0, False, None, None, None])
        random.shuffle(self.current_candidates)
        self.required_successes = 1

    def leave(self, to_state):
        """ Implementation of State virtual. """
        if to_state.name == self.success_state:
            # Set the graph from the result
            graph = None
            for candidate in self.ordered:
                result = candidate[5]
                if not result is None and result[0] == b'AllData':
                    graph = parse_graph(result[2])

            assert not graph is None

            self.parent.ctx.graph = graph

            # Allow pending requests to run to completion.
            for tag in self.pending:
                request = self.pending[tag]
                request.tag = "orphaned_%s_%s" % (str(request.tag), self.name)
                assert not request.tag in self.parent.ctx.orphaned
                self.parent.ctx.orphaned[request.tag] = request
            self.pending.clear()

# Allow entry into starting
QUIESCENT = b'QUIESCENT'

# Get the request_uri from the insert_uri
INVERTING_URI = b'INVERTING_URI'

# Get the request_uri from the insert_uri, and start inserting
INVERTING_URI_4_INSERT = b'INVERTING_URI_4_INSERT'

# Used to lookup graph.
REQUESTING_URI_4_INSERT = b'REQUESTING_URI_4_INSERT'

# Read the graph out of freenet.
REQUESTING_GRAPH = b'REQUESTING_GRAPH'

# Wait for bundles to insert, handle metadata salting.
INSERTING_BUNDLES = b'INSERTING_BUNDLES'
# Wait for graphs to insert.
INSERTING_GRAPH = b'INSERTING_GRAPH'
# Wait for ssks to insert
INSERTING_URI = b'INSERTING_URI'
# Wait for pending requests to finish
CANCELING = b'CANCELING'
FAILING = b'FAILING'
FINISHING = b'FINISHING'

REQUESTING_URI = b'REQUESTING_URI'
REQUESTING_BUNDLES = b'REQUESTING_BUNDLES'
REQUESTING_URI_4_COPY = b'REQUESTING_URI_4_COPY'

REQUESTING_URI_4_HEADS = b'REQUESTING_URI_4_HEADS'
REQUIRES_GRAPH_4_HEADS  = b'REQUIRES_GRAPH'
REQUESTING_GRAPH_4_HEADS = b'REQUESTING_GRAPH_4_HEADS'

RUNNING_SINGLE_REQUEST = b'RUNNING_SINGLE_REQUEST'
# REDFLAG: DRY out (after merging wiki stuff)
# 1. write state_name(string) func to create state names by inserting them
#    into globals.
# 2. Helper func to add states to states member so you don't have to repeat
#    the name
class UpdateStateMachine(RequestQueue, StateMachine):
    """ A StateMachine implementaion to create, push to and pull from
        Infocalypse repositories. """

    def __init__(self, runner, ctx):
        RequestQueue.__init__(self, runner)
        StateMachine.__init__(self)
        self.ctx = None
        self.set_context(ctx) # Do early. States might depend on ctx.
        self.states = {
            QUIESCENT:Quiescent(self, QUIESCENT),

            # Justing inverting
            INVERTING_URI:InvertingUri(self, INVERTING_URI,
                                       QUIESCENT,
                                       FAILING),

            # Requesting previous graph in order to do insert.
            INVERTING_URI_4_INSERT:InvertingUri(self, INVERTING_URI_4_INSERT,
                                                REQUESTING_URI_4_INSERT,
                                                FAILING),

            REQUESTING_URI_4_INSERT:RequestingUri(self,
                                                  REQUESTING_URI_4_INSERT,
                                                  REQUESTING_GRAPH,
                                                  FAILING),
            REQUESTING_GRAPH:RequestingGraph(self, REQUESTING_GRAPH,
                                             INSERTING_BUNDLES,
                                             FAILING),


            # Inserting
            INSERTING_BUNDLES:InsertingBundles(self,
                                               INSERTING_BUNDLES),
            INSERTING_GRAPH:InsertingGraph(self, INSERTING_GRAPH,
                                           INSERTING_URI,
                                           FAILING),
            INSERTING_URI:InsertingUri(self,INSERTING_URI,
                                       FINISHING,
                                       FAILING),
            CANCELING:CleaningUp(self, CANCELING, QUIESCENT),
            FAILING:CleaningUp(self, FAILING, QUIESCENT),

            # Requesting
            REQUESTING_URI:RequestingUri(self, REQUESTING_URI,
                                         REQUESTING_BUNDLES,
                                         FAILING),

            REQUESTING_BUNDLES:RequestingBundles(self, REQUESTING_BUNDLES,
                                                 FINISHING,
                                                 FAILING),

            FINISHING:CleaningUp(self, FINISHING, QUIESCENT),


            # Requesting head info from freenet
            REQUESTING_URI_4_HEADS:RequestingUri(self, REQUESTING_URI_4_HEADS,
                                                 REQUIRES_GRAPH_4_HEADS,
                                                 FAILING),

            REQUIRES_GRAPH_4_HEADS:RequiresGraph(self, REQUIRES_GRAPH_4_HEADS,
                                                 REQUESTING_GRAPH_4_HEADS,
                                                 FINISHING),

            REQUESTING_GRAPH_4_HEADS:RequestingGraph(self,
                                                     REQUESTING_GRAPH_4_HEADS,
                                                     FINISHING,
                                                     FAILING),

            # Run and arbitrary StatefulRequest.
            RUNNING_SINGLE_REQUEST:RunningSingleRequest(self,
                                                        RUNNING_SINGLE_REQUEST,
                                                        FINISHING,
                                                        FAILING),

            # Copying.
            # This doesn't verify that the graph chk(s) are fetchable.
            REQUESTING_URI_4_COPY:RequestingUri(self, REQUESTING_URI_4_COPY,
                                                INSERTING_URI,
                                                FAILING),

            }

        self.current_state = self.get_state(QUIESCENT)

        self.params = {}
        # Must not change any state!
        self.monitor_callback = lambda parent, client, msg: None

        runner.add_queue(self)

    def set_context(self, new_ctx):
        """ Set the context. """
        self.ctx = new_ctx
        self.ctx.parent = self

    def reset(self):
        """ StateMachine override. """
        StateMachine.reset(self)

        ctx = UpdateContext(self)
        ctx.repo = self.ctx.repo
        ctx.ui_ = self.ctx.ui_
        ctx.bundle_cache = self.ctx.bundle_cache
        if len(self.ctx.orphaned) > 0:
            print("BUG?: Abandoning orphaned requests.")
            self.ctx.orphaned.clear()

        self.ctx = ctx

    def start_inserting(self, graph, to_versions, insert_uri=b'CHK@'):
        """ Start and insert of the graph and any required new edge CHKs
            to the insert URI. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = graph
        self.ctx[b'TARGET_VERSIONS'] = to_versions
        self.ctx[b'INSERT_URI'] = insert_uri
        self.transition(INSERTING_BUNDLES)

    # Update a repo USK.
    # REDFLAG: later, keys_match=False arg
    def start_pushing(self, insert_uri, to_versions=(b'tip',), request_uri=None,
                      is_keypair=False):

        """ Start pushing local changes up to to_version to an existing
             Infocalypse repository. """

        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None
        self.ctx[b'INSERT_URI'] = insert_uri
        self.ctx[b'REQUEST_URI'] = request_uri
        # Hmmmm... better exception if to_version isn't in the repo?
        self.ctx[b'TARGET_VERSIONS'] = tuple([hex_version(self.ctx.repo, ver)
                                             for ver in to_versions])
        if request_uri is None:
            self.ctx[b'IS_KEYPAIR'] = True
            self.transition(INVERTING_URI_4_INSERT)
        else:
            self.ctx[b'IS_KEYPAIR'] = is_keypair
            self.transition(REQUESTING_URI_4_INSERT)

    # Pull from a repo USK.
    def start_pulling(self, request_uri):
        """ Start pulling changes from an Infocalypse repository URI
            in Freenet into the local hg repository. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None
        self.ctx[b'REQUEST_URI'] = request_uri
        self.transition(REQUESTING_URI)

    def start_requesting_heads(self, request_uri):
        """ Start fetching the top key and graph if necessary to retrieve
            the list of the latest heads in Freenet.
        """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None
        self.ctx[b'REQUEST_URI'] = request_uri
        self.transition(REQUESTING_URI_4_HEADS)

    def start_single_request(self, stateful_request):
        """ Run a single StatefulRequest on the state machine.
        """
        assert not stateful_request is None
        assert not stateful_request.in_params is None
        assert not stateful_request.in_params.definition is None
        self.require_state(QUIESCENT)
        self.reset()
        self.get_state(RUNNING_SINGLE_REQUEST).request = stateful_request
        self.transition(RUNNING_SINGLE_REQUEST)

    def start_copying(self, from_uri, to_insert_uri):
        """ Start pulling changes from an Infocalypse repository URI
            in Freenet into the local hg repository. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None

        assert not from_uri is None
        assert not to_insert_uri is None

        self.ctx[b'REQUEST_URI'] = from_uri
        self.ctx[b'INSERT_URI'] = to_insert_uri
        self.ctx[b'IS_KEYPAIR'] = False
        self.transition(REQUESTING_URI_4_COPY)

    # REDFLAG: SSK case untested
    def start_inverting(self, insert_uri):
        """ Start inverting a Freenet URI into it's analogous
            request URI. """
        assert is_usk(insert_uri) or is_ssk(insert_uri)
        self.require_state(QUIESCENT)
        self.reset()
        self.get_state(INVERTING_URI).insert_uri = insert_uri
        self.transition(INVERTING_URI)

    def start_reinserting(self, request_uri, insert_uri=None, is_keypair=False,
                          level = 3):
        """ Start reinserting the repository"""
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx[b'REQUEST_URI'] = request_uri
        self.ctx[b'INSERT_URI'] = insert_uri
        self.ctx[b'IS_KEYPAIR'] = is_keypair
        self.ctx[b'REINSERT'] = level
        # REDFLAG: added hack code to InsertingUri to handle
        # reinsert w/o insert uri?
        # Tradedoff: hacks in states vs. creating extra state
        # instances just to be pedantic...
        self.transition(REQUESTING_URI_4_INSERT)

    # REDFLAG: UNTESTED
    def cancel(self):
        """ Start canceling the current operation. """

        if (self.current_state.name != QUIESCENT and
            self.current_state.name != FAILING):
            self.transition(CANCELING)

    ############################################################
    def handled_orphan(self, client, dummy_msg):
        """ Handle cleanup of requests that aren't owned by any state. """
        if not client.tag in self.ctx.orphaned:
            return False
        if client.is_finished():
            del self.ctx.orphaned[client.tag]
        return True

    def next_runnable(self):
        """ Implementation of RequestQueue virtual. """
        return self.current_state.next_runnable()

    def request_progress(self, client, msg):
        """ Implementation of RequestQueue virtual. """
        self.monitor_callback(self, client, msg)
        if self.handled_orphan(client, msg):
            return
        # Don't let client time out while it's making progress.
        self.ctx.set_cancel_time(client) # Hmmmm
        self.current_state.request_progress(client, msg)

    def request_done(self, client, msg):
        """ Implementation of RequestQueue virtual. """
        try:
            self.monitor_callback(self, client, msg)
            if self.handled_orphan(client, msg):
                return
            self.current_state.request_done(client, msg)
        finally:
            # Clean up all upload and download files.
            delete_client_file(client)

# REDFLAG: rationalize. writing updated state into ctx vs.
# leaving it in state instances
# REDFLAG: audit. is_usk vs. is_usk_file
