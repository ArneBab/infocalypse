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

# Classes for inserting to or updating from freenet
# REDFLAG: better name.

import os
import random
import time

from fcpclient import get_ssk_for_usk_version, get_usk_for_usk_version, \
     is_usk, is_ssk, is_usk_file, get_version, get_negative_usk
from fcpconnection import SUCCESS_MSGS
from fcpmessage import GET_DEF, PUT_FILE_DEF, GET_REQUEST_URI_DEF

from requestqueue import RequestQueue

from chk import clear_control_bytes
from bundlecache import make_temp_file
from graph import INSERT_NORMAL, INSERT_PADDED, INSERT_SALTED_METADATA, \
     minimal_update_graph, graph_to_string, \
     FREENET_BLOCK_LEN, has_version, pull_bundle, parse_graph, hex_version

from topkey import bytes_to_top_key_tuple, top_key_tuple_to_bytes

from statemachine import StatefulRequest, RequestQueueState, StateMachine, \
     Quiescent, Canceling, RetryingRequestList, CandidateRequest, \
     require_state, delete_client_file

from insertingbundles import InsertingBundles
from requestingbundles import RequestingBundles

HG_MIME_TYPE = 'application/mercurial-bundle'
HG_MIME_TYPE_FMT = HG_MIME_TYPE + ';%i'

METADATA_MARKER = HG_MIME_TYPE + ';'
PAD_BYTE = '\xff'

# Hmmm... do better?
# IIF ends with .R1 second ssk ends with .R0.
# Makes it easy for paranoid people to disable redundant
# top key fetching. ie. just request *R0 instead of *R1.
# Also could intuitively be expanded to higher levels of
# redundancy.
def make_redundant_ssk(usk, version):
    """ Returns a redundant ssk pair for the USK version IFF the file
        part of usk ends with '.R1', otherwise a single
        ssk for the usk specified version. """
    ssk = get_ssk_for_usk_version(usk, version)
    fields = ssk.split('-')
    if not fields[-2].endswith('.R1'):
        return (ssk, )
    #print "make_redundant_ssk -- is redundant"
    fields[-2] = fields[-2][:-2] + 'R0'
    return (ssk, '-'.join(fields))

# For search
def make_search_uris(uri):
    """ Returns a redundant USK pair if the file part of uri ends
        with '.R1', a tuple containing only uri. """
    if not is_usk_file(uri):
        return (uri,)
    fields = uri.split('/')
    if not fields[-2].endswith('.R1'):
        return (uri, )
    #print "make_search_uris -- is redundant"
    fields[-2] = fields[-2][:-2] + 'R0'
    return (uri, '/'.join(fields))

# For insert
def make_insert_uris(uri):
    """ Returns a possibly redundant insert uri tuple.
        NOTE: This increments the version by 1 if uri is a USK.
    """
    if uri == 'CHK@':
        return (uri,)
    assert is_usk_file(uri)
    version = get_version(uri)
    # REDFLAG: does index increment really belong here?
    return make_redundant_ssk(uri, version + 1)

def ssk_to_usk(ssk):
    """ Convert an SSK for a file USK back into a file USK. """
    fields = ssk.split('-')
    end = '/'.join(fields[-2:])
    fields = fields[:-2] + [end, ]
    return 'USK' + '-'.join(fields)[3:]

class UpdateContext(dict):
    """ A class to hold inter-state data used while the state machine is
        running. """

    def __init__(self, parent):
        dict.__init__(self)

        # Parent state machine.
        self.parent = parent

        # Merurial state
        self.repo = None
        self.ui_ = None
        self.bundle_cache = None

        # Orphaned request handling hmmm...
        self.orphaned = {}

        # UpdateGraph instance.
        self.graph = None

        # If this is True states can use the results of index searches on the
        # public key to update the private key.
        self['IS_KEYPAIR'] = False

        self['TARGET_VERSION'] = None
        self['INSERT_URI'] = 'CHK@'
        self['REQUEST_URI'] = None

    def has_version(self, version):
        """ Returns True if version is already in the hg repository,
            False otherwise. """
        return has_version(self.repo, version)

    def pull(self, file_name):
        """ Pulls an hg bundle file into the local repository. """
        self.ui_.pushbuffer() # Hmmm.. add param to make this optional?
        try:
            pull_bundle(self.repo, self.ui_, file_name)
        finally:
            self.ui_.popbuffer()

    def set_cancel_time(self, request):
        """ Sets the timeout on a QueueableRequest. """
        request.cancel_time_secs = time.time() \
                                   + self.parent.params['CANCEL_TIME_SECS']
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
        request.in_params.fcp_params['URI'] = uri
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
        request.in_params.fcp_params['URI'] = 'CHK@'
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
            if old_salt != '0':
                raise Exception("Unexpected salt byte: %s" % old_salt)

            twiddled_bytes = raw_bytes[:salted_pos] + '1' \
                             + raw_bytes[salted_pos + 1:]
            assert len(raw_bytes) == len(twiddled_bytes)

            request.in_params.send_data = twiddled_bytes
            self.set_cancel_time(request)
            return request

        assert kind == INSERT_NORMAL or kind == INSERT_PADDED
        pad = (kind == INSERT_PADDED)
        #print "make_edge_insert_request -- from disk: pad"

        tmp_file, mime_type = self._get_bundle(edge, pad)
        request.in_params.file_name = tmp_file
        request.in_params.send_data = True
        if not mime_type is None:
            request.in_params.fcp_params['Metadata.ContentType'] = mime_type
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
                                                          edge[:2],
                                                          tmp_file)
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


class CleaningUp(Canceling):
    """ Cancel all pending requests including orphaned ones and wait
        for them to finish. """

    def __init__(self, parent, name, finished_state):
        Canceling.__init__(self, parent, name, finished_state)

    def enter(self, from_state):
        """ Override Cancel implementation to grab all orphaned requests."""
        self.parent.ctx.orphan_requests(from_state)
        self.pending.update(self.parent.ctx.orphaned)
        self.parent.ctx.orphaned.clear()
        # Hmmm... should be ok to recancel already canceled requests.
        for request in self.pending.values():
            self.parent.runner.cancel_request(request)
        if len(self.pending) == 0:
            self.parent.transition(self.finished_state)

# Uses:
# Inserting Graph -- insert 2 chks
# Inserting URI -- insert up to 2 keys
# Requesting URI -- request up to 2 keys
# Requesting Graph -- request up to 2

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
        request.in_params.fcp_params['URI'] = candidate[0]
        if candidate[2]:
            # Insert from raw data.
            request.in_params.definition = PUT_FILE_DEF
            if not candidate[4] is None:
                mime_type = candidate[4]
                request.in_params.fcp_params['Metadata.ContentType'] = mime_type
            request.in_params.send_data = candidate[3]
        else:
            # Request data
            request.in_params.definition = GET_DEF
            request.in_params.fcp_params['MaxSize'] = FREENET_BLOCK_LEN
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
            self.parent.ctx.ui_.status("--- Updated Graph ---\n")
            self.parent.ctx.ui_.status(graph_to_string(self.parent.ctx.graph)
                                   + '\n')

        # Create minimal graph that will fit in a 32k block.
        self.working_graph = minimal_update_graph(self.parent.ctx.graph,
                                                  31 * 1024, graph_to_string)

        if self.parent.params.get('DUMP_GRAPH', False):
            self.parent.ctx.ui_.status("--- Minimal Graph ---\n")
            self.parent.ctx.ui_.status(graph_to_string(minimal_update_graph(
                self.working_graph,
                31 * 1024, graph_to_string)) + '\n---\n')

        # Make sure the string rep is small enough!
        graph_bytes = graph_to_string(self.working_graph)
        assert len(graph_bytes) < 31 * 1024

        # Insert the graph twice for redundancy
        self.queue(['CHK@', 0, True, '#A\n' + graph_bytes, None, None])
        self.queue(['CHK@', 0, True, '#B\n' + graph_bytes, None, None])
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

    def get_top_key_tuple(self):
        """ Get the python rep of the data required to insert a new URI
            with the updated graph CHK(s). """
        graph = self.parent.ctx.graph
        assert not graph is None
        return ((self.get_result(0)[1]['URI'],
                 self.get_result(1)[1]['URI']),
                get_top_key_updates(graph))

def get_top_key_updates(graph):
    """ Returns the update tuples needed to build the top key."""

    graph.rep_invariant()

    edges = graph.get_top_key_edges()

    coalesced_edges = []
    ordinals = {}
    for edge in edges:
        assert edge[2] >= 0 and edge[2] < 2
        assert edge[2] == 0 or (edge[0], edge[1], 0) in edges
        ordinal = ordinals.get(edge[:2])
        if ordinal is None:
            ordinal = 0
            coalesced_edges.append(edge[:2])
        ordinals[edge[:2]] = max(ordinal,  edge[2])

    ret = []
    for edge in coalesced_edges:
        parent_rev = graph.index_table[edge[0]][1]
        latest_rev = graph.index_table[edge[1]][1]
        length = graph.get_length(edge)
        assert len(graph.edge_table[edge][1:]) > 0

        #(length, parent_rev, latest_rev, (CHK, ...))
        update = (length, parent_rev, latest_rev,
                  graph.edge_table[edge][1:])
        ret.append(update)

    return ret

class InsertingUri(StaticRequestList):
    """ A state to insert the top level URI for an Infocalypse repository
        into Freenet."""
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                             failure_state)

    def enter(self, from_state):
        """ Implementation of State virtual.

            This creates the binary rep for the top level key
            data and starts inserting it into Freenet.
        """
        if not hasattr(from_state, 'get_top_key_tuple'):
            raise Exception("Illegal Transition from: %s" % from_state.name)

        top_key_tuple = from_state.get_top_key_tuple()

        salt = {0:0x00, 1:0xff} # grrr.... less code.
        insert_uris = make_insert_uris(self.parent.ctx['INSERT_URI'])
        assert len(insert_uris) < 3
        for index, uri in enumerate(insert_uris):
            if self.parent.params.get('DUMP_URIS', False):
                self.parent.ctx.ui_.status("INSERT_URI: %s\n" % uri)
            self.queue([uri, 0, True,
                        top_key_tuple_to_bytes(top_key_tuple, salt[index]),
                        None, None])
        self.required_successes = len(insert_uris)

    def leave(self, to_state):
        """ Implementation of State virtual. """
        if to_state.name == self.success_state:
            # Hmmm... what about chks?
            # Update the index in the insert_uri on success
            if is_usk(self.parent.ctx['INSERT_URI']):
                version = get_version(self.parent.ctx['INSERT_URI']) + 1
                self.parent.ctx['INSERT_URI'] = (
                    get_usk_for_usk_version(self.parent.ctx['INSERT_URI'],
                                            version))
                if self.parent.params.get('DUMP_URIS', False):
                    self.parent.ctx.ui_.status(("INSERT UPDATED INSERT "
                                               + "URI:\n%s\n")
                                               % self.parent.ctx['INSERT_URI'])
    def get_request_uris(self):
        """ Return the inserted request uri(s). """
        ret = []
        was_usk = is_usk_file(self.parent.ctx['INSERT_URI'])
        for candidate in self.ordered:
            uri = candidate[5][1]['URI']
            if is_ssk(uri) and was_usk:
                uri = ssk_to_usk(uri)
            ret.append(uri)
        return ret

class RequestingUri(StaticRequestList):
    """ A state to request the top level URI for an Infocalypse
        repository. """
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                                   failure_state)
        self.try_all = True # Hmmmm...

    def enter(self, dummy):
        """ Implementation of State virtual. """
        #require_state(from_state, QUIESCENT)

        #print "REQUEST_URI:"
        #print self.parent.ctx['REQUEST_URI']

        request_uri = self.parent.ctx['REQUEST_URI']
        if (is_usk(request_uri) and
            self.parent.params.get('AGGRESSIVE_SEARCH', False)):
            request_uri = get_negative_usk(request_uri)

        request_uris = make_search_uris(request_uri)
        for uri in request_uris:
            #[uri, tries, is_insert, raw_data, mime_type, last_msg]
            if self.parent.params.get('DUMP_URIS', False):
                self.parent.ctx.ui_.status("REQUEST URI: %s\n" % uri)
            self.queue([uri, 0, False, None, None, None])

        self.required_successes = 1 #len(self.results) # Hmmm fix, but how

        # So we don't implictly favor one by requesting it first.
        random.shuffle(self.current_candidates)

    def leave(self, to_state):
        """ Implementation of State virtual. """
        if to_state.name == self.success_state:
            self.parent.ctx['REQUEST_URI'] = self.get_latest_uri()
            if is_usk(self.parent.ctx['REQUEST_URI']):
                self.parent.ctx.ui_.status("Current USK version: %i\n" %
                                       get_version(self.parent
                                                   .ctx['REQUEST_URI']))

            if (self.parent.ctx['IS_KEYPAIR'] and
                is_usk(self.parent.ctx['REQUEST_URI']) and # lose usk checks?
                is_usk(self.parent.ctx['INSERT_URI'])):
                version = get_version(self.parent.ctx['REQUEST_URI'])
                self.parent.ctx['INSERT_URI'] = (
                    get_usk_for_usk_version(self.parent.ctx['INSERT_URI'],
                                            version))
                #print "SEARCH UPDATED INSERT URI: ", \
                # self.parent.ctx['INSERT_URI']

            # Allow pending requests to run to completion.
            self.parent.ctx.orphan_requests(self)

    def get_top_key_tuple(self):
        """ Get the python rep of the data in the URI. """
        top_key_tuple = None
        for candidate in self.ordered:
            result = candidate[5]
            if result is None or result[0] != 'AllData':
                continue
            top_key_tuple = bytes_to_top_key_tuple(result[2])
            break
        assert not top_key_tuple is None
        return top_key_tuple

    def get_latest_uri(self):
        """ Returns the URI with the version part update if the URI is a USK."""
        max_version = None
        for candidate in self.ordered:
            result = candidate[5]
            if result is None or result[0] != 'AllData':
                continue
            uri = result[1]['URI']
            if not is_usk_file(uri):
                return uri
            max_version = max(max_version, abs(get_version(uri)))
            break

        assert not max_version is None
        # The .R1 URI is queued first.
        assert (len(self.ordered) < 2 or
                self.ordered[0][0].find('.R1') != -1)
        return get_usk_for_usk_version(self.ordered[0][0],
                                       max_version)

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
        if self.msg is None or self.msg[0] != 'PutSuccessful':
            return None
        inverted = self.msg[1]['URI']
        public = inverted[inverted.find('@') + 1: inverted.find('/')]
        return self.insert_uri[:self.insert_uri.find('@') + 1] + public \
               + self.insert_uri[self.insert_uri.find('/'):]

    def enter(self, dummy):
        """ Implementation of State virtual. """
        if self.insert_uri == None:
            self.insert_uri = self.parent.ctx['INSERT_URI']
        assert not self.insert_uri is None
        #print "INVERTING: ", self.insert_uri

    def leave(self, to_state):
        """ Implementation of State virtual.

            Sets the REQUEST_URI in the context on success.
        """
        if to_state.name == self.success_state:
            # Don't overwrite request_uri in the pushing from case.
            if self.parent.ctx['REQUEST_URI'] is None:
                self.parent.ctx['REQUEST_URI'] = self.get_request_uri()

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
                                        'MaxRetries': 1,
                                        'PriorityClass':1,
                                        'UploadFrom':'direct',
                                        'GetCHKOnly':True}
        request.in_params.send_data = '@' * 9
        request.in_params.fcp_params['DataLength'] = (
            len(request.in_params.send_data))
        request.tag = 'only_invert' # Hmmmm...
        self.parent.ctx.set_cancel_time(request)
        return request

    def request_done(self, dummy_client, msg):
        """ Implementation of RequestQueueState virtual. """
        #print "INVERTING DONE:", msg
        self.msg = msg
        if msg[0] == 'PutSuccessful':
            #print "REQUEST_URI: ", self.get_request_uri()
            self.parent.transition(self.success_state)
            return
        self.parent.transition(self.failure_state)

class RequestingGraph(StaticRequestList):
    """ A state to request the update graph for an Infocalypse repository. """
    def __init__(self, parent, name, success_state, failure_state):
        StaticRequestList.__init__(self, parent, name, success_state,
                                   failure_state)

    # REDFLAG: remove this? why aren't I just calling get_top_key_tuple
    # on REQUESTING_URI_4_INSERT???
    def get_top_key_tuple(self):
        """ Returns the Python rep of the data in the request uri. """
        results = [candidate[5] for candidate in
                   self.parent.get_state(REQUESTING_URI_4_INSERT).ordered]
        top_key_tuple = None
        for result in results:
            if result is None or result[0] != 'AllData':
                continue
            top_key_tuple = bytes_to_top_key_tuple(result[2])
            break
        assert not top_key_tuple is None
        return top_key_tuple

    def enter(self, from_state):
        """ Implementation of State virtual. """
        require_state(from_state, REQUESTING_URI_4_INSERT)

        top_key_tuple = self.get_top_key_tuple()
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
                if not result is None and result[0] == 'AllData':
                    graph = parse_graph(result[2])
                    break
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
QUIESCENT = 'QUIESCENT'

# Get the request_uri from the insert_uri
INVERTING_URI = 'INVERTING_URI'

# Get the request_uri from the insert_uri, and start inserting
INVERTING_URI_4_INSERT = 'INVERTING_URI_4_INSERT'

# Used to lookup graph.
REQUESTING_URI_4_INSERT = 'REQUESTING_URI_4_INSERT'

# Read the graph out of freenet.
REQUESTING_GRAPH = 'REQUESTING_GRAPH'

# Wait for bundles to insert, handle metadata salting.
INSERTING_BUNDLES = 'INSERTING_BUNDLES'
# Wait for graphs to insert.
INSERTING_GRAPH = 'INSERTING_GRAPH'
# Wait for ssks to insert
INSERTING_URI = 'INSERTING_URI'
# Wait for pending requests to finish
CANCELING = 'CANCELING'
FAILING = 'FAILING'
FINISHING = 'FINISHING'

REQUESTING_URI = 'REQUESTING_URI'
REQUESTING_BUNDLES = 'REQUESTING_BUNDLES'
REQUESTING_URI_4_COPY = 'REQUESTING_URI_4_COPY'

class UpdateStateMachine(RequestQueue, StateMachine):
    """ A StateMachine implementaion to create, push to and pull from
        Infocalypse repositories. """

    def __init__(self, runner, repo, ui_, bundle_cache):
        RequestQueue.__init__(self, runner)
        StateMachine.__init__(self)

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

        self.ctx = UpdateContext(self)
        self.ctx.repo = repo
        self.ctx.ui_ = ui_
        self.ctx.bundle_cache = bundle_cache

        runner.add_queue(self)

    def reset(self):
        """ StateMachine override. """
        StateMachine.reset(self)

        ctx = UpdateContext(self)
        ctx.repo = self.ctx.repo
        ctx.ui_ = self.ctx.ui_
        ctx.bundle_cache = self.ctx.bundle_cache
        if len(self.ctx.orphaned) > 0:
            print "BUG?: Abandoning orphaned requests."
            self.ctx.orphaned.clear()

        self.ctx = ctx

    def start_inserting(self, graph, to_version, insert_uri='CHK@'):
        """ Start and insert of the graph and any required new edge CHKs
            to the insert URI. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = graph
        self.ctx['TARGET_VERSION'] = to_version
        self.ctx['INSERT_URI'] = insert_uri
        self.transition(INSERTING_BUNDLES)

    # Update a repo USK.
    # REDFLAG: later, keys_match=False arg
    def start_pushing(self, insert_uri, to_version='tip', request_uri=None,
                      is_keypair=False):

        """ Start pushing local changes up to to_version to an existing
             Infocalypse repository. """

        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None
        self.ctx['INSERT_URI'] = insert_uri
        self.ctx['REQUEST_URI'] = request_uri
        # Hmmmm... better exception if to_version isn't in the repo?
        self.ctx['TARGET_VERSION'] = hex_version(self.ctx.repo, to_version)
        if request_uri is None:
            self.ctx['IS_KEYPAIR'] = True
            self.transition(INVERTING_URI_4_INSERT)
        else:
            self.ctx['IS_KEYPAIR'] = is_keypair
            self.transition(REQUESTING_URI_4_INSERT)

    # Pull from a repo USK.
    def start_pulling(self, request_uri):
        """ Start pulling changes from an Infocalypse repository URI
            in Freenet into the local hg repository. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None
        self.ctx['REQUEST_URI'] = request_uri
        self.transition(REQUESTING_URI)


    def start_copying(self, from_uri, to_insert_uri):
        """ Start pulling changes from an Infocalypse repository URI
            in Freenet into the local hg repository. """
        self.require_state(QUIESCENT)
        self.reset()
        self.ctx.graph = None

        assert not from_uri is None
        assert not to_insert_uri is None

        self.ctx['REQUEST_URI'] = from_uri
        self.ctx['INSERT_URI'] = to_insert_uri
        self.ctx['IS_KEYPAIR'] = False
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

# REDFLAG: fix orphan handling to use special state iff it is the current state.
# REDFLAG: rationalize. writing updated state into ctx vs.
# leaving it in state instances
# REDFLAG: audit. is_usk vs. is_usk_file
