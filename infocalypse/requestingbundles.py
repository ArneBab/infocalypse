""" A RequestQueueState for which requests the hg bundle CHKS
    required to update a repository.

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

# REDFLAG: reevaluate on failure?
import os
import random # Hmmm... good enough?

from fcpmessage import GET_DEF

from bundlecache import make_temp_file
from graph import latest_index, \
     FREENET_BLOCK_LEN, chk_to_edge_triple_map, \
     dump_paths, MAX_PATH_LEN, get_heads, canonical_path_itr
from graphutil import parse_graph
from choose import get_update_edges, dump_update_edges, SaltingState

from statemachine import RetryingRequestList, CandidateRequest

from chk import clear_control_bytes

# FUNCTIONAL REQUIREMENTS:
# 0) Update as fast as possible
# 1) Single block fetch alternate keys.
# 2) Choose between primary and alternate keys "fairly"
# 3) transition from no graph to graph case.
# 4) deal with padding hacks
# 5) Optionally disable alternate single block fetching?
# ?6) serialize? Easier to write from scratch?

# What this does:
# 0) Fetches graph(s)
# 1) Fetches early bundles in parallel with graphs
# 2) Fixes up pending requests to graph edges when the graph arrives
# 3) Handles metadata salting for bundle requests
# 4) Keeps track of what requests are required to update and requests them.

# a candidate is a list:
# [CHK, tries, single_block, edge_triple, update_data, msg, is_graph_request]
class RequestingBundles(RetryingRequestList):
    """ A RequestQueueState for which requests the hg bundle CHKS
    required to update a repository. """

    def __init__(self, parent, name, success_state, failure_state):
        RetryingRequestList.__init__(self, parent, name)
        self.success_state = success_state
        self.failure_state = failure_state
        self.top_key_tuple = None # FNA sskdata
        self.freenet_heads = None

    ############################################################
    # State implementation
    ############################################################
    def enter(self, from_state):
        """ Implementation of State virtual. """
        if hasattr(from_state, 'get_top_key_tuple'):
            self._initialize(from_state.get_top_key_tuple())
            return

        self._initialize()
        #self.dump()

    def reset(self):
        """ Implementation of State virtual. """
        #print "reset -- pending: ", len(self.pending)
        self.top_key_tuple = None
        RetryingRequestList.reset(self)

    ############################################################
    # Implementation of RetryingRequestList virtuals
    ############################################################
    def candidate_done(self, client, msg, candidate):
        """ Implementation of RetryingRequestList virtual. """
        # Hmmmm... special case hack code to handle graph.
        if not self._graph_request_done(client, msg, candidate):
            if msg[0] == 'AllData':
                self._handle_success(client, msg, candidate)
            else:
                self._handle_failure(client, msg, candidate)

        # Catch state machine stalls.
        if (self.parent.current_state == self and
            self.is_stalled()):
            self.parent.ctx.ui_.warn("Giving up because the state "
                                     + "machine stalled.\n")
            self.parent.transition(self.failure_state)

    # DONT add to pending. Base class does that.
    def make_request(self, candidate):
        """ Implementation of RetryingRequestList virtual. """
        #print "CANDIDATE: ", candidate
        assert len(candidate) >= 7
        candidate[1] += 1 # Keep track of the number of times it has been tried
        # tag == edge, but what if we don't have an edge yet?
        request = CandidateRequest(self.parent)
        request.in_params.fcp_params = self.parent.params.copy()

        uri = candidate[0]
        if candidate[2]:
            uri = clear_control_bytes(uri)
        request.in_params.fcp_params['URI'] = uri

        request.in_params.definition = GET_DEF
        request.in_params.file_name = (
            make_temp_file(self.parent.ctx.bundle_cache.base_dir))
        self.parent.ctx.set_cancel_time(request)

        # Set tag
        if not candidate[3] is None:
            request.tag = candidate[3] # Edge
        else:
            # REDFLAG: Do better!
            # Some random digit string.
            request.tag = request.in_params.file_name[-12:]

        # Set candidate
        request.candidate = candidate

        #print "make_request --", request.tag, candidate[0]
        # Tags must be unique or we will loose requests!
        assert not request.tag in self.pending

        #request.in_params.fcp_params['MaxSize'] = ???

        return request

    ############################################################
    # DEALING: With partial heads, partial bases?
    # REDFLAG: deal with optional request serialization?
    # REDFLAG: Move
    # ASSUMPTION: Keys are in descenting order of latest_rev.
    # ASSUMPTION: Keys are in order of descending parent rev.
    #
    # Returns index of last update queued.
    # Does gobbledygook to make single block requesting work.
    #
    # REDFLAG: candymachine? look at start_index / last_queued
    def _queue_from_updates(self, candidate_list,
                           start_index, one_full, only_latest=False):
        """ INTERNAL:  Queues an hg bundle CHK request from the
            top key data. """
        updates = self.top_key_tuple[1]

        last_queued = -1
        for index, update in enumerate(updates):
            if index < start_index:
                continue # REDFLAG: do better?
            if not update[4] or not update[5]:
                # Don't attempt to queue updates if we don't know
                # full parent/head info.
                # REDFLAG: remove test code
                print "_queue_from_updates -- bailing out", update[4], update[5]
                break

            if only_latest and update[0] > 5 * FREENET_BLOCK_LEN:
                # Short circuit (-1, top_index) rollup case.
                break

            if only_latest and update[2] != updates[0][2]:
                # Only full updates.
                break

            if not self.parent.ctx.has_versions(update[1]):
                # Only updates we can pull.
                if only_latest:
                    # Don't want big bundles from the canonical path.
                    break
                else:
                    continue

            if self.parent.ctx.has_versions(update[2]):
                # Only updates we need.
                continue

            chks = list(update[3][:])
            full_chk = random.choice(chks)
            chks.remove(full_chk)
            candidate = [full_chk, 0, not one_full, None, update, None, False]
            one_full = True
            candidate_list.insert(0, candidate)

            for chk in chks:
                candidate = [chk, 0, True, None, update, None, False]
                candidate_list.insert(0, candidate)
            last_queued = index
            if index > 1:
                break

        return last_queued


    def _handle_testing_hacks(self):
        """ INTERNAL: Helper function to implement TEST_DISABLE_UPDATES
            and TEST_DISABLE_GRAPH testing params. """
        if self.top_key_tuple is None:
            return
        if self.parent.params.get("TEST_DISABLE_UPDATES", False):
            updates = list(self.top_key_tuple[1])
            for index in range(0, len(updates)):
                update = list(updates[index])
                update[4] = False
                update[5] = False
                updates[index] = tuple(update)
            top = list(self.top_key_tuple)
            top[1] = tuple(updates)
            self.top_key_tuple = tuple(top)
            self.parent.ctx.ui_.warn("TEST_DISABLE_UPDATES == True\n"
                                     + "Disabled updating w/o graph\n")

        if self.parent.params.get("TEST_DISABLE_GRAPH", False):
            top = list(self.top_key_tuple)
            # REDFLAG: Fix post 1208
            #          Using bad keys is a more realistic test but there's
            #          an FCP bug in 1208 that kills the connection on
            #          cancel.  Go back to this when 1209 comes out.
            top[0] = ('CHK@badroutingkeyA55JblbGup0yNSpoDJgVPnL8E5WXoc,'
                      +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8',
                      'CHK@badroutingkeyB55JblbGup0yNSpoDJgVPnL8E5WXoc,'
                      +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8',
                      )
            top[0] = ()
            self.top_key_tuple = tuple(top)
            self.parent.ctx.ui_.warn("TEST_DISABLE_GRAPH == True\n"
                                     + "Disabled graph by removing graph "
                                     + "chks.\n")

    # Hack special case code to add the graph.
    def _initialize(self, top_key_tuple=None):
        """ INTERNAL: Initialize.

            If the graph isn't available yet kick off
            requests for it and also a request for a full
            update if there's one available in the top key data.

            If the graph is available, use it to determine which
            keys to request next.
        """
        self.top_key_tuple = top_key_tuple

        self._handle_testing_hacks()
        ############################################################
        # Hack used to test graph request failure.
        #bad_chk = ('CHK@badroutingkeyA55JblbGup0yNSpoDJgVPnL8E5WXoc,'
        #           +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
        #bad_update = list(top_key_tuple[1][0])
        #bad_update[3] = (bad_chk, )
        #print "old:", top_key_tuple

        #self.top_key_tuple = ((bad_chk,), (bad_update, ))
        #print "new:",  self.top_key_tuple
        ############################################################

        # If we don't have the graph, request it, and update
        # from the data in the top key.
        if self.parent.ctx.graph is None:
            if self.top_key_tuple is None:
                raise Exception("No top key data.")

            updates = self.top_key_tuple[1]
            if updates[0][5]:
                self.freenet_heads = updates[0][2]
                self.parent.ctx.ui_.status('Freenet heads: %s\n' %
                                           ' '.join([ver[:12] for ver in
                                                     updates[0][2]]))

                if self.parent.ctx.has_versions(updates[0][2]):
                    self.parent.ctx.ui_.warn("All remote heads are already "
                                             + "in the local repo.\n")
                    self.parent.transition(self.success_state)
                    return

                # INTENT: Improve throughput for most common update case.
                # If it is possible to update fully in one fetch, queue the
                # (possibly redundant) key(s) BEFORE graph requests.
                latest_queued = self._queue_from_updates(self.
                                                         current_candidates,
                                                         -1, False, True)

                if latest_queued != -1:
                    self.parent.ctx.ui_.status("Full update is possible in a "
                                               + "single FCP fetch. :-)\n")

            else:
                self.parent.ctx.ui_.warn("Couldn't read all Freenet heads from "
                                         + "top key.\n"
                                         + "Dunno if you're up to date :-(\n"
                                         + "Waiting for graph...\n")

                if len(self.top_key_tuple[0]) == 0:
                    self.parent.ctx.ui_.warn("No graph CHKs in top key! "
                                             + "Giving up...\n")
                    self.parent.transition(self.failure_state)
                    return
            # Kick off the fetch(es) for the full update graph.
            # REDFLAG: make a parameter
            parallel_graph_fetch = True
            chks = list(self.top_key_tuple[0][:])
            random.shuffle(chks)
            #chks = [] # Hack to test bootstrapping w/o graph
            for chk in chks:
                candidate = [chk, 0, False, None, None, None, True]
                # insert not append, because this should run AFTER
                # initial single fetch update queued above.
                self.current_candidates.insert(0, candidate)
                if not parallel_graph_fetch:
                    break


            if self.freenet_heads is None:
                # Need to wait for the graph.
                return

            # Queue remaining fetchable keys in the NEXT pass.
            # INTENT:
            # The graph might have a better update path. So we don't try these
            # until we have tried to get the graph.
            self._queue_from_updates(self.next_candidates, latest_queued + 1,
                                    latest_queued == -1, False)
            return

        # Otherwise, use the graph to figure out what keys we need.
        self._reevaluate()

    # REDFLAG: Move
    # Set the graph and fixup all candidates with real edges.
    def _set_graph(self, graph):
        """ INTERNAL: Set the graph and fixup any pending CHK edge
            requests with their edges.  """

        def fixup(edges, candidate_list):
            """ INTERNAL : Helper fixes up CHK->edges. """
            for candidate in candidate_list:
                edge = edges[candidate[0]]
                candidate[3] = edge
                candidate[4] = None

        edges = chk_to_edge_triple_map(graph)

        skip_chks = set([]) # REDFLAG: remove!
        for request in self.pending.values():
            candidate = request.candidate
            if candidate[6]:
                continue
            edge = edges[candidate[0]]
            candidate[3] = edge
            candidate[4] = None
            #print "_set_graph -- fixed up: ", request.tag, edge
            # REDFLAG: why am I keeping state in two places?
            old_tag = request.tag
            request.tag = edge
            del self.pending[old_tag]
            self.pending[request.tag] = request
            skip_chks.add(candidate[0])

        #print "pending.keys(): ", self.pending.keys()

        fixup(edges, self.current_candidates)
        fixup(edges, self.next_candidates)
        fixup(edges, self.finished_candidates)

        assert not self.top_key_tuple is None
        if self.parent.params.get('DUMP_TOP_KEY', False):
            text = "Fixed up top key CHKs:\n"
            for update in self.top_key_tuple[1]:
                for chk in update[3]:
                    if chk in edges:
                        text += "   " + str(edges[chk]) + ":" + chk + "\n"
                    else:
                        text += "   BAD TOP KEY DATA!" + ":" + chk + "\n"
            self.parent.ctx.ui_.status(text)

        all_heads = get_heads(graph)

        assert (self.freenet_heads is None or
                self.freenet_heads == all_heads)
        self.freenet_heads = all_heads
        self.parent.ctx.graph = graph

        self.rep_invariant()

        # REDFLAG: remove testing code
        #kill_prob = 0.00
        #print "BREAKING EDGES: Pkill==", kill_prob
        #print skip_chks
        #break_edges(graph, kill_prob, skip_chks)

        # "fix" (i.e. break) pending good chks.
        # REDFLAG: comment this out too?
        for candidate in self.current_candidates + self.next_candidates:
            if candidate[6]:
                continue
            edge = candidate[3]
            assert not edge is None
            if graph.get_chk(edge).find("badrouting") != -1:
                candidate[0] = graph.get_chk(edge)

        #self.dump()
        self.rep_invariant()

        self.parent.ctx.ui_.status("Got graph. Latest graph index: %i\n" %
                                   graph.latest_index)

    def _handle_graph_failure(self, candidate):
        """ INTERNAL: Handle failed FCP requests for the graph. """
        max_retries = self.parent.params.get('MAX_RETRIES', 1)
        if candidate[1] < max_retries + 1:
            #print "_should_retry -- returned False"
            #return False
            # Append retries immediately. Hmmmm...
            self.current_candidates.append(candidate)
            return

        self.finished_candidates.append(candidate)
        if self.is_stalled():
            # BUG: Kind of. We can update w/o the graph without ever reporting
            # that we couldn't get the graph.
            self.parent.ctx.ui_.warn("Couldn't read graph from Freenet!\n")
            self.parent.transition(self.failure_state)

    def _handle_dump_canonical_paths(self, graph):
        """ INTERNAL: Dump the top 20 canonical paths. """
        if not self.parent.params.get('DUMP_CANONICAL_PATHS', False):
            return

        paths = canonical_path_itr(graph, 0, graph.latest_index,
                                               MAX_PATH_LEN)
        first_paths = []
        # REDFLAG: Magick number
        while len(first_paths) < 20:
            try:
                first_paths.append(paths.next())
            except StopIteration:
                break

        dump_paths(graph,
                   first_paths,
                   "Canonical paths")

    def _graph_request_done(self, client, msg, candidate):
        """ INTERNAL: Handle requests for the graph. """
        #print "CANDIDATE:", candidate
        #print "_graph_request_done -- ", candidate[6]
        if not candidate[6]:
            return False

        if not self.parent.ctx.graph is None:
            self.finished_candidates.append(candidate)
            return True

        if msg[0] == 'AllData':
            in_file = open(client.in_params.file_name, 'rb')
            try:
                data = in_file.read()
                # REDFLAG: log graph?
                if self.parent.params.get('DUMP_GRAPH', False):
                    self.parent.ctx.ui_.status("--- Raw Graph Data ---\n")
                    self.parent.ctx.ui_.status(data)
                    self.parent.ctx.ui_.status("\n---\n")
                graph = parse_graph(data)
                self._handle_dump_canonical_paths(graph)
                self._set_graph(graph)
                assert(not self.freenet_heads is None)
                if self.parent.ctx.has_versions(self.freenet_heads):
                    # Handle case where we are up to date but the heads list
                    # didn't fit in the top key.
                    self.parent.ctx.ui_.status('Freenet heads: %s\n' %
                                           ' '.join([ver[:12] for ver in
                                                     self.freenet_heads]))
                    self.parent.ctx.ui_.warn("All remote heads are already "
                                             + "in the local repo.\n")
                    self.parent.transition(self.success_state)
                    return True
                self._reevaluate()
            finally:
                in_file.close()
            self.finished_candidates.append(candidate)
        else:
            if not self.top_key_tuple is None:
                pending, current, next, finished = self._known_chks()
                all_chks = pending.union(current).union(next).union(finished)

                for chk in self.top_key_tuple[0]:
                    if not chk in all_chks and chk != candidate[0]:
                        # REDFLAG: Test this code path.
                        # Queue the other graph chk.
                        candidate = [chk, 0, False, None, None, None, True]
                        # Run next!
                        #print "QUEUEING OTHER GRAPH CHK"
                        # append retries immediately. Hmmm...
                        self.current_candidates.append(candidate)
                        break


        # Careful, can drive state transition.
        self._handle_graph_failure(candidate)
        return True

    def _force_single_block(self, edge):
        """ INTERNAL: Make sure there is only one non-single-block request
            running for a redundant edge. """
        for candidate in self.current_candidates:
            if candidate[3] == edge and not candidate[2]:
                candidate[2] = True
                # break. paranoia?

        for candidate in self.next_candidates:
            if candidate[3] == edge and not candidate[2]:
                candidate[2] = True
                # break. paranoia?

    # REDFLAG: for now, do parallel multiblock fetches.
    def _handled_multiblock_case(self, candidate):
        """ INTERNAL: Handle requeueing full fetches when we don't have
            the graph yet. """
        if (candidate[2] and self._multiple_block(candidate) and
            self.parent.ctx.graph is None):
            assert not candidate[4] is None
            update = candidate[4]
            # Compare without control bytes, which were cleared.
            target = candidate[0].split(',')[:-1]
            for chk in update[3]:
                if chk.split(',')[:-1] == target:
                    # Reset the CHK because the control bytes were zorched.
                    candidate[0] = chk
                    candidate[2] = False
                    candidate[5] = None # Reset!
                    self.current_candidates.insert(0, candidate)
                    return True

            assert False

    def _handle_success(self, client, msg, candidate):
        """ INTERNAL: Handle successful FCP requests. """
        #print "_handle_success -- ", candidate
        if not self._needs_bundle(candidate):
            #print "_handle_success -- doesn't need bundle."
            candidate[5] = msg
            self.finished_candidates.append(candidate)
            return
        if self._handled_multiblock_case(candidate):
            return

        if (candidate[2] and self._multiple_block(candidate)):
            #print "_handle_success -- multiple block..."
            # Cases:
            # 0) No redundant edge exists, -> requeue
            # 1) Redundant edge request running, single block -> requeue
            # 2) Redundant edge request running, full -> finish
            # 3) Redundant edge request queued, full -> flip to single_block
            # 4) Redundant edge request queued, single_block ->  nop
            edge = candidate[3]
            redundant_edge = (edge[0], edge[1], int(not edge[2]))
            if (not self.parent.ctx.graph is None and
                self.parent.ctx.graph.is_redundant(edge)):
                for value in self.pending_candidates():
                    if (value[3] == redundant_edge and
                        not value[2]):
                        # Bail out because there's already a request for that
                        # data running.
                        candidate[5] = msg
                        # Make sure the candidate will re-run if the running
                        # request fails.
                        candidate[1] = 0
                        self.next_candidates.insert(0, candidate)
                        #print "_handle_success -- already another running."
                        self.parent.ctx.ui_.status(("Other salted key is "
                                                    + "running. Didn't "
                                                    + "requeue: %s\n")
                                                   % str(candidate[3]))
                        return
            self.parent.ctx.ui_.status("Requeuing full download for: %s\n"
                              % str(candidate[3]))
            # Reset the CHK because the control bytes were zorched.
            candidate[0] = self.parent.ctx.graph.get_chk(candidate[3])
            #candidate[1] += 1
            candidate[2] = False
            candidate[5] = None # Reset!
            self.rep_invariant()
            self.current_candidates.insert(0, candidate)
            self._force_single_block(redundant_edge)
            self.rep_invariant()
            return

        #print "_handle_success -- bottom"
        candidate[5] = msg
        self.finished_candidates.append(candidate)
        #print "_handle_success -- pulling!"
        name = str(candidate[3])
        if name == 'None':
            name = "%s:%s" % (','.join([ver[:12] for ver in candidate[4][1]]),
                              ','.join([ver[:12] for ver in candidate[4][2]]))

        #print "Trying to pull: ", name
        self._pull_bundle(client, msg, candidate)
        #print "_handle_success -- pulled bundle ", candidate[3]

        self.parent.ctx.ui_.status("Pulled bundle: %s\n" % name)

        if self.parent.ctx.has_versions(self.freenet_heads):
            # Done and done!
            #print "SUCCEEDED!"
            self.parent.transition(self.success_state)
            return

        #print "_reevaluate -- called"
        self._reevaluate()
        #print "_reevaluate -- exited"

    # REDFLAG: move
    def _should_retry(self, candidate):
        """ Return True if the FCP request for the candidate should
            be retried, False otherwise. """
        max_retries = self.parent.params.get('MAX_RETRIES', 0)
        if candidate[1] - 1 >= max_retries:
            #print "_should_retry -- returned False"
            return False
        if not self._needs_bundle(candidate) and not candidate[6]:
            return False
        return True

    def _queued_redundant_edge(self, candidate):
        """ INTERNAL: Return True if a redundant request was queued for
            the candidate. """
        edge = candidate[3]
        if edge is None or candidate[6]:
            return False

        if not self.parent.ctx.graph.is_redundant(edge):
            return False

        pending, current, next, finished, dummy = self._known_edges()
        # Must include finished! REDFLAG: re-examine other cases.
        all_edges = pending.union(current).union(next).union(finished)
        alternate_edge = (edge[0], edge[1], int(not edge[2]))
        if alternate_edge in all_edges:
            return False

        self.parent.ctx.ui_.status("Queueing redundant edge: %s\n"
                               % str(alternate_edge))

        # Order is important because this changes SaltingState.
        self.next_candidates.insert(0, candidate)
        self._queue_candidate(self.next_candidates, alternate_edge,
                             not SaltingState(self).needs_full_request(
            self.parent.ctx.graph, alternate_edge))
        return True

    def _handle_failure(self, dummy, msg, candidate):
        """ INTERNAL: Handle FCP request failure for a candidate. """
        if not self._needs_bundle(candidate):
            #print "_handle_failure -- doesn't need bundle."
            candidate[5] = msg
            self.finished_candidates.append(candidate)
            return
        #print "_handle_failure -- ", candidate
        if self._should_retry(candidate):
            #print "_handle_failure -- retrying..."
            # Order important.  Allow should_retry to see previous msg.
            candidate[5] = msg
            if not self._queued_redundant_edge(candidate):
                self.next_candidates.insert(0, candidate)
        else:
            #print "_handle_failure -- abandoning..."
            candidate[5] = msg
            # Thought about adding code to queue redundant salted request here,
            # but it doesn't make sense.
            self.finished_candidates.append(candidate)

        if self.is_stalled():
            self.parent.ctx.ui_.warn("Too many failures. Gave up :-(\n")
            self.parent.transition(self.failure_state)

    def _multiple_block(self, candidate):
        """ INTERNAL: Return True if the candidate's FCP request is
            more than one block. """
        graph = self.parent.ctx.graph
        if not graph is None:
            step = candidate[3]
            # Should have been fixed up when we got the graph.
            assert not step is None
            return graph.insert_length(step) > FREENET_BLOCK_LEN

        # BUG: returns True for length == 32k w/o  padding hack.
        # Ugly but benign. Just causes an unnesc. re-fetch. Happens rarely.
        return candidate[4][0] >= FREENET_BLOCK_LEN

    # REDFLAG: Returns false for bundles you can't pull. CANT PREFETCH?
    # False if parent rev not available.
    def _needs_bundle(self, candidate):
        """ INTERNAL: Returns True if the hg bundle for the candidate's edge
            could be pulled and contains changes that we don't already have. """
        versions = self._get_versions(candidate)
        #print "_needs_bundle -- ", versions
        if not self.parent.ctx.has_versions(versions[0]):
            #print "Doesn't have parent ", versions
            return False # Doesn't have parent.

        return not self.parent.ctx.has_versions(versions[1])

    # REDFLAGE: remove msg arg?
    def _pull_bundle(self, client, dummy_msg, candidate):
        """ INTERNAL: Pull the candidates bundle from the file in
            the client param. """
        length = os.path.getsize(client.in_params.file_name)
        if not candidate[3] is None:
            expected_length = self.parent.ctx.graph.get_length(candidate[3])
        else:
            expected_length = candidate[4][0]

        #print "expected_length: ", expected_length
        #print "length         : ", length
        # Unwind padding hack. grrrr... ugly.
        assert length >= expected_length
        if length != expected_length:
            out_file = open(client.in_params.file_name, 'ab')
            try:
                out_file.truncate(expected_length)
            finally:
                out_file.close()
            assert (os.path.getsize(client.in_params.file_name)
                    == expected_length)

        self.parent.ctx.pull(client.in_params.file_name)

    def _reevaluate_without_graph(self):
        """ Decide which additional edges to request using the top key data
            only.  """
        # Use chks since we don't have access to edges.
        pending, current, next, finished = self._known_chks()
        all_chks = pending.union(current).union(next).union(finished)

        for update in self.top_key_tuple[1]:
            if not self.parent.ctx.has_versions(update[1]):
                # Still works with incomplete base.
                continue # Don't have parent.

            if self.parent.ctx.has_versions(update[2]):
                # Not guaranteed to work with incomplete heads.
                continue # Already have the update's changes.


            new_chks = []
            for chk in update[3]:
                if not chk in all_chks:
                    new_chks.append(chk)

            if len(new_chks) == 0:
                continue

            full_request_chk = random.choice(new_chks)
            new_chks.remove(full_request_chk)
            candidate = [full_request_chk, 0, False, None,
                         update, None, False]
            self.current_candidates.insert(0, candidate)
            for chk in new_chks:
                candidate = [chk, 0, True, None, update, None, False]
                self.current_candidates.insert(0, candidate)

    # NOT CHEAP!
    def _reevaluate(self):
        """ Queue addition edge requests if necessary. """
        #print "_reevaluate -- called."
        self._remove_old_candidates()
        graph = self.parent.ctx.graph

        if graph is None:
            self._reevaluate_without_graph()
            return

        # REDFLAG: make parameters
        redundancy = 4

        # Query graph for current index.
        index = latest_index(graph, self.parent.ctx.repo)

        # REDFLAG: remove debugging code
        #latest = min(index + 1, graph.latest_index)
        #dump_paths(graph, graph.enumerate_update_paths(index + 1,
        #                                               latest,
        #                                               MAX_PATH_LEN * 2),
        #           "All paths %i -> %i" % (index + 1, latest))

        # Find all extant edges.
        pending, current, next, finished, never_run = self._known_edges()
         # Ignore edges which have never been run.
        self._remove_unrun(never_run)
        all_edges = pending.union(current).union(next).union(finished) \
                    - never_run
        #print "sets:", pending, current, next, finished
        #print "finished_candidates: ", self.finished_candidates
        if None in all_edges:
            all_edges.remove(None)

        assert not None in all_edges

        # Find the edges we need to update.
        first, second = get_update_edges(graph, index, redundancy, True,
                                         all_edges)

        if self.parent.params.get('DUMP_UPDATE_EDGES', False):
            dump_update_edges(first, second, all_edges)

        assert not None in first
        assert not None in second
        assert len(set(first)) == len(first)
        assert len(set(second)) == len(second)
        assert len(set(first).intersection(all_edges)) == 0
        assert len(set(second).intersection(all_edges)) == 0

        self.rep_invariant()
        #self.dump()
        # first.reverse() ?

        salting = SaltingState(self)

        #print "FIRST: ", first
        for edge in first:
            assert not edge is None
            #print "EDGE:", edge
            full = salting.needs_full_request(graph, edge)
            self._queue_candidate(self.current_candidates, edge, not full)
            salting.add(edge, not full)
        self.rep_invariant()

        # second.reverse() ?
        #print "SECOND: ", second
        for edge in second:
            full = salting.needs_full_request(graph, edge)
            self._queue_candidate(self.next_candidates, edge, not full)
            salting.add(edge, not full)

        self.rep_invariant()

    def _queue_candidate(self, candidate_list, edge, single_block=False):
        """ INTERNAL: Queue a request for a single candidate. """
        #print "queue_candidate -- called ", edge, single_block
        assert not edge is None

        chk = self.parent.ctx.graph.get_chk(edge)
        candidate = [chk,
                     0, single_block, edge, None, None, False]
        candidate_list.insert(0, candidate)

    def _remove_old_candidates(self):
        """ INTERNAL: Remove requests for candidates which are no longer
            required. """
        #print "_remove_old_candidates -- called"
        # Cancel pending requests which are no longer required.
        for client in self.pending.values():
            candidate = client.candidate
            if candidate[6]:
                continue # Skip graph requests.
            versions = self._get_versions(candidate)
            if self.parent.ctx.has_versions(versions[1]):
                self.parent.runner.cancel_request(client)

        # "finish" requests which are no longer required.
        victims = []
        for candidate in self.current_candidates:
            versions = self._get_versions(candidate)
            if self.parent.ctx.has_versions(versions[1]):
                victims.append(candidate)
        for victim in victims:
            self.current_candidates.remove(victim)
        self.finished_candidates += victims

        # REDFLAG: C&P
        victims = []
        for candidate in self.next_candidates:
            versions = self._get_versions(candidate)
            if self.parent.ctx.has_versions(versions[1]):
                victims.append(candidate)
        for victim in victims:
            self.next_candidates.remove(victim)

        self.finished_candidates += victims

    def _get_versions(self, candidate):
        """ Return the mercurial 40 digit hex version strings for the
            parent versions and latest versions of the candidate's edge. """
        assert not candidate[6] # graph request!
        graph = self.parent.ctx.graph
        if graph is None:
            update_data = candidate[4]
            assert not update_data is None
            #print "_get_versions -- (no edge) ", update_data[1], update_data[2]
            return(update_data[1], update_data[2])

        # Should have been fixed up when we got the graph.
        step = candidate[3]
        #print "CANDIDATE: ", candidate
        assert not step is None

        #print "_get_versions -- ", step, graph.index_table[step[0]][1], \
        #   graph.index_table[step[1]][2]
        return (graph.index_table[step[0] + 1][0],
                graph.index_table[step[1]][1])

    def _known_chks(self):
        """ INTERNAL: Returns a tuple of sets of all CHKs which are
            pending, currently scheduled, scheduled next or already
            finished. """
        return (set([candidate[0] for candidate in
                     self.pending_candidates()]),
                set([candidate[0] for candidate in self.current_candidates]),
                set([candidate[0] for candidate in self.next_candidates]),
                set([candidate[0] for candidate in self.finished_candidates]))

    # REDFLAG: need to fix these to skip graph special case candidates
    # otherwise you get a None in the sets.
    def _known_edges(self):
        """ INTERNAL: Returns a tuple of sets of all edges which are
            pending, currently scheduled, scheduled next or already
            finished. """

        def process_queue(queue, never_run=None):
            """ INTERNAL: Helper function to reduce c&p. """
            ret = set([])
            for candidate in queue:
                if candidate[3] is None:
                    continue
                ret.add(candidate[3])
                if not never_run is None and candidate[1] <= 0:
                    never_run.add(candidate[3])
            return ret

        never_run = set([])

        pending = set([candidate[3] for candidate in
                       self.pending_candidates()
                       if not candidate[3] is None])

        current = process_queue(self.current_candidates, never_run)
        next = process_queue(self.next_candidates, never_run)
        finished = process_queue(self.finished_candidates)
        return (pending, current, next, finished, never_run)

    def _remove_unrun(self, never_run):
        """ INTERNAL: Remove edges that have never been run from the
            current and next queues. """
        never_run = never_run.copy()
        for queue in (self.current_candidates, self.next_candidates):
            for candidate in queue[:]:
                if candidate[3] in never_run:
                    queue.remove(candidate)
                    never_run.remove(candidate[3])
        assert len(never_run) == 0


    ############################################################
    # Public helper functions for debugging
    ############################################################

    # Expensive, only for debugging.
    def rep_invariant(self):
        """ Debugging function to check the instance's invariants. """
        def count_edges(table, bad, candidate_list):
            """ INTERNAL: Helper function to count edges. """
            for candidate in candidate_list:
                if candidate[3] is None:
                    continue
                count = table.get(candidate[3], 0)
                edge_counts[candidate[3]] = count + 1
                if edge_counts[candidate[3]] > 1:
                    bad.add(candidate[3])

        bad_counts = set([])
        edge_counts = {}
        count_edges(edge_counts, bad_counts, self.current_candidates)
        count_edges(edge_counts, bad_counts, self.next_candidates)
        count_edges(edge_counts, bad_counts, self.pending_candidates())

        if len(bad_counts) > 0:
            print "MULTIPLE EDGES: ", bad_counts
            self.dump()
            assert False

    def dump(self):
        """ Debugging function to dump the instance. """
        def print_list(msg, values):
            """ INTERNAL: print a list of values. """
            self.parent.ctx.ui_.status(msg + '\n')
            for value in values:
                self.parent.ctx.ui_.status("   " + str(value) + '\n')

        self.parent.ctx.ui_.status("--- dumping state: " + self.name + '\n')
        print_list("pending_candidates", self.pending_candidates())
        print_list("current_candidates", self.current_candidates)
        print_list("next_candidates", self.next_candidates)
