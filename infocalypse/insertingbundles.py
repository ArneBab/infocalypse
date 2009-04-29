""" A RequestQueueState which inserts hg bundles corresponding to
    edges in the Infocalypse update graph into Freenet.

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

from graph import UpToDate, INSERT_SALTED_METADATA, INSERT_HUGE, \
     FREENET_BLOCK_LEN, build_version_table, get_heads, \
     PENDING_INSERT1, get_huge_top_key_edges
from graphutil import graph_to_string
from bundlecache import BundleException

from statemachine import RequestQueueState

# REDFLAG: duplicated to get around circular deps.
INSERTING_GRAPH = 'INSERTING_GRAPH'
FAILING = 'FAILING'
CANCELING = 'CANCELING'
QUIESCENT = 'QUIESCENT'

# Hmmmm... hard coded exit states.
class InsertingBundles(RequestQueueState):
    """ A state to insert hg bundles corresponding to the edges in an
        Infocalypse update graph into Freenet. """
    def __init__(self, parent, name):
        RequestQueueState.__init__(self, parent, name)

        # edge -> StatefulRequest
        self.pending = {}
        self.new_edges = []
        self.required_edges = []
        # HACK:
        # edge -> (x,y, 0) Freenet metadata bytes
        self.salting_cache = {}

    def enter(self, dummy):
        """ Implementation of State virtual.

            This checks the graph against the local repository and
            adds edges required to update it to the TARGET_VERSION
            specified in the context object. Then it starts inserting
            CHKS for the new edges into Freenet, doing padding /
            metadata salting as required.

        """
        #require_state(from_state, QUIESCENT)
        assert (self.parent.ctx.get('REINSERT', 0) > 0 or
                (not self.parent.ctx['INSERT_URI'] is None))
        assert not self.parent.ctx.graph is None

        graph = self.parent.ctx.graph.clone()
        if self.parent.params.get('DUMP_GRAPH', False):
            self.parent.ctx.ui_.status("--- Initial Graph ---\n")
            self.parent.ctx.ui_.status(graph_to_string(graph) +'\n')

        latest_revs = get_heads(graph)

        self.parent.ctx.ui_.status("Latest heads(s) in Freenet: %s\n"
                                 % ' '.join([ver[:12] for ver in latest_revs]))

        if self.parent.ctx.get('REINSERT', 0) == 1:
            self.parent.ctx.ui_.status("No bundles to reinsert.\n")
            # REDFLAG: Think this through. Crappy code, but expedient.
            # Hmmmm.... need version table to build minimal graph
            self.parent.ctx.version_table = build_version_table(graph,
                                                                self.parent.ctx.
                                                                repo)
            self.parent.transition(INSERTING_GRAPH)
            return

        if not self.parent.ctx.has_versions(latest_revs):
            self.parent.ctx.ui_.warn("The local repository isn't up "
                                     + "to date.\n"
                                     + "Try doing an fn-pull.\n")
            self.parent.transition(FAILING) # Hmmm... hard coded state name
            return

        # Update graph.
        try:
            self.set_new_edges(graph)
        except UpToDate, err:
            # REDFLAG: Later, add FORCE_INSERT parameter?
            self.parent.ctx.ui_.warn(str(err) + '\n') # Hmmm
            self.parent.transition(FAILING) # Hmmm... hard coded state name
            return

        text = ''
        for edge in self.new_edges:
            text += "%i:%s\n" % (graph.get_length(edge), str(edge))
        if len(text) > 0:
            self.parent.ctx.ui_.status('Inserting bundles:\n' + text)

        #print "--- Updated Graph ---"
        #print graph_to_string(graph)
        #print "--- Minimal Graph ---"
        #print graph_to_string(minimal_update_graph(graph,31 * 1024,
        # graph_to_string))
        #print "---"
        #dump_top_key_tuple((('CHK@', 'CHK@'),
        #                    get_top_key_updates(graph)))

        self._check_new_edges("Up to date")

        self.parent.ctx.graph = graph

        # Edge CHKs required to do metadata salting.
        # Most of these probably won't exist yet.
        self.required_edges = []
        for edge in self.new_edges:
            assert edge[2] <= 1
            if graph.insert_type(edge) == INSERT_SALTED_METADATA:
                # Have to wait for the primary insert to finish.
                self.required_edges.append((edge[0], edge[1], 0))

        for edge in self.required_edges:
            # Will be re-added when the required metadata arrives.
            self.new_edges.remove((edge[0], edge[1], 1))

    # REDFLAG: no longer needed?
    def leave(self, dummy):
        """ Implementation of State virtual. """
        # Hmmm...
        for request in self.pending.values():
            self.parent.runner.cancel_request(request)

    def reset(self):
        """ Implementation of State virtual. """
        self.new_edges = []
        self.required_edges = []
        self.salting_cache = {}
        RequestQueueState.reset(self)

    def next_runnable(self):
        """ Implementation of RequestQueueState virtual. """
        for edge in self.required_edges:
            if edge in self.pending:
                # Already running.
                continue

            # We can't count on the graph when reinserting.
            # Because the chks are already set.

            #if not self.parent.ctx.graph.has_chk(edge):
            #    # Depends on an edge which hasn't been inserted yet.
            #    continue

            if edge in self.new_edges:
                # Depends on an edge which hasn't been inserted yet.
                continue

            assert not edge in self.pending
            request = self.parent.ctx.make_splitfile_metadata_request(edge,
                                                                      edge)
            self.pending[edge] = request
            return request

        if len(self.new_edges) == 0:
            return None

        request = None
        try:
            edge = self.new_edges.pop()
            request = self.parent.ctx.make_edge_insert_request(edge, edge,
                                                           self.salting_cache)
            self.pending[edge] = request
        except BundleException:
            if self.parent.ctx.get('REINSERT', 0) > 0:
                self.parent.ctx.ui_.warn("Couldn't create an identical "
                                         + "bundle to re-insert.\n"
                                         + "Possible causes:\n"
                                         + "0) Changes been locally commited "
                                         + "but not fn-push'd yet.\n"
                                         + "1) The repository was inserted "
                                         + "with a different version of hg.\n")
                self.parent.transition(FAILING)
            else:
                # Dunno what's going on.
                raise

        return request

    def request_done(self, client, msg):
        """ Implementation of RequestQueueState virtual. """
        #print "TAG: ", client.tag
        assert client.tag in self.pending
        edge = client.tag
        del self.pending[edge]
        if msg[0] == 'AllData':
            self.salting_cache[client.tag] = msg[2]

            # Queue insert request now that the required data is cached.
            if edge in self.required_edges:
                assert edge[2] == 0
                self.required_edges.remove(edge)
                self.parent.ctx.ui_.status("Re-adding put request for salted "
                                       + "metadata: %s\n"
                                       % str((edge[0], edge[1], 1)))
                self.new_edges.append((edge[0], edge[1], 1))
        elif msg[0] == 'PutSuccessful':
            chk1 = msg[1]['URI']
            graph = self.parent.ctx.graph
            if edge[2] == 1 and graph.insert_length(edge) > FREENET_BLOCK_LEN:
                # HACK HACK HACK
                # TRICKY:
                # Scrape the control bytes from the full request
                # to enable metadata handling.
                # REDFLAG: Do better?
                chk0 = graph.get_chk((edge[0], edge[1], 0))
                chk0_fields = chk0.split(',')
                chk1_fields = chk1.split(',')
                #print "FIELDS: ", chk0_fields, chk1_fields
                # Hmmm... also no file names.
                assert len(chk0_fields) == len(chk1_fields)
                chk1 = ','.join(chk1_fields[:-1] + chk0_fields[-1:])
            if self.parent.ctx.get('REINSERT', 0) < 1:
                graph.set_chk(edge[:2], edge[2],
                              graph.get_length(edge),
                              chk1)
            else:
                if (graph.insert_type(edge) == INSERT_HUGE and
                    graph.get_chk(edge) == PENDING_INSERT1):
                    assert edge[2] == 1
                    graph.set_chk(edge[:2], edge[2],
                              graph.get_length(edge),
                              chk1)
                if chk1 != graph.get_chk(edge):
                    self.parent.ctx.ui_.status("Bad CHK: %s %s\n" %
                                               (str(edge), chk1))
                    self.parent.ctx.ui_.warn("CHK for reinserted edge doesn't "
                                             + "match!\n")
                    self.parent.transition(FAILING)

        else:
            # REDFLAG: retrying?
            # REDFLAG: More failure information, FAILING state?
            self.parent.transition(FAILING)
            return

        if (len(self.pending) == 0 and
            len(self.new_edges) == 0 and
            len(self.required_edges) == 0):
            self.parent.transition(INSERTING_GRAPH)

    def _check_new_edges(self, msg):
        """ INTERNAL: Helper function to raise if new_edges is empty. """
        if len(self.new_edges) == 0:
            raise UpToDate(msg)

    def set_new_edges(self, graph):
        """ INTERNAL: Set the list of new edges to insert. """

        # REDFLAG: Think this through.
        self.parent.ctx.version_table = build_version_table(graph,
                                                            self.parent.ctx.
                                                            repo)
        # Hmmmm level == 1 handled elsewhere...
        level = self.parent.ctx.get('REINSERT', 0)
        if level == 0: # Insert update, don't re-insert
            self.new_edges = graph.update(self.parent.ctx.repo,
                                          self.parent.ctx.ui_,
                                          self.parent.ctx['TARGET_VERSIONS'],
                                          self.parent.ctx.bundle_cache)
        elif level ==  2 or level == 3: # Topkey(s), graphs(s), updates
            # Hmmmm... later support different values of REINSERT?
            self.new_edges = graph.get_top_key_edges()
            if level == 2: # 3 == All top key updates.
                # Only the latest update.
                self.new_edges = self.new_edges[:1]

            redundant = []
            for edge in  self.new_edges:
                if graph.is_redundant(edge):
                    alternate_edge = (edge[0], edge[1], int(not edge[2]))
                    if not alternate_edge in self.new_edges:
                        redundant.append(alternate_edge)
            self.new_edges += redundant
            for edge in self.new_edges[:]: # Deep copy!
                if graph.insert_type(edge) == INSERT_HUGE:
                    # User can do this with level == 5
                    self.parent.ctx.ui_.status("Skipping unsalted re-insert of "
                                               + "big edge: %s\n" % str(edge))
                    self.new_edges.remove(edge)
        elif level == 4: # Add redundancy for big updates.
            self.new_edges = get_huge_top_key_edges(graph, False)
            self._check_new_edges("There are no big edges to add.")

        elif level == 5: # Reinsert big updates.
            self.new_edges =  get_huge_top_key_edges(graph, True)
            self._check_new_edges("There are no big edges to re-insert.")
