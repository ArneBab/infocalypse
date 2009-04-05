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

from graph import graph_to_string, UpToDate, INSERT_SALTED_METADATA, \
     FREENET_BLOCK_LEN

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
            self.parent.ctx.ui_.status('Adding new bundles:\n' + text)

        #print "--- Updated Graph ---"
        #print graph_to_string(graph)
        #print "--- Minimal Graph ---"
        #print graph_to_string(minimal_update_graph(graph,31 * 1024,
        # graph_to_string))
        #print "---"
        #dump_top_key_tuple((('CHK@', 'CHK@'),
        #                    get_top_key_updates(graph)))

        if len(self.new_edges) == 0:
            raise Exception("Up to date")

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
        print "REQUIRED_EDGES:", self.required_edges, self.new_edges

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

        edge = self.new_edges.pop()
        request = self.parent.ctx.make_edge_insert_request(edge, edge,
                                                           self.salting_cache)
        self.pending[edge] = request
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
            graph.set_chk(edge[:2], edge[2],
                          graph.get_length(edge),
                          chk1)
        else:
            # REDFLAG: retrying?
            # REDFLAG: More failure information, FAILING state?
            self.parent.transition(FAILING)
            return

        if (len(self.pending) == 0 and
            len(self.new_edges) == 0 and
            len(self.required_edges) == 0):
            self.parent.transition(INSERTING_GRAPH)

    def set_new_edges(self, graph):
        """ INTERNAL: Set the list of new edges to insert. """
        if self.parent.ctx.get('REINSERT', 0) == 0:
            self.new_edges = graph.update(self.parent.ctx.repo,
                                          self.parent.ctx.ui_,
                                          self.parent.ctx['TARGET_VERSION'],
                                          self.parent.ctx.bundle_cache)
            return

        # Hmmmm... later support different int values of REINSERT?
        self.new_edges = graph.get_top_key_edges()
        redundant = []
        for edge in  self.new_edges:
            if graph.is_redundant(edge):
                alternate_edge = (edge[0], edge[1], int(not edge[2]))
                if not alternate_edge in self.new_edges:
                    redundant.append(alternate_edge)
        self.new_edges += redundant
