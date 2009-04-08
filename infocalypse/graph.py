""" Infocalypse Freenet hg repo update graph.

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

# REDFLAG: Document how dealing with missing indices works...
# REDFLAG: Document how pruning works.
# REDFLAG: Remove unused crap from this file
# REDFLAG: push MAX_PATH_LEN into graph class -> max_cannonical_len

import copy
import mercurial
import os
import random

from binascii import hexlify
from mercurial import commands

# Index for an empty repo.
FIRST_INDEX = -1
NULL_REV = '0000000000000000000000000000000000000000'
PENDING_INSERT = 'pending'
PENDING_INSERT1 = 'pending1'
MAX_PATH_LEN = 4

INSERT_NORMAL = 1 # Don't transform inserted data.
INSERT_PADDED = 2 # Add one trailing byte.
INSERT_SALTED_METADATA = 3 # Salt Freenet splitfile metadata.

# The size of Freenet data blocks.
FREENET_BLOCK_LEN = 32 * 1024

# Hmmm... arbitrary.
MAX_REDUNDANT_LENGTH = 128 * 1024

# HACK: Approximate multi-level splitfile boundry
MAX_METADATA_HACK_LEN = 7 * 1024 * 1024

############################################################
# Mercurial helper functions.
def hex_version(repo, version = 'tip', offset=0):
    """ Returns the 40 digit hex changeset id for an changeset in repo. """
    #print "hex_version -- ", version
    ctx = repo.changectx(version)
    assert not ctx is None
    if offset != 0:
        ctx = repo.changectx(ctx.rev() + offset)
        assert not ctx is None
    return hexlify(ctx.node())

def has_version(repo, version):
    """ Returns True if repo already contains the changeset version,
        False otherwise. """
    try:
        # Is there a faster way?
        repo.changectx(version)
    except mercurial.repo.RepoError:
        return False
    except mercurial.revlog.LookupError:
        return False
    return True

def pull_bundle(repo, ui_, bundle_file):
    """ Pull an hg bundle file.

        bundle_file must be an absolute path.
    """

    # REDFLAG: djk20090319, is this still an issue?
    # IMPORTANT:
    # You must be in the repository root directory in order to pull
    # from the bundle.  This is not obvious from the Hg doc.
    #
    # See: http://marc.info/?l=mercurial&m=118362491521186&w=2
    #
    # MUST use --cwd
    # MUST use an absolute path for the bundle field
    prev_cwd = os.getcwd()
    os.chdir(repo.root)
    try:
        commands.pull(ui_, repo, bundle_file, rev=[],
                      force=None, update=None)
    finally:
        os.chdir(prev_cwd)

############################################################

def cmp_age_weight(path_a, path_b):
    """ Comparison function used to sort paths in ascending order
        of 'canonicalness'. """
    # Only works for equivalent paths!
    assert path_a[0][0] == path_b[0][0]
    assert path_b[-1][1] == path_b[-1][1]

    # NO! path step tuples contain a third entry which keeps this
    # from working.
    # if path_a == path_b:
    #    return 0

    index = 0
    while index < len(path_a) and index < len(path_b):
        if path_a[index][1] == path_b[index][1]:
            if path_a[index][2] == path_b[index][2]:
                index += 1
                continue
            # If the edges are the same age prefer the one
            # the the lower (i.e. older) CHK ordinal.
            return path_b[index][2] - path_a[index][2]
        return path_a[index][1] - path_b[index][1]

    #print "CMP == ", path_a, path_b
    return 0

def block_cost(length):
    """ Return the number of Freenet blocks required to store
        data of length, length. """
    blocks = length/FREENET_BLOCK_LEN
    if (length % FREENET_BLOCK_LEN) != 0:
        blocks += 1
    return blocks

############################################################
# Doesn't dump FIRST_INDEX entry.
def graph_to_string(graph):
    """ Returns a human readable representation of the graph. """
    lines = []
    # Indices
    indices = graph.index_table.keys()
    indices.sort()
    for index in indices:
        if index == FIRST_INDEX:
            continue

        entry = graph.index_table[index]
        #print entry
        lines.append("I:%i:%s:%s" % (index, entry[0], entry[1]))

    # Edges
    index_pairs = graph.edge_table.keys()
    for index_pair in index_pairs:
        edge_info = graph.edge_table[index_pair]
        as_str = ':'.join(edge_info[1:])
        if as_str != '':
            as_str = ':' + as_str
        lines.append("E:%i:%i:%i%s" % (index_pair[0], index_pair[1],
                                       edge_info[0],
                                       as_str))

    return '\n'.join(lines) + '\n'

def parse_graph(text):
    """ Returns a graph parsed from text.
        text must be in the format used by graph_to_string().
        Lines starting with '#' are ignored.
    """

    graph = UpdateGraph()
    lines = text.split('\n')
    for line in lines:
        fields = line.split(':')
        if fields[0] == 'I':
            if len(fields) != 4:
                raise ValueError("Exception parsing index values.")
            index = int(fields[1])
            if index in graph.index_table:
                print "OVERWRITING INDEX: " , index
            if len(tuple(fields[2:])) != 2:
                raise ValueError("Error parsing index value: %i" % index)
            graph.index_table[index] = tuple(fields[2:])
        elif fields[0] == 'E':
            #print fields
            if len(fields) < 5:
                raise ValueError("Exception parsing edge values.")
            index_pair = (int(fields[1]), int(fields[2]))
            length = int(fields[3])
            chk_list = []
            for chk in fields[4:]:
                chk_list.append(chk)
            graph.edge_table[index_pair] = tuple([length, ] + chk_list)
        #else:
        #    print "SKIPPED LINE:"
        #    print line
    indices = graph.index_table.keys()
    if len(indices) == 0:
        raise ValueError("No indices?")
    indices.sort()
    graph.latest_index = indices[-1]

    graph.rep_invariant()

    return graph

############################################################

class UpdateGraphException(Exception):
    """ Base class for UpdateGraph exceptions. """
    def __init__(self, msg):
        Exception.__init__(self, msg)

class UpToDate(UpdateGraphException):
    """ Exception thrown to indicate that an update failed because
        the graph already contains the specified local changes. """
    def __init__(self, msg):
        UpdateGraphException.__init__(self, msg)

class UpdateGraph:
    """ A digraph representing an Infocalypse Freenet
        hg repository. """

    def __init__(self):
        # Vertices in the update digraph.
        # index_ordinal -> (start_rev, end_rev)
        self.index_table = {FIRST_INDEX:(NULL_REV, NULL_REV)}

        # These are edges in the update digraph.
        # There can be multiple redundant edges.
        #
        # This is what is actually stored in Freenet.
        # Edges contain changesets for the indices from
        # start_index + 1 to end_index, but not for start_index.
        # (start_index, end_index) -> (length, chk@, chk@,  ...)
        self.edge_table = {}

        # Bound path search length.
        self.max_search_path = 10

        self.latest_index = -1

    def clone(self):
        """ Return a deep copy of the graph. """
        return copy.deepcopy(self) # REDFLAG: correct

    # Contains the end_index changesets but not the start index changesets.
    def add_edge(self, index_pair, length_chk_pair):
        """ Add a new edge to the graph. """
        assert len(index_pair) == 2
        assert len(length_chk_pair) == 2
        assert index_pair[0] <= index_pair[1]

        edge_info = self.edge_table.get(index_pair)
        if edge_info is None:
            edge_info = tuple(length_chk_pair)
        else:
            if length_chk_pair[0] != edge_info[0]:
                raise ValueError("Redundant edge doesn't have same length.")
            edge_info = list(edge_info)
            edge_info.append(length_chk_pair[1])
            edge_info = tuple(edge_info)

        self.edge_table[index_pair] = edge_info
        return (index_pair[0], index_pair[1],
                len(self.edge_table[index_pair]) - 2)

    def subgraph(self, containing_paths):
        """ Return a subgraph which contains the vertices and
            edges in containing_paths. """
        self.rep_invariant()
        graph = UpdateGraph()
        max_index = -1

        for path in containing_paths:
            for step in path:
                pair = step[:2]
                # REDFLAG: copies ALL redundant paths
                graph.edge_table[pair] = self.edge_table[pair][:]
                for index in pair:
                    if index not in graph.index_table:
                        graph.index_table[index] = self.index_table[index][:]
                    max_index = max(max_index, index)

        graph.latest_index = max_index
        graph.rep_invariant()
        return graph

    ############################################################
    # Helper functions used when inserting / requesting
    # the edge CHKs.
    ############################################################
    def has_chk(self, edge_triple):
        """ Return True if the graph has a CHK for the edge,
            false otherwise. """
        chk = self.edge_table.get(edge_triple[:2])[1:][edge_triple[2]]
        return chk.startswith('CHK@') # Hmmm... False for pending???

    def get_chk(self, edge_triple):
        """ Return the CHK for an edge. """
        return self.edge_table[edge_triple[:2]][1:][edge_triple[2]]

    def get_length(self, edge_triple):
        """ Return the length of the hg bundle file for an edge. """
        return self.edge_table.get(edge_triple[:2])[0]

    def is_redundant(self, edge_triple):
        """ Return True if there is more than one CHK for the
            edge, False otherwise. """
        return len(self.edge_table[edge_triple[:2]]) > 2

    # REDFLAG: fix signature to take an edge triplet?
    # Hmmm... too much paranoia. just assert?
    def set_chk(self, index_pair, ordinal, length, chk):
        """ Set the CHK for an edge. """
        edge_info = self.edge_table.get(index_pair)
        if edge_info is None:
            raise UpdateGraphException("No such edge: %s" % str(index_pair))
        edge_list = list(edge_info)
        if len(edge_list) < ordinal + 2:
            raise UpdateGraphException("No such chk ordinal: [%s]:%i"
                                       % (str(index_pair), ordinal))
        if edge_list[0] != length:
            raise UpdateGraphException("Length mismatch: [%s]:%i"
                                       % (str(index_pair), ordinal))
        if not edge_list[ordinal + 1].startswith(PENDING_INSERT):
            print "set_chk -- replacing a non pending chk (%i, %i, %i)?" % \
                  (index_pair[0], index_pair[1], ordinal)
            if edge_list[ordinal + 1] == chk:
                print "Values are same."
            else:
                print "Values are different:"
                print "old:", edge_list[ordinal + 1]
                print "new:", chk
        edge_list[ordinal + 1] = chk
        self.edge_table[index_pair] = tuple(edge_list)

# def insert_type_(self, edge_triple):
#         """ Return the kind of insert required to insert the CHK
#             for the edge.

#             INSERT_NORMAL -> No modification to the bundle file.
#             INSERT_PADDED -> Add one trailing pad byte.
#             INSERT_SALTED_METADATA -> Copy and salt the Freenet
#             split file metadata for the normal insert. """
#         edge_info = self.edge_table[edge_triple[:2]]
#         #print "insert_type -- ", edge_triple, entry
#         if edge_info[edge_triple[2] + 1] == PENDING_INSERT:
#             return INSERT_NORMAL
#         if edge_info[edge_triple[2] + 1] != PENDING_INSERT1:
#             raise ValueError("CHK already set?")
#         if edge_info[0] <= FREENET_BLOCK_LEN:
#             return INSERT_PADDED
#         return INSERT_SALTED_METADATA

    def insert_type(self, edge_triple):
        """ Return the kind of insert required to insert the CHK
            for the edge.

            INSERT_NORMAL -> No modification to the bundle file.
            INSERT_PADDED -> Add one trailing pad byte.
            INSERT_SALTED_METADATA -> Copy and salt the Freenet
            split file metadata for the normal insert. """

        if edge_triple[2] == 0:
            return INSERT_NORMAL

        assert edge_triple[2] == 1

        length = self.edge_table[edge_triple[:2]][0]

        # REDFLAG: DCI. MUST DEAL WITH ==32k case
        if length <= FREENET_BLOCK_LEN:
            # Made redundant path by padding.
            return  INSERT_PADDED

        if length <= MAX_METADATA_HACK_LEN:
            return INSERT_SALTED_METADATA

        print "insert_type called for edge that's too big to salt???"
        print edge_triple
        assert False

    def insert_length(self, step):
        """ Returns the actual length of the data inserted into
            Freenet for the edge. """
        length = self.edge_table.get(step[:2])[0]
        if step[2] == 0:
            # No hacks on primary insert.
            return length
        if length < FREENET_BLOCK_LEN:
            # Made redundant path by padding.
            return  length + 1

        # Salted the metadata. Data length unaffected.
        return length

    ############################################################

    # REDFLAG: really no need for ui? if so, remove arg
    # Index and edges to insert
    # Returns index triples with new edges that need to be inserted.
    def update(self, repo, dummy, version, cache):
        """ Update the graph to include versions up to version
            in repo.

            This may add multiple edges for redundancy.

            Returns the new edges.

            The client code is responsible for setting their CHKs!"""

        if self.latest_index > FIRST_INDEX:
            if (repo.changectx(version).rev() <=
                repo.changectx(self.index_table[self.latest_index][1]).rev()):
                raise UpToDate("Version: %s is already in the repo." %
                               hex_version(repo, version)[:12])

        new_edges = []

        # Add changes to graph.
        prev_changes = self.index_table[self.latest_index]
        parent_rev = prev_changes[1]
        # REDFLAG: Think. What are the implicit assumptions here?
        first_rev = hex_version(repo, prev_changes[1], 1)
        latest_rev = hex_version(repo, version)

        index = self._add_changes(parent_rev, first_rev, latest_rev)
        #print "ADDED INDEX: ", index
        #print self.index_table
        # Insert index w/ rollup if possible.
        first_bundle = cache.make_redundant_bundle(self, index)

        new_edges.append(self.add_edge(first_bundle[2],
                                       (first_bundle[0], PENDING_INSERT)))
        #print "ADDED EDGE: ", new_edges[-1]

        canonical_path = self.canonical_path(index, MAX_PATH_LEN + 1)
        assert len(canonical_path) <= MAX_PATH_LEN + 1

        bundle = None
        if len(canonical_path) > MAX_PATH_LEN:
            print "CANNONICAL LEN: ", len(canonical_path)
            short_cut = self._compress_canonical_path(index, MAX_PATH_LEN + 1)
            bundle = cache.make_bundle(self, short_cut)
            new_edges.append(self.add_edge(bundle[2],
                                           (bundle[0], PENDING_INSERT)))
            canonical_path = self.canonical_path(index, MAX_PATH_LEN + 1)
            assert len(canonical_path) <= MAX_PATH_LEN

        if bundle == None:
            if (first_bundle[0] <= FREENET_BLOCK_LEN and
                first_bundle[2][0] < index - 1):
                # This gives us redundancy at the cost of one 32K block.
                bundle = cache.make_bundle(self,
                                           (first_bundle[2][0] + 1,
                                            index))
                new_edges.append(self.add_edge(bundle[2],
                                               (bundle[0],
                                                PENDING_INSERT)))
            elif first_bundle[0] <= MAX_METADATA_HACK_LEN:
                # Request insert of a redundant copy of exactly the same
                # bundle.
                bundle = first_bundle[:]
                new_edges.append(self.add_edge(bundle[2], (bundle[0],
                                                           PENDING_INSERT1)))
            else:
                print "update -- Bundle too big to add redundant CHK: %i" \
                      % first_bundle[0]

        new_edges = new_edges + self._add_canonical_path_redundancy()

        return new_edges

    def get_top_key_edges(self):
        """ Returns the ordered list of edges that should be
            included in the top key. """
        self.rep_invariant()

        edges = []

        #print "LATEST_INDEX: ", self.latest_index

        paths = self.enumerate_update_paths(self.latest_index,
                                             self.latest_index, 1)
        #print paths

        paths.sort(self._cmp_block_cost)
        #dump_paths(self, paths, "Paths sorted by block cost")

        if len(paths) > 0:
            # Path with the most changes in the least blocks.
            edges.append(paths[0][0])
            del paths[0]

        if len(paths) > 0:
            # REDFLAG: == 32k case for padding crosses block boundry...
            if (block_cost(self.path_cost([edges[0], ])) ==
                block_cost(self.path_cost(paths[0]))):
                # One more at the same cost if there is one.
                edges.append(paths[0][0])
                del paths[0]

        # The canonical path
        path = list(self.canonical_path(self.latest_index, MAX_PATH_LEN))

        path.reverse() # most recent first.
        for step in path:
            if not step in edges:
                edges.append(step)

        # 2 possibly redundant immediate update keys, and MAX_PATH_LEN
        # canonical path keys. Actually at least one of the canonical keys
        # should already be in the immediate updates.
        assert len(edges) < 4 + MAX_PATH_LEN
        return edges

    def enumerate_update_paths(self, containing_start, to_end, max_len,
                                partial_path=()):

        """ INTERNAL: Returns a list of paths from the start index to the end
            index. """

        if max_len <= 0:
            return []
        ret = []

        candidates = self.contain(containing_start)
        #print "CANDIDATES: ", candidates
        for candidate in candidates:
            if candidate[1] >= to_end:
                ret.append(partial_path + (candidate,))
            else:
                ret += self.enumerate_update_paths(candidate[1] + 1, to_end,
                                                   max_len - 1,
                                                   partial_path
                                                   + (candidate,))
        return ret

    # REQUIRES: Using the same index mappings!
    def copy_path(self, from_graph, path):
        """ Copy a path from one graph to another. """
        copied = False
        for step in path:
            pair = step[:2]
            if not pair in self.edge_table:
                copied = True
                self.edge_table[pair] = (
                    from_graph.edge_table[pair][:]) # Deep copy
                for index in pair:
                    if index not in self.index_table:
                        self.index_table[index] = (
                            from_graph.index_table[index][:]) # Deep copy
        return copied

    def canonical_path(self, to_index, max_search_len):
        """ Returns shortest preferred path from no updates
            to latest_index.

            This is what you would use to bootstrap from hg rev -1. """

        return self.canonical_paths(to_index, max_search_len)[-1]

    def canonical_paths(self, to_index, max_search_len):
        """ Returns a list of paths from no updates to to_index in
            ascending order of 'canonicalness'. i.e. so you
            can pop() the candidates off the list. """

        paths = self.enumerate_update_paths(0, to_index, max_search_len)
        if len(paths) == 0:
            raise UpdateGraphException("No such path: %s"
                                       % str((0, to_index)))

        paths.sort(cmp_age_weight)
        return paths

    def path_cost(self, path, blocks=False):
        """ The sum of the lengths of the hg bundles required to update
            using the path. """

        value = 0
        for step in path:
            if blocks:
                value += block_cost(self.edge_table[step[:2]][0])
            else:
                value += self.edge_table[step[:2]][0]

        return value

    # Returns ((start_index, end_index, chk_list_ordinal), ...)
    def contain(self, contains_index):
        """ Returns a list of edge triples which contain contains_index. """
        ret = []
        for pair in self.edge_table:
            if pair[0] >= contains_index:
                continue
            if pair[1] < contains_index:
                continue
            for index in range(0, len(self.edge_table[pair]) - 1):
                ret.append(pair + (index,))
        return ret

    def cmp_recency(self, path_a, path_b):
        """ INTERNAL: A comparison function for sorting single edge paths
            by recency. """
        # Only for steps
        assert len(path_a) == 1
        assert len(path_b) == 1

         # Only steps in the paths.
        step_a = path_a[0]
        step_b = path_b[0]

        if step_a[1] == step_b[1]:
            if step_a[2] == step_b[2]:
                # Ascending Length. TRICKY: because of padding hacks.
                return (self.insert_length(step_a)
                        - self.insert_length(step_b))
            # Ascending redundancy. i.e. "most canonical" first
            return step_a[2] - step_b[2]

        # descending initial update. i.e. Most recent first.
        return step_b[1] - step_a[1]

    # REDFLAG: add_index instead ???
    # REDFLAG: rethink parent_rev
    def _add_changes(self, parent_rev, first_rev, last_rev):
        """ Add changes to the graph. """
        assert parent_rev == self.index_table[self.latest_index][1]
        self.latest_index += 1
        self.index_table[self.latest_index] = (first_rev, last_rev)
        return self.latest_index

    def _cmp_block_cost(self, path_a, path_b):
        """ INTERNAL: A comparison function for sorting single edge paths
            in order of ascending order of block count. """
        assert len(path_a) == 1
        assert len(path_b) == 1

        cost_a = self.insert_length(path_a[0])
        cost_b = self.insert_length(path_b[0])

        # Actually block cost - 1, but that's ok.
        block_cost_a = cost_a / FREENET_BLOCK_LEN
        block_cost_b = cost_b / FREENET_BLOCK_LEN

        if block_cost_a == block_cost_b:
            mod_a = cost_a % FREENET_BLOCK_LEN
            mod_b = cost_b % FREENET_BLOCK_LEN
            if mod_a == mod_b:
                # Ascending order of redundancy ordinal.
                return int(path_a[0][2] - path_b[0][2])

            # Descending order of length (for same block size)
            return int(mod_b - mod_a)

        # Ascending order of length in blocks
        return int(block_cost_a - block_cost_b)

    # REDFLAG: Can the edge already exists?
    # Only makes sense for latest index. get rid of latest_index argument?

    # enforce constraint that sum of costs head must be less
    # than the cost for the prev step. power law???

    # REQURIES: len(canonical_path(latest_index)) > 1
    def _compress_canonical_path(self, to_index, max_search_len=10):
        """ Return an index tuple for a new shortcut path that would
            reduces the canonical path length by at least one, favoring
            accumulation of hg bundle size  at the start of the path. """


        shortest_known = self.canonical_path(to_index, max_search_len)
        #print "SHORTEST_KNOWN: ", shortest_known
        assert len(shortest_known) > 1

        if len(shortest_known) == 2:
            # We only have one move.
            return (shortest_known[0][0], shortest_known[-1][1])

        # REDFLAG: Shouldn't this be using block cost?
        for index in range(1, len(shortest_known)):
            prev_cost = self.path_cost((shortest_known[index - 1],))
            if self.path_cost(shortest_known[index:]) > prev_cost:
                return (shortest_known[index - 1][0], shortest_known[-1][1])
        return (shortest_known[-2][0], shortest_known[-1][1])

    def _add_canonical_path_redundancy(self):
        """ Adds redundant edges for steps on the canonical path.

            Returns the new edges.
        """
        ret = []
        path = self.canonical_path(self.latest_index, MAX_PATH_LEN)
        for index, step in enumerate(path):
            if index == MAX_PATH_LEN - 1:
                # Don't try to add redundancy to the last (latest) step
                break
            entries = self.edge_table[step[:2]]
            if len(entries) > 2:
                # Already redundant
                continue
            assert step[2] == 0
            assert entries[1] != PENDING_INSERT1
            if entries[0] <= FREENET_BLOCK_LEN:
                #print "_add_canonical_path_redundancy -- too small: ", \
                # str(step)
                continue
            if entries[0] > MAX_METADATA_HACK_LEN:
                #print "_add_canonical_path_redundancy -- too big: ", str(step)
                continue
            edge = self.add_edge(step[:2], (entries[0], PENDING_INSERT1))
            #print "_add_canonical_path_redundancy -- added edge: ", str(edge)
            ret.append(edge)
        return ret

    def rep_invariant(self):
        """ Debugging function to check invariants. """
        max_index = -1
        for index in self.index_table.keys():
            max_index = max(index, max_index)

        assert self.latest_index == max_index

        for edge in self.edge_table.keys():
            assert edge[0] in self.index_table
            assert edge[1] in self.index_table

# REDFLAG: O(n), has_index().
def latest_index(graph, repo):
    """ Returns the index of the latest hg version in the graph
        that exists in repo. """
    graph.rep_invariant()
    for index in range(graph.latest_index, FIRST_INDEX - 1, -1):
        if not index in graph.index_table:
            continue
        if has_version(repo, graph.index_table[index][1]):
            return index
    return FIRST_INDEX

# REDFLAG: fix this so that it always includes pending edges.
def minimal_update_graph(graph, max_size=32*1024,
                         formatter_func=graph_to_string):
    """ Returns a subgraph that can be formatted to <= max_size
        bytes with formatter_func. """

    index = graph.latest_index
    assert index > FIRST_INDEX

    # All the edges that would be included in the top key.
    # This includes the canonical bootstrap path and the
    # two cheapest updates from the previous index.
    paths = [[edge, ] for edge in graph.get_top_key_edges()]

    minimal = graph.subgraph(paths)
    if len(formatter_func(minimal)) > max_size:
        raise UpdateGraphException("Too big with only required paths.")

    # REDFLAG: read up on clone()
    prev_minimal = minimal.clone()

    # Then add all other full bootstrap paths.
    canonical_paths = graph.canonical_paths(index, MAX_PATH_LEN)

    while len(canonical_paths):
        if minimal.copy_path(graph, canonical_paths.pop()):
            size = len(formatter_func(minimal))
            #print "minimal_update_graph -- size: %i " % size
            if size > max_size:
                return prev_minimal
            else:
                prev_minimal = minimal.clone()

    if index == 0:
        return prev_minimal

    # Favors older edges
    # Then add bootstrap paths back to previous indices
    for upper_index in range(index - 1, FIRST_INDEX, - 1):
        canonical_paths = graph.canonical_paths(upper_index, MAX_PATH_LEN)
        while len(canonical_paths):
            if minimal.copy_path(graph, canonical_paths.pop()):
                size = len(formatter_func(minimal))
                #print "minimal_update_graph -- size(1): %i" % size
                if size > max_size:
                    return prev_minimal
                else:
                    prev_minimal = minimal.clone()

    return prev_minimal


def chk_to_edge_triple_map(graph):
    """ Returns a CHK -> edge triple map. """
    ret = {}
    for edge in graph.edge_table:
        #print "EDGE: ", edge
        chks = graph.edge_table[edge][1:]
        #print "ENTRIES: ", entries
        for index, chk in enumerate(chks):
            assert ret.get(chk) is None
            ret[chk] = (edge[0], edge[1], index)
    return ret

def break_edges(graph, kill_probability, skip_chks):
    """ Testing function breaks edges by replacing the CHKs with a known
        bad one. """
    bad_chk = ('CHK@badroutingkeyB55JblbGup0yNSpoDJgVPnL8E5WXoc,'
               +'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
    for edge in graph.edge_table:
        edge_info = graph.edge_table[edge[:2]]
        length = edge_info[0]
        chks = edge_info[1:]
        for index in range(0, len(chks)):
            if graph.get_chk((edge[0], edge[1], index)) in skip_chks:
                # Hack to skip pending requests.
                print "break_edges -- skipped: ", (edge[0], edge[1], index)
                continue
            if random.random() < kill_probability:
                graph.set_chk(edge, index, length, bad_chk)

def pretty_index(index):
    """ Format an index value for output. """
    if index == FIRST_INDEX:
        return "."
    else:
        return str(index)

def dump_path(graph, path):
    """ Debugging function to print a path. """
    if len(path) == 0:
        print "EMPTY PATH!"
        return

    print "(%s)-->[%s] cost=%0.2f" % (pretty_index(path[0][0]),
                                      pretty_index(path[-1][1]),
                                      graph.path_cost(path, True))
    for step in path:
        cost = graph.get_length(step)
        print "   (%s) -- (%0.2f, %i) --> [%s]" % (pretty_index(step[0]),
                                                cost,
                                                step[2],
                                                pretty_index(step[1]))
def dump_paths(graph, paths, msg):
    """ Debugging function to dump a list of paths. """
    print  "--- %s ---" % msg
    for path in paths:
        dump_path(graph, path)
    print "---"

def print_list(msg, values):
    """ INTERNAL: Helper function. """
    if msg:
        print msg
    for value in values:
        print "   ", value
    if len(values) == 0:
        print

