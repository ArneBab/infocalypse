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
# REDFLAG: stash version map info in the graph?
# REDFLAG: DOCUMENT version sorting assumptions/requirements
import copy
import random

from binascii import hexlify
from mercurial import commands

# Index for an empty repo.
FIRST_INDEX = -1
NULL_REV = '0000000000000000000000000000000000000000'
PENDING_INSERT = 'pending'
PENDING_INSERT1 = 'pending1'

# Values greater than 4  won't work without fixing the implementation
# of canonical_paths().
MAX_PATH_LEN = 4

INSERT_NORMAL = 1 # Don't transform inserted data.
INSERT_PADDED = 2 # Add one trailing byte.
INSERT_SALTED_METADATA = 3 # Salt Freenet splitfile metadata.
INSERT_HUGE = 4 # Full re-insert with alternate metadata.

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
    except:
        # REDFLAG: Back to this. Hack for 1.2
        # Mercurial 1.2 can't find RepoError???
        return False
#     except mercurial.repo.RepoError:
#         return False
#     except mercurial.revlog.LookupError:
#         return False
    return True

def pull_bundle(repo, ui_, bundle_file):
    """ Pull an hg bundle file.

        bundle_file must be an absolute path.
    """

    # Not 
    commands.unbundle(ui_, repo, bundle_file, rev=[],
                      force=None, update=None)
    # Not required anymore, unbundle seems to work.
    # 
    # REDFLAG: djk20090319, is this still an issue?
    # IMPORTANT:
    # You must be in the repository root directory in order to pull
    # from the bundle.  This is not obvious from the Hg doc.
    #
    # See: http://marc.info/?l=mercurial&m=118362491521186&w=2
    #
    # MUST use --cwd
    # MUST use an absolute path for the bundle field
    #prev_cwd = os.getcwd()
    #os.chdir(repo.root)
    #try:
    #    commands.pull(ui_, repo, bundle_file, rev=[],
    #                  force=None, update=None)
    #finally:
    #    os.chdir(prev_cwd)

############################################################

def edges_containing(graph, index):
    """ INTERNAL: Returns a list of edges containing index in order of
        ascending 'canonicalness'.
    """
    def cmp_edge(edge_a, edge_b):
        """ INTERNAL: Comparison function. """
        # First, ascending final index. == Most recent.
        diff = edge_a[1] - edge_b[1]
        if diff == 0:
            # Then, descending  initial index. == Most redundant
            diff = edge_b[0] - edge_a[0]
            if diff == 0:
                # Finally, descending 'canonicalness'
                diff = edge_b[2] - edge_a[2]
        return diff

    edges = graph.contain(index)
    edges.sort(cmp_edge) # Best last so you can pop
    #print "--- dumping edges_containing ---"
    #print '\n'.join([str(edge) for edge in edges])
    #print "---"
    return edges

def tail(list_value):
    """ Returns the tail of a list. """
    return list_value[len(list_value) - 1]

def canonical_path_itr(graph, from_index, to_index, max_search_len):
    """ A generator which returns a sequence of canonical paths in
        descending order of 'canonicalness'. """

    returned = set([])
    min_search_len = -1
    while min_search_len <= max_search_len:
        visited = set([]) # Retraverse for each length! REDFLAG: Do better?
        steps = [edges_containing(graph, from_index), ]
        current_search_len = max_search_len
        while len(steps) > 0:
            while len(tail(steps)) > 0:
                #candidate = [tail(paths) for paths in steps]
                #print "candidate: ", candidate
                #print "end: ", tail(tail(steps))
                if tail(tail(steps))[1] >= to_index:
                    # The edge at the bottom of every list.
                    value = [tail(step) for step in steps]
                    #print "HIT:"
                    #print value

                    if min_search_len == -1:
                        min_search_len = len(steps)

                    current_search_len = max(len(steps), min_search_len)
                    tag = str(value)
                    if not tag in returned:
                        returned.add(tag)

                        # Shorter paths should already be in returned.
                        assert len(value) >= min_search_len
                        assert len(value) <= max_search_len
                        yield value
                    tail(steps).pop()
                elif len(steps) < current_search_len:
                    tag = str([tail(step) for step in steps])
                    if not tag in visited:
                        # Follow the path one more step.
                        visited.add(tag)
                        steps.append(edges_containing(graph,
                                                      tail(tail(steps))[1] + 1))
                    else:
                        tail(steps).pop()
                else:
                    # Abandon the path because it's too long.
                    tail(steps).pop()

            # Get rid of the empty list
            assert len(tail(steps)) == 0
            steps.pop()
        if min_search_len == -1:
            #print "No such path."
            return
        min_search_len += 1

    #print "exiting"
    # Done iterating.

def get_changes(repo, version_map, versions):
    """ INTERNAL: Helper function used by UpdateGraph.update()
        to determine which changes need to be added. """
    if versions == None:
        versions = [hexlify(head) for head in repo.heads()]
    else:
        versions = list(versions) # Hmmmm...
        # Normalize all versions to 40 digit hex strings.
        for index, version in enumerate(versions):
            versions[index] = hex_version(repo, version)

    if NULL_REV in versions:
        versions.remove(NULL_REV)

    new_heads = []
    for version in versions:
        if not version in version_map:
            new_heads.append(version)

    if len(new_heads) == 0:
        if len(versions) > 0:
            versions.sort()
            raise UpToDate("Already in repo: " + ' '.join([ver[:12] for
                                                           ver in versions]))
        else:
            raise UpToDate("Empty repository. Nothing to add.")

    if len(version_map) == 1:
        return ((NULL_REV,), new_heads)

    #print "VERSION MAP:"
    #print version_map
    # Determine base revs.
    base_revs = set([])
    traversed = set([])
    for head in new_heads:
        find_latest_bases(repo, head, version_map, traversed, base_revs)

    return (base_revs, new_heads)

############################################################

def block_cost(length):
    """ Return the number of Freenet blocks required to store
        data of length, length. """
    blocks = length/FREENET_BLOCK_LEN
    if (length % FREENET_BLOCK_LEN) != 0:
        blocks += 1
    return blocks

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
        hg repository. """ # REDFLAG: digraph of what dude?

    def __init__(self):
        # Vertices in the update digraph.
        #
        # An indice is an encapsulation of the parameters that you
        # need to bundle a collection of changes.
        #
        # index_ordinal -> ((base_revs, ), (tip_revs, ))
        self.index_table = {FIRST_INDEX:((), (NULL_REV,))}

        # These are edges in the update digraph.
        # There can be multiple redundant edges.
        #
        # This is what is actually stored in Freenet.
        # Edges contain changesets for the indices from
        # start_index + 1 to end_index, but not for start_index.
        # (start_index, end_index) -> (length, chk@, chk@,  ...)
        self.edge_table = {}

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
            split file metadata for the normal insert.
            INSERT_HUGE -> Full re-insert of data that's too big
            for metadata salting.
            """

        if edge_triple[2] == 0:
            return INSERT_NORMAL

        assert edge_triple[2] == 1

        length = self.edge_table[edge_triple[:2]][0]

        # REDFLAG: MUST DEAL WITH ==32k case, djk20080425 -- I think this is ok
        if length <= FREENET_BLOCK_LEN:
            # Made redundant path by padding.
            return  INSERT_PADDED

        if length <= MAX_METADATA_HACK_LEN:
            return INSERT_SALTED_METADATA

        print "insert_type -- called for edge that's too big to salt???"
        print edge_triple
        return INSERT_HUGE

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

    def add_index(self, base_revs, new_heads):
        """ Add changes to the graph. """
        assert not NULL_REV in new_heads
        assert len(base_revs) > 0
        assert len(new_heads) > 0
        base_revs.sort()
        new_heads.sort()
        if self.latest_index != FIRST_INDEX and NULL_REV in base_revs:
            print "add_index -- base=null in base_revs. Really do that?"
        self.latest_index += 1
        self.index_table[self.latest_index] = (tuple(base_revs),
                                               tuple(new_heads))
        return self.latest_index

    # REDFLAG: really no need for ui? if so, remove arg
    # Index and edges to insert
    # Returns index triples with new edges that need to be inserted.
    def update(self, repo, dummy, versions, cache):
        """ Update the graph to include versions up to version
            in repo.

            This may add multiple edges for redundancy.

            Returns the new edges.

            The client code is responsible for setting their CHKs!"""

        version_map = build_version_table(self, repo)

        base_revs, new_heads = get_changes(repo, version_map, versions)

        # IMPORTANT: Order matters. Must be after find_latest_bases above.
        # Update the map. REDFLAG: required?
        #for version in new_heads:
        #    version_map[version] = self.latest_index + 1

        index = self.add_index(list(base_revs), new_heads)
        new_edges = []

        #print "ADDED INDEX: ", index
        #print self.index_table
        # Insert index w/ rollup if possible.
        first_bundle = cache.make_redundant_bundle(self, version_map, index)

        new_edges.append(self.add_edge(first_bundle[2],
                                       (first_bundle[0], PENDING_INSERT)))
        #print "ADDED EDGE: ", new_edges[-1]
        bundle = None
        try:
            canonical_path = self.canonical_path(index, MAX_PATH_LEN)
            assert len(canonical_path) <= MAX_PATH_LEN
        except UpdateGraphException:
            # We need to compress the path.
            short_cut = self._compress_canonical_path(index, MAX_PATH_LEN + 1)

            bundle = cache.make_bundle(self, version_map, short_cut)

            new_edges.append(self.add_edge(bundle[2],
                                           (bundle[0], PENDING_INSERT)))
            # MAX_PATH_LEN + 1 search can be very slow.
            canonical_path = self.canonical_path(index, MAX_PATH_LEN + 1)

            assert len(canonical_path) <= MAX_PATH_LEN

        if bundle == None:
            if (first_bundle[0] <= FREENET_BLOCK_LEN and
                first_bundle[2][0] < index - 1):
                # This gives us redundancy at the cost of one 32K block.

                bundle = cache.make_bundle(self,
                                           version_map,
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
                print "update -- Bundle too big to salt! CHK: %i" \
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
        try:
            return canonical_path_itr(self, 0, to_index, max_search_len).next()
        except StopIteration:
            raise UpdateGraphException("No such path: %s"
                                       % str((0, to_index)))

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

    def rep_invariant(self, repo=None, full=True):
        """ Debugging function to check invariants. """
        max_index = -1
        min_index = -1
        for index in self.index_table.keys():
            max_index = max(index, max_index)
            min_index = min(index, min_index)
        assert self.latest_index == max_index
        assert min_index == FIRST_INDEX

        assert self.index_table[FIRST_INDEX][0] == ()
        assert self.index_table[FIRST_INDEX][1] == (NULL_REV, )
        for index in range(0, self.latest_index + 1):
            # Indices must be contiguous.
            assert index in self.index_table

            # Each index except for the empty graph sentinel
            # must have at least one base and head rev.
            assert len(self.index_table[index][0]) > 0
            assert len(self.index_table[index][1]) > 0

        # All edges must be resolvable.
        for edge in self.edge_table.keys():
            assert edge[0] in self.index_table
            assert edge[1] in self.index_table
            assert edge[0] < edge[1]


        if repo is None:
            return

        # Slow
        version_map = build_version_table(self, repo)

        values = set(version_map.values())
        values = list(values)
        values.sort()
        assert values[-1] == max_index
        assert values[0] == FIRST_INDEX
        # Indices contiguous
        assert values == range(FIRST_INDEX, max_index + 1)

        if full:
            # Verify that version map is complete.
            copied = version_map.copy()
            for rev in range(-1, repo['tip'].rev() + 1):
                version = hex_version(repo, rev)
                assert version in copied
                del copied[version]

            assert len(copied) == 0

        every_head = set([])
        for index in range(FIRST_INDEX, max_index + 1):
            versions = set([])
            for version in (self.index_table[index][0]
                            + self.index_table[index][0]):
                assert version in version_map
                if version in versions:
                    continue

                assert has_version(repo, version)
                versions.add(version)

            # Base versions must have a lower index.
            for version in self.index_table[index][0]:
                assert version_map[version] < index

            # Heads must have index == index.
            for version in self.index_table[index][1]:
                assert version_map[version] == index
                # Each head should appear in one and only one index.
                assert not version in every_head
                every_head.add(version)

# REDFLAG: O(n), has_index().
def latest_index(graph, repo):
    """ Returns the index of the latest hg version in the graph
        that exists in repo. """
    graph.rep_invariant()
    for index in range(graph.latest_index, FIRST_INDEX - 1, -1):
        if not index in graph.index_table:
            continue
        # BUG: Dog slow for big repos? cache index -> heads map
        skip = False
        for head in get_heads(graph, index):
            if not has_version(repo, head):
                skip = True
                break # Inner loop... grrr named continue?

        if skip:
            continue
        return index

    return FIRST_INDEX

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
# REDFLAG: is it a version_map or a version_table? decide an fix all names
# REDFLAG: Scales to what? 10k nodes?
# Returns version -> index mapping
# REQUIRES: Every version is in an index!
def build_version_table(graph, repo):
    """ INTERNAL: Build a version -> index ordinal map for all changesets
        in the graph. """
    table = {NULL_REV:-1}
    for index in range(0, graph.latest_index + 1):
        assert index in graph.index_table
        dummy, heads = graph.index_table[index]
        for head in heads:
            if not head in table:
                assert not head in table
                table[head] = index

            ancestors = repo[head].ancestors()
            for ancestor in ancestors:
                version = hexlify(ancestor.node())
                if version in table:
                    continue
                table[version] = index
    return table

# Find most recent ancestors for version which are already in
# the version map. REDFLAG: fix. don't make reference to time

def find_latest_bases(repo, version, version_map, traversed, base_revs):
    """ INTERNAL: Add latest known base revs for version to base_revs. """
    #print "find_latest_bases -- called: ", version[:12]
    assert version_map != {NULL_REV:FIRST_INDEX}
    if version in traversed:
        return
    traversed.add(version)
    if version in version_map:
        #print "   find_latest_bases -- adding: ", version[:12]
        base_revs.add(version)
        return
    parents = [hexlify(parent.node()) for parent in repo[version].parents()]
    for parent in parents:
        find_latest_bases(repo, parent, version_map, traversed, base_revs)


# REDFLAG: correct?  I can't come up with a counter example.
def get_heads(graph, to_index=None):
    """ Returns the 40 digit hex changeset ids of the heads. """
    if to_index is None:
        to_index = graph.latest_index

    heads = set([])
    bases = set([])
    for index in range(FIRST_INDEX, to_index + 1):
        for base in graph.index_table[index][0]:
            bases.add(base)
        for head in graph.index_table[index][1]:
            heads.add(head)
    heads = list(heads - bases)
    heads.sort()
    return tuple(heads)

# ASSUMPTIONS:
# 0) head which don't appear in bases are tip heads. True?

# INVARIANTS:
# o every changeset must exist "in" one and only one index
#   -> contiguousness
# o the parent revs for all the changesets in every index
#   must exist in a previous index (though not necessarily the
#   immediate predecessor)
# o indices referenced by edges must exist
# o latest index must be set correctly
# o inices must be contiguous
# o FIRST_INDEX in index_table

