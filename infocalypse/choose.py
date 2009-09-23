""" Functions to choose which bundle to fetch next.

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

import random

from graph import MAX_PATH_LEN, block_cost, print_list, canonical_path_itr, \
     build_version_table

from graphutil import get_rollup_bounds
# This is the maximum allowed ratio of allowed path block cost
# to minimum full update block cost.
# It is used in low_block_cost_edges() to determine when a
# path is too expensive to include.
MAX_COST_RATIO = 2.0

def step_contains(step, index):
    """ Returns True if step contains index. """
    return index > step[0] and index <= step[1]

# REDFLAG: dog slow for long lists (worse than O(n^2))
def shuffle_edge_redundancy(graph, first, second, known):
    """ INTERNAL: Shuffle the redundancy for redundant edges in
        the values returned by get_update_edges.  """
    # Kick it Pascal style.
    def shuffle_one(graph, current, other, known):
        """ INTERNAL: shuffle redundancy for a single edge list.  """
        for index, edge in enumerate(current):
            if not graph.is_redundant(edge):
                continue # Not redundant

            new_ordinal = random.choice((0, 1))
            if new_ordinal == edge[2]:
                # NOP is a valid choice.
                continue

            alternate_edge = (edge[0], edge[1], new_ordinal)
            if alternate_edge in known: # fast
                # Already queued
                continue

            try:
                pos = other.index(alternate_edge) # slow
            except ValueError:
                pos = -1

            if pos != -1:
                # If already in other,  swap
                #print "shuffle_one -- swapped with other list %s -> %s" % \
                #      (str(current[index]), str(other[pos]))
                tmp = other[pos]
                other[pos] = current[index]
                current[index] = tmp

                continue
            try:
                pos = current.index(alternate_edge) # slow
            except ValueError:
                pos = -1

            if pos != -1:
                # If already in current
                #print "shuffle_one -- swapped in same list %s -> %s" % \
                #      (str(current[index]), str(current[pos]))
                current[pos] = current[index]
            #else:
            #    print "shuffle_one -- flipped  %s -> %s" % \
            #          (str(current[index]), str(alternate_edge))

            current[index] = alternate_edge

    assert len(set(first).intersection(known)) == 0
    assert len(set(second).intersection(known)) == 0

    # #ifdef _DEBUG_?  only used to check invariants.
    first_len = len(first)
    second_len = len(second)

    shuffle_one(graph, first, second, known)
    shuffle_one(graph, second, first, known)

    assert len(set(first).intersection(known)) == 0
    assert len(set(second).intersection(known)) == 0
    assert len(first) == first_len
    assert len(second) == second_len


# Returns the number of edges which contain the index.
def contained_edge_count(edges, index, max_count=None):
    """ INTERNAL: Helper function returns the number of edges
        which contain index. """
    count = 0
    for step in edges:
        assert not step is None
        if step_contains(step, index):
            count += 1
            if not max_count is None and count >= max_count:
                return count
    return count

# ORDER: Best candidates first.
# INTENT:
# Inserter does 2 33K updates. The last one causes a "rollup"
# of the entire multi-megabyte repo into one CHK.
# This code is intended to make sure that we try fetching the
# first 33k update  before the rollup CHK even though it means
# fetching more keys.
def low_block_cost_edges(graph, known_edges, from_index, allowed):
    """ INTERNAL:  Returns the best update edges that aren't too big. """
    # MAX_PATH_LEN - 1.  If it takes more steps you should be using
    # a canonical path.
    paths = graph.enumerate_update_paths(from_index + 1,
                                         graph.latest_index, MAX_PATH_LEN - 1)
    if len(paths) == 0:
        return []

    first = []

    with_cost = []
    for path in paths:
        total = 0
        for step in path:
            total += block_cost(graph.insert_length(step))
        with_cost.append((total, path))
    with_cost.sort()
    #for item in with_cost:
    #    print "COST: ", item[0], item

    # Ignore paths which are too much bigger than the shortest path.
    allowed_cost = int(with_cost[0][0] * MAX_COST_RATIO)
    #print "get_update_edges -- min block cost: ", \
    #   with_cost[0][0], allowed_cost
    # First steps of the paths with a cost <= allowed_cost
    first_steps = [[value[1][0], ] for value in with_cost if value[0]
                   <= allowed_cost]
    #print "FIRST_STEPS: ", first_steps
    first_steps.sort(graph.cmp_recency)
    for path in first_steps:
        assert len(path) == 1
        step = path[0]
        if step in known_edges:
            continue
        first.append(step)
        known_edges.add(step)
        allowed -= 1
        if allowed <= 0:
            break
    return first

# ORDER: Most canonical first.
# Best candidates at the end of the list
def canonical_path_edges(graph, known_edges, from_index, allowed):
    """ INTERNAL: Returns edges containing from_index from canonical paths. """
    # Steps from canonical paths
    # REDFLAG: fix, efficiency, bound number of path, add alternate edges
    #paths = graph.canonical_paths(graph.latest_index, MAX_PATH_LEN)
    paths = canonical_path_itr(graph, 0, graph.latest_index, MAX_PATH_LEN)

    second = []
    #print "get_update_edges -- after"
    for path in paths:
        # We need the tmp gook because the edges can overlap
        # and we want the most recent ones.
        tmp = []
        for step in path:
            assert not step is None
            if (step_contains(step, from_index + 1) and
                not step in known_edges):
                tmp.append([step, ])

        tmp.sort(graph.cmp_recency)
        for tmp_path in tmp:
            assert len(tmp_path) == 1
            assert not tmp_path[0] is None
            second.append(tmp_path[0])
            known_edges.add(tmp_path[0])
            allowed -= 1
            if allowed <= 0:
                return second

    return second

# STEP BACK:
# This function answers two questions:
# 0) What should I request to update as quickly as possible?
# A: Steps from paths <= MAX_COST_RATIO * (minimal block cost)
#    if there are any.
# 1) What should I request if that doesn't work.
# A: Most canonical.
#    Then backfill by recency.

# Will I always be able to fully enumerate search paths? or too slow

# REDFLAG: g'ter done code. Simplify and re-examine efficiency.
# REDFLAG: rename redundancy. redundancy == 2 -> 2 paths, NOT 3

# Returns (first_choice_steps, second_choice_steps)
def get_update_edges(graph, from_index, redundancy, shuffle_redundancy=False,
                     known_edges=None):
    """ Gets edges not already in known edges which could be used to
        update (pull). """

    if known_edges is None:
        known_edges = set([])

    assert not None in known_edges

    allowed = redundancy - contained_edge_count(known_edges, from_index + 1,
                                                redundancy)
    if allowed <= 0:
        # Bail out if we already have enough edges.
        return ([], [])

    original_known = known_edges
    known_edges = known_edges.copy()

    # 0) First get some low block cost paths.
    # Hmmm... make allowed cheap edges a parameter
    first = low_block_cost_edges(graph, known_edges, from_index,
                                 min(2, allowed))

    allowed -= len(first)
    second = []
    if allowed > 0:
        # 1) Then get edges from canonical path
        second = canonical_path_edges(graph, known_edges,
                                      from_index, allowed)

        # Resort by recency.
        second_paths = [[edge, ] for edge in second]
        second_paths.sort(graph.cmp_recency)
        second = [path[0] for path in second_paths]

        allowed -= len(second)

    if allowed > 0:
        # 2) Finally backfill with most recent other edges which
        # advance us at least one step.
        containing_paths = [[edge, ] for edge in
                            graph.contain(from_index + 1)
                            if edge not in known_edges]

        containing_paths.sort(graph.cmp_recency)

        for path in containing_paths:
            second.insert(0, path[0])
            known_edges.add(path[0])
            allowed -= 1
            if allowed <= 0:
                break

    # Hmmmm... previously I was always sorting second by recency.
    if shuffle_redundancy:
        shuffle_edge_redundancy(graph, first, second, original_known)

    #print "get_update_edges -- exiting", len(first), len(second)
    if len(first) == 0 and len(original_known) == 0:
        # HACK: Prevents stall waiting for the second graph CHK
        # when bootstrapping from an empty repo.
        first = list(second)
        second = []

    return (first, list(second))

def dump_update_edges(first, second, all_edges):
    """ Debugging function to print update edges. """
    print "--- update edges --- "
    print_list("known edges  :", all_edges)
    print_list("first choice :", first)
    print_list("second choice:", second)
    print "---"

def get_top_key_updates(graph, repo, version_table=None):
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

    if version_table is None:
        version_table = build_version_table(graph, repo)
    ret = []
    for edge in coalesced_edges:
        parents, latest = get_rollup_bounds(graph, repo,
                                             edge[0] + 1, edge[1],
                                             version_table)

        length = graph.get_length(edge)
        assert len(graph.edge_table[edge][1:]) > 0

        #(length, parent_rev, latest_rev, (CHK, ...))
        update = (length, parents, latest,
                  graph.edge_table[edge][1:],
                  True, True)
        ret.append(update)


    # Stuff additional remote heads into first update.
    result = get_rollup_bounds(graph,
                               repo,
                               0,
                               graph.latest_index,
                               version_table)

    for head in ret[0][2]:
        if not head in result[1]:
            print "Expected head not in all_heads!", head[:12]
            assert False

    #top_update = list(ret[0])
    #top_update[2] = tuple(all_heads)
    #ret[0] = tuple(top_update)

    ret[0] = list(ret[0])
    ret[0][2] = tuple(result[1])
    ret[0] = tuple(ret[0])

    return ret

def build_salting_table(target):
    """ INTERNAL: Build table used to keep track of metadata salting. """
    def traverse_candidates(candidate_list, table):
        """ INTERNAL: Helper function to traverse a single candidate list. """
        for candidate in candidate_list:
            if candidate[6]:
                continue
            edge = candidate[3]
            value = table.get(edge, [])
            value.append(candidate[2])
            table[edge] = value
    ret = {}
    traverse_candidates(target.pending_candidates(), ret)
    traverse_candidates(target.current_candidates, ret)
    traverse_candidates(target.next_candidates, ret)
    return ret

# REDFLAG: get rid of unused methods.
# Hmmm... feels like coding my way out of a design problem.
class SaltingState:
    """ INTERNAL: Helper class to keep track of metadata salting state.
    """
    def __init__(self, target):
        self.table = build_salting_table(target)

    def full_request(self, edge):
        """ Return True if a full request is scheduled for the edge. """
        if not edge in self.table:
            return False

        for value in self.table[edge]:
            if not value:
                return True
        return False

    def add(self, edge, is_partial):
        """ Add an entry to the table. """
        value = self.table.get(edge, [])
        value.append(is_partial)
        self.table[edge] = value

    def needs_full_request(self, graph, edge):
        """ Returns True if a full request is required. """
        assert len(edge) == 3
        if not graph.is_redundant(edge):
            return False
        return not (self.full_request(edge) or
                    self.full_request((edge[0], edge[1], int(not edge[2]))))
