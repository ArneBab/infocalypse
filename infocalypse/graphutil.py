""" Functions for manipulating UpdateGraphs.

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


from binascii import hexlify

from graph import FIRST_INDEX, MAX_PATH_LEN, UpdateGraph, \
     UpdateGraphException, canonical_path_itr

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

        # Example:
        # I:0:aaaaaaaaaaaa:|:bbbbbbbbbbbb:cccccccccccc
        lines.append(':'.join(('I', str(index), ':'.join(entry[0]), '|',
                               ':'.join(entry[1]))))

    # Edges
    index_pairs = graph.edge_table.keys()
    # MUST sort so you get the same CHK for the same graph instance.
    index_pairs.sort()
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
            fields.pop(0)
            try:
                if len(fields) == 0:
                    raise ValueError("HACK")
                index = int(fields.pop(0))
            except ValueError:
                raise ValueError("Syntax error reading index")
            try:
                divider = fields.index('|')
            except ValueError:
                raise ValueError("Syntax error reading index %i" % index)
            parents = fields[:divider]
            heads = fields[divider + 1:]

            if index in graph.index_table:
                print "OVERWRITING INDEX: " , index
            if len(parents) < 1:
                raise ValueError("index %i has no parent revs" % index)
            if len(heads) < 1:
                raise ValueError("index %i has no head revs" % index)

            graph.index_table[index] = (tuple(parents), tuple(heads))
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


def parse_v100_graph(text):
    """ Returns a graph parsed from text in old format.
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
            versions = tuple(fields[2:])
            graph.index_table[index] = ((versions[0], ), (versions[1], ))
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


def should_add_head(repo, version_table, head, to_index):
    """ INTERNAL: Helper function used by get_rollup_bounds. """
    children = [hexlify(child.node()) for child in repo[head].children()]
    for child in children:
        # Versions that we don't know about, don't count.
        # REDFLAG: Think this through
        if not child in version_table:
            #print "head: %s %s not in version table. IGNORING" \
            #      % (head[:12], child[:12])
            continue

        # REDFLAG: Check. This graph stuff makes me crazy.
        child_index = version_table[child]
        if child_index <= to_index:
            # Has a child  in or under the index we are rolling up to.
            # Not a head!
            #print "should_add_head -- returned False, %i:%s" %
            # (child_index, child[:12])
            return False

    #print "should_add_head -- returned True"
    return True

# TRICKY:
# You need the repository changset DAG in order to determine
# the base revs. because new changes might have branched from
# a change in the middle of a previous index which doesn't
# appear explictly in the graph.
#
def get_rollup_bounds(graph, repo, from_index, to_index, version_table):
    """ Return a  ((parent_revs, ), (head_revs,) tuple required to
        create a bundle with all the changes [from_index, to_index] (inclusive).
    """
    assert from_index <= to_index
    assert to_index > FIRST_INDEX
    #print "get_rollup_bounds -- ", from_index, to_index
    #print "version_table:", len(version_table)

    new_bases = set([])
    new_heads = set([])

    for index in range(from_index, to_index + 1):
        bases, heads = graph.index_table[index]
        for base in bases:
            #print "   considering base ", base[:12], version_table[base]
            if version_table[base] < from_index:
                #print "   adding base ", base[:12], version_table[base]
                new_bases.add(base)
        for head in heads:
            if should_add_head(repo, version_table, head, to_index):
                new_heads.add(head)

    new_bases = list(new_bases)
    new_bases.sort()
    new_heads = list(new_heads)
    new_heads.sort()

    #print "get_rollup_bounds -- returning"
    #print "   bases: ", new_bases
    #print "   heads: ", new_heads
    assert len(new_bases) > 0
    assert len(new_heads) > 0
    return (tuple(new_bases), tuple(new_heads))


# call this from subgraph
def coalesce_indices(original_graph, graph, repo, version_table):
    """ INTERNAL: Coalesce changes so that indices (and changes)
        are contiguous. """
    original_graph.rep_invariant()
    # graph invariants are broken !
    assert FIRST_INDEX in graph.index_table
    assert len(graph.index_table) > 1

    # Roll up info in  missing indices into existing ones.
    lacuna = False
    prev_index = graph.latest_index
    for index in range(graph.latest_index - 1, FIRST_INDEX -1, -1):
        # REDFLAG: There was a bad bug here. Better testing?
        if index in graph.index_table:
            if lacuna:
                # Rollup all changes in the missing indices into
                # the latest one.
                graph.index_table[prev_index] = (
                    get_rollup_bounds(original_graph,
                                      repo,
                                      index + 1,
                                      prev_index,
                                      version_table))
                lacuna = False
            prev_index = index
        else:
            lacuna = True
    # Hmmm... or graph is empty
    assert lacuna == False

    # Make indices contiguous.
    indices = graph.index_table.keys()
    indices.sort()

    assert indices[0] == FIRST_INDEX
    assert FIRST_INDEX == -1

    fixups = {}
    for ordinal, value in enumerate(indices):
        fixups[value] = ordinal - 1

    new_indices = {}
    for old_index in indices:
        # Ok not to copy, value is immutable (a tuple).
        new_indices[fixups[old_index]] = graph.index_table[old_index]

    new_edges = {}
    for edge in graph.edge_table:
        # Deep copy? Nothing else has a ref to the values.
        new_edges[(fixups[edge[0]], fixups[edge[1]])] = graph.edge_table[edge]

    graph.index_table.clear()
    graph.edge_table.clear()
    graph.index_table.update(new_indices)
    graph.edge_table.update(new_edges)
    graph.latest_index = max(graph.index_table.keys())

    original_graph.rep_invariant()
    #print "FAILING:"
    #print graph_to_string(graph)
    graph.rep_invariant()

def subgraph(graph, repo, version_table, containing_paths):
    """ Return a subgraph which contains the vertices and
    edges in containing_paths. """
    graph.rep_invariant()
    small_graph = UpdateGraph()
    max_index = -1

    # Copy edges and indices.
    for path in containing_paths:
        for step in path:
            pair = step[:2]
            # REDFLAG: copies ALL redundant paths
            small_graph.edge_table[pair] = graph.edge_table[pair][:]
            for index in pair:
                if index not in small_graph.index_table:
                    # Don't need to deep copy because index info is
                    # immutable. (a tuple)
                    small_graph.index_table[index] = graph.index_table[index]
                max_index = max(max_index, index)

    small_graph.latest_index = max_index

    # Fix contiguousness.
    coalesce_indices(graph, small_graph, repo, version_table)

    # Invariants should be fixed.
    small_graph.rep_invariant()
    graph.rep_invariant()

    return small_graph

# REDFLAG: TERMINATE when all edges in graph have been yielded?
def important_edge_itr(graph, known_paths):
    """ INTERNAL: A generator which returns a sequence of edges in order
        of descending criticalness."""
    known_edges = set([])
    for path in known_paths:
        for edge in path:
            known_edges.add(edge)

    # Edges which are in the canonical path
    index = graph.latest_index
    canonical_paths = canonical_path_itr(graph, 0, index, MAX_PATH_LEN)

    for path in canonical_paths:
        for edge in path:
            if edge in known_edges:
                continue
            known_edges.add(edge)
            yield edge

    if index == 0:
        return

    # Then add bootstrap paths back to previous indices
    # Favors older edges.
    for upper_index in range(index - 1, FIRST_INDEX, - 1):
        canonical_paths = canonical_path_itr(graph, 0, upper_index,
                                             MAX_PATH_LEN)
        for path in canonical_paths:
            for edge in path:
                if edge in known_edges:
                    continue
                known_edges.add(edge)
                yield edge
    return

# Really slow
def minimal_graph(graph, repo, version_table, max_size=32*1024,
                  formatter_func=graph_to_string):
    """ Returns a subgraph that can be formatted to <= max_size
        bytes with formatter_func. """

    length = len(formatter_func(graph))
    if length <= max_size:
        #print "minimal_update_graph -- graph fits as is: ", length
        # Hmmm... must clone for consistent semantics.
        return graph.clone()

    index = graph.latest_index
    assert index > FIRST_INDEX

    # All the edges that would be included in the top key.
    # This includes the canonical bootstrap path and the
    # two cheapest updates from the previous index.
    paths = [[edge, ] for edge in graph.get_top_key_edges()]
    minimal = subgraph(graph, repo, version_table, paths)
    length = len(formatter_func(minimal))
    if length > max_size:
        raise UpdateGraphException("Too big with only required paths (%i > %i)"
                                   % (length, max_size))

    prev_minimal = minimal.clone()

    for edge in important_edge_itr(graph, paths):
        paths.append([edge, ])
        minimal = subgraph(graph, repo, version_table, paths)
        length = len(formatter_func(minimal))
        if length > max_size:
            return prev_minimal
        else:
            prev_minimal = minimal.clone()

    return prev_minimal

