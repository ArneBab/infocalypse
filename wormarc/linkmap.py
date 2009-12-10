""" A class to keep track of history links stored in a set of files.

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

from binaryrep import read_link, str_sha

class LinkMap(dict):
    """ A history link hash addressable index of the history links in
        a set of block files. """
    def __init__(self):
        dict.__init__(self)
        self.files = []

    def read(self, file_list, keep_data=False):
        """ Read the index from a collection of block files. """
        counts = [0 for dummy in range(0, len(file_list))]
        age = 0 # Hmmmm
        for index, name in enumerate(file_list):
            in_stream = open(name, 'rb')
            raised = True
            try:
                latest_age, count = self.read_from_stream(in_stream,
                                                          index, keep_data)
                age = max(age, latest_age)
                counts[index] = count
                raised = False
            finally:
                if raised or keep_data:
                    in_stream.close()
                else:
                    self.files.append(in_stream)
        return age, tuple(counts)


    def read_from_stream(self, in_stream, index, keep_data=False):
        """ Read links from a stream. """
        age = 0
        count = 0
        while True:
            link = read_link(in_stream, keep_data, in_stream.tell(),
                             index)
            if link is None:
                break

            age = max(age, link[1])
            prev = list(self.get(link[0], []))
            link = list(link) # REDFLAG: ??? tuple -> list -> tuple
            prev.append(tuple(link))
            self[link[0]] = tuple(prev)
            count += 1

        return age, count

    # SLOW, get rid of list copy?
    # fixups is a old_index -> new index map
    # Omit from fixups == delete
    def _update_block_ordinals(self, fixups):
        """ INTERNAL: Implementation helper for update_blocks(). """
        for sha_hash in self.keys():
            prev = self.get(sha_hash)
            updated = []
            for link in prev:
                assert link[0] == sha_hash
                if not link[5] in fixups:
                    continue # Dropped block
                link = list(link)
                link[5] = fixups[link[5]]
                updated.append(tuple(link))
            if len(updated) > 0:
                self[sha_hash] = tuple(updated)
            else:
                del self[sha_hash]

    # Fixes ordinals in referenced links
    # Drops omited blocks
    # Closes and re-opens all file streams.
    # Loads links from the streams in new_indices.
    def update_blocks(self, fixups, file_list, new_indices, keep_data=False):
        """ Update the index to track addition, deletion and reordering of
        the underlying block files. """

        assert len(self.files) == 0 # must be closed.
        self._update_block_ordinals(fixups)
        self.files = []
        age = 0
        raised = True
        try:
            for index, name in enumerate(file_list):
                self.files.append(open(name, 'rb'))
                if not index in new_indices:
                    continue

                # Need to read links out of the new file.
                latest_age, dummy = self.read_from_stream(self.files[index],
                                                          index, keep_data)
                age = max(age, latest_age)
            raised = False
            return age
        finally:
            if raised:
                self.close()

    def close(self):
        """ Close the index. """
        for in_file in self.files:
            in_file.close()
        self.files = []

    def get_link(self, link_sha, need_data=False):
        """ Get a history link by its sha1 hash. """
        links = self.get(link_sha, None)
        if links is None:
            raise IOError("Unresolved link: " + str_sha(link_sha))

        assert len(links) > 0
        # REDFLAG: Fully think through.
        # The same link can exist in multiple files.
        link = links[0]

        if (not need_data) or (not link[3] is None):
            return link

        index = link[5]
        self.files[index].seek(link[4])
        ret = read_link(self.files[index], True)
        if ret is None:
            raise IOError("Couldn't read blob from disk.")

        assert ret[0] == link[0]
        assert ret[1] == link[1]
        assert ret[2] == link[2]
        assert not ret[3] is None
        assert ret[0] ==  link_sha
        return ret

def raw_block_read(link_map, ordinal):
    """ Read a single block file. """
    table = {}
    in_stream = link_map.files[ordinal]
    in_stream.seek(0)
    while True:
        start_pos = in_stream.tell()
        link = read_link(in_stream, False, start_pos, ordinal)
        # read_link() never returns None except for eof, right?
        # Otherwise we'd only do a partial read...
        if link is None:
            break
        entry = table.get(link[0], [])
        entry.append(link)
        table[link[0]] = entry
    return table

def links_by_block(link_map):
    """ INTERNAL: Implementation helper function for
        verify_link_map(). """
    tables = [{} for dummy in range(0, len(link_map.files))]
    for links in link_map.values():
        assert len(links) > 0
        for link in links:
            ordinal = link[5]
            assert ordinal >= 0 and ordinal < len(link_map.files)
            entry = tables[ordinal].get(link[0], [])
            entry.append(link)
            tables[ordinal][link[0]] = entry
    return tables

def verify_link_map(link_map):
    """ Debugging function to verify the integrity of a LinkMap instance. """

    assert len(link_map.files) > 0
    count = 0
    by_block = links_by_block(link_map)

    for ordinal in range(0, len(link_map.files)):
        raw_shas = raw_block_read(link_map, ordinal)
        # Hashes read from the raw file are the same as
        # the ones that the LinkMap thinks should be in the file.
        assert frozenset(raw_shas.keys()) == frozenset(by_block[ordinal].keys())

        # Now check values.
        for link_sha in raw_shas:
            assert (frozenset(raw_shas[link_sha]) ==
                    frozenset(by_block[ordinal][link_sha]))
            count += 1
    return count
