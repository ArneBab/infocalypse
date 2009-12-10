""" Classes to maintain an updateable file archive on top of
    bounded number of WORM (Write Once Read Many) blocks.

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
from binaryrep import NULL_SHA, write_raw_link, check_shas #, str_sha
from blocknames import BLOCK_SUFFIX, ReadWriteNames

# Just happens to be Freenet block size ;-)
MIN_BLOCK_LEN = 32 * 1024

MAX_HISTORY_LEN = 16

# 1 effectively causes a full reinsert when history chains are shortened.
# Larger values favor smaller incremental deltas at the expense of
# a longer history chain and larger total history size.
COALESCE_FACTOR = 1.5

#----------------------------------------------------------#

def is_ordered(partitions):
    """ Return True if the partitions are in ascending order,
        False otherwise. """

    # Ignore trailing 0 length blocks.
    lengths = [value[2] for value in partitions]
    while len(lengths) > 0 and lengths[-1] == 0:
        lengths = lengths[:-1]

    for index in range (0, len(lengths) - 1):
        #if lengths[index] >= lengths[index + 1]:
        if lengths[index] > lengths[index + 1]:
            return False
    return True

def is_contiguous(partitions):
    """ Return True if the block numbers in adjacent
        partitions are contiguous, False othewise. """
    if len(partitions) == 0:
        return True

    if partitions[-1][0] > partitions[-1][1]:
        return False # Hmmmm...

    for index in range (0, len(partitions) - 1):
        if partitions[index][0] > partitions[index][1]:
            return False # Hmmmm...
        span = partitions[index + 1][0] - partitions[index][1]
        if span < 0 or span > 1:
            return False

    return True

# [(start_block, end_block, length), ...]
def repartition(partitions, multiple=2):
    """ Merge newest to oldest until
        len(partition[n-1]) <= multiple * len(partition[n])
        for all partitions. """

    for index in range (0, len(partitions) - 1):
        if partitions[index][2] * multiple >= partitions[index + 1][2]:
            good = partitions[0:index]
            rest = partitions[index:]
            # Hmmm... if this is True, maybe you should simplify your rep.???
            assert rest[1][0] - rest[0][1] >= 0 and rest[1][0] - rest[0][1] < 2
            rest[1] = (rest[0][0], rest[1][1], rest[0][2] + rest[1][2])
            rest = rest[1:]
            ret = good + repartition(rest)
            assert is_ordered(ret)
            # Removed this constraint so I can drop empty partions
            # assert is_contiguous(ret)
            return ret

    ret = partitions[:] # Hmmmm
    assert is_ordered(ret)
    assert is_contiguous(ret)
    return ret

def compress(partitions, max_len, multiple=2):
    """ Reduce the length of the partitions to <= max_len.

        Drops zero length partitions. """

    partitions = partitions[:]
    partitions = [partition for partition in partitions
                  if  partition[2] > 0]

    if len(partitions) <= max_len:
        return partitions

    assert max_len > 1
    while len(partitions) > max_len:
        combined = (partitions[0][0], partitions[1][1],
                    partitions[0][2] + partitions[1][2])
        partitions[1] = combined
        # Enforce the ordering constraint.
        partitions = repartition(partitions[1:], multiple)

    assert is_ordered(partitions)
    return partitions

#----------------------------------------------------------#

class WORMBlockArchive:
    """ A file archive implemented on top of a bounded length sequence
        of Write Once Read Many blocks.

        Updating the archive means replacing one or more of the
        underlying blocks.

        The fundamental atom of storage is a 'history' link.  A
        history link contains an age, the sha1 hash of its parent
        link, and a blob of delta encoded change data.  Age is an
        integer which is incremented with every update to the
        archive. History links have at most one parent, but may have
        many children.

        The archive has an index which maps history link sha1 hashes
        to history links.

        Files are represented as chains of history links.  They are
        retrieved from the archive by running the delta decoding
        algorithm over all the patch blobs in the chain.  Files are
        addressable by the sha1 hash of the head link in the history
        chain.  The FileManifest class allows files in the archive to
        be accessed by human readable names.

        The start_update() method creates a temporary block for update
        writes. write_new_delta() writes a new history link into the
        temporary block.  commit_update() permanently adds the updates
        in the temporary block to the archive, re-writing blocks as
        necessary in order to bound the total number of blocks in the
        archive at max_blocks.

        There is no explict notion of deleting history links or files
        but unreferenced history links may be dropped whenever new
        blocks are created.

        The design for this module was influenced by looking at
        revlog.py in Mercurial, and to a lesser extent by reading
        about how git works.

        It was written to implement incrementally updateable file
        archives on top of Freenet.

    """
    def __init__(self, delta_coder, blocks):
        self.delta_coder = delta_coder
        self.delta_coder.get_data_func = self.get_data
        self.blocks = blocks
        self.max_blocks = 4
        # Hmmm...
        self.age = 0

    def create(self, block_dir, base_name, overwrite=False ):
        """ Create a new archive. """
        names = ReadWriteNames(block_dir, base_name, BLOCK_SUFFIX)
        self.age = self.blocks.create(names, self.max_blocks, overwrite)

    # Updateable.
    # LATER: read only???
    def load(self, block_dir, base_name, tags=None):
        """ Load an existing archive. """
        names = ReadWriteNames(block_dir, base_name, BLOCK_SUFFIX)
        self.age = self.blocks.load(names, self.max_blocks, tags)

    # MUST call this if you called load() or create()
    def close(self):
        """ Close the archive. """
        self.blocks.close()

    # Callback used by DeltaCoder.
    def get_data(self, link_sha, return_stream=False):
        """ INTERNAL: Helper function used by DeltaCoder to get raw
            change data. """
        assert not return_stream
        return self.blocks.link_map.get_link(link_sha, True)[3]

    # by head history link sha, NOT file sha
    def get_file(self, history_sha, out_file):
        """ Get a file by the sha1 hash of the head link in its
            history link chain. """
        check_shas([history_sha, ]) # hmmmm...
        if history_sha == NULL_SHA:
            tmp = open(out_file, 'wb')
            tmp.close()
            return

        self.delta_coder.apply_deltas(self.blocks.get_history(history_sha),
                                      out_file)

    # Hmmmm... too pedantic. how much faster would this run
    # if it were in BlockStorage?
    # DESIGN INTENT: BlockStorage shouldn't need to know
    #                about DeltaCoder.
    def write_new_delta(self, history_sha, new_file):
        """ Writes a new history link to the update file.

            history_sha can be NULL_SHA.

            Can ignore history. i.e. not link to previous history.

            Returns the new link.

            REQUIRES: is updating.
            REQUIRES: history_sha is present in the currently committed
                      version of the archive.
                      You CANNOT reference uncommited history links.
        """
        check_shas([history_sha, ])

        self.require_blocks()
        if self.blocks.update_file is None:
            raise Exception("Not updating.")

        history = self.blocks.get_history(history_sha)
        tmp_file = self.blocks.tmps.make_temp_file()
        old_file = self.blocks.tmps.make_temp_file()
        oldest_delta = self.blocks.tmps.make_temp_file()
        blob_file = None
        try:
            # REDFLAG: Think through.
            # It would make more sense for the delta coder to decide when to
            # truncate history, but I don't want to expose the full archive
            # interface to the delta coder implementation.
            if len(history) >= MAX_HISTORY_LEN:
                # Delta to original file.
                self.get_file(history[-1][0], old_file)
                parent0 = self.delta_coder.make_delta(history[-1:],
                                                      old_file,
                                                      new_file,
                                                      oldest_delta)
                # Full reinsert
                parent1 = self.delta_coder.make_full_insert(new_file,
                                                            tmp_file)

                #print "full: %i old: %i delta: %i target: %i" % (
                #     os.path.getsize(tmp_file),
                #     history[-1][6],
                #     os.path.getsize(oldest_delta),
                #     COALESCE_FACTOR * os.path.getsize(tmp_file))

                # LATER: Back to this.
                # This is bottom up history shortening driven by the most
                # recent changes.  We should also have some mechanism shortening
                # history (to 1 link) for files which haven't changed in many
                # updates, whenever blocks are merged.
                # Hmmmm... hard (impossible?) to decouple from manifest because
                # files are addressed by head history link sha
                if (COALESCE_FACTOR * os.path.getsize(tmp_file) <
                    (os.path.getsize(oldest_delta) + history[-1][6])):
                    parent = parent1
                    blob_file = tmp_file
                    #print "SHORTENED: FULL REINSERT"
                else:
                    #print "history:"
                    #for link in history:
                    #    print " ", str_sha(link[0]), str_sha(link[2])

                    parent = parent0
                    #print
                    #print "parent: ", str_sha(parent)

                    blob_file = oldest_delta
                    #print "SHORTENED: COMPRESSED DELTAS"
            else:
                self.get_file(history_sha, old_file)
                parent = self.delta_coder.make_delta(history, old_file,
                                                     new_file,
                                                     tmp_file)
                blob_file = tmp_file


            self.blocks.update_links.append(
                write_raw_link(self.blocks.update_stream,
                               self.age + 1, parent,
                               blob_file, 0))
            return self.blocks.update_links[-1]
        finally:
            self.blocks.tmps.remove_temp_file(old_file)
            self.blocks.tmps.remove_temp_file(oldest_delta)
            self.blocks.tmps.remove_temp_file(tmp_file)

    def require_blocks(self):
        """ INTERNAL: Raises if the BlockStorage delegate isn't initialized."""
        if self.blocks is None:
            raise Exception("Uninitialized. Run create() or load().")

    def start_update(self):
        """ Create temporary storage required to update the archive. """
        self.require_blocks()
        self.blocks.start_update()

    def abandon_update(self):
        """ Abandon all changes made to the archive since
            start_update() and free temporary storage. """
        if not self.blocks is None: # Hmmmm...
            self.blocks.abandon_update()

    # Allowed to drop history not in the referenced shas
    # list.
    #
    # Returns an (blocks_added, blocks_removed) tuple.
    def commit_update(self, referenced_shas=None):
        """ Permanently write changes into the archive. """

        self.require_blocks()
        if referenced_shas is None:
            referenced_shas = set([])
        self.age = self.blocks.commit_update(referenced_shas)
        self.compress(referenced_shas)


    # Restores length and ordering invariants.
    def compress(self, referenced_shas):
        """ Compresses the archive to fit in max_blocks blocks.

            REQUIRES: self.blocks.total_blocks() > max_blocks

            Merges blocks such that:
            n <= max_blocks
            and
            block[0] < block[1] ... < block[n -1]
        """

        if referenced_shas is None:
            referenced_shas = set([])

        check_shas(referenced_shas)

        #count = self.blocks.nonzero_blocks()

        # Compute the "real" size of each block without unreferenced links
        real_lens = [0 for dummy in range(0, len(self.blocks.tags))]

        for links in self.blocks.link_map.values():
            for link in links:
                if not link[0] in referenced_shas:
                    continue
                real_lens[link[5]] += link[6]

        uncompressed = [[index, index, real_lens[index]]
                        for index in range(0, len(self.blocks.tags))]

        compressed = compress(uncompressed, self.max_blocks)
        # Can't put lists in a set.
        compressed = [tuple(value) for value in compressed]
        uncompressed = [tuple(value) for value in uncompressed]

        if compressed == uncompressed:
            return False

        self.blocks.update_blocks(uncompressed, compressed,
                                         referenced_shas, self.max_blocks)
        return True

    def referenced_shas(self, head_sha_list, include_updates=True):
        """ Return the SHA1 hashes of all history links referenced by the
            links in the head_sha_list. """

        check_shas(head_sha_list)

        ret = set([])
        for head_sha in head_sha_list:
            for link in self.blocks.get_history(head_sha):
                ret.add(link[0])
        if include_updates:
            ret = ret.union(self.uncommited_shas())

        # Hmmm... frozenset faster?
        return ret

    def uncommited_shas(self):
        """ Return a set of SHA1 hash digests for history links that have
            been added since start_update().

            Note that get_file() fails for these SHA1 because the aren't
            commited yet. """
        return set([link[0] for link in
                    self.blocks.update_links])

class UpToDateException(Exception):
    """ Raised to signal that no changes were required to the archive.  """
    def __init__(self, msg):
        Exception.__init__(self, msg)

