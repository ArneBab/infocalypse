""" Classes to store collections of archive history links in files.

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

# REDFLAG: CLEANUP ERROR HANDLING. Failures can lose or corrupt blocks!

import os

from archive import MIN_BLOCK_LEN, UpToDateException
from linkmap import LinkMap
from binaryrep import NULL_SHA, copy_raw_links

# REDFLAG: rtfm python tempfile module. is this really needed?
class ITempFileManager:
    """ Delegate to handle temp file creation and deletion. """
    def __init__(self):
        pass
    def make_temp_file(self):
        """ Return a new unique temp file name including full path. """
        raise NotImplementedError()
    def remove_temp_file(self, name):
        """ Remove and existing temp file. """
        raise NotImplementedError()

def has_internal_zero(sequence):
    """ Return True if the sequence has a zero to non-zero transition,
        False otherwise. """
    saw_zero = False
    for value in sequence:
        if value == 0:
            saw_zero = True
        else:
            if saw_zero:
                return True
    return False

# DESIGN INTENT: Push file system dependancies out of archive code.
class BlockStorage:
    """ A class to store history links in a collection of files. """
    def __init__(self, tmps, name_policy=None):
        self.tmps = tmps
        self.names = name_policy
        self.tags = ['', ] # Is also a proxy for length.

        self.link_map = None

        # Hmmmm... file and stream belong in storage
        # but links belongs in the archive....
        self.update_file = None
        self.update_stream = None
        self.update_links = []

    def is_updating(self):
        """ Return True if updating, False otherwise. """
        # Hmmm...
        return not self.update_stream is None

    def close(self):
        """ Close the files. """
        self.abandon_update()
        if self.link_map is None:
            return
        self.link_map.close()
        self.link_map = None

    def full_path(self, ordinal, read=True):
        """ Return the full path to an underlying block file. """
        if read:
            return self.names.read_path(ordinal)

        return self.names.write_path(ordinal)

    def get_history(self, head_sha1):
        """ Return the history link chain which has a head link with hash
            head_sha1. """
        if head_sha1 == NULL_SHA:
            return []
        ret = []
        head = head_sha1
        while True:
            link = self.link_map.get_link(head)
            ret.append(link[:]) # Copy
            if link[2] == NULL_SHA:
                return ret
            head = link[2]

    def start_update(self):
        """ Create temporary storage required to write an update.

            You MUST call commit_update() or abandon_update() after
            calling this. """

        if not self.update_file is None:
            raise Exception("Commmit or abandon the previous update!")
        self.update_file = self.tmps.make_temp_file()
        self.update_links = []
        raised = True
        try:
            self.update_stream = open(self.update_file, "wb")
            raised = False
        finally:
            if raised:
                self.abandon_update()

    # UpToDateException is recoverable, all others fatal. i.e. zorch instance.
    def commit_update(self, referenced_shas=None):
        """ Permanently write changes into the archive.

            This creates a new block which may replace an
            existing one. """

        assert not referenced_shas is None

        if self.update_file is None or len(self.update_links) == None:
            UpToDateException("No changes to commit.")

        age = 0
        # Automagically add history for self and parents
        for link in self.update_links:
            age = max(age, link[1])
            # New link
            referenced_shas.add(link[0])
            # Previous history
            # TRICKY: You can't call get_history on the new link itself
            #         because it isn't commited yet.
            for child in self.get_history(link[2]):
                referenced_shas.add(child[0])

        try:
            self.update_stream.close()
            self.update_stream = None # see is_updating()
            self.add_block(referenced_shas)
            return age
        finally:
            # Always clean up, even on success.
            # EXCEPTIONS ARE FATAL!
            self.abandon_update()

    def abandon_update(self):
        """ Free temporary storage associated with an update without
            committing it. """

        self.update_links = []
        if not self.update_stream is None:
            self.update_stream.close()
            self.update_stream = None
        if not self.update_file is None:
            self.tmps.remove_temp_file(self.update_file)
            self.update_file = None

    #  Returns 0
    def create(self, name_policy, num_blocks, overwrite=False ):
        """ Initialize the instance by creating a new set of empty
            block files. """

        if name_policy.read_only:
            raise ValueError("Names are read only! Use load() instead?")
        self.names = name_policy
        self.tags = ['', ] # Length == 1
        if not overwrite:
            for ordinal in range(0, num_blocks):
                if os.path.exists(self.full_path(ordinal, False)):
                    raise IOError("Already exists: %s" %
                                  self.full_path(ordinal, False))

        for ordinal in range(0, num_blocks):
            out_file = open(self.full_path(ordinal, False), 'wb')
            out_file.close()

        return self.load(name_policy, num_blocks)

    # hmmmm... want to use hash names for blocks
    # blocks is [[file_name, desc, dirty], ...]
    # returns maximum age
    def load(self, name_policy, num_blocks, tags=None):
        """ Initialize the instance by loading from an existing set of
            block files. """

        self.names = name_policy
        if tags is None:
            tags = ['' for dummy in range(0, num_blocks)]

        # DESIGN INTENT: Meant for keeping track of Freenet CHK's
        assert len(tags) == num_blocks
        self.tags = tags[:]
        self.link_map = LinkMap()
        age, counts = self.link_map.read([self.full_path(ordinal, False)
                                          for ordinal in range(0, num_blocks)])
        assert not has_internal_zero(counts)
        if max(counts) == 0:
            self.tags = self.tags[:1] # Length == 1
        return age

    # Includes 0 length blocks
    def total_blocks(self):
        """ Return the total number of blocks including 0 length ones. """
        return len(self.tags)

    # Hmmmm... physical length.
    def nonzero_blocks(self):
        """ Return the number of non-zero length blocks. """
        for ordinal in range(0, len(self.tags)):
            if os.path.getsize(self.full_path(ordinal)) == 0:
                # Check for illegal internal zero length blocks.
                for index in range(ordinal + 1, len(self.tags)):
                    if os.path.exists(self.full_path(index)):
                        assert os.path.getsize(self.full_path(index)) == 0
                return ordinal

        return len(self.tags)

    # This may delete self.update_file, but caller is
    # still responsible for cleaning it up.
    #
    # Breaks length and ordering invariants.
    def add_block(self, referenced_shas, tag=''):
        """ INTERNAL: Add the temporary update file to the permanent
            block files. """

        assert not self.is_updating()
        assert not self.names.read_only
        update_len = os.path.getsize(self.update_file)
        head_len = os.path.getsize(self.full_path(0, False))
        tmp = None
        try:
            # Doesn't change length
            if update_len + head_len < MIN_BLOCK_LEN:
                # Link map has open file descriptors.
                # Must close or os.remove() below fails on Windows.
                self.link_map.close()

                # We might merge with an empty block here, but it
                # doesn't matter since the length is bounded. Do better?

                # Can just append to the first block.
                # [N + O1] ...
                tmp = self.merge_blocks((self.update_file,
                                         self.full_path(0, False)),
                                        referenced_shas)
                if os.path.exists(self.full_path(0, False)):
                    # REDFLAG: What if this fails?
                    os.remove(self.full_path(0, False))

                os.rename(tmp, self.full_path(0, False))
                self.tags[0] = tag

                # Deletes update file IFF we get here.
                tmp = self.update_file
                self.update_file = None

                # Fix the link map.
                fixups = {} # Drop links in head block.
                for index in range(1, self.total_blocks()):
                    fixups[index] = index
                # Potentially SLOW.
                self.link_map.update_blocks(fixups,
                                            [self.full_path(index, False)
                                             for index in
                                             range(0, self.total_blocks())],
                                            [0,]) # Read links from head block.
                return


            # Deletes update file always.
            tmp = self.update_file
            self.update_file = None

            # Close the link map before messing with files.
            self.link_map.close()

            self.prepend_block(tmp)
            self.tags.insert(0, tag) # Increments implicit length

            # Fix the link map.
            fixups = {}
            for index in range(0, self.total_blocks() - 1): # We inserted!
                fixups[index] = index + 1
            # Potentially SLOW.
            self.link_map.update_blocks(fixups,
                                        [self.full_path(index, False)
                                         for index in
                                         range(0, self.total_blocks())],
                                        [0,]) # Read links from head block.
        finally:
            self.tmps.remove_temp_file(tmp)

    # Returns tmp file with merged blocks.
    # Caller must delete tmp file.
    def merge_blocks(self, block_file_list, referenced_shas):
        """ INTERNAL: Merge blocks into a single file. """
        tmp = self.tmps.make_temp_file()
        copied_shas = set([])
        raised = True
        try:
            out_file = open(tmp, 'wb')
            try:
                for name in block_file_list:
                    in_file = open(name, "rb")
                    try:
                        # Hmmm... do something with count?
                        #count = copy_raw_links(in_file, out_file,
                        #                       referenced_shas)
                        copy_raw_links(in_file, out_file,
                                       referenced_shas, copied_shas)
                    finally:
                        in_file.close()
            finally:
                out_file.close()
            raised = False
            return tmp
        finally:
            if raised:
                self.tmps.remove_temp_file(tmp)

    # Implementation helper function, caller deals with file cleanup.
    # REQUIRES: new_block not an extant block file.
    def prepend_block(self, new_block):
        """ INTERNAL: Insert a new block at the head of the block list. """

        assert not self.is_updating()
        assert self.update_file is None
        # Shift all extant blocks up by one index
        for index in range(self.total_blocks() - 1, -1, -1):
            if os.path.exists(self.full_path(index + 1, False)):
                # REDFLAG: failure?
                os.remove(self.full_path(index + 1, False))
            # REDFLAG: failure?
            os.rename(self.full_path(index, False),
                      self.full_path(index + 1, False))
        # Now copy the update block into the 0 position.
        os.rename(new_block, self.full_path(0, False))


    def _make_new_files(self, new_blocks, referenced_shas, tmp_files):
        """ INTERNAL: Implementation helper for update_blocks(). """
        new_files = {}
        for partition in new_blocks:
            # Calling code should have already dropped empty blocks.
            new_files[partition] = self.merge_blocks([self.full_path(index,
                                                                     False)
                                                      for index in
                                                      range(partition[0],
                                                            partition[1]
                                                            + 1)],
                                                     referenced_shas)
            tmp_files.append(new_files[partition])
        return new_files

    def _remove_old_files(self, dropped_blocks):
        """ INTERNAL: Implementation helper for update_blocks(). """
        # Delete the files for dropped blocks
        for partition in dropped_blocks:
            assert partition[0] == partition[1]
            if not os.path.exists(self.full_path(partition[0], False)):
                continue
            os.remove(self.full_path(partition[0], False))

    def _copy_old_blocks(self, old_blocks, tmp_files):
        """ INTERNAL: Implementation helper for update_blocks(). """
        renamed = {}
        for partition in old_blocks:
            assert partition[0] == partition[1]

            src = self.full_path(partition[0], False)
            assert os.path.exists(src)
            dest = self.tmps.make_temp_file()
            tmp_files.append(dest)
            os.rename(src, dest)
            renamed[partition] = dest
        return renamed

    def _update_block_files(self, compressed, uncompressed,
                            referenced_shas, tmp_files):
        """ INTERNAL: Implementation helper for update_blocks(). """

        # Hmmm... to appease pylint max local vars constraint.
        #new_blocks = set(compressed) - set(uncompressed)
        old_blocks = set(compressed).intersection(set(uncompressed))
        #dropped_blocks = set(uncompressed) - old_blocks

        # Build new blocks in tmp files
        new_files = self._make_new_files(set(compressed) - set(uncompressed),
                                         referenced_shas,
                                         tmp_files)
        # Delete the files for dropped blocks
        self._remove_old_files(set(uncompressed) - old_blocks)
        # Move old blocks into tmp files
        renamed = self._copy_old_blocks(old_blocks, tmp_files)

        new_tags = ['' for dummy in range(0, len(compressed))]
        new_indices = []
        ordinal_fixups = {}
        # Rename blocks onto new block ordinals
        for index, block in enumerate(compressed): #hmmm not a set???
            dest = self.full_path(index, False)
            assert not os.path.exists(dest)
            if block in set(compressed) - set(uncompressed):
                os.rename(new_files[block], dest)
                new_tags[index] = 'new' # best we can do.
                new_indices.append(index)
                continue

            assert block in old_blocks
            os.rename(renamed[block], dest)
            # Copy the old tag value into the right position
            new_tags[index] = self.tags[block[0]]
            # Save info we need to fix the link_map
            ordinal_fixups[block[0]] = index
        self.tags = new_tags
        return (new_tags, new_indices, ordinal_fixups)

    # REDFLAG: Failure.
    def update_blocks(self, uncompressed, compressed, referenced_shas,
                      min_blocks):
        """ Repartition the underlying block files into the partitions
            described by compressed. """

        assert not self.is_updating()
        assert not self.names.read_only

        tmp_files = []
        try:
            self.link_map.close()
            self.tags, new_indices, ordinal_fixups = \
                       self._update_block_files(compressed, uncompressed,
                                                referenced_shas, tmp_files)

            # Drop links for unreferenced blocks and shift indices.
            # Then read links from new block files.
            self.link_map.update_blocks(ordinal_fixups,
                                        [self.full_path(index, False)
                                         for index in
                                         range(0, self.total_blocks())],
                                        new_indices)

            # Add trailing zero length blocks.
            for index in range(self.nonzero_blocks(), min_blocks):
                out_file = open(self.full_path(index, False), 'wb')
                out_file.close()
        finally:
            for name in tmp_files:
                self.tmps.remove_temp_file(name)


