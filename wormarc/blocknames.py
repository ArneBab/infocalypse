""" Classes used by BlockStorage to map block ordinals to file names.

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

# Grrrr... separate file to avoid circular dependency.


import os

BLOCK_SUFFIX = '.bin'

class BlockNames:
    """ ABC to map ordinals to file names. """
    def __init__(self, read_only):
        self.read_only = read_only

    def read_path(self, ordinal):
        """ Return a file name to read the block from. """
        raise NotImplementedError()

    def write_path(self, ordinal):
        """ Return a file name to write the block to.
            This can raise a ValueError if the blocks are read only.
        """
        if self.read_only:
            raise ValueError("Blocks are read only!")
        return self.read_path(ordinal)

class ReadWriteNames(BlockNames):
    """ A naming policy for an updateable set of blocks. """
    def __init__(self, block_dir, block_name, suffix):
        BlockNames.__init__(self, False)
        self.block_dir = block_dir
        self.block_name = block_name
        self.suffix = suffix

    def read_path(self, ordinal):
        """ Implement pure virtual. """
        return os.path.join(self.block_dir, "%s_%s%s" %
                            (self.block_name,
                             str(ordinal),
                             self.suffix))

# UNTESTED!
# DESIGN INTENT: Adapter that allows you to load a BlockStorage from
#                a static cache of CHK blocks.
class ReadOnlyNames(BlockNames):
    """ A naming policy for a read only set of blocks. """
    def __init__(self, read_only_file_names):
        BlockNames.__init__(self, True)
        self.file_names = read_only_file_names

    def read_path(self, ordinal):
        """ Implement pure virtual. """
        if ordinal < 0 or ordinal >= len(self.file_names):
            raise IndexError("No such file: %i" % ordinal)
        return self.file_names[ordinal]

