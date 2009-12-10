""" Classes and functions to manage a local incremental archive cache.

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
import re
import shutil
import random

import archivetop
from fcpclient import get_version, get_usk_hash
from graph import MAX_METADATA_HACK_LEN
from archivesm import choose_word, chk_file_name, BLOCK_DIR, TMP_DIR, \
     TOP_KEY_NAME_FMT

# Archive stuff
from pathhacks import add_parallel_sys_path
add_parallel_sys_path('wormarc')
from blocks import BlockStorage, ITempFileManager
from archive import WORMBlockArchive, UpToDateException
from deltacoder import DeltaCoder
from filemanifest import FileManifest, entries_from_dir, manifest_to_dir

BLOCK_NAME = "block"

# REDFLAG: move to manifest.py ???
# An integer type constant for the root object in the archive.
# i.e. so that we know how to read it.
KIND_MANIFEST = 1 # 32 bit unsigned id value.

# MUST match blocknames.BLOCK_SUFFIX
BLOCK_NAME_FMT = BLOCK_NAME +"_%i.bin"

class HandleTemps(ITempFileManager):
    """ Delegate to handle temp file creation and deletion. """
    def __init__(self, base_dir):
        ITempFileManager.__init__(self)
        self.base_dir = base_dir

    def make_temp_file(self):
        """ Return a new unique temp file name including full path. """
        return os.path.join(self.base_dir, "__TMP__%s" %
                            str(random.random())[2:])

    def remove_temp_file(self, full_path):
        """ Remove and existing temp file. """

        if not full_path:
            return # Allowed.

        if not os.path.split(full_path)[-1].startswith("__TMP__"):
            raise IOError("Didn't create: %s" % full_path)

        if not os.path.exists(full_path):
            return
        os.remove(full_path)

def cache_dir_name(cache_dir, uri):
    """ Return the name of the cache directory. """
    return os.path.join(cache_dir, get_usk_hash(uri))

def cached_block(cache_dir, uri, block):
    """ Return the file name of a cached block. """
    for chk in block[1]:
        full_path = os.path.join(cache_dir_name(cache_dir, uri),
                                 chk_file_name(chk))
        if os.path.exists(full_path):
            if os.path.getsize(full_path) != block[0]:
                raise IOError("Wrong size: %s, expected: %i, got: %i" %
                              (full_path, block[0],
                               os.path.getsize(full_path)))
            #print "FOUND: ", chk
            return full_path
        #else:
        #    print "MISSING: ", chk

    raise IOError("Not cached: %s" % str(block))

def load_cached_top_key(cache_dir, uri):
    """ Return a top key tuple from a cached top key. """
    full_path = os.path.join(cache_dir_name(cache_dir, uri),
                             TOP_KEY_NAME_FMT % get_version(uri))

    in_file = open(full_path, 'rb')
    try:
        try:
            return archivetop.bytes_to_top_key_tuple(in_file.read())[0]
        except ValueError:
            # Remove the corrupt file from the cache.
            in_file.close()
            if os.path.exists(full_path):
                os.remove(full_path)
            raise
    finally:
        in_file.close()

# Means retrievable, NOT that every block is cached. Change name?
def verify_fully_cached(cache_dir, uri, top_key):
    """ Raise an IOError if all blocks in top_key aren't
        retrievable. """
    for block in top_key[0]:
        cached_block(cache_dir, uri, block)

def setup_block_dir(cache_dir, uri, top_key=None, copy_blocks=False,
                    pad_to=4):
    """ Create a temporary block directory for reading and writing
        archive blocks. """
    block_dir = os.path.join(cache_dir, BLOCK_DIR)
    if os.path.exists(block_dir):
        shutil.rmtree(block_dir) # Hmmmm...
    os.makedirs(block_dir)

    if copy_blocks:
        for index, block in enumerate(top_key[0]):
            src = cached_block(cache_dir, uri, block)
            dest = os.path.join(block_dir,
                                BLOCK_NAME_FMT % index)
            shutil.copyfile(src, dest)
        # 'pad' with empty block files.
        for index in range(len(top_key[0]), pad_to):
            dest = os.path.join(block_dir,
                                BLOCK_NAME_FMT % index)
            out_file = open(dest, 'wb')
            out_file.close()

    return block_dir


def create_archive(cache_dir, uri):
    """ Create a new archive. """
    block_dir = setup_block_dir(cache_dir, uri)

    tmps = HandleTemps(os.path.join(cache_dir, TMP_DIR))
    archive = WORMBlockArchive(DeltaCoder(), BlockStorage(tmps))
    archive.create(block_dir, BLOCK_NAME)

    return archive

def load_cached_archive(cache_dir, uri):
    """ Load an archive from the cache. """
    top_key = load_cached_top_key(cache_dir, uri)
    if len(top_key[1]) != 1 or top_key[1][0][1] != KIND_MANIFEST:
        raise Exception("Can't read manifest from archive.")

    verify_fully_cached(cache_dir, uri, top_key)

    # Clear previous block dir and copy cached blocks into it.
    block_dir = setup_block_dir(cache_dir, uri, top_key, True)

    tmps = HandleTemps(os.path.join(cache_dir, TMP_DIR))
    archive = WORMBlockArchive(DeltaCoder(), BlockStorage(tmps))
    archive.load(block_dir, BLOCK_NAME)

    # IMPORTANT: Set tags so we can keep track of
    #            unchanged blocks.
    for index in range(0, len(top_key[0])):
        archive.blocks.tags[index] = str(index)

    return top_key, archive

# Returns the files you need to insert and a provisional
# top key tuple with 'CHK@' for new files.
def provisional_top_key(archive, manifest, old_top_key, reinsert=False):
    """ Create a new top key which has place holder 'CHK@' block CHKs
        for new blocks.

        Return (file_list, top_key)

        where file_list is a list of files to insert the new CHKs
        from and top_key is the provisional top key tuple.
        """
    files =  []
    blocks = []
    for index, tag in enumerate(archive.blocks.tags):
        if reinsert or tag == '' or tag == 'new': # REDFLAG: make constant?
            full_path = archive.blocks.full_path(index)
            length = os.path.getsize(full_path)
            if length == 0:
                continue # Skip empty blocks.
            files.append(full_path)
            if length < MAX_METADATA_HACK_LEN:
                blocks.append((length, ['CHK@', 'CHK@'], archive.age))
            else:
                # MUST get the number of required CHKs right or
                # FixingUpTopkey will fail.
                blocks.append((length, ['CHK@', ], archive.age))
            continue
        blocks.append(old_top_key[0][int(tag)])

    provisional = (tuple(blocks), ((manifest.stored_sha, KIND_MANIFEST),),
                   archive.age)

    return files, provisional

# Archive MUST be fully locally cached.
def local_reinsert(cache_dir, uri):
    """ Return the top_key, file list info needed to fully reinsert
        the archive. """
    # Load cached topkey
    top_key, archive = load_cached_archive(cache_dir, uri)
    try:
        manifest = FileManifest.from_archive(archive, top_key[1][0][0])
        return provisional_top_key(archive, manifest, top_key, True)
    finally:
        archive.close()

# Only modifies local <cache_dir>/blocks directory.
# REQUIRES: cached topkey and blocks
def local_update(cache_dir, uri, from_dir):
    """ Update the archive by inserting deltas against from_dir. """
    # Load cached topkey
    top_key, archive = load_cached_archive(cache_dir, uri)
    try:
        # Load the old file manifest and use it to update.
        manifest = FileManifest.from_archive(archive, top_key[1][0][0])
        try:
            manifest.update(archive,
                            entries_from_dir(from_dir, True,
                                             make_skip_regex(cache_dir)))
        except UpToDateException:
            # Hmmm don't want to force client code
            # to import archive module
            return (None, None)

        return provisional_top_key(archive, manifest, top_key)
    finally:
        archive.close()
        
# A regex that skips the archive cache dir
def make_skip_regex(cache_dir):
    """ INTERNAL: Regular expression to ignore the local cache directory. """
    first, second = os.path.split(cache_dir)
    if second == '':
        first, second = os.path.split(first)
    assert not second == ''
    #print "SKIPPING: ", second
    return re.compile(".*%s$" % second.replace('.', '\.'))

def local_create(cache_dir, uri, from_dir):
    """ Create a new local archive. """

    # Load cached topkey
    archive = create_archive(cache_dir, uri)
    try:
        # Create an empty manifest and use it to update the archive.
        manifest = FileManifest()

        manifest.update(archive,
                        entries_from_dir(from_dir,
                                         True,
                                         make_skip_regex(cache_dir)))

        return provisional_top_key(archive, manifest, ((), (), 0))
    finally:
        archive.close()

# Overwrites!
# LATER: verify=False
def local_synch(ui_, cache_dir, uri, to_dir):
    """ Update to_dir from the archive in cache_dir.

        CAUTION: May delete files and directories.
    """

    top_key, archive = load_cached_archive(cache_dir, uri)
    try:
        # Load the old file manifest and use it to extract.
        manifest = FileManifest.from_archive(archive, top_key[1][0][0])

        result = manifest_to_dir(archive, manifest,
                                 to_dir, make_skip_regex(cache_dir))
        ui_.status(("Created: %i, Modified: %i, Removed: %i\n") %
                   (len(result[0]), len(result[1]), len(result[2])))

        if len(result[3]) > 0:
            ui_.status("Removed %i local %s.\n" % (len(result[3]),
                choose_word(result[3] == 1, "subdirectory", "subdirectories")))

    finally:
        archive.close()
