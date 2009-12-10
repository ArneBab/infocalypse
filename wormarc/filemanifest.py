""" Classes to address files stored in a WORMBlockArchive by
    human readable name.

    ATTRIBUTION: Contains source fragements written by Matt Mackall.

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
import shutil

from binaryrep import NULL_SHA, manifest_from_file, \
     manifest_to_file, get_file_sha, check_shas, str_sha

from archive import UpToDateException

def is_printable_ascii(value):
    """ Return True if all the characters in value are printable
        ASCII, False otherwise. """
    value = [ord(c) for c in value]
    # Hmmm... allow spaces
    return max(value) <= 0x7e and min(value) >= 0x20

#----------------------------------------------------------#

# Hmmmm... this feels horrifically overdesigned, but I need a way
# to decouple the data that you can insert into a manifest from
# the manifest implementation.
class IManifestEntry:
    """ Abstract base class for things that can be referenced
        from a FileManifest. """
    def __init__(self):
        pass

    def get_name(self):
        """ Returns the name to insert this entry under in the manifest. """
        #raise NotImplementedError()
        pass

    def make_file(self):
        """ Returns the full path to the data to insert.
            May create a temp file which it can clean up in release().
        """
        #raise NotImplementedError()
        pass

    # MUST call this.
    def release(self):
        """ Cleanup method called when the instance is no longer in use. """
        #raise NotImplementedError()
        pass

class FileManifest:
    """ An index which maps human readable names to files in an archive. """
    def __init__(self, name_map=None, history_sha=NULL_SHA):
        check_shas([history_sha, ])
        if name_map == None:
            name_map = {}

        # name ->  (file sha1, patch chain head sha1)
        self.name_map = name_map
        # Hmmmm... convenient, but it ties the manifest to an archive.
        self.stored_sha = history_sha

    @classmethod
    def from_archive(cls, archive, history_sha):
        """ Create a FileManifest from a file in the archive. """
        check_shas([history_sha, ])
        tmp_name = archive.blocks.tmps.make_temp_file()
        try:
            archive.get_file(history_sha, tmp_name)
            # Hmmmm... age... put back in manifest?
            name_map = manifest_from_file(tmp_name)
            return FileManifest(name_map, history_sha)
        finally:
            archive.blocks.tmps.remove_temp_file(tmp_name)

    # hmmmm... not to_archive, would expect that to be an instance member.
    @classmethod
    def write_manifest(cls, archive, name_map, history_sha):
        """ Helper, writes updated manifest to archive.
            Returns link.
        """
        check_shas([history_sha, ])
        # Add manifest
        tmp_file_name = archive.blocks.tmps.make_temp_file()
        try:
            manifest_to_file(tmp_file_name, name_map)
            return archive.write_new_delta(history_sha, tmp_file_name)
        finally:
            archive.blocks.tmps.remove_temp_file(tmp_file_name)

    def make_file_sha_map(self):
        """ INTERNAL: Make a file_sha -> (file_sha, patch_sha) map
            from name_map. """
        file_sha_map =  {}
        for name in self.name_map:
            pair = self.name_map[name]
            file_sha_map[pair[0]] = pair
        return file_sha_map

    # Doesn't change manifest or archive.
    def write_changes(self, archive, entry_infos, prev_manifest_sha=NULL_SHA):
        """ INTERNAL: Helper function for update().

            Writes the changes required to add the IManifestEntries
            in entries_infos to an archive.

            Raises UpToDateException if there are no changes.

            Return an (updated_name_map, manifest_sha) tuple. """

        check_shas([prev_manifest_sha, ])

        file_sha_map =  self.make_file_sha_map()
        new_name_map = {}
        updated = False

        for info in entry_infos:
            full_path = info.make_file()
            try:
                name = info.get_name()
                if not is_printable_ascii(name):
                    raise IOError("Non-ASCII name: %s" % repr(name))
                hash_info = self.name_map.get(name, None)
                file_sha = get_file_sha(full_path)
                if hash_info is None:
                    updated = True
                    if file_sha in file_sha_map:
                        # Renamed
                        new_name_map[name] = file_sha_map[file_sha]
                    else:
                        # REDFLAG: We lose history for files which are renamed
                        #          and modified.
                        # Created (or renamed and modified)
                        link = archive.write_new_delta(NULL_SHA, full_path)
                        new_name_map[name] = (file_sha, link[0])
                else:
                    if self.name_map[name][0] == file_sha:
                        # Exists in manifest and is unmodified.
                        new_name_map[name] = self.name_map[name]
                        continue

                    # Modified
                    updated = True
                    link = archive.write_new_delta(self.name_map[name][1],
                                                   full_path)
                    new_name_map[name] = (file_sha, link[0])

                # delete == ophaned history, NOP
            finally:
                info.release()

        if not updated:
            if (frozenset(new_name_map.keys()) ==
                frozenset(self.name_map.keys())):
                raise UpToDateException("The file manifest is up to date.")

        # Add updated manifest
        link = FileManifest.write_manifest(archive, new_name_map,
                                           prev_manifest_sha)

        return (new_name_map, link[0])

    # Only works if fully committed!
    def all_shas(self, archive):
        """ Return the SHA1 hashes of all history links required to store
            the files referenced by the manifest. """
        shas = [entry[1] for entry in self.name_map]
        shas.add(self.stored_sha)
        history_shas = set([])
        for value in shas:
            history_shas.union(set([link[0] for link in
                                    archive.blocks.get_history(value)]))
        return shas.union(history_shas)

    # Changes both the manifest and the archive.
    # other_head_shas is for other files in the archive not
    # handled by this manifest.
    def update(self, archive, entry_infos, other_head_shas=None,
               truncate_manifest_history=False):
        """ Update the manifest with the changes in entry infos and
            write the changes and the updated manifest into the archive. """
        if other_head_shas is None:
            other_head_shas = set([])

        check_shas(other_head_shas)

        archive.start_update()
        raised = True
        try:
            prev_sha = self.stored_sha
            if truncate_manifest_history:
                prev_sha = NULL_SHA

            new_names, root_sha = self.write_changes(archive,
                                                     entry_infos,
                                                     prev_sha)

            # History for all files except recently modified ones.
            old_shas = set([])

            new_shas = archive.uncommited_shas()

            for value in new_names.values():
                if value[1] in new_shas:
                    # Adding history for new values is handled by
                    # commit_update().
                    continue

                # We need to explictly add history for the files which
                # still exist in the manifest but didn't change.
                for link in (archive.blocks.get_history(value[1])):
                    old_shas.add(link[0])

            all_shas = archive.referenced_shas(old_shas.
                                               union(other_head_shas))

            archive.commit_update(all_shas)
            self.stored_sha = root_sha
            self.name_map = new_names
            raised = False
        finally:
            if raised:
                archive.abandon_update()


def verify_manifest(archive, manifest, brief=False):
    """ Debugging function to verify the integrity of a manifest. """
    failures = 0
    for name in manifest.name_map:
        tmp = archive.blocks.tmps.make_temp_file()
        file_sha, link_sha = manifest.name_map[name]
        if not brief:
            print "Verifying: %s  %s => %s)" % (name,
                                              str_sha(file_sha),
                                              str_sha(link_sha))
        archive.get_file(link_sha, tmp)
        history = archive.blocks.get_history(link_sha)
        if not brief:
            print "History: " + " ".join([str_sha(link[0])
                                          for link in history])

        retrieved_sha = get_file_sha(tmp)
        if retrieved_sha != file_sha:
            print "Expected: %s, but got %s." % (str_sha(file_sha),
                                                 str_sha(retrieved_sha))
            failures += 1
        else:
            if not brief:
                print "Ok. Read %i bytes." % os.path.getsize(tmp)

        archive.blocks.tmps.remove_temp_file(tmp)

    if failures > 0:
        print "%i entries failed to verify!" % failures
        assert False

def fix_backwards_slashes(name):
    """ Helper to fix backwards slashes in windows file names. """
    if os.sep != '\\' or name.find('\\') == -1:
        return name

    return '/'.join(name.split('\\'))

class PathEntry(IManifestEntry):
    """ IManifestEntry implementation for a path to a file on the
        local filesystem. """
    def __init__(self, full_path, name):
        IManifestEntry.__init__(self)
        self.full_path = full_path
        self.name = fix_backwards_slashes(name)

    def get_name(self):
        """ IManifestEntry implementation. """
        return self.name

    def make_file(self):
        """ IManifestEntry implementation. """
        return self.full_path


    # make_file(), release() are NOPs


# skips empty directories
# LATER: updates w/o sending all data?
# only send files which have changes since
# a local sha1 list file has changed, just send sha1s of others.
# LATER: add accept_regex?
def entries_from_dir(start_dir, recurse, ignore_regex=None, include_dirs=False):
    """ An iterator which yields FileManifestEntries for
        files in a directory. """
    stack = [start_dir]
    while len(stack) > 0:
        current_dir = stack.pop()
        names = os.listdir(current_dir)
        for name in names:
            if not ignore_regex is None and ignore_regex.match(name):
                continue
            full_path = os.path.join(current_dir, name)
            if os.path.isdir(full_path) and recurse:
                if include_dirs:
                    # Hack so that I can delete unreferenced dirs
                    # in manifest_to_dir
                    yield PathEntry(full_path, '')
                stack.append(full_path)
            if os.path.isfile(full_path):
                name = full_path[len(start_dir):]
                while len(name) > 0 and name.startswith(os.sep):
                    name = name[1:]
                if len(name) > 0:
                    yield PathEntry(full_path, name)

def find_dirs(name_map, target_dir):
    """ INTERNAL: Helper function used by manifest_to_dir(). """

    dirs = set([])
    for file_name in name_map:
        dir_name = os.path.dirname(os.path.join(target_dir, file_name))
        if not dir_name:
            continue # Hmmm
        if dir_name == os.sep:
            continue # Hmmm
        dirs.add(dir_name)

    return dirs

def read_local_dir(manifest, target_dir, dirs, ignore_regex):
    """ INTERNAL: Helper function used by manifest_to_dir(). """
    # Read local directory state.
    overwrite = set([])
    remove = {} # name -> path
    local_dirs = set([])
    extant = set([])
    for entry in entries_from_dir(target_dir, True, ignore_regex, True):
        name = entry.get_name()
        extant.add(name)
        full_path = entry.make_file()
        if name == '':
            # Because we told entries_from_dir to return directories.
            local_dirs.add(full_path)
            continue

        local_dirs.add(os.path.dirname(full_path))
        if name in manifest.name_map:
            overwrite.add(name)
        else: # skip directory entries
            remove[name] = entry.make_file()
        entry.release()

    # O(N*M) hmmm....
    # Remove non-leaf subdirectories.
    for stored_dir in dirs:
        for local_dir in local_dirs.copy():
            if stored_dir.startswith(local_dir):
                local_dirs.remove(local_dir)


    return (overwrite, remove, local_dirs, extant)

# Hmmm... wackamole code.
# REDFLAG: Other ways to make sleazy path references.
def validate_path(base_dir, full_path):
    """ Catch references to direcories above base_dir. """
    base_dir = os.path.abspath(base_dir)

    if type(full_path) is unicode:
        raise IOError("Unicode path name: %s" % repr(full_path))
    if not is_printable_ascii(full_path):
        raise IOError("Non-ASCII path name: %s" % repr(full_path))

    full_path = os.path.abspath(full_path)

    if not (len(full_path) > len(base_dir) and
            full_path.startswith(base_dir)):
        raise IOError("Hinky path in manifest: %s" % full_path)

# No error handling or cleanup.
# Doubt this will work on Windows, must handle backwards path sep.
def manifest_to_dir(archive, manifest, target_dir, ignore_regex=None,
                    dry_run=False):

    """ Update files in a local directory by extracting files in a manifest.

        WARNING. NOT WELL TESTED. POTENTIALLY DANGEROUS.
        PROBABLY BROKEN ON WINDOWS. """

    dirs = find_dirs(manifest.name_map, target_dir)

    overwrite, remove, local_dirs, extant = \
               read_local_dir(manifest, target_dir, dirs, ignore_regex)

    remove_dirs = local_dirs - dirs
    create = set(manifest.name_map.keys()) - extant
    if dry_run:
        return (create, overwrite, set(remove.keys()), remove_dirs)

    # Remove files
    for victim in remove.values():
        if os.path.exists(victim):
            validate_path(target_dir, victim)
            os.remove(victim)

    # Remove directories
    for victim in (remove_dirs):
        if os.path.exists(victim):
             # REDFLAG: I saw this fail silently once
            validate_path(target_dir, victim)
            shutil.rmtree(victim)
            assert not os.path.exists(victim)

    # Make directories that exist in manifest, but not locally.
    for dir_name in dirs:
        if not os.path.exists(dir_name):
            validate_path(target_dir, dir_name)
            os.makedirs(dir_name)

    # Copy files out of the archive, onto the local file system.
    for file_name in manifest.name_map:
        validate_path(target_dir, os.path.join(target_dir, file_name))
        archive.get_file(manifest.name_map[file_name][1],
                         os.path.join(target_dir, file_name))

    return (create, overwrite, set(remove.keys()), remove_dirs)

class RawDataTupleEntry(IManifestEntry):
    """ IManifestEntry implementation for a path to a file on the
        local filesystem. """
    def __init__(self, tmps, raw_tuple):
        IManifestEntry.__init__(self)
        self.tmps = tmps
        self.raw_tuple = raw_tuple
        self.full_path = None
    def get_name(self):
        """ IManifestEntry implementation. """
        return self.raw_tuple[0]

    def make_file(self):
        """ IManifestEntry implementation. """
        assert self.full_path is None
        self.full_path = self.tmps.make_temp_file()
        out_file = open(self.full_path, 'wb')
        try:
            out_file.write(self.raw_tuple[1])
        finally:
            out_file.close()

        return self.full_path

    # MUST call this.
    def release(self):
        """ IManifestEntry implementation. """
        if not self.full_path is None:
            self.tmps.remove_temp_file(self.full_path)
            self.full_path = None

        # REDFLAG: Does this really help garbage collection or just CC?
        self.raw_tuple = None
        self.tmps = None

def entries_from_seq(tmps, sequence):
    """ An iterator which yields FileManifestEntries from a sequence of
        (name, raw_data) tuples.

        REQUIRES: sequence not modified while iterating.
    """
    for value in sequence:
        yield RawDataTupleEntry(tmps, value)
