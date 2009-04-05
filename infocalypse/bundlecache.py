""" Helper class used by InsertingBundles to create hg bundle files
    and cache information about their sizes.

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
import random

from mercurial import commands

from graph import FIRST_INDEX, FREENET_BLOCK_LEN, MAX_REDUNDANT_LENGTH

def make_temp_file(temp_dir):
    """ Make a temporary file name. """
    return os.path.join(temp_dir, '_tmp_' + ('%0.16f' % random.random())[2:14])

def is_writable(dir_name):
    """ Check whether the directory exists and is writable.  """
    tmp_file = os.path.join(dir_name, '_tmp_test_write')
    out_file = None
    try:
        try:
            out_file = open(tmp_file, 'wb')
            out_file.write('Can I write here?\n')
            return True
        except IOError:
            return False
        return True
    finally:
        if not out_file is None:
            out_file.close()
        if os.path.exists(tmp_file):
            os.remove(tmp_file)


class BundleException(Exception):
    """ An Exception for problems encountered with bundles."""
    def __init__(self, msg):
        Exception.__init__(self, msg)

class BundleCache:
    """ Class to create hg bundle files and cache information about
        their sizes. """

    def __init__(self, repo, ui_, base_dir):
        self.graph = None
        self.repo = repo
        self.ui_ = ui_
        self.redundant_table = {}
        self.base_dir = os.path.abspath(base_dir)
        assert is_writable(self.base_dir)
        self.enabled = True

    def get_bundle_path(self, index_pair):
        """ INTERNAL: Get the full path to a bundle file for the given edge. """
        start_info = self.graph.index_table[index_pair[0]]
        end_info = self.graph.index_table[index_pair[1]]
        return os.path.join(self.base_dir, "_tmp_%s_%s.hg"
                            % (start_info[1], end_info[1]))

    def get_cached_bundle(self, index_pair, out_file):
        """ INTERNAL: Copy the cached bundle file for the edge to out_file. """
        full_path = self.get_bundle_path(index_pair)
        if not os.path.exists(full_path):
            return None

        if not out_file is None:
            # can't do this for paths that don't exist
            #assert not os.path.samefile(out_file, full_path)
            if os.path.exists(out_file):
                os.remove(out_file)

            raised = True
            try:
                shutil.copyfile(full_path, out_file)
                raised = False
            finally:
                if raised and os.path.exists(out_file):
                    os.remove(out_file)

        return (os.path.getsize(full_path), out_file, index_pair)

    def update_cache(self, index_pair, out_file):
        """ INTERNAL: Store a file in the cache. """
        assert out_file != self.get_bundle_path(index_pair)

        raised = True
        try:
            shutil.copyfile(out_file, self.get_bundle_path(index_pair))
            raised = False
        finally:
            if raised and os.path.exists(out_file):
                os.remove(out_file)

    def make_bundle(self, graph, index_pair, out_file=None):
        """ Create an hg bundle file corresponding to the edge in graph. """
        #print "INDEX_PAIR:", index_pair
        assert not index_pair is None
        self.graph = graph

        cached = self.get_cached_bundle(index_pair, out_file)
        if not cached is None:
            #print "make_bundle -- cache hit: ", index_pair
            return cached

        delete_out_file = out_file is None
        if out_file is None:
            out_file = make_temp_file(self.base_dir)
        try:
            start_info = self.graph.index_table[index_pair[0]]
            end_info = self.graph.index_table[index_pair[1]]

            # Hmmm... ok to suppress mercurial noise here.
            self.ui_.pushbuffer()
            try:
                commands.bundle(self.ui_, self.repo, out_file,
                                None, base=[start_info[1]], rev=[end_info[1]])
            finally:
                self.ui_.popbuffer()

            if self.enabled:
                self.update_cache(index_pair, out_file)
            file_field = None
            if not delete_out_file:
                file_field = out_file
            return (os.path.getsize(out_file), file_field, index_pair)
        finally:
            if delete_out_file and os.path.exists(out_file):
                os.remove(out_file)

    # INTENT: Freenet stores data in 32K blocks.  If we can stuff
    # extra changes into the bundle file under the block boundry
    # we get extra redundancy for free.
    def make_redundant_bundle(self, graph, last_index, out_file=None):
        """ Make an hg bundle file including the changes in the edge and
            other earlier changes if it is possible to fit them under
            the 32K block size boundry. """
        self.graph = graph
        #print "make_redundant_bundle -- called for index: ", last_index

        if out_file is None and last_index in self.redundant_table:
            #print "make_redundant_bundle -- cache hit: ", last_index
            return self.redundant_table[last_index]

        size_boundry = None
        prev_length = None
        earliest_index = last_index - 1
        while earliest_index >= FIRST_INDEX:
            pair = (earliest_index, last_index)
            #print "PAIR:", pair
            bundle = self.make_bundle(graph,
                                      pair,
                                      out_file)

            #print "make_redundant_bundle -- looping: ", earliest_index, \
            #      last_index, bundle[0]
            assert bundle[0] > 0 # hmmmm

            if size_boundry is None:
                size_boundry = ((bundle[0] / FREENET_BLOCK_LEN)
                                * FREENET_BLOCK_LEN)
                prev_length = bundle[0]
                if (bundle[0] % FREENET_BLOCK_LEN) == 0:
                    # REDFLAG: test this code path
                    self.redundant_table[bundle[2]] = bundle
                    return bundle # Falls exactly on a 32k boundry
                else:
                    size_boundry += FREENET_BLOCK_LEN

                # Purely to bound the effort spent creating bundles.
                if bundle[0] > MAX_REDUNDANT_LENGTH:
                    #print "make_redundant_bundle -- to big for redundancy"
                    self.redundant_table[bundle[2]] = bundle
                    return bundle

            if bundle[0] > size_boundry:
                earliest_index += 1 # Can only happen after first pass???
                #print "make_redundant_bundle -- breaking"
                break

            earliest_index -= 1
            prev_length = bundle[0]

        bundle =  (prev_length, out_file,
                   (max(FIRST_INDEX, earliest_index), last_index))
        #           ^--- possible to fix loop so this is not required?

        #print "make_redundant_bundle -- return: ", bundle
        self.redundant_table[bundle[2]] = bundle
        return bundle

    def remove_files(self):
        """ Remove cached files. """
        for name in os.listdir(self.base_dir):
            # Only remove files that we created in case cache_dir
            # is set to something like ~/.
            if name.startswith("_tmp_"):
                os.remove(os.path.join(self.base_dir, name))

