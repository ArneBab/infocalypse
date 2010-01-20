""" Classes to support overlayed file writing so that that piki can edit
    without modifying the original copy.

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
import codecs
import os
import stat

# NOTE: There are hard coded references to utf8 in piki.py, submission.py
#       and hgoverlay.py.  Look there before changing this value.
WIKITEXT_ENCODING = "utf8"

class IFileFunctions:
    """ An ABC for file system operations. """
    def __init__(self, base_path):
        self.base_path = base_path
    def overlay_path(self, path):
        """ Return the path that writes should be written to. """
    def write(self, path, bytes, mode='wb'):
        """ Write a file. """
        raise NotImplementedError()
    def read(self, path, mode='rb', dummy_non_overlayed=False):
        """ Read a file. """
        raise NotImplementedError()
    def exists(self, path, dummy_non_overlayed=False):
        """ Return True if the file exists, False otherwise. """
        raise NotImplementedError()
    def modtime(self, path, dummy_non_overlayed=False):
        """ Return the modtime for the file."""
        raise NotImplementedError()
    def list_pages(self, path, dummy_non_overlayed=False):
        """ Return a list of all pages. """
        raise NotImplementedError()
    def has_overlay(self, path):
        """ Return True if there's an overlay for the file, False otherwise. """
        raise NotImplementedError()
    def remove_overlay(self, path):
        """ Remove the overlayed version of the file. """
        raise NotImplementedError
    def is_overlayed(self):
        """ Return True if the instance supports overlaying, False
            otherwise. """
        raise NotImplementedError

class DirectFiles(IFileFunctions):
    """ An IFileFunctions implementation which writes directly to
        the file system. """
    def __init__(self, base_path):
        IFileFunctions.__init__(self, base_path)

    def overlay_path(self, path):
        """ IFileFunctions implementation. """
        return path

    def write(self, path, bytes, mode='wb'):
        """ IFileFunctions implementation. """
        # There were hacks in the original piki code
        # to handle nt refusing to rename to an existing
        # file name.  Not sure if it is a problem on
        # modern windows.
        tmp_name = path + '.__%s__' % str(os.getpid())
        try:
            out_file = codecs.open(tmp_name, mode, WIKITEXT_ENCODING)
            try:
                if len(bytes) > 0: # Truncating is allowed.
                    out_file.write(bytes)
            finally:
                out_file.close()

            if os.path.exists(path):
                os.remove(path)

            os.rename(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)

    def read(self, path, mode='rb', dummy_non_overlayed=False):
        """ IFileFunctions implementation. """

        in_file = codecs.open(path, mode, WIKITEXT_ENCODING)
        try:
            return in_file.read()
        finally:
            in_file.close()

    def exists(self, path, dummy_non_overlayed=False):
        """ IFileFunctions implementation. """
        return os.path.exists(path)

    def modtime(self, path, dummy_non_overlayed=False):
        """ IFileFunctions implementation. """
        return os.stat(path)[stat.ST_MTIME]

    def list_pages(self, path, dummy_non_overlayed=False):
        """ IFileFunctions implementation. """
        return [name for name in os.listdir(path)
                if (os.path.isfile(os.path.join(path, name)) and
                    not os.path.islink(os.path.join(path, name)))]

    def has_overlay(self, dummy_path):
        """ IFileFunctions implementation. """
        return False

    def remove_overlay(self, dummy_path):
        """ IFileFunctions implementation. """
        assert False

    def is_overlayed(self):
        """ IFileFunctions implementation. """
        return False

OVERLAY_DIR = 'OVERLAY'

class OverlayedFiles(DirectFiles):
    """ An IFileFunctions implementation which overlays writes into a separate
        parallel OVERLAY directory.

        e.g. if:
        base_dir == /foo/bar/baz
        then,
        path == /foo/bar/baz/snafu.txt
        maps to,
        overlay == /foo/bar/OVERLAY/snafu.txt
    """
    def __init__(self, base_path):
        DirectFiles.__init__(self, base_path)

    def overlay_path(self, path):
        """ Return the path that overlayed writes should be written to. """
        path = os.path.abspath(path)
        assert path.startswith(self.base_path)
        rest = path[len(self.base_path):]
        if rest.startswith(os.sep):
            rest = rest[len(os.sep):]

        overlay_base = os.path.split(self.base_path)[0] # Hmmm... errors?

        overlayed = os.path.join(os.path.join(overlay_base, OVERLAY_DIR),
                                 rest)
        return overlayed

    # You cannot write to the non-overlayed files.
    def write(self, path, bytes, mode='wb'):
        """ IFileFunctions implementation. """
        DirectFiles.write(self, self.overlay_path(path), bytes, mode)

    def read(self, path, mode='rb', non_overlayed=False):
        """ IFileFunctions implementation. """
        if non_overlayed:
            return DirectFiles.read(self, path, mode)

        overlayed = self.overlay_path(path)
        if os.path.exists(overlayed):
            return DirectFiles.read(self, overlayed, mode)

        return DirectFiles.read(self, path, mode)

    # Zero length file means delete.
    def exists(self, path, non_overlayed=False):
        """ IFileFunctions implementation. """
        if non_overlayed:
            return DirectFiles.exists(self, path)

        overlay = self.overlay_path(path)
        if os.path.exists(overlay):
            if os.path.getsize(overlay) == 0:
                return False
            else:
                return True

        return os.path.exists(path)

    def modtime(self, path, non_overlayed=False):
        """ IFileFunctions implementation. """
        if non_overlayed:
            return DirectFiles.modtime(self, path)

        overlay = self.overlay_path(path)
        if os.path.exists(overlay) and os.path.getsize(overlay) > 0:
            return DirectFiles.modtime(self, overlay)

        return DirectFiles.modtime(self, path)

    def list_pages(self, path, non_overlayed=False):
        """ IFileFunctions implementation. """
        if non_overlayed:
            return DirectFiles.list_pages(self, path)

        overlay = self.overlay_path(path)
        overlay_pages = set([])
        if os.path.exists(overlay):
            overlay_pages = set(DirectFiles.list_pages(self, overlay))

        deleted = set([])
        for name in overlay_pages:
            if os.path.getsize(os.path.join(overlay, name)) == 0:
                deleted.add(name)

        return list(overlay_pages.union(
            set(DirectFiles.list_pages(self, path)) - deleted))

    # Hmmmm... Returns True for zero length file. i.e. "mark to delete"
    def has_overlay(self, path):
        """ IFileFunctions implementation. """
        return os.path.exists(self.overlay_path(path))

    def remove_overlay(self, path):
        """ IFileFunctions implementation. """
        overlay = self.overlay_path(path)
        if os.path.exists(overlay):
            os.remove(overlay)

    def is_overlayed(self):
        """ IFileFunctions implementation. """
        return True

def get_file_funcs(base_path, is_overlayed=False):
    """ Returns an overlayed IFileFunctions implementation if
        is_overlayed is True, and a direct implementation otherwise. """
    if not is_overlayed:
        return DirectFiles(base_path)

    return OverlayedFiles(base_path)

def remove_redundant_files(overlay, wikitext_dir, out_func=lambda msg:None):
    """ Removes files which are identical in the overlayed and non-overlayed
        directories.

        Also removes empty deletion marker files for files which have
        been deleted from the non-overlayed directory. """

    assert overlay.is_overlayed()
    for name in overlay.list_pages(wikitext_dir):
        full_path = os.path.join(wikitext_dir, name)
        if overlay.has_overlay(full_path):
            if not overlay.exists(full_path, True):
                if len(overlay.read(full_path, 'rb', False)) == 0:
                    overlay.remove_overlay(full_path)
                    out_func("Removed redundant overlayed file: %s" % name)
                continue

            if (overlay.read(full_path, 'rb', False) ==
                overlay.read(full_path, 'rb', True)):
                overlay.remove_overlay(full_path)
                out_func("Removed redundant overlayed file: %s" % name)
                continue

