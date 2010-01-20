""" An IFileFunctions subclass which reads files from a particular version of
    an hg repo.

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
from mercurial import cmdutil

from pathhacks import add_parallel_sys_path
add_parallel_sys_path('fniki')
from fileoverlay import OverlayedFiles, DirectFiles, WIKITEXT_ENCODING

# ATTRIBUTION: Pillaged from commands.cat() in the hg source.
def get_hg_file(repo, file_name, rev, tmp_file_name, dump_to_file = False):
    """ INTERNAL: read a file from the hg repo.
        If dump_to_file, the data is written into tmp_file_name.
        Otherwise, the data is returned and tmp_file_name is deleted.
    """
    #print "get_hg_file -- ", file_name, rev

    file_name = os.path.join(repo.root, file_name)
    ctx = repo[rev]
    bytes = None
    err = True
    matches = cmdutil.match(repo, (file_name,))
    for abs_ in ctx.walk(matches):
        assert err # Wacky. Why are we looping again?
        # REDFLAG: ripped out decode code. Will I need that on windows?
        file_ptr = None # Hmmmm starting to look like crappy Java code :-(
        in_file = None
        try:
            file_ptr = cmdutil.make_file(repo, tmp_file_name, ctx.node(),
                                         pathname=abs_)
            file_ptr.write(ctx[abs_].data())
            file_ptr.close()
            file_ptr = None
            if not dump_to_file:
                in_file = open(tmp_file_name)
                bytes = in_file.read()
        finally:
            if file_ptr:
                file_ptr.close()
            if in_file:
                in_file.close()
            if not dump_to_file and os.path.exists(tmp_file_name):
                os.remove(tmp_file_name)

        err = False
    if err:
        raise KeyError("File: %s doesn't exist in version: %s" \
                       % (file_name, rev))
    if dump_to_file:
        return "The data was written into: %s" % tmp_file_name

    return bytes


class HgFileOverlay(OverlayedFiles):
    """ An IFileOverlay that reads files from a mercurial revision."""
    def __init__(self, ui_, repo, base_dir, tmp_file):
        OverlayedFiles.__init__(self, os.path.join(repo.root, base_dir))
        self.base_dir = base_dir # i.e. root wrt repo
        self.ui_ = ui_
        self.repo = repo
        self.version = 'tip'
        self.tmp_file = tmp_file

    def repo_path(self, path):
        """ Return path w.r.t. the repository root. """
        path = os.path.abspath(path)
        assert path.startswith(self.base_path)
        assert path.startswith(self.repo.root)

        rest = path[len(self.repo.root):]
        if rest.startswith(os.sep):
            rest = rest[len(os.sep):]

        return rest

    def repo_pages(self, path):
        """ INTERNAL: Enumerate files in a repo subdirectory. """
        if not path.endswith('wikitext'):
            raise ValueError("Dunno how to enumerate wikitext pages from: %s"
                             % path)
        wikitext_dir = self.repo_path(path)
        # Hmmmm... won't work for files in root. use -1?
        return tuple([os.path.split(name)[1] for name in
                      self.repo.changectx(self.version).
                      manifest().keys() if name.startswith(wikitext_dir)])

    def exists_in_repo(self, path):
        """ INTERNAL: Return True if the file exists in the repo,
            False otherwise. """
        return (self.repo_path(path) in
                self.repo.changectx(self.version).manifest())

    def read(self, path, mode='rb', non_overlayed=False):
        """ Read a file. """
        if non_overlayed:
            return unicode(
                get_hg_file(self.repo, self.repo_path(path),
                            self.version, self.tmp_file),
                WIKITEXT_ENCODING)
        overlayed = self.overlay_path(path)
        if os.path.exists(overlayed):
            return DirectFiles.read(self, overlayed, mode)

        return unicode(get_hg_file(self.repo, self.repo_path(path),
                                   self.version, self.tmp_file),
                       WIKITEXT_ENCODING)

    def exists(self, path, non_overlayed=False):
        """ Return True if the file exists, False otherwise. """
        if non_overlayed:
            return self.exists_in_repo(path)

        overlay = self.overlay_path(path)
        if os.path.exists(overlay):
            if os.path.getsize(overlay) == 0:
                return False
            else:
                return True

        return self.exists_in_repo(path)

    def modtime(self, path, non_overlayed=False):
        """ Return the modtime for the file."""
        if non_overlayed:
            # Hmmm commit time for changeset, not file. Good enough.
            return int(self.repo.changectx(self.version).date()[0])

        overlay = self.overlay_path(path)
        if os.path.exists(overlay) and os.path.getsize(overlay) > 0:
            return DirectFiles.modtime(self, overlay)

        return int(self.repo.changectx(self.version).date()[0])

    def list_pages(self, path, non_overlayed=False):
        """ IFileFunctions implementation. """
        if non_overlayed:
            return self.repo_pages()

        overlay_pages = set([])
        overlay = self.overlay_path(path)
        if os.path.exists(overlay):
            overlay_pages = set(DirectFiles.list_pages(self, overlay))

        deleted = set([])
        for name in overlay_pages:
            if os.path.getsize(os.path.join(overlay, name)) == 0:
                deleted.add(name)

        return list(overlay_pages.union(set(self.repo_pages(path)) - deleted))
