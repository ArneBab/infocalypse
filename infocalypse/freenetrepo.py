# infocalypse
#
# Copyright 2012 Arne Babenhauserheide <arne_bab at web dot de>,
#    though most of this file is taken from hg-git from Scott Chacon
#    <schacon at gmail dot com> also some code (and help) borrowed
#    from durin42
#
# This software may be used and distributed according to the terms
# of the GNU General Public License, incorporated herein by reference.

from mercurial import repo, util
try:
    from mercurial.error import RepoError
except ImportError:
    from mercurial.repo import RepoError

class freenetrepo(repo.repository):
    capabilities = ['lookup']

    def __init__(self, ui, path, create):
        if create: # pragma: no cover
            raise util.Abort('Cannot create a freenet repository, yet.')
        self.ui = ui
        self.path = path

    def lookup(self, key):
        if isinstance(key, str):
            return key

    def local(self):
        # a freenet repo is never local
        raise RepoError

    def heads(self):
        return []

    def listkeys(self, namespace):
        return {}

    def pushkey(self, namespace, key, old, new):
        return False

    # used by incoming in hg <= 1.6
    def branches(self, nodes):
        return []

instance = freenetrepo

def islocal(path):
    u = util.url(path)
    return not u.scheme or u.scheme == 'file'
