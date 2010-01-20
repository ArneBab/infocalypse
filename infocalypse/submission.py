""" Functions to bundle and unbundle wiki submission zip files.

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
import time
import StringIO

from mercurial import mdiff
from mercurial import commands
from zipfile import ZipFile
from binascii import hexlify

from graph import hex_version, has_version
from validate import is_hex_string
from hgoverlay import HgFileOverlay

from pathhacks import add_parallel_sys_path

add_parallel_sys_path('wormarc')
from shafunc import new_sha
from binaryrep import NULL_SHA
from deltacoder import compress, decompress

add_parallel_sys_path('fniki')
from fileoverlay import DirectFiles
from piki import versioned_page_re as WIKINAME_REGEX

# Reasons submission were rejected.
REJECT_UNKNOWN = 0 # Dunno why submission failed.
REJECT_FCPFAIL = 1 # FCP request for CHK failed
REJECT_NOTRUST = 2 # Not enough trust
REJECT_APPLIED = 3 # Submission was already fully applied.
# Hmmm no longer needed?
REJECT_PARTIAL = 4 # Submission was already partially applied.
REJECT_CONFLICT = 5 # Submission couldn't be applied because of conflict.
REJECT_ILLEGAL = 6 # Submission bundle malformed or illegal.
SENTINEL_VER = '000000000000'

MAX_INFO_LEN = 1024 # Arbitrary, reasonable bound.

#----------------------------------------------------------#
CRLF = '\x0d\x0a'
EMPTY_FILE_SHA_HEX = new_sha('').hexdigest()
EMPTY_FILE_SHA = new_sha('').digest()
#----------------------------------------------------------#
# diff / patch helper funcs
#
# LATER: Use unified diffs?
# RESEARCH: No patch in python standard modules?
# 0) http://docs.python.org/library/difflib.html
#    can write diffs (context, unified) but doesn't
#    read them.
#    restore() operates on the entire file rep. ie. not just deltas
#    Double check.
# 1) http://code.google.com/p/python-patch/
#    Looks promising, but too new to rely on.
# 2) http://code.google.com/p/google-diff-match-patch/
#    Looks like it would do the trick but forces licensing
#    to GPL3 since it is Apache. Double check.
# 3) Mercurial patch/diff. [WINNER]
#    Non-standard format, but will get the job done fast.

# REQUIRE 8-bit strings!

# RAM!
def make_patch(old_text, new_text):
    """ Return a raw patch bytes which transforms old_text into new_text. """
    values = compress(mdiff.textdiff(old_text, new_text))
    if values[0]:
        return ''.join(values)
    return values[1]

# RAM!
def apply_patch(old_text, patch):
    """ Return raw new file bytes by applying patch to old_text. """ 
    return mdiff.patches(old_text,
                         [decompress(patch)])
#----------------------------------------------------------#
# Returns a unicode string.
def unicode_apply_patch(old_text, patch, updated_sha, name):
    """ Helper wrapper around apply_patch() which takes a unicode string
        for old_text.

        raises a SubmitError if the SHA1 of the patched text != updated_sha. """
    ret = apply_patch(old_text.encode('utf8'), patch)
    if new_sha(ret).digest() != updated_sha:
        raise SubmitError("Patch failed to validate: %s" %
                          name, True)
    return ret.decode('utf8')

# Returns an 8-bit string.
def unicode_make_patch(old_text, new_text):
    """ Helper wrapper around make_patch() which takes unicode strings."""
    values = compress(mdiff.textdiff(old_text.encode('utf8'),
                                     new_text.encode('utf8')))
    if values[0]:
        return ''.join(values)

    return values[1]

def utf8_sha(unicode_text):
    """ Return a SHA1 hash instance for the utf8 8-bit string rep
        of unicode_text."""
    return new_sha(unicode_text.encode('utf8'))

class SubmitError(Exception):
    """ Exception used to indicate failure by bundle_wikitext and
        unbundle_wikitext. """
    def __init__(self, msg, illegal=False):
        Exception.__init__(self, msg, illegal)
        self.illegal = illegal

class NoChangesError(SubmitError):
    """ Exception to indicate that there are no local
        changes to be submitted. """
    def __init__(self):
        SubmitError. __init__(self, "No changes found." , False)

def pack_info(version, submitter):
    """ INTERNAL: Validate and pack __INFO__ contents into 7-bit ASCII. """
    try:
        submitter.encode('ascii')
    except UnicodeError:
        raise SubmitError("Non-ASCII characters in submitter name: %s" %
                          repr(submitter), True)
    if not is_hex_string(version, 40):
        raise SubmitError("Version isn't a 40 digit hex string: %s" %
                          repr(version), True)
    try:
        ret = ("%s\n%s\n" % (version, submitter)).encode('ascii')
    except UnicodeError:
        # Impossible?
        raise SubmitError("Unexpected error packing info???", True)

    if len(ret) > MAX_INFO_LEN:
        raise SubmitError("Info file too big.", True)

    return ret

def unpack_info(info_text):
    """ INTERNAL: Validate and unpack __INFO__ contents from 7-bit ASCII. """
    try:
        info_text = info_text.decode('ascii')
    except UnicodeError:
        raise SubmitError("Non-ASCII characters in info file.", True)

    if len(info_text) > MAX_INFO_LEN:
        raise SubmitError("Info file too big.", True)

    fields = info_text.splitlines(False)
    if len(fields) != 2:
        raise SubmitError("Format error in info file.", True)

    if not is_hex_string(fields[0], 40):
        raise SubmitError("Version isn't a 40 digit hex string: %s" %
                          repr(fields[0]), True)
    # Hmmmm... empty submitter is ok
    return fields[0], fields[1].strip()

def get_read_only_list(overlay):
    """ Helper reads the 'readonly.txt' list of locked page names. """
    full_path = os.path.join(overlay.base_path, 'readonly.txt')
    if not os.path.exists(full_path):
        return frozenset([])

    in_file = open(full_path, 'rb')
    try:
        return frozenset([value.strip()
                          for value in in_file.read().splitlines()])
    finally:
        in_file.close()

# RAM
def bundle_wikitext(overlay, version, submitter):
    """ Return raw zipfile bytes containing the overlayed wiki changes
        in the overlay_base dir. """

    assert overlay.is_overlayed()

    # Catch bad wikitext.
    validate_wikitext(overlay)


    wiki_text = os.path.join(overlay.base_path, 'wikitext')

    names = (set(overlay.list_pages(wiki_text)).
             union(overlay.list_pages(wiki_text, True)))

    # Catch illegal names.
    for name in names:
        if not WIKINAME_REGEX.match(name):
            raise SubmitError("File name is not a WikiWord: %s" % name, True)
        page_ver = WIKINAME_REGEX.match(name).group('version')
        if not page_ver:
            continue
        if not overlay.exists(os.path.join(wiki_text, name), True):
            raise SubmitError("Forked page doesn't exist in base version: %s" \
                              % name, True)
    # Catch unresolved merges.
    check_merges([name for name in names
                     if overlay.has_overlay(os.path.join(wiki_text, name))],
                 names,
                 OverlayHasher(overlay).hexdigest)

    illegal_writes = get_read_only_list(overlay)

    buf = StringIO.StringIO()
    arch = ZipFile(buf, 'w')
    assert version
    arch.writestr('__INFO__', pack_info(version, submitter))
    count = 0
    for name in names:
        full_path = os.path.join(wiki_text, name)
        if not overlay.has_overlay(full_path):
            # has_overlay is True for locally deleted files.
            continue

        if not overlay.exists(full_path, True):
            original_sha = NULL_SHA
            original_raw = ''
        else:
            # Compute SHA1 of original file.
            original_raw = overlay.read(full_path, 'rb', True)
            original_sha = utf8_sha(original_raw).digest()

        new_raw = overlay.read(full_path, 'rb')
        if new_raw == original_raw:
            # Don't bundle changes which are already in the repo
            # even if we have a copy of them in the overlay
            # directory.
            continue

        if name in illegal_writes:
            raise SubmitError("Can't modify read only page: %s" % name, True)

        # Make patch.
        delta = unicode_make_patch(original_raw, new_raw)

        # REDFLAG: BLOAT. Worth 40 bytes / file ???
        # Prepend old and new SHA1 to patch so we will know if we
        # are trying to patch against the wrong file or patch
        # a file that has already been patched.
        delta = original_sha + utf8_sha(new_raw).digest() + delta
        arch.writestr(name, delta)
        count += 1
    arch.close()
    if count < 1:
        raise NoChangesError()
    return buf.getvalue()

# (version, submitter)
def get_info(in_stream):
    """ Return the version and submitter strings from zipfile byte stream. """
    arch = ZipFile(in_stream, 'r')
    try:
        return unpack_info(arch.read('__INFO__'))
    finally:
        arch.close()



# 0 created
# 1 modified
# 2 removed
# 3 Already applied
def extract_wikitext(arch, overlay, name):
    """ Helper to simplify unbundle_wikitext. """
    ret = -1
    raw_delta = checked_read_delta(arch, name)
    #print "NAME: %s, raw len: %i" % (name, len(raw_delta))
    base_sha = raw_delta[:20]
    updated_sha = raw_delta[20:40]
    raw_delta = raw_delta[40:]
    #print "base: %s, new: %s remaining: %i" % (hexlify(base_sha)[:12],
    #                                           hexlify(updated_sha)[:12],
    #                                           len(raw_delta))
    full_path = os.path.join(os.path.join(overlay.base_path, 'wikitext'),
                             name)

    if base_sha == NULL_SHA:
        # New file.
        if overlay.exists(full_path):
            if utf8_sha(overlay.read(full_path, 'rb')).digest() == updated_sha:
                return 3 # Already patched.
            raise SubmitError("New file already exists: %s" % name)
        raw_a = ''
        ret = 0
    else:
        #print "OVERLAYED: ", overlay.overlay_path(full_path)
        if not overlay.exists(full_path):
            if updated_sha == EMPTY_FILE_SHA:
                return 3 # Already patched.
            raise SubmitError("Base file doesn't exist: %s" % name)
        raw_a = overlay.read(full_path, 'rb')
        tmp_sha = utf8_sha(raw_a).digest()
        if tmp_sha == updated_sha:
            return 3 # Already patched.
        if not tmp_sha == base_sha:
            # Hmmmm... windows vs. *nix line terminators?
            raise SubmitError("Base file SHA1 hash failure: %s" % name)
        ret = 1
    #print "Extracting: %s [%s] " % (name, hexlify(base_sha)[:12])
    #print "ORIGINAL:"
    #print repr(raw_a)
    #print "PATCH:"
    #print repr(raw_delta)

    raw_file = unicode_apply_patch(raw_a, raw_delta, updated_sha, name)

    #print "PATCHED:"
    #print repr(raw_file)

    if len(raw_file) == 0:
        # HACK. len == 0 => delete
        ret = 2
        if not overlay.is_overlayed():
            os.remove(full_path)
            return ret

    overlay.write(full_path, raw_file, 'wb')

    return ret


def raise_if_not_merging(is_merging, msg):
    """ INTERNAL: Helper to raise a SubmitError when not merging."""
    if not is_merging:
        raise SubmitError(msg)

def handle_conflict(head, full_path, name, bytes, updated_sha):
    """ INTERNAL: Helper to deal with conflicting merges.  """
    assert full_path.endswith(name)
    versioned_name = "%s_%s" % (name, hexlify(updated_sha))
    # REDFLAG: LATER: explict hg copy to minimize repo size? 
    head.write(os.path.join(os.path.split(full_path)[0],
                            versioned_name),
               bytes, 'wb')
    return versioned_name

def checked_read_delta(arch, name):
    """ INTERNAL: Read a raw delta from an archive."""
    raw_delta = arch.read(name)
    if len(raw_delta) < 40:
        raise SubmitError("<40 bytes: %s" % name, True)
    return raw_delta
# DCI: BUG: Don't fork if final version == current version. i.e. already applied
#      bug from a different base version.
def forking_extract_wikitext(arch, overlay, head, name):
    """ Helper function used by merge_wikitext() to merge a single
        file. """
    assert not overlay is None
    assert not head is None
    assert not head == overlay
    ret = -1
    raw_delta = checked_read_delta(arch, name)

    #print "NAME: %s, raw len: %i" % (name, len(raw_delta))
    base_sha = raw_delta[:20]
    updated_sha = raw_delta[20:40]
    raw_delta = raw_delta[40:]
    #print "base: %s, new: %s remaining: %i" % (hexlify(base_sha)[:12],
    #                                           hexlify(updated_sha)[:12],
    #                                           len(raw_delta))
    full_path = os.path.join(os.path.join(overlay.base_path, 'wikitext'),
                             name)

    if base_sha == NULL_SHA:
        # New file.
        if overlay.exists(full_path):
            # ILLEGAL.
            raise SubmitError("New file already exists in base version: %s"
                              % name, True)
        if head.exists(full_path):
            # CONFLICT.
            # Create a versioned conflict file because the file the
            # submitter wants to create already exists in the repo.
            raw_file = unicode_apply_patch('', raw_delta, updated_sha, name)
            # Wrote conflicting version.
            return 4, handle_conflict(head, full_path,
                                      name, raw_file, updated_sha)

        raw_a = ''
        ret = 0
    else:
        #print "OVERLAYED: ", overlay.overlay_path(full_path)
        if not overlay.exists(full_path):
            # ILLEGAL
            raise SubmitError("Base file doesn't exist in base version: %s" %
                              name, True)

        if not head.exists(full_path):
            if updated_sha == EMPTY_FILE_SHA:
                return 3, name # Already patched.

            # CONFLICT
            # Create a versioned conflict file because the file the
            # submitter wants to modify already was deleted from
            # the repo.
            #
            # Patch against the SUBMITTER'S version!
            raw_file = unicode_apply_patch(overlay.read(full_path, 'rb'),
                                           raw_delta, updated_sha, name)
            return 4, handle_conflict(head, full_path,
                                      name, raw_file, updated_sha)

        raw_a = overlay.read(full_path, 'rb')
        tmp_sha = utf8_sha(raw_a).digest()
        if not tmp_sha == base_sha:
            # ILLEGAL
            raise SubmitError(("Base file SHA1 hash failure against base " +
                               "version: %s") % name, True)
        head_sha = utf8_sha(head.read(full_path)).digest()
        if head_sha != tmp_sha:
            # CONFLICT
            # Create a versioned conflict file because the file the
            # submitter wants to modify already was modified in the repo.
            # Patch against the SUBMITTER'S version!
            raw_file = unicode_apply_patch(raw_a, raw_delta, updated_sha, name)
            return 4, handle_conflict(head, full_path, name,
                                      raw_file, updated_sha)

        if tmp_sha == updated_sha:
            return 3, name # Already patched.

        ret = 1
    #print "Extracting: %s [%s] " % (name, hexlify(base_sha)[:12])
    #print "ORIGINAL:"
    #print repr(raw_a)
    #print "PATCH:"
    #print repr(raw_delta)

    raw_file = unicode_apply_patch(raw_a, raw_delta, updated_sha, name)

    #print "PATCHED:"
    #print repr(raw_file)

    if len(raw_file) == 0:
        # HACK. len == 0 => delete
        ret = 2
        if not head.is_overlayed():
            os.remove(full_path)
            return ret, name

    head.write(full_path, raw_file, 'wb')

    return ret, name

# Hmmm ugly code duplication, but we want to fail
# WITHOUT writing if any update fails.
def check_base_shas(arch, overlay):
    """ Helper to simplify unbundle_wikitext. """
    for name in arch.namelist():
        #print "CHECKING NAME: ", name
        if name == '__INFO__':
            continue
        if not WIKINAME_REGEX.match(name):
            raise SubmitError("File name is not a WikiWord: %s" % name, True)

        raw_delta = arch.read(name)
        base_sha = raw_delta[:20]
        updated_sha = raw_delta[20:40]
        full_path = os.path.join(os.path.join(overlay.base_path, 'wikitext'),
                                 name)
        if base_sha == NULL_SHA:
            # New file.
            if overlay.exists(full_path):
                if (utf8_sha(overlay.read(full_path, 'rb')).digest()
                    == updated_sha):
                    continue
                raise SubmitError("New file already exists: %s" % name)
        else:
            if not overlay.exists(full_path):
                if updated_sha == EMPTY_FILE_SHA:
                    continue
                raise SubmitError("Base file doesn't exist(1): %s [%s]" %
                                  (name, full_path))
            raw_a = overlay.read(full_path, 'rb')
            tmp_sha = utf8_sha(raw_a).digest()
            if tmp_sha == updated_sha:
                continue
            if not tmp_sha == base_sha:
                # Hmmmm... windows vs. *nix line terminators?
                raise SubmitError("Base file SHA1 hash failure(1): %s" % name)

def check_writable(overlay, arch):
    """ Helper raises SubmitError if any pages in the zip are read only. """
    names = set([])
    for name in arch.namelist():
        match = WIKINAME_REGEX.match(name)
        if not match:
            continue
        names.add(match.group('wikiword'))

    illegal_writes = names.intersection(get_read_only_list(overlay))

    if len(illegal_writes) > 0:
        raise SubmitError("Attempt to modify read only page(s): %s" %
                          ','.join(illegal_writes), True)
# REDFLAG: get rid of required_* args?
# LATER: get_version_func(name, version)
# target_name is subdir i.e. wikitext
def unbundle_wikitext(overlay, in_stream,
                      required_version = None,
                      required_submitter = None):
    """ Unbundle a wiki submission bundle from a zipfile byte stream.
    """

    wiki_text = os.path.join(overlay.base_path, 'wikitext')
    if not os.path.exists(overlay.overlay_path(wiki_text)):
        os.makedirs(overlay.overlay_path(wiki_text))
    # created, modified, removed, skipped
    op_lut = (set([]), set([]), set([]), set([]))
    arch = ZipFile(in_stream, 'r')
    try:
        base_ver, submitter = unpack_info(arch.read('__INFO__'))
        if not required_version is None and required_version != base_ver:
            raise SubmitError("Expected version: %s, got: %s" %
                              (required_version[:12], base_ver[:12]))
        if not required_submitter is None and submitter != required_submitter:
            raise SubmitError("Expected submitter: %s, got: %s" % \
                              (required_submitter, submitter))
        if required_version is None:
            check_base_shas(arch, overlay)

        check_writable(overlay, arch)

        for name in arch.namelist():
            if name == "__INFO__":
                continue
            if not WIKINAME_REGEX.match(name):
                raise SubmitError("File name is not a WikiWord: %s" %
                                  name, True)
            action = extract_wikitext(arch, overlay, name)
            op_lut[action].add(name)
        return op_lut
    finally:
        arch.close()

def validate_wikitext_str(raw_text, full_path="unknown_file"):
    """ Raises a SubmitError when illegal wikitext is encountered.

        For now, it only checks for DOS line terminators. """

    if raw_text.find(CRLF) != -1:
        raise SubmitError("Saw DOS line terminator: %s" % full_path,
                          True)

def validate_wikitext(overlay, non_overlayed=False):
    """ Runs the valididate_wikitext_str() function over every
        page in the overlay. """

    path = os.path.join(overlay.base_path, 'wikitext')
    for name in overlay.list_pages(path, non_overlayed):
        full_path = os.path.join(path, name)
        validate_wikitext_str(overlay.read(full_path, 'rb', non_overlayed),
                              full_path)

def conflict_table(names):
    """ INTERNAL: Make a WikiName -> version map from a list of
        'WikiName_40digithexversion' names. """
    ret = {}
    for name in names:
        #print "conflict_table -- NAME: ", name
        match = WIKINAME_REGEX.match(name)
        if not match:
            continue

        wiki_word = match.group('wikiword')
        version = match.group('version')

        if not version or not wiki_word: # hmmm... not wiki_word???
            continue

        entry = ret.get(wiki_word, set([]))
        assert not version in entry # Slow! but list should be short
        entry.add(version)
        ret[wiki_word] = entry

    return ret


class ArchiveHasher:
    """ Helper class to get page hexdigests out of submission .zip archives
       for check_merges(). """

    def __init__(self, arch):
        self.arch = arch

    def hexdigest(self, versioned_name):
        """ Return the hexdigest for the updated page stored in the archive.

            THIS VALUE IS NOT VALIDATED.

            Illegal values will be caught later when apply_patch() fails.
        """
        raw_delta = checked_read_delta(self.arch, versioned_name)
        return hexlify(raw_delta[20:40])

class OverlayHasher:
    """ Helper class to get hexdigests of wiki pages from an overlay
       for check_merges(). """
    def __init__(self, overlay):
        self.overlay = overlay
        assert overlay.is_overlayed()

    def hexdigest(self, wiki_name):
        """ Return the hexdigest of page with name wiki_name. """
        wikitext_dir = os.path.join(self.overlay.base_path, 'wikitext')
        full_path = os.path.join(wikitext_dir, wiki_name)
        return utf8_sha(self.overlay.read(full_path)).hexdigest()

# WHY? Make users *look at* files before deleting them.
# evildoer can just autogenerate versioned delete files
#   BUT at least they must know the version they are deleting against.
# will check for full deletion on submit

# Check:
# o All previous versioned files for any modified file
#   deleted.
# o Not adding any versioned files
def check_merges(submitted_pages, all_pages, hexdigest_func):
    """ INTERNAL: Raises a SubmitError if the merge constraints
        aren't met. """
    #print "SUBMITTED_PAGES: ", submitted_pages
    conflicts = conflict_table(all_pages)
    resolved = conflict_table(submitted_pages)
    for name in submitted_pages:
        #print "check_merges -- NAME: ", name
        assert WIKINAME_REGEX.match(name)
        if name in conflicts:
            if resolved.get(name, set([])) != conflicts[name]:
                unresolved = set([ver for ver in conflicts[name]
                                  if not ver in resolved.get(name, set([]))])

                raise SubmitError("Unresolved fork(s): [%s]:%s" %
                                  (WIKINAME_REGEX.match(name).group('wikiword'),
                                   ','.join([ver[:12] for ver in unresolved])),
                                  True)

    for name in resolved:
        for version in resolved[name]:
            versioned_name = '%s_%s' % (name, version)
            if hexdigest_func(versioned_name) != EMPTY_FILE_SHA_HEX:
                raise SubmitError("Not deleted!: %s" % versioned_name,
                                  True)

def merge_wikitext(ui_, repo, base_dir, tmp_file, in_stream):
    """ Merge changes from a submission zip file into the
        repository. """

    # HgFileOverlay to read bundle files with.
    prev_overlay = HgFileOverlay(ui_, repo, base_dir, tmp_file)

    # Direct overlay to write updates into the repo.
    head_overlay = DirectFiles(os.path.join(repo.root, base_dir))

    arch = ZipFile(in_stream, 'r')
    try:
        base_ver, dummy = unpack_info(arch.read('__INFO__'))
        if not has_version(repo, base_ver):
            # REDFLAG: Think. What about 000000000000?
            #          It is always legal. hmmmm...
            raise SubmitError("Base version: %s not in repository." %
                              base_ver[:12], True)

        # Still need to check for illegal submissions.
        prev_overlay.version = base_ver

        # REDFLAG: revisit.
        # just assert in forking_extract_wikitext and
        # get rid of extra checking / exception raising?
        check_base_shas(arch, prev_overlay)
        # Hmmmm... checking against a version of readonly.txt
        # which may be later than the one that the submitter
        # used.
        check_writable(head_overlay, arch)
        check_merges([name for name in arch.namelist()
                      if name != '__INFO__'],
                     # pylint gives spurious E1101 here ???
                     #pylint: disable-msg=E1101
                     prev_overlay.list_pages(os.path.join(prev_overlay.
                                                          base_path,
                                                          'wikitext')),
                     ArchiveHasher(arch).hexdigest)

        # created, modified, removed, skipped, forked
        op_lut = (set([]), set([]), set([]), set([]), set([]))

        for name in arch.namelist():
            # check_base_sha validates wikinames.
            if name == "__INFO__":
                continue
            action, versioned_name = forking_extract_wikitext(arch,
                                                              prev_overlay,
                                                              head_overlay,
                                                              name)
            op_lut[action].add(versioned_name)
        return op_lut
    finally:
        arch.close()


class ForkingSubmissionHandler:
    """ Class which applies submissions to wikitext in an hg repo, creating
    version suffixed pages on merge conflicts. """
    def __init__(self):
        self.ui_ = None
        self.repo = None
        self.logger = None
        self.base_dir = None # relative wrt self.repo.root
        self.notify_needs_commit = lambda :None
        self.notify_committed = lambda succeeded:None

    def full_base_path(self):
        """ INTERNAL: Returns the full path to the dir which contains the
            wikitext dir. """
        return os.path.join(self.repo.root, self.base_dir)

    def apply_submission(self, msg_id, submission_tuple, raw_zip_bytes,
                         tmp_file):
        """ Apply a submission zip bundle. """
        code = REJECT_CONFLICT
        try:
            self.commit_results(msg_id, submission_tuple,
                                merge_wikitext(self.ui_,
                                               self.repo,
                                               self.base_dir,
                                               tmp_file,
                                               StringIO.StringIO(
                                                   raw_zip_bytes)))
            return True

        except SubmitError, err:
            self.logger.debug("apply_submission --  err: %s" % str(err))

            if err.illegal:
                self.logger.warn("apply_submission -- ILLEGAL .zip: %s" %
                                 str(submission_tuple))
                code = REJECT_ILLEGAL

        except Exception, err:
            self.logger.warn("apply_submission -- ILLEGAL .zip(1): %s" %
                              str(submission_tuple))
            raise # DCI
        self.update_change_log(msg_id, submission_tuple,
                                code, False)
        return False

    # Sets needs commit on failure, but not success. Hmmm...
    # Update <wiki_root>/submitted.txt
    # Update <wiki_root>/rejected.txt
    def update_change_log(self, msg_id, submission_tuple, result=None,
                          succeeded=False):
        """ Update the accepted.txt or rejected.txt change log
            based on the results of a submission. """
        self.logger.trace("update_change_log:\n%s\n%s\n%s\n%s" %
                          (msg_id, submission_tuple, str(result),
                           str(succeeded)))

        full_path = self.full_base_path()
        if succeeded:
            full_path = os.path.join(full_path, 'accepted.txt')
            out_file = open(full_path, 'ab')
            try:
                out_file.write("%s:%i:%s:%s\n" % ( SENTINEL_VER,
                                                   time.time(),
                                                   submission_tuple[0],
                                                   submission_tuple[3]))
                # Created, modified, removed, skipped, forked
                op_lut = ('C', 'M', 'R', '*', 'F')
                for index, values in enumerate(result):
                    if index == 3 or index > 4:
                        continue # HACK # REDFLAG: change order?
                    if len(values):
                        values = list(values)
                        values.sort()
                        out_file.write("%s:%s\n" % (op_lut[index],
                                                    ':'.join(values)))
            finally:
                out_file.close()
            # Caller is resposible for commiting or setting "needs commit".
            return

        # Failed
        full_path = os.path.join(full_path, 'rejected.txt')
        if result is None:
            result = REJECT_UNKNOWN # ??? just assert?
        out_file = open(full_path, 'ab')
        try:
            out_file.write("%s:%i:%s:%s:%i\n" % (hex_version(self.repo)[:12],
                                                 time.time(),
                                                 submission_tuple[0],
                                                 submission_tuple[3],
                                                 int(result)))
        finally:
            out_file.close()

        self.notify_needs_commit()


    # Internal helper function which is only called immediately after
    # a successful commit.
    # LATER: also truncate?
    # Just commit twice so that you don't have to deal with this?
    # i.e. because we have to write the log entry *before* we know the version.
    def fixup_accepted_log(self):
        """ INTERNAL: Hack to fix the hg version int the accepted.txt log. """
        version = hex_version(self.repo)[:12] # The new tip.
        self.logger.debug("fixup_accept_log -- fixing up: %s" % version)
        assert len(version) == len(SENTINEL_VER)

        full_path = os.path.join(self.full_base_path(), 'accepted.txt')
        in_file = open(full_path, 'rb')
        try:
            lines = in_file.readlines()
            in_file.close()
            pos = len(lines) -1
            while pos >= 0:
                if not lines[pos].startswith(SENTINEL_VER):
                    pos -= 1
                    continue
                lines[pos] = version + lines[pos][len(version):]
                break

            assert pos >= 0
            # Replace existing file.
            os.remove(full_path)
            try:
                out_file = open(full_path, 'wb')
                out_file.write("".join(lines))
            finally:
                out_file.close()
        finally:
            in_file.close()


    # DCI: need code to scrub non vcd files?
    # DCI: failure cases?
    # REDFLAG: LATER: rework ordering of results entries?
    # IMPLIES SUCCESS.
    def commit_results(self, msg_id, submission_tuple, results):
        """ INTERNAL: Commit the results of a submission to the local repo. """
        assert len(results[3]) == 0
        wikitext_dir = os.path.join(self.full_base_path(), 'wikitext')
        raised = True
        # grrr pylint gives spurious
        #pylint: disable-msg=E1101
        self.ui_.pushbuffer()
        try:
            # hg add new files.
            for name in results[0]:
                full_path = os.path.join(wikitext_dir, name)
                commands.add(self.ui_, self.repo, full_path)

            # hg add fork files
            for name in results[4]:
                full_path = os.path.join(wikitext_dir, name)
                commands.add(self.ui_, self.repo, full_path)

            # hg remove removed files.
            for name in results[2]:
                full_path = os.path.join(wikitext_dir, name)
                commands.remove(self.ui_, self.repo, full_path)

            # Writes to/prunes special file used to generate RemoteChanges.
            self.update_change_log(msg_id, submission_tuple, results, True)

            # REDFLAG: LATER, STAKING? later allow third field for staker.
            # fms_id|chk
            commit_msg = "%s|%s" % (submission_tuple[0],
                                    submission_tuple[3])
            # hg commit
            commands.commit(self.ui_, self.repo,
                            logfile=None, addremove=None, user=None,
                            date=None,
                            message=commit_msg)
            self.fixup_accepted_log() # Fix version in accepted.txt
            self.notify_committed(True)
            raised = False
        finally:
            text = self.ui_.popbuffer()
            if raised:
                self.logger.debug("commit_results -- popped log:\n%s" % text)


    def force_commit(self):
        """ Force a commit to the repository after failure. """
        self.logger.trace("force_commit -- Commit local changes " +
                           "after failure.")
        commands.commit(self.ui_, self.repo,
                        logfile=None, addremove=None, user=None,
                        date=None,
                        message='F') # Must have failed.
        self.notify_committed(False)
