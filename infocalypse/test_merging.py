#pylint: disable-msg=C0111,C0103,R0904,W0201
import os
import sys
import shutil
import unittest

from mercurial import ui, hg, commands

from pathhacks import add_parallel_sys_path
add_parallel_sys_path('wormarc')
from shafunc import new_sha

add_parallel_sys_path('fniki')
from fileoverlay import get_file_funcs

from graph import hex_version
from submission import bundle_wikitext, ForkingSubmissionHandler, SubmitError
from hgoverlay import HgFileOverlay

TEST_BASE = '/tmp'
TEST_ROOT = '__merging_test_run__'
TMP_DIR = "__TMP__"

# ONLY files from the last test to run!
LEAVE_TEST_DIR = True

class RepoTests(unittest.TestCase):
    def setup_test_dirs(self, base_dir, dir_name):
        if not os.path.exists(base_dir):
            raise IOError("Base test directory doesn't exist: %s" % base_dir)

        full_path = os.path.join(base_dir, dir_name)
        self.test_root = full_path
        self.tmp_dir  = os.path.join(self.test_root, TMP_DIR)

        if LEAVE_TEST_DIR and os.path.exists(full_path):
            print "Cleaning up directory from previous test run..."
            self.remove_test_dirs()

        if os.path.exists(full_path):
            raise IOError("Test directory exists: %s" % full_path)

        os.makedirs(full_path)
        os.makedirs(self.tmp_dir)

    def remove_test_dirs(self):
        assert self.test_root.endswith(TEST_ROOT)
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)

    def make_repo(self, base_dir):
        repo_root = os.path.join(self.test_root, base_dir)
        if not os.path.exists(repo_root):
            os.makedirs(repo_root)
        return hg.repository(self.ui_, repo_root, True)

    def clone_repo(self, repo, out_dir, to_rev=None):
        #if not to_rev is None:
        #    to_rev = repo[to_rev]

        return hg.clone(self.ui_, repo,
                        dest=os.path.join(self.test_root, out_dir),
                        pull=False, rev=to_rev, update=True, stream=False)[1]

    # DOESN'T REMOVE FILES
    def commit_revision(self, repo, raw_files, msg='no comment'):
        # DCI: Assert working dir is tip?
        manifest = repo['tip'].manifest()
        for fname, raw_bytes in raw_files:
            full_path = os.path.join(repo.root, fname)
            dname = os.path.dirname(full_path)
            if dname and not os.path.exists(dname):
                print "CREATED: ", dname
                os.makedirs(dname)

            out_file = open(full_path, 'wb')
            try:
                out_file.write(raw_bytes)
            finally:
                out_file.close()
            if not fname in manifest:
                commands.add(self.ui_, repo, full_path)

        commands.commit(self.ui_, repo,
                        logfile=None, addremove=None, user=None,
                        date=None,
                        message=msg)

    def commit_deletions(self, repo, file_names, msg='no comment'):
        for fname in file_names:
            commands.remove(self.ui_, repo,
                            os.path.join(repo.root, fname))
        commands.commit(self.ui_, repo,
                        logfile=None, addremove=None, user=None,
                        date=None,
                        message=msg)

    @classmethod
    def get_write_overlay(cls, repo):
        base_dir = os.path.join(repo.root, DEFAULT_WIKI_ROOT)
        base_dir = os.path.join(repo.root, base_dir)
        filefuncs = get_file_funcs(base_dir, True)
        text_dir = os.path.join(base_dir, 'wikitext')
        full_path = filefuncs.overlay_path(text_dir)
        if not os.path.exists(full_path):
            os.makedirs(full_path)

        return filefuncs

    def get_hg_overlay(self, repo):
        return HgFileOverlay(self.ui_, repo,
                             DEFAULT_WIKI_ROOT,
                             os.path.join(self.tmp_dir,
                                          '_tmp_shared_hg_overlay_tmp'))

    def make_submission_zip(self, repo):
        return bundle_wikitext(self.get_write_overlay(repo),
                               hex_version(repo),
                               DEFAULT_SUBMITTER)

    def get_applier(self, repo):
        ret = ForkingSubmissionHandler()
        ret.ui_ = self.ui_
        ret.repo = repo
        ret.logger = Logging()
        ret.base_dir = DEFAULT_WIKI_ROOT
        ret.notify_needs_commit = needs_commit
        ret.notify_committed = committed
        return ret

    def setUp(self):
        self.setup_test_dirs(TEST_BASE, TEST_ROOT)
        self.ui_ = ui.ui()

    def tearDown(self):
        if not LEAVE_TEST_DIR:
            self.remove_test_dirs()

class Logging:
    def __init__(self):
        pass
    @classmethod
    def out(cls, msg):
        print msg
    def trace(self, msg):
        self.out("T:" + str(msg))
    def debug(self, msg):
        self.out("D:" + str(msg))
    def warn(self, msg):
        self.out("W:" + str(msg))

def needs_commit():
    print "NEEDS COMMIT"

def committed(result):
    print "COMMITTED: %s" % str(result)

DEFAULT_WIKI_ROOT = 'wiki_root'
DEFAULT_SUBMITTER = 'freenetizen@this_is_not_a_real_fms_id'

# hard coded path assumptions
def has_forks(overlay):
    for name in overlay.list_pages(os.path.join(overlay.base_path, 'wikitext')):
        if name.find('_') != -1:
            return True
    return False

class NoConflictTests(RepoTests):
    def testrepo(self):
        repo = self.make_repo('foobar')
        self.commit_revision(repo, (('wiki_root/wikitext/SomePage',
                                     'This is a page.\n'),),
                             'automagically generated test repo.')
        cloned = self.clone_repo(repo, 'snarfu')
        print "REPO: ", repo.root
        print "CLONED: ", cloned.root


    ############################################################
    # Smoketest create, remove, modify w/o conflict
    def test_create_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'This the default front page.\n'),),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/NewPage'
        page_bytes = 'This is my new page.\n\n'

        # write a new file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))


        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(not server_overlay.exists(server_page_path))

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(not server_overlay.exists(server_page_path))
        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # check that the versions are the same
        self.assertTrue(server_overlay.read(server_page_path) == page_bytes)

    def test_remove_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'This the default front page.\n'),),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = ''

        # write a new file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))


        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)

        # Check that the target page exists.
        self.assertTrue(server_overlay.exists(server_page_path))

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                        self.tmp_dir,
                                                        '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(not server_overlay.exists(server_page_path))


    def test_modify_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'This the default front page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               original_page_bytes),),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = original_page_bytes + 'Client changes.\n'

        # write the updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))

        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)

        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # check that the versions are the same
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes + 'Client changes.\n')


    def test_modify_read_only_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'This the default front page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               original_page_bytes),),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = original_page_bytes + 'Client changes.\n'

        # write the updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))

        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)



        # Add FrontPage to the read only list on the server end.
        self.commit_revision(server_repo,
                             (('wiki_root/readonly.txt',
                               'FrontPage\n'),),
                             'Make FrontPage read only.')
        server_overlay.version = hex_version(server_repo) # tip


        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))


        # Should remain unchanged.
        server_overlay.version = hex_version(server_repo) # tip
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)



    def test_nop_modify_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'This the default front page.\n'
        final_page_bytes = 'This the final front page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               original_page_bytes),),
                             'Initial checkin of server repo.')

        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               final_page_bytes),),
                             'Second commit of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = final_page_bytes

        # write the updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))

        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)

        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # check that the versions are the same
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)
        self.assertTrue(not has_forks(server_overlay))

    def test_partial_nop_apply_file(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'This the default front page.\n'
        final_page_bytes = 'This the final front page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               original_page_bytes),),
                             'Initial checkin of server repo.')

        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               final_page_bytes),),
                             'Second commit of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = final_page_bytes

        new_page_path = 'wiki_root/wikitext/NewPage'
        new_page_bytes = 'this is a new page\n'

        # write the updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)

        # write a new page
        overlay.write(os.path.join(client_repo.root,
                                   new_page_path),
                      new_page_bytes)


        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))

        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo) # tip
        server_page_path = os.path.join(server_repo.root, page_path)
        server_new_page_path = os.path.join(server_repo.root,
                                            new_page_path)

        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)
        self.assertTrue(not server_overlay.exists(server_new_page_path))

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)
        self.assertTrue(not server_overlay.exists(server_new_page_path))

        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # check that the versions are the same
        self.assertTrue(server_overlay.read(server_page_path) ==
                        final_page_bytes)

        self.assertTrue(server_overlay.exists(server_new_page_path))
        self.assertTrue(server_overlay.read(server_new_page_path) ==
                        new_page_bytes)

        self.assertTrue(not has_forks(server_overlay))

class ConflictTests(RepoTests):
    ############################################################
    # Smoketest create, remove, modify with conflict

    def has_forked_version(self, overlay, page_path, raw_bytes):
        if not overlay.exists(page_path):
            return False
        sha_value = new_sha(raw_bytes).hexdigest()

        versioned_path = "%s_%s" % (page_path, sha_value)
        if not overlay.exists(versioned_path):
            return False

        if new_sha(overlay.read(versioned_path)).hexdigest() != sha_value:
            print "SHA FAILS: ", versioned_path
            self.assertTrue(False)

        # quick and dirty test for has forks
        self.assertTrue(has_forks(overlay))

        return True

    def test_create_file_conflict(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'Server side addition of a new page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'This the default front page.\n'),),
                             'Initial checkin of server repo.')

        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/NewPage',
                               original_page_bytes),),
                             'Second checkin of server repo.')

        # pull the client repo but only up to the first version
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/NewPage'
        client_page_path = os.path.join(client_repo.root, page_path)
        page_bytes = 'Conflicting client side changes.\n\n'

        self.assertTrue(not overlay.exists(client_page_path))
        # write a new file into it.
        overlay.write(client_page_path, page_bytes)
        # make a submission bundle
        self.assertTrue(overlay.exists(client_page_path))
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo, '0')[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))


        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo, '0') # clients version
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(not server_overlay.exists(server_page_path))

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(not server_overlay.exists(server_page_path))
        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # Check that the head version is the servers.
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)

        # Check that the forked version was created.
        self.has_forked_version(server_overlay, server_page_path, page_bytes)


    def test_remove_file_conflict(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'This the default front page.\n'),),
                             'Initial checkin of server repo.')
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'This the updated front page.\n'),),
                             'Second checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = ''

        # write a new file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))


        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo, '0')
        server_page_path = os.path.join(server_repo.root, page_path)

        # Check that the target page exists.
        self.assertTrue(server_overlay.exists(server_page_path))

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # Check that the head version is the servers.
        self.assertTrue(server_overlay.read(server_page_path) ==
                        'This the updated front page.\n')

        # Check that the forked version was created.
        self.has_forked_version(server_overlay, server_page_path, '')

    def test_modify_file_conflict(self):
        # setup the server repository
        server_repo = self.make_repo('server')
        original_page_bytes = 'This the default front page.\n'
        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               original_page_bytes),),
                             'Initial checkin of server repo.')

        self.commit_revision(server_repo,
                             (('wiki_root/wikitext/FrontPage',
                               'Updated front page.\n'),),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_path = 'wiki_root/wikitext/FrontPage'
        page_bytes = original_page_bytes + 'Client changes.\n'

        # write the updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)
        # make a submission bundle
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))

        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo, '0')
        server_page_path = os.path.join(server_repo.root, page_path)

        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)

        # apply the submission bundle to the server repo
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) ==
                        original_page_bytes)

        server_overlay.version = hex_version(server_repo) # new tip
        self.assertTrue(server_overlay.exists(server_page_path))

        # Check that the head versions i unchanged.
        self.assertTrue(server_overlay.read(server_page_path) ==
                        'Updated front page.\n')

        # Check that the forked version was created.
        self.has_forked_version(server_overlay, server_page_path,
                                original_page_bytes + 'Client changes.\n')

    def test_unresolved_merge(self):
        # setup the server repository
        server_repo = self.make_repo('server')

        page_path = 'wiki_root/wikitext/FrontPage'

        texts = ('This the default front page.\n',
                 'This fork 1 of the front page.\n',
                 'This fork 2 of the front page.\n',)

        print "---"
        print "Main  : FrontPage"
        print "fork 1: ", ("%s_%s" % (page_path, new_sha(texts[1]).
                                          hexdigest()))
        print "fork 2: ", ("%s_%s" % (page_path, new_sha(texts[2]).
                                          hexdigest()))
        print "---"
        self.commit_revision(server_repo,
                             ((page_path,
                               texts[0]),
                              ("%s_%s" % (page_path, new_sha(texts[1]).
                                          hexdigest()),
                               texts[1]),
                              ("%s_%s" % (page_path, new_sha(texts[2]).
                                          hexdigest()),
                               texts[2]),
                              ),
                             'Initial checkin of server repo.')

        # pull the client repo
        client_repo = self.clone_repo(server_repo, 'client', '0')

        # get a write overlay for the client repo
        overlay = self.get_write_overlay(client_repo)

        page_bytes = 'Modify front page without deleting forks.\n'

        # write an updated file into it.
        overlay.write(os.path.join(client_repo.root,
                                   page_path),
                      page_bytes)

        # verify write.
        self.assertTrue(overlay.read(os.path.join(client_repo.root,
                                                  page_path)) ==
                        page_bytes)

        # make a submission bundle w/ 2 unresolved forks
        try:
            raw_zip_bytes = self.make_submission_zip(client_repo)
            self.assertTrue(False)
        except SubmitError, err0:
            print "Got expected error:"
            print err0
            self.assertTrue(err0.illegal)

        # Resolve one fork in client overlay.
        overlay.write(os.path.join(client_repo.root,
                                   "%s_%s" % (page_path, new_sha(texts[1]).
                                          hexdigest())),
                      '')

        # make a submission bundle w/ 1 unresolved fork
        try:
            raw_zip_bytes = self.make_submission_zip(client_repo)
            self.assertTrue(False)
        except SubmitError, err1:
            print "Got second expected error:"
            print err1
            self.assertTrue(err1.illegal)


        # Resolve the final fork in client overlay.
        overlay.write(os.path.join(client_repo.root,
                                   "%s_%s" % (page_path, new_sha(texts[2]).
                                          hexdigest())),
                      '')

        # make a submission bundle w/ all forks resolved.
        raw_zip_bytes = self.make_submission_zip(client_repo)

        #(fms_id, usk_hash, base_version, chk, length)
        msg_id = 'fake_msg_id_000'
        submission_tuple = (DEFAULT_SUBMITTER,
                            '000000000000',
                            hex_version(server_repo)[:12],
                            'CHK@fakechk',
                            len(raw_zip_bytes))


        server_overlay = self.get_hg_overlay(server_repo)
        server_overlay.version = hex_version(server_repo, '0')
        server_page_path = os.path.join(server_repo.root, page_path)

        # Check that the target page exists.
        self.assertTrue(server_overlay.exists(server_page_path))
        self.assertTrue(server_overlay.read(server_page_path) != page_bytes)


        self.assertTrue(overlay.read(os.path.join(client_repo.root,
                                                  page_path)) ==
                        page_bytes)

        # Apply the bundle
        self.get_applier(server_repo).apply_submission(msg_id,
                                                       submission_tuple,
                                                       raw_zip_bytes,
                                                       os.path.join(
                                                           self.tmp_dir,
                                                           '_tmp__applying'))
        self.assertTrue(overlay.read(os.path.join(client_repo.root,
                                                  page_path)) ==
                        page_bytes)

        # Check that the head version is the clients
        server_overlay.version = hex_version(server_repo) # tip
        self.assertTrue(server_overlay.read(server_page_path) == page_bytes)

# Test fixures
# hg repo
#  create_repo(root path)
#  commit_repo(repo)
#  rest done w/ IFileFunctions? hmmmm... commit currently done above the line...


# Whitebox
# version sha's verify against file contents

# Cases

# Error/Illegal
# Create, Remove, Modify w/o deleting all previous versions
# base sha verification failure wrt stated base version
# final patch sha failure


# Non-Error
# Create
#  Conflict
#  No conflict
# Remove
#  Conflict
#  No conflict
# Modify
#  Conflict
#  No conflict

if __name__ == '__main__':
    # use -v on command line to get verbose output.
    # verbosity keyword arg not supported in 2.6?
    if len(sys.argv) >= 2 and sys.argv[1] != '-v':
        # Run a single test case
        suite = unittest.TestSuite()
        #suite.addTest(ConflictTests(sys.argv[1]))
        suite.addTest(NoConflictTests(sys.argv[1]))
        unittest.TextTestRunner().run(suite)
    else:
        # Run everything.
        unittest.main()
