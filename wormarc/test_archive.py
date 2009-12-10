""" Unit tests.

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


# OK to be a little sloppy for test code.
# pylint: disable-msg=C0111
# For setUp() and tearDown()
# pylint: disable-msg=C0103
# Allow attribute creation in setUp()
# pylint: disable-msg=W0201
# Allow test methods that don't reference self.
# pylint: disable-msg=R0201
# Allow many test methods.
# pylint: disable-msg=R0904
import os
import shutil
import traceback
import random
import time
import sys
import unittest

from shafunc import new_sha as sha1

from binaryrep import NULL_SHA, get_file_sha, str_sha
from blocks import BlockStorage, ITempFileManager
from linkmap import verify_link_map
from filemanifest import FileManifest, entries_from_dir, entries_from_seq, \
     manifest_to_dir, verify_manifest, validate_path

from archive import WORMBlockArchive, is_ordered, is_contiguous, \
     repartition, compress

from deltacoder import DeltaCoder

from hghelper import export_hg_repo

# False causes test dir to be cleaned up automatically
# after every run.
LEAVE_TEST_DIR = False

# Absolute path to some hg repository to use for
# testing.
# You MUST MODIFY this for test_hg_repo_torture_test() to work
HG_REPO_DIR = ""
# e.g.:
#HG_REPO_DIR = os.path.expanduser("~/mess/hg_zoo/somedude")

#----------------------------------------------------------#
TEST_BASE = '/tmp/'
TEST_ROOT = '__latest_test_run__'

TMP_DIR = '__TMP__'
TEST_DIR = 'test'


class HandleTemps(ITempFileManager):
    """ Delegate to handle temp file creation and deletion. """
    def __init__(self, base_dir):
        ITempFileManager.__init__(self)
        self.base_dir = base_dir
        self.callers = {}
    def make_temp_file(self):
        """ Return a new unique temp file name including full path. """
        name = os.path.join(self.base_dir, "__TMP__%s" %
                            str(random.random())[2:])
        self.callers[name] = traceback.extract_stack()
        return name

    def remove_temp_file(self, full_path):
        """ Remove and existing temp file. """
        if not os.path.split(full_path)[-1].startswith("__TMP__"):
            raise IOError("Didn't create: %s" % full_path)

        if not os.path.exists(full_path):
            return

        if full_path in self.callers.keys():
            del self.callers[full_path]
        else:
            print "HandleTemps.remove_file() -- removing non-managed file???"
            print full_path

        os.remove(full_path)

    def check_for_leaks(self):
        for name in self.callers:
            if not os.path.exists(name):
                continue

            print "LEAKED: ", name
            print "FROM:"
            print self.callers[name]

        if len(os.listdir(self.base_dir)) > 0:
            file_count = 0
            for name in os.listdir(self.base_dir):
                if os.path.isdir(os.path.join(self.base_dir, name)):
                    # Allow directories. e.g. __hg_repo__, __unarchived__.
                    print "HandleTemps.check_for_leaks -- ignored dir: ", name
                    continue
                print name
                file_count += 1

            if file_count > 0:
                raise IOError("Undeleted temp files!")

def dump_blocks(blocks, msg=None, brief=False):
    if not msg is None:
        print msg
    values = []
    for index in range(0, len(blocks.tags)):
        path = blocks.full_path(index)
        if os.path.exists(path):
            length = str(os.path.getsize(path))
        else:
            length = "no_file"
        if brief:
            values.append(length)
        else:
            values.append("%s:[%s]" % (path, length))

    if brief:
        print "blocks: " + " ".join(values)
    else:
        print "blocks\n" + "\n".join(values)

def link_str(link):
    return "(%s, %i, %s, data: %s, %i, %s)" % (str_sha(link[0]),
                                               link[1],
                                               str_sha(link[2]),
                                               bool(link[3]),
                                               link[4],
                                               link[5])
def dump_links(links, msg=None):
    if not msg is None:
        print msg
    for link in links:
        print link_str(link)

def dump_link_map(link_map, msg=None, brief=False):
    if not msg is None:
        print msg
    print "keys: ", len(link_map)
    if brief:
        return
    keys = link_map.keys()
    keys.sort()
    for key in keys:
        print str_sha(key)
        dump_links(link_map[key])

def dump_names_map(names_map, msg=None):
    if not msg is None:
        print msg
    keys = names_map.keys()
    keys.sort()
    for key in keys:
        hashes = names_map[key]
        print "%s->(%s, %s)" % (key, str_sha(hashes[0]), str_sha(hashes[1]))

def dump_archive(archive, msg=None, brief=False):
    print "--- start archive dump ---"
    if not msg is None:
        print msg
    print "age: %i max_blocks: %i" % (archive.age, archive.max_blocks)
    dump_blocks(archive.blocks, "blocks:")

    dump_link_map(archive.blocks.link_map, "link_map:", brief)
    print "--- end ---"


def words():
    while True:
        yield sha1(str(random.random())).hexdigest()[:random.randrange(1, 9)]

WORD_ITR = words()

def lines(count):
    line = ""
    while count > 0:
        line += WORD_ITR.next()
        line += " "
        if len(line) > 60:
            ret = line
            line = ""
            count -= 1
            yield ret.strip()
    return

class ArchiveTestCase(unittest.TestCase):
    def setup_test_dirs(self, base_dir, dir_name):
        if not os.path.exists(base_dir):
            raise IOError("Base test directory doesn't exist: %s" % base_dir)

        full_path = os.path.join(base_dir, dir_name)
        if os.path.exists(full_path):
            raise IOError("Test directory exists: %s" % full_path)

        os.makedirs(full_path)
        self.test_root = full_path
        self.test_dir = os.path.join(self.test_root, TEST_DIR)
        self.tmp_dir  = os.path.join(self.test_root, TMP_DIR)
        os.makedirs(self.test_dir)
        os.makedirs(self.tmp_dir)

    def remove_test_dirs(self):
        assert self.test_root.endswith(TEST_ROOT)
        try:
            self.tmps.check_for_leaks()
        finally:
            if not LEAVE_TEST_DIR:
                shutil.rmtree(self.test_root)

    # Caller must release temp file.
    def write_file(self, raw):
        file_name = self.tmps.make_temp_file()
        out_file = open(file_name, 'wb')
        raised = True
        try:
            out_file.write(raw)
            out_file.close()
            raised = False
        finally:
            out_file.close()
            if raised:
                self.tmps.remove_temp_file(file_name)

        return file_name

    def read_file(self, file_name, remove_tmp=True):
        in_file = open(file_name, 'rb')
        try:
            ret = in_file.read()
        finally:
            in_file.close()
            if remove_tmp:
                self.tmps.remove_temp_file(file_name)
        return ret


    def setUp(self):
        self.setup_test_dirs(TEST_BASE, TEST_ROOT)
        self.tmps = HandleTemps(self.tmp_dir)

    def tearDown(self):
        self.remove_test_dirs()

class SmokeTests(ArchiveTestCase):
    def _testLeakATempFile(self):
        out_file = open(self.tmps.make_temp_file(), 'wb')
        out_file.write("OH NOES! FILZ IZ LIIKAN!!!")
        out_file.close()

    def make_empty_archive(self, block_name):
        archive = WORMBlockArchive(DeltaCoder(), BlockStorage(self.tmps))

        archive.create(self.test_dir, block_name)

        return archive

    def load_archive(self, block_name):
        archive = WORMBlockArchive(DeltaCoder(), BlockStorage(self.tmps))
        archive.load(self.test_dir, block_name)

        return archive

    def test_create_archive(self):
        print
        archive = self.make_empty_archive('A')
        dump_archive(archive)

    def test_load_archive(self):
        print
        self.make_empty_archive('A')
        b = self.load_archive('A')
        dump_archive(b)

    def test_archive_write_read(self):
        a = self.make_empty_archive('A')
        dump_archive(a, "empty")

        r0 = self.write_file("OH HAI!")
        r1 = self.write_file("OH HAI! AGAIN")
        r2 = self.write_file("STILL ME")

        t1 = self.tmps.make_temp_file()
        try:
            a.start_update()
            link0 = a.write_new_delta(NULL_SHA, r0)
            link1 = a.write_new_delta(NULL_SHA, r1)
            link2 = a.write_new_delta(NULL_SHA, r2)

            # Write
            a.commit_update()
            dump_archive(a, "updated")

            # Read
            print
            print str_sha(link0[0]), a.get_data(link0[0])
            print str_sha(link1[0]), a.get_data(link1[0])
            print str_sha(link2[0]), a.get_data(link2[0])

            a.close()

            b = self.load_archive('A')
            dump_archive(b, "[Reloaded from disk]")
            print
            # Mix up order.
            print str_sha(link1[0]), b.get_data(link1[0])
            print str_sha(link0[0]), b.get_data(link0[0])
            print str_sha(link2[0]), b.get_data(link2[0])
        finally:
            self.tmps.remove_temp_file(t1)
            self.tmps.remove_temp_file(r0)
            self.tmps.remove_temp_file(r1)
            self.tmps.remove_temp_file(r2)
            #a.abandon_update()

    def test_torture_a_single_chain(self):
        a = self.make_empty_archive('A')
        dump_archive(a, "empty")

        text = ""
        prev = NULL_SHA
        for iteration in range(0, 5000):
            # Write
            a.start_update()
            text += str(time.time()) +  '\n'
            t2 = self.write_file(text)
            #print "Adding to: ", str_sha(prev)

            link = a.write_new_delta(prev, t2)
            new_sha = link[0]
            link = None
            #print "Added: ", str_sha(new_sha), str_sha(new_parent)
            a.commit_update()
            self.tmps.remove_temp_file(t2)

            #history = a.blocks.get_history(new_sha)
            #history_size = sum([value[6] for value in history])
            #print "History: ", len(history), history_size, len(text)
            #print
            #dump_archive(a, "updated", True)

            t3 = self.tmps.make_temp_file()
            a.get_file(new_sha, t3)

            self.assertTrue(text == self.read_file(t3))

            prev = new_sha
            if iteration > 0 and iteration % 100 == 0:
                print "iteration: ", iteration

    # grrr... giving up on temp files
    def test_single_update(self):
        a = self.make_empty_archive('A')
        m = FileManifest()
        data = ( \
            ('foo.txt', 'This is the foo file.\n'),
            ('empty.txt', ''),
            ('big.txt', '*' * (1024 * 128)),
            )
        entries = entries_from_seq(self.tmps, data)
        m.update(a, entries)
        dump_archive(a)

    def test_multiple_updates(self):
        a = self.make_empty_archive('A')
        m = FileManifest()
        data0 = ( \
            ('foo.txt', 'This is the foo file.\n'),
            ('empty.txt', ''),
            ('big.txt', '*' * (1 * 128)),
            )

        print "manifest sha: ", str_sha(m.stored_sha)
        m.update(a, entries_from_seq(self.tmps, data0))
        print "manifest sha: ", str_sha(m.stored_sha)

        dump_archive(a, "AFTER FIRST WRITE:")
        verify_manifest(a, m)

        data1 = ( \
            ('foo.txt', 'This is the foo file.\n'),
            ('empty.txt', ''),
            ('big.txt', 'hello' + ('*' * (1 * 128))),
            )

        m.update(a, entries_from_seq(self.tmps, data1))
        print "manifest sha: ", str_sha(m.stored_sha)
        dump_archive(a)
        verify_link_map(a.blocks.link_map)
        verify_manifest(a, m)

    def test_words(self):
        print WORD_ITR.next()

    def test_lines(self):
        for line in lines(10):
            print line

    def test_many_updates(self):

        a = self.make_empty_archive('A')
        m = FileManifest()

        files = ("A.txt", "B.txt", "C.txt")

        updates = 100
        for dummy in range(0, updates):
            names = list(files)
            random.shuffle(names)
            #names = names[:random.randrange(1, len(files))]
            data = []
            for name in names:
                text = ''
                if name in m.name_map:
                    tmp = self.tmps.make_temp_file()
                    a.get_file(m.name_map[name][1], tmp)
                    text = self.read_file(tmp)
                text += "\n".join([line for line in lines(20)])

                data.append((name, text))

            #print "updating:"
            #for value in data:
            #    print value[0], len(value[1])

            #print "manifest sha: ", str_sha(m.stored_sha)
            #dump_archive(a, "BEFORE UPDATE: %i" % count, True)
            m.update(a, entries_from_seq(self.tmps, data))
            #print "manifest sha: ", str_sha(m.stored_sha)

            #dump_archive(a, "AFTER UPDATE: %i" % count, True)
            verify_manifest(a, m, True)
            verify_link_map(a.blocks.link_map)
            dump_blocks(a.blocks, None, True)

        a.close()


    def test_validate_path(self):
        base_dir = "/tmp/test/foo"
        validate_path(base_dir, "/tmp/test/foo/bar")
        validate_path(base_dir, "/tmp/test/foo/baz")
        validate_path(base_dir, "/tmp/test/foo/barf/text.dat")

        try:
            validate_path(base_dir, "/tmp/test/foo/../../../etc/passwd")
            self.assertTrue(False)
        except IOError, e:
            print "Got expected exception: ", e

        try:
            validate_path(base_dir, "/tmp/test/foo/../forbidden")
            self.assertTrue(False)
        except IOError, e:
            print "Got expected exception: ", e

        try:
            validate_path(base_dir,
                          u"/tmp/test/foo/f\xc3\xb6rbjuden.txt")
            self.assertTrue(False)
        except IOError, e:
            print "Got expected exception: ", e

        try:
            validate_path(base_dir,
                          "/tmp/test/foo/f\xc3\xb6rbjuden.txt")
            self.assertTrue(False)
        except IOError, e:
            print "Got expected exception: ", e

    def test_is_contiguous(self):
        self.assertTrue(is_contiguous( () ))
        self.assertTrue(is_contiguous( ((0, 0, '?'), ) ))
        self.assertTrue(is_contiguous( ((0, 0, 2), (1, 1, '?')) ))
        self.assertTrue(is_contiguous( ((0, 1, 2), (2, 3, '?')) ))
        self.assertFalse(is_contiguous( ((0, 0, 2), (2, 2, '?')) ))
        self.assertFalse(is_contiguous( ((0, 1, 2), (3, 3, '?')) ))

    # Trailing Zeros are ignored.
    def test_is_ordered(self):
        self.assertTrue(is_ordered( () ))
        self.assertTrue(is_ordered( (('?', '?', 2),) ))
        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 2)) ))
        self.assertFalse(is_ordered( (('?', '?', 2), ('?', '?', 1)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2)) ))
        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 2),
                                     ('?', '?', 2)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2),
                                     ('?', '?', 2)) ))
        self.assertFalse(is_ordered( (('?', '?', 1), ('?', '?', 0),
                                      ('?', '?', 2)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2),
                                     ('?', '?', 3)) ))

        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 0)) ))
        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 2),
                                     ('?', '?', 0)) ))
        self.assertFalse(is_ordered( (('?', '?', 2), ('?', '?', 1),
                                      ('?', '?', 0)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2),
                                     ('?', '?', 0), ('?', '?', 0)) ))


        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 2),
                                     ('?', '?', 2),
                                      ('?', '?', 0)) ))


        self.assertTrue(is_ordered( (('?', '?', 2), ('?', '?', 2),
                                     ('?', '?', 2),
                                     ('?', '?', 0)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2),
                                     ('?', '?', 2),
                                      ('?', '?', 0)) ))
        self.assertFalse(is_ordered( (('?', '?', 1), ('?', '?', 0),
                                      ('?', '?', 2),
                                      ('?', '?', 0)) ))
        self.assertTrue(is_ordered( (('?', '?', 1), ('?', '?', 2),
                                     ('?', '?', 3),
                                     ('?', '?', 0)) ))

        self.assertFalse(is_ordered( (('?', '?', 3), ('?', '?', 2),
                                      ('?', '?', 1),
                                      ('?', '?', 0)) ))

        self.assertFalse(is_ordered( (('?', '?', 3), ('?', '?', 2),
                                      ('?', '?', 1) )) )


    def test_repartition(self):
        for dummy in range(0, 1000):
            length = random.randrange(1, 8)
            blocks = [(index, index, random.randrange(1, 10))
                      for index in range(0, length)]
            self.assertTrue(is_contiguous(blocks))
            original_blocks = blocks[:]
            #were_ordered = is_ordered(blocks)
            #print blocks
            repartioned = repartition(blocks)
            #print repartioned
            self.assertTrue(is_ordered(repartioned))
            self.assertTrue(blocks == original_blocks)

            # Can't assert this anymore.
            # Trips when in order partitions get merged because they
            # don't meet the multiple constraint.
            # #self.assertTrue((were_ordered and blocks == repartioned) or
            #                ((not were_ordered) and blocks != repartioned))

            self.assertTrue(is_contiguous(repartioned))


    def updateFunc(self, blocks, change_len, max_len):
        assert len(blocks) > 0
        blocks = blocks[:]
        if blocks[0][2] + change_len < 32 * 1024:
            blocks[0] = (blocks[0][0], blocks[0][1], blocks[0][2] + change_len)
            return blocks
        # Add and compress
        blocks.insert(0, (-1, -1, change_len))
        return compress(blocks, max_len)

    def histogram(self, values, bin_width):
        table = {}
        for value in values:
            index = int(value/bin_width)
            table[index] = table.get(index, 0) + 1

        max_bin = max(table.keys())
        return tuple([(index, table.get(index, 0))
                      for index in range(0, max_bin + 1)])


    # Naive
    # DOESN'T SIMULATE:
    # o Dropping unreferenced chains.
    #   o GOOD: reduces total archive size
    #   o BAD: effective length of older blocks declines with time
    #          as unreferenced chains drop out. -> churn ???
    # o variance in commit sizes

    # HACKed this together fast, not sure it is correct.
    # Looks like I'm getting a power law dist.
    def test_simulate_updates(self):
        max_blocks = 4
        iterations = 10000
        change_size = 2*1024
        blocks = [(index, index, 0) for index in range(0, max_blocks)]
        changes = []
        for dummy in range(0, iterations):
            old_blocks = blocks[:]
            blocks = self.updateFunc(blocks, change_size, max_blocks)

            if not ((is_ordered(blocks) or
                (is_ordered(blocks[1:]) and blocks[0][2] < 32 * 1024))):
                print blocks

            self.assertTrue(is_ordered(blocks) or
                            (is_ordered(blocks[1:]) and
                             blocks[0][2] < 32 * 1024))

            changed = set(old_blocks) - set(blocks)
            for value in changed:
                # i.e. the number of bytes we had to write
                changes.append(value[2])

            # Fix ordinals. Shouldn't matter.
            blocks = [(index, index, blocks[index][2]) for index
                      in range(0, len(blocks))]

        #hist = self.histogram(changes, 32 * 1024)
        #for value in hist:
        #    print value[0], value[1]

        changes.sort()
        #max_insert = max(changes)
        for percent in (50, 75, 80, 85, 90, 95, 99, 100):
            point = changes[min(int((percent/100.0) * len(changes)),
                                len(changes) - 1)]
            print "%i %i %i" % (percent, point, point/(32*1024 + 1))


    def test_hg_repo_torture_test(self):
        if HG_REPO_DIR == '':
            print "Set HG_REPO_DIR!"
            self.assertTrue(False)

        writer = self.make_empty_archive('hgtst')
        manifest = FileManifest()

        rev = 0
        max_rev = 1 # Set below
        while rev < max_rev:
            target_dir = os.path.join(self.tmp_dir, '__hg_repo__')
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir) # DANGEROUS

            # export the repo
            # FIX: Wacky way to set max_rev.
            print "Exporting rev: ", rev
            max_rev = export_hg_repo(HG_REPO_DIR, target_dir, rev)
            if rev >= max_rev:
                break

            # put the export dir into the archive
            # print "Inserting into the archive..."

            entries = entries_from_dir(target_dir, True)
            manifest.update(writer, entries)

            # Will be written into Freenet top key
            # along with rest of archive info.
            s3kr1t = manifest.stored_sha

            dump_blocks(writer.blocks, None, True)
            # create a second archive instance from the same block files.
            # REDFLAG: Would this work on windoze?
            #          writer still has files open for reading.
            reader = self.load_archive('hgtst')
            read_manifest = FileManifest.from_archive(reader, s3kr1t)
            # REDFLAG: audit for other places where I could do
            # direct dict compares?
            assert (read_manifest.name_map ==  manifest.name_map)

            # clean the archive output dir
            unarchived_dir = os.path.join(self.tmp_dir, '__unarchived__')
            if os.path.exists(unarchived_dir):
                shutil.rmtree(unarchived_dir) # DANGEROUS

            os.makedirs(unarchived_dir)

            # extract the archive to the cleaned files
            manifest_to_dir(reader, read_manifest, unarchived_dir)
            reader.close()

            # diff the directories

            # A poor man's diff.
            insert_map = {}
            for entry in entries_from_dir(target_dir, True):
                insert_map[entry.get_name()] = get_file_sha(entry.make_file())
                entry.release() # NOP

            unarchived_map = {}
            for entry in entries_from_dir(unarchived_dir, True):
                unarchived_map[entry.get_name()] = (
                    get_file_sha(entry.make_file()))
                entry.release() # NOP


            assert len(insert_map) > 0
            assert insert_map == unarchived_map
            print "%i files compared equal." % len(insert_map)

            rev += 1


if __name__ == '__main__':
    # use -v on command line to get verbose output.
    # verbosity keyword arg not supported in 2.6?
    if len(sys.argv) >= 2 and sys.argv[1] != '-v':
        # Run a single test case
        suite = unittest.TestSuite()
        suite.addTest(SmokeTests(sys.argv[1]))
        unittest.TextTestRunner().run(suite)
    else:
        # Run everything.
        unittest.main()
