""" Minimal UTs for InsertingRedundantBlocks, RequestingRedundantBlocks.

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

# REQUIRES: mercurial in PYTHONPATH!
# REDFLAG: LATER: Add test case for blocks too big to salt! (tested manually)

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
# Allow mocked ui class, FakeUI() with only 2 public methods.
# pylint: disable-msg=R0903
import os
import random
import shutil
import sys
import unittest

from infcmds import UICallbacks, run_until_quiescent
from fcpconnection import PolledSocket, FCPConnection
from fcpclient import FCPClient
from requestqueue import RequestRunner
from statemachine import State
from archivesm import ArchiveStateMachine, ArchiveUpdateContext, \
     create_dirs, start, InsertingRedundantBlocks, RequestingRedundantBlocks, \
     chk_file_name

from updatesm import FAILING, FINISHING, QUIESCENT
from graph import FREENET_BLOCK_LEN

TEST_BASE = '/tmp'
TEST_ROOT = '__block_test_run__'
TMP_DIR = "__TMP__"

FCP_HOST = '127.0.0.1'
FCP_PORT = 19481
N_CONCURRENT = 4
POLL_SECS = 0.25
CANCEL_TIME_SECS = 5 * 60

# Doesn't need to be fetchable, just for building cache subdir.
SOME_USK = ('USK@Q60BTelEyg6V2KK97k1WNHA7N77pkE-v3m5~hHbm3ew,' +
            'IgRKyz2LoDCv0a1ptc5ycWtYknNP6DL1E8o4VM0tZ6Q,AQACAAE/small/4')


BLOCK_DEF = \
((44003, ('CHK@A6rpa~7jmUbZ55fugPuziwrZdLhmDUo6OorLVGB45f8,' +
         '4P~momeirpQpvnCIqT3P5D5Z~a486IQXqI3s7R6FQjg,AAIC--8',
         'CHK@LzqlbkyyUAixGXD52kMu8uad1CxGgW0QGSHNP-WrP-4,' +
         'mc3P0kb17xpAHtjh2rG2EfDWujp8bN0~L5GuezNV50E,AAIC--8')
 ),)

FILE_BLOCKS_DEF = \
(('', 44003, ('CHK@A6rpa~7jmUbZ55fugPuziwrZdLhmDUo6OorLVGB45f8,' +
              '4P~momeirpQpvnCIqT3P5D5Z~a486IQXqI3s7R6FQjg,AAIC--8',
              'CHK@LzqlbkyyUAixGXD52kMu8uad1CxGgW0QGSHNP-WrP-4,' +
              'mc3P0kb17xpAHtjh2rG2EfDWujp8bN0~L5GuezNV50E,AAIC--8')
 ),)


# State stored across tests.
SHARED_STATE = {}
#SHARED_STATE['FILE_BLOCKS'] = FILE_BLOCKS_DEF


def bytes(count, offset):
    return "".join([chr((index + offset) % 256) for index in range(0, count)])

def bad_chk_itr():
    ordinal = ord('A')

    while ordinal <= ord('Z'):
        yield (('CHK@badroutingkey0%s0JblbGup0yNSpoDJgVPnL8E5WXoc,' %
                chr(ordinal)) +
               'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
        ordinal += 1
    return

BAD_CHKS = bad_chk_itr()

def break_primary(chks):
    chks = list(chks)
    assert len(chks) > 0
    chks[0] = BAD_CHKS.next()
    return chks

def break_redundant(chks):
    chks = list(chks)
    assert len(chks) > 0
    chks[-1] = BAD_CHKS.next()
    return chks

# Not sure that this will work.
class FakeUI:
    def __init__(self):
        pass

    def status(self, text):
        if text.endswith('\n'):
            text = text[:-1]
            print text

class HoldingBlocks(State):
    """ State to hold blocks for testing RequestingRedundantBlocks """
    def __init__(self, parent, name, next_state):
        State.__init__(self, parent, name)
        self.next_state = next_state
        self.blocks = ()

    def enter(self, dummy_from_state):
        """ State implemenation. """

        print self.blocks
        self.parent.transition(self.next_state)

    def reset(self):
        pass

    def get_blocks(self):
        """ Return the cached blocks. """
        return self.blocks

class RedundancyTests(unittest.TestCase):
    def setup_test_dirs(self, base_dir, dir_name):
        if not os.path.exists(base_dir):
            raise IOError("Base test directory doesn't exist: %s" % base_dir)

        full_path = os.path.join(base_dir, dir_name)
        if os.path.exists(full_path):
            raise IOError("Test directory exists: %s" % full_path)

        os.makedirs(full_path)
        self.test_root = full_path
        self.tmp_dir  = os.path.join(self.test_root, TMP_DIR)
        os.makedirs(self.tmp_dir)

    def remove_test_dirs(self):
        assert self.test_root.endswith(TEST_ROOT)
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)

    def setUp(self):
        self.setup_test_dirs(TEST_BASE, TEST_ROOT)
        self.connection = None

    def tearDown(self):
        if not self.connection is None:
            self.connection.close()

        self.remove_test_dirs()

    def make_state_machine(self):
        if not self.connection is None:
            self.connection.close()

        callbacks = UICallbacks(FakeUI())
        callbacks.verbosity = 5
        # Knows about reading and writing bytes.
        async_socket = PolledSocket(FCP_HOST, FCP_PORT)
        # Knows about running the FCP protocol over async_socket.
        self.connection = FCPConnection(async_socket, True,
                                        callbacks.connection_state)
        # Knows about running requests from a request queue.
        runner = RequestRunner(self.connection, N_CONCURRENT)
        # Knows how to run series of requests to perform operations
        # on an archive in Freenet.
        sm = ArchiveStateMachine(runner, ArchiveUpdateContext())
        sm.transition_callback = callbacks.transition_callback
        sm.monitor_callback = callbacks.monitor_callback
        sm.params['CANCEL_TIME_SECS'] = CANCEL_TIME_SECS

        return sm


    def checkCHK(self, chk, logical_len, length, data=None):
        print "---"
        print "Checking: ", chk
        # Something is closing the connection?
        resp = FCPClient.connect(FCP_HOST, FCP_PORT).get(chk)
        self.assertTrue(resp[0] == 'AllData')
        print "Length: ", len(resp[2])
        print "Mime_Type: ", resp[1]['Metadata.ContentType']
        if len(resp[2]) != length:
            print "Expected len: %i, got: %i!" % (length, len(resp[2]))
            self.assertTrue(False)
        if not data is None and resp[2][:logical_len] != data:
            print "Data doesn't match! (only showing first 16 bytes below)"
            print "got: ", repr(resp[2][:logical_len][:16])
            print "expected: " , repr(data[:16])
            self.assertTrue(False)

    def _testCheckCHK(self):
        self.make_state_machine()
        self.checkCHK("CHK@Q~xLO5t0tVCkrJ8MAZUeFijK090CsJdJ1RGoRQPbUfY," +
                      "gWj4935igWd~LuhckS6bST~-qfJ5oW8E5YEa7Yy-tzk,AAIC--8",
                      32767,
                      32767 + 1)

    def test_inserting(self):
        # Takes longer to insert existing blocks?
        offset = random.randrange(0, 256)
        print "offset: ", offset
        lengths = (FREENET_BLOCK_LEN - 1,
                   FREENET_BLOCK_LEN,
                   FREENET_BLOCK_LEN + 1,
                   1,
                   FREENET_BLOCK_LEN + 11235,
                   )

        insert_files = []
        for index, length in enumerate(lengths):
            full_path = os.path.join(self.tmp_dir,
                                     "%i.bin" % index)
            out_file = open(full_path, 'wb')
            out_file.write(bytes(length, offset))
            out_file.close()
            self.assertTrue(os.path.getsize(full_path) == length)
            insert_files.append(full_path)

        update_sm = self.make_state_machine()
        self.assertTrue(not 'TEST_STATE' in update_sm.states)
        update_sm.states['TEST_STATE'] = (
            InsertingRedundantBlocks(update_sm,
                                     'TEST_STATE',
                                     FINISHING,
                                     FAILING))


        ctx = ArchiveUpdateContext(update_sm, FakeUI())
        ctx.update({'ARCHIVE_CACHE_DIR':self.tmp_dir,
                    'REQUEST_URI':SOME_USK,
                    'ARCHIVE_BLOCK_FILES':insert_files,
                    'START_STATE':'TEST_STATE'})

        create_dirs(ctx.ui_,
                    ctx['ARCHIVE_CACHE_DIR'],
                    ctx['REQUEST_URI'])

        start(update_sm, ctx)
        run_until_quiescent(update_sm, POLL_SECS)
        self.assertTrue(update_sm.get_state(QUIESCENT).
                        arrived_from(((FINISHING,))))

        blocks = update_sm.states['TEST_STATE'].files
        for index, entry in enumerate(blocks):
            print "block [%i]: len: %i" % (index, entry[1])
            for chk in entry[2]:
                print "   ", chk

        # FREENET_BLOCK_LEN - 1, first is unpadded
        self.checkCHK(blocks[0][2][0], blocks[0][1], blocks[0][1],
                      bytes(blocks[0][1], offset))
        # FREENET_BLOCK_LEN - 1, second is padded
        self.checkCHK(blocks[0][2][1], blocks[0][1], blocks[0][1] + 1,
                      bytes(blocks[0][1], offset))

        # FREENET_BLOCK_LEN first is padded
        self.checkCHK(blocks[1][2][0], blocks[1][1], blocks[1][1] + 1,
                      bytes(blocks[1][1], offset))
        # FREENET_BLOCK_LEN second is padded
        self.checkCHK(blocks[1][2][1], blocks[1][1], blocks[1][1] + 1,
                      bytes(blocks[1][1], offset))

        # FREENET_BLOCK_LEN + 1, first is unpadded
        self.checkCHK(blocks[2][2][0], blocks[2][1], blocks[2][1],
                      bytes(blocks[2][1], offset))
        # FREENET_BLOCK_LEN + 1, second is unpadded
        self.checkCHK(blocks[2][2][1], blocks[2][1], blocks[2][1],
                      bytes(blocks[2][1], offset))

        # 1, first is unpadded
        self.checkCHK(blocks[3][2][0], blocks[3][1], blocks[3][1],
                      bytes(blocks[3][1], offset))

        # 1, second is padded
        self.checkCHK(blocks[3][2][1], blocks[3][1], blocks[3][1] + 1,
                      bytes(blocks[3][1], offset))


        # FREENET_BLOCK_LEN + 11235, first is unpadded
        self.checkCHK(blocks[4][2][0], blocks[4][1], blocks[4][1],
                      bytes(blocks[4][1], offset))

        # FREENET_BLOCK_LEN + 11235, second is unpadded
        self.checkCHK(blocks[4][2][1], blocks[4][1], blocks[4][1],
                      bytes(blocks[4][1], offset))

        # Save info for use in request testing
        SHARED_STATE['FILE_BLOCKS'] =  blocks
        SHARED_STATE['OFFSET'] = offset


    def setup_request_sm(self):
        """ Helper sets up a state machine instance containing a
            RequestingRedundantBlocks instance. """
        update_sm = self.make_state_machine()
        self.assertTrue(not 'TEST_HAS_BLOCKS' in update_sm.states)

        update_sm.states['TEST_HAS_BLOCKS'] = (
            HoldingBlocks(update_sm, 'TEST_HAS_BLOCKS',
                          'TEST_REQUESTING'))


        update_sm.states['TEST_REQUESTING'] = (
            RequestingRedundantBlocks(update_sm,
                                      'TEST_REQUESTING',
                                      FINISHING,
                                      FAILING))

        ctx = ArchiveUpdateContext(update_sm, FakeUI())
        ctx.update({'ARCHIVE_CACHE_DIR':self.tmp_dir,
                    'REQUEST_URI':SOME_USK,
                    'START_STATE':'TEST_HAS_BLOCKS'})

        create_dirs(ctx.ui_,
                    ctx['ARCHIVE_CACHE_DIR'],
                    ctx['REQUEST_URI'])

        return (ctx, update_sm, update_sm.states['TEST_HAS_BLOCKS'])


    def verify_not_cached(self, ctx, blocks):
        for block in blocks:
            for chk in block[1]:
                full_path = os.path.join(ctx.arch_cache_dir(),
                                         chk_file_name(chk))
                if os.path.exists(full_path):
                    print "Already cached: ", chk
                    self.assertTrue(False)


    def verify_cached(self, ctx, blocks):
        for index, block in enumerate(blocks):
            count = 0
            for ordinal, chk in enumerate(block[1]):
                full_path = os.path.join(ctx.arch_cache_dir(),
                                         chk_file_name(chk))
                if os.path.exists(full_path):
                    print "%s: CACHED" % str((index, ordinal))
                    self.assertTrue(os.path.getsize(full_path) ==
                                    block[0])
                    count += 1
                else:
                    print "%s: MISSING" % str((index, ordinal))
            self.assertTrue(count > 0)


    # REQUIRES: test_inserting run first.
    def test_requesting_all(self):
        if not 'FILE_BLOCKS' in SHARED_STATE:
            print "You must run test_inserting() before this test."
            self.assertTrue(False)

        ctx, update_sm, start_state = self.setup_request_sm()

        blocks = []
        for entry in SHARED_STATE['FILE_BLOCKS']:
            blocks.append((entry[1], tuple(entry[2])))

        self.verify_not_cached(ctx, blocks)
        start_state.blocks = tuple(blocks)

        start(update_sm, ctx)
        run_until_quiescent(update_sm, POLL_SECS)
        self.assertTrue(update_sm.get_state(QUIESCENT).
                        arrived_from(((FINISHING,))))

        self.verify_cached(ctx, blocks)

    def test_requesting_primary(self):
        if not 'FILE_BLOCKS' in SHARED_STATE:
            print "You must run test_inserting() before this test."
            self.assertTrue(False)

        ctx, update_sm, start_state = self.setup_request_sm()

        blocks = []
        for entry in SHARED_STATE['FILE_BLOCKS']:
            blocks.append((entry[1], tuple(break_redundant(entry[2]))))

        self.verify_not_cached(ctx, blocks)
        start_state.blocks = tuple(blocks)

        start(update_sm, ctx)
        run_until_quiescent(update_sm, POLL_SECS)
        self.assertTrue(update_sm.get_state(QUIESCENT).
                        arrived_from(((FINISHING,))))

        self.verify_cached(ctx, blocks)

    def test_requesting_redundant(self):
        if not 'FILE_BLOCKS' in SHARED_STATE:
            print "You must run test_inserting() before this test."
            self.assertTrue(False)

        ctx, update_sm, start_state = self.setup_request_sm()

        blocks = []
        for entry in SHARED_STATE['FILE_BLOCKS']:
            blocks.append((entry[1], tuple(break_primary(entry[2]))))

        self.verify_not_cached(ctx, blocks)
        start_state.blocks = tuple(blocks)

        start(update_sm, ctx)
        run_until_quiescent(update_sm, POLL_SECS)
        self.assertTrue(update_sm.get_state(QUIESCENT).
                        arrived_from(((FINISHING,))))

        self.verify_cached(ctx, blocks)



if __name__ == '__main__':
    # use -v on command line to get verbose output.
    # verbosity keyword arg not supported in 2.6?
    if len(sys.argv) >= 2 and sys.argv[1] != '-v':
        # Run a single test case
        suite = unittest.TestSuite()
        suite.addTest(RedundancyTests(sys.argv[1]))
        unittest.TextTestRunner().run(suite)
    else:
        # Run everything.
        unittest.main()
