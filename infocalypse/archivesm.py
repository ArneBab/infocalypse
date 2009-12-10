""" Classes to asynchronously create, push and pull incremental archives.

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
import random
import shutil

import archivetop

from fcpconnection import make_id, SUCCESS_MSGS, sha1_hexdigest
from fcpclient import get_version, get_usk_hash, get_usk_for_usk_version, \
     is_usk
from fcpmessage import GET_DEF, PUT_FILE_DEF

from statemachine import StateMachine, State, DecisionState, \
     RetryingRequestList, CandidateRequest
from updatesm import UpdateStateMachine, QUIESCENT, FAILING, FINISHING, \
     RequestingUri, InsertingUri, UpdateContextBase, PAD_BYTE

from archivetop import top_key_tuple_to_bytes, default_out

from chk import clear_control_bytes
from graph import FREENET_BLOCK_LEN, MAX_METADATA_HACK_LEN

TMP_DIR = "__TMP__"
BLOCK_DIR = "__TMP_BLOCKS__"

ARC_MIME_TYPE = 'application/archive-block'
ARC_MIME_TYPE_FMT = ARC_MIME_TYPE + ';%i'
ARC_METADATA_MARKER = ARC_MIME_TYPE + ';'

# Careful when changing. There is code that depends on '_' chars.
TOP_KEY_NAME_PREFIX = "_top_"
TOP_KEY_NAME_FMT = TOP_KEY_NAME_PREFIX + "%i_.bin"
CHK_NAME_PREFIX = "_chk_"
CHK_NAME_FMT = CHK_NAME_PREFIX + "%s.bin"

ARC_REQUESTING_URI = 'ARC_REQUESTING_URI'
ARC_CACHING_TOPKEY = 'ARC_CACHING_TOPKEY'
ARC_REQUESTING_BLOCKS = 'ARC_REQUESTING_BLOCKS'

ARC_INSERTING_BLOCKS = 'ARC_INSERTING_BLOCKS'
ARC_FIXING_UP_TOP_KEY = 'ARC_FIXING_UP_TOP_KEY'
ARC_INSERTING_URI = 'ARC_INSERTING_URI'
ARC_CACHING_INSERTED_TOPKEY = 'ARC_CACHING_INSERTED_TOPKEY'

# File name that 0) is tagged for deletion and
# 1) doesn't have weird chars in it. e.g. '~'
# Hmmm... it would be more studly to extract the (SHA256?)
# hash from the CHK and use that
def chk_file_name(chk):
    """ Return a file name for the CHK. """
    return CHK_NAME_FMT % sha1_hexdigest(chk)

class ArchiveUpdateContext(UpdateContextBase):
    """ An UpdateContextBase for running incremental archive commands. """
    def __init__(self, parent=None, ui_=None):
        UpdateContextBase.__init__(self, parent)
        self.ui_ = ui_

    def arch_cache_top_key(self, uri, top_key_tuple):
        """ Store top key in local archive cache. """
        out_file = open(os.path.join(self.arch_cache_dir(),
                                     TOP_KEY_NAME_FMT % get_version(uri)),
                        'wb')
        try:
            out_file.write(top_key_tuple_to_bytes(top_key_tuple))
        finally:
            out_file.close()

    # Might rename file_name. Might not. Hmmm....
    def arch_cache_block(self, chk, file_name, length=None):
        """ Store block in local cache. """
        dest =  os.path.join(self.arch_cache_dir(),
                             chk_file_name(chk))

        if os.path.exists(dest):
            return

        if not os.path.exists(file_name):
            print "DOESN'T EXIST: ", file_name
            return

        if not length is None:
            #print length, os.path.getsize(file_name)
            assert length <= os.path.getsize(file_name)
            if length < os.path.getsize(file_name):
                out_file = open(file_name, 'ab')
                try:
                    out_file.truncate(length)
                finally:
                    out_file.close()
            assert length == os.path.getsize(file_name)

        os.rename(file_name, dest)

    def required_blocks(self, top_key_tuple):
        """ Return ((block_len, (chk0, ..), ...) for
            non-locally-cached blocks. """
        ret = []
        for block in top_key_tuple[0]:
            required_chks = []
            cached = 0
            for chk in block[1]:
                if not os.path.exists(os.path.join(self.arch_cache_dir(),
                                                   chk_file_name(chk))):
                    #print "NEEDS: ", chk
                    required_chks.append(chk)
                else:
                    #print "HAS: ", chk
                    cached += 1
            if cached == 0:
                assert len(required_chks) > 0
                ret.append((block[0], tuple(required_chks)))
        return ret

    def arch_cache_dir(self):
        """ Return the local cache directory. """
        return os.path.join(self['ARCHIVE_CACHE_DIR'],
                            get_usk_hash(self['REQUEST_URI']))


class RequestingArchiveUri(RequestingUri):
    """ A state to request the top level URI for an archive. """
    def __init__(self, parent, name, success_state, failure_state):
        RequestingUri.__init__(self, parent, name, success_state,
                               failure_state)
        self.topkey_funcs = archivetop

class InsertingArchiveUri(InsertingUri):
    """ A state to insert the top level URI for an archive into Freenet."""
    def __init__(self, parent, name, success_state, failure_state):
        InsertingUri.__init__(self, parent, name, success_state, failure_state)
        self.topkey_funcs = archivetop

    # Why didn't I do this in the base class? Can't remember.
    def leave(self, to_state):
        """ Override to update REQUEST_URI in the parent's context. """
        InsertingUri.leave(self, to_state)

        if to_state.name != self.success_state:
            return

        if self.parent.ctx['INSERT_URI'] is None:
            # Assert reinserting???
            return # i.e. for reinserting.

        if not (is_usk(self.parent.ctx['INSERT_URI']) and
                is_usk(self.parent.ctx['REQUEST_URI'])):
            return

        if (get_version(self.parent.ctx['INSERT_URI']) >
            get_version(self.parent.ctx['REQUEST_URI'])):

            version = get_version(self.parent.ctx['INSERT_URI'])

            self.parent.ctx['REQUEST_URI'] = (
                get_usk_for_usk_version(self.parent.ctx['REQUEST_URI'],
                                        version))

def break_top_key(top_key, failure_probability=0.5, max_broken=3, max_keys=1):
    """ Debugging helper function to test block failures. """
    ordinal = ord('A')
    blocks = []
    for block in top_key[0]:
        chks = []
        count = 0
        for chk in block[1]:
            if random.random() < failure_probability and count < max_broken:
                chk = (('CHK@badroutingkey0%s0JblbGup0yNSpoDJgVPnL8E5WXoc,' %
                        chr(ordinal)) +
                       'KZ6azHOwEm4ga6dLy6UfbdSzVhJEz3OvIbSS4o5BMKU,AAIC--8')
                ordinal += 1
                count += 1
            chks.append(chk)
            if len(chks) >= max_keys:
                break
        blocks.append((block[0], tuple(chks), block[2]))

    top_key = list(top_key)
    top_key[0] = blocks
    top_key = tuple(top_key)
    return top_key

class CachingTopKey(DecisionState):
    """ State to locally cache the archive top key. """
    def __init__(self, parent, name, has_blocks_state,
                 needs_blocks_state):
        DecisionState.__init__(self, parent, name)
        self.has_blocks_state = has_blocks_state
        self.needs_blocks_state = needs_blocks_state
        self.cached_blocks = None

    def reset(self):
        """ State override. """
        self.cached_blocks = None

    def decide_next_state(self, from_state):
        """ DecisionState implementation. """

        if not hasattr(from_state, 'get_top_key_tuple'):
            raise Exception("Illegal Transition from: %s" % from_state.name)
        top_key = from_state.get_top_key_tuple()

        #top_key = break_top_key(top_key)
        #archivetop.dump_top_key_tuple(top_key)

        uri = self.parent.ctx['REQUEST_URI'] # WRONG FOR INSERT (+1)

        # Hmmmm... push this into the context? ctx.request_uri()
        insert_uri = self.parent.ctx.get('INSERT_URI', None)
        if not insert_uri is None and insert_uri != 'CHK@':
            version = get_version(insert_uri)
            uri = get_usk_for_usk_version(uri, max(version, get_version(uri)))

        self.parent.ctx.arch_cache_top_key(uri, top_key)
        self.cached_blocks = self.parent.ctx.required_blocks(top_key)
        if len(self.cached_blocks) > 0:
            #print "NEEDS BLOCKS: ", len(self.cached_blocks)
            return self.needs_blocks_state
        #print "HAS BLOCKS"
        return self.has_blocks_state

    def get_blocks(self):
        """ Return the blocks from the previous state. """
        return self.cached_blocks

def twiddle_metadata_salting(raw_bytes, marker):
    """ INTERNAL: Flip the embedded salting string in splitfile metadata. """
    assert len(raw_bytes) <= FREENET_BLOCK_LEN
    pos = raw_bytes.find(marker)
    if pos == -1 or len(raw_bytes) < pos + len(marker) + 1:
        raise Exception("Couldn't read marker string.")

    salted_pos = pos + len(marker)
    old_salt = raw_bytes[salted_pos]
    if old_salt != '0':
        raise Exception("Unexpected salt byte: %s" % old_salt)

    twiddled_bytes = raw_bytes[:salted_pos] + '1' \
                     + raw_bytes[salted_pos + 1:]
    assert len(raw_bytes) == len(twiddled_bytes)

    return twiddled_bytes


class InsertingRedundantBlocks(RetryingRequestList):
    """ State to redundantly insert CHK blocks. """
    def __init__(self, parent, name, success_state, failure_state):
        RetryingRequestList.__init__(self, parent, name)
        # [file_name, file_len, [CHK0, CHK1], raw_top_key_data]
        self.files = []

        # Candidate is:
        # (index, ordinal)
        # index is an index into self.files
        # ordinal 2 means request topkey data
        # ordinal 0, 1 insert
        self.success_state = success_state
        self.failure_state = failure_state

    def enter(self, dummy_from_state):
        """ State implementation. """
        self.files = []
        if len(self.parent.ctx['ARCHIVE_BLOCK_FILES']) == 0:
            raise ValueError("No files to in ctx['ARCHIVE_BLOCK_FILES'].")

        for value in self.parent.ctx['ARCHIVE_BLOCK_FILES']:
            # REDFLAG: LATER: partial redundant insert handling ?
            length = os.path.getsize(value)
            self.files.append([value, length,
                              [None, None], None])
            # Unsalted.
            self.current_candidates.insert(0, (len(self.files) - 1, 0))
            #print "QUEUED NORMAL: ", self.current_candidates[0],
            #   self.files[-1][1]
            if length < FREENET_BLOCK_LEN:
                # Padded.
                self.current_candidates.insert(0, (len(self.files) - 1, 1))
                #print "QUEUED PADDED: ", self.current_candidates[0],
                #   self.files[-1][1]
            # else:
            #   candidate_done() will queue a salted insert.

    def leave(self, to_state):
        """ State implementation. """
        if to_state.name == self.success_state:
            #print "SUCCEEDED: ", self.name
            # Dump the inserted blocks into the cache.
            for entry in self.files:
                assert not entry[2][0] is None
                self.parent.ctx.arch_cache_block(entry[2][0],
                                                 entry[0], entry[1])
                #print "CACHED: ", entry[2][0]
        #else:
        #    print "FAILED: ", self.name

    def reset(self):
        """ State implementation. """
        RetryingRequestList.reset(self)
        self.files = []

    def get_blocks(self):
        """ Return the block definitions. """
        return self.files[:]

    def make_request(self, candidate):
        """ RetryingRequestList implementation. """
        #print "CREATED: ", candidate
        entry = self.files[candidate[0]]
        request = CandidateRequest(self.parent)
        request.tag = str(candidate) # Hmmm
        request.candidate = candidate
        request.in_params.fcp_params = self.parent.params.copy()

        request.in_params.definition = PUT_FILE_DEF
        request.in_params.fcp_params['URI'] = 'CHK@'

        if candidate[1] == 0:
            # Simple insert.
            request.in_params.file_name = entry[0]
            request.in_params.send_data = True
            # IMPORTANT: Don't add metadata to < 32K blocks to avoid redirect.
            if entry[1] >= FREENET_BLOCK_LEN:
                request.in_params.fcp_params['Metadata.ContentType'] = (
                   ARC_MIME_TYPE_FMT % 0)

            if entry[1] == FREENET_BLOCK_LEN:
                # UT hits this code path.
                #print "HIT len==FREENET_BLOCK_LEN case"
                # IMPORTANT: Special case len == FREENET_BLOCK_LEN
                # PAD to force splitfile insertion, so that we can salt.
                in_file = open(entry[0],'rb')
                try:
                    # Read raw data and add one zero pad byte.
                    request.in_params.send_data = in_file.read() + PAD_BYTE
                    assert (len(request.in_params.send_data) ==
                            FREENET_BLOCK_LEN + 1)
                    request.in_params.file_name = None # i.e. from string above.
                finally:
                    in_file.close()

        elif candidate[1] == 1:
            # Redundant insert.
            if entry[1] < FREENET_BLOCK_LEN:
                in_file = open(entry[0],'rb')
                try:
                    # Read raw data and add one zero pad byte.
                    request.in_params.send_data = in_file.read() + PAD_BYTE
                finally:
                    in_file.close()
            else:
                # Salted metadata.
                assert not entry[3] is None
                request.in_params.send_data = (
                    twiddle_metadata_salting(entry[3], ARC_METADATA_MARKER))
        elif candidate[1] == 2:
            # Raw topkey request
            assert entry[2][0] != None
            request.in_params.definition = GET_DEF
            request.in_params.fcp_params['MaxSize'] = FREENET_BLOCK_LEN
            request.in_params.fcp_params['URI'] = (
                clear_control_bytes(entry[2][0]))
        else:
            raise ValueError("Bad candidate: " + candidate)

        self.parent.ctx.set_cancel_time(request)
        return request

    def candidate_done(self, client, msg, candidate):
        """ RetryingRequestList implementation. """
        #print "DONE: ", candidate
        if not msg[0] in SUCCESS_MSGS:
            # LATER: Retry on failure???
            # REDFLAG: message to ui?
            self.parent.transition(self.failure_state)
            return

        # Keep UpdateStateMachine.request_done() from deleting.
        client.in_params.file_name = None

        index, chk_ordinal = candidate
        if chk_ordinal < 2:
            # Stash inserted URI.
            chk = msg[1]['URI']
            if chk_ordinal == 0:
                self.files[index][2][0] = chk
            else:
                if self.files[index][1] >= FREENET_BLOCK_LEN:
                    # HACK HACK HACK
                    # TRICKY:
                    # Scrape the control bytes from the full request
                    # to enable metadata handling.
                    chk0 = self.files[index][2][0]
                    chk0_fields = chk0.split(',')
                    chk1_fields = chk.split(',')
                    # Hmmm... also no file names.
                    assert len(chk0_fields) == len(chk1_fields)
                    chk = ','.join(chk1_fields[:-1] + chk0_fields[-1:])

                self.files[index][2][1] = chk

        if chk_ordinal == 0 and (self.files[index][1] >= FREENET_BLOCK_LEN and
                                 self.files[index][1] <= MAX_METADATA_HACK_LEN):
            # Queue a top block only request for the inserted splitfile
            # metadata so that we can salt it.
            self.current_candidates.append((index, 2)) # LIFO

        if chk_ordinal == 2:
            # Insert a salted alias for the splitfile metadata.
            assert self.files[index][1] >= FREENET_BLOCK_LEN
            self.files[index][3] = msg[2]
            self.current_candidates.append((index, 1)) # LIFO

        if self.is_stalled():
            # Nothing more to do, so we succeeded.
            self.parent.transition(self.success_state)

def chk_iter(inserter_files):
    """ INTERNAL: Iterator which yields CHKs from
        InsertingRedundantBlocks.files. """
    for block in inserter_files:
        for chk in block[2]:
            if not chk is None:
                yield chk

class FixingUpTopKey(State):
    """ State to fix up missing CHKs in a top key with inserted values. """
    def __init__(self, parent, name, success_state):
        State.__init__(self, parent, name)
        self.success_state = success_state
        self.fixed_up_top_key = None

    def reset(self):
        """ State implementation. """
        self.fixed_up_top_key = None

    def enter(self, from_state):
        """ State implementation. """
        if not hasattr(from_state, 'get_blocks'):
            raise Exception("Illegal transition from: %s" % from_state.name)

        assert 'PROVISIONAL_TOP_KEY' in self.parent.ctx
        top_key = self.parent.ctx['PROVISIONAL_TOP_KEY']
        # Hmmm... Opaque. Fails with StopIteration if something goes wrong.
        chks = chk_iter(from_state.get_blocks())
        updated_blocks = []
        for block in top_key[0]:
            new_block = list(block)
            new_block[1] = list(new_block[1])
            for index, chk in enumerate(block[1]):
                if chk == 'CHK@':
                    # Use the CHK's inserted by the previous state
                    # to fixup the CHK values in the provisional top key tuple.
                    new_block[1][index] = chks.next()
            new_block[1] = tuple(new_block[1])
            new_block = tuple(new_block)
            updated_blocks.append(new_block)

        top_key = list(top_key)
        top_key[0] = tuple(updated_blocks)
        top_key = tuple(top_key)
        self.fixed_up_top_key = top_key
        self.parent.transition(self.success_state)

    def get_top_key_tuple(self):
        """ Return the fixed up top key. """
        assert not self.fixed_up_top_key is None
        return self.fixed_up_top_key

# REDFLAG: feels like way too much code.  Really this complicated?
IS_RUNNING = 'running' # SENTINAL
class RequestHistory:
    """ INTERNAL: Helper class to keep track of redundant block request
        state. """
    def __init__(self):
        self.history  = {}

    def reset(self):
        """ Reset. """
        self.history = {}

    def dump(self, out_func=default_out):
        """ Debugging dump function. """
        keys = self.history.keys()
        keys.sort()
        out_func("--- dumping request history ---\n")
        for key in keys:
            out_func("%s->%s\n" % (str(key), str(self.history[key])))
        out_func("---\n")

    def started_request(self, candidate):
        """ Record that a request for the candidate was started. """
        #print "started_request -- ", candidate
        #self.dump()
        assert not candidate in self.history
        self.history[candidate] = IS_RUNNING

    def finished_request(self, candidate, result):
        """ Record that a request for the candidate finished. """
        #print "finished_request -- ", candidate, result
        #self.dump()
        assert candidate in self.history
        assert self.history[candidate] is IS_RUNNING
        self.history[candidate] = bool(result)

    def is_running (self, candidate):
        """ Return True if a request for the candidate is running. """
        return self.history.get(candidate, None) is IS_RUNNING

    def tried(self, candidate):
        """ Return True if the candidate is was ever tried. """
        return candidate in self.history

    def succeeded(self, candidate):
        """ Return True if a request for the candidate succeeded. """
        if not candidate in self.history:
            return False

        return self.history[candidate] is True

    def finished(self, candidate):
        """ Return True if a request for the candidate finished. """
        return (candidate in self.history and
                (not self.history[candidate] is  IS_RUNNING))

    def subblock_failed(self, index, ordinal, count):
        """ Return True if a full or partial request for the sub-block
            failed. """
        assert not self.succeeded(index)
        assert count <= 2
        for flipflop in range(0, count):
            candidate = (index, ordinal, flipflop)
            if self.finished(candidate) and not self.succeeded(candidate):
                return True
        return False

    def block_finished(self, index, count):
        """ Return True if the block finished. """
        if self.block_succeeded(index):
            return True

        # If any subblock isn't finished, the index can't be finished.
        for ordinal in range(0, count):
            if not self.subblock_failed(index, ordinal, count):
                return False
        return True

    def block_succeeded(self, index):
        """ Return True if the block succeeded. """
        return (self.succeeded((index, 0, False)) or
                self.succeeded((index, 1, False)))

    def block_failed(self, index, count):
        """ Return True if the block failed. """
        if self.block_succeeded(index):
            return False
        return self.block_finished(index, count)

# REDFLAG: Really no library func. to do this? RTFM
def choose_word(condition, true_word, false_word):
    """ Return true_word, if condition, false_word otherwise. """
    if condition:
        return true_word
    return false_word

# DESIGN INTENT: Keep BOTH top keys for each block in the network
#                ALWAYS do a top block only request for the other
#                redudant key when making a full request for the
#                other block.
class RequestingRedundantBlocks(RetryingRequestList):
    """ A State to request redundant block CHKs. """
    def __init__(self, parent, name, success_state, failure_state):
        RetryingRequestList.__init__(self, parent, name)
        # block -> (length, (CHK, CHK, ...))
        self.success_state = success_state
        self.failure_state = failure_state
        self.blocks = ()
        self.history = RequestHistory()

    def enter(self, from_state):
        """ State implementation. """
        if not hasattr(from_state, 'get_blocks'):
            raise Exception("Illegal Transition from: %s" % from_state.name)

        # Deep copy.
        self.blocks = []
        for block in from_state.get_blocks():
            self.blocks.append((block[0], tuple(block[1])))
        self.blocks = tuple(self.blocks)
        assert len(self.blocks) > 0
        self.queue_initial_requests()

    def reset(self):
        """ State implementation. """
        RetryingRequestList.reset(self)
        self.blocks = ()
        self.history.reset()

    def queue_initial_requests(self):
        """ INTERNAL: Queue initial candidates. """
        self.current_candidates = []
        self.next_candidates = []
        for block_ordinal, block in enumerate(self.blocks):
            if len(block[1]) == 0:
                continue

            chk_ordinals = range(0, len(block[1]))
            # DESIGN INTENT: Don't favor primary over redundant.
            random.shuffle(chk_ordinals)
            ordinal = chk_ordinals.pop()
            # Randomly enqueue one full request.
            self.current_candidates.append((block_ordinal, ordinal, False))

            # Only handle single redudancy!
            assert len(chk_ordinals) <= 1

            while len(chk_ordinals) > 0:
                ordinal = chk_ordinals.pop()
                # Hmmmm... full requests for data under 32K
                self.current_candidates.append((block_ordinal, ordinal,
                                                block[0] >= FREENET_BLOCK_LEN))
        # DESIGN INTENT: Don't any particular block.
        random.shuffle(self.current_candidates)
    # REDFLAG: avoid pending / history same state in two places?
    def queue_single_full_request(self, candidate):
        """ INTERNAL: Queue a single full request for the block if
            possible. """
        assert candidate[2]

        pending = self.pending_candidates()
        for value in pending:
            assert self.history.is_running(value)

        #print "PENDING: ", pending
        #print "CURRENT: ", self.current_candidates
        #print "NEXT: ", self.next_candidates

        assert not candidate in pending
        full = (candidate[0], candidate[1], False)
        if self.history.is_running(full) or self.history.tried(full):
            self.parent.ctx.ui_.status("Didn't requeue, full request "
                                       + "already %s.\n" %
                                       choose_word(self.history.
                                                   is_running(full),
                                                   'running', 'queued'))
            return

        assert not full in pending

        alternate = (candidate[0], int(not candidate[1]), False)
        if self.history.is_running(alternate):
            self.parent.ctx.ui_.status("Didn't requeue, other salted key "
                                       + "already running.\n")
            assert alternate in pending
            return

        if alternate in self.current_candidates:
            self.parent.ctx.ui_.status("Didn't requeue, other salted key "
                                       + "already queued.\n")
            return

        if full in self.current_candidates:
            self.current_candidates.remove(full)

        assert not full in self.current_candidates
        #print "QUEUED: ", full
        self.current_candidates.insert(0, full) # FIFO

    def _finished(self):
        """ INTERNAL: Return True if finished requesting. """
        for index, block in enumerate(self.blocks):
            if not self.history.block_finished(index, len(block[1])):
                return False
        return True

    def _block_finished(self, candidate):
        """ INTERNAL: Return True if the block is finished. """
        return self.history.block_finished(candidate[0],
                                           len(self.blocks[candidate[0]][1]))

    def queue_alternate(self, candidate):
        """ INTERNAL: Queue an alternate full request if possible.  """
        #print "BLOCKS:", self.blocks
        if len(self.blocks[candidate[0]][1]) < 2:
            return False  # No alternate key. We're toast.

        assert len(self.blocks[candidate[0]][1]) == 2
        if self.history.block_failed(candidate[0], 2):
            return False # Both CHKs already failed. We're toast.

        alternate = (candidate[0], int(not candidate[1]), False)
        alternate_partial = (candidate[0], int(not candidate[1]), True)
        assert not self.history.subblock_failed(alternate[0], alternate[1], 2)

        if (self.history.is_running(alternate) or
            self.history.is_running(alternate_partial)):
            self.parent.ctx.ui_.status("%s failed but %s is already running.\n"
                                       % (str(candidate), choose_word(
                                           self.history.is_running(alternate),
                                           str(alternate),
                                           str(alternate_partial))))
            return True

        if self.history.tried(alternate):
            self.parent.ctx.ui_.status("Already tried running alternate %s.\n"
                                       % str(alternate))
            return False

        if alternate_partial in self.current_candidates:
            self.current_candidates.remove(alternate_partial)
            self.parent.ctx.ui_.status("Removed %s from the queue.\n" %
                                       str(alternate_partial))
            assert not alternate_partial in self.current_candidates

        if alternate in self.current_candidates:
            self.parent.ctx.ui_.status("%s failed but %s already queued.\n" %
                                       (str(candidate), str(alternate)))
            return True

        self.current_candidates.insert(0, alternate) # FIFO
        return True

    def candidate_done(self, client, msg, candidate):
        """ RetryingRequestList implementation. """
        #print "CANDIDATE_DONE: ", msg[0], candidate
        assert not self._finished()
        succeeded = msg[0] in SUCCESS_MSGS
        self.history.finished_request(candidate, succeeded)
        if succeeded:
            # Success
            if (candidate[2] and not self._block_finished(candidate)):
                self.queue_single_full_request(candidate)
            elif not candidate[2]:
                #print "FINISHED: ", candidate
                # Dump the block data into the local cache.
                self.parent.ctx.arch_cache_block(
                    self.blocks[candidate[0]][1][candidate[1]], # CHK
                    client.in_params.file_name,
                    self.blocks[candidate[0]][0]) # length
                if self._finished():
                    self.parent.transition(self.success_state)
            return

        if ((not self._block_finished(candidate)) and
             (not self.queue_alternate(candidate))):
            self.parent.ctx.ui_.status("Download failed:\n" +
                                       '\n'.join(self.blocks[candidate[0]][1]) +
                                       '\n')
            self.parent.transition(self.failure_state)
            return

        if self.is_stalled(): # REDFLAG: I think this is now unreachable???
            self.parent.transition(self.failure_state)

    def make_request(self, candidate):
        """ RetryingRequestList implementation. """
        uri = self.blocks[candidate[0]][1][candidate[1]]
        if candidate[2]:
            # Just top block.
            uri = clear_control_bytes(uri)

        request = CandidateRequest(self.parent)
        request.tag = str(candidate) # Hmmm
        request.candidate = candidate
        request.in_params.fcp_params = self.parent.params.copy()

        request.in_params.definition = GET_DEF
        request.in_params.fcp_params['URI'] = uri
        out_file = os.path.join(
            os.path.join(self.parent.ctx['ARCHIVE_CACHE_DIR'],
                         TMP_DIR), make_id())
        request.in_params.file_name = out_file

        self.parent.ctx.set_cancel_time(request)
        self.history.started_request(candidate)
        return request

# ctx has ui,  but not bundlecache or repo
class ArchiveStateMachine(UpdateStateMachine):
    """ An UpdateStateMachine subclass for creating, pull and pushing
        incremental archives. """
    def __init__(self, runner, ctx):
        UpdateStateMachine.__init__(self, runner, ctx)
        self.states.update(self.new_states())

    # REDFLAG: Fix base class
    def reset(self):
        """ UpdateStateMachin override.

        """
        StateMachine.reset(self)
        if len(self.ctx.orphaned) > 0:
            print "BUG?: Abandoning orphaned requests."
            self.ctx.orphaned.clear()
        self.ctx = ArchiveUpdateContext(self, self.ctx.ui_)

    def new_states(self):
        """ INTERNAL: Create the new states and transitions. """
        return {

            # Requesting
            ARC_REQUESTING_URI:RequestingArchiveUri(self,
                                                    ARC_REQUESTING_URI,
                                                    ARC_CACHING_TOPKEY,
                                                    FAILING),

            ARC_CACHING_TOPKEY:CachingTopKey(self,
                                             ARC_CACHING_TOPKEY,
                                             FINISHING,
                                             ARC_REQUESTING_BLOCKS),

            ARC_REQUESTING_BLOCKS:
            RequestingRedundantBlocks(self,
                                      ARC_REQUESTING_BLOCKS,
                                      FINISHING,
                                      FAILING),


            # Inserting
            ARC_INSERTING_BLOCKS:InsertingRedundantBlocks(self,
                                                          ARC_INSERTING_BLOCKS,
                                                          ARC_FIXING_UP_TOP_KEY,
                                                          FAILING),
            ARC_FIXING_UP_TOP_KEY:FixingUpTopKey(self,
                                                 ARC_FIXING_UP_TOP_KEY,
                                                 ARC_INSERTING_URI),

            ARC_INSERTING_URI:InsertingArchiveUri(self,
                                                  ARC_INSERTING_URI,
                                                  ARC_CACHING_INSERTED_TOPKEY,
                                                  FAILING),

            ARC_CACHING_INSERTED_TOPKEY:
            CachingTopKey(self,
                          ARC_CACHING_INSERTED_TOPKEY,
                          FINISHING,
                          FAILING), # hmmm
            }



def create_dirs(ui_, cache_dir, uri):
    """ Create cache and temp directories for an archive. """
    full_path = os.path.join(cache_dir, get_usk_hash(uri))
    if not os.path.exists(full_path):
        ui_.status("Creating cache dir:\n%s\n" % full_path)
        os.makedirs(full_path)

    tmp_dir = os.path.join(cache_dir, TMP_DIR)
    if not os.path.exists(tmp_dir):
        ui_.status("Creating temp dir:\n%s\n" % tmp_dir)
        os.makedirs(tmp_dir)

def cleanup_dirs(ui_, cache_dir, uri, top_key=None):
    """ Remove unneeded files from the archive cache dir. """

    # Remove temp dir
    tmp_dir = os.path.join(cache_dir, TMP_DIR)
    if os.path.exists(tmp_dir):
        ui_.status("Removing: %s\n" % tmp_dir)
        shutil.rmtree(tmp_dir)

    # Remove block dir
    block_dir = os.path.join(cache_dir, BLOCK_DIR)
    if os.path.exists(block_dir):
        ui_.status("Removing: %s\n" % block_dir)
        shutil.rmtree(block_dir)

    if top_key is None:
        return

    # Remove old cached top keys and unneeded cached CHKs.
    survivors = set([])
    survivors.add(TOP_KEY_NAME_FMT % get_version(uri))
    for block in top_key[0]:
        for chk in block[1]:
            survivors.add(chk_file_name(chk))

    archive_dir = os.path.join(cache_dir, get_usk_hash(uri))
    for name in os.listdir(archive_dir):
        if not (name.startswith(CHK_NAME_PREFIX) or
               name.startswith(TOP_KEY_NAME_PREFIX)):
            # Hmmm leave other files alone. Too paranoid?
            continue

        if not name in survivors:
            full_path = os.path.join(archive_dir, name)
            ui_.status("Removing: %s\n" % full_path)
            os.remove(full_path)
    if len(survivors) > 0:
        ui_.status("Leaving %i file%s in : %s\n" % (
            len(survivors),
            choose_word(len(survivors) == 1, '','s'),
            archive_dir))

# LATER: Add "START_STATE" to context, get rid of
#        most start_* members on UpdateStateMachine
#        and replace them with a generic start(ctx) function.


def check_keys(ctx, required_keys):
    """ Raise a KeyError if all keys in required_keys are not in ctx. """
    # Just let it raise a KeyError
    # Better but causes W0104
    # [ctx[key] for key in required_keys]
    #
    # Grrr... hacking to avoid pylint W0104
    for key in required_keys:
        if not key in ctx and ctx[key]: # Let it raise KeyError
            print "You just executed unreachable code???"

def start(update_sm, ctx):
    """ Start running a context on a state machine. """
    update_sm.require_state(QUIESCENT)
    assert 'START_STATE' in ctx
    update_sm.reset()
    update_sm.set_context(ctx)
    update_sm.transition(ctx['START_STATE'])

def start_requesting_blocks(update_sm, ctx):
    """ Start requesting redundant archive blocks. """
    check_keys(ctx, ('REQUEST_URI', 'ARCHIVE_CACHE_DIR'))
    create_dirs(ctx.ui_,
                ctx['ARCHIVE_CACHE_DIR'],
                ctx['REQUEST_URI'])
    ctx['START_STATE'] = ARC_REQUESTING_URI
    start(update_sm, ctx)

# Doesn't check! Just fails w/ collision
def start_inserting_blocks(update_sm, ctx):
    """ Start inserting redundant archive blocks. """
    check_keys(ctx, ('REQUEST_URI', 'INSERT_URI', 'ARCHIVE_CACHE_DIR',
                     'PROVISIONAL_TOP_KEY', 'ARCHIVE_BLOCK_FILES'))
    create_dirs(ctx.ui_,
                ctx['ARCHIVE_CACHE_DIR'],
                ctx['REQUEST_URI'])
    ctx['START_STATE'] = ARC_INSERTING_BLOCKS
    start(update_sm, ctx)

