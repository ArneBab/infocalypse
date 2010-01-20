""" An FMSBot to run a wiki over freenet.

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

# DCI: fix debugging param values. e.g.: short timeouts that cause 10, 25 errors
import os
import shutil
import time

from mercurial import ui, hg, commands

from fcpmessage import GET_DEF, PUT_COMPLEX_DIR_DEF
from fcpclient import parse_progress, get_file_infos, \
     set_index_file, dir_data_source

from requestqueue import QueueableRequest, RequestQueue

from validate import is_hex_string
from chk import ENCODED_CHK_SIZE
from fms import TrustCache, to_msg_string
from fmsbot import FMSBot
from submission import ForkingSubmissionHandler, REJECT_NOTRUST, REJECT_FCPFAIL

from bundlecache import BundleCache, is_writable, make_temp_file
from updatesm import UpdateContext, UpdateStateMachine, QUIESCENT, FINISHING
from infcmds import UICallbacks, set_debug_vars

# freesite insert stuff
from statemachine import StatefulRequest
from sitecmds import dump_wiki_html

from wikibotctx import WikiBotContext, context_to_str

from pathhacks import add_parallel_sys_path
add_parallel_sys_path('fniki')

HTML_DUMP_DIR = '__html_dump_deletable__'

# Parameters used by WikiBot.
REQUIRED_PARAMS = frozenset([
    'FCP_HOST', 'FCP_PORT', 'FCP_POLL_SECS', 'N_CONCURRENT',
    'CANCEL_TIME_SECS',
    'FMS_HOST', 'FMS_PORT', 'FMS_POLL_SECS',
    'BOT_STORAGE_DIR', 'LATEST_INDEX', 'SITE_KEY', 'SITE_NAME',
    'SITE_DEFAULT_FILE', 'INSERT_URI', 'REQUEST_URI','VERBOSITY',
    'TMP_DIR', 'NO_SEARCH', 'USK_HASH', 'FNPUSH_COALESCE_SECS',
    'SITE_COALESCE_SECS', 'NOTIFY_COALESCE_SECS', 'COMMIT_COALESCE_SECS',
    'FMS_GROUP', 'FMS_ID', 'FMS_TRUST_CACHE_SECS', 'FMS_MIN_TRUST',
    'NONE_TRUST',
    'REPO_DIR', 'WIKI_ROOT',])

# LATER: Aggregated report message to the list?
# DCI: Think through dos attacks
# DCI: Keep track of patches that have already been applied?
#      implicit in hg log, explicit in success.txt ?
# wiki submission tuple is:
# (usk_hash, base_version, chk, length)
class SubmissionRequest(QueueableRequest):
    """ A QueueableRequest subclass to read submission CHK zips for
        wiki submissions. """
    def __init__(self, queue, msg_id):
        QueueableRequest.__init__(self, queue)
        self.msg_id = msg_id

def parse_submission(fms_id, lines, usk_hash):
    """ Parse a single submission from raw fms message lines
        and returns a submission tuple of the form:

        (fms_id, usk_hash, base_version, chk, length)

        Returns None if no submission could be parsed.
    """
    print "LINES:"
    print lines
    for line in lines:
        if not line.startswith('W:'):
            continue
        # 0 1           2               3    4
        # W:<repo_hash>:<base_version>:<chk><length>
        fields = line.strip().split(':')
        if not is_hex_string(fields[1]) or not is_hex_string(fields[2]):

            continue
        if fields[1] != usk_hash:
            continue
        if (not fields[3].startswith('CHK@') or
            len(fields[3]) != ENCODED_CHK_SIZE):
            continue
        try:
            length = int(fields[4])
        except ValueError:
            continue
        # (fms_id, usk_hash, base_version, chk, length)
        return (fms_id, fields[1], fields[2], fields[3], length)
    return None

class WikiBot(FMSBot, RequestQueue):
    """ An FMSBot implementation to run a wiki over freenet. """
    def __init__(self, name, params, request_runner):
        FMSBot.__init__(self, name)
        RequestQueue.__init__(self, request_runner)

        self.ctx = None
        self.applier = None
        self.params = params.copy()
        self.ui_ = None
        self.repo = None

        self.trust = None
        self.update_sm = None
        # Why doesn't the base class ctr do this?
        request_runner.add_queue(self)

    def trace(self, msg):
        """ Write a log message at trace level. """
        self.log("T:" + msg)

    def debug(self, msg):
        """ Write a log message at debug level. """
        self.log("D:" + msg)

    def warn(self, msg):
        """ Write a log message at warn level. """
        self.log("W:" + msg)

    #----------------------------------------------------------#
    # FMSBot implementation.
    def on_startup(self):
        """ Set up the bot instance. """
        self.trace("on_startup")
        # Fail early and predictably.
        for required in REQUIRED_PARAMS:
            if not required in self.params:
                raise KeyError(required)

        # NOT ATOMIC
        # REDFLAG: LATER: RTFM python ATOMIC file locking.
        if os.path.exists(self.parent.get_path(self, 'pid')):
            self.warn("on_startup -- lock file exists!: %s" %
                      self.parent.get_path(self, 'pid'))
            raise IOError("Already running or previous instance crashed.")
        pid_file = open(self.parent.get_path(self, 'pid'), 'wb')
        try:
            pid_file.write("%i\n" % os.getpid())
            self.trace("on_startup -- pid[%i], created lock file: %s"
                       % (os.getpid(), self.parent.get_path(self, 'pid')))
        finally:
            pid_file.close()

        self.ctx = WikiBotContext(self)
        self.ctx.setup_dbs(self.params)

        # Can't push this up into the FMSBotRunner because it
        # requires a particular fms_id.
        # DCI: how does this get reconnected? when server drops
        self.trust = TrustCache(self.parent.nntp_server,
                                self.params['FMS_TRUST_CACHE_SECS'])
        # Mercurial stuff.
        self.ui_ = WikiBotUI(None, self)
        self.repo = hg.repository(self.ui_, self.params['REPO_DIR'])
        self.trace("Loaded hg repo from: %s" % self.params['REPO_DIR'])

        self.applier = ForkingSubmissionHandler()
        self.applier.ui_ = self.ui_
        self.applier.repo = self.repo
        self.applier.logger = self
        self.applier.base_dir = os.path.join(self.repo.root,
                                             self.params['WIKI_ROOT'])

        print "BASE_DIR:", self.applier.base_dir
        
        # 2qt?
        self.applier.notify_needs_commit = (
            lambda: self.ctx.set_timeout('COMMIT_COALESCE_SECS'))
        self.applier.notify_committed = self.ctx.committed
        self._send_status_notification('STARTED')

    def on_shutdown(self, why):
        """ Shut down the bot instance. """
        self.trace("on_shutdown -- %s" % why)
        self.ctx.close_dbs()
        self._cleanup_temp_files()
        if os.path.exists(self.parent.get_path(self, 'pid')):
            try:
                os.remove(self.parent.get_path(self, 'pid'))
                self.trace("on_shutdown -- removed lock file: %s"
                           % self.parent.get_path(self, 'pid'))

            except IOError, err:
                self.warn("on_shutdown -- err: %s" % str(err))

        self._send_status_notification('STOPPED')

    def on_fms_change(self, connected):
        """ FMSBot implementation. """
        self.trust.server = self.parent.nntp_server
        if not connected:
            self.debug("The fms server disconnected.")
            self.warn("REQUESTING BOT SHUTDOWN!")
            self.exit = True

    # Thought about putting the repo hash in the subject but I want to
    # keep the subject human readable.
    def wants_msg(self, group, items):
        """ Return True for messages to the target groups that haven't
            been handled."""

        # Ignore our own repo update notifications.
        if not items[1].strip().startswith('Submit:'):
            self.trace("wants_msg -- ignored: %s" % items[1])
            return False

        msg_id = items[4]
        # Hmmm...better to provide ctx.wants(msg_id) accessor?
        if ((not group in self.groups) or
            (msg_id in self.ctx.store_handled_ids)) :
            #self.trace("wants_msg -- skipped: %s" % msg_id)
            return False
        self.trace("wants_msg -- accepted: %s" % msg_id)
        return True

    def on_idle(self):
        """ FMSBot implementation.

            This handles pushing updates of the wikitext repo into Freenet and
            re-inserting the wiki freesite as necessary.
        """
        self.trace(context_to_str(self.ctx))
        self.ctx.synch_dbs()

        if self.ctx.should_notify():
            self._send_update_notification()

        if not self.update_sm is None:
            return

        # DCI: Is this working as expected?
        if self.ctx.has_submissions():
            return

        if self.ctx.timed_out('COMMIT_COALESCE_SECS'):
            self.trace("Commit local changes after failure.")
            self.applier.force_commit()
            self.ctx.committed() # DCI: Required?
            # Only update freesite on success.

        if self.ctx.timed_out('FNPUSH_COALESCE_SECS'):
            self.trace("Starting push into freenet.")
            self._start_fnpush()
            return

        if self.ctx.should_insert_site():
            self.trace("Starting freesite insertion.")
            self._start_freesite_insert()

    # Handle a single message
    def recv_fms_msg(self, dummy_group, items, lines):
        """ FMSBot implementation to handle incoming FMS messages. """
        msg_id = items[4]
        self.trace("recv_fms_msg -- called: %s" % msg_id)
        # Hmmm... accessor? ctx.mark_recvd() or put in ctx.wants() ???
        self.ctx.store_handled_ids[msg_id] = "" # (ab)use as hashset

        sender_fms_id = items[2]
        submission = parse_submission(sender_fms_id, lines,
                                      self.params['USK_HASH'])
        if submission is None:
            self.trace("recv_fms_msg -- couldn't parse submission: %s" % msg_id)
            return

        if not self._has_enough_trust(msg_id, submission,
                                      self.params['NONE_TRUST']):
            self.trace("recv_fms_msg -- not enough trust: %s" % msg_id)
            return

        self.trace("recv_fms_msg -- parsed: %s" % str(submission))

        self.ctx.queue_submission(msg_id, submission)
        # Will get picked up by next_runnable.

    #----------------------------------------------------------#
    def _cleanup_temp_files(self):
        """ Helper to clean up temp files. """
        site_root = os.path.join(self.params['TMP_DIR'], HTML_DUMP_DIR)
        if os.path.exists(site_root):
            if not site_root.find("deletable"):
                raise ValueError("staging dir name must contain 'deletable'")
            shutil.rmtree(site_root)
            assert not os.path.exists(site_root)

        # Order is import. remove_files() errors if there are dirs.
        if (not self.update_sm is None and
            not self.update_sm.ctx.bundle_cache is None):
            self.update_sm.cancel()
            self.update_sm.ctx.bundle_cache.remove_files()

    def _has_enough_trust(self, msg_id, submission, none_trust=0):
        """ INTERNAL: Returns True if the sender is trusted enough
            to commit to the wiki.

            Writes a REJECT_NOTRUST record into rejected.txt when
            it returns False.
        """
        assert self.trust.server # DCI: fix!
        trust = self.trust.get_trust(submission[0])
        self.trace("has_enough_trust -- %s" % str(trust))

        trust_value = trust[2]
        if trust_value is None:
            self.trace("has_enough_trust -- used %i for 'None'" %
                       none_trust)
        if trust_value < self.params['FMS_MIN_TRUST']:
            # Use %s for trust because it can be 'None'
            self.debug("has_enough_trust -- Failed: %s < %s" % (
                str(trust_value), str(self.params['FMS_MIN_TRUST'])))
            self.applier.update_change_log(msg_id, submission,
                                           REJECT_NOTRUST, False)
            return False
        return True

    def _start_fnpush(self):
        """ INTERNAL: Starts asynchronous push of local repository into
            Freenet. """
        # Intialize update_sm
        # start it
        assert self.update_sm is None
        self.update_sm = setup_sm(self.ui_, self.repo, self.runner, self.params)
        # LATER: Replace UICallbacks and back out dorky chaining?
        self.update_sm.transition_callback = (
            ChainedCallback.chain((self.update_sm.transition_callback,
                                   self._fnpush_transition)))
        self.update_sm.start_pushing(self.ctx.insert_uri())

    def _start_freesite_insert(self):
        """ INTERNAL: Start asynchronous insert of Wiki freesite. """
        assert self.update_sm is None
        self.debug("start_freesite_insert -- starting insert of edition: %i" %
                   (latest_site_index(self.repo) + 1))

        self.update_sm = setup_sm(self.ui_, self.repo, self.runner, self.params)
        # LATER: Replace UICallbacks and back out dorky chaining?
        self.update_sm.transition_callback = (
            ChainedCallback.chain((self.update_sm.transition_callback,
                                   self._freesite_transition)))

        # DCI: try block, with file cleanup
        # DCI: need to check that there are no uncommited files!
        site_root = os.path.join(self.params['TMP_DIR'], HTML_DUMP_DIR)
        dump_wiki_html(os.path.join(self.repo.root, self.params['WIKI_ROOT']),
                       site_root, False)

        infos = get_file_infos(site_root)
        set_index_file(infos, self.params['SITE_DEFAULT_FILE'])
        self.debug('start_freesite_insert -- dumped %i files' % len(infos))
        self.trace('--- files ---')
        for info in infos:
            self.trace('%s %s' % (info[0], info[1]))
        self.trace('---')

        request = StatefulRequest(self.update_sm)
        request.tag = 'freesite_insert'
        request.in_params.definition = PUT_COMPLEX_DIR_DEF
        request.in_params.fcp_params = self.params.copy()
        request.in_params.fcp_params['DontCompress'] = False
        request.in_params.fcp_params['URI'] = self._freesite_insert_uri()

        # dir_data_source() creates an IDataSource which allows
        # the FCPConnection to slurp the files up over the
        # FCP socket as one contiguous blob.

        # Sets up in_params for ClientPutComplexDir as a side effect.
        request.custom_data_source = (
            dir_data_source(infos, request.in_params, 'text/html'))

        request.cancel_time_secs = (time.time() +
                                    self.params['CANCEL_TIME_SECS'])
        self.update_sm.start_single_request(request)

    def _freesite_insert_uri(self):
        """ Return the insert URI for the freesite. """
        return '%s/%s-%i/' % (self.params['SITE_KEY'],
                              self.params['SITE_NAME'],
                              latest_site_index(self.repo) + 1)

    def _fnpush_transition(self, old_state, new_state):
        """ INTERNAL: Handle UpdateStateMachine state changes while pushing
            the local repo into Freenet. """
        self.trace("fnpush_transition -- [%s]->[%s]" %
                   (old_state.name, new_state.name))
        if new_state.name != QUIESCENT:
            return

        if old_state.name == FINISHING:
            # Success
            self.ctx.pushed()
            self.debug("fnpush_transition -- fn-push finished.")
            prev_value = self.ctx.store_info['LATEST_INDEX']
            self.ctx.update_latest_index(self.update_sm.ctx['INSERT_URI'])
            if self.ctx.store_info['LATEST_INDEX'] > prev_value:
                self.trace("fnpush_transition -- incremented index to: %i " %
                           self.ctx.store_info['LATEST_INDEX'])
        else:
            # Failure
            self.debug("fnpush_transition -- fn-push failed.")
            # djk20091219 weird ClientPut collision failure
            #             fails with 9, BUT subsequent get doesn't
            #             get the updated version.
            #DCI: recoverable vs. non-recoverable errors. THINK!
            # For now, all infocalypse errors are fatal.
            # We can get robustness by other means. e.g. cron.
            self.debug("REQUESTING BOT SHUTDOWN!")
            self.exit = True

        # Cleanup
        self._cleanup_temp_files()
        self.update_sm = None

    def _freesite_transition(self, old_state, new_state):
        """ INTERNAL: Handle UpdateStateMachine state changes while inserting
            the freesite. """

        self.trace("freesite_transition -- [%s]->[%s]" %
                   (old_state.name, new_state.name))
        if new_state.name != QUIESCENT:
            return

        if old_state.name == FINISHING:
            # Success
            self.ctx.clear_timeout('SITE_COALESCE_SECS')
            self.debug("freesite_transition -- freesite insertion finished.")
            tag_site_index(self.ui_, self.repo)
        else:
            # Failure
            self.debug("freesite_transition -- freesite insertion FAILED.")
            self.debug("REQUESTING BOT SHUTDOWN!")
            self.exit = True

        # Cleanup
        self._cleanup_temp_files()
        self.update_sm = None

    #----------------------------------------------------------#
    # RequestQueue implementation.
    def next_runnable(self):
        """ RequestQueue implementation. """
        if not self.update_sm is None:
            return None # Don't run CHK request while fn-pushing repo.

        msg_id = self.ctx.pop_msg_id()
        if msg_id is None:
            return None

        self.trace("next_runnable -- popped: %s" % msg_id)

        chk = self.ctx.store_running_requests[msg_id][3] # hmmm why not 0 or 1?

        self.trace("next_runnable -- chk: %s" % chk)
        request = SubmissionRequest(self, msg_id)
        request.in_params.definition = GET_DEF
        request.in_params.fcp_params = self.params.copy()
        request.in_params.fcp_params['URI'] = chk
        request.in_params.fcp_params['MaxSize'] = 32 * 1024

        request.cancel_time_secs = (time.time() +
                                    self.params['CANCEL_TIME_SECS'])
        # DCI: Retrying ?
        self.ctx.mark_running(msg_id)

        return request

    def request_progress(self, dummy_client, msg):
        """ RequestQueue implementation dumps progress to log."""
        if msg[0] != 'SimpleProgress':
            self.debug(msg[0])
            return

        self.debug(str(parse_progress(msg)))

    def request_done(self, client, msg):
        """ RequestQueue implementation. """
        msg_id = client.msg_id
        self.debug("request_done -- : %s" % msg_id)
        self.ctx.mark_finished(msg_id)

        # DCI: Retrying ???
        submission_tuple = self.ctx.remove_submission(msg_id)

        if msg[0] == 'AllData': # Success
            self._handle_submission(msg_id, submission_tuple, msg)
        else:
            self._handle_fcp_failure(msg_id, submission_tuple, msg)


    def _handle_submission(self, msg_id, submission_tuple, msg):
        """ INTERNAL: Handle incoming submission bundles."""
        self.debug("handle_submission --  %s" % msg_id)
        self.trace("handle_submission --  %s" % str(submission_tuple))
        tmp_file = make_temp_file(self.params['TMP_DIR'])
        try:
            self.applier.apply_submission(msg_id, submission_tuple,
                                          msg[2], tmp_file)
        finally:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

    def _handle_fcp_failure(self, msg_id, submission_tuple, msg):
        """ INTERNAL: Handle FCP request failure when requesting CHK
            for submission .zip."""
        code = -1
        if 'Code' in msg[1]:
            try:
                code = int(msg[1]['Code'])
            except ValueError:
                code = -1 # Silence W0704
        self.debug("handle_fcp_failure --  %s[%i]" % (msg[0], code))
        self.trace("handle_fcp_failure --  msg:\n%s" % str(msg))
        # DCI: Handle PutFailed, code 9
        self.applier.update_change_log(msg_id, submission_tuple,
                                       REJECT_FCPFAIL, False)

    def _send_status_notification(self, short_msg, long_msg=None):
        """ Post a status message to FMS. """
        if long_msg is None:
            long_msg = "EOM"

        if not long_msg.endswith('\n'):
            long_msg += '\n'

        self.parent.queue_msg((self.params['FMS_ID'],
                               self.params['FMS_GROUP'],
                               'wikibot[%s]:%s' % (self.params['USK_HASH'],
                                                   short_msg),
                               long_msg))

    def _send_update_notification(self):
        """ INTERNAL: Send an FMS notification for the latest repo index. """
        self.trace("send_update_notification -- repo index: %i" %
                   self.ctx.store_info['LATEST_INDEX'])

        subject = ('Wikitext Update:' +
                   '/'.join(self.ctx.request_uri().split('/')[1:]))
        text = to_msg_string(((self.params['USK_HASH'],
                              self.ctx.store_info['LATEST_INDEX']), )) + '\n'
        groups = self.params['FMS_GROUP']
        if self.params.get('FMS_NOTIFY_GROUP', ''):
            groups = "%s, %s" % (groups, self.params['FMS_NOTIFY_GROUP'])
            self.trace("send_update_notification -- groups: %s" % groups)

        self.parent.queue_msg((self.params['FMS_ID'],
                               groups,
                               subject,
                               text))
        # DCI: better to use send confirm callback?
        self.ctx.clear_timeout('NOTIFY_COALESCE_SECS')

def latest_site_index(repo):
    """ Read the latest known freesite index out of the hg changelog. """
    for tag, dummy in reversed(repo.tagslist()):
        if tag.startswith('I_'):
            return int(tag.split('_')[1])
    return -1

def tag_site_index(ui_, repo, index=None):
    """ Tag the local repository with a freesite index. """
    if index is None:
        index = latest_site_index(repo) + 1 # hmmmm... lazy vs. explicit.
    commands.tag(ui_, repo, 'I_%i' % index)

def scrub_eol(text):
    """ Return text w/o last trailing '\\n'. """
    if text.endswith('\n'):
        text = text[:-1]
    return text

# Tested w/ Mercurial 1.3.1 on x86 Linux. Works.
class WikiBotUI(ui.ui):
    """ A Mercurial ui subclass which routes all output through
        the WikiBot logging functions. """

    def __init__(self, src=None, wikibot=None):
        ui.ui.__init__(self, src)
        # Hmmm... I just copied pattern of base class __init__.
        # Why doesn't copy() copy wikibot member?
        if src:
            self.wikibot = src.wikibot
        elif not wikibot is None:
            self.wikibot = wikibot
        assert not self.wikibot is None

    def write(self, *args):
        """ ui override which writes into the WikiBot log. """
        for arg in args:
            self.wikibot.trace(scrub_eol(str(arg)))

    def write_err(self, *args):
        """ ui override which writes into the WikiBot log. """
        for arg in args:
            self.wikibot.warn(scrub_eol(str(arg)))

    def flush(self):
        """ ui override which is a NOP."""
        pass

    # Have no choice, must implement hg's ui interface.
    #pylint: disable-msg=R0201
    def interactive(self):
        """ ui override which returns False """
        return False

    # DCI: remove?
    # This does get called.
    def copy(self):
        """ ui override."""
        assert hasattr(self, 'wikibot')
        ret = self.__class__(self)
        assert hasattr(ret, 'wikibot')
        return ret
# 2qt?
class ChainedCallback:
    """ Helper class to chain UpdateStateMachine transition callbacks. """
    def __init__(self, callbacks):
        self.callbacks = callbacks

    def chained_dispatch(self, old_state, new_state):
        """ A transition callback implementation which runs the chained
            callbacks sequentially. """
        for callback in self.callbacks:
            callback(old_state, new_state)

    @classmethod
    def chain(cls, callbacks):
        """ Returns a transition callback implementation which chains
            the callbacks in the callbacks sequence. """
        return ChainedCallback(callbacks).chained_dispatch

def setup_sm(ui_, repo, runner, params):
    """ INTERNAL: Helper function which sets up an UpdateStateMachine
        instance. """
    assert is_writable(os.path.expanduser(params['TMP_DIR']))

    verbosity = params.get('VERBOSITY', 1)
    set_debug_vars(verbosity, params)

    callbacks = UICallbacks(ui_)
    callbacks.verbosity = verbosity
    # DCI: bundle cache needed for inserting?
    cache = BundleCache(repo, ui_, params['TMP_DIR'])

    # For Infocalypse repositories
    ctx = UpdateContext(None)
    ctx.repo = repo
    ctx.ui_ = ui_
    ctx.bundle_cache = cache
    update_sm = UpdateStateMachine(runner, ctx)

    update_sm.params = params.copy()
    update_sm.transition_callback = callbacks.transition_callback
    update_sm.monitor_callback = callbacks.monitor_callback

    # Modify only after copy.
    update_sm.params['FREENET_BUILD'] = runner.connection.node_hello[1]['Build']

    return update_sm

