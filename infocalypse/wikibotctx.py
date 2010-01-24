""" Class to hold the runtime state of a WikiBot instance.

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

import shelve
import time

from fcpclient import get_version, get_usk_for_usk_version

def pretty_timeout(future_time):
    """ Return a human readable string for a timeout. """
    if future_time is None:
        return 'False'
    diff = future_time - time.time()
    if diff < 0:
        return 'True'
    if diff < 1.0:
        diff = 1 # Don't display 0 for <1 sec remaining.
    return str(int(diff))

def context_to_str(ctx):
    """ Return human readable info about a WikiBotContext in a string."""
    return (("running: %i, queued: %i, " +
             "commit: %s, fnpush: %s, freesite: %s") %
            (len(ctx.store_running_requests['running']),
             len(ctx.store_running_requests['queued']),
             # DCI: clean list comprehension?
             pretty_timeout(ctx.timeouts.get('COMMIT_COALESCE_SECS',
                                              None)),
             pretty_timeout(ctx.timeouts.get('FNPUSH_COALESCE_SECS',
                                              None)),
             pretty_timeout(ctx.timeouts.get('SITE_COALESCE_SECS',
                                              None))
             ))


class WikiBotContext:
    # DCI: not exactly, better doc
    """ Class to hold the runtime state of a WikiBot instance. """
    def __init__(self, parent):
        # shelve storage
        self.parent = parent
        self.store_handled_ids = None
        self.store_running_requests = None
        self.store_applied_requests = None
        self.store_info = None

        self.timeouts = {}

    def set_timeout(self, key):
        """ Set a timeout for key value. """
        self.timeouts[key] = time.time() + self.parent.params[key]

    def clear_timeout(self, key):
        """ Reset the timeout for key. """
        if key in self.timeouts:
            del self.timeouts[key]

    def timed_out(self, key):
        """ Return True if a timeout was set for key and it timed out,
            False otherwise. """
        if not key in self.timeouts:
            return False
        return time.time() >= self.timeouts[key]

    def is_set(self, key):
        """ Return True if there's a timeout set for key, False otherwise. """
        return key in self.timeouts

    def synch_dbs(self):
        """ Force write of databases to disk. """
        if not self.store_handled_ids is None:
            self.store_handled_ids.sync()
        if not self.store_running_requests is None:
            self.store_running_requests.sync()
        if not self.store_applied_requests is None:
            self.store_applied_requests.sync()

    def setup_dbs(self, params):
        """ Initialize the databases used for persistent storage. """
        # Load shelves.
        # Set of handled msg_ids
        assert not self.parent is None
        assert not self.parent.parent is None

        self.store_handled_ids = shelve.open(
            self.parent.parent.get_path(self.parent, 'store_handled_ids'))
        # msg_id -> submission_tuple map
        # 'running' -> list of currently running request msg_ids
        # 'queued' -> FIFO of msg_ids for enqueued requests
        self.store_running_requests = shelve.open(
            self.parent.parent.get_path(self.parent,
                                        'store_running_requests'))
        self.store_applied_requests = shelve.open(
            self.parent.parent.get_path(self.parent,
                                        'store_applied_requests'))
        self.store_info = shelve.open(
            self.parent.parent.get_path(self.parent,
                                        'store_info'))

        self.parent.trace("Opened shelve dbs.")
        if not 'running' in self.store_running_requests:
            self.store_running_requests['running'] = []
        if not 'queued' in self.store_running_requests:
            self.store_running_requests['queued'] = []

        if self.store_info.get('USK_HASH', '') != params['USK_HASH']:
            # Reset if the repos usk changed. hmmmm possible?
            self.store_info['USK_HASH'] =  params['USK_HASH']
            self.store_info['LATEST_INDEX'] = params['LATEST_INDEX']

        # Make sure we have the latest index.
        if params['LATEST_INDEX'] > self.store_info.get('LATEST_INDEX', 0):
            self.store_info['LATEST_INDEX'] = params['LATEST_INDEX']
        self.update_latest_index(params['INSERT_URI'])
        self.update_latest_index(params['REQUEST_URI'])

        del params['LATEST_INDEX'] # DCI: debugging hack!

        running = self.store_running_requests['running']
        queued = self.store_running_requests['queued']
        if len(running) > 0:
            # DCI: Test
            self.parent.debug("Cleaning up crashed requests:\n%s" %
                              '\n'.join(running))
            # Hmmmm... what if a running request caused the crash?
            # Reset after crash.
            self.store_running_requests['queued'] = running + queued
            self.store_running_requests['running'] = []

    def close_dbs(self):
        """ Close the databases used for persistent storage. """
        if not self.store_handled_ids is None:
            self.store_handled_ids.close()
        if not self.store_running_requests is None:
            self.store_running_requests.close()
        if not self.store_applied_requests is None:
            self.store_applied_requests.close()
        if not self.store_info is None:
            self.store_info.close()

    def queue_submission(self, msg_id, submission):
        """ Add a submission to the submission FIFO. """
        assert not msg_id in self.store_running_requests
        queued = self.store_running_requests['queued']
        assert not msg_id in queued
        assert not msg_id in self.store_running_requests['running']

        self.store_running_requests[msg_id] = submission
        queued.append(msg_id)
        self.store_running_requests['queued'] = queued

    # can return None
    def pop_msg_id(self):
        """ Remove the oldest submission from the FIFO and return it. """
        queued = self.store_running_requests['queued']
        if len(queued) == 0:
            return None

        msg_id = queued.pop(0)
        self.store_running_requests['queued'] = queued # force sync.

        #self.trace("next_runnable -- popped: %s" % msg_id)
        running = self.store_running_requests['running']
        assert not msg_id in running
        return msg_id

    def mark_running(self, msg_id):
        """ Persistently mark the submission as running. """
        running = self.store_running_requests['running']
        assert not msg_id in running
        running.append(msg_id)
        self.store_running_requests['running'] = running # force sync.

    def mark_finished(self, msg_id):
        """ Persistently mark the submission as not running. """
        running = self.store_running_requests['running']
        assert msg_id in running
        running.remove(msg_id)
        self.store_running_requests['running'] = running # force sync.

    def update_latest_index(self, uri):
        """ Update the latest known version of the stored repo usk. """
        if uri is None:
            return
        version = get_version(uri)
        if version > self.store_info['LATEST_INDEX']:
            self.store_info['LATEST_INDEX'] = version

    def remove_submission(self, msg_id):
        """ Remove stored the stored information for the submission. """
        ret = self.store_running_requests[msg_id]
        del self.store_running_requests[msg_id]
        return ret

    def request_uri(self):
        """ Return the repository request URI. """
        return get_usk_for_usk_version(self.parent.params['REQUEST_URI'],
                                       self.store_info['LATEST_INDEX'])

    def insert_uri(self):
        """ Return the repository insert URI. """
        return get_usk_for_usk_version(self.parent.params['INSERT_URI'],
                                       self.store_info['LATEST_INDEX'])

    def should_notify(self):
        """ Return True if an FMS repo update message should be posted,
            False otherwise. """
        return (self.timed_out('NOTIFY_COALESCE_SECS') and
                not self.is_set('FNPUSH_COALESCE_SECS'))

    def committed(self, success=False):
        """ Handle commit to the local repository. """
        self.clear_timeout('COMMIT_COALESCE_SECS')
        self.set_timeout('FNPUSH_COALESCE_SECS')
        if success:
            # Update freesite on success.
            self.set_timeout('SITE_COALESCE_SECS')

    def pushed(self):
        """ Handle push of local repo into Freenet. """
        self.clear_timeout('FNPUSH_COALESCE_SECS')
        self.set_timeout('NOTIFY_COALESCE_SECS')

    def should_insert_site(self):
        """ Return True if the freesite needs to be inserted. """
        return (self.timed_out('SITE_COALESCE_SECS') and
                not self.is_set('FNPUSH_COALESCE_SECS'))

    # DCI: correct?
    def has_submissions(self):
        """ Return True if there are subissions which are running or need
            to be run, False otherwise. """
        return (len(self.store_running_requests['running']) > 0 or
                len(self.store_running_requests['queued']) > 0)

# REDFLAG: revisit during code cleanup
# pylint error about too many public methods. grrrr...
#     def already_applied(self, submission):
#         """ Return True if the submissions CHK has already been applied,
#             False otherwise.

#             SIDE EFFECT: Adds submission's CHK to the applied list.
#         """
#         chk = submission[3]
#         if chk in self.store_applied_requests:
#             return True
#         self.store_applied_requests[chk] = '' # Dummy value.
