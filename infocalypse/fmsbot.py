""" Classes to run bots over FMS.

    Please use for good and not evil.

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
import socket
import time

import fms
from fms import IFmsMessageSink

class FMSBotRunner(IFmsMessageSink):
    """ Container class which owns and runs one or more FMSBots. """
    def __init__(self, params):
        IFmsMessageSink.__init__(self)
        self.bots = []
        self.msg_targets = []
        self.groups = set([])
        self.max_articles = {}
        self.outgoing_msgs = []
        self.nntp = fms # So it can be swapped out for testing.
        self.nntp_server = None
        self.params = params.copy()
        assert self.params.get('FMS_HOST')
        assert self.params.get('FMS_PORT')
        assert self.params.get('BOT_STORAGE_DIR')

        if not (os.path.exists(params['BOT_STORAGE_DIR']) and
                os.path.isdir(params['BOT_STORAGE_DIR'])):
            raise ValueError(("Storage dir doesn't exist: %s") %
                             self.params['BOT_STORAGE_DIR'])

    def log(self, msg):
        """ Print a log message. """
        print msg

    def nntp_reconnect(self, suppress_events=False):
        """ Connect to fms. """
        if not self.nntp_server is None:
            return self.nntp_server

        try:
            fms_id = self.params.get('FMS_ID', None)
            self.nntp_server = self.nntp.get_connection(self.params['FMS_HOST'],
                                                        self.params['FMS_PORT'],
                                                        fms_id)
        except Exception, err: # DCI: what else do I need to catch?
            self.log("FMSBotRunner.nntp_reconnect -- failed: %s" % str(err))
            return None

        if not suppress_events:
            for bot in self.bots:
                bot.on_fms_change(True)

        return self.nntp_server

    def nntp_close(self):
        """ Disconnect from fms. """
        if self.nntp_server is None:
            return
        try:
            try:
                self.nntp_server.quit()
            except IOError, err:
                self.log("FMSBotRunner.nntp_close -- failed: %s" % str(err))
            except EOFError, err:
                self.log("FMSBotRunner.nntp_close -- failed: %s" % str(err))
        finally:
            self.nntp_server = None

        for bot in self.bots:
            bot.on_fms_change(False)

    def nntp_send(self):
        """ Send pending fms messages. """
        if not self.outgoing_msgs:
            return False
        if self.nntp_server is None:
            self.log("FMSBotRunner.nntp_send -- nntp_send not connected!")
            return False
        try:
            raised = True
            try:
                self.nntp.send_msgs(self.nntp_server,
                                    self.outgoing_msgs)
                # i.e. Don't clear if there was an exception.
                self.outgoing_msgs = []
                raised = False
            finally:
                if raised:
                    self.nntp_close()
        except Exception, err: # DCI: what else do I need to catch?
            # ??? fail silently???
            self.log("FMSBotRunner.nntp_send -- send_msgs failed: %s" %
                     str(err))
            return False
        return True

    def wants_msg(self, group, items):
        """ IFmsMessageSink implementation. """
        # REDFLAG: unwind recv_msgs instead of dorky hack?
        self.msg_targets = [bot for bot in self.bots
                            if not bot.exit and bot.wants_msg(group, items)]
        return len(self.msg_targets) > 0

    def recv_fms_msg(self, group, items, lines):
        """ IFmsMessageSink implementation. """
        for bot in self.msg_targets:
            assert not bot.exit
            bot.recv_fms_msg(group, items, lines)

    # REDFLAG: exceptions
    def startup(self):
        """ Run on_startup() handler on all bots. """
        self.nntp_reconnect(True) # Suppress events. Bots not started.
        for bot in self.bots:
            bot.on_startup()

    def shutdown(self, why):
        """ Run on_startup() handler on all bots which haven't exited. """
        for bot in self.bots:
            bot.on_shutdown(why)

        # Allow bots to send messages on shutdown.
        self.nntp_send()

    def idle(self):
        """ Run on_idle() handler on all bots which haven't exited """

        self.nntp_reconnect()
        self.nntp_send()

        for bot in self.bots[:]:
            if bot.exit:
                bot.on_shutdown('Set exit=True')
                self.bots.remove(bot)
                continue
            bot.on_idle()

    def is_running(self):
        """ Returns True if the runner has at least one bot that
            hasn't exited, False otherwise. """
        for bot in self.bots:
            if not bot.exit:
                return True
        return False

    def get_path(self, bot, fname):
        """ Get a bot specific path. """
        assert fname.find(os.path.sep) == -1
        return os.path.join(self.params['BOT_STORAGE_DIR'],
                            "%s_%s" %(bot.name, fname))

    def queue_msg(self, msg_tuple):
        """ Queue an outgoing message.

            You can set a callback in the msg_tuple which is
            called when the message is actually sent.
            See fms.send_msgs().
        """
        self.outgoing_msgs.append(msg_tuple)

    def recv_msgs(self):
        """ Poll for new fms messages and dispatch them to registed bots. """
        if not self.nntp_server:
            self.log("FMSBotRunner.recv_msgs -- not connected")
            return False

        try:
            raised = True
            try:
                self.nntp.recv_msgs(self.nntp_server,
                                    self, self.groups, self.max_articles)
                raised = False
            finally:
                if raised:
                    self.nntp_close()
        except Exception, err: # DCI: what else do I need to catch?
            self.log("FMSBotRunner.recv_msgs -- failed: %s" % str(err))
            raise # DCI: NEED TO FIX THIS
            return False

        return True

    def register_bot(self, bot, groups):
        """ Add a bot to the FMSBotRunner.

            Adds groups to bot.groups as a side effect.

            REQUIRES: No other bot already registered with bot.name.
        """
        assert bot.name and len(bot.name.strip())
        assert not groups is None # Empty is ok, None is not
        assert bot not in self.bots

        groups = set(groups)

        bot.parent = self
        bot.groups.update(groups)

        self.groups.update(groups)
        self.bots.append(bot)

class FMSBot(IFmsMessageSink):
    """ Abstract base class for bots which run over FMS. """

    def __init__(self, name):
        IFmsMessageSink.__init__(self)
        self.parent = None
        self.name = name # UNIQUE, PERSISTENT NAME
        self.groups = set([])
        self.exit = False

    def log(self, text):
        """ Display log messages. """
        print "%s:%s" % (self.name, text)

    def on_startup(self):
        """ Event handler which is run once when the bot is started. """
        # setup shelves db
        pass

    def on_shutdown(self, why):
        """ Event handler which is run once when the bot is shutdown. """
        # tear down shelves db
        pass

    # Hook to kick a state machine.
    def on_idle(self):
        """ Event handler called intermittenly when the bot is idle. """
        pass

    # Filter messages
    def wants_msg(self, group, dummy_items):
        """ Return True if the bot should handle the message,
            False otherwise.
        """
        return group in self.groups

    # Handle a single message
    def recv_fms_msg(self, group, items, lines):
        """ Handle a single message. """
        pass

    # DCI: Too hacky?
    def on_fms_change(self, dummy_connected):
        """ Called when the fms server drops or reconnects """
        pass

def run_bots(bot_runner, poll_time, sleep_func=time.sleep):
    """ Run the bot_runner until all it's bots exit. """
    bot_runner.startup()
    reason = "Unknown exception" # REDFLAG: Do better!
    try:
        while bot_runner.is_running():
            bot_runner.recv_msgs()
            if not bot_runner.is_running():
                break # Shutdown while recv'ing
            bot_runner.idle()
            if not bot_runner.is_running():
                break # Shutdown while idle()
            sleep_func(poll_time)
        reason = "Clean exit"
    finally:
        bot_runner.shutdown(reason)

# Hmmm not wikibot specific
def run_event_loops(bot_runner, request_runner,
                    bot_poll_secs = 5 * 60,
                    fcp_poll_secs = 0.25,
                    out_func = lambda msg:None):
    """ Graft the event loops for the FMSBotRunner and RequestQueue together."""
    assert bot_poll_secs > fcp_poll_secs
    connection = request_runner.connection
    assert not connection is None
    shutdown_msg = "unknown error"
    try:
        bot_runner.recv_msgs()
        timeout = time.time() + bot_poll_secs
        while True:
            # Run the FCP event loop (frequent)
            try:
                if not connection.socket.poll():
                    out_func("Exiting because FCP poll exited.\n")
                    break
                # Nudge the state machine.
                request_runner.kick()
            except socket.error: # Not an IOError until 2.6.
                out_func("Exiting because of an error on the FCP socket.\n")
                raise
            except IOError:
                out_func("Exiting because of an IO error.\n")
                raise

            if time.time() < timeout:
                # Rest a little. :-)
                time.sleep(fcp_poll_secs)
                continue

            # Run FMSBotRunner event loop (infrequent)
            bot_runner.recv_msgs()
            if not bot_runner.is_running():
                out_func("Exiting because the FMS bot runner exited.\n")
                break # Shutdown while recv'ing
            bot_runner.idle()
            if not bot_runner.is_running():
                out_func("Exiting because the FMS bot runner " +
                         "exited while idle.\n")
                break # Shutdown while idle()

            timeout = time.time() + bot_poll_secs # Wash. Rinse. Repeat.
        shutdown_msg = "orderly shutdown"
    finally:
        connection.close()
        bot_runner.shutdown(shutdown_msg)


