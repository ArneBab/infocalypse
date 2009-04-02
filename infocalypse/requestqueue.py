# REDFLAG: modified need to push changes back into main repo
""" This module contains classes for scheduling and running large numbers
    of FCP requests.

    Copyright (C) 2008 Darrell Karbott

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

import time

from fcpconnection import MinimalClient

class QueueableRequest(MinimalClient):
    """ A request which can be queued in a RequestQueue and run
        by a RequestRunner.
    """
    def __init__(self, queue):
        MinimalClient.__init__(self)
        self.queue = queue
        self.message_callback = None # set by RequestRunner
        # The time after which this request should be canceled.
        self.cancel_time_secs = None # RequestQueue.next_request() MUST set this

class RequestRunner:
    """ Class to run requests scheduled on one or more RequestQueues. """
    def __init__(self, connection, concurrent):
        self.connection = connection
        self.concurrent = concurrent
        # request id -> client
        self.running = {}
        self.request_queues = []
        self.index = 0

    def add_queue(self, request_queue):
        """ Add a queue to the scheduler. """
        if not request_queue in self.request_queues:
            self.request_queues.append(request_queue)

    def remove_queue(self, request_queue):
        """ Remove a queue from the scheduler. """
        if request_queue in self.request_queues:
            self.request_queues.remove(request_queue)

    def cancel_request(self, client):
        """ Cancel a request.

            This is asynchronous.
        """
        #print "CLIENT: ", client, type(client)
        if type(client) == type(1):
            raise Exception("Hack added to find bug: REDFLAG")

        self.connection.remove_request(client.request_id())
        # REDFLAG: BUG: fix to set cancel time in the past.
        #               fix kick to check cancel time before starting?
    def kick(self):
        """ Run the scheduler state machine.

            You MUST call this frequently.
        """

        if self.connection.is_uploading():
            # REDFLAG: Test this code path!
            #print "kick -- bailed out, still UPLOADING..."
            # Wait for upload to finish.
            return

        # Cancel running requests which have timed out.
        now = time.time()
        for client in self.running.values():
            assert client.cancel_time_secs
            if client.cancel_time_secs < now:
                self.connection.remove_request(client.request_id())

        # REDFLAG: test this code with multiple queues!!!
        # Round robin schedule requests from queues
        idle_queues = 0
        # Catch before uninsightful /0 error on the next line.
        assert len(self.request_queues) > 0
        self.index = self.index % len(self.request_queues) # Paranoid
        start_index = self.index
        while (len(self.running) < self.concurrent
               and idle_queues <  len(self.request_queues)
               and not self.connection.is_uploading()):
            #print "IDLE_QUEUES:", idle_queues
            if self.index == start_index:
                idle_queues = 0
            client = self.request_queues[self.index].next_runnable()
            #print "CLIENT:", client
            if client:
                #print "client.queue: ", client.queue
                #print "running: ", client
                #if 'URI' in client.in_params.fcp_params:
                #    print "   ", client.in_params.fcp_params['URI']
                assert client.queue == self.request_queues[self.index]
                client.in_params.async = True
                client.message_callback = self.msg_callback
                self.running[self.connection.start_request(client)] \
                                                                        = client
            else:
                idle_queues += 1
            self.index = (self.index + 1) % len(self.request_queues)

    def msg_callback(self, client, msg):
        """ Route incoming FCP messages to the appropriate queues. """
        if client.is_finished():
            client.queue.request_done(client, msg)
            #print "RUNNING:"
            #print self.running
            del self.running[client.request_id()]
            self.kick() # haha
        else:
            client.queue.request_progress(client, msg)


class RequestQueue:
    """ Abstract base class for request queues. """
    def __init__(self, runner):
        self.runner = runner

    def next_runnable(self):
        """ Return a MinimalClient instance for the next request to
            be run or None if none is available. """
        pass

    def request_progress(self, client, msg):
        """ Handle non-terminal FCP messages for running requests. """
        pass

    def request_done(self, client, msg):
        """ Handle terminal FCP messages for running requests. """
        pass

