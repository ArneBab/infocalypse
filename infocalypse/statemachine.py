""" Classes for making complicated / interdependent sequences of
    FCP requests using state machine logic.

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


# REDFLAG: move this into requestqueue?

import os

from fcpconnection import SUCCESS_MSGS
from requestqueue import QueueableRequest

# Move this to fcpconnection?
def delete_client_file(client):
    """ Delete the file in client.inf_params.file_name. """
    if client.in_params is None:
        return
    if client.in_params.file_name is None:
        return
    if not os.path.exists(client.in_params.file_name):
        return
    assert client.is_finished()
    os.remove(client.in_params.file_name)
# allow tuples, lists?
def require_state(state, state_name):
    """ Raise if state.name != state_name. """
    if state is None or state.name != state_name:
        raise Exception("Illegal State")

# Halting when connection drops?
class StateMachine:
    """ CS101 state machine treatment. """
    def __init__(self):
        # name -> State
        self.states = {}
        self.current_state = None # Subclass should set.
        self.transition_callback = lambda old_state, new_state: None

    def get_state(self, state_name):
        """ Get a state object by name. """
        return self.states[state_name]

    def require_state(self, state_name):
        """ Assert that the current state has the name state_name """
        require_state(self.current_state, state_name)

    def transition(self, to_name):
        """ Transition to the state to_name. """
        new_state = self.states[to_name]
        assert new_state.name == to_name
        old_state = self.current_state
        old_state.leave(new_state) # Shouldn't change state.
        assert self.current_state == old_state
        self.current_state = new_state # Hmmm... order
        self.transition_callback(old_state, new_state) # Shouldn't change state
        assert self.current_state == new_state
        new_state.enter(old_state) # Can change state.

    def reset(self):
        """ Reset all State instances owned by the StateMachine. """
        for state in self.states.values():
            state.reset()

class State:
    """ A class to represent a state in the StateMachine. """
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name

    def enter(self, from_state):
        """ Virtual called when the state is entered.

            It is legal to transition to another state  here. """
        pass

    def leave(self, to_state):
        """ Virtual called when the state is exited. """
        # Handle canceling here.
        pass

    def reset(self):
        """ Pure virtual to reset the state. """
        print self.name
        raise NotImplementedError()

class StatefulRequest(QueueableRequest):
    """ A QueueableRequest which can be processed by a RequestQueueState. """
    def __init__(self, queue):
        QueueableRequest.__init__(self, queue)
        self.tag = None

# Is a delegate which can handle RequestQueue protocol but doesn't
# implement it.
class RequestQueueState(State):
    """ A State subclass which implements the RequestQueue method
        call protocol without subclassing it. """
    def __init__(self, parent, name):
        State.__init__(self, parent, name)
        # ? -> StatefulRequest, key type is implementation dependant
        self.pending = {}

    def reset(self):
        """ Implementation of State virtual. """
        if len(self.pending) > 0:
            print ("BUG?: Reseting state: %s with %i pending requests!" %
                   (self.name, len(self.pending)))

    def next_runnable(self):
        """ Return a MinimalClient instance for the next request to
            be run or None if none is available. """
        pass
        #return None # Trips pylint r201

    def request_progress(self, client, msg):
        """ Handle non-terminal FCP messages for running requests. """
        pass

    def request_done(self, client, msg):
        """ Handle terminal FCP messages for running requests. """
        pass

class DecisionState(RequestQueueState):
    """ Synthetic State which drives a transition to another state
        in enter()."""
    def __init__(self, parent, name):
        RequestQueueState.__init__(self, parent, name)

    def enter(self, from_state):
        """ Immediately drive transition to decide_next_state(). """
        target_state =  self.decide_next_state(from_state)
        assert target_state != self.name
        assert target_state != from_state
        self.parent.transition(target_state)

    # State instance NOT name.
    def decide_next_state(self, dummy_from_state):
        """ Pure virtual.

            Return the state to transition into. """
        print "ENOTIMPL:" + self.name
        return ""

    # Doesn't handle FCP requests.
    def next_runnable(self):
        """ Illegal. """
        assert False

    def request_progress(self, dummy_client, dummy_msg):
        """ Illegal. """
        assert False

    def request_done(self, dummy_client, dummy_msg):
        """ Illegal. """
        assert False

class RunningSingleRequest(RequestQueueState):
    """ RequestQueueState to run a single StatefulRequest.

        Caller MUST set request field.
    """
    def __init__(self, parent, name, success_state, failure_state):
        RequestQueueState.__init__(self, parent, name)
        self.success_state = success_state
        self.failure_state = failure_state
        self.request = None
        self.queued = False
        self.final_msg = None

    def enter(self, dummy_from_state):
        """ Implementation of State virtual. """
        assert not self.queued
        assert len(self.pending) == 0
        assert not self.request is None
        assert not self.request.tag is None

    def reset(self):
        """ Implementation of State virtual. """
        RequestQueueState.reset(self)
        self.request = None
        self.queued = False
        self.final_msg = None

    def next_runnable(self):
        """ Send request for the file once."""
        if self.queued:
            return None

        # REDFLAG: sucky code, weird coupling
        self.parent.ctx.set_cancel_time(self.request)

        self.queued = True
        self.pending[self.request.tag] = self.request
        return self.request

    def request_done(self, client, msg):
        """ Implement virtual. """
        assert self.request == client
        del self.pending[self.request.tag]
        self.final_msg = msg
        if msg[0] in SUCCESS_MSGS:
            self.parent.transition(self.success_state)
            return

        self.parent.transition(self.failure_state)

class Quiescent(RequestQueueState):
    """ The quiescent state for the state machine. """
    def __init__(self, parent, name):
        RequestQueueState.__init__(self, parent, name)
        self.prev_state = 'UNKNOWN'

    def enter(self, from_state):
        """ Implementation of State virtual. """
        self.prev_state = from_state.name

    def reset(self):
        """ Implementation of State virtual. """
        self.prev_state = 'UNKNOWN'
        RequestQueueState.reset(self)

    def arrived_from(self, allowed_states):
        """ Returns True IFF the state machine transitioned to this state
            from one of the states in allowed_states, False otherwise. """
        return self.prev_state in allowed_states

class Canceling(RequestQueueState):
    """ State which cancels FCP requests from the previous state and
        waits for them to finish. """

    def __init__(self, parent, name, finished_state):
        RequestQueueState.__init__(self, parent, name)
        self.finished_state = finished_state

    def enter(self, from_state):
        """ Implementation of State virtual. """
        if not hasattr(from_state, 'pending') or len(from_state.pending) == 0:
            self.parent.transition(self.finished_state)
            return

        self.pending = from_state.pending.copy()
        for request in self.pending.values():
            self.parent.runner.cancel_request(request)

    def request_done(self, client, dummy):
        """ Implementation of RequestQueueState virtual. """
        tag = client.tag
        del self.pending[tag]

        if len(self.pending) == 0:
            self.parent.transition(self.finished_state)
            return

class CandidateRequest(StatefulRequest):
    """ A StatefulRequest subclass that was made from
        some kind of candidate. """
    def __init__(self, queue):
        StatefulRequest.__init__(self, queue)
        self.candidate = None

# This is not as well thought out as the other stuff in this file.
# REDFLAG: better name?
class RetryingRequestList(RequestQueueState):
    """ A RequestQueueState subclass which maintains a collection
        of 'candidate' objects which it uses to make request from.

        NOTE:
        The definition of what a candidate is is left to the subclass.
    """
    def __init__(self, parent, name):
        RequestQueueState.__init__(self, parent, name)
        self.current_candidates = []
        self.next_candidates = []
        self.finished_candidates = []

    def reset(self):
        """ Implementation of State virtual. """
        self.current_candidates = []
        self.next_candidates = []
        self.finished_candidates = []
        RequestQueueState.reset(self)

    def next_runnable(self):
        """ Implementation of RequestQueueState virtual. """
        candidate = self.get_candidate()
        if candidate is None:
            return None

        request = self.make_request(candidate)
        self.pending[request.tag] = request
        return request

    def request_done(self, client, msg):
        """ Implementation of RequestQueueState virtual. """
        candidate = client.candidate
        assert not candidate is None
        del self.pending[client.tag]
        # REDFLAG: fix signature? to get rid of candidate
        self.candidate_done(client, msg, candidate)

    ############################################################
    def is_stalled(self):
        """ Returns True if there are no more candidates to run,
            False otherwise. """
        return (len(self.pending) + len(self.current_candidates)
                + len(self.next_candidates) == 0)

    def pending_candidates(self):
        """ Returns the candiates that are currently being run
            by the RequestQueue. """
        return [request.candidate for request in self.pending.values()]

    # ORDER:
    # 0) Candidates are popped of the lists.
    # 1) All candidates are popped off of current before any are popped
    #    off of next.
    # 2) When current is empty AND all pending requests have finished
    #    next and current are swapped.
    def get_candidate(self):
        """ INTERNAL: Gets the next candidate to run, or None if none
            is available. """
        if len(self.current_candidates) == 0:
            if len(self.pending) != 0 or len(self.next_candidates) == 0:
                # i.e. Don't run requests from the next_candidates
                # until requests for current candidates have finished.
                # REDFLAG: Add a parameter to control this behavior?
                return None

            self.current_candidates = self.next_candidates
            self.next_candidates = []
            return self.get_candidate()

        #print "get_candidate -- ", len(self.pending)
        #   len(self.current_candidates), \
        # len(self.next_candidates)
        #print "CURRENT:"
        #print self.current_candidates
        #print "NEXT:"
        #print self.next_candidates

        candidate = self.current_candidates.pop()

        return candidate

    ############################################################
    def candidate_done(self, client, msg, candidate):
        """ Pure virtual.

            Add candidate to next_candidates here to retry.
            Add candidate to finished_candidates here if done. """
        # Commented out to avoid pylint R0922
        #raise NotImplementedError()
        pass

    def make_request(self, dummy_candidate):
        """ Subclasses must return CandidateRequest or CandidateRequest
            subclass for the candidate."""
        #raise NotImplementedError()

        return CandidateRequest(self.parent)

