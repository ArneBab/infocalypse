""" Classes to create a multiplexed asynchronous connection to an
    FCP server.

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

    OVERVIEW:
    IAsyncSocket is an abstract interface to an asynchronous
    socket.  The intent is that client code can plug in a
    framework appropriate implementation. i.e. for Twisted,
    asyncore, Tkinter, pyQt, pyGtk, etc.  A platform agnostic
    implementation, PolledSocket is supplied.

    FCPConnection uses an IAsyncSocket delegate to run the
    FCP 2.0 protocol over a single socket connection to an FCP server.

    FCPConnection fully(*) supports multiplexing multiple requests.
    Client code runs requests by passing an instance of MinimalClient
    into FCPConnection.start_request().  The FCPClient MinimalClient
    subclass provides convenience wrapper functions for common requests.

    Both blocking and non-blocking client requests are supported.
    If MinimalClient.in_params.async == True, FCPConnection.start_connection()
    returns a request id string immediately.  This is the same request
    id which appears in the 'Identifier' field of subsequent incoming.
    FCP messages. The MinimalClient.message_callback(client, msg)
    callback function is called for every incoming client message for
    the request. Async client code can detect the request has finished
    by checking client.is_finished() from this callback.

    (*) GOTCHA: If you start a request which writes trailing data
    to the FCP server, the FCPConnection will transition into the
    UPLOADING state and you won't be able to start new requests until
    uploading finishes and it transitions back to the CONNECTED state.
    It is recommended that you use a dedicated FCPConnection instance
    for file uploads. You don't have to worry about this if you use
    blocking requests exclusively.
"""
# REDFLAG: get pylint to acknowledge inherited doc strings from ABCs?

import os, os.path, random, select, socket, time

try:
    from hashlib import sha1
    def sha1_hexdigest(bytes):
        """ Return the SHA1 hexdigest of bytes using the hashlib module. """
        return sha1(bytes).hexdigest()
except ImportError:
    # Fall back so that code still runs on pre 2.6 systems.
    import sha
    def sha1_hexdigest(bytes):
        """ Return the SHA1 hexdigest of bytes using the sha module. """
        return sha.new(bytes).hexdigest()

from fcpmessage import make_request, FCPParser, HELLO_DEF, REMOVE_REQUEST_DEF

FCP_VERSION = '2.0' # Expected version value sent in ClientHello

RECV_BLOCK = 4096 # socket recv
SEND_BLOCK = 4096 # socket send
READ_BLOCK = 16 * 1024  # disk read

MAX_SOCKET_READ = 33 * 1024 # approx. max bytes read during IAsyncSocket.poll()

POLL_TIME_SECS = 0.25 # hmmmm...

# FCPConnection states.
CONNECTING = 1
CONNECTED  = 2
CLOSED     = 3
UPLOADING  = 4

CONNECTION_STATES = {CONNECTING:'CONNECTING',
                     CONNECTED:'CONNECTED',
                     CLOSED:'CLOSED',
                     UPLOADING:'UPLOADING'}

def example_state_callback(dummy, state):
    """ Example FCPConnection.state_callback function. """

    value = CONNECTION_STATES.get(state)
    if not value:
        value = "UNKNOWN"
    print "FCPConnection State -> [%s]" % value

def make_id():
    """ INTERNAL: Make a unique id string. """
    return sha1_hexdigest(str(random.random()) + str(time.time()))

#-----------------------------------------------------------#
# Byte level socket handling
#-----------------------------------------------------------#

class IAsyncSocket:
    """ Abstract interface for an asynchronous socket. """
    def __init__(self):
        # lambda's prevent pylint E1102 warning

        # Data arrived on socket
        self.recv_callback = lambda x:None
        # Socket closed
        self.closed_callback = lambda :None
        # Socket wants data to write. This can be None.
        self.writable_callback = None

    def write_bytes(self, bytes):
        """ Write bytes to the socket. """
        pass

    def close(self):
        """ Release all resources associated with the socket. """
        pass

    # HACK to implement waiting on messages.
    def poll(self):
        """ Do whatever is required to check for new activity
            on the socket.

            e.g. run gui framework message pump, explictly poll, etc.
            MUST call recv_callback, writable_callback
        """
        pass

class NonBlockingSocket(IAsyncSocket):
    """ Base class used for IAsyncSocket implementations based on
        non-blocking BSD style sockets.
    """
    def __init__(self, connected_socket):
        """ REQUIRES: connected_socket is non-blocking and fully connected. """
        IAsyncSocket.__init__(self)
        self.buffer = ""
        self.socket = connected_socket

    def write_bytes(self, bytes):
        """ IAsyncSocket implementation. """
        assert bytes
        self.buffer += bytes
        #print "write_bytes: ", self.buffer

    def close(self):
        """ IAsyncSocket implementation. """
        if self.socket:
            self.socket.close() # sync?
            self.closed_callback()
        self.socket = None

    def do_write(self):
        """ INTERNAL: Write to the socket.

            Returns True if data was written, false otherwise.

            REQUIRES: buffer has data or the writable_callback is set.
        """

        assert self.buffer or self.writable_callback
        if not self.buffer:
            # pylint doesn't infer that this must be set.
            # pylint: disable-msg=E1102
            self.writable_callback()
        if self.buffer:
            chunk = self.buffer[:SEND_BLOCK]
            sent = self.socket.send(chunk)
            #print "WRITING:", self.buffer[:sent]
            assert sent >= 0
            #print "TO_WIRE:"
            #print repr(self.buffer[:sent])
            self.buffer = self.buffer[sent:]
            return True
        assert not self.writable_callback # Hmmmm... This is a client error.
        return False

    def do_read(self):
        """ INTERNAL: Read from the socket.

            Returns the data read from the socket or None
            on EOF.

            Closes on EOF as a side effect.
        """
        data = self.socket.recv(RECV_BLOCK)
        if not data:
            return None

        #print "FROM_WIRE:"
        #print  repr(data)
        return data


class PolledSocket(NonBlockingSocket):
    """ Sucky polled IAsyncSocket implementation which should
        work everywhere. i.e. *nix, Windows, OSX. """

    def __init__(self, host, port):
        connected_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # REDFLAG: Can block here.
        connected_socket.connect((host, port))
        connected_socket.setblocking(0)
        NonBlockingSocket.__init__(self, connected_socket)

    def poll(self):
        """ IAsyncSocket implementation. """
        #print "PolledSocket.poll -- called"
        if not self.socket:
            #print "PolledSocket.poll -- CLOSED"
            raise IOError("The socket is closed")
        # Why? Because we don't want to call the recv_callback while
        # reading... wacky re-entrance issues....
        read = ''
        ret = True
        while len(read) < MAX_SOCKET_READ: # bound read length
            check_writable  = []
            if self.buffer or self.writable_callback:
                check_writable = [self.socket]
            readable, writable, errs = \
                      select.select([self.socket], check_writable,
                                    [self.socket], 0)

            #print "result:", readable, writable, errs

            stop = True
            if errs:
                #print "GOT AN ERROR"
                # Hack. Force an IO exception.
                self.socket.sendall(RECV_BLOCK)
                # Err... should never get here.
                raise IOError("Unknown socket error")

            if readable:
                data = self.do_read()
                if not data:
                    ret = False
                    break

                read += data
                stop = False

            if writable:
                if self.do_write():
                    stop = False

            if stop:
                break

        if read:
            self.recv_callback(read)
        #print "PolledSocket.poll -- exited"
        return ret

#-----------------------------------------------------------#
# Message level FCP protocol handling.
#-----------------------------------------------------------#

# NOTE:
# 'DataFound' is sometimes terminal. See msg_is_terminal().
#
# NOTE:
# This list is not complete.  It only lists
# messages generated by supported FCP commands.
# Messages which always indicate that an FCP request ended in success.
SUCCESS_MSGS = frozenset([ \
    'NodeHello', 'SSKKeypair', 'AllData', 'PutSuccessful', 'NodeData',
    ])

# Messages which always indicate that an FCP request ended in failure.
FAILURE_MSGS = frozenset([ \
    'CloseConnectionDuplicateClientName', 'PutFailed', 'GetFailed',
    'ProtocolError', 'IdentifierCollision', 'UnknownNodeIdentifier',
    'UnknownPeerNoteType'
    ])

# Messages which always indicate that an FCP request ended.
TERMINAL_MSGS = SUCCESS_MSGS.union(FAILURE_MSGS)

def msg_is_terminal(msg, params):
    """ INTERNAL: Return True if the message ends an FCP request,
    False otherwise.
    """

    if msg[0] in TERMINAL_MSGS:
        return True

    # Special cases
    if msg[0] == 'DataFound' and 'ReturnType' in params and \
           params['ReturnType'] == 'none':
        return True

    #print "msg_is_terminal: False"
    #print "MSG:", msg
    #print "PARAMS:", params
    
    return False

def get_code(msg):
    """ Returns integer error code if msg has a 'Code' field
        None otherwise.
    """

    # Hmmmm... does 'Code' ever appear in non-error messages?
    #if not msg[0] in FAILURE_MSGS:
    #    # Message is not an error.
    #    return None

    if not 'Code' in msg[1]:
        if msg[0] in FAILURE_MSGS:
            print "WARNING: get_code(msg, code) couldn't read 'Code'."
        return None

    return int(msg[1]['Code'])

def is_code(msg, error_code):
    """ Returns True if msg has a 'Code' field and it is
        equal to error_code, False, otherwise.
    """

    code = get_code(msg)
    if code is None:
        return False
    return code == error_code

def is_fatal_error(msg):
    """ Returns True if msg has a 'Fatal' field and it
        indicates a non-recoverable error, False otherwise.
    """

    value = msg[1].get('Fatal')
    if value is None:
        return False # hmmm...
    return bool(value.lower() == 'true')

class FCPError(Exception):
    """ An Exception raised when an FCP command fails. """

    def __init__(self, msg):
        Exception.__init__(self, msg[0])
        self.fcp_msg = msg
        self.last_uri = None

    def __str__(self):
        text = "FCPError: " + self.fcp_msg[0]
        if self.fcp_msg[1].has_key('CodeDescription'):
            text += " -- " + self.fcp_msg[1]['CodeDescription']
        return text

    def is_code(self, error_code):
        """ Returns True if the 'Code' field in the FCP error message
            is equal to error_code, False, otherwise.
        """

        if not self.fcp_msg or not 'Code' in self.fcp_msg[1]:
            # YES. This does happen.
            # Hmmmm... just assert?  Can this really happen.
            print "WARNING: FCPError.is_code() couldn't read 'Code'."
            return False

        return is_code(self.fcp_msg, error_code)

def raise_on_error(msg):
    """ INTERNAL: raise an FCPError if msg indicates an error. """

    assert msg
    if msg[0] in FAILURE_MSGS:
        raise FCPError(msg)

class IDataSource:
    """ Abstract interface which provides data written up to
        the FCP Server as part of an FCP request. """
    def __init__(self):
        pass

    def initialize(self):
        """ Initialize. """
        raise NotImplementedError()

    def data_length(self):
        """ Returns the total length of the data which will be
            returned by read(). """
        raise NotImplementedError()

    def release(self):
        """ Release all resources associated with the IDataSource
            implementation. """
        raise NotImplementedError()

    def read(self):
        """ Returns a raw byte block or None if no more data
            is available. """
        raise NotImplementedError()

class FileDataSource(IDataSource):
    """ IDataSource implementation which get's its data from a single
        file.
    """
    def __init__(self, file_name):
        IDataSource.__init__(self)
        self.file_name = file_name
        self.file = None

    def initialize(self):
        """ IDataSource implementation. """
        self.file = open(self.file_name, 'rb')

    def data_length(self):
        """ IDataSource implementation. """
        return os.path.getsize(self.file_name)

    def release(self):
        """ IDataSource implementation. """
        if self.file:
            self.file.close()
            self.file = None

    def read(self):
        """ IDataSource implementation. """
        assert self.file
        return self.file.read(READ_BLOCK)

# MESSAGE LEVEL

class FCPConnection:
    """Class for a single persistent socket connection
       to an FCP server.

       Socket level IO is handled by the IAsyncSocket delegate.

       The connection is multiplexed (i.e. it can handle multiple
       concurrent client requests).

    """

    def __init__(self, socket_, wait_for_connect = False,
                 state_callback = None):
        """ Create an FCPConnection from an open IAsyncSocket instance.

            REQUIRES: socket_ ready for writing.
        """
        self.running_clients = {}
        # Delegate handles parsing FCP protocol off the wire.
        self.parser = FCPParser()
        self.parser.msg_callback = self.msg_handler
        self.parser.context_callback = self.get_context

        self.socket = socket_
        if state_callback:
            self.state_callback = state_callback
        else:
            self.state_callback = lambda x, y: None
        self.socket.recv_callback = self.parser.parse_bytes
        self.socket.closed_callback = self.closed_handler

        self.node_hello = None

        # Only used for uploads.
        self.data_source = None

        # Tell the client code that we are trying to connect.
        self.state_callback(self, CONNECTING)

        # Send a ClientHello
        params = {'Name':'FCPConnection[%s]' % make_id(),
                  'ExpectedVersion': FCP_VERSION}
        self.socket.write_bytes(make_request(HELLO_DEF, params))
        if wait_for_connect:
            # Wait for the reply
            while not self.is_connected():
                if not self.socket.poll():
                    raise IOError("Socket closed")
                time.sleep(POLL_TIME_SECS)

    def is_connected(self):
        """ Returns True if the instance is fully connected to the
            FCP Server and ready to process requests, False otherwise.
        """
        return not self.node_hello is None

    def is_uploading(self):
        """ Returns True if the instance is uploading data, False
            otherwise.
        """
        return (self.data_source or
                self.socket.writable_callback)

    def close(self):
        """ Close the connection and the underlying IAsyncSocket
            delegate.
        """
        if self.socket:
            self.socket.close()

    # set_data_length only applies if data_source is set
    def start_request(self, client, data_source = None, set_data_length = True):
        """ Start an FCP request.

            If in_params.async is True this returns immediately, otherwise
            it blocks until the request finishes.

            If client.in_params.send_data is set, trailing data is sent
            after the request message.  If data_source is not None, then
            the data in it is sent.  Otherwise if client.in_params.file is
            not None, the data in the file is sent. Finally if neither of
            the other sources are not None the contents of
            client.in_params.send_data are sent.

            If set_data_length is True the 'DataLength' field is set in the
            requests FCP message.

            If in_params.async it True, this method returns the identifier
            for the request, otherwise, returns the FCP message which
            terminated the request.
        """
        assert not self.is_uploading()
        assert not client.context
        assert not client.response
        assert not 'Identifier' in client.in_params.fcp_params
        identifier = make_id()
        client.in_params.fcp_params['Identifier'] = identifier
        write_string = False
        if client.in_params.send_data:
            assert not self.data_source
            if data_source:
                data_source.initialize()
                if set_data_length:
                    client.in_params.fcp_params['DataLength'] = (data_source.
                                                                 data_length())
                self.data_source = data_source
                self.socket.writable_callback = self.writable_handler
            elif client.in_params.file_name:
                self.data_source = FileDataSource(client.in_params.file_name)
                self.data_source.initialize()
                client.in_params.fcp_params['DataLength'] = (self.
                                                             data_source.
                                                             data_length())
                self.socket.writable_callback = self.writable_handler
            else:
                client.in_params.fcp_params['DataLength'] = len(client.
                                                                in_params.
                                                                send_data)
                write_string = True

        self.socket.write_bytes(make_request(client.in_params.definition,
                                             client.in_params.fcp_params,
                                             client.in_params.
                                             default_fcp_params))

        if write_string:
            self.socket.write_bytes(client.in_params.send_data)

        assert not client.context
        client.context = RequestContext(client.in_params.allowed_redirects,
                                        identifier,
                                        client.in_params.fcp_params.get('URI'))
        if not client.in_params.send_data:
            client.context.file_name = client.in_params.file_name

        #print "MAPPED [%s]->[%s]" % (identifier, str(client))
        self.running_clients[identifier] = client

        if self.data_source:
            self.state_callback(self, UPLOADING)

        if client.in_params.async:
            return identifier

        resp = self.wait_for_terminal(client)
        raise_on_error(resp)
        return client.response

    def remove_request(self, identifier, is_global = False):
        """ Cancel a running request.
            NOT ALLOWED WHILE UPLOADING DATA.
        """
        if self.is_uploading():
            raise Exception("Can't remove while uploading. Sorry :-(")

        if not identifier in self.running_clients:
            print "FCPConnection.remove_request -- unknown identifier: ", \
                  identifier
        params = {'Identifier': identifier,
                  'Global': is_global}
        self.socket.write_bytes(make_request(REMOVE_REQUEST_DEF, params))

    def wait_for_terminal(self, client):
        """ Wait until the request running on client finishes. """
        while not client.is_finished():
            if not self.socket.poll():
                break
            time.sleep(POLL_TIME_SECS)

        # Doh saw this trip 20080124. Regression from NonBlockingSocket changes?
        # assert client.response
        if not client.response:
            raise IOError("No response. Maybe the socket dropped?")

        return client.response

    def handled_redirect(self, msg, client):
        """ INTERNAL: Handle code 27 redirects. """

        # BITCH: This is a design flaw in the FCP 2.0 protocol.
        #        They should have used unique numbers for all error
        #        codes so that client coders don't need to keep track
        #        of the initiating request in order to interpret the
        #        error code.
        if client.in_params.definition[0] == 'ClientGet' and is_code(msg, 27):
            #print "Checking for allowed redirect"
            if client.context.allowed_redirects:
                #print "Handling redirect"
                client.context.allowed_redirects -= 1
                assert client.context.initiating_id
                assert client.context.initiating_id in self.running_clients
                assert client.context.running_id
                if client.context.running_id != client.context.initiating_id:
                    # Remove the context for the intermediate redirect.
                    #print "DELETED: ", client.context.running_id
                    del self.running_clients[client.context.running_id]

                client.context.running_id = make_id()
                client.context.last_uri = msg[1]['RedirectURI']

                # Copy, don't modify params.
                params = {}
                params.update(client.in_params.fcp_params)
                params['URI'] = client.context.last_uri
                params['Identifier'] = client.context.running_id

                # Send new request.
                self.socket.write_bytes(make_request(client.in_params.
                                                     definition, params))

                #print "MAPPED(1) [%s]->[%s]" % (client.context.running_id,
                #                                str(client))
                self.running_clients[client.context.running_id] = client

                # REDFLAG: change callback to include identifier?
                # Hmmm...fixup identifier in msg?
                if client.message_callback:
                    client.message_callback(client, msg)
                return True

        return False


    def handle_unexpected_msgs(self, msg):
        """ INTERNAL: Process unexpected messages. """

        if not self.node_hello:
            if msg[0] == 'NodeHello':
                self.node_hello = msg
                self.state_callback(self, CONNECTED)
                return True

            raise Exception("Unexpected message before NodeHello: %s"
                            % msg[0])

        if not 'Identifier' in msg[1]:
            print "Saw message without 'Identifier': %s" % msg[0]
            print msg
            return True

        if not msg[1]['Identifier'] in self.running_clients:
            #print "No client for identifier: %s" % msg[1]['Identifier']
            # BITCH: You get a PersistentRequestRemoved msg even for non
            #        peristent requests AND you get it after the GetFailed.
            #print msg[0]
            return True

        return False

    def get_context(self, request_id):
        """ INTERNAL: Lookup RequestContexts for the FCPParser delegate.
        """

        client = self.running_clients.get(request_id)
        if not client:
            raise Exception("No client for identifier: %s" % request_id)
        assert client.context
        return client.context

    def msg_handler(self, msg):
        """INTERNAL: Process incoming FCP messages from the FCPParser delegate.
        """

        if self.handle_unexpected_msgs(msg):
            return

        client = self.running_clients[msg[1]['Identifier']]
        assert client.is_running()

        if msg_is_terminal(msg, client.in_params.fcp_params):
            if self.handled_redirect(msg, client):
                return

            # Remove running context entries
            assert msg[1]['Identifier'] == client.context.running_id
            #print "DELETED: ", client.context.running_id
            del self.running_clients[client.context.running_id]
            if client.context.running_id != client.context.initiating_id:
                #print "DELETED: ", client.context.initiating_id
                del self.running_clients[client.context.initiating_id]

            if msg[0] == 'DataFound' or msg[0] == 'AllData':
                # REDFLAG: Always do this? and fix FCPError.last_uri?
                # Copy URI into final message. i.e. so client
                # sees the final redirect not the inital URI.
                msg[1]['URI'] = client.context.last_uri
            if msg[0] == 'AllData':
                # Copy metadata into final message
                msg[1]['Metadata.ContentType'] = client.context.metadata

                # Add a third entry to the msg tuple containing the raw data,
                # or a comment saying where it was written.
                assert len(msg) == 2
                msg = list(msg)
                if client.context.data_sink.file_name:
                    msg.append("Wrote raw data to: %s" \
                               % client.context.file_name)
                else:
                    msg.append(client.context.data_sink.raw_data)
                msg = tuple(msg)


            # So that MinimalClient.request_id() returns the
            # initiating id correctly even after following
            # redirects.
            msg[1]['Identifier'] = client.context.initiating_id

            # Reset the context
            client.context.release()
            client.context = None

            client.response = msg
            assert not client.is_running()
        else:
            if 'Metadata.ContentType' in msg[1]:
                # Keep track of metadata as we follow redirects
                client.context.metadata = msg[1]['Metadata.ContentType']

        # Notify client.
        if client.message_callback:
            client.message_callback(client, msg)

    def closed_handler(self):
        """ INTERNAL: Callback called by the IAsyncSocket delegate when the
            socket closes. """
        def dropping(data): # REDFLAG: Harmless but remove eventually.
            """ INTERNAL: Print warning when data is dropped after close. """
            print "DROPPING %i BYTES OF DATA AFTER CLOSE!" % len(data)

        self.node_hello = None
        if not self.socket is None:
            self.socket.recv_callback = lambda x:None
            self.socket.recv_callback = dropping # Ignore any subsequent data.

        # Hmmmm... other info, ok to share this?
        fake_msg = ('ProtocolError', {'CodeDescription':'Socket closed'})
        #print "NOTIFIED: CLOSED"

        # Hmmmm... iterate over values instead of keys?
        for identifier in self.running_clients:
            client = self.running_clients[identifier]
            # Remove client from list of running clients.
            #print "CLIENT:", client
            #print "CLIENT.CONTEXT:", client.context
            assert client.context
            assert client.context.running_id
            # Notify client that it has stopped.
            if (client.context.initiating_id == client.context.running_id
                and client.message_callback):
                client.message_callback(client, fake_msg)

        self.running_clients.clear()
        self.state_callback(self, CLOSED)

    def writable_handler(self):
        """ INTERNAL: Callback called by the IAsyncSocket delegate when
            it needs more data to write.
        """

        if not self.data_source:
            return
        data = self.data_source.read()
        if not data:
            self.data_source.release()
            self.data_source = None
            self.socket.writable_callback = None
            if self.is_connected():
                self.state_callback(self, CONNECTED)
            return
        self.socket.write_bytes(data)

# Writes to file if file_name is set, raw_data otherwise
class DataSink:
    """ INTERNAL: Helper class used to save trailing data for FCP
        messages.
    """

    def __init__(self):
        self.file_name = None
        self.file = None
        self.raw_data = ''
        self.data_bytes = 0

    def initialize(self, data_length, file_name):
        """ Initialize the instance.
            If file_name is not None the data is written into
            the file, otherwise, it is saved in the raw_data member.
        """
        # This should only be called once. You can't reuse the datasink.
        assert not self.file and not self.raw_data and not self.data_bytes
        self.data_bytes = data_length
        self.file_name = file_name

    def write_bytes(self, bytes):
        """ Write bytes into the instance.

            Multiple calls can be made. The final amount of
            data written into the instance MUST be equal to
            the data_length value passed into the initialize()
            call.
        """

        #print "WRITE_BYTES called."
        if self.file_name and not self.file:
            self.file = open(self.file_name, 'wb')

        if self.file:
            #print "WRITE_BYTES writing to file"
            if self.file.closed:
                print "FileOrStringDataSink -- refusing to write" \
                      + " to closed file!"
                return
            self.file.write(bytes)
            self.data_bytes -= len(bytes)
            assert self.data_bytes >= 0
            if self.data_bytes == 0:
                self.file.close()
            return

        self.raw_data += bytes
        self.data_bytes -= len(bytes)
        assert self.data_bytes >= 0

    def release(self):
        """ Release all resources associated with the instance. """

        if self.data_bytes != 0:
            print "DataSink.release -- DIDN'T FINISH PREVIOUS READ!", \
                  self.data_bytes
        if self.file:
            self.file.close()
        self.file_name = None
        self.file = None
        self.raw_data = ''
        self.data_bytes = 0

class RequestContext:
    """ INTERNAL: 'Live' context information which an FCPConnection needs
        to keep about a single FCP request.
    """
    def __init__(self, allowed_redirects, identifier, uri):
        self.initiating_id = identifier
        self.running_id = identifier

        # Redirect handling
        self.allowed_redirects = allowed_redirects
        self.last_uri = uri
        self.metadata = "" # Hmmm...

        # Incoming data handling
        self.data_sink = DataSink()

    def writable(self):
        """ Returns the number of additional bytes which can be written
            into the data_sink member.
        """

        return self.data_sink.data_bytes

    def release(self):
        """ Release all resources associated with the instance. """

        self.data_sink.release()


#-----------------------------------------------------------#
# Client code
#-----------------------------------------------------------#

# Hmmmm... created separate class because pylint was complaining
# about too many attributes in MinimalClient and FCPClient
class ClientParams:
    """ A helper class to aggregate request parameters. """

    def __init__(self):
        self.definition = None
        # These are default values which can be modified by the client code.
        # THE IMPLEMENTATION CODE i.e. fcp(connection/client/message)
        # MUST NOT MODIFY THEM.
        self.default_fcp_params = {}
        # These are per request values. They can be modified / reset.
        self.fcp_params = {}
        self.async = False
        self.file_name = None
        self.send_data = None
        self.allowed_redirects = 0

    def reset(self):
        """ Reset all members EXCEPT async, allowed_redirects and
            default_fcp_params to their default values.
        """

        self.definition = None
        self.fcp_params = {}
        self.file_name = None
        self.send_data = None

    # HACK: Not really required, but supresses pylint R0903
    def pretty(self):
        """Returns a human readable rep of the params. """

        return "%s: %s %s %s %s %s %s" % \
               ( self.definition[0],
                 str(self.send_data),
                 str(self.async),
                 self.file_name,
                 self.allowed_redirects,
                 self.fcp_params,
                 self.default_fcp_params )

class MinimalClient:
    """ A single FCP request which can be executed via the
        FCPConnection.start_request() method.

        If in_params.async is True the request runs asynchronously,
        otherwise it causes FCPConnection.start_request() to block.

        The message_callback notifier function is called for
        each incoming FCP message during the request. The first
        argument is the client instance.  Its is_finished()
        method will return True for the final message. message_callback
        implementations MUST NOT modify the state of the client
        instance while is_finished() is False.
    """

    def __init__(self):
        # IN parameters.
        self.in_params = ClientParams()

        # OUT parameter
        self.response = None

        # Variables used while client request is running.
        self.context = None

        # Notification
        self.message_callback = lambda client, msg:None

    def reset(self, reset_params = True):
        """ Reset all members EXCEPT self.in_params.allowed_redirects,
            self.in_params.default_fcp_params and
            self.in_params.async to their default values.
        """
        assert not self.is_running()
        if reset_params:
            self.in_params.reset()
        self.response = None
        self.context = None

    def is_running(self):
        """ Returns True if a request is running, False otherwise. """

        return self.context

    def is_finished(self):
        """ Returns True if the request is finished, False otherwise. """

        return not self.response is None

    def request_id(self):
        """ Returns the request id. """
        if self.response and not self.context:
            return self.response[1]['Identifier']
        elif self.context:
            return self.context.initiating_id
        return None
