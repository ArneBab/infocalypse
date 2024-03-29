""" Classes and functions for creating and parsing FCP messages.

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

    An FCP message is represented as a
    (msg_name, msg_values_dict) tuple.

    Some message e.g. AllData may have a third entry
    which contains the raw data string for the FCP
    message's trailing data.
"""

#-----------------------------------------------------------#
# FCP mesage creation helper functions
#-----------------------------------------------------------#

def merge_params(params, allowed, defaults = None):
    """ Return a new dictionary instance containing only the values
        which have keys in the allowed field list.

        Values are taken from defaults only if they are not
        set in params.
    """

    ret = {}
    for param in allowed:
        if param in params:
            ret[param] = params[param]
        elif defaults and param in defaults:
            ret[param] = defaults[param]
    return ret

def format_params(params, allowed, required):
    """ INTERNAL: Format params into an FCP message body string. """

    ret = b''
    for field in params:
        if not field in allowed:
            raise ValueError("Illegal field [%s]." % field)

    # print("params, allowed, required", params, allowed, required)
    for field in allowed:
        if field in params:
            if field == b'Files':
                # Special case Files dictionary.
                assert params[b'Files']
                for subfield in params[b'Files']:
                    ret += b"%b=%b\n" % (subfield, params[b'Files'][subfield])
                continue
            value = params[field]
            if not value:
                raise ValueError("Illegal value for field [%s]." % field)
            if isinstance(value, int):
                value = str(value).encode('utf-8')
            if value.lower() == b'true' or value.lower() == b'false':
                value = value.lower()
            ret += b"%b=%b\n" % (field, value)
        elif field in required:
            #print "FIELD:", field, required
            raise ValueError("A required field [%s] was not set. Params: %s" % (field, params))
    return ret

# REDFLAG: remove trailing_data?
def make_request(definition, params, defaults = None, trailing_data = None):
    """ Make a request message string from a definition tuple
        and params parameters dictionary.

        Values for allowed parameters not specified in params are
        taken from defaults if they are present and params IS
        UPDATED to include these values.

        A definition tuple has the following entries:
        (msg_name, allowed_fields, required_fields, contraint_func)

        msg_name is the FCP message name.
        allowed_fields is a sequence of field names which are allowed
           in params.
        required_fields is a sequence of field names which are required
           in params. If this is None all the allowed fields are
           assumed to be required.
        constraint_func is a function which takes definitions, params
           arguments and can raise if contraints on the params values
           are not met. This can be None.
    """

    #if 'Identifier' in params:
    #    print "MAKE_REQUEST: ", definition[0], params['Identifier']
    #else:
    #    print "MAKE_REQUEST: ", definition[0], "NO_IDENTIFIER"

    #print "DEFINITION:"
    #print definition
    #print "PARAMS:"
    #print params
    name, allowed, required, constraint_func = definition
    assert name

    real_params = merge_params(params, allowed, defaults)

    # Don't force repetition if required is the same.
    if required is None:
        required = allowed

    ret = name + b'\n' + format_params(real_params, allowed, required) \
          + b'EndMessage\n'

    # Run extra checks on parameter values
    # Order is important.  Format_params can raise on missing fields.
    if constraint_func:
        constraint_func(definition, real_params)

    if trailing_data:
        ret += trailing_data

    params.clear()
    params.update(real_params)

    return ret

#-----------------------------------------------------------#
# FCP request definitions for make_request()
#-----------------------------------------------------------#

def get_constraint(dummy, params):
    """ INTERNAL: Check get params. """
    if b'ReturnType' in params and params[b'ReturnType'] != b'disk':
        if b'Filename' in params or b'TempFilename' in params:
            raise ValueError("'Filename' and 'TempFileName' only allowed" \
                             + " when 'ReturnType' is disk.")

def put_file_constraint(dummy, params):
    """ INTERNAL: Check put_file params. """
    # Hmmmm... this only checks for required arguments, it
    # doesn't report values that have no effect.
    upload_from = b'direct'
    if b'UploadFrom' in params:
        upload_from = params[b'UploadFrom']
    if upload_from == b'direct':
        if not b'DataLength' in params:
            raise ValueError("'DataLength' MUST be set, 'UploadFrom =="
                                 + " 'direct'.")
    elif upload_from == b'disk':
        if not b'Filename' in params:
            raise ValueError("'Filename' MUST be set, 'UploadFrom =="
                             + " 'disk'.")
        elif upload_from == b'redirect':
            if not b'TargetURI' in params:
                raise ValueError("'TargetURI' MUST be set, 'UploadFrom =="
                                 + " 'redirect'.")
    else:
        raise ValueError("Unknown value, b'UploadFrom' == %s" % upload_from)


HELLO_DEF = (b'ClientHello', (b'Name', b'ExpectedVersion'), None, None)

# Identifier not included in doc?
GETNODE_DEF = (b'GetNode', (b'Identifier', b'GiveOpennetRef', b'WithPrivate',
                           b'WithVolatile'),
               None, None)

#IMPORTANT: One entry tuple MUST have trailing comma or it will evaluate
#           to a string instead of a tuple.
GENERATE_SSK_DEF = (b'GenerateSSK', (b'Identifier',), None, None)
GET_REQUEST_URI_DEF = (b'ClientPut',
                       (b'URI', b'Identifier', b'MaxRetries', b'PriorityClass',
                        b'UploadFrom', b'DataLength', b'GetCHKOnly',b'RealTimeFlag',),
                       (b'URI', b'Identifier',
                        b'UploadFrom', b'DataLength', b'GetCHKOnly',),
                       None)
GET_DEF = (b'ClientGet',
           (b'IgnoreDS', b'DSOnly', b'URI', b'Identifier', b'Verbosity',
            b'MaxSize', b'MaxTempSize', b'MaxRetries', b'PriorityClass',
            b'Persistence', b'ClientToken', b'Global', b'ReturnType',
            b'BinaryBlob', b'AllowedMimeTypes', b'FileName', b'TmpFileName',
            b'RealTimeFlag',),
           (b'URI', b'Identifier'),
           get_constraint)
PUT_FILE_DEF = (b'ClientPut',
                (b'URI', b'Metadata.ContentType', b'Identifier', b'Verbosity',
                 b'MaxRetries', b'PriorityClass', b'GetCHKOnly', b'Global',
                 b'DontCompress','ClientToken', b'Persistence',
                 b'TargetFilename', b'EarlyEncode', b'UploadFrom', b'DataLength',
                 b'Filename', b'TargetURI', b'FileHash', b'BinaryBlob',
                 b'RealTimeFlag',),
                (b'URI', b'Identifier'),
                put_file_constraint)
PUT_REDIRECT_DEF = (b'ClientPut',
                    (b'URI', b'Metadata.ContentType', b'Identifier', b'Verbosity',
                     b'MaxRetries', b'PriorityClass', b'GetCHKOnly', b'Global',
                     b'ClientToken', b'Persistence', b'UploadFrom',
                     b'TargetURI',
                     b'RealTimeFlag',),
                    (b'URI', b'Identifier', b'TargetURI'),
                    None)
PUT_COMPLEX_DIR_DEF = (b'ClientPutComplexDir',
                       (b'URI', b'Identifier', b'Verbosity',
                        b'MaxRetries', b'PriorityClass', b'GetCHKOnly', b'Global',
                        b'DontCompress', b'ClientToken', b'Persistence',
                        b'TargetFileName', b'EarlyEncode', b'DefaultName',
                        b'RealTimeFlag',
                        b'Files'), #<- one off code in format_params() for this
                       (b'URI', b'Identifier'),
                       None)

REMOVE_REQUEST_DEF = (b'RemoveRequest', (b'Identifier', b'Global'), None, None)

# REDFLAG: Shouldn't assert on bad data! raise instead.
# Hmmmm... I hacked this together by unwinding a "pull" parser
# to make a "push" parser.  Feels like there's too much code here.
class FCPParser:
    """Parse a raw byte stream into FCP messages and trailing data blobs.

       Push bytes into the parser by calling FCPParser.parse_bytes().
       Set FCPParser.msg_callback to get the resulting FCP messages.
       Set FCPParser.context_callback to control how trailing data is written.
       See RequestContext in the fcpconnection module for an example of how
       contexts are supposed to work.

       NOTE: This only handles byte level presentation. It DOES NOT validate
             that the incoming messages are correct w.r.t. the FCP 2.0 spec.
    """
    def __init__(self):
        self.msg = None
        self.prev_chunk = b""
        self.data_context = None

        # lambda's prevent pylint E1102 warning
        # Called for each parsed message.
        self.msg_callback = lambda msg:None

        # MUST set this callback.
        # Return the RequestContext for the request_id
        self.context_callback = None #lambda request_id:RequestContext()

    def handle_line(self, line):
        """ INTERNAL: Process a single line of an FCP message. """
        if not line:
            return False

        if not self.msg:
            # Start of a new message
            self.msg = [line, {}]
            return False

        pos = line.find(b'=')
        if pos != -1:
            # name=value pair
            fields = (line[:pos], line[pos + 1:])
            # CANNOT just split
            # fields = line.split('=')
            # e.g.
            # ExtraDescription=Invalid precompressed size: 81588 maxlength=10
            assert len(fields) ==  2
            self.msg[1][fields[0].strip()] = fields[1].strip()
        else:
            # end of message line
            if line == b'Data':
                # Handle trailing data
                assert self.msg
                # REDFLAG: runtime protocol error (should never happen)
                assert b'Identifier' in self.msg[1]
                assert not self.data_context
                self.data_context = self.context_callback(self.msg[1]
                                                          [b'Identifier'])
                self.data_context.data_sink.initialize(int(self.msg[1]
                                                           [b'DataLength']),
                                                       self.data_context.
                                                       file_name)
                return True

            assert line == b'End' or line == b'EndMessage'
            msg = self.msg
            self.msg = None
            assert not self.data_context or self.data_context.writable() == 0
            self.msg_callback(msg)

        return False

    def handle_data(self, data):
        """ INTERNAL: Handle trailing data following an FCP message. """
        #print "RECVD: ", len(data), "bytes of data."
        assert self.data_context
        self.data_context.data_sink.write_bytes(data)
        if self.data_context.writable() == 0:
            assert self.msg
            msg = self.msg
            self.msg = None
            self.data_context = None
            self.msg_callback(msg)

    def parse_bytes(self, bytes):
        """ This method drives an FCP Message parser and eventually causes
            calls into msg_callback().
        """
        #print "FCPParser.parse_bytes -- called"
        if self.data_context and self.data_context.writable():
            # Expecting raw data.
            assert not self.prev_chunk
            data = bytes[:self.data_context.writable()]
            self.handle_data(data) # MUST handle msg notification!
            bytes = bytes[len(data):]
            if bytes:
                # Hmmm... recursion depth
                self.parse_bytes(bytes)
        else:
            # Expecting a \n terminated line.
            bytes = self.prev_chunk + bytes
            self.prev_chunk = b""
            last_eol = -1
            pos = bytes.find(b'\n')
            while pos != -1:
                if last_eol <= 0:
                    last_eol = 0

                line = bytes[last_eol:pos].strip()
                last_eol = pos
                if self.handle_line(line):
                    # Reading trailing data
                    # Hmmm... recursion depth
                    self.parse_bytes(bytes[last_eol + 1:])
                    return
                pos = bytes.find(b'\n', last_eol + 1)

            assert not self.data_context or not self.data_context.writable()
            self.prev_chunk = bytes[last_eol + 1:]

