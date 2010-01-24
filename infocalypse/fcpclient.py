# REDFLAG: There are changes here that haven't been pushed back into main repo.
""" Simplified client interface for common FCP request.

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

import mimetypes, os, re

from fcpconnection import FCPConnection, IDataSource, READ_BLOCK, \
     MinimalClient, PolledSocket, FCPError, sha1_hexdigest

from fcpmessage import GETNODE_DEF, GENERATE_SSK_DEF, \
     GET_REQUEST_URI_DEF, GET_DEF, \
     PUT_FILE_DEF, PUT_REDIRECT_DEF,  PUT_COMPLEX_DIR_DEF

# Defaults for commonly used FCP parameters.
FCP_PARAM_DEFAULTS = {
    'ReturnType':'direct',
    'IgnoreDS':False,
    'MaxRetries':3,
    'DontCompress':True, # Hmmmm...
    'Verbosity':1023 # MUST set this to get progress messages.
}

#-----------------------------------------------------------#
# file_info helper functions
#-----------------------------------------------------------#

def get_file_infos(directory, forced_mime_type=None, accept_regex = None):
    """ Traverse a directory and return a list of file information
        tuples which is suitable for use by
        FCPClient.put_complex_dir().

        forced_mime_type determines the value of
        the mime_type field in the returned tuples.

        If acceptRegex is not None only files which match
        it are added.

        TUPLE FORMAT:
        (name, length, mime_type, full_path)
    """

    def walk_visitor(file_info, dirname, names):
        """ Function scope visitor implementation passed to os.path.walk.
        """

        for name in names:
            full_name = os.path.join(dirname, name)
            if os.path.isfile(full_name):
                base = file_info[0]
                local_path = full_name.replace(base, '')
                # REDFLAG: More principled way to do this?
                # Fix slashes on windows.
                local_path = local_path.replace('\\', '/') 
                if file_info[2] and not file_info[2].match(local_path):
                    # Skip files rejected by the regex
                    continue

                file_info[1].append((local_path,
                                     os.path.getsize(full_name),
                                     forced_mime_type,
                                     full_name))
    if directory[-1] != os.path.sep:
        # Force trailing path separator.
        directory += os.path.sep
    file_info = (directory, [], accept_regex) #REDFLAG: bad variable name
    os.path.walk(directory, walk_visitor, file_info)
    return file_info[1]

def total_length(file_infos):
    """ Returns the sum of the file lengths in file_info list. """

    total = 0
    for info in file_infos:
        total += info[1]
    return total

def set_index_file(file_infos, file_name):
    """ Move the tuple with the name file_name to the front of
        file_infos so that it will be used as the index.
    """
    index = None
    for info in file_infos: # hmmm... faster search?
        if info[0] == file_name:
            index = info
            break

    if index is None:
        raise ValueError("No file named: %s" % file_name)

    file_infos.remove(index)
    file_infos.insert(0, index)

def sort_file_infos(file_infos):
    """ Helper function forces file infos into a fixed order.

        Note: Doesn't move the first entry.
    """

    if len(file_infos) < 3:
        return file_infos
    rest = file_infos[1:]
    rest.sort()

    return file_infos[:1] + rest

class FileInfoDataSource(IDataSource):
    """ IDataSource which concatenates files in a list of
        file infos into a contiguous data stream.

        Useful for direct ClientPutComplexDir requests.
    """

    MSG_LENGTH_MISMATCH = "Upload bytes doesn't match sum of " \
                          + "lengths in file_infos. Did the files " \
                          + "change during uploading?"

    def __init__(self, file_infos):
        IDataSource.__init__(self)
        assert file_infos
        self.infos = file_infos
        self.total_length = total_length(file_infos)
        self.running_total = 0
        self.chunks = None
        self.input_file = None

    def data_generator(self, infos):
        """ INTERNAL: Returns a generator which yields the concatenated
            data from all the file infos.
        """

        for info in infos:
            #print "FileInfoDataSource.GEN -- opening", info[3]
            self.input_file = open(info[3], 'rb')
            while True:
                raised = True
                try:
                    data = self.input_file.read(READ_BLOCK)
                    #print "FileInfoDataSource.GEN -- read:", len(data)
                    raised = False
                finally:
                    # Note: Wacky control flow because you can't yield
                    #       from a finally block
                    if raised or data is None:
                        #print "FileInfoDataSource.GEN -- closing", info[3]
                        self.input_file.close()
                        self.input_file = None
                if not data:
                    break
                self.running_total += len(data)
                if self.running_total > self.total_length:
                    raise IOError(self.MSG_LENGTH_MISMATCH)
                #print "FileInfoDataSource.GEN -- yeilding", len(data)
                yield data

        if self.running_total != self.total_length:
            raise IOError(self.MSG_LENGTH_MISMATCH)

        yield None
        return

    def initialize(self):
        """ IDataSource implementation. """
        #print "FileInfoDataSource.initialize -- called"
        assert self.chunks is None
        self.chunks = self.data_generator(self.infos)

    def data_length(self):
        """ IDataSource implementation. """
        #print "FileInfoDataSource.data_length -- ", self.total_length
        return self.total_length

    def release(self):
        """ IDataSource implementation. """
        #print "FileInfoDataSource.release -- called"
        if not self.chunks is None:
            self.chunks = None
        if not self.input_file:
            self.input_file.close()
            self.input_file = None

    def read(self):
        """ IDataSource implementation. """
        #print "FileInfoDataSource.read -- called"
        assert not self.chunks is None
        if self.chunks:
            ret = self.chunks.next()
            if ret is None:
                self.chunks = None
                #print "FileInfoDataSource.read -- returned None"
                return None
            #print "FileInfoDataSource.read -- returned:", len(ret)
            return ret
        #print "FileInfoDataSource.read(1) -- returned None, \
        #      SHOULD NEVER HAPPEN"
        return None




#-----------------------------------------------------------#
# Key classification and manipulation helper functions
#-----------------------------------------------------------#

# REDFLAG:  Use a common regex?  Not sure that would cut loc...
USK_FILE_REGEX = re.compile('(freenet:)?(USK).*/((\\-)?[0-9]+[0-9]*)$')
def is_usk_file(uri):
    """ Returns True if uri points to a single file, False otherwise. """
    return bool(USK_FILE_REGEX.match(uri))

USK_CONTAINER_REGEX = re.compile('(freenet:)?(USK).*/((\\-)?[0-9]+[0-9]*)/$')
def is_usk_container(uri):
    """ Return True if uri is USK uri which points to a Freenet
        Container, False otherwise.
    """
    return bool(USK_CONTAINER_REGEX.match(uri))

KEY_TYPE_REGEX = re.compile('(freenet:)?(?P<key_type>CHK|KSK|SSK|USK)@')
def key_type(uri):
    """ Returns the key type. """

    match = KEY_TYPE_REGEX.match(uri)
    if not match:
        raise Exception("Doesn't look like a Freenet URI: %s" % uri)
    return match.groupdict()['key_type']

def is_chk(uri):
    """ Returns True if the URI is a CHK key, False otherwise. """
    return key_type(uri) == 'CHK'

def is_ksk(uri):
    """ Returns True if the URI is a KSK key, False otherwise. """
    return key_type(uri) == 'KSK'

def is_ssk(uri):
    """ Returns True if the URI is a SSK key, False otherwise. """
    return key_type(uri) == 'SSK'

def is_usk(uri):
    """ Returns True if the URI is a USK key, False otherwise. """
    return key_type(uri) == 'USK'

# LATER: fix regex to work for SSKs too.
VERSION_REGEX = re.compile('(?P<usk>USK)@(.*)/(?P<version>'
                           + '(\\-)?[0-9]+[0-9]*)(/.*)?')
def get_version(uri):
    """ Return the version index of USK.

        Raises ValueError if no version could be extracted.
    """

    try:
        version = int(VERSION_REGEX.match(uri).
                      groupdict()['version'])
    except:
        raise ValueError("Couldn't parse a USK or SSK version from: %s" % uri)
    return version

def get_ssk_for_usk_version(usk_uri, version):
    """ Return an SSK for a specific version of a USK.

        NOTE:
        The version in usk_uri is ignored.
    """
    match = VERSION_REGEX.match(usk_uri)
    if not match:
        raise Exception("Couldn't parse version from USK: %s" % usk_uri)

    return 'SSK' + usk_uri[match.end('usk') : match.start('version') - 1] \
           + '-' + str(version) + usk_uri[match.end('version'):]

def get_usk_for_usk_version(usk_uri, version, negative = False):
    """ Return an USK for a specific version of a USK.

        NOTE:
        The version in usk_uri is ignored.
        Works for both containers and files.
    """
    match = VERSION_REGEX.match(usk_uri)
    if not match:
        raise Exception("Couldn't parse version from USK: %s" % usk_uri)
    if negative and version > 0:
        version = -1 * version
    version_str = str(version)
    if version == 0 and negative:
        version_str = '-0'
        # BITCH:
        # They should have picked some other symbol ('*'?) which doesn't
        # encourage implementers to jam the version into an integer.
        # i.e. because you can't represent the version with an integer
        # because -0 == 0.
    assert not negative or version_str.find('-') > -1

    return usk_uri[0 : match.start('version')] \
           + version_str + usk_uri[match.end('version'):]

def is_negative_usk(usk_uri):
    """ Returns True if usk_uri has a negative version index,
        False otherwise.

        REQUIRES: usk_uri is a USK key.
    """
    match = VERSION_REGEX.match(usk_uri)
    if not match:
        raise Exception("Couldn't parse version from USK: %s" % usk_uri)
    return match.groupdict()['version'].find('-') > -1

def get_negative_usk(usk_uri):
    """ Return an USK with a negative version index.

        NOTE:
        Using a negative index causes the FCP server to search
        harder for later versions in ClientGet requests.

        NOTE:
        This is a NOP if usk_uri is already negative.
    """
    version = get_version(usk_uri)
    if is_negative_usk(usk_uri):
        return usk_uri

    return get_usk_for_usk_version(usk_uri, version, True)

def prefetch_usk(client, usk_uri, allowed_redirects = 3,
                 message_callback = None):
    """ Force the FCP server to explicitly search for updates
        to the USK.

        Returns the latest version as an integer or None if
        no version could be determined.

        This works by sending a negative index value for the USK.

        Note that this can return a version LESS THAN the version
        in usk_uri.
    """

    if client.in_params.async:
        raise ValueError("This function only works synchronously.")

    usk_uri = get_negative_usk(usk_uri)
    client.reset()
    callback = client.message_callback
    return_type = client.in_params.default_fcp_params.get('ReturnType')
    version = None
    try:
        if message_callback:
            # Install a custom message callback
            client.message_callback = message_callback
        client.in_params.default_fcp_params['ReturnType'] = 'none'
        try:
            # BUG: HANGS
            version = get_version(client.get(usk_uri,
                                             allowed_redirects)[1]['URI'])
        except FCPError:
            version = None
    finally:
        client.message_callback = callback
        if return_type:
            client.in_params.default_fcp_params['ReturnType'] = return_type

    return version

def latest_usk_index(client, usk_uri, allowed_redirects = 1,
                     message_callback = None):
    """ Determines the version index of a USK key.

        Returns a (version, data_found) tuple where version
        is the integer version and data_found is the data_found
        message for the latest index.

        
        NOTE:
        This fetches the key and discards the data.
        It may take a very long time if you call it for
        a key which points to a large block of data.
    """

    if client.in_params.async:
        raise ValueError("This function only works synchronously.")

    client.reset()
    callback = client.message_callback
    #print "PARAMS:", client.in_params.default_fcp_params
    return_type = client.in_params.default_fcp_params.get('ReturnType')
    try:
        if message_callback:
            # Install a custom message callback
            client.message_callback = message_callback
        client.in_params.default_fcp_params['ReturnType'] = 'none'
        prev = None
        while True:
            # Hmmmm... Make sure that the USK has 'settled'
            next = client.get(usk_uri, allowed_redirects)
            if prev and next[1]['URI'] == prev[1]['URI']:
                break
            prev = next
    finally:
        client.message_callback = callback
        if return_type:
            client.in_params.default_fcp_params['ReturnType'] = return_type

    return (get_version(prev[1]['URI']), prev)

def get_insert_chk_filename(uri):
    """ Returns the file name part of CHK@/file_part.ext style
    CHK insert uris. """
    assert uri.startswith('CHK@')
    if not uri.startswith('CHK@/'):
        if uri != 'CHK@':
            raise ValueError("Unexpected data after '@'. Maybe you forgot the "
                             + "'/' before the filename part?")
        return None
    return uri[5:]

def set_insert_uri(params, uri):
    """ INTERNAL: Set the 'URI' and 'TargetFilename' in params,
    correctly handling CHK@/filename.ext style insert URIs. """

    if is_chk(uri):
        params['URI'] = 'CHK@'
        filename = get_insert_chk_filename(uri)
        if not filename is None:
            params['TargetFilename'] = filename
    else:
        params['URI'] = uri

def get_usk_hash(usk):
    """ Returns a 12 hex digit hash for a USK which is independant
    of verison. """
    return sha1_hexdigest(get_usk_for_usk_version(usk, 0))[:12]

def check_usk_hash(usk, hash_value):
    """ Returns True if the hash matches, False otherwise. """
    return (sha1_hexdigest(get_usk_for_usk_version(usk, 0))[:12]
           == hash_value)

def show_progress(dummy, msg):
    """ Default message callback implementation. """

    if msg[0] == 'SimpleProgress':
        print "Progress: (%s/%s/%s)" % (msg[1]['Succeeded'],
                                        msg[1]['Required'],
                                        msg[1]['Total'])
    else:
        print "Progress: %s" % msg[0]

def parse_progress(msg):
    """ Parse a SimpleProgress message into a tuple. """
    assert msg[0] == 'SimpleProgress'

    return (int(msg[1]['Succeeded']),
            int(msg[1]['Required']),
            int(msg[1]['Total']),
            int(msg[1]['Failed']),
            int(msg[1]['FatallyFailed']),
            bool(msg[1]['FinalizedTotal'].lower() == 'true'))

class FCPClient(MinimalClient):
    """ A class to execute common FCP requests.

        This class provides a simplified interface for common FCP commands.
        Calls are blocking by default.  Set FCPClient.in_params.async = True
        to run asynchronously.

        You can set FCP parameters using the
        FCPClient.in_params.default_fcp_params dictionary.

        GOTCHA:
        Don't set FCPClient.in_params.fcp_params directly. It is reset
        before most calls so changes to it probably won't have any effect.
    """
    def __init__(self, conn):
        MinimalClient.__init__(self)
        self.conn = conn
        self.message_callback = show_progress
        self.in_params.default_fcp_params = FCP_PARAM_DEFAULTS.copy()

    @classmethod
    def connect(cls, host, port, socket_class = PolledSocket,
                state_callback = None):
        """ Create an FCPClient which owns a new FCPConnection.

            NOTE: If you need multiple FCPClient instances it is
                  better to explictly create an FCPConnection and
                  use the FCPClient.__init__() method so that all
                  instances are multiplexed over the same connection.
            """
        sock = None
        conn = None
        raised = True
        try:
            sock = socket_class(host, port)
            conn = FCPConnection(sock, True, state_callback)
            raised = False
        finally:
            if raised:
                if conn:
                    conn.close()
                if sock:
                    sock.close()

        return FCPClient(conn)


    def wait_until_finished(self):
        """ Wait for the current request to finish. """
        assert self.conn
        self.conn.wait_for_terminal(self)

    def close(self):
        """ Close the underlying FCPConnection. """
        if self.conn:
            self.conn.close()

    def get_node(self, opennet = False, private = False, volatile = True):
        """ Query node information by sending an FCP GetNode message. """

        # Hmmmm... I added an 'Identifier' value to request message
        # even though there's None in the doc. See GETNODE_DEF.
        # It seems to work.
        self.reset()
        self.in_params.definition = GETNODE_DEF
        self.in_params.fcp_params = {'GiveOpennetRef': opennet,
                                     'WithPrivate': private,
                                     'WithVolatile': volatile }

        return self.conn.start_request(self)

    def generate_ssk(self):
        """ Generate an SSK key pair.

        Returns the SSKKeyPair message.
        """
        self.reset()
        self.in_params.definition = GENERATE_SSK_DEF
        return self.conn.start_request(self)

    def get_request_uri(self, insert_uri):
        """ Return the request URI corresponding to the insert URI.

            REQUIRES: insert_uri is a private SSK or USK.
        """

        if self.in_params.async:
            raise ValueError("This function only works synchronously.")

        assert is_usk(insert_uri) or is_ssk(insert_uri)

        if is_usk(insert_uri):
            target = get_ssk_for_usk_version(insert_uri, 0)
        else:
            target = insert_uri

        self.reset()
        self.in_params.definition = GET_REQUEST_URI_DEF
        self.in_params.fcp_params = {'URI': target,
                                     'MaxRetries': 1,
                                     'PriorityClass':1,
                                     'UploadFrom':'direct',
                                                'DataLength':9,
                                     'GetCHKOnly':True}
        self.in_params.send_data = '012345678' # 9 bytes of data
        inverted = self.conn.start_request(self)[1]['URI']
        public = inverted[inverted.find('@') + 1: inverted.find('/')]
        return insert_uri[:insert_uri.find('@') + 1] + public \
               + insert_uri[insert_uri.find('/'):]

    def get(self, uri, allowed_redirects = 0, output_file = None):
        """ Requests the data corresponding to the URI from the
        FCP server.

        Returns an AllData or DataFound (when
        self.default_fcp_params['ReturnType'] == 'none') message
        on success.

        If output_file or self.output_file is not None, write the
        raw data to file instead of returning it as a string.

        Raises an FCPError on failure.

        An extra 'URI' entry is added to the returned message
        containing the final URI the data was requested
        from after redirecting.

        An extra 'Metadata.ContentType' entry is added to the
        returned AllData message containing the mime type
        information extracted from the last DataFound.
        """
        self.reset()
        self.in_params.definition = GET_DEF
        self.in_params.fcp_params = {'URI':uri }
        self.in_params.allowed_redirects = allowed_redirects
        self.in_params.file_name = output_file
        # REDFLAG: fix
        self.in_params.send_data = False
        return self.conn.start_request(self)


    def put(self, uri, bytes, mime_type=None):
        """ Insert a string into Freenet.

            Returns a PutSuccessful message on success.
            Raises an FCPError on failure.
        """
        self.reset()
        self.in_params.definition = PUT_FILE_DEF
        set_insert_uri(self.in_params.fcp_params, uri)
        if mime_type:
            self.in_params.fcp_params['Metadata.ContentType'] = mime_type

        self.in_params.send_data = bytes
        return self.conn.start_request(self)

    def put_file(self, uri, path, mime_type=None):
        """ Insert a single file into Freenet.

        Returns a PutSuccessful message on success.
        Raises an FCPError on failure.

        REQUIRES: The size of the file can't change during this
                  call.
        """

        self.reset()
        self.in_params.definition = PUT_FILE_DEF
        set_insert_uri(self.in_params.fcp_params, uri)

        if mime_type:
            self.in_params.fcp_params['Metadata.ContentType'] = mime_type

        # REDFLAG: test. not sure this ever worked in previous version
        #if 'UploadFrom' in params and params['UploadFrom'] == 'disk':
        #    # REDFLAG: test this code path!
        #    params['FileName'] = path
        #    path = None

        self.in_params.file_name = path
        # REDFLAG: fix
        self.in_params.send_data = True
        return self.conn.start_request(self)

    def put_redirect(self, uri, target_uri, mime_type=None):
        """ Insert a redirect into freenet.

        Returns a PutSuccessful message on success.
        Raises an FCPError on failure.
        """
        self.reset()
        self.in_params.definition = PUT_REDIRECT_DEF
        self.in_params.fcp_params = {'URI':uri,
                                     'TargetURI':target_uri,
                                     'UploadFrom':'redirect'}
        if mime_type:
            self.in_params.fcp_params['Metadata.ContentType'] = mime_type
        return self.conn.start_request(self)

    def put_complex_dir(self, uri, file_infos,
                        default_mime_type = 'text/plain'):
        """ Insert a collection of files into a Freenet Container.

            file_infos must be a list of
            (name, length, mime_type, full_path) tuples.

            file_infos[0] is inserted as the default document.

            mime types:
            If the mime_type value in the file_infos tuple for the
            file is not None, it is used.  Otherwise the mime type
            is guessed from the file extension. Finally, if guessing
            fails, default_mime_type is used.
        """

        assert default_mime_type
        assert file_infos

        self.reset()
        self.in_params.definition = PUT_COMPLEX_DIR_DEF
        self.in_params.fcp_params = {'URI': uri}

        # IMPORTANT: Don't set the data length.
        return self.conn.start_request(self,
                                       dir_data_source(file_infos,
                                                       self.in_params,
                                                       default_mime_type),
                                       False)


# Break out implementation helper so I can use it elsewhere.
def dir_data_source(file_infos, in_params, default_mime_type):
    """ Return an IDataSource for a list of file_infos.

        NOTE: Also sets up Files.* fields in in_params as a
              side effect. """

    for field in in_params.default_fcp_params:
        if field.startswith("Files"):
            raise ValueError("You can't set file entries via "
                             + " default_fcp_params.")
    if 'DefaultName' in in_params.default_fcp_params:
        raise ValueError("You can't set 'DefaultName' via "
                         + "default_fcp_params.")

    # IMPORTANT: Sort the file infos so that the same set of
    #            file_infos always yields the same inserted data blob.
    #            file_infos[0] isn't moved.
    file_infos = sort_file_infos(file_infos)

    files = {}
    index = 0
    for info in file_infos:
        mime_type = info[2]
        if not mime_type:
            # First try to guess from the extension.
            type_tuple = mimetypes.guess_type(info[0])
            if type_tuple:
                mime_type = type_tuple[0]
        if not mime_type:
            # Fall back to the default.
            mime_type = default_mime_type

        files['Files.%i.Name' % index] = info[0]
        files['Files.%i.UploadFrom' % index] = 'direct'
        files['Files.%i.DataLength' % index] = info[1]
        files['Files.%i.Metadata.ContentType' % index] = mime_type

        index += 1

    in_params.fcp_params['Files'] = files
    in_params.fcp_params['DefaultName'] = file_infos[0][0]

    #REDFLAG: Fix
    in_params.send_data = True

    return FileInfoDataSource(file_infos)

############################################################
# Helper function for hg changeset bundle handling.
############################################################

# Saw here:
# http://sage.math.washington.edu/home/robertwb/trac-bundle/test \
#       /sage_trac/log/trac.log
HG_MIME_TYPE = 'application/mercurial-bundle'

def package_metadata(metadata):
    """ Package the bundle contents metadata into a string which
        can be inserted into to the Metadata.ContentType field
        of the Freenet key.

        All args must be full 40 digit hex keys.
    """
    return "%s;%s,%s,%s" % (HG_MIME_TYPE, metadata[0], metadata[1], metadata[2])

CHANGESET_REGEX = re.compile('.*;\s*([0-9a-fA-F]{40,40})\s*,'
                              + '\s*([0-9a-fA-F]{40,40})\s*,'
                              + '\s*([0-9a-fA-F]{40,40})')
def parse_metadata(msg):
    """ INTERNAL: Parse the (base_rev, first_rev, tip) info out of the
        Metadata.ContentType field of msg.

        FCP2.0 doesn't have support for user defined metadata, so we
        jam the metadata we need into the mime type field.
    """
    match = CHANGESET_REGEX.match(msg[1]['Metadata.ContentType'])
    if not match or len(match.groups()) != 3:
        # This happens for bundles inserted with older versions
        # of hg2fn.py
        raise ValueError("Couldn't parse changeset info from [%s]." \
                         % msg[1]['Metadata.ContentType'])
    return match.groups()

def make_rollup_filename(rollup_info, request_uri):
    """ Return a filename containing info for a rollup bundle. """
    if not is_usk_file(request_uri):
        raise ValueError("request_uri is not a USK file uri.")

    # Hmmmm.... get rid of symbolic names?
    tip = rollup_info[0][0]
    parent = rollup_info[0][1]
    start_index = rollup_info[0][2]
    end_index = rollup_info[0][3]
    assert len(tip) == 40 # LATER: is_changset_id_str() func?
    assert len(parent) == 40
    assert start_index >= 0
    assert end_index >= 0
    assert end_index >= start_index

    human_readable = request_uri.split('/')[1]
    # hmmmm... always supress .hg
    if human_readable.lower().endswith('.hg'):
        human_readable = human_readable[:-3]
    # <human_name>_<end_index>_<start_index>_<tip>_<parent>_ID<repoid>
    return "%s_%i_%i_%s_%s_ID%s" % (human_readable, end_index, start_index,
                                    tip[:12], parent[:12],
                                    get_usk_hash(request_uri))

def parse_rollup_filename(filename):
    """ Parse a filename created with make_rollup_filename
        into a tuple."""
    fields = filename.split('_')
    repo_id = fields[-1]
    if not repo_id.startswith("ID") or len(repo_id) != 14:
        raise ValueError("Couldn't parse repo usk hash.")
    repo_id = repo_id[2:]
    parent = fields[-2]
    if len(parent) != 12:
        raise ValueError("Couldn't parse parent.")
    tip = fields[-3]
    if len(tip) != 12:
        raise ValueError("Couldn't parse tip.")
    start_index = int(fields[-4])
    end_index = int(fields[-5])
    human_readable = '_'.join(fields[:-6]) # REDFLAG: dci obo?
    return (human_readable, start_index, end_index, tip, parent, repo_id)


############################################################
# Stuff moved from updatesm.py, cleanup.
############################################################

# Hmmm... do better?
# IIF ends with .R1 second ssk ends with .R0.
# Makes it easy for paranoid people to disable redundant
# top key fetching. ie. just request *R0 instead of *R1.
# Also could intuitively be expanded to higher levels of
# redundancy.
def make_redundant_ssk(usk, version):
    """ Returns a redundant ssk pair for the USK version IFF the file
        part of usk ends with '.R1', otherwise a single
        ssk for the usk specified version. """
    ssk = get_ssk_for_usk_version(usk, version)
    fields = ssk.split('-')
    if not fields[-2].endswith('.R1'):
        return (ssk, )
    #print "make_redundant_ssk -- is redundant"
    fields[-2] = fields[-2][:-2] + 'R0'
    return (ssk, '-'.join(fields))

# For search
def make_search_uris(uri):
    """ Returns a redundant USK pair if the file part of uri ends
        with '.R1', a tuple containing only uri. """
    if not is_usk_file(uri):
        return (uri,)
    fields = uri.split('/')
    if not fields[-2].endswith('.R1'):
        return (uri, )
    #print "make_search_uris -- is redundant"
    fields[-2] = fields[-2][:-2] + 'R0'
    return (uri, '/'.join(fields))

def make_frozen_uris(uri, increment=True):
    """ Returns a possibly redundant SSK tuple for the 'frozen'
        version of file USK uris, a tuple containing uri for other uris.

        NOTE: This increments the version by 1 if uri is a USK
              and increment is True.
    """
    if uri == 'CHK@':
        return (uri,)
    assert is_usk_file(uri)
    version = get_version(uri)
    return make_redundant_ssk(uri, version + int(bool(increment)))

def ssk_to_usk(ssk):
    """ Convert an SSK for a file USK back into a file USK. """
    fields = ssk.split('-')
    end = '/'.join(fields[-2:])
    fields = fields[:-2] + [end, ]
    return 'USK' + '-'.join(fields)[3:]


