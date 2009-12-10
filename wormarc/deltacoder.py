""" A delta encoder/decoder based on Mercurial's binary diff/patch code.

    ATTRIBUTION: Contains source fragements written by Matt Mackall.

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

# For names in pillaged Mercurial code.
# pylint: disable-msg=C0103, W0141

import zlib
from mercurial import mdiff


from binaryrep import NULL_SHA
############################################################
# ATTRIBUTION: Pillaged from Mercurial revlog.py by Matt Mackall
#              Then hacked, so bugs are mine.
_compress = zlib.compress
_decompress = zlib.decompress

def compress(text):
    """ generate a possibly-compressed representation of text """
    if not text:
        return ("", text)
    l = len(text)
    bin = None
    if l < 44: # Is this Mercurial specific or a zlib overhead thing?
        pass
    elif l > 1000000:
        # zlib makes an internal copy, thus doubling memory usage for
        # large files, so lets do this in pieces
        z = zlib.compressobj()
        p = []
        pos = 0
        while pos < l:
            pos2 = pos + 2**20
            p.append(z.compress(text[pos:pos2]))
            pos = pos2
        p.append(z.flush())
        if sum(map(len, p)) < l:
            bin = "".join(p)
    else:
        bin = _compress(text)
    if bin is None or len(bin) > l:
        if text[0] == '\0':
            return ("", text)
        return ('u', text)
    return ("", bin)

def decompress(bin):
    """ decompress the given input """
    if not bin:
        return bin
    t = bin[0]
    if t == '\0':
        return bin
    if t == 'x':
        return _decompress(bin)
    if t == 'u':
        return bin[1:]

    raise Exception("unknown compression type %r" % t)

    # _ is a function defined in i18n.py to call i18n.gettext.
    #raise RevlogError(_("unknown compression type %r") % t)

############################################################

# REDFLAG: wants_stream ENOTIMPL, who closes stream?
# Returns raw patch data if if it's not set
# returns a readable stream if wants_stream is True, otherwise the raw data
# def example_get_data_func(history_link, wants_stream=False):
#    pass

class DeltaCoder:
    """ Wrapper around the delta compression/decompression implementation
        used by the Mercurial Revlog.

        See revlog.py, mdiff.py, mpatch.c, bdiff.c in Mercurial codebase.
    """
    def __init__(self):
        self.get_data_func = lambda x:None
        self.tmp_file_mgr = None

    # Define an ABC? What would the runtime overhead be?
    # Subclass might need tmp_file_mgr or get_data_func.
    # pylint: disable-msg=R0201
    def make_full_insert(self, new_file, out_file_name,
                         disable_compression=False):
        """ Make a blob readable by apply_deltas containing the entire file. """

        in_file = open(new_file, 'rb')
        raw_new = None
        try:
            raw_new = in_file.read()
        finally:
            in_file.close()

        if disable_compression:
            values = ('u', raw_new)
        else:
            values = compress(raw_new)

        out_file = open(out_file_name, 'wb')
        try:
            if values[0]:
                out_file.write(values[0])
            out_file.write(values[1])
        finally:
            out_file.close()

        return NULL_SHA

    # Writes a new delta blob into out_files
    # Returns parent sha1.
    # Can truncate history by returning NULL_SHA
    def make_delta(self, history_chain, old_file, new_file, out_file_name):
        """ Make a new binary change blob and write it into out_file_name.

        """
        if len(history_chain) == 0:
            #print "DOING FULL INSERT"
            return self.make_full_insert(new_file, out_file_name)

        #print "MAKING DELTA"
        in_file = open(new_file, 'rb')
        raw_new = None
        try:
            raw_new = in_file.read()
        finally:
            in_file.close()

        parent = NULL_SHA
        in_old = open(old_file, 'rb')
        try:
            raw_old = in_old.read()
            values = compress(mdiff.textdiff(raw_old, raw_new))
            parent = history_chain[0][0]
            out_file = open(out_file_name, 'wb')
            try:
                if values[0]:
                    out_file.write(values[0])
                out_file.write(values[1])
            finally:
                out_file.close()
        finally:
            in_old.close()

        return parent

    # All text and patches kept in RAM.
    # Rebuilds the file by applying all the deltas in the history chain.
    def apply_deltas(self, history_chain, out_file_name):
        """ Rebuild a file from a series of patches and write it into
            out_file_name. """
        assert len(history_chain) > 0

        deltas = []
        text = None
        index = 0
        while index < len(history_chain):
            link = history_chain[index]
            if link[2] == NULL_SHA:
                text = link[3]
                if text is None:
                    text = self.get_data_func(link[0])
                break

            delta = link[3]
            if delta is None:
                delta = self.get_data_func(link[0])
                assert not delta is None
            deltas.append(delta)
            index += 1

        assert not text is None
        text = decompress(text)
        if len(deltas) == 0:
            raw = text
        else:
            for index in range(0, len(deltas)):
                deltas[index] = decompress(deltas[index])
            deltas.reverse() # iterate in reverse?
            raw = mdiff.patches(text, deltas)

        text = None
        out_file = open(out_file_name, "wb")
        try:
            out_file.write(raw)
        finally:
            out_file.close()
