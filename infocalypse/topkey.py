""" Helper functions to read and write the update stored in Hg
    Infocalypse top level keys.

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


    The Python rep of top key data is just a tuple:
    ((graph_a_chk, graph_b_chk), (<update>,...))

    Where:
    <update> := (length, parent_rev, latest_rev, (CHK, ...))

    top_key_data_to_bytes() converts from the tuple format to
    a compact binary rep.
    bytes_to_top_key_data() converts the binary rep back to a tuple.
"""


# Hmmm... this is essentially a bespoke solution for limitations in
# Freenet metadata processing.
import struct

from binascii import hexlify, unhexlify

from chk import CHK_SIZE, bytes_to_chk, chk_to_bytes

from fcpconnection import sha1_hexdigest

MAJOR_VERSION = '1'
MINOR_VERSION = '00'

HDR_VERSION = MAJOR_VERSION + MINOR_VERSION
HDR_BYTES = 'HGINF%s' % HDR_VERSION

# Header length 'HGINF100'
HDR_SIZE = 8
assert len(HDR_BYTES) == HDR_SIZE

# Length of the binary rep of an hg version
HGVER_SIZE = 20

# <header bytes><salt byte><num graph chks>
BASE_FMT = "!%isBB" % HDR_SIZE
# bundle_len:parent_rev:latest_rev:CHK [:CHK]
BASE_UPDATE_FMT = "!q%is%isB" % (HGVER_SIZE, HGVER_SIZE)
# Binary rep of a single CHK
CHK_FMT = "!%is" % CHK_SIZE

BASE_LEN = struct.calcsize(BASE_FMT)
BASE_UPDATE_LEN = struct.calcsize(BASE_UPDATE_FMT)

def top_key_tuple_to_bytes(top_key_tuple, salt_byte=0):
    """ Returns a binary representation of top_key_tuple. """

    ret = struct.pack(BASE_FMT, HDR_BYTES, salt_byte, len(top_key_tuple[0]))
    for graph_chk in top_key_tuple[0]:
        ret += chk_to_bytes(graph_chk)

    for update in top_key_tuple[1]:
        assert len(update[1]) == 40
        assert len(update[2]) == 40
        ret += struct.pack(BASE_UPDATE_FMT, update[0],
                           unhexlify(update[1]),
                           unhexlify(update[2]),
                           len(update[3]))
        for chk in update[3]:
            chk_bytes = struct.pack(CHK_FMT, chk_to_bytes(chk))
            assert len(chk_bytes) == CHK_SIZE
            ret += chk_bytes

    return ret

def bytes_to_top_key_tuple(bytes):
    """ Parses the top key data from a byte block and returns a tuple. """

    if len(bytes) < BASE_LEN:
        raise ValueError("Not enough data to parse static fields.")

    # Hmmm... return the salt byte?
    hdr, dummy, graph_chk_count = struct.unpack(BASE_FMT, bytes[:BASE_LEN])
    #print "bytes_to_top_key_data -- salt: ", dummy
    bytes = bytes[BASE_LEN:]
    if hdr != HDR_BYTES:
        print "bytes_to_top_key_data -- header doesn't match! Expect problems."
    if len(bytes) == 0:
        print "bytes_to_top_key_data -- No updates?"

    graph_chks = []
    for dummy in range(0, graph_chk_count):
        graph_chks.append(bytes_to_chk(struct.unpack(CHK_FMT,
                                                     bytes[:CHK_SIZE])[0]))
        bytes = bytes[CHK_SIZE:]

    updates = []
    while len(bytes) > BASE_UPDATE_LEN:
        length, raw_parent, raw_latest, chk_count = struct.unpack(
            BASE_UPDATE_FMT,
            bytes[:BASE_UPDATE_LEN])

        bytes = bytes[BASE_UPDATE_LEN:]
        chks = []
        for dummy in range(0, chk_count):
            chks.append(bytes_to_chk(struct.unpack(CHK_FMT,
                                                   bytes[:CHK_SIZE])[0]))
            bytes = bytes[CHK_SIZE:]

        updates.append((length, hexlify(raw_parent), hexlify(raw_latest),
                       tuple(chks)))

    return (tuple(graph_chks), tuple(updates))

def default_out(text):
    """ Default output function for dump_top_key_tuple(). """
    if text.endswith('\n'):
        text = text[:-1]
    print text

def dump_top_key_tuple(top_key_tuple, out_func=default_out):
    """ Debugging function to print a top_key_tuple. """
    out_func("---top key tuple---\n")
    for index, chk in enumerate(top_key_tuple[0]):
        out_func("graph_%s:%s\n" % (chr(ord('a') + index), chk))
    for index, update in enumerate(top_key_tuple[1]):
        out_func("update[%i]\n" % index)
        out_func("   length    : %i\n" % update[0])
        out_func("   parent_rev: %s\n" % update[1])
        out_func("   latest_rev: %s\n" % update[2])
        for index, chk in enumerate(update[3]):
            out_func("   CHK[%i]:%s\n" % (index, chk))
    out_func("binary rep sha1:\n0x00:%s\n0xff:%s\n" %
             (sha1_hexdigest(top_key_tuple_to_bytes(top_key_tuple, 0)),
              sha1_hexdigest(top_key_tuple_to_bytes(top_key_tuple, 0xff))))
    out_func("---\n")
