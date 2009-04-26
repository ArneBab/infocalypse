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
    <update> := (length, (parent_rev, ...), (head_revs, ...), (CHK, ...),
                 all_parent_revs, all_head_revs)

    all_parent_revs is True iff all parent revs  are included.
    all_head_revs is True iff all parent revs  are included.

    top_key_data_to_bytes() converts from the tuple format to
    a compact binary rep.
    bytes_to_top_key_data() converts the binary rep back to a tuple.
"""


# Hmmm... this is essentially a bespoke solution for limitations in
# Freenet metadata processing.
import struct

from binascii import hexlify, unhexlify

from fcpconnection import sha1_hexdigest

from chk import CHK_SIZE, bytes_to_chk, chk_to_bytes

# Known versions:
# 1.00 -- Initial release.
# 2.00 -- Support for multiple head and parent versions and incomplete lists.

HDR_V1 = 'HGINF100' # Obsolete

MAJOR_VERSION = '2'
MINOR_VERSION = '00'

HDR_VERSION = MAJOR_VERSION + MINOR_VERSION
HDR_PREFIX = 'HGINF'
HDR_BYTES = HDR_PREFIX + HDR_VERSION

# Header length 'HGINF100'
HDR_SIZE = 8
assert len(HDR_BYTES) == HDR_SIZE

# Length of the binary rep of an hg version
HGVER_SIZE = 20

# <header bytes><salt byte><num graph chks><num updates>
BASE_FMT = "!%isBBB" % HDR_SIZE
# <bundle_len><flags><parent_count><head_count><chk_count> \
#   [parent data][head data][chk data]
BASE_UPDATE_FMT = "!qBBBB"

BASE_LEN = struct.calcsize(BASE_FMT)
BASE_UPDATE_LEN = struct.calcsize(BASE_UPDATE_FMT)

# More pythonic way?
# Hmmm... why are you using bit bashing in the 21st century?
HAS_PARENTS = 0x01
HAS_HEADS = 0x02

def versions_to_bytes(versions):
    """ INTERNAL: Return raw byte string from hg 40 digit hex
    version list. """
    bytes = ''
    for version in versions:
        try:
            raw = unhexlify(version)
            if len(raw) != HGVER_SIZE:
                raise TypeError() # Hmmm... git'r done.
        except TypeError:
            # REDFLAG: Test code path.
            raise ValueError("Couldn't parse 40 digit hex version from: "
                             + str(version))
        bytes += raw
    return bytes

def top_key_tuple_to_bytes(top_key_tuple, salt_byte=0):
    """ Returns a binary representation of top_key_tuple. """

    ret = struct.pack(BASE_FMT, HDR_BYTES, salt_byte,
                      len(top_key_tuple[0]), len(top_key_tuple[1]))
    for graph_chk in top_key_tuple[0]:
        ret += chk_to_bytes(graph_chk)

    # Can't find doc. True for all modern Python
    assert int(True) == 1 and int(False) == 0
    for update in top_key_tuple[1]:
        flags = (((int(update[4]) * 0xff) & HAS_PARENTS)
                 | ((int(update[5]) * 0xff) & HAS_HEADS))

        ret += struct.pack(BASE_UPDATE_FMT,
                           update[0], flags,
                           len(update[1]), len(update[2]),
                           len(update[3]))

        ret += versions_to_bytes(update[1]) # parents
        ret += versions_to_bytes(update[2]) # heads
        for chk in update[3]:
            chk_bytes = chk_to_bytes(chk)
            assert len(chk_bytes) == CHK_SIZE
            ret += chk_bytes

    return ret

def versions_from_bytes(version_bytes):
    """ INTERNAL: Parse a list of hg 40 digit hex version strings from
        a raw byte block. """
    assert (len(version_bytes) % HGVER_SIZE) == 0
    ret = []
    for count in range(0, len(version_bytes) / HGVER_SIZE):
        try:
            ret.append(hexlify(version_bytes[count * HGVER_SIZE:
                                             (count + 1) * HGVER_SIZE]))
        except TypeError:
            raise ValueError("Error parsing an hg version.")
    return tuple(ret)

def bytes_to_update_tuple(bytes):
    """ INTERNAL: Read a single update from raw bytes. """
    length, flags, parent_count, head_count, chk_count = struct.unpack(
        BASE_UPDATE_FMT,
        bytes[:BASE_UPDATE_LEN])

    bytes = bytes[BASE_UPDATE_LEN:]

    parents = versions_from_bytes(bytes[:HGVER_SIZE * parent_count])
    bytes = bytes[HGVER_SIZE * parent_count:]

    heads = versions_from_bytes(bytes[:HGVER_SIZE * head_count])
    bytes = bytes[HGVER_SIZE * head_count:]

    chks = []
    for dummy in range(0, chk_count):
        chks.append(bytes_to_chk(bytes[:CHK_SIZE]))
        bytes = bytes[CHK_SIZE:]

    return ((length, parents, heads, tuple(chks),
             bool(flags & HAS_PARENTS), bool(flags & HAS_HEADS)),
            bytes)


def bytes_to_top_key_tuple(bytes):
    """ Parses the top key data from a byte block and
        returns a (top_key_tuple, header_string, salt_byte) tuple. """

    if len(bytes) < BASE_LEN:
        raise ValueError("Not enough data to parse static fields.")

    if not bytes.startswith(HDR_PREFIX):
        raise ValueError("Doesn't look like top key binary data.")

    # Hmmm... return the salt byte?
    hdr, salt, graph_chk_count, update_count = struct.unpack(BASE_FMT,
                                                             bytes[:BASE_LEN])
    #print "bytes_to_top_key_data -- salt: ", dummy
    bytes = bytes[BASE_LEN:]
    if hdr != HDR_BYTES:
        if hdr[5] != MAJOR_VERSION:
            # DOH! should have done this in initial release.
            raise ValueError("Format version mismatch. "
                             + "Maybe you're running old code?")
        print "bytes_to_top_key_data -- minor version mismatch: ", hdr
    if len(bytes) == 0:
        print "bytes_to_top_key_data -- No updates?"

    graph_chks = []
    for dummy in range(0, graph_chk_count):
        graph_chks.append(bytes_to_chk(bytes[:CHK_SIZE]))
        bytes = bytes[CHK_SIZE:]

    # REDFLAG: Fix range errors for incomplete / bad data.
    updates = []
    for dummy in range(0, update_count):
        update, bytes = bytes_to_update_tuple(bytes)
        updates.append(update)

    return ((tuple(graph_chks), tuple(updates)), hdr, salt)

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
        if update[4] and update[5]:
            text = "full graph info"
        elif not (update[4] or update[5]):
            text = "incomplete parent, head lists"
        elif not update[4]:
            text = "incomplete parent list"
        else:
            text = "incomplete head list"
        out_func("update[%i] (%s)\n" % (index, text))
        out_func("   length : %i\n" % update[0])
        out_func("   parents: %s\n" % ' '.join([ver[:12] for ver in update[1]]))
        out_func("   heads  : %s\n" % ' '.join([ver[:12] for ver in update[2]]))
        for index, chk in enumerate(update[3]):
            out_func("   CHK[%i]:%s\n" % (index, chk))
    out_func("binary rep sha1:\n0x00:%s\n0xff:%s\n" %
             (sha1_hexdigest(top_key_tuple_to_bytes(top_key_tuple, 0)),
              sha1_hexdigest(top_key_tuple_to_bytes(top_key_tuple, 0xff))))
    out_func("---\n")
