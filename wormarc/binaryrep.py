""" Functions to read and write binary representation of archive data.

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

# REDFLAG: Only tested on x86 32-bit Intel Linux. Alignment/endedness issues?
# REDFLAG: OK to read/write byte strings directly w/o (un)pack'ing, right?
# REDFLAG: REDUCE RAM: do chunked read/writes/hash digests where possible.

import struct
from binascii import hexlify
from hashlib import sha1

NULL_SHA = '\x00' * 20

LINK_HEADER_FMT = '!LL20s'
LINK_HEADER_LEN = struct.calcsize(LINK_HEADER_FMT)

COUNT_FMT = "!L"
COUNT_LEN = struct.calcsize(COUNT_FMT)

# REDFLAG: doc <16k name length
MANIFEST_ENTRY_HDR_FMT = "!H20s20s"
MANIFEST_ENTRY_HDR_LEN = struct.calcsize(MANIFEST_ENTRY_HDR_FMT)
MANIFEST_ENTRY_FMT = MANIFEST_ENTRY_HDR_FMT + "%is"

MSG_INCOMPLETE_READ = "Bad stream, EOF during read."

READ_CHUNK_LEN = 1024 * 16

def str_sha(raw_sha):
    """ Return a 12 digit hex string for a raw SHA1 hash. """
    return hexlify(raw_sha)[:12]

# Used to catch pilot error which otherwise shows up as weird failures.
def check_shas(raw_sha_sequence):
    """ INTERNAL: Raise a ValueError if the sequence values don't look like
        raw SHA1 hashes. """
    if raw_sha_sequence is None:
        raise ValueError("SHA1 has sequence is None?")
    for value in raw_sha_sequence:
        if value is None:
            raise ValueError("None instead of binary SHA1 digest")

        if not len(value) == 20:
            raise ValueError("Doesn't look like a binary SHA1 digest: %s" %
                             repr(value))

def checked_read(in_stream, length, allow_eof=False):
    """ Read a fixed number of bytes from an open input stream.

        Raises IOError if EOF is encountered before all bytes are read.
    """

    bytes = in_stream.read(length)
    if allow_eof and bytes == '':
        return bytes
    if len(bytes) != length:
        raise IOError(MSG_INCOMPLETE_READ)
    return bytes

# Wire rep:
# <total length><age><parent><blob data>
#
# Python rep
#  0     1    2       3     4              5             6
# (sha1, age, parent, data, stream_offset, stream_index, physical_len)
#
# sha1 is hash of parent + data
# physical_len is the number of bytes of storage used to persist
# the link.
def read_link(in_stream, keep_data=True, pos=None, stream_index=None):
    """ Read a single history link from an open stream. """

    bytes = checked_read(in_stream, LINK_HEADER_LEN, True)
    if bytes == '':
        return None # Clean EOF

    length, age, parent = struct.unpack(LINK_HEADER_FMT, bytes)
    payload_len = length - LINK_HEADER_LEN # already read header
    raw = checked_read(in_stream, payload_len)

    # READFLAG: incrementally read / hash
    sha_value = sha1(str(age))
    sha_value.update(parent)
    sha_value.update(raw)

    if not keep_data:
        raw = None

    return (sha_value.digest(), age, parent, raw,
            pos, stream_index, payload_len)


def copy_raw_links(in_stream, out_stream, allowed_shas, copied_shas):
    """ Copy any links with SHA1 hashes in allowed_shas from in_instream to
        out_stream.
    """
    count = 0
    while True:
        hdr = checked_read(in_stream, LINK_HEADER_LEN, True)
        if hdr == '':
            return count # Clean EOF
        length, age, parent = struct.unpack(LINK_HEADER_FMT, hdr)
        sha_value = sha1(str(age))
        sha_value.update(parent)
        rest = checked_read(in_stream, length - LINK_HEADER_LEN)
        sha_value.update(rest)
        value = sha_value.digest()
        if value in copied_shas:
            continue # Only copy once.

        if allowed_shas is None or value in allowed_shas:
            out_stream.write(hdr)
            out_stream.write(rest)
            count += 1
            copied_shas.add(value)

# Sets pos, but caller must fix stream index
def write_raw_link(out_stream, age, parent, raw_file, stream_index):
    """ Write a history link to an open stream.

        Returns a history link tuple for the link written. """

    assert len(parent) == 20 # Raw, not hex string

    pos = out_stream.tell()
    in_file = open(raw_file, 'rb')
    try:
        raw = in_file.read()

        out_stream.write(struct.pack(LINK_HEADER_FMT,
                                     len(raw) + LINK_HEADER_LEN,
                                     age,
                                     parent))

        sha_value = sha1(str(age))
        sha_value.update(parent)

        out_stream.write(raw)
        # REDFLAG: read / hash incrementally
        sha_value.update(raw)
    finally:
        in_file.close()

    return (sha_value.digest(), age, parent, None,
            pos, stream_index, len(raw) + LINK_HEADER_LEN)

def write_file_manifest(name_map, out_stream):
    """ Write file manifest data to an open stream. """

    out_stream.write(struct.pack(COUNT_FMT, len(name_map)))
    # Sort to make it easier for diff algos to find contiguous
    # changes.
    names = name_map.keys()
    names.sort()
    for name in names:
        length = MANIFEST_ENTRY_HDR_LEN + len(name)
        file_sha, history_sha = name_map[name]

        out_stream.write(struct.pack(MANIFEST_ENTRY_FMT % len(name),
                                     length,
                                     file_sha,
                                     history_sha,
                                     name))
def read_file_manifest(in_stream):
    """ Read file manifest data from an open input stream. """
    count = struct.unpack(COUNT_FMT, checked_read(in_stream, COUNT_LEN))[0]
    name_map = {}
    for dummy in range(0, count):
        length, file_sha, history_sha = \
                struct.unpack(MANIFEST_ENTRY_HDR_FMT,
                              checked_read(in_stream,
                                           MANIFEST_ENTRY_HDR_LEN))

        length -= MANIFEST_ENTRY_HDR_LEN
        name = checked_read(in_stream, length)

        assert not name in name_map
        name_map[name] = (file_sha, history_sha)
    return name_map

def manifest_to_file(file_name, name_map):
    """ Write a single manifest to a file. """
    out_file = open(file_name, 'wb')
    try:
        write_file_manifest(name_map, out_file)
    finally:
        out_file.close()

def manifest_from_file(file_name):
    """ Read a single manifest from a file. """
    in_file = open(file_name, 'rb')
    try:
        return read_file_manifest(in_file)
    finally:
        in_file.close()

def get_file_sha(full_path):
    """ Return the 20 byte sha1 hash digest of a file. """
    in_file = open(full_path, 'rb')
    try:
        # Bug: why doesn't this use sha_func?
        sha_value = sha1()
        while True:
            bytes = in_file.read(READ_CHUNK_LEN)
            if bytes == "":
                break
            sha_value.update(bytes)
        return sha_value.digest()
    finally:
        in_file.close()

