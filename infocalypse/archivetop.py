"""
Helper functions used to read and write the binary rep. of incremental
archive top keys.

Eccentricities:
0) There is a salt byte whose only purpose is to perturb the hash
   value of the binary rep.
1) There are multiple CHKS per block.

This allows us to do top key redundancy like in Infocalypse.
"""

import struct

from fcpconnection import sha1_hexdigest
from chk import CHK_SIZE, bytes_to_chk, chk_to_bytes
from topkey import default_out

MAJOR_VERSION = '01'
MINOR_VERSION = '02'

HDR_VERSION = MAJOR_VERSION + MINOR_VERSION
HDR_PREFIX = 'WORM'
HDR_BYTES = HDR_PREFIX + HDR_VERSION

# Header length 'WORM0100'
HDR_SIZE = 8
assert len(HDR_BYTES) == HDR_SIZE

KNOWN_VERS = (HDR_BYTES, )
EXPECTED_VER = KNOWN_VERS[-1]

# <header><salt_byte><age><num_blocks><num_root_objects>
BASE_FMT = "!%isBIBB" % HDR_SIZE
BASE_LEN = struct.calcsize(BASE_FMT)

# <root_object> = <20 byte SHA1><kind>
ROOT_OBJ_FMT = "!20sI"
ROOT_OBJ_LEN = struct.calcsize(ROOT_OBJ_FMT)

BLOCK_BASE_FMT = "!qLB"
BLOCK_BASE_LEN = struct.calcsize(BLOCK_BASE_FMT)

def check_version(version):
    """ Raises a ValueError if the format version isn't parsable. """
    if version != EXPECTED_VER:
        raise ValueError("Can't parse format version. Saw: %s, expected: %s" %
                         (version, EXPECTED_VER))


def bytes_to_top_key_tuple(bytes):
    """ Writes the binary rep of an archive top key.

        Where top_key_tuple is:
        ( ((length, (chk,), max_age), .. ), ((root_sha, kind), ..), age )
    """

    if len(bytes) < BASE_LEN:
        raise ValueError("Not enough data to parse static fields.")

    if not bytes.startswith(HDR_PREFIX):
        raise ValueError("Doesn't look like %s top key binary data." %
                         HDR_PREFIX)


    version, salt, age, num_blocks, num_obs = struct.unpack(BASE_FMT,
                                                            bytes[:BASE_LEN])

    check_version(version)

    bytes = bytes[BASE_LEN:]

    blocks = []
    for dummy0 in range(0, num_blocks):
        length, age, chk_count = struct.unpack(BLOCK_BASE_FMT,
                                               bytes[:BLOCK_BASE_LEN])
        bytes = bytes[BLOCK_BASE_LEN:]

        chks = []
        for dummy1 in range(0, chk_count):
            chks.append(bytes_to_chk(bytes[:CHK_SIZE]))
            bytes = bytes[CHK_SIZE:]

        blocks.append((length, tuple(chks), age))


    root_objs = []
    for dummy2 in range(0, num_obs):
        root_objs.append(struct.unpack(ROOT_OBJ_FMT,
                                       bytes[:ROOT_OBJ_LEN]))
        bytes = bytes[ROOT_OBJ_LEN:]

    return ((tuple(blocks), tuple(root_objs), age), salt)

def top_key_tuple_to_bytes(values, salt_byte=0):
    """ Reads the binary rep of an archive top key. """

    ret = struct.pack(BASE_FMT, HDR_BYTES,
                      salt_byte,
                      values[2],
                      len(values[0]),
                      len(values[1]))

    for block in values[0]:
        ret += struct.pack(BLOCK_BASE_FMT,
                           block[0], block[2], len(block[1]))
        for chk in block[1]:
            ret += chk_to_bytes(chk)

    for obj in values[1]:
        ret += struct.pack(ROOT_OBJ_FMT,
                           obj[0],
                           obj[1])
    return ret

def dump_top_key_tuple(top_key_tuple, out_func=default_out):
    """ Debugging function to print a top_key_tuple. """
    out_func("--- %s top key tuple---\n" % HDR_BYTES)
    out_func("age: %i\n" % top_key_tuple[2])
    for index, block in enumerate(top_key_tuple[0]):
        out_func("block[%i]\n   len: %i max age: %i\n" %
                 (index, block[0], block[2]))
        for chk in block[1]:
            out_func("   %s\n" % chk)
    for index, obj in enumerate(top_key_tuple[1]):
        out_func("root_sha[%i]: %s %i\n" % (index, sha1_hexdigest(obj[0]),
                                                                  obj[1]))

    out_func("---\n")

