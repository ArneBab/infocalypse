""" Freenet CHK key helper functions.

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

import base64

# Length of the binary rep of a CHK.
CHK_SIZE = 69
# Length of a human readable CHK w/o '/' or filename.
ENCODED_CHK_SIZE = 99

# REDFLAG: Is this correct?
def freenet_base64_encode(data):
    """ INTERNAL: Base64 encode data using Freenet's base64 algo. """
    encoded =  base64.b64encode(data, ['~', '-'])
    length = len(encoded)
    while encoded[length - 1] == '=':
        length -= 1
    return encoded[:length]

# REDFLAG: Is this correct?
def freenet_base64_decode(data):
    """ INTERNAL: Base64 decode data using Freenet's base64 algo. """
    while len(data) % 4 != 0:
        data += '='
    return base64.b64decode(data, ['~', '-'])

def bytes_to_chk(bytes):
    """ Reads the binary representation of a Freenet CHK and returns
    the human readable equivalent. """
    assert len(bytes) == CHK_SIZE

    return 'CHK@' + freenet_base64_encode(bytes[5:37]) + ',' \
           + freenet_base64_encode(bytes[37:69]) + ',' \
           + freenet_base64_encode(bytes[:5])

def chk_to_bytes(chk):
    """ Returns the binary representation of a Freenet CHK."""

    assert chk.startswith('CHK@')
    # NO / or filename allowed.
    assert len(chk) == ENCODED_CHK_SIZE
    fields = chk[4:].split(',')
    assert len(fields) == 3

    # [0, 4] -- control bytes
    # [5, 36] -- routing key
    # [37, 68] -- crypto key
    ret = (freenet_base64_decode(fields[2])
           + freenet_base64_decode(fields[0])
           + freenet_base64_decode(fields[1]))
    assert len(ret) == CHK_SIZE

    return ret

# ATTRIBUTION:
# Based on code from SomeDude's ffp-src-1.1.0.zip
# sha1: b765d05ac320d4c89051740bd575040108db9791  ffp-src-1.1.0.zip
def clear_control_bytes(key):
    """ Returns a CHK with the control bytes cleared.

        This is used to fetch raw Freenet metadata.

        REQUIRES: key is a CHK key.
    """

    if not key.startswith('CHK@'):
        raise ValueError("Only works for CHK keys.")
    fields = key.split('/')
    key_fields = fields[0].split(',')

    bytes = freenet_base64_decode(key_fields[2])

    # Hmmm... ok since it is very short
    bytes = bytes[:2] + '\x00' + bytes[3:]
    ret = key_fields[0] + ','  + key_fields[1] + ',' \
          + freenet_base64_encode(bytes)

    # REDFLAG: different.  was there really a bug in somedudes code???
    if len(fields) > 1:
        for index in range(1, len(fields)):
            ret += '/' + fields[index]
    return ret

