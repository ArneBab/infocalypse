""" Helper functions to validate input from fms and config file.

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

HEX_CHARS = frozenset(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                       'a', 'b', 'c', 'd', 'e', 'f'])

# Really no library function to do this?
# REQUIRES: Lowercase!
def is_hex_string(value, length=12):
    """ Returns True if value is a lowercase hex digit string,
        False otherwise. """
    if not length is None:
        if len(value) != length:
            return False

    for char in value:
        if not char in HEX_CHARS:
            return False
    return True

# http://wiki.freenetproject.org/Base64
FREENET_BASE64_CHARS = frozenset(
    [ chr(c) for c in
      (range(ord('0'), ord('9') + 1)
       + range(ord('a'), ord('z') + 1)
       + range(ord('A'), ord('Z') + 1)
       + [ord('~'), ord('-')])
      ])

def is_fms_id(value):
    """ Returns True if value looks like a plausible FMS id."""
    fields = value.split('@')
    if len(fields) != 2:
        return False

    # REDFLAG: Faster way? Does it matter?
    for character in fields[1]:
        if not character in FREENET_BASE64_CHARS:
            return False

    return True


