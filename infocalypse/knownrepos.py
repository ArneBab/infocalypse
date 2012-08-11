""" Information about known Infocalypse repositories.

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

    If you maintain a repository that doesn't contain illicit content
    let me know and I'll add it.
"""

from fcpclient import get_usk_hash

# LATER: remove this file
# djk20110918:  Bad out of date info is worse than none at all.
INFOCALYPSE_INDEX = 20

KNOWN_REPOS = (
    ('djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks',
     'USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
     + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,AQACAAE/'
     + 'wiki_hacking.R1/%i' % INFOCALYPSE_INDEX), # This code.
    )


def build_trust_list(id_usk_list):
    """ INTERNAL: Compile the default trust map from a list of
        (trusted_fms_id, USK) tuples. """
    table = {}
    for fms_id, usk in id_usk_list:
        hashes = table.get(fms_id, [])
        usk_hash = get_usk_hash(usk)
        if not usk_hash in hashes:
            hashes.append(usk_hash)
        table[fms_id] = hashes
    for fms_id in table.keys()[:]:
        table[fms_id] = tuple(table[fms_id])
    return table

# fms_id -> (usk_hash0, ..., usk_hashn) map
DEFAULT_TRUST = build_trust_list(KNOWN_REPOS)

DEFAULT_GROUPS = ('infocalypse.notify', )
DEFAULT_NOTIFICATION_GROUP = 'infocalypse.notify'
