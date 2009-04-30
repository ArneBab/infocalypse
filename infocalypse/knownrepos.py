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

# Not sure about this. Flat text file instead?
KNOWN_REPOS = (
    ('djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks',
     'USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
    + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,AQACAAE/'
    + 'infocalypse.hgext.R1/23'),
    )

# LATER: Compile from KNOWN_REPOS? To risky?
DEFAULT_TRUST = {
    'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks':
    ('be68e8feccdd', ),
    }

DEFAULT_GROUPS = ('infocalypse.notify', )
DEFAULT_NOTIFICATION_GROUP = 'infocalypse.notify'
