#!/usr/bin/env python

# Need to suppress all exceptions.
# pylint: disable-msg=W0702

""" Print an fms/freemail signature string with repo update information
    readable by fn-fmsread.

    Copyright (C) 2009 Darrell Karbott

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public
    License as published by the Free Software Foundation; either
    version 2.0 of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

    Author: djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks

    This program has no command line interface.
    You must modify the variables at the top of the file.
"""
import os

from fcpclient import get_usk_hash
from config import Config, DEFAULT_CFG_PATH
from fms import to_msg_string

# The maximum number of updates to include.
MAX_UPDATES = 4 #  == 67 chars

# The full path to your .infocalypse / infocalypse.ini
# file. Should work for the default location.
CFG_PATH = os.path.expanduser(DEFAULT_CFG_PATH)

# The static part of your sig message with no trailing '\n'
STATIC_TEXT = ('Incremental hg repos in Freenet (Not pyfreenethg!):\n'
               + 'USK@-bk9znYylSCOEDuSWAvo5m72nUeMxKkDmH3nIqAeI-0,'
               + 'qfu5H3FZsZ-5rfNBY-jQHS5Ke7AT2PtJWd13IrPZjcg,'
               + 'AQACAAE/feral_codewright/12/infocalypse_howto.html')

# Your repo Request (not Insert!) URIs go here:
#
# The versions don't matter, they are read from your .infocalpse file.
# Hmmm... using request uris means you can broadcast information about
# repos you have pulled but didn't insert.
REPO_USKS = ('USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
             + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,'
             + 'AQACAAE/fred_staging.R1/1',
             'USK@kRM~jJVREwnN2qnA8R0Vt8HmpfRzBZ0j4rHC2cQ-0hw,'
             + '2xcoQVdQLyqfTpF2DpkdUIbHFCeL4W~2X1phUYymnhM,'
             + 'AQACAAE/infocalypse.hgext.R1/12',
             'USK@EbQbLWtWLRBgQl4Ly-SjQJvzADdJPfIXNQfCbKzgCFI,'
             + 'XDLYQTC0nYD4rhIIP~Ff~itkvVVF2u4WU8YVSL2f5RA,'
             +'AQACAAE/collaborate.R1/1'
             )

USK_HASHES = tuple([get_usk_hash(usk) for usk in REPO_USKS])

def print_updates():
    """ Print a sig message with embedded update strings or nothing
        at all if there's an error. """
    try:
        stored_cfg = Config.from_file(CFG_PATH)
        updates = []
        for usk_hash in USK_HASHES:
            index = stored_cfg.get_index(usk_hash)
            if index is None:
                # Uncomment this and run from the command line if
                # you get no output.
                #print "No stored index for usk hash: ", usk_hash
                continue
            updates.append((usk_hash, index))
        updates.sort()
        # Hmmm... silently truncate
        updates = updates[:MAX_UPDATES]
        if len(updates) > 0:
            print STATIC_TEXT
            print to_msg_string(updates, None, ':')
    except:
        # Fail silently, rather than spewing garbage into sig.
        return

if __name__ == "__main__":
    print_updates()
