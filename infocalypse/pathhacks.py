""" Keep hacks in one place.

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

import os
import sys

# REDFLAG: do better
# HACK: but at least it's explict and encapsulated.
def add_parallel_sys_path(dir_name):
    """ Add the directory dir_name to the sys.path.

        REQUIRES: dir_name is in a directory parallel to
        the this module.
    """
    target_dir = os.path.abspath(os.path.join(os.path.dirname(
        os.path.dirname(__file__)),
                                              dir_name))
    if not target_dir in sys.path:
        #print "ADDED: ", target_dir
        sys.path.append(target_dir)
