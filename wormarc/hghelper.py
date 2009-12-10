""" Testing helper functions for using hg repos.

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

from mercurial import hg, commands, ui

#{'rev': '0', 'no_decode': None, 'prefix': '', 'exclude': [],
# 'include': [], 'type': ''}
def export_hg_repo(src_dir, dest_dir, target_rev):
    """ Export the files in the hg repo in src_dir to dest_dir. """
    ui_ = ui.ui()
    repo = hg.repository(ui_, src_dir)
    commands.archive(ui_,
                     repo,
                     dest_dir,
                     rev=target_rev,
                     prefix='' # <- needs this to work.
                     )
    return repo['tip'].rev()


