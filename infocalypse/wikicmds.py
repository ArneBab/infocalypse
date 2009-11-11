""" Implementation of experiment commands for wikis over freenet.

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

#------------------------------------------------------------
# REDFLAG: DCI path hacks
import validate
ADD_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.dirname(validate.__file__))),
                       'clean_piki')
sys.path.append(ADD_DIR)
#------------------------------------------------------------

from servepiki import serve_wiki, create_empty_wiki

from mercurial import util

from config import write_default_config

# REDFLAG: DCI path hacks
# piki's required files are in that directory.
import servepiki
PIKI_WWW_SRC = os.path.dirname(servepiki.__file__)

def execute_wiki(ui_, repo, params):
    """ Run the wiki command. """
    def out_func(text):
        """ Helper displays output from serve_wiki via ui.status. """
        ui_.status(text + '\n')
    if params['WIKI'] == 'run':
        if not os.path.exists(os.path.join(repo.root, 'fnwiki.cfg')):
            raise util.Abort("Can't read fnwiki.cfg. Did you forget hg " +
                             "fn-wiki --createconfig?")
        serve_wiki(params['HTTP_PORT'], params['HTTP_BIND'], out_func)
        return

    # Hmmmm... some basic UI depends on wikitext. not sure
    # how useful a completely empty wiki is.
    if params['WIKI'] == 'createconfig':
        if os.path.exists(os.path.join(repo.root, 'fnwiki.cfg')):
            raise util.Abort("fnwiki.cfg already exists!")

        if os.path.exists(os.path.join(repo.root, 'wiki_root')):
            raise util.Abort("The wiki_root subdirectory already exists! " +
                             "Move it out of the way to continue.")

        create_empty_wiki(os.path.join(repo.root, 'wiki_root'),  PIKI_WWW_SRC)
        ui_.status("Created skeleton wiki_root dir.\n")
        write_default_config(ui_, repo, True)
        return

    raise util.Abort("Unsupported subcommand: " + params.get('WIKI', 'unknown'))

