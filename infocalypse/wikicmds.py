""" Implementation of experimental commands for wikis over freenet.

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
import StringIO

from binascii import hexlify

from mercurial import util

from config import write_default_config, read_freesite_cfg, normalize
from submission import bundle_wikitext, unbundle_wikitext, get_info, \
     NoChangesError, validate_wikitext
from hgoverlay import HgFileOverlay
from infcmds import setup, run_until_quiescent, cleanup
from statemachine import StatefulRequest
from fcpmessage import PUT_FILE_DEF, GET_DEF
from graph import FREENET_BLOCK_LEN, has_version
from updatesm import QUIESCENT, FINISHING, RUNNING_SINGLE_REQUEST
from bundlecache import make_temp_file
# HACK
from pathhacks import add_parallel_sys_path
add_parallel_sys_path('fniki')

import servepiki
from piki import create_default_wiki

from fileoverlay import get_file_funcs

def execute_wiki(ui_, repo, params):
    """ Run the wiki command. """
    def out_func(text):
        """ Helper displays output from serve_wiki via ui.status. """
        ui_.status(text + '\n')
    if params['WIKI'] == 'run':
        ui_.status("wikitext version: %s\n" % get_hg_version(repo)[:12])
        if not os.path.exists(os.path.join(repo.root, 'fnwiki.cfg')):
            raise util.Abort("Can't read fnwiki.cfg. Did you forget hg " +
                             "fn-wiki --createconfig?")

        servepiki.serve_wiki(params['HTTP_PORT'], params['HTTP_BIND'], out_func)
        return

    # Hmmmm... some basic UI depends on wikitext. not sure
    # how useful a completely empty wiki is.
    if params['WIKI'] == 'createconfig':
        if os.path.exists(os.path.join(repo.root, 'fnwiki.cfg')):
            raise util.Abort("fnwiki.cfg already exists!")

        if os.path.exists(os.path.join(repo.root, 'wiki_root')):
            raise util.Abort("The wiki_root subdirectory already exists! " +
                             "Move it out of the way to continue.")

        create_default_wiki(os.path.join(repo.root, 'wiki_root'))
        ui_.status("Created skeleton wiki_root dir.\n")
        write_default_config(ui_, repo, True)
        return

    raise util.Abort("Unsupported subcommand: " + params.get('WIKI', 'unknown'))

def get_hg_version(repo):
    """ Return the 40 digit hex string for the current hg version. """
    heads = repo.heads()
    if len(heads) > 1:
        raise util.Abort("Multiple heads!")

    parents = [cp.node() for cp in repo[None].parents()]
    if len(parents) > 1:
        raise util.Abort("Multiple parents!")
    if heads[0] != parents[0]:
        raise util.Abort("Working directory is not the head! Run hg update.")

    return hexlify(heads[0])
# LATER: better error message when there's no OVERLAY dir?
def execute_wiki_submit(ui_, repo, params, stored_cfg):
    """ Insert and overlayed wiki change submission CHK into freenet and
        return a notification message string. """
    update_sm = None
    try:
        # Read submitter out of stored_cfg
        submitter = stored_cfg.defaults.get('FMS_ID', None)
        assert not submitter is None
        assert submitter.find('@') == -1

        # Get version, i.e. just the hg parent == hg head
        version = get_hg_version(repo)

        params['ISWIKI'] = True
        read_freesite_cfg(ui_, repo, params, stored_cfg)
        if not params.get('OVERLAYED', False):
            raise util.Abort("Can't submit from non-overlayed wiki edits!")
        if not params.get('CLIENT_WIKI_GROUP', None):
            # DCI: test code path
            raise util.Abort("No wiki_group in fnwiki.cfg. Don't " +
                             "know where to post to!")

        ui_.status("\nPreparing to submit to %s FMS group as %s.\n" %
                   (params['CLIENT_WIKI_GROUP'], submitter))

        # Create submission zip file in RAM.
        overlay = get_file_funcs(os.path.join(repo.root, params['WIKI_ROOT']),
                                 True)
        try:
            raw_bytes = bundle_wikitext(overlay, version, submitter)
        except NoChangesError:
            raise util.Abort("There are no overlayed changes to submit.")
        # Punt if it's too big.
        if len(raw_bytes) >= FREENET_BLOCK_LEN:
            raise util.Abort("Too many changes. Change .zip must be <32K")

        update_sm = setup(ui_, repo, params, stored_cfg)

        # Make an FCP file insert request which will run on the
        # on the state machine.
        request = StatefulRequest(update_sm)
        request.tag = 'submission_zip_insert'
        request.in_params.definition = PUT_FILE_DEF
        request.in_params.fcp_params = update_sm.params.copy()
        request.in_params.fcp_params['URI'] = 'CHK@'
        request.in_params.send_data = raw_bytes

        ui_.status("Inserting %i byte submission CHK...\n" % len(raw_bytes))
        update_sm.start_single_request(request)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        heads = [hexlify(head) for head in repo.heads()]
        heads.sort()
        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            chk = update_sm.get_state(RUNNING_SINGLE_REQUEST).\
                  final_msg[1]['URI']
            ui_.status("Patch CHK:\n%s\n" %
                       chk)
            # ':', '|' not in freenet base64
            # DCI: why normalize???
            # (usk_hash, base_version, chk, length)
            ret = ':'.join(('W',
                            normalize(params['REQUEST_URI']),
                            version[:12],
                            chk,
                            str(len(raw_bytes))))

            ui_.status("\nNotification:\n%s\n" % ret
                        + '\n')

            return ret, params['CLIENT_WIKI_GROUP']

        raise util.Abort("Submission CHK insert failed.")

    finally:
        # Cleans up out file.
        cleanup(update_sm)

def execute_wiki_apply(ui_, repo, params, stored_cfg):
    """ Fetch a wiki change submission CHK and apply it to a local
        directory. """
    update_sm = None
    try:
        assert 'REQUEST_URI' in params
        # Get version, i.e. just the hg parent == hg head
        version = get_hg_version(repo)

        # Get target directory.
        params['ISWIKI'] = True
        read_freesite_cfg(ui_, repo, params, stored_cfg)

        update_sm = setup(ui_, repo, params, stored_cfg)

        # Make an FCP download request which will run on the
        # on the state machine.
        request = StatefulRequest(update_sm)
        request.tag = 'submission_zip_request'
        request.in_params.definition = GET_DEF # To RAM.
        request.in_params.fcp_params = update_sm.params.copy()
        request.in_params.fcp_params['URI'] = params['REQUEST_URI']
        # Knee high barrier against abuse.
        request.in_params.fcp_params['MaxSize'] = FREENET_BLOCK_LEN

        ui_.status("Requesting wiki submission from...\n%s\n" %
                   params['REQUEST_URI'])
        update_sm.start_single_request(request)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            raw_bytes = update_sm.get_state(RUNNING_SINGLE_REQUEST).\
                        final_msg[2]
            assert request.response[0] == 'AllData'
            ui_.status("Fetched %i byte submission.\n" % len(raw_bytes))
            base_ver, submitter = get_info(StringIO.StringIO(raw_bytes))
            ui_.status("Base version: %s, Submitter: %s (unverifiable!)\n"
                       % (base_ver[:12], submitter))

            #print "H_ACKING base_ver to test exception!"
            #base_ver = 'da2f653c5c47b7ee7a814e668aa1d63c50c3a4f3'
            if not has_version(repo, base_ver):
                ui_.warn("That version isn't in the local repo.\n" +
                         "Try running hg fn-pull --aggressive.\n")
                raise util.Abort("%s not in local repo" % base_ver[:12])

            if base_ver != version:
                ui_.warn("Version mismatch! You might have to " +
                         "manually merge.\n")

            # Set up an IFileFunctions that reads the correct versions of
            # the unpatched files out of Mercurial.
            overlay = HgFileOverlay(ui_, repo,
                                    # i.e. "<>/wiki_root" NOT "
                                    # <>/wiki_root/wikitext"
                                    os.path.join(repo.root,
                                                 params['WIKI_ROOT']),
                                    # cleanup() in finally deletes this.
                                    make_temp_file(update_sm.ctx.
                                                   bundle_cache.base_dir))
            overlay.version = base_ver
            validate_wikitext(overlay)
            updates = unbundle_wikitext(overlay,
                                        StringIO.StringIO(raw_bytes))
            for index, label in enumerate(('CREATED', 'MODIFIED', 'REMOVED',
                                           'ALREADY PATCHED')):
                if len(updates[index]) > 0:
                    values = list(updates[index])
                    values.sort()
                    ui_.status('%s:\n%s\n' % (label, '\n'.join(values)))

    finally:
        cleanup(update_sm)
