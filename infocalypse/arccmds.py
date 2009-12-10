""" Implementation of fn-archive command.

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

from mercurial import util

from fcpclient import get_version, get_usk_for_usk_version, is_usk_file, is_usk

from config import Config
from infcmds import setup, do_key_setup, is_redundant, run_until_quiescent
from updatesm import QUIESCENT, FINISHING
from archivesm import create_dirs, ArchiveUpdateContext, \
     start_inserting_blocks, start_requesting_blocks, cleanup_dirs, \
     ARC_INSERTING_URI, ARC_REQUESTING_URI, ARC_CACHING_TOPKEY

from arclocal import local_create, local_synch, local_update, local_reinsert


def arc_cleanup(update_sm, top_key_state=None):
    """ INTERNAL: Cleanup after running an archive command. """

    if update_sm is None:
        return

    # Cleanup archive temp files.
    top_key = None
    if not top_key_state is None:
        top_key = update_sm.get_state(top_key_state).get_top_key_tuple()

    ctx = update_sm.ctx
    if (not ctx is None and
        'ARCHIVE_CACHE_DIR' in update_sm.ctx and
        'REQUEST_URI' in update_sm.ctx):
        cleanup_dirs(ctx.ui_,
                     ctx['ARCHIVE_CACHE_DIR'],
                     ctx['REQUEST_URI'],
                     top_key)

    # Previous cleanup code.
    if not update_sm.runner is None:
        update_sm.runner.connection.close()

    if not update_sm.ctx.bundle_cache is None:
        update_sm.ctx.bundle_cache.remove_files() # Unreachable???


def arc_handle_updating_config(update_sm, params, stored_cfg,
                               is_pulling=False):
    """ INTERNAL: Write updates into the config file IFF the previous
        command succeeded. """

    base_dir = params['ARCHIVE_CACHE_DIR']

    if not is_pulling:
        if not update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            return

        if (params['INSERT_URI'] is None or # <- re-insert w/o insert uri
            not is_usk_file(params['INSERT_URI'])):
            return

        inverted_uri = params['INVERTED_INSERT_URI']

        # Cache the request_uri - insert_uri mapping.
        stored_cfg.set_insert_uri(inverted_uri, update_sm.ctx['INSERT_URI'])

        # Cache the updated index for the insert.
        version = get_version(update_sm.ctx['INSERT_URI'])
        stored_cfg.update_index(inverted_uri, version)
        stored_cfg.update_dir(base_dir, inverted_uri)

        # Hmmm... if we wanted to be clever we could update the request
        # uri too when it doesn't match the insert uri. Ok for now.
        # Only for usks and only on success.
        #print "UPDATED STORED CONFIG(0)"
        Config.to_file(stored_cfg)

    else:
        # Only finishing required. same. REDFLAG: look at this again
        if not update_sm.get_state(QUIESCENT).arrived_from((FINISHING,)):
            return

        if not is_usk(params['REQUEST_URI']):
            return

        state = update_sm.get_state(ARC_REQUESTING_URI)
        updated_uri = state.get_latest_uri()
        version = get_version(updated_uri)
        stored_cfg.update_index(updated_uri, version)
        stored_cfg.update_dir(base_dir, updated_uri)
        #print "UPDATED STORED CONFIG(1)"
        Config.to_file(stored_cfg)


def execute_arc_create(ui_, params, stored_cfg):
    """ Create a new incremental archive. """
    update_sm = None
    top_key_state = None
    try:
        assert 'ARCHIVE_CACHE_DIR' in params
        assert 'FROM_DIR' in params
        update_sm = setup(ui_, None, params, stored_cfg)
        request_uri, dummy = do_key_setup(ui_, update_sm, params, stored_cfg)
        create_dirs(ui_, params['ARCHIVE_CACHE_DIR'], request_uri)
        ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params['INSERT_URI']),
                                            params['INSERT_URI']))

        # Create the local blocks.
        files, top_key = local_create(params['ARCHIVE_CACHE_DIR'],
                                      request_uri,
                                      params['FROM_DIR'])

        for block in top_key[0]:
            if block[1][0] == 'CHK@':
                ui_.status("Created new %i byte block.\n" % block[0])

        # Insert them into Freenet.
        ctx = ArchiveUpdateContext(update_sm, ui_)
        ctx.update({'REQUEST_URI':request_uri,
                    'INSERT_URI':params['INSERT_URI'],
                    'ARCHIVE_CACHE_DIR':params['ARCHIVE_CACHE_DIR'],
                    'PROVISIONAL_TOP_KEY':top_key,
                    'ARCHIVE_BLOCK_FILES':files})

        start_inserting_blocks(update_sm, ctx)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Inserted to:\n%s\n" %
                       '\n'.join(update_sm.get_state(ARC_INSERTING_URI).
                                 get_request_uris()))
            top_key_state = ARC_INSERTING_URI
        else:
            ui_.status("Archive create failed.\n")

        arc_handle_updating_config(update_sm, params, stored_cfg)
    finally:
        arc_cleanup(update_sm, top_key_state)

def execute_arc_pull(ui_, params, stored_cfg):
    """ Update from an existing incremental archive in Freenet. """
    update_sm = None
    top_key_state = None
    try:
        assert 'ARCHIVE_CACHE_DIR' in params
        assert not params['REQUEST_URI'] is None
        if not params['NO_SEARCH'] and is_usk_file(params['REQUEST_URI']):
            index = stored_cfg.get_index(params['REQUEST_URI'])
            if not index is None:
                if index >= get_version(params['REQUEST_URI']):
                    # Update index to the latest known value
                    # for the --uri case.
                    params['REQUEST_URI'] = get_usk_for_usk_version(
                        params['REQUEST_URI'], index)
                else:
                    ui_.status(("Cached index [%i] < index in USK [%i].  "
                                + "Using the index from the USK.\n"
                                + "You're sure that index exists, right?\n") %
                               (index, get_version(params['REQUEST_URI'])))

        update_sm = setup(ui_, None, params, stored_cfg)
        ui_.status("%sRequest URI:\n%s\n" % (
            is_redundant(params['REQUEST_URI']),
            params['REQUEST_URI']))

        # Pull changes into the local block cache.
        ctx = ArchiveUpdateContext(update_sm, ui_)
        ctx.update({'REQUEST_URI':params['REQUEST_URI'],
                    'ARCHIVE_CACHE_DIR':params['ARCHIVE_CACHE_DIR']})
        start_requesting_blocks(update_sm, ctx)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            uri = update_sm.get_state(ARC_REQUESTING_URI).get_latest_uri()
            blocks = update_sm.get_state(ARC_CACHING_TOPKEY).get_blocks()
            plural = ''
            if len(blocks) != 1:
                plural = 's'
            ui_.status("Fetched %i bytes in %i CHK%s from:\n%s\n" %
                       (sum([block[0] for block in blocks]),
                        len(blocks), plural, uri))
            ui_.status("Updating local directory...\n")
            local_synch(ui_,
                        params['ARCHIVE_CACHE_DIR'],
                        # Use the updated URI below so we get the
                        # right cached topkey.
                        uri,
                        params['TO_DIR'])
            top_key_state = ARC_REQUESTING_URI
        else:
            ui_.status("Synchronize failed.\n")

        arc_handle_updating_config(update_sm, params, stored_cfg, True)
    finally:
        arc_cleanup(update_sm, top_key_state)


def execute_arc_push(ui_, params, stored_cfg):
    """ Push an update into an incremental archive in Freenet. """
    assert params.get('REQUEST_URI', None) is None # REDFLAG: why ?
    update_sm = None
    top_key_state = None
    try:
        update_sm = setup(ui_, None, params, stored_cfg)
        request_uri, dummy_is_keypair = do_key_setup(ui_, update_sm, params,
                                                     stored_cfg)
        create_dirs(ui_, params['ARCHIVE_CACHE_DIR'], request_uri)
        ui_.status("%sInsert URI:\n%s\n" % (is_redundant(params['INSERT_URI']),
                                            params['INSERT_URI']))


        # Update the local archive.
        files, top_key = local_update(params['ARCHIVE_CACHE_DIR'],
                                      request_uri,
                                      params['FROM_DIR'])

        if files is None:
            raise util.Abort("There are no local changes to add.")

        for block in top_key[0]:
            if block[1][0] == 'CHK@':
                ui_.status("Created new %i byte block.\n" % block[0])

        # Insert them into Freenet.
        ctx = ArchiveUpdateContext(update_sm, ui_)
        ctx.update({'REQUEST_URI':request_uri,
                    'INSERT_URI':params['INSERT_URI'],
                    'ARCHIVE_CACHE_DIR':params['ARCHIVE_CACHE_DIR'],
                    'PROVISIONAL_TOP_KEY':top_key,
                    'ARCHIVE_BLOCK_FILES':files})

        start_inserting_blocks(update_sm, ctx)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Inserted to:\n%s\n" %
                       '\n'.join(update_sm.get_state(ARC_INSERTING_URI).
                                 get_request_uris()))
            top_key_state = ARC_INSERTING_URI
        else:
            ui_.status("Push to archive failed.\n")

        arc_handle_updating_config(update_sm, params, stored_cfg)
    finally:
        arc_cleanup(update_sm, top_key_state)

def execute_arc_reinsert(ui_, params, stored_cfg):
    """ Reinsert the archive into Freenet. """
    assert not params.get('REQUEST_URI', None) is None
    assert params.get('REINSERT_LEVEL', 0) > 0

    update_sm = None
    try:
        update_sm = setup(ui_, None, params, stored_cfg)
        request_uri, dummy_is_keypair = do_key_setup(ui_, update_sm, params,
                                                     stored_cfg)
        create_dirs(ui_, params['ARCHIVE_CACHE_DIR'], request_uri)

        ui_.status("%sRequest URI:\n%s\n" % (is_redundant(request_uri),
                                             request_uri))

        # Get the blocks to re-insert.
        files, top_key = local_reinsert(params['ARCHIVE_CACHE_DIR'],
                                        request_uri)

        # Tell the user about them.
        for block in top_key[0]:
            if block[1][0] == 'CHK@':
                ui_.status("Re-inserting %i byte block.\n" % block[0])

        # Start re-inserting them.
        ctx = ArchiveUpdateContext(update_sm, ui_)
        ctx.update({'REQUEST_URI':request_uri,
                    'INSERT_URI':params['INSERT_URI'],
                    'ARCHIVE_CACHE_DIR':params['ARCHIVE_CACHE_DIR'],
                    'PROVISIONAL_TOP_KEY':top_key,
                    'ARCHIVE_BLOCK_FILES':files,
                    'REINSERT':params['REINSERT_LEVEL']})

        start_inserting_blocks(update_sm, ctx)
        run_until_quiescent(update_sm, params['POLL_SECS'])

        if update_sm.get_state(QUIESCENT).arrived_from(((FINISHING,))):
            ui_.status("Re-insert finished.\n")
        else:
            ui_.status("Re-insert failed.\n")

        arc_handle_updating_config(update_sm, params, stored_cfg)
    finally:
        arc_cleanup(update_sm, None) # Don't prune cache.
