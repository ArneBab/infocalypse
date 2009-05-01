""" Implementation of fms update and notification commands for
    Infocalypse mercurial extension.

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

from fcpclient import get_usk_hash

from fms import USKAnnouncementParser, USKIndexUpdateParser, recv_msgs, \
     to_msg_string, MSG_TEMPLATE, send_msgs

from config import Config
from infcmds import do_key_setup, setup, cleanup

def handled_list(ui_, params, stored_cfg):
    """ INTERNAL: Helper function to simplify execute_fmsread. """
    if params['FMSREAD'] != 'list' and params['FMSREAD'] != 'listall':
        return False

    if params['FMSREAD'] == 'listall':
        parser = USKAnnouncementParser(None, True)
        if params['VERBOSITY'] >= 2:
            ui_.status('Listing all repo USKs.\n')
    else:
        trust_map = stored_cfg.fmsread_trust_map.copy() # paranoid copy
        if params['VERBOSITY'] >= 2:
            fms_ids = trust_map.keys()
            fms_ids.sort()
            ui_.status(("Only listing repo USKs from trusted "
                            + "fms IDs:\n%s\n\n") % '\n'.join(fms_ids))
        parser = USKAnnouncementParser(trust_map, True)

    recv_msgs(stored_cfg.defaults['FMS_HOST'],
              stored_cfg.defaults['FMS_PORT'],
              parser,
              stored_cfg.fmsread_groups)

    if len(parser.usks) == 0:
        ui_.status("No USKs found.\n")
        return True

    ui_.status("\n")
    for usk in parser.usks:
        usk_entry = parser.usks[usk]
        ui_.status("USK Hash: %s\n%s\n%s\n\n" %
                   (get_usk_hash(usk), usk,
                    '\n'.join(usk_entry)))

    return True

def dump_trust_map(ui_, params, trust_map):
    """ Show verbose trust map information. """
    if params['VERBOSITY'] < 2:
        return

    if not params['REQUEST_URI'] is None:
        ui_.status("USK Hash: %s\n" % get_usk_hash(params['REQUEST_URI']))
    fms_ids = trust_map.keys()
    fms_ids.sort()
    ui_.status("Update Trust Map:\n")
    for fms_id in fms_ids:
        ui_.status("   %s: %s\n" % (fms_id,
                                    ' '.join(trust_map[fms_id])))
    ui_.status("\n")

def execute_fmsread(ui_, params, stored_cfg):
    """ Run the fmsread command. """
    if params['VERBOSITY'] >= 2:
        ui_.status(('Connecting to fms on %s:%i\n'
                    + 'Searching groups: %s\n') %
                   (stored_cfg.defaults['FMS_HOST'],
                    stored_cfg.defaults['FMS_PORT'],
                    ' '.join(stored_cfg.fmsread_groups)))

    # Listing announced Repo USKs
    if handled_list(ui_, params, stored_cfg):
        return

    # Updating Repo USK indices for repos which are
    # listed int the fmsread_trust_map section of the
    # config file.
    trust_map = stored_cfg.fmsread_trust_map.copy() # paranoid copy

    dump_trust_map(ui_, params, trust_map)

    ui_.status("Raking through fms messages. This make take a while...\n")
    parser = USKIndexUpdateParser(trust_map, True)
    recv_msgs(stored_cfg.defaults['FMS_HOST'],
              stored_cfg.defaults['FMS_PORT'],
              parser,
              stored_cfg.fmsread_groups)
    changed = parser.updated(stored_cfg.version_table)

    if params['VERBOSITY'] >= 2:
        if parser.untrusted and len(parser.untrusted) > 0:
            text = 'Skipped Untrusted Updates:\n'
            for usk_hash in parser.untrusted:
                text += usk_hash + ':\n'
                fms_ids = parser.untrusted[usk_hash]
                for fms_id in fms_ids:
                    text += '   ' + fms_id + '\n'
            text += '\n'
            ui_.status(text)

    if len(changed) == 0:
        ui_.status('No updates found.\n')
        return

    # Back map to uris ? Can't always do it.
    if len(changed) > 0:
        text = 'Updates:\n'
        for usk_hash in changed:
            text += '%s:%i\n' % (usk_hash, changed[usk_hash])
        ui_.status(text)
        if ((not params['REQUEST_URI'] is None) and
            get_usk_hash(params['REQUEST_URI']) in changed):
            ui_.status("Current repo has update to index %s.\n" %
                       changed[get_usk_hash(params['REQUEST_URI'])])

    if params['DRYRUN']:
        ui_.status('Exiting without saving because --dryrun was set.\n')
        return

    for usk_hash in changed:
        stored_cfg.update_index(usk_hash, changed[usk_hash])

    Config.to_file(stored_cfg)
    ui_.status('Saved updated indices.\n')


# REDFLAG: Catch this in config when depersisting?
def is_none(value):
    """ Return True if value is None or 'None',  False otherwise. """
    return value is None or value == 'None'

def execute_fmsnotify(ui_, repo, params, stored_cfg):
    """ Run fmsnotify command. """
    update_sm = None
    try:
        # Insert URI MUST be stored.
        update_sm = setup(ui_, repo, params, stored_cfg)
        request_uri, dummy = do_key_setup(ui_, update_sm,
                                          params, stored_cfg)
        if request_uri is None: # Just assert?
            ui_.warn("Only works for USK file URIs.\n")
            return

        usk_hash = get_usk_hash(request_uri)
        index = stored_cfg.get_index(usk_hash)
        if index is None:
            ui_.warn("Can't notify because there's no stored index "
                     + "for %s.\n" % usk_hash)
            return

        if is_none(stored_cfg.defaults['FMS_ID']):
            ui_.warn("Can't notify because the fms ID isn't set in the "
                     + "config file.\n")
            ui_.status("Update the fms_id = line and try again.\n")
            return

        if is_none(stored_cfg.defaults['FMSNOTIFY_GROUP']):
            ui_.warn("Can't notify because fms group isn't set in the "
                     + "config file.\n")
            ui_.status("Update the fmsnotify_group = line and try again.\n")
            return

        if params['ANNOUNCE']:
            text = to_msg_string(None, (request_uri, ))
        else:
            text = to_msg_string(((usk_hash, index), ))

        subject = 'Update:' + '/'.join(request_uri.split('/')[1:])
        msg_tuple = (stored_cfg.defaults['FMS_ID'],
                     stored_cfg.defaults['FMSNOTIFY_GROUP'],
                     subject,
                     text)

        if params['VERBOSITY'] >= 2:
            ui_.status('Connecting to fms on %s:%i\n' %
                       (stored_cfg.defaults['FMS_HOST'],
                        stored_cfg.defaults['FMS_PORT']))

        ui_.status('Sender : %s\nGroup  : %s\nSubject: %s\n%s\n' %
                   (stored_cfg.defaults['FMS_ID'],
                    stored_cfg.defaults['FMSNOTIFY_GROUP'],
                    subject, text))

        if params['VERBOSITY'] >= 5:
            raw_msg = MSG_TEMPLATE % (msg_tuple[0],
                                      msg_tuple[1],
                                      msg_tuple[2],
                                      msg_tuple[3])
            ui_.status('--- Raw Message ---\n%s\n---\n' % raw_msg)

        if params['DRYRUN']:
            ui_.status('Exiting without sending because --dryrun was set.\n')
            return

        send_msgs(stored_cfg.defaults['FMS_HOST'],
                  stored_cfg.defaults['FMS_PORT'],
                  (msg_tuple, ))

        ui_.status('Notification message sent.\n'
                   'Be patient.  It may take up to a day to show up.\n')
    finally:
        cleanup(update_sm)

