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
from mercurial import util

from fcpclient import get_usk_hash

from knownrepos import KNOWN_REPOS

from fms import recv_msgs, to_msg_string, MSG_TEMPLATE, send_msgs, \
     USKNotificationParser, show_table

from config import Config, trust_id_for_repo, untrust_id_for_repo, known_hashes
from infcmds import do_key_setup, setup, cleanup

def handled_list(ui_, params, stored_cfg):
    """ INTERNAL: HACKED"""
    if params['FMSREAD'] != 'listall' and params['FMSREAD'] != 'list':
        return False

    trust_map = None
    if params['FMSREAD'] == 'list':
        trust_map = stored_cfg.fmsread_trust_map.copy() # paranoid copy
        fms_ids = trust_map.keys()
        fms_ids.sort()
        ui_.status(("Only listing repo USKs from trusted "
                    + "FMS IDs:\n   %s\n\n") % '\n   '.join(fms_ids))

    parser = USKNotificationParser(trust_map)
    parser.add_default_repos(KNOWN_REPOS)
    recv_msgs(stored_cfg.defaults['FMS_HOST'],
              stored_cfg.defaults['FMS_PORT'],
              parser,
              stored_cfg.fmsread_groups)
    show_table(parser, ui_.status)

    return True

def dump_trust_map(ui_, params, trust_map, force=False):
    """ Show verbose trust map information. """
    if  not force and params['VERBOSITY'] < 2:
        return

    if not force and not params['REQUEST_URI'] is None:
        ui_.status("USK hash for local repository: %s\n" %
                   get_usk_hash(params['REQUEST_URI']))
    fms_ids = trust_map.keys()
    fms_ids.sort()
    ui_.status("Update Trust Map:\n")
    for fms_id in fms_ids:
        ui_.status("   %s\n      %s\n" % (fms_id,
                                         '\n      '.join(trust_map[fms_id])))
    ui_.status("\n")

def handled_trust_cmd(ui_, params, stored_cfg):
    """ INTERNAL: Handle --trust, --untrust and --showtrust. """
    if params['FMSREAD'] == 'trust':
        if trust_id_for_repo(stored_cfg.fmsread_trust_map,
                             params['FMSREAD_FMSID'],
                             params['FMSREAD_HASH']):
            ui_.status("Updated the trust map.\n")
            Config.to_file(stored_cfg)
        return True
    elif params['FMSREAD'] == 'untrust':
        if untrust_id_for_repo(stored_cfg.fmsread_trust_map,
                               params['FMSREAD_FMSID'],
                               params['FMSREAD_HASH']):
            ui_.status("Updated the trust map.\n")
            Config.to_file(stored_cfg)
        return True

    elif params['FMSREAD'] == 'showtrust':
        dump_trust_map(ui_, params, stored_cfg.fmsread_trust_map, True)
        return True

    return False

# To appease pylint
def show_fms_info(ui_, params, stored_cfg, show_groups=True):
    """ INTERNAL: Helper function prints fms info. """

    if params['VERBOSITY'] < 2:
        return

    if show_groups:
        ui_.status(('Connecting to fms on %s:%i\n'
                    + 'Searching groups: %s\n') %
                   (stored_cfg.defaults['FMS_HOST'],
                    stored_cfg.defaults['FMS_PORT'],
                    ' '.join(stored_cfg.fmsread_groups)))
    else:
        ui_.status(('Connecting to fms on %s:%i\n') %
                   (stored_cfg.defaults['FMS_HOST'],
                    stored_cfg.defaults['FMS_PORT']))

def execute_fmsread(ui_, params, stored_cfg):
    """ Run the fmsread command. """

    if handled_trust_cmd(ui_, params, stored_cfg):
        return

    show_fms_info(ui_, params, stored_cfg)

    # Listing announced Repo USKs
    if handled_list(ui_, params, stored_cfg):
        return

    # Updating Repo USK indices for repos which are
    # listed in the fmsread_trust_map section of the
    # config file.
    trust_map = stored_cfg.fmsread_trust_map.copy() # paranoid copy

    dump_trust_map(ui_, params, trust_map)

    ui_.status("Raking through fms messages. This may take a while...\n")
    parser = USKNotificationParser()
    recv_msgs(stored_cfg.defaults['FMS_HOST'],
              stored_cfg.defaults['FMS_PORT'],
              parser,
              stored_cfg.fmsread_groups)

    # IMPORTANT: Must include versions that are in the trust map
    #            but which we haven't seen before.
    full_version_table = stored_cfg.version_table.copy()
    for usk_hash in known_hashes(trust_map):
        if not usk_hash in full_version_table:
            full_version_table[usk_hash] = None # works

    changed, untrusted = parser.get_updated(trust_map, full_version_table)

    if params['VERBOSITY'] >= 2 and len(untrusted) > 0:
        text = 'Skipped untrusted updates:\n'
        for usk_hash in untrusted:
            text += "   %i:%s\n" % (untrusted[usk_hash], usk_hash)
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

        show_fms_info(ui_, params, stored_cfg, False)

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

def check_trust_map(ui_, stored_cfg, repo_hash, notifiers, trusted_notifiers):
    """ INTERNAL: Function to interactively update the trust map. """
    if len(trusted_notifiers) > 0:
        return
    ui_.warn("\nYou MUST trust at least one FMS Id to "
             + "provide update notifications.\n\n")

    added = False
    fms_ids = notifiers.keys()
    fms_ids.sort()

    done = False
    for fms_id in fms_ids:
        if done:
            break
        ui_.status("Trust notifications from %s\n" % fms_id)
        while not done:
            result = ui_.prompt("(y)es, (n)o, (d)one, (a)bort?").lower()
            if result is None:
                raise util.Abort("Interactive input required.")
            elif result == 'y':
                trust_id_for_repo(stored_cfg.fmsread_trust_map, fms_id,
                                  repo_hash)
                added = True
                break
            elif result == 'n':
                break
            elif result == 'd':
                done = True
                break
            elif result == 'a':
                raise util.Abort("User aborted editing trust map.")

    if not added:
        raise util.Abort("No trusted notifiers!")

    Config.to_file(stored_cfg)
    ui_.status("Saved updated config file.\n\n")

# Broke into a separate function to appease pylint.
def get_trust_map(ui_, params, stored_cfg):
    """ INTERNAL: Helper function to set up the trust map if required. """
    trust_map = None
    if params['FMSREAD_ONLYTRUSTED']:
        # HACK to deal with spam of the announcement group.'
        trust_map = stored_cfg.fmsread_trust_map.copy() # paranoid copy
        fms_ids = trust_map.keys()
        fms_ids.sort()
        ui_.status(("Only using announcements from trusted "
                    + "FMS IDs:\n   %s\n\n") % '\n   '.join(fms_ids))

    return trust_map

def get_uri_from_hash(ui_, dummy, params, stored_cfg):
    """ Use FMS to get the URI for a repo hash. """

    show_fms_info(ui_, params, stored_cfg)

    parser = USKNotificationParser(get_trust_map(ui_, params, stored_cfg))
    parser.add_default_repos(KNOWN_REPOS)

    ui_.status("Raking through fms messages. This may take a while...\n")
    recv_msgs(stored_cfg.defaults['FMS_HOST'],
              stored_cfg.defaults['FMS_PORT'],
              parser,
              stored_cfg.fmsread_groups)

    target_usk = None
    fms_id_map, announce_map, update_map = parser.invert_table()

    # Find URI
    for usk in announce_map:
        if params['FMSREAD_HASH'] == get_usk_hash(usk):
            # We don't care who announced. The hash matches.
            target_usk = usk
            break

    if target_usk is None:
        raise util.Abort(("No announcement found for [%s]. "
                          +"Use --uri to set the URI.") %
                         params['FMSREAD_HASH'])

    if params['VERBOSITY'] >= 2:
        ui_.status("Found URI announcement:\n%s\n" % target_usk)

    trusted_notifiers = stored_cfg.trusted_notifiers(params['FMSREAD_HASH'])

    notifiers = {}
    for clean_id in update_map[params['FMSREAD_HASH']]:
        notifiers[fms_id_map[clean_id]] = (parser.table[clean_id][1]
                                           [params['FMSREAD_HASH']])

    fms_ids = notifiers.keys()
    fms_ids.sort()

    ui_.status("Found Updates:\n")
    for fms_id in fms_ids:
        if fms_id in trusted_notifiers:
            ui_.status("   [trusted]:%i:%s\n" % (notifiers[fms_id], fms_id))
        else:
            ui_.status("   [untrusted]:%i:%s\n" % (notifiers[fms_id], fms_id))

    check_trust_map(ui_, stored_cfg, params['FMSREAD_HASH'],
                    notifiers, trusted_notifiers)

    # Check for updates against the updated trust map.
    trusted_notifiers = stored_cfg.trusted_notifiers(params['FMSREAD_HASH'])
    for fms_id in fms_ids:
        if fms_id in trusted_notifiers:
            if (notifiers[fms_id] >
                stored_cfg.get_index(params['FMSREAD_HASH'])):
                stored_cfg.update_index(params['FMSREAD_HASH'],
                                        notifiers[fms_id])

    return target_usk

