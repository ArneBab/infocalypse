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

# REDFLAG: Go back and fix all the places where you return instead of Abort()
import socket

from mercurial import util

from fcpclient import get_usk_hash

from knownrepos import KNOWN_REPOS

from fms import recv_msgs, to_msg_string, MSG_TEMPLATE, send_msgs, \
     USKNotificationParser, show_table, get_connection

from config import Config, trust_id_for_repo, untrust_id_for_repo, known_hashes
from infcmds import do_key_setup, setup, cleanup, execute_insert_patch
from wikicmds import execute_wiki_submit

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
    recv_msgs(get_connection(stored_cfg.defaults['FMS_HOST'],
                             stored_cfg.defaults['FMS_PORT'],
                             None),
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
    recv_msgs(get_connection(stored_cfg.defaults['FMS_HOST'],
                             stored_cfg.defaults['FMS_PORT'],
                             None),
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
            text += "   %i:%s\n" % (untrusted[usk_hash][0], usk_hash)
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

def check_fms_cfg(ui_, params, stored_cfg):
    """ INTERNAL: Helper aborts on bad fms configuration. """
    if (is_none(stored_cfg.defaults['FMS_ID']) or
        stored_cfg.defaults['FMS_ID'].strip() == ''):
        ui_.warn("Can't notify because the fms ID isn't set in the "
                 + "config file.\n")
        raise util.Abort("Fix the fms_id = line in the config file and " +
                         "and try again.\n")

    if stored_cfg.defaults['FMS_ID'].find('@') != -1:
        ui_.warn("The fms_id line should only "
                 + "contain the part before the '@'.\n")
        raise util.Abort("Fix the fms_id = line in the config file and " +
                         "and try again.\n")

    if (is_none(stored_cfg.defaults['FMSNOTIFY_GROUP']) or
        (stored_cfg.defaults['FMSNOTIFY_GROUP'].strip() == '') and
        not params.get('SUBMIT_WIKI', False)):
        ui_.warn("Can't notify because fms group isn't set in the "
                 + "config file.\n")
        raise util.Abort("Update the fmsnotify_group = line and try again.\n")


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

        check_fms_cfg(ui_, params, stored_cfg)

        usk_hash = get_usk_hash(request_uri)
        index = stored_cfg.get_index(usk_hash)
        if index is None and not (params.get('SUBMIT_BUNDLE', False) or
                                  params.get('SUBMIT_WIKI', False)):
            ui_.warn("Can't notify because there's no stored index "
                     + "for %s.\n" % usk_hash)
            return

        group = stored_cfg.defaults.get('FMSNOTIFY_GROUP', None)
        subject = 'Update:' + '/'.join(request_uri.split('/')[1:])
        if params['ANNOUNCE']:
            text = to_msg_string(None, (request_uri, ))
        elif params['SUBMIT_BUNDLE']:
            params['REQUEST_URI'] = request_uri # REDFLAG: Think through.
            text = execute_insert_patch(ui_, repo, params, stored_cfg)
            subject = 'Patch:' + '/'.join(request_uri.split('/')[1:])
        elif params['SUBMIT_WIKI']:
            params['REQUEST_URI'] = request_uri # REDFLAG: Think through.
            text, group = execute_wiki_submit(ui_, repo, params, stored_cfg)
            subject = 'Submit:' + '/'.join(request_uri.split('/')[1:])
        else:
            text = to_msg_string(((usk_hash, index), ))

        msg_tuple = (stored_cfg.defaults['FMS_ID'],
                     group,
                     subject,
                     text)

        show_fms_info(ui_, params, stored_cfg, False)

        ui_.status('Sender : %s\nGroup  : %s\nSubject: %s\n%s\n' %
                   (stored_cfg.defaults['FMS_ID'],
                    group,
                    subject, text))

        if params['VERBOSITY'] >= 5:
            ui_.status('--- Raw Message ---\n%s\n---\n' % (
                MSG_TEMPLATE % (msg_tuple[0], msg_tuple[1],
                                msg_tuple[2], msg_tuple[3])))

        if params['DRYRUN']:
            ui_.status('Exiting without sending because --dryrun was set.\n')
            return

        # REDFLAG: for testing!
        if 'MSG_SPOOL_DIR' in params:
            ui_.warn("DEBUG HACK!!! Writing fms msg to local spool:\n%s\n" %
                      params['MSG_SPOOL_DIR'])
            import fmsstub

            # LATER: fix config file to store full fmsid?
            # grrrr... hacks piled upon hacks.
            lut = {'djk':'djk@isFiaD04zgAgnrEC5XJt1i4IE7AkNPqhBG5bONi6Yks'}
            fmsstub.FMSStub(params['MSG_SPOOL_DIR'], group,
                            lut).send_msgs(
                get_connection(stored_cfg.defaults['FMS_HOST'],
                               stored_cfg.defaults['FMS_PORT'],
                               None),
                (msg_tuple, ), True)
        else:
            send_msgs(get_connection(stored_cfg.defaults['FMS_HOST'],
                                     stored_cfg.defaults['FMS_PORT'],
                                     None),
                                     (msg_tuple, ), True)

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
    recv_msgs(get_connection(stored_cfg.defaults['FMS_HOST'],
                             stored_cfg.defaults['FMS_PORT'],
                             None),
              parser,
              stored_cfg.fmsread_groups,
              None,
              True)

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


CRLF = '\x0d\x0a'
FMS_TIMEOUT_SECS = 30
FMS_SOCKET_ERR_MSG = """
Socket level error.
It looks like your FMS host or port might be wrong.
Set them with --fmshost and/or --fmsport.
"""

def connect_to_fms(ui_, fms_host, fms_port, timeout):
    """ INTERNAL: Helper, connects to fms and reads the login msg. """
    ui_.status("Testing FMS connection [%s:%i]...\n" % (fms_host, fms_port))
    try:
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        connected_socket = None
        try:
            connected_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connected_socket.connect((fms_host, fms_port))
            bytes =  ''
            while bytes.find(CRLF) == -1:
                bytes = bytes + connected_socket.recv(4096)
        finally:
            socket.setdefaulttimeout(old_timeout)
            if connected_socket:
                connected_socket.close()

    except socket.error: # Not an IOError until 2.6.
        ui_.warn(FMS_SOCKET_ERR_MSG)
        return None

    except IOError:
        ui_.warn(FMS_SOCKET_ERR_MSG)
        return None

    return bytes


# Passing opts instead of separate args to get around pylint
# warning about long arg list.
def get_fms_args(cfg, opts):
    """ INTERNAL: Helper to extract args from Config/mercurial opts. """
    def false_to_none(value):
        """ INTERNAL: Return None if not bool(value),  value otherwise. """
        if value:
            return value
        return None

    fms_id = false_to_none(opts['fmsid'])
    fms_host = false_to_none(opts['fmshost'])
    fms_port = false_to_none(opts['fmsport'])
    timeout = opts['timeout']

    if not cfg is None:
        if fms_id is None:
            fms_id = cfg.defaults.get('FMS_ID', None)
        if fms_host is None:
            fms_host = cfg.defaults.get('FMS_HOST', None)
        if fms_port is None:
            fms_port = cfg.defaults.get('FMS_PORT', None)

    if fms_id is None:
        fms_id = 'None' # hmmm
    if fms_host is None:
        fms_host = '127.0.0.1'
    if fms_port is None:
        fms_port = 1119

    return (fms_id, fms_host, fms_port, timeout)

# DCI: clean up defaults
def setup_fms_config(ui_, cfg, opts):
    """ INTERNAL: helper tests the fms connection. """

    fms_id, fms_host, fms_port, timeout = get_fms_args(cfg, opts)

    ui_.status("Running FMS checks...\nChecking fms_id...\n")
    if fms_id.find('@') != -1:
        ui_.warn("\n")
        ui_.warn("""   The FMS id should only contain the part before the '@'!
   You won't be able to use fn-fmsnotify until this is fixed.
   Run: hg fn-setupfms with the --fmsid argument.

""")
    elif fms_id.lower() == 'none':
        ui_.warn("""   FMS id isn't set!
   You won't be able to use fn-fmsnotify until this is fixed.
   Run: hg fn-setupfms with the --fmsid argument.

""")
    else:
        ui_.status("OK.\n\n") # hmmm... what if they manually edited the config?

    bytes = connect_to_fms(ui_, fms_host, fms_port, timeout)
    if not bytes:
        if not bytes is None:
            ui_.warn("Connected but no response. Are you sure that's "
                     "an FMS server?\n")
        return None

    fields = bytes.split(' ')
    if fields[0] != '200':
        ui_.warn("Didn't get expected response from FMS server!\n")
        return None

    if not bytes.lower().find("posting allowed"):
        ui_.warn("Didn't see expected 'posting allowed' message.\n")
        ui_.warn("Check that FMS is setup to allow outgoing message.\n")
        return None # Hmmm.. feeble, relying on message text.
    else:
        ui_.status("Got expected response from FMS. Looks good.\n")

    return (fms_host, fms_port, fms_id)

def execute_setupfms(ui_, opts):
    """ Execute the fn-setupfms command. """
    cfg = Config.from_ui(ui_)
    result = setup_fms_config(ui_, cfg, opts)
    if result:
        cfg.defaults['FMS_ID'] = result[2]
        cfg.defaults['FMS_HOST'] = result[0]
        cfg.defaults['FMS_PORT'] = result[1]
        ui_.status("""Updating config file:
fms_id = %s
fms_host = %s
fms_port = %i
""" % (result[2], result[0], result[1]))
        Config.to_file(cfg)
    else:
        ui_.warn("""
Run:
   hg fn-setupfms
with the appropriate arguments to try to fix the problem.

""")

