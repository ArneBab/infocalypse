import fcp
from config import Config
import xml.etree.ElementTree as ET

def update_repo_listing(ui, for_identity):
    # TODO: WoT property containing edition. Used when requesting.
    config = Config.from_ui(ui)
    root = ET.Element('vcs', {'version': '0'})

    # Add request URIs associated with the given identity.
    for request_uri in config.request_usks.itervalues():
        if config.get_wot_identity(request_uri) == for_identity:
            repo = ET.SubElement(root, 'repository', {
                'vcs': 'Infocalypse',
            })
            repo.text = request_uri

    # TODO: Nonstandard IP and port.
    node = fcp.FCPNode()
    # TODO: Does it make sense to query the node here for the private key?
    attributes = resolve_local_identity(ui, key_prefix=for_identity)
    # TODO: Repetitive key parsing again!
    insert_uri = attributes['InsertURI']
    # Expecting USK@key/WebOfTrust/edition; want only key.
    insert_uri = insert_uri.split('/', 1)[0] + 'vcs/0'
    ui.status("Inserting with URI:\n{0}\n".format(insert_uri))
    uri = node.put(uri=insert_uri, mimetype='application/xml',
                   data=ET.tostring(root))

    if uri is None:
        ui.warn("Failed to update repository listing.")
    else:
        ui.status("Updated repository listing:\n{0}\n".format(uri))

# Support for querying WoT for own identities and identities meeting various
# criteria.
# TODO: "cmds" suffix to module name to fit fms, arc, inf?

def execute_setup_wot(ui_, opts):
    cfg = Config.from_ui(ui_)
    wot_id = opts['truster']

    # TODO: Code for wot_id parsing duplicated between here and WoT pull.
    nickname_prefix = ''
    key_prefix = ''
    # Could be nick@key, nick, @key
    split = wot_id.split('@')
    nickname_prefix = split[0]

    if len(split) == 2:
        key_prefix = split[1]

    # TODO: Support key
    response = resolve_local_identity(ui_, nickname_prefix=nickname_prefix)

    if response is None:
        return

    ui_.status("Setting default truster to {0}@{1}\n".format(
        response['Nickname'],
        response['Identity']))

    cfg.defaults['DEFAULT_TRUSTER'] = response['Identity']
    Config.to_file(cfg)


def resolve_local_identity(ui, nickname_prefix=None, key_prefix=None):
    """
    Mercurial ui for error messages.
    Nickname prefix should be enough to not be ambiguous.
    If the nickname is not set the key must be.
    # TODO: Does not support duplicate nicknames between local identities.
    # Could support looking at identity to resolve further.

    Returns a dictionary of the nickname, insert and request URIs,
    and identity that match the given criteria.
    In the case of an error prints a message and returns None.
    """
    node = fcp.FCPNode()
    response =\
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params={'Message':
                                             'GetOwnIdentities'})[0]

    if response['header'] != 'FCPPluginReply' or\
            'Replies.Message' not in response or\
            response['Replies.Message'] != 'OwnIdentities':
        ui.warn("Unexpected reply. Got {0}\n.".format(response))
        return

    # TODO: Single function to resolve identity used for own and remote?
    # Not preferable if the flag leads to two different code paths.

    prefix = 'Replies.Identity'
    id_num = -1
    # Go by full key instead.
    if nickname_prefix is None:
        for item in response.iteritems():
            if item[1] == key_prefix:
                # Assuming identities will always be unique.
                id_num = item[0][len(prefix):]
                return read_local_identity(response, id_num)

        ui.warn("No identity found with key '{0}'.\n".format(key_prefix))
        return

    # TODO: Cleaner flow of control between key-only and nick-and-optional-key
    # Find nicknames starting with the supplied nickname prefix.
    prefix = 'Replies.Nickname'
    nickname = None
    for key in response.iterkeys():
        if key.startswith(prefix) and\
                response[key].startswith(nickname_prefix):
            if nickname is not None:
                # More than one matched.
                ui.warn("Prefix '{0}' is ambiguous.\n".format(nickname_prefix))
                return

            nickname = response[key]
            # Key is Replies.Nickname<number>, where number is used in
            # the other attributes returned for that identity.
            id_num = key[len(prefix):]

    if nickname is None:
        ui.warn("No nicknames start with '{0}'.\n".format(nickname_prefix))
        return

    return read_local_identity(response, id_num)


def resolve_identity(ui, truster, nickname_prefix=None, key_prefix=''):
    """
    If using LCWoT, either the nickname prefix should be enough to be
    unambiguous, or failing that enough of the key.
    If using WoT, partial search is not supported, and the entire key must be
    specified.

    Returns a dictionary of the nickname, request URI,
    and identity that matches the given criteria.
    In the case of an error prints a message and returns None.

    :param ui: Mercurial ui for error messages.
    :param truster: Check trust list of this local identity.
    :param nickname_prefix: Partial (prefix) of nickname. Can be whole.
    :param key_prefix: Partial (prefix) of key. Can be empty.
    """
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode()

    # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
    # https://github.com/tmarkus/LessCrappyWebOfTrust/blob/master/src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname.java
    params = {'Message': 'GetIdentitiesByPartialNickname',
              'Truster': truster,
              'PartialNickname': nickname_prefix,
              'PartialID': key_prefix,
              'MaxIdentities': 1,  # Match must be unambiguous.
              'Context': 'vcs'}
    response =\
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    if response['header'] != 'FCPPluginReply' or\
       'Replies.Message' not in response:
            ui.warn('Unexpected reply. Got {0}\n'.format(response))
            return
    elif response['Replies.Message'] == 'Identities':
        return read_identity(response, 0)
    elif response['Replies.Message'] == 'Error':
        # The difficulty here is that the message type is Error for both an
        # unrecognized message type and ambiguous search terms.
        # TODO: This seems likely to break - the Description seems intended
        # for human readers and will probably change.
        if response['Replies.Description'].startswith('Number of matched'):
            # Supported version of LCWoT - terms ambiguous.
            ui.warn("'{0}@{1}' is ambiguous.".format(nickname_prefix,
                                                     key_prefix))
            return
        elif response['Replies.Description'].startswith('Unknown message') or\
             response['Replies.Description'].startswith('Could not match'):
            # Not supported; check for exact identity.
            ui.warn('Searching by partial nickname/key not supported.')

    # Attempt to search failed - check for exact key. Here key_prefix must be
    # a complete key for the lookup to succeed.
    params = {'Message': 'GetIdentity',
              'Truster': truster,
              'Identity': key_prefix}
    response =\
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    # There should be only one result.
    # Depends on https://bugs.freenetproject.org/view.php?id=5729
    print read_identity(response, 0)


def read_local_identity(message, id_num):
    """
    Reads an FCP response from a WoT plugin describing a local identity and
    returns a dictionary of Nickname, InsertURI, RequestURI, and Identity.
    """
    result = read_identity(message, id_num)
    result['InsertURI'] = message['Replies.InsertURI{0}'.format(id_num)]
    return result


def read_identity(message, id_num):
    """
    Reads an FCP response from a WoT plugin describing an identity and
    returns a dictionary of Nickname, RequestURI, and Identity.
    """
    # Return properties for the selected identity. (by number)
    result = {}
    for item in [ 'Nickname', 'RequestURI', 'Identity' ]:
        result[item] = message['Replies.{0}{1}'.format(item, id_num)]

    # LCWoT also puts these things as properties, which would be nicer to
    # depend on and would allow just returning all properties for the identity.
    #property_prefix = "Replies.Properties{0}".format(id_num)

    return result
