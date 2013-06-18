import fcp
from config import Config
import xml.etree.ElementTree as ET
from defusedxml.ElementTree import fromstring


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
    # Key goes after @ - before is nickname.
    attributes = resolve_local_identity(ui, '@' + for_identity)
    # TODO: Repetitive key parsing again!
    insert_uri = attributes['InsertURI']
    # Expecting USK@key/WebOfTrust/edition; want only key.
    insert_uri = insert_uri.split('/', 1)[0] + '/vcs/0'
    ui.status("Inserting with URI:\n{0}\n".format(insert_uri))
    uri = node.put(uri=insert_uri, mimetype='application/xml',
                   data=ET.tostring(root), priority=1)

    if uri is None:
        ui.warn("Failed to update repository listing.")
    else:
        ui.status("Updated repository listing:\n{0}\n".format(uri))


def read_repo_listing(ui, truster, identity):
    """
    Read a repo listing for a given identity.
    Return a dictionary of repository URIs keyed by name.
    """
    identity = resolve_identity(ui, truster, identity)
    if identity is None:
        return

    uri = identity['RequestURI']
    uri = uri.split('/', 1)[0] + '/vcs/0'

    # TODO: Set and read vcs edition property.
    node = fcp.FCPNode()
    ui.status("Fetching {0}\n".format(uri))
    mime_type, repo_xml, msg = node.get(uri, priority=1)

    ui.status("Parsing.\n")
    repositories = {}
    root = fromstring(repo_xml)
    for repository in root.iterfind('repository'):
        if repository.get('vcs') == 'Infocalypse':
            uri = repository.text
            # Expecting key/reponame.R<num>/edition
            name = uri.split('/')[1].split('.')[0]
            ui.status("Found repository \"{0}\" at {1}\n".format(name, uri))
            repositories[name] = uri

    return repositories

# Support for querying WoT for own identities and identities meeting various
# criteria.
# TODO: "cmds" suffix to module name to fit fms, arc, inf?


def execute_setup_wot(ui_, opts):
    cfg = Config.from_ui(ui_)
    response = resolve_local_identity(ui_, opts['truster'])

    if response is None:
        return

    ui_.status("Setting default truster to {0}@{1}\n".format(
        response['Nickname'],
        response['Identity']))

    cfg.defaults['DEFAULT_TRUSTER'] = response['Identity']
    Config.to_file(cfg)


def resolve_local_identity(ui, identity):
    """
    Mercurial ui for error messages.

    Returns a dictionary of the nickname, insert and request URIs,
    and identity that match the given criteria.
    In the case of an error prints a message and returns None.
    """
    nickname_prefix, key_prefix = parse_name(identity)

    node = fcp.FCPNode()
    response = \
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params={'Message':
                                             'GetOwnIdentities'})[0]

    if response['header'] != 'FCPPluginReply' or \
            'Replies.Message' not in response or \
            response['Replies.Message'] != 'OwnIdentities':
        ui.warn("Unexpected reply. Got {0}\n.".format(response))
        return

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

    # Find nicknames starting with the supplied nickname prefix.
    prefix = 'Replies.Nickname'
    # Key: nickname, value (id_num, public key hash).
    matches = {}
    for key in response.iterkeys():
        if key.startswith(prefix) and \
                response[key].startswith(nickname_prefix):

            # Key is Replies.Nickname<number>, where number is used in
            # the other attributes returned for that identity.
            id_num = key[len(prefix):]

            nickname = response[key]
            pubkey_hash = response['Replies.Identity{0}'.format(id_num)]

            matches[nickname] = (id_num, pubkey_hash)

    # Remove matching nicknames not also matching the (possibly partial)
    # public key hash.
    for key in matches.keys():
        # public key hash is second member of value tuple.
        if not matches[key][1].startswith(key_prefix):
            del matches[key]

    if len(matches) > 1:
        ui.warn("'{0}' is ambiguous.\n".format(identity))
        return

    if len(matches) == 0:
        ui.warn("No local identities match '{0}'.\n".format(identity))
        return

    assert len(matches) == 1

    # id_num is first member of value tuple.
    only_key = matches.keys()[0]
    id_num = matches[only_key][0]

    return read_local_identity(response, id_num)


def resolve_identity(ui, truster, identity):
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
    :param identity: Nickname and key, delimited by @. Either half can be
    omitted.
    """
    nickname_prefix, key_prefix = parse_name(identity)
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode()

    # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
    # https://github.com/tmarkus/LessCrappyWebOfTrust/blob/master/src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname.java
    # TODO: LCWoT allows limiting by context, but how to make sure otherwise?
    # TODO: Should this manually ensure an identity has a vcs context
    # otherwise?
    params = {'Message': 'GetIdentitiesByPartialNickname',
              'Truster': truster,
              'PartialNickname': nickname_prefix,
              'PartialID': key_prefix,
              'MaxIdentities': 1, # Match must be unambiguous.
              'Context': 'vcs'}
    response = \
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    if response['header'] != 'FCPPluginReply' or \
            'Replies.Message' not in response:
        ui.warn('Unexpected reply. Got {0}\n'.format(response))
        return
    elif response['Replies.Message'] == 'Identities':
        # TODO: What if no identities matched?
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
        elif response['Replies.Description'].startswith('Unknown message') or \
                response['Replies.Description'].startswith('Could not match'):
            # Not supported; check for exact identity.
            ui.warn('Searching by partial nickname/key not supported.')

    # Attempt to search failed - check for exact key. Here key_prefix must be
    # a complete key for the lookup to succeed.
    params = {'Message': 'GetIdentity',
              'Truster': truster,
              'Identity': key_prefix}
    response = \
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    # There should be only one result.
    # Depends on https://bugs.freenetproject.org/view.php?id=5729
    return read_identity(response, 0)


def read_local_identity(message, id_num):
    """
    Reads an FCP response from a WoT plugin describing a local identity and
    returns a dictionary of Nickname, InsertURI, RequestURI, Identity, and
    each numbered Context.
    """
    result = read_identity(message, id_num)
    result['InsertURI'] = message['Replies.InsertURI{0}'.format(id_num)]
    return result


def read_identity(message, id_num):
    """
    Reads an FCP response from a WoT plugin describing an identity and
    returns a dictionary of Nickname, RequestURI, Identity, and Contexts.
    """
    # Return properties for the selected identity. (by number)
    result = {}
    for item in ['Nickname', 'RequestURI', 'Identity']:
        result[item] = message['Replies.{0}{1}'.format(item, id_num)]

    # LCWoT also puts these things as properties, which would be nicer to
    # depend on and would allow just returning all properties for the identity.
    #property_prefix = "Replies.Properties{0}".format(id_num)

    # Add contexts for the identity too.
    # TODO: Unflattening WoT response? Several places check for prefix like
    # this.
    prefix = "Replies.Contexts{0}.Context".format(id_num)
    for key in message.iterkeys():
        if key.startswith(prefix):
            num = key[len(prefix):]
            result["Context{0}".format(num)] = message[key]

    return result


def parse_name(identity):
    """
    Parse identity of the forms: nick
                                 nick@key
                                 @key
    Return nick, key. If a part is not given return an empty string.
    """
    split = identity.split('@', 1)
    nickname_prefix = split[0]

    key_prefix = ''
    if len(split) == 2:
        key_prefix = split[1]

    return nickname_prefix, key_prefix
