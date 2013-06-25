import string
import fcp
from config import Config
import xml.etree.ElementTree as ET
from defusedxml.ElementTree import fromstring
import smtplib
from base64 import b32encode
from fcp.node import base64decode
from keys import USK


def send_pull_request(ui, from_identity, to_identity):
    local_identity = resolve_local_identity(ui, from_identity)
    target_identity = resolve_identity(ui, from_identity, to_identity)

    if local_identity is None or target_identity is None:
        # Error.
        return

    from_address = to_freemail_address(local_identity)
    to_address = to_freemail_address(to_identity)

    if from_address is None or to_address is None:
        if from_address is None:
            ui.warn("{0} is not using Freemail.\n".format(from_identity[
                    'Nickname']))
        if to_address is None:
            ui.warn("{0} is not using Freemail.\n".format(to_identity[
                    'Nickname']))
        return

    # TODO: Use FCP host; default port.
    smtp = smtplib.SMTP()
    # TODO: Where to configure Freemail password?
    smtp.login(from_address, )
    smtp.sendmail()


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


def find_repo(ui, truster, wot_identifier, repo_name):
    """
    Return a request URI for a repo of the given name published by an
    identity matching the given identifier.
    Print an error message and return None on failure.
    """
    listing = read_repo_listing(ui, truster, wot_identifier)

    if listing is None:
        return

    if repo_name not in listing:
        # TODO: Perhaps resolve again; print full nick / key?
        # TODO: Maybe print key found in the resolve_*identity?
        ui.warn("{0} does not publish a repo named '{1}'\n".format(
            wot_identifier, repo_name))
        return

    return listing[repo_name]


def read_repo_listing(ui, truster, wot_identifier):
    """
    Read a repo listing for a given identity.
    Return a dictionary of repository request URIs keyed by name.
    """
    identity = resolve_identity(ui, truster, wot_identifier)
    if identity is None:
        return

    ui.status("Found {0}@{1}.\n".format(identity['Nickname'],
                                        identity['Identity']))

    uri = identity['RequestURI']
    uri = uri.split('/', 1)[0] + '/vcs/0'

    # TODO: Set and read vcs edition property.
    node = fcp.FCPNode()
    ui.status("Fetching {0}\n".format(uri))
    # TODO: What exception can this throw on failure? Catch it,
    # print its description, and return None.
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


def resolve_pull_uri(ui, path, truster):
        """
        Return a pull URI for the given path.
        Print an error message and return None on failure.
        TODO: Is it appropriate to outline possible errors?
        Possible failures are being unable to fetch a repo list for the given
        identity, which may be a fetch failure or being unable to find the
        identity, and not finding the requested repo in the list.

        :param ui: For feedback.
        :param path: path describing a repo: nick@key/reponame
        :param truster: identity whose trust list to use.
        :return:
        """
        # Expecting <id stuff>/reponame
        wot_id, repo_name = path.split('/', 1)

        # TODO: How to handle redundancy? Does Infocalypse automatically try
        # an R0 if an R1 fails?

        return find_repo(ui, truster, wot_id, repo_name)


def resolve_push_uri(ui, path):
    """
    Return a push URI for the given path.
    Print an error message and return None on failure.

    :param ui: For feedback.
    :param path: path describing a repo - nick@key/repo_name,
    where the identity is a local one. (Such that the insert URI is known.)
    """
    # Expecting <id stuff>/repo_name
    # TODO: Duplicate with resolve_pull
    wot_id, repo_name = path.split('/', 1)

    local_id = resolve_local_identity(ui, wot_id)

    if local_id is None:
        return

    insert_uri = USK(local_id['InsertURI'])

    identifier = local_id['Nickname'] + '@' + local_id['Identity']

    repo = find_repo(ui, local_id['Identity'], identifier, repo_name)

    if repo is None:
        return

    # Request URI
    repo_uri = USK(repo)

    # Maintains path, edition.
    repo_uri.key = insert_uri.key

    return str(repo_uri)

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


def execute_setup_freemail(ui, wot_identifier):
    """
    Prompt for, test, and set a Freemail password for the identity.
    """
    local_id = resolve_local_identity(ui, wot_identifier)

    if local_id is None:
        return

    address = to_freemail_address(local_id)

    if address is None:
        ui.warn("{0}@{1} does not have a Freemail address.\n".format(
            local_id['Nickname'], local_id['Identity']))
        return

    password = ui.getpass()
    if password is None:
        ui.warn("Cannot prompt for a password in a non-interactive context.")
        return

    ui.status("Checking password for {0}@{1}.\n".format(local_id['Nickname'],
                                                        local_id['Identity']))

    cfg = Config.from_ui(ui)

    # Check that the password works.
    try:
        # TODO: Is this the correct way to get the configured host?
        smtp = smtplib.SMTP(cfg.defaults['HOST'], FREEMAIL_SMTP_PORT)
        smtp.login(address, password)
    except smtplib.SMTPAuthenticationError, e:
        ui.warn("Could not log in using password '{0}'.\n".format(password))
        ui.warn("Got '{0}'\n".format(e.smtp_error))
        return
    except smtplib.SMTPConnectError, e:
        ui.warn("Could not connect to server.\n")
        ui.warn("Got '{0}'\n".format(e.smtp_error))
        return

    cfg.set_freemail_password(local_id['Identity'], password)
    Config.to_file(cfg)
    ui.status("Password set.\n")


def resolve_local_identity(ui, wot_identifier):
    """
    Mercurial ui for error messages.

    Returns a dictionary of the nickname, insert and request URIs,
    and identity that match the given criteria.
    In the case of an error prints a message and returns None.
    """
    nickname_prefix, key_prefix = parse_name(wot_identifier)

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
        ui.warn("'{0}' is ambiguous.\n".format(wot_identifier))
        return

    if len(matches) == 0:
        ui.warn("No local identities match '{0}'.\n".format(wot_identifier))
        return

    assert len(matches) == 1

    # id_num is first member of value tuple.
    only_key = matches.keys()[0]
    id_num = matches[only_key][0]

    return read_local_identity(response, id_num)


def resolve_identity(ui, truster, wot_identifier):
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
    :param wot_identifier: Nickname and key, delimited by @. Either half can be
    omitted.
    """
    nickname_prefix, key_prefix = parse_name(wot_identifier)
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode()

    # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
    # src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname.java
    # TODO: LCWoT allows limiting by context, but how to make sure otherwise?
    # TODO: Should this manually ensure an identity has a vcs context
    # otherwise?

    # LCWoT can have * to allow a wildcard match, but a wildcard alone is not
    # allowed. See Lucine Term Modifiers documentation. The nickname uses
    # this syntax but the ID is inherently startswith().
    params = {'Message': 'GetIdentitiesByPartialNickname',
              'Truster': truster,
              'PartialNickname':
              nickname_prefix + '*' if nickname_prefix else '',
              'PartialID': key_prefix,
              'MaxIdentities': 2,
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
        matches = response['Replies.IdentitiesMatched']
        if matches == 0:
            ui.warn("No identities match '{0}'\n".format(wot_identifier))
            return
        elif matches == 1:
            return read_identity(response, 0)
        else:
            ui.warn("'{0}' is ambiguous.\n".format(wot_identifier))
            return

    # Partial matching not supported, or unknown truster. The only difference
    # in the errors is human-readable, so just try the exact match.
    assert response['Replies.Message'] == 'Error'

    # key_prefix must be a complete key for the lookup to succeed.
    params = {'Message': 'GetIdentity',
              'Truster': truster,
              'Identity': key_prefix}
    response = \
        node.fcpPluginMessage(async=False,
                              plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    if response['Replies.Message'] == 'Error':
        # Searching by exact public key hash, not matching.
        ui.warn("No such identity '{0}'.\n".format(wot_identifier))
        return

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


def parse_name(wot_identifier):
    """
    Parse identifier of the forms: nick
                                   nick@key
                                   @key
    Return nick, key. If a part is not given return an empty string for it.
    """
    split = wot_identifier.split('@', 1)
    nickname_prefix = split[0]

    key_prefix = ''
    if len(split) == 2:
        key_prefix = split[1]

    return nickname_prefix, key_prefix


def to_freemail_address(identity):
    """
    Return a Freemail address to contact the given identity if it has a
    Freemail context. Return None if it does not have a Freemail context.
    """

    # Freemail addresses encode the public key hash with base32 instead of
    # base64 as WoT does. This is to be case insensitive because email
    # addresses are not case sensitive, so some clients may mangle case.
    # See https://github.com/zidel/Freemail/blob/v0.2.2.1/docs/spec/spec.tex#L32

    for item in identity.iteritems():
        if item[1] == 'Freemail' and item[0].startswith('Context'):
            re_encode = b32encode(base64decode(identity['Identity']))
            # Remove trailing '=' padding.
            re_encode = re_encode.rstrip('=')

            # Freemail addresses are lower case.
            return string.lower(identity['Nickname'] + '@' + re_encode +
                                '.freemail')

    return None
