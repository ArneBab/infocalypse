import fcp
from mercurial import util
import string
from keys import USK
from base64 import b32encode
from fcp.node import base64decode


class WoT_ID(object):
    """
    Represents a WoT ID.

    TODO: Is this list appropriate to have?
    * nickname - str
    * request_uri - USK
    * identity_id - str
    * contexts - list
    * properties - dict
    * freemail_address - str
    """

    def __init__(self, wot_identifier, truster, id_num=0, message=None):
        """
        If using LCWoT, either the nickname prefix should be enough to be
        unambiguous, or failing that enough of the key.
        If using WoT, partial search is not supported, and the entire key must
        be specified.

        :type truster: Local_WoT_ID
        :type wot_identifier: str
        :param truster: Check trust list of this local identity.
        :param wot_identifier: Nickname and key, delimited by @. Either half can
        be omitted.
        """
        # id_num and message are internal and used to allow constructing
        # a WoT_ID for a Local_WoT_ID. Their default values parse the first
        # (and only) identity described by an unspecified message, in which case
        # it queries WoT to produce one.
        if not message:
            message = _get_identity(wot_identifier, truster)

        def get_attribute(attribute):
            return message['Replies.{0}{1}'.format(attribute, id_num)]

        self.nickname = get_attribute('Nickname')
        self.request_uri = USK(get_attribute('RequestURI'))
        self.identity_id = get_attribute('Identity')

        self.contexts = []
        self.properties = {}
        context_prefix = "Replies.Contexts{0}.Context".format(id_num)
        property_prefix = "Replies.Properties{0}.Property".format(id_num)
        for key in message.iterkeys():
            if key.startswith(context_prefix):
                self.contexts.append(message[key])
            elif key.startswith(property_prefix) and key.endswith(".Name"):
                # ".Name" is 5 characters, before which is the number.
                num = key[len(property_prefix):-5]

                # Example:
                # Replies.Properties1.Property1.Name = IntroductionPuzzleCount
                # Replies.Properties1.Property1.Value = 10
                name = message[key]
                value = message[property_prefix + num + '.Value']

                # LCWoT returns many things with duplicates in properties,
                # so this conflict is something that can happen. Checking for
                # value conflict restricts the message to cases where it
                # actually has an effect.
                if name in self.properties and value != self.properties[name]:
                    print("WARNING: '{0}' has conflicting value as a property."
                          .format(name))

                self.properties[name] = value

        # Freemail addresses encode the public key hash with base32 instead of
        # base64 as WoT does. This is to be case insensitive because email
        # addresses are not case sensitive, so some clients may mangle case.
        # See:
        # https://github.com/zidel/Freemail/blob/v0.2.2.1/docs/spec/spec.tex#L32

        if not 'Freemail' in self.contexts:
            self.freemail_address = None
        else:
            re_encode = b32encode(base64decode(self.identity_id))
            # Remove trailing '=' padding.
            re_encode = re_encode.rstrip('=')

            # Freemail addresses are lower case.
            self.freemail_address = string.lower(self.nickname + '@' + re_encode
                                                 + '.freemail')

    def __str__(self):
        return self.nickname + '@' + self.identity_id


class Local_WoT_ID(WoT_ID):
    """
    Represents a local WoT ID.

    * nickname - str
    * request_uri - USK
    * insert_uri - USK
    * identity_id - str
    * contexts - list
    * properties - dict
    """

    def __init__(self, wot_identifier):
        """
        Create a WoT_ID for a local identity matching the identifier.

        :type wot_identifier: str
        """
        id_num, message = _get_local_identity(wot_identifier)

        self.insert_uri = USK(message['Replies.InsertURI{0}'.format(id_num)])

        WoT_ID.__init__(self, None, None, id_num=id_num, message=message)


def _get_identity(wot_identifier, truster):
    """
    Internal.

    Return an FCP reply from WoT for an identity on the truster's trust list
    matching the identifier. Abort if anything but exactly one match is found.

    :type wot_identifier: str
    :type truster: Local_WoT_ID
    """
    nickname_prefix, key_prefix = _parse_name(wot_identifier)
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode()

    # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
    # src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname
    # TODO: LCWoT allows limiting by context; how to make sure otherwise?
    # TODO: Should this manually ensure an identity has a vcs context
    # otherwise?

    # GetIdentitiesByPartialNickname does not support empty nicknames.
    if nickname_prefix:
        params = {'Message': 'GetIdentitiesByPartialNickname',
                  'Truster': truster.identity_id,
                  'PartialNickname':
                  nickname_prefix + '*',
                  'PartialID': key_prefix,
                  'MaxIdentities': 2,
                  'Context': 'vcs'}

        response = \
            node.fcpPluginMessage(plugin_name="plugins.WebOfTrust.WebOfTrust",
                                  plugin_params=params)[0]

        if response['header'] != 'FCPPluginReply' or \
                'Replies.Message' not in response:
            raise util.Abort('Unexpected reply. Got {0}\n'.format(response))
        elif response['Replies.Message'] == 'Identities':
            matches = response['Replies.IdentitiesMatched']
            if matches == 0:
                raise util.Abort("No identities match '{0}'."
                                 .format(wot_identifier))
            elif matches == 1:
                return response
            else:
                raise util.Abort("'{0}' matches more than one identity."
                                 .format(wot_identifier))

        # Partial matching not supported, or unknown truster. The only
        # difference in the errors is human-readable, so try the exact match.
        assert response['Replies.Message'] == 'Error'

    # key_prefix must be a complete key for the lookup to succeed.
    params = {'Message': 'GetIdentity',
              'Truster': truster.identity_id,
              'Identity': key_prefix}
    response = \
        node.fcpPluginMessage(plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    if response['Replies.Message'] == 'Error':
        # Searching by exact public key hash, not matching.
        raise util.Abort("No identity has the complete public key hash '{0}'. "
                         "({1}) To flexibly match by partial nickname and key "
                         "use LCWoT for now."
                         .format(key_prefix, wot_identifier))

    # There should be only one result.
    # Depends on https://bugs.freenetproject.org/view.php?id=5729
    return response


def _get_local_identity(wot_identifier):
    """
    Internal.

    Return (id_number, FCP reply) from WoT for a local identity matching the
    identifier. Abort if anything but exactly one match is found.

    :type wot_identifier: str
    """
    nickname_prefix, key_prefix = _parse_name(wot_identifier)

    node = fcp.FCPNode()
    response = \
        node.fcpPluginMessage(plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params={'Message':
                                             'GetOwnIdentities'})[0]

    if response['header'] != 'FCPPluginReply' or \
            'Replies.Message' not in response or \
            response['Replies.Message'] != 'OwnIdentities':
        raise util.Abort("Unexpected reply. Got {0}\n.".format(response))

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
        raise util.Abort("'{0}' matches more than one local identity."
                         .format(wot_identifier))

    if len(matches) == 0:
        raise util.Abort("No local identities match '{0}'."
                         .format(wot_identifier))

    assert len(matches) == 1

    # id_num is first member of value tuple.
    only_key = matches.keys()[0]
    id_num = matches[only_key][0]

    return id_num, response


def _parse_name(wot_identifier):
    """
    Internal.

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
