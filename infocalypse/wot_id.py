import fcp
from mercurial import util, error
import string
import atexit
from .keys import USK
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

    def __init__(self, wot_identifier, truster, id_num=0, message=None, fcpopts={}):
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
        is_local_identity = message is not None
        if not message:
            message = _get_identity(wot_identifier, truster, fcpopts=fcpopts)

        def get_attribute(attribute):
            return message['Replies.{0}{1}'.format(attribute, id_num)]

        self.nickname = get_attribute('Nickname')
        self.request_uri = USK(get_attribute('RequestURI').encode("utf-8"))
        self.identity_id = get_attribute('Identity')

        self.contexts = []
        self.properties = {}
        context_prefix = "Replies.Contexts{0}.Context".format(id_num)
        property_prefix = "Replies.Properties{0}.Property".format(id_num)
        for key in message.keys():
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
                    print(("WARNING: '{0}' has conflicting value as a property."
                          .format(name)))

                self.properties[name] = value

        # Freemail addresses encode the public key hash with base32 instead of
        # base64 as WoT does. This is to be case insensitive because email
        # addresses are not case sensitive, so some clients may mangle case.
        # See:
        # https://github.com/zidel/Freemail/blob/v0.2.2.1/docs/spec/spec.tex#L32

        if not 'Freemail' in self.contexts:
            self.freemail_address = None
        else:
            re_encode = b32encode(base64decode(self.identity_id)).decode("utf-8")
            # Remove trailing '=' padding.
            re_encode = re_encode.rstrip('=')

            # Freemail addresses are lower case.
            self.freemail_address = (self.nickname + '@' + re_encode
                                                 + '.freemail').lower()

        # TODO: Would it be preferable to use ui to obey quieting switches?
        if is_local_identity:
            print(("Using local identity {0}".format(self)))
        else:
            print(("Using identity {0}".format(self)))

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

    def __init__(self, wot_identifier, fcpopts={}):
        """
        Create a WoT_ID for a local identity matching the identifier.

        :type wot_identifier: str
        """
        id_num, message = _get_local_identity(wot_identifier, fcpopts=fcpopts)

        self.insert_uri = USK(message['Replies.InsertURI{0}'.format(id_num)].encode("utf-8"))

        WoT_ID.__init__(self, None, None, id_num=id_num, message=message, fcpopts=fcpopts)


def _request_matching_identities_lcwot(truster, context="vcs", prefix=None, fcpopts={}):
    """
    Return a response for a partial nickname request.
    """
    nickname_prefix, key_prefix = _parse_name(prefix)
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode(**fcpopts)
    atexit.register(node.shutdown)

    # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
    # src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname
    # TODO: LCWoT allows limiting by context; should we make sure otherwise?
    # Feature request for WoT: https://bugs.freenetproject.org/view.php?id=6184

    # GetIdentitiesByPartialNickname does not support empty nicknames.
    if not nickname_prefix:
        raise error.Abort(
            "Partial matching in LCWoT does not support empty nicknames. Got {}"
            .format(prefix))
    
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
        raise error.Abort('Unexpected reply. Got {0}\n'.format(response))
            
    return response
            
        
def _request_matching_identities(truster, context="vcs", prefix=None, fcpopts={}):
    """
    Return a list of responses for all matching identities.
    """
    node = fcp.FCPNode(**fcpopts)
    atexit.register(node.shutdown)
    params = {'Message': 'GetIdentities', # GetIdentitiesByScore is much slower
              'Truster': truster.identity_id}

    if context:
        params['Context'] = context
    
    response = node.fcpPluginMessage(
        plugin_name="plugins.WebOfTrust.WebOfTrust",
        plugin_params=params)[0]
    
    if response['header'] != 'FCPPluginReply' or \
       'Replies.Message' not in response:
        raise error.Abort('Unexpected reply. Got {0}\n'.format(response))
    nIDs = int(response["Replies.Identities.Amount"])

    def get_attribute(attribute, i, message):
        return message['Replies.Identities.{0}.{1}'.format(i, attribute)]
    
    responses = []
    for i in range(nIDs):
        identifier = "@".join(
            (get_attribute("Nickname", i, response),
             get_attribute("Identity", i, response)))
        if not prefix or identifier.startswith(prefix):
            responses.append(_get_identity(identifier, truster, exact=True, fcpopts=fcpopts))
    return responses
        
        
def _get_identity(wot_identifier, truster, exact=False, fcpopts={}):
    """
    Internal.

    Return an FCP reply from WoT for an identity on the truster's trust list
    matching the identifier. Abort if anything but exactly one match is found.

    :type wot_identifier: str
    :type truster: Local_WoT_ID
    :param exact: Whether to match the wot_identifier exactly or use it as prefix.
    """
    nickname_prefix, key_prefix = _parse_name(wot_identifier)
    # TODO: Support different FCP IP / port.
    node = fcp.FCPNode(**fcpopts)
    atexit.register(node.shutdown)

    if not exact:
        # Test for GetIdentitiesByPartialNickname support. currently LCWoT-only.
        # src/main/java/plugins/WebOfTrust/fcp/GetIdentitiesByPartialNickname
        # TODO: LCWoT allows limiting by context; should we make sure otherwise?
        # Feature request for WoT: https://bugs.freenetproject.org/view.php?id=6184
    
        # GetIdentitiesByPartialNickname does not support empty nicknames.
        try:
            response = _request_matching_identities_lcwot(
                truster, context="vcs", prefix=wot_identifier, fcpopts=fcpopts)
            if response['Replies.Message'] == 'Identities':
                matches = response['Replies.IdentitiesMatched']
            else:
                raise error.Abort("WoT does not support partial matching.")
        except error.Abort:
            all_responses = _request_matching_identities(truster, prefix=wot_identifier, fcpopts=fcpopts)
            matches = len(all_responses)
            if matches:
                response = all_responses[0]
        
        if matches == 0:
            raise error.Abort("No identities match '{0}'."
                             .format(wot_identifier))
        elif matches == 1:
            return response
        else:
            # TODO: Ask the user to choose interactively (select 1, 2, 3, ...)
            raise error.Abort("'{0}' matches more than one identity."
                             .format(wot_identifier))

    # exact matching requested. The key_prefix must be the complete key.
    # key_prefix must be a complete key for the lookup to succeed.
    params = {'Message': 'GetIdentity',
              'Truster': truster.identity_id,
              'Identity': key_prefix}
    response = \
        node.fcpPluginMessage(plugin_name="plugins.WebOfTrust.WebOfTrust",
                              plugin_params=params)[0]

    if response['Replies.Message'] == 'Error':
        # Searching by exact public key hash, not matching.
        raise error.Abort("No identity has the complete public key hash '{0}'. "
                         "({1}). Error: {2}"
                         .format(key_prefix, wot_identifier, response.get('Replies.Message', "")))

    # There should be only one result.
    # Depends on https://bugs.freenetproject.org/view.php?id=5729
    return response


def _get_local_identity(wot_identifier, fcpopts={}):
    """
    Internal.

    Return (id_number, FCP reply) from WoT for a local identity matching the
    identifier. Abort if anything but exactly one match is found.

    :type wot_identifier: str
    """
    nickname_prefix, key_prefix = _parse_name(wot_identifier)

    node = fcp.FCPNode(**fcpopts)
    atexit.register(node.shutdown)
    # print("get local identity for", wot_identifier, fcpopts)
    plugin_name = "plugins.WebOfTrust.WebOfTrust"
    plugin_params = {'Message':
                     'GetOwnIdentities'}
    # print(plugin_name, plugin_params)
    response = \
        node.fcpPluginMessage(plugin_name=plugin_name,
                              plugin_params=plugin_params)[0]
    # print(response)
    if response['header'] != 'FCPPluginReply' or \
            'Replies.Message' not in response or \
            response['Replies.Message'] != 'OwnIdentities':
        raise error.Abort(b"Unexpected reply. Got %b\n." % str(response).encode("utf-8"))

    # Find nicknames starting with the supplied nickname prefix.
    prefix = 'Replies.Nickname'
    # Key: nickname, value (id_num, public key hash).
    matches = {}
    for key in response.keys():
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
    for key in list(matches.keys()):
        # public key hash is second member of value tuple.
        if not matches[key][1].startswith(key_prefix):
            del matches[key]

    if len(matches) > 1:
        raise error.Abort(b"'%b' matches more than one local identity."
                          % wot_identifier.encode("utf-8"))

    if len(matches) == 0:
        raise error.Abort(b"No local identities match '%b'."
                         % wot_identifier.encode("utf-8"))

    assert len(matches) == 1

    # id_num is first member of value tuple.
    only_key = list(matches.keys())[0]
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
