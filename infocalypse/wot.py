import fcp

# Support for querying WoT for own identities and identities meeting various
# criteria.

def resolve_local_identity(ui, nickname_prefix=None):
    """
    Mercurial ui for status updates and error messages.
    Nickname prefix should be enough to not be ambiguous.
    # TODO: Does not support duplicate nicknames between local identities.
    # Could support looking at identity to resolve further.

    Returns a dictionary of the nickname, insert and request URIs,
    and identity that match the given criteria.
    In the case of an error prints a message and returns None.
    """
    ui.status("Querying WoT for local identities.\n")
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

    # Find nicknames starting with the supplied nickname prefix.
    prefix = 'Replies.Nickname'
    nickname = None
    id_num = -1
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

    # Return properties for the selected identity. (by number)
    result = {}
    for item in [ 'Nickname', 'InsertURI','RequestURI', 'Identity' ]:
        result[item] = response['Replies.{0}{1}'.format(item, id_num)]

    # LCWoT also puts these things as properties, which would be nicer to
    # depend on and would allow just returning all properties for the identity.
    #property_prefix = "Replies.Properties{0}".format(id_num)

    return result
