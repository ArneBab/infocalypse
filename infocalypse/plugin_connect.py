from signal import signal, SIGINT
from time import sleep
import fcp
import threading
from mercurial import util
import sys
from config import Config
from wot_id import WoT_ID, Local_WoT_ID
import wot

PLUGIN_NAME = "org.freenetproject.plugin.dvcs_webui.main.Plugin"


def connect(ui, repo):
    """
    Connect to the WebUI plugin to provide local support.
    
    TODO: Add command option handling (fcphost and fcpport).
    """
    node = fcp.FCPNode()

    ui.status("Connecting.\n")

    # TODO: Would it be worthwhile to have a wrapper that includes PLUGIN_NAME?
    # TODO: Where to document the spec? devnotes.txt? How to format?
    hi_there = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                     plugin_params={'Message': 'Hello',
                                                    'VoidQuery': 'true'})[0]

    if hi_there['header'] == 'Error':
        raise util.Abort("The DVCS web UI plugin is not loaded.")

    if hi_there['Replies.Message'] == 'Error':
        # TODO: Debugging
        print hi_there
        raise util.Abort("Another VCS instance is already connected.")

    session_token = hi_there['Replies.SessionToken']

    ui.status("Connected.\n")

    def disconnect(signum, frame):
        ui.status("Disconnecting.\n")
        node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                              plugin_params=
                              {'Message': 'Disconnect',
                               'SessionToken': session_token})
        sys.exit()

    # Send Disconnect on interrupt instead of waiting on timeout.
    signal(SIGINT, disconnect)

    def ping():
        # Loop with delay.
        while True:
            pong = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                         plugin_params=
                                         {'Message': 'Ping',
                                          'SessionToken': session_token})[0]
            if pong['Replies.Message'] == 'Error':
                raise util.Abort(pong['Replies.Description'])
            elif pong['Replies.Message'] != 'Pong':
                ui.warn("Got unrecognized Ping reply '{0}'.\n".format(pong[
                        'Replies.Message']))

            # Wait for less than timeout threshold. In testing responses take
            # a little over a second.
            sleep(3.5)

    # Start self-perpetuating pinging in the background.
    t = threading.Timer(0.0, ping)
    # Daemon threads do not hold up the process exiting. Allows prompt
    # response to - for instance - SIGTERM.
    t.daemon = True
    t.start()

    while True:
        # Load the config each time - it could change.
        # TODO: Monitor config file for change events instead.
        cfg = Config.from_ui(ui)

        query_identifier = node._getUniqueId()
        # The event-querying is single-threaded, which makes things slow as
        # everything waits on the completion of the current operation.
        # Asynchronous code would require changes on the plugin side but
        # potentially have much lower latency.
        # TODO: Can wrap away PLUGIN_NAME, SessionToken, and QueryIdentifier?
        command = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                        plugin_params=
                                        {'Message': 'Ready',
                                         'SessionToken': session_token,
                                         'QueryIdentifier': query_identifier})[0]

        response = command['Replies.Message']
        if response == 'Error':
            raise util.Abort(command['Replies.Description'])

        if response not in handlers:
            raise util.Abort("Unsupported query '{0}'\n".format(response))

        ui.status("Got query: {0}\n".format(response))

        # Handlers are indexed by the query message name, take the query
        # message, and return (result_name, plugin_params).
        result_name, plugin_params = handlers[response](command, cfg=cfg,
                                                        ui=ui)

        plugin_params['Message'] = result_name
        plugin_params['QueryIdentifier'] = query_identifier
        plugin_params['SessionToken'] = session_token

        ack = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                    plugin_params=plugin_params)[0]

        if ack['Replies.Message'] != "Ack":
            raise util.Abort("Received unexpected message instead of result "
                             "acknowledgement:\n{0}\n".format(ack))

        ui.status("Query complete.\n")


# Handlers return two items: result message name, message-specific parameters.
# The sending code handles the plugin name, required parameters and plugin name.
# TODO: Is it reasonable to lock in the "NameQuery"/"NameResult" naming pattern?
# TODO: Docstrings on handlers. Or would it make more sense to document on
# the plugin side? Both?
# Keywords arguments handlers can use: TODO: Appropriate to ignore others?
# * cfg - configuration


def VoidQuery(_, **opts):
    return "VoidResult", {}


def LocalRepoQuery(_, cfg, **opts):
    params = {}
    # Request USKs are keyed by repo path.
    repo_index = 0
    for path in cfg.request_usks.iterkeys():
        params['Path.{0}'.format(repo_index)] = path
        repo_index += 1

    return "LocalRepoResult", params


def RepoListQuery(command, ui, **opts):
    params = {}

    # TODO: Failure should result in an error message sent to the plugin.
    # Truster is the ID of the identity only. Prepend '@' for identifier.
    truster = Local_WoT_ID('@' + command['Replies.Truster'])
    identity = WoT_ID(command['Replies.RemoteIdentifier'], truster)

    repo_list = wot.read_repo_listing(ui, identity)

    for name, key in repo_list.iteritems():
        params['Repo.' + name] = key

    return "RepoListResult", params


# TODO: Perhaps look up method by name directly?
handlers = {'VoidQuery': VoidQuery,
            'RepoListQuery': RepoListQuery,
            'LocalRepoQuery': LocalRepoQuery}
