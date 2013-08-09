import fcp
import threading
from mercurial import util
from config import Config

PLUGIN_NAME = "org.freenetproject.plugin.dvcs_webui.main.Plugin"


def connect(ui, repo):
    node = fcp.FCPNode()

    # TODO: Should I be using this? Looks internal. The identifier needs to
    # be consistent though.
    fcp_id = node._getUniqueId()

    ui.status("Connecting as '%s'.\n" % fcp_id)

    def ping():
        pong = node.fcpPluginMessage(plugin_name=PLUGIN_NAME, id=fcp_id,
                                     plugin_params={'Message': 'Ping'})[0]
        if pong['Replies.Message'] == 'Error':
            raise util.Abort(pong['Replies.Description'])
        # Must be faster than the timeout threshold. (5 seconds)
        threading.Timer(4.0, ping).start()

    # Start self-perpetuating pinging in the background.
    t = threading.Timer(0.0, ping)
    # Daemon threads do not hold up the process exiting. Allows prompt
    # response to - for instance - SIGTERM.
    t.daemon = True
    t.start()

    while True:
        sequenceID = node._getUniqueId()
        # The event-querying is single-threaded, which makes things slow as
        # everything waits on the completion of the current operation.
        # Asynchronous code would require changes on the plugin side but
        # potentially have much lower latency.
        command = node.fcpPluginMessage(plugin_name=PLUGIN_NAME, id=fcp_id,
                                        plugin_params=
                                        {'Message': 'ClearToSend',
                                         'SequenceID': sequenceID})[0]
        # TODO: Look up handlers in a dictionary.
        print command

        # Reload the config each time - it may have changed between messages.
        cfg = Config.from_ui(ui)

        response = command['Replies.Message']
        if response == 'Error':
            raise util.Abort(command['Replies.Description'])
        elif response == 'ListLocalRepos':
            params = {'Message': 'RepoList',
                      'SequenceID': sequenceID}

            # Request USKs are keyed by repo path.
            repo_index = 0
            for path in cfg.request_usks.iterkeys():
                params['Repo%s' % repo_index] = path
                repo_index += 1

            ack = node.fcpPluginMessage(plugin_name=PLUGIN_NAME, id=fcp_id,
                                        plugin_params=params)[0]
            print ack

