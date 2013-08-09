import fcp
import threading
from mercurial import util
from config import Config

PLUGIN_NAME = "org.freenetproject.plugin.dvcs_webui.main.Plugin"


def connect(ui, repo):
    node = fcp.FCPNode()

    ui.status("Connecting.\n")

    # TODO: Would it be worthwhile to have a wrapper that includes PLUGIN_NAME?
    # TODO: Where to document the spec? devnotes.txt? How to format?
    hi_there = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                     plugin_params={'Message': 'Hello',
                                                    'GetRepoList': 'true'})[0]

    if hi_there['header'] == 'Error':
        raise util.Abort("The DVCS web UI plugin is not loaded.")

    if hi_there['Replies.Message'] == 'Error':
        raise util.Abort("Another VCS instance is already connected.")

    print "Connected."
    import sys
    sys.exit()

    def ping():
        pong = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
                                     plugin_params={'Message': 'Ping'})[0]
        if pong['Replies.Message'] == 'Error':
            raise util.Abort(pong['Replies.Description'])
        elif pong['Replies.Message'] != 'Pong':
            ui.warn("Got unrecognized Ping reply '{0}'.\n".format(pong[
                    'Replies.Message']))
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
        command = node.fcpPluginMessage(plugin_name=PLUGIN_NAME,
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

