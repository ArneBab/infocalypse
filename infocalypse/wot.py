import fcp
from mercurial import util
from config import Config
import xml.etree.ElementTree as ET
from defusedxml.ElementTree import fromstring
import smtplib
from keys import USK
import yaml
from email.mime.text import MIMEText
import imaplib
import threading
from wot_id import Local_WoT_ID, WoT_ID

FREEMAIL_SMTP_PORT = 4025
FREEMAIL_IMAP_PORT = 4143
VCS_TOKEN = "[vcs]"
PLUGIN_NAME = "org.freenetproject.plugin.infocalypse_webui.main.InfocalypsePlugin"


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


def send_pull_request(ui, repo, from_identity, to_identity, to_repo_name):
    """
    Prompt for a pull request message, and send a pull request from
    from_identity to to_identity for the repository to_repo_name.

    :type to_identity: WoT_ID
    :type from_identity: Local_WoT_ID
    """
    from_address = require_freemail(from_identity)
    to_address = require_freemail(to_identity)

    cfg = Config.from_ui(ui)
    password = cfg.get_freemail_password(from_identity.identity_id)

    to_repo = find_repo(ui, to_identity, to_repo_name)

    repo_context = repo['tip']
    # TODO: Will there always be a request URI set in the config? What about
    # a path? The repo could be missing a request URI, if that URI is
    # set manually. We could check whether the default path is a
    # freenet path. We cannot be sure whether the request uri will
    # always be the uri we want to send the pull-request to, though:
    # It might be an URI we used to get some changes which we now want
    # to send back to the maintainer of the canonical repo.
    from_uri = cfg.get_request_uri(repo.root)
    from_branch = repo_context.branch()

    # Use double-quoted scalars so that Unicode can be included. (Nicknames.)
    # "infocalypse" is lower case in case it is used somewhere mixed case can
    # cause problems like a filesystem path.
    footer = yaml.dump({'request': 'pull',
                        'vcs': 'infocalypse',
                        'source': from_uri + '#' + from_branch,
                        'target': to_repo}, default_style='"',
                       explicit_start=True, explicit_end=True,
                       allow_unicode=True)

    # TODO: Break config sanity check and sending apart so that further
    # things can check config, prompt for whatever, then send.

    source_text = ui.edit("""

HG: Enter pull request message here. Lines beginning with 'HG:' are removed.
HG: The first line has "{0}" added before it in transit and is the subject.
HG: The second line is ignored.
HG: Subsequent lines are the body of the message.
""".format(VCS_TOKEN), str(from_identity))
    # TODO: Save message and load later in case sending fails.

    source_lines = source_text.splitlines()

    source_lines = [line for line in source_lines if not line.startswith('HG:')]

    if not ''.join(source_lines).strip():
        raise util.Abort("Empty pull request message.")

    # Body is third line and after.
    msg = MIMEText('\n'.join(source_lines[2:]) + footer)
    msg['Subject'] = VCS_TOKEN + ' ' + source_lines[0]
    msg['To'] = to_address
    msg['From'] = from_address

    smtp = smtplib.SMTP(cfg.defaults['HOST'], FREEMAIL_SMTP_PORT)
    smtp.login(from_address, password)
    # TODO: Catch exceptions and give nice error messages.
    smtp.sendmail(from_address, to_address, msg.as_string())

    ui.status("Pull request sent.\n")


def check_notifications(ui, local_identity):
    """
    Check Freemail for local_identity and print information on any VCS
    messages received.

    :type local_identity: Local_WoT_ID
    """
    address = require_freemail(local_identity)

    # Log in and open inbox.
    cfg = Config.from_ui(ui)
    imap = imaplib.IMAP4(cfg.defaults['HOST'], FREEMAIL_IMAP_PORT)
    imap.login(address, cfg.get_freemail_password(local_identity))
    imap.select()

    # Parenthesis to work around erroneous quotes:
    # http://bugs.python.org/issue917120
    reply_type, message_numbers = imap.search(None, '(SUBJECT %s)' % VCS_TOKEN)

    # imaplib returns numbers in a singleton string separated by whitespace.
    message_numbers = message_numbers[0].split()

    if not message_numbers:
        # TODO: Is aborting appropriate here? Should this be ui.status and
        # return?
        raise util.Abort("No notifications found.")

    # fetch() expects strings for both. Individual message numbers are
    # separated by commas. It seems desirable to peek because it's not yet
    # apparent that this is a [vcs] message with YAML.
    # Parenthesis to prevent quotes: http://bugs.python.org/issue917120
    status, subjects = imap.fetch(','.join(message_numbers),
                                  r'(body[header.fields Subject])')

    # Expecting 2 list items from imaplib for each message, for example:
    # ('5 (body[HEADER.FIELDS Subject] {47}', 'Subject: [vcs]  ...\r\n\r\n'),
    # ')',

    # Exclude closing parens, which are of length one.
    subjects = filter(lambda x: len(x) == 2, subjects)

    subjects = [x[1] for x in subjects]

    # Match message numbers with subjects; remove prefix and trim whitespace.
    subjects = dict((message_number, subject[len('Subject: '):].rstrip()) for
                    message_number, subject in zip(message_numbers, subjects))

    for message_number, subject in subjects.iteritems():
        status, fetched = imap.fetch(str(message_number),
                                     r'(body[text] '
                                     r'body[header.fields From)')

        # Expecting 3 list items, as with the subject fetch above.
        body = fetched[0][1]
        from_address = fetched[1][1][len('From: '):].rstrip()

        read_message_yaml(ui, from_address, subject, body)


def read_message_yaml(ui, from_address, subject, body):
    """
    Print information about the given message.
    """
    # Get consistent line endings.
    body = '\n'.join(body.splitlines())
    yaml_start = body.rfind('---\n')
    # The .join() does not add a trailing newline, and the end token might be
    # the last line.
    end_token = '...'
    yaml_end = body.rfind(end_token)

    cfg = Config.from_ui(ui)

    if not yaml_end == -1:
        # Better to point to the end of the end token, but don't confuse
        # failure.
        yaml_end += len(end_token)

    if yaml_start == -1 or yaml_end == -1:
        ui.status("Notification '%s' does not have a request.\n" % subject)
        return

    def require(field, request):
        if field not in request:
            ui.status("Notification '%s' has a properly formatted request "
                      "that does not include necessary information. ('%s')\n"
                      % (subject, field))
            return False
        return True

    try:
        request = yaml.safe_load(body[yaml_start:yaml_end])

        if not require('vcs', request) or not require('request', request):
            return
    except yaml.YAMLError, e:
        ui.status("Notification '%s' has a request but it is not properly"
                  " formatted. Details:\n%s\n" % (subject, e))
        return

    # "infocalypse" is lower case in case it is used somewhere mixed case can
    # cause problems like a filesystem path.
    if request['vcs'] != 'infocalypse':
        ui.status("Notification '%s' is for '%s', not Infocalypse.\n"
                  % (subject, request['vcs']))
        return

    if request['request'] == 'pull':
        ui.status("Found pull request from '%s':\n" % from_address)
        separator = ('-' * len(subject)) + '\n'

        ui.status(separator)
        ui.status(subject[subject.find(VCS_TOKEN) + len(VCS_TOKEN):] + '\n')

        ui.status(separator)
        ui.status(body[:yaml_start] + '\n')
        ui.status(separator)

        ui.status("To accept this request, pull from: %s\n"
                  "               To your repository: %s\n" %
                  (request['source'], cfg.get_repo_dir(request['target'])))
        return

    ui.status("Notification '%s' has an unrecognized request of type '%s'"
              % (subject, request['request']))


def require_freemail(wot_identity):
    """
    Return the given identity's Freemail address.
    Abort with an error message if the given identity does not have a
    Freemail address / context.

    :type wot_identity: WoT_ID
    """
    if not wot_identity.freemail_address:
        raise util.Abort("{0} is not using Freemail.\n".format(wot_identity))

    return wot_identity.freemail_address


def update_repo_listing(ui, for_identity):
    """
    Insert list of repositories published by the given identity.

    :type for_identity: Local_WoT_ID
    """
    # TODO: WoT property containing repo list edition. Used when requesting.
    # Version number to support possible format changes.
    root = ET.Element('vcs', {'version': '0'})

    ui.status("Updating repo listing for '%s'\n" % for_identity)

    for request_uri in build_repo_list(ui, for_identity):
        repo = ET.SubElement(root, 'repository', {
            'vcs': 'Infocalypse',
        })
        repo.text = request_uri

    # TODO: Nonstandard IP and port.
    node = fcp.FCPNode()

    insert_uri = for_identity.insert_uri.clone()

    # TODO: Somehow store the edition, perhaps in ~/.infocalypse. WoT
    # properties are apparently not appropriate.

    insert_uri.name = 'vcs'
    insert_uri.edition = '0'

    ui.status("Inserting with URI:\n{0}\n".format(insert_uri))
    uri = node.put(uri=str(insert_uri), mimetype='application/xml',
                   data=ET.tostring(root), priority=1)

    if uri is None:
        ui.warn("Failed to update repository listing.")
    else:
        ui.status("Updated repository listing:\n{0}\n".format(uri))


def build_repo_list(ui, for_identity):
    """
    Return a list of request URIs to repos for the given local identity.

    :type for_identity: Local_WoT_ID
    :param ui: to provide feedback
    :param for_identity: local WoT identity to list repos for.
    """
    config = Config.from_ui(ui)

    repos = []

    # Add request URIs associated with the given identity.
    for request_uri in config.request_usks.itervalues():
        if config.get_wot_identity(request_uri) == for_identity.identity_id:
            repos.append(request_uri)

    return repos


def find_repo(ui, identity, repo_name):
    """
    Return a request URI for a repo of the given name published by an
    identity matching the given identifier.
    Raise util.Abort if unable to read repo listing or a repo by that name
    does not exist.

    :type identity: WoT_ID
    """
    listing = read_repo_listing(ui, identity)

    if repo_name not in listing:
        raise util.Abort("{0} does not publish a repo named '{1}'\n"
                         .format(identity, repo_name))

    return listing[repo_name]


def read_repo_listing(ui, identity):
    """
    Read a repo listing for a given identity.
    Return a dictionary of repository request URIs keyed by name.

    :type identity: WoT_ID
    """
    uri = identity.request_uri.clone()
    uri.name = 'vcs'
    uri.edition = 0

    # TODO: Set and read vcs edition property.
    node = fcp.FCPNode()
    ui.status("Fetching {0}\n".format(uri))
    # TODO: What exception can this throw on failure? Catch it,
    # print its description, and return None.
    mime_type, repo_xml, msg = node.get(str(uri), priority=1,
                                        followRedirect=True)

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
        Print an error message and abort on failure.

        TODO: Is it appropriate to outline possible errors?
        Possible failures are being unable to fetch a repo list for the given
        identity, which may be a fetch failure or being unable to find the
        identity, and not finding the requested repo in the list.

        :type truster: Local_WoT_ID
        :param ui: For feedback.
        :param path: path describing a repo. nick@key/reponame
        :param truster: identity whose trust list to use.
        :return:
        """
        # Expecting <id stuff>/reponame
        wot_id, repo_name = path.split('/', 1)

        identity = WoT_ID(wot_id, truster)

        # TODO: How to handle redundancy? Does Infocalypse automatically try
        # an R0 if an R1 fails?

        return find_repo(ui, identity, repo_name)


def resolve_push_uri(ui, path):
    """
    Return a push URI for the given path.
    Raise util.Abort if unable to resolve identity or repository.

    :param ui: For feedback.
    :param path: path describing a repo - nick@key/repo_name,
    where the identity is a local one. (Such that the insert URI is known.)
    """
    # Expecting <id stuff>/repo_name
    wot_id, repo_name = path.split('/', 1)

    local_id = Local_WoT_ID(wot_id)

    # TODO: find_repo should make it clearer that it returns a request URI,
    # and return a USK.
    repo = find_repo(ui, local_id, repo_name)

    # Request URI
    repo_uri = USK(repo)

    # Maintains name, edition.
    repo_uri.key = local_id.insert_uri.key

    return str(repo_uri)


def execute_setup_wot(ui_, local_id):
    """
    Set WoT-related defaults.

    :type local_id: Local_WoT_ID
    """
    cfg = Config.from_ui(ui_)

    ui_.status("Setting default truster to {0}.\n".format(local_id))

    cfg.defaults['DEFAULT_TRUSTER'] = local_id.identity_id
    Config.to_file(cfg)


def execute_setup_freemail(ui, local_id):
    """
    Prompt for, test, and set a Freemail password for the identity.

    :type local_id: Local_WoT_ID
    """
    address = require_freemail(local_id)

    password = ui.getpass()
    if password is None:
        raise util.Abort("Cannot prompt for a password in a non-interactive "
                         "context.\n")

    ui.status("Checking password for {0}.\n".format(local_id))

    cfg = Config.from_ui(ui)

    # Check that the password works.
    try:
        # TODO: Is this the correct way to get the configured host?
        smtp = smtplib.SMTP(cfg.defaults['HOST'], FREEMAIL_SMTP_PORT)
        smtp.login(address, password)
    except smtplib.SMTPAuthenticationError, e:
        raise util.Abort("Could not log in using password '{0}'.\nGot '{1}'\n"
                         .format(password, e.smtp_error))
    except smtplib.SMTPConnectError, e:
        raise util.Abort("Could not connect to server.\nGot '{0}'\n"
                         .format(e.smtp_error))

    cfg.set_freemail_password(local_id, password)
    Config.to_file(cfg)
    ui.status("Password set.\n")
