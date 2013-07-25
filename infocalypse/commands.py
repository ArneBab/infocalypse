from binascii import hexlify
from mercurial import util

from infcmds import get_config_info, execute_create, execute_pull, \
    execute_push, execute_setup, execute_copy, execute_reinsert, \
    execute_info

from fmscmds import execute_fmsread, execute_fmsnotify, get_uri_from_hash, \
    execute_setupfms

from sitecmds import execute_putsite, execute_genkey
from wikicmds import execute_wiki, execute_wiki_apply
from arccmds import execute_arc_create, execute_arc_pull, execute_arc_push, \
    execute_arc_reinsert

from config import read_freesite_cfg, Config
from validate import is_hex_string, is_fms_id

import os


def set_target_version(ui_, repo, opts, params, msg_fmt):
    """ INTERNAL: Update TARGET_VERSION in params. """

    revs = opts.get('rev') or None
    if not revs is None:
        for rev in revs:
            repo.changectx(rev)  # Fail if we don't have the rev.

        params['TO_VERSIONS'] = tuple(revs)
        ui_.status(msg_fmt % ' '.join([ver[:12] for ver in revs]))
    else:
        # REDFLAG: get rid of default versions arguments?
        params['TO_VERSIONS'] = tuple([hexlify(head) for head in repo.heads()])
        #print "set_target_version -- using all head"
        #print params['TO_VERSIONS']


def infocalypse_create(ui_, repo, **opts):
    """ Create a new Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)

    insert_uri = ''
    attributes = None
    if opts['uri'] != '' and opts['wot'] != '':
        ui_.warn("Please specify only one of --uri or --wot.\n")
        return
    elif opts['uri'] != '':
        insert_uri = opts['uri']
    elif opts['wot'] != '':
        # Expecting nick_prefix/repo_name.R<redundancy num>/edition/
        nick_prefix, repo_desc = opts['wot'].split('/', 1)

        import wot

        ui_.status("Querying WoT for local identities.\n")

        attributes = wot.resolve_local_identity(ui_, nick_prefix)
        if attributes is None:
            # Something went wrong; the function already printed an error.
            return

        ui_.status('Found {0}@{1}\n'.format(attributes['Nickname'],
                                            attributes['Identity']))

        insert_uri = attributes['InsertURI']

        # LCWoT returns URIs with a "freenet:" prefix, and WoT does not. The
        # rest of Infocalypse does not support the prefix. The local way to fix
        # this is to remove it here, but the more flexible way that is also
        # more work is to expand support to the rest of Infocalypse.
        # TODO: More widespread support for "freenet:" URI prefix.
        prefix = "freenet:"
        if insert_uri.startswith(prefix):
            insert_uri = insert_uri[len(prefix):]

        # URI is USK@key/WebOfTrust/<edition>, but we only want USK@key
        insert_uri = insert_uri.split('/', 1)[0]
        insert_uri += '/' + repo_desc

        # Add "vcs" context. No-op if the identity already has it.
        msg_params = {'Message': 'AddContext',
                      'Identity': attributes['Identity'],
                      'Context': 'vcs'}

        import fcp
        node = fcp.FCPNode()
        vcs_response =\
            node.fcpPluginMessage(async=False,
                                  plugin_name="plugins.WebOfTrust.WebOfTrust",
                                  plugin_params=msg_params)[0]

        if vcs_response['header'] != 'FCPPluginReply' or\
                'Replies.Message' not in vcs_response or\
                vcs_response['Replies.Message'] != 'ContextAdded':
            ui_.warn("Failed to add context. Got {0}\n.".format(vcs_response))
            return

    else:
        ui_.warn("Please set the insert key with either --uri or --wot.\n")

    set_target_version(ui_, repo, opts, params,
                       "Only inserting to version(s): %s\n")
    params['INSERT_URI'] = insert_uri
    inserted_to = execute_create(ui_, repo, params, stored_cfg)

    if inserted_to and opts['wot']:
        # TODO: Would it be friendlier to include the nickname as well?
        # creation returns a list of request URIs; use the first.
        stored_cfg.set_wot_identity(inserted_to[0],
                                    attributes['Identity'])
        Config.to_file(stored_cfg)

        # TODO: Imports don't go out of scope, right? The variables
        # from the import are only visible in the function, so yes.
        import wot
        wot.update_repo_listing(ui_, attributes['Identity'])


def infocalypse_copy(ui_, repo, **opts):
    """ Copy an Infocalypse repository to a new URI. """
    params, stored_cfg = get_config_info(ui_, opts)

    insert_uri = opts['inserturi']
    if insert_uri == '':
        # REDFLAG: fix parameter definition so that it is required?
        ui_.warn("Please set the insert URI with --inserturi.\n")
        return

    request_uri = opts['requesturi']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --requesturi option.\n")
            return

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    execute_copy(ui_, repo, params, stored_cfg)


def infocalypse_reinsert(ui_, repo, **opts):
    """ Reinsert the current version of an Infocalypse repository. """
    params, stored_cfg = get_config_info(ui_, opts)

    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Do a fn-pull from a repository USK and try again.\n")
            return

    level = opts['level']
    if level < 1 or level > 5:
        ui_.warn("level must be 1,2,3,4 or 5.\n")
        return

    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri:
        if level == 1 or level == 4:
            ui_.warn(("You can't re-insert at level %i without the "
                     + "insert URI.\n") % level)
            return

        ui_.status("No insert URI. Will skip re-insert of top key.\n")
        insert_uri = None

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    params['REINSERT_LEVEL'] = level
    execute_reinsert(ui_, repo, params, stored_cfg)


def infocalypse_pull(ui_, repo, **opts):
    """ Pull from an Infocalypse repository in Freenet.
     """
    params, stored_cfg = get_config_info(ui_, opts)

    if opts['hash']:
        # Use FMS to lookup the uri from the repo hash.
        if opts['uri'] != '':
            ui_.warn("Ignoring --uri because --hash is set!\n")
        if len(opts['hash']) != 1:
            raise util.Abort("Only one --hash value is allowed.")
        params['FMSREAD_HASH'] = opts['hash'][0]
        params['FMSREAD_ONLYTRUSTED'] = bool(opts['onlytrusted'])
        request_uri = get_uri_from_hash(ui_, repo, params, stored_cfg)
    elif opts['wot']:
        import wot
        if opts['truster']:
            local_id = wot.resolve_local_identity(ui_, opts['truster'])
            truster = local_id['Identity']
        else:
            truster = stored_cfg.get_wot_identity(
                stored_cfg.get_dir_insert_uri(repo.root))

        request_uri = wot.resolve_pull_uri(ui_, opts['wot'], truster)

        if request_uri is None:
            return
    else:
        request_uri = opts['uri']

    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    params['REQUEST_URI'] = request_uri
    # Hmmmm... can't really implement rev.
    execute_pull(ui_, repo, params, stored_cfg)


def infocalypse_pull_request(ui, repo, **opts):
    import wot
    if not opts['wot']:
        raise util.Abort("Who do you want to send the pull request to? Set "
                         "--wot.\n")

    wot_id, repo_name = opts['wot'].split('/', 1)
    wot.send_pull_request(ui, repo, get_truster(ui, repo, opts), wot_id,
                          repo_name)


def infocalypse_check_notifications(ui, repo, **opts):
    import wot
    if not opts['wot']:
        raise util.Abort("What ID do you want to check for notifications? Set"
                         " --wot.\n")

    wot.check_notifications(ui, opts['wot'])


def infocalypse_connect(ui, repo, **opts):
    import wot
    wot.connect(ui, repo)


def infocalypse_push(ui_, repo, **opts):
    """ Push to an Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)
    insert_uri = opts['uri']
    if insert_uri == '':
        insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    set_target_version(ui_, repo, opts, params,
                       "Only pushing to version(s): %s\n")
    params['INSERT_URI'] = insert_uri
    #if opts['requesturi'] != '':
    #    # DOESN'T search the insert uri index.
    #    ui_.status(("Copying from:\n%s\nTo:\n%s\n\nThis is an "
    #                + "advanced feature. "
    #                + "I hope you know what you're doing.\n") %
    #               (opts['requesturi'], insert_uri))
    #    params['REQUEST_URI'] = opts['requesturi']

    inserted_to = execute_push(ui_, repo, params, stored_cfg)
    # TODO: Messy. Is there a better way to check what repos are under WoT?
    request_uri = stored_cfg.get_request_uri(repo.root)
    if inserted_to is not None and stored_cfg.has_wot_identity(request_uri):
        import wot
        wot.update_repo_listing(ui_, stored_cfg.get_wot_identity(request_uri))


def infocalypse_info(ui_, repo, **opts):
    """ Display information about an Infocalypse repository.
     """
    # FCP not required. Hmmm... Hack
    opts['fcphost'] = ''
    opts['fcpport'] = 0
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    params['REQUEST_URI'] = request_uri
    execute_info(ui_, repo, params, stored_cfg)


def parse_trust_args(params, opts):
    """ INTERNAL: Helper function to parse  --hash and --fmsid. """
    if not opts.get('hash', []):
        raise util.Abort("Use --hash to set the USK hash.")
    if len(opts['hash']) != 1:
        raise util.Abort("Only one --hash value is allowed.")
    if not is_hex_string(opts['hash'][0]):
        raise util.Abort("[%s] doesn't look like a USK hash." %
                         opts['hash'][0])

    if not opts.get('fmsid', []):
        raise util.Abort("Use --fmsid to set the FMS id.")
    if len(opts['fmsid']) != 1:
        raise util.Abort("Only one --fmsid value is allowed.")
    if not is_fms_id(opts['fmsid'][0]):
        raise util.Abort("[%s] doesn't look like an FMS id."
                         % opts['fmsid'][0])

    params['FMSREAD_HASH'] = opts['hash'][0]
    params['FMSREAD_FMSID'] = opts['fmsid'][0]


def parse_fmsread_subcmd(params, opts):
    """ INTERNAL: Parse subcommand for fmsread."""
    if opts['listall']:
        params['FMSREAD'] = 'listall'
    elif opts['list']:
        params['FMSREAD'] = 'list'
    elif opts['showtrust']:
        params['FMSREAD'] = 'showtrust'
    elif opts['trust']:
        params['FMSREAD'] = 'trust'
        parse_trust_args(params, opts)
    elif opts['untrust']:
        params['FMSREAD'] = 'untrust'
        parse_trust_args(params, opts)
    else:
        params['FMSREAD'] = 'update'


def infocalypse_fmsread(ui_, repo, **opts):
    """ Read repository update information from fms.
    """
    # FCP not required. Hmmm... Hack
    opts['fcphost'] = ''
    opts['fcpport'] = 0
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if request_uri == '':
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.status("There is no stored request URI for this repo.\n")
            request_uri = None
    parse_fmsread_subcmd(params, opts)
    params['DRYRUN'] = opts['dryrun']
    params['REQUEST_URI'] = request_uri
    execute_fmsread(ui_, params, stored_cfg)


def infocalypse_fmsnotify(ui_, repo, **opts):
    """ Post a msg with the current repository USK index to fms.
    """
    params, stored_cfg = get_config_info(ui_, opts)
    insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
    if not insert_uri and not (opts['submitbundle'] or
                               opts['submitwiki']):
        ui_.warn("You can't notify because there's no stored "
                 + "insert URI for this repo.\n"
                 + "Run from the directory you inserted from.\n")
        return

    params['ANNOUNCE'] = opts['announce']
    params['SUBMIT_BUNDLE'] = opts['submitbundle']
    params['SUBMIT_WIKI'] = opts['submitwiki']
    if params['SUBMIT_WIKI'] or params['SUBMIT_BUNDLE']:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n")
            raise util.Abort("No request URI.")
        params['REQUEST_URI'] = request_uri

    params['DRYRUN'] = opts['dryrun']
    params['INSERT_URI'] = insert_uri
    execute_fmsnotify(ui_, repo, params, stored_cfg)

MSG_BAD_INDEX = 'You must set --index to a value >= 0.'


def infocalypse_putsite(ui_, repo, **opts):
    """ Insert an update to a freesite.
    """

    if opts['createconfig']:
        if opts['wiki']:
            raise util.Abort("Use fn-wiki --createconfig.")
        params = {'SITE_CREATE_CONFIG': True}
        execute_putsite(ui_, repo, params)
        return

    params, stored_cfg = get_config_info(ui_, opts)
    if opts['key'] != '':  # order important
        params['SITE_KEY'] = opts['key']
        if not (params['SITE_KEY'].startswith('SSK') or
                params['SITE_KEY'] == 'CHK@'):
            raise util.Abort("--key must be a valid SSK "
                             + "insert key or CHK@.")

    params['ISWIKI'] = opts['wiki']
    read_freesite_cfg(ui_, repo, params, stored_cfg)

    try:
        # --index not required for CHK@
        if not params['SITE_KEY'].startswith('CHK'):
            params['SITE_INDEX'] = int(opts['index'])
            if params['SITE_INDEX'] < 0:
                raise ValueError()
        else:
            params['SITE_INDEX'] = -1
    except ValueError:
        raise util.Abort(MSG_BAD_INDEX)
    except TypeError:
        raise util.Abort(MSG_BAD_INDEX)

    params['DRYRUN'] = opts['dryrun']

    if not params.get('SITE_KEY', None):
        insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
        if not insert_uri:
            ui_.warn("You don't have the insert URI for this repo.\n"
                     + "Supply a private key with --key or fn-push "
                     + "the repo.\n")
            return  # REDFLAG: hmmm... abort?
        params['SITE_KEY'] = 'SSK' + insert_uri.split('/')[0][3:]

    execute_putsite(ui_, repo, params)


def infocalypse_wiki(ui_, repo, **opts):
    """ View and edit the current repository as a wiki. """
    if os.getcwd() != repo.root:
        raise util.Abort("You must be in the repository root directory.")

    subcmds = ('run', 'createconfig', 'apply')
    required = sum([bool(opts[cmd]) for cmd in subcmds])
    if required == 0:
        raise util.Abort("You must specify either --run, " +
                         "--createconfig, --apply")
    if required > 1:
        raise util.Abort("Use either --run, --createconfig, or --apply")

    if opts['apply'] != '':
        params, stored_cfg = get_config_info(ui_, opts)
        params['REQUEST_URI'] = opts['apply']
        execute_wiki_apply(ui_, repo, params, stored_cfg)
        return

    if opts['fcphost'] != '' or opts['fcpport'] != 0:
        raise util.Abort("--fcphost, --fcpport only for --apply")

    # hmmmm.... useless copy?
    params = {'WIKI': [cmd for cmd in subcmds if opts[cmd]][0],
              'HTTP_PORT': opts['http_port'],
              'HTTP_BIND': opts['http_bind']}
    execute_wiki(ui_, repo, params)


def infocalypse_genkey(ui_, **opts):
    """ Print a new SSK key pair. """
    params, dummy = get_config_info(ui_, opts)
    execute_genkey(ui_, params)


def infocalypse_setup(ui_, **opts):
    """ Setup the extension for use for the first time. """

    execute_setup(ui_,
                  opts['fcphost'],
                  opts['fcpport'],
                  opts['tmpdir'])

    if not opts['nofms']:
        execute_setupfms(ui_, opts)
    else:
        ui_.status("Skipped FMS configuration because --nofms was set.\n")

    if not opts['nowot']:
        import wot
        wot.execute_setup_wot(ui_, opts)
    else:
        ui_.status("Skipped WoT configuration because --nowot was set.\n")


def infocalypse_setupfms(ui_, **opts):
    """ Setup or modify the fms configuration. """
    # REQUIRES config file.
    execute_setupfms(ui_, opts)


# TODO: Why ui with trailing underscore? Is there a global "ui" somewhere?
def infocalypse_setupwot(ui_, **opts):
    import wot
    wot.execute_setup_wot(ui_, opts)


def infocalypse_setupfreemail(ui, repo, **opts):
    """
    Set a Freemail password. If --truster is not given uses the default
    truster.
    """
    import wot
    # TODO: Here --truster doesn't make sense. There is no trust involved.
    # TODO: Should this be part of the normal fn-setup?
    wot.execute_setup_freemail(ui, get_truster(ui, repo, opts))


def get_truster(ui, repo, opts):
    if opts['truster']:
        return opts['truster']
    else:
        cfg = Config().from_ui(ui)
        # TODO: What about if there's no request URI set?
        return '@' + cfg.get_wot_identity(cfg.get_request_uri(repo.root))

#----------------------------------------------------------"


def do_archive_create(ui_, opts, params, stored_cfg):
    """ fn-archive --create."""
    insert_uri = opts['uri']
    if insert_uri == '':
        raise util.Abort("Please set the insert URI with --uri.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()
    execute_arc_create(ui_, params, stored_cfg)


def do_archive_push(ui_, opts, params, stored_cfg):
    """ fn-archive --push."""
    insert_uri = opts['uri']
    if insert_uri == '':
        insert_uri = (
            stored_cfg.get_dir_insert_uri(params['ARCHIVE_CACHE_DIR']))
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise util.Abort("No Insert URI.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()

    execute_arc_push(ui_, params, stored_cfg)


def do_archive_pull(ui_, opts, params, stored_cfg):
    """ fn-archive --pull."""
    request_uri = opts['uri']

    if request_uri == '':
        request_uri = (
            stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR']))
        if not request_uri:
            ui_.warn("There is no stored request URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise util.Abort("No request URI.")

    params['REQUEST_URI'] = request_uri
    params['TO_DIR'] = os.getcwd()
    execute_arc_pull(ui_,  params, stored_cfg)

ILLEGAL_FOR_REINSERT = ('uri', 'aggressive', 'nosearch')


def do_archive_reinsert(ui_, opts, params, stored_cfg):
    """ fn-archive --reinsert."""
    illegal = [value for value in ILLEGAL_FOR_REINSERT
               if value in opts and opts[value]]
    if illegal:
        raise util.Abort("--uri, --aggressive, --nosearch illegal " +
                         "for reinsert.")
    request_uri = stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR'])
    if request_uri is None:
        ui_.warn("There is no stored request URI for this archive.\n" +
                 "Run fn-archive --pull first!.\n")
        raise util.Abort(" No request URI, can't re-insert")

    insert_uri = stored_cfg.get_dir_insert_uri(params['ARCHIVE_CACHE_DIR'])
    params['REQUEST_URI'] = request_uri
    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()  # hmmm not used.
    params['REINSERT_LEVEL'] = 3
    execute_arc_reinsert(ui_, params, stored_cfg)

ARCHIVE_SUBCMDS = {'create': do_archive_create,
                   'push': do_archive_push,
                   'pull': do_archive_pull,
                   'reinsert': do_archive_reinsert}
ARCHIVE_CACHE_DIR = '.ARCHIVE_CACHE'


def infocalypse_archive(ui_, **opts):
    """ Commands to maintain a non-hg incremental archive."""
    subcmd = [value for value in ARCHIVE_SUBCMDS if opts[value]]
    if len(subcmd) > 1:
        raise util.Abort("--create, --pull, --push are mutally exclusive. " +
                         "Only specify one.")
    if len(subcmd) > 0:
        subcmd = subcmd[0]
    else:
        subcmd = "pull"

    params, stored_cfg = get_config_info(ui_, opts)
    params['ARCHIVE_CACHE_DIR'] = os.path.join(os.getcwd(), ARCHIVE_CACHE_DIR)

    if not subcmd in ARCHIVE_SUBCMDS:
        raise util.Abort("Unhandled subcommand: " + subcmd)

    # 2 qt?
    ARCHIVE_SUBCMDS[subcmd](ui_, opts, params, stored_cfg)
