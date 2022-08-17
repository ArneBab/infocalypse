from binascii import hexlify
from mercurial import util, error

from . import infcmds
get_config_info = infcmds.get_config_info
execute_create = infcmds.execute_create
execute_pull = infcmds.execute_pull
execute_push = infcmds.execute_push
execute_setup = infcmds.execute_setup
execute_copy = infcmds.execute_copy
execute_reinsert = infcmds.execute_reinsert
execute_info = infcmds.execute_info



from .fmscmds import execute_fmsread, execute_fmsnotify, get_uri_from_hash, \
    execute_setupfms

from .sitecmds import execute_putsite, execute_genkey
from .wikicmds import execute_wiki, execute_wiki_apply
from .arccmds import execute_arc_create, execute_arc_pull, execute_arc_push, \
    execute_arc_reinsert

from . import config

from .validate import is_hex_string, is_fms_id

import os
import atexit

from .keys import parse_repo_path, USK


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


def infocalypse_update_repo_list(ui, **opts):
    if not opts['wot']:
        raise error.Abort(b"Update which repository list? Use --wot")

    from . import wot
    from .wot_id import Local_WoT_ID
    wot.update_repo_listing(ui, Local_WoT_ID(opts['wot'], fcpopts=wot.get_fcpopts(ui,
        fcphost=opts["fcphost"],
        fcpport=opts["fcpport"])), 
                            fcphost=opts["fcphost"],
                            fcpport=opts["fcpport"])


def infocalypse_create(ui_, repo, local_identity=None, **opts):
    """ Create a new Infocalypse repository in Freenet.
    :type local_identity: Local_WoT_ID
    :param local_identity: If specified the new repository is associated with
                           that identity.
    """
    params, stored_cfg = get_config_info(ui_, opts)

    if opts['uri'] and opts['wot']:
        ui_.warn("Please specify only one of --uri or --wot.\n")
        return
    elif opts['uri']:
        insert_uri = parse_repo_path(opts['uri'])
    elif opts['wot']:
        opts['wot'] = parse_repo_path(opts['wot'])
        nick_prefix, repo_name, repo_edition = opts['wot'].split('/', 2)

        if not repo_name.endswith('.R1') and not repo_name.endswith('.R0'):
            ui_.warn("Warning: Creating repository without redundancy. (R0 or"
                     " R1)\n")

        from .wot_id import Local_WoT_ID

        local_identity = Local_WoT_ID(nick_prefix)

        insert_uri = local_identity.insert_uri.clone()

        insert_uri.name = repo_name
        insert_uri.edition = repo_edition
        # Before passing along into execute_create().
        insert_uri = str(insert_uri)
    else:
        ui_.warn("Please set the insert key with either --uri or --wot.\n")
        return

    # This is a WoT repository.
    if local_identity:
        # Prompt whether to replace in the case of conflicting names.
        from .wot import build_repo_list

        request_usks = build_repo_list(ui_, local_identity)
        names = [USK(x).get_repo_name() for x in request_usks]
        new_name = USK(insert_uri).get_repo_name()

        if new_name in names:
            replace = ui_.prompt("A repository with the name '{0}' is already"
                                 " published by {1}. Replace it? [y/N]"
                                 .format(new_name, local_identity),
                                 default='n')

            if replace.lower() != 'y':
                raise error.Abort(b"A repository with this name already exists.")

            # Remove the existing repository from each configuration section.
            existing_usk = request_usks[names.index(new_name)]

            existing_dir = None
            for directory, request_usk in stored_cfg.request_usks.items():
                if request_usk == existing_usk:
                    if existing_dir:
                        raise error.Abort(b"config.Configuration lists the same "
                                          b"request USK multiple times.")
                    existing_dir = directory

            assert existing_dir

            existing_hash = config.normalize(existing_usk)

            # config.Config file changes will not be written until a successful insert
            # below.
            del stored_cfg.version_table[existing_hash]
            del stored_cfg.request_usks[existing_dir]
            del stored_cfg.insert_usks[existing_hash]
            del stored_cfg.wot_identities[existing_hash]

        # Add "vcs" context. No-op if the identity already has it.
        msg_params = {'Message': 'AddContext',
                      'Identity': local_identity.identity_id,
                      'Context': 'vcs'}

        import fcp
        from . import wot
        node = fcp.FCPNode(**wot.get_fcpopts(ui_, fcphost=opts["fcphost"],
                                             fcpport=opts["fcpport"]))
        atexit.register(node.shutdown)
        vcs_response =\
            node.fcpPluginMessage(plugin_name="plugins.WebOfTrust.WebOfTrust",
                                  plugin_params=msg_params)[0]

        if vcs_response['header'] != 'FCPPluginReply' or\
                'Replies.Message' not in vcs_response or\
                vcs_response['Replies.Message'] != 'ContextAdded':
            raise error.Abort(("Failed to add context. Got {0}\n.".format(
                vcs_response)).encode("utf-8"))

    set_target_version(ui_, repo, opts, params,
                       b"Only inserting to version(s): %s\n")
    params['INSERT_URI'] = insert_uri
    inserted_to = execute_create(ui_, repo, params, stored_cfg)

    if inserted_to and local_identity:
        # creation returns a list of request URIs; use the first.
        stored_cfg.set_wot_identity(inserted_to[0], local_identity)
        config.Config.to_file(stored_cfg)

        from . import wot
        wot.update_repo_listing(ui_, local_identity, 
                                fcphost=opts["fcphost"],
                                fcpport=opts["fcpport"])


def infocalypse_copy(ui_, repo, **opts):
    """ Copy an Infocalypse repository to a new URI. """
    params, stored_cfg = get_config_info(ui_, opts)

    if not opts['inserturi']:
        # REDFLAG: fix parameter definition so that it is required?
        ui_.warn("Please set the insert URI with --inserturi.\n")
        return
    else:
        insert_uri = parse_repo_path(opts['inserturi'])

    if not opts['requesturi']:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --requesturi option.\n")
            return
    else:
        request_uri = parse_repo_path(opts['requesturi'])

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    execute_copy(ui_, repo, params, stored_cfg)


def infocalypse_reinsert(ui_, repo, **opts):
    """ Reinsert the current version of an Infocalypse repository. """
    params, stored_cfg = get_config_info(ui_, opts)

    if not opts['uri']:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Do a fn-pull from a repository USK and try again.\n")
            return
    else:
        request_uri = parse_repo_path(opts['uri'])

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

        ui_.status(b"No insert URI. Will skip re-insert of top key.\n")
        insert_uri = None

    params['INSERT_URI'] = insert_uri
    params['REQUEST_URI'] = request_uri
    params['REINSERT_LEVEL'] = level
    execute_reinsert(ui_, repo, params, stored_cfg)


def infocalypse_pull(ui_, repo, **opts):
    """ Pull from an Infocalypse repository in Freenet.
     """
    params, stored_cfg = get_config_info(ui_, opts)

    request_uri = ''

    if opts['hash']:
        # Use FMS to lookup the uri from the repo hash.
        if opts['uri']:
            ui_.warn("Ignoring --uri because --hash is set!\n")
        if len(opts['hash']) != 1:
            raise error.Abort(b"Only one --hash value is allowed.")
        params['FMSREAD_HASH'] = opts['hash'][0]
        params['FMSREAD_ONLYTRUSTED'] = bool(opts['onlytrusted'])
        request_uri = get_uri_from_hash(ui_, repo, params, stored_cfg)
    elif opts['wot']:
        from . import wot
        truster = get_truster(ui_, repo, opts['truster'],
                              fcpport=opts["fcpport"], fcphost=opts["fcphost"])
        request_uri = wot.resolve_pull_uri(ui_, opts['wot'], truster, repo,
                                           fcphost=opts['fcphost'], fcpport=opts['fcpport'])
    elif opts['uri']:
        request_uri = parse_repo_path(opts['uri'])

    if not request_uri:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.warn("There is no stored request URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return

    params['REQUEST_URI'] = request_uri
    # Hmmmm... can't really implement rev.
    execute_pull(ui_, repo, params, stored_cfg)


def infocalypse_pull_request(ui, repo, **opts):
    from . import wot
    from .wot_id import WoT_ID
    if not opts['wot']:
        raise error.Abort(b"Who do you want to send the pull request to? Set "
                          b"--wot.\n")

    wot_id, repo_name = opts['wot'].split('/', 1)
    from_identity = get_truster(ui, repo, opts['truster'],
                                fcpport=opts["fcpport"], fcphost=opts["fcphost"])
    to_identity = WoT_ID(wot_id, from_identity)
    wot.send_pull_request(ui, repo, from_identity, to_identity, repo_name,
                          mailhost=opts["mailhost"], smtpport=opts["smtpport"])


def infocalypse_check_notifications(ui, repo, **opts):
    from . import wot
    from .wot_id import Local_WoT_ID
    if not opts['wot']:
        raise error.Abort(b"What ID do you want to check for notifications? Set"
                          b" --wot.\n")

    fcpopts = wot.get_fcpopts(ui, fcpport=opts["fcpport"], fcphost=opts["fcphost"])
    wot.check_notifications(ui, Local_WoT_ID(opts['wot'], fcpopts=fcpopts))


def infocalypse_connect(ui, repo, **opts):
    from . import plugin_connect
    plugin_connect.connect(ui, repo)


def infocalypse_push(ui_, repo, **opts):
    """ Push to an Infocalypse repository in Freenet. """
    params, stored_cfg = get_config_info(ui_, opts)

    if not opts['uri']:
        insert_uri = stored_cfg.get_dir_insert_uri(repo.root)
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this repo.\n"
                     "Please set one with the --uri option.\n")
            return
    else:
        insert_uri = parse_repo_path(opts['uri'])

    set_target_version(ui_, repo, opts, params,
                       b"Only pushing to version(s): %s\n")
    params['INSERT_URI'] = insert_uri
    #if opts['requesturi'] != '':
    #    # DOESN'T search the insert uri index.
    #    ui_.status((("Copying from:\n%s\nTo:\n%s\n\nThis is an "
    #                 + "advanced feature. "
    #                 + "I hope you know what you're doing.\n") %
    #                (opts['requesturi'], insert_uri).encode("utf-8"))
    #    params['REQUEST_URI'] = opts['requesturi']

    inserted_to = execute_push(ui_, repo, params, stored_cfg)

    request_uri = stored_cfg.get_request_uri(repo.root)
    associated_wot_id = stored_cfg.get_wot_identity(request_uri)
    if inserted_to and associated_wot_id:
        from . import wot
        from .wot_id import Local_WoT_ID
        local_id = Local_WoT_ID('@' + associated_wot_id)
        wot.update_repo_listing(ui_, local_id, 
                                fcphost=opts["fcphost"],
                                fcpport=opts["fcpport"])


def infocalypse_info(ui_, repo, **opts):
    """ Display information about an Infocalypse repository.
     """
    # FCP not required. Hmmm... Hack
    opts['fcphost'] = ''
    opts['fcpport'] = 0
    print(get_config_info(ui_, opts))
    params, stored_cfg = get_config_info(ui_, opts)
    request_uri = opts['uri']
    if not request_uri:
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
        raise error.Abort(b"Use --hash to set the USK hash.")
    if len(opts['hash']) != 1:
        raise error.Abort(b"Only one --hash value is allowed.")
    if not is_hex_string(opts['hash'][0]):
        raise error.Abort(("[%s] doesn't look like a USK hash." %
                           opts['hash'][0]).encode("utf-8"))

    if not opts.get('fmsid', []):
        raise error.Abort(b"Use --fmsid to set the FMS id.")
    if len(opts['fmsid']) != 1:
        raise error.Abort(b"Only one --fmsid value is allowed.")
    if not is_fms_id(opts['fmsid'][0]):
        raise error.Abort(("[%s] doesn't look like an FMS id."
                           % opts['fmsid'][0]).encode("utf-8"))

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
    if not request_uri:
        request_uri = stored_cfg.get_request_uri(repo.root)
        if not request_uri:
            ui_.status(b"There is no stored request URI for this repo.\n")
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
            raise error.Abort(b"No request URI.")
        params['REQUEST_URI'] = request_uri

    params['DRYRUN'] = opts['dryrun']
    params['INSERT_URI'] = insert_uri
    execute_fmsnotify(ui_, repo, params, stored_cfg)

MSG_BAD_INDEX = b'You must set --index to a value >= 0.'


def infocalypse_putsite(ui_, repo, **opts):
    """ Insert an update to a freesite.
    """

    if opts['createconfig']:
        if opts['wiki']:
            raise error.Abort(b"Use fn-wiki --createconfig.")
        params = {'SITE_CREATE_CONFIG': True}
        execute_putsite(ui_, repo, params)
        return

    params, stored_cfg = get_config_info(ui_, opts)
    if opts['key']:  # order important
        params['SITE_KEY'] = opts['key']
        if not (params['SITE_KEY'].startswith('SSK') or
                params['SITE_KEY'] == 'CHK@'):
            raise error.Abort(b"--key must be a valid SSK "
                              + b"insert key or CHK@.")

    params['ISWIKI'] = opts['wiki']
    config.read_freesite_cfg(ui_, repo, params, stored_cfg)

    try:
        # --index not required for CHK@
        if not params['SITE_KEY'].startswith('CHK'):
            params['SITE_INDEX'] = int(opts['index'])
            if params['SITE_INDEX'] < 0:
                raise ValueError()
        else:
            params['SITE_INDEX'] = -1
    except ValueError:
        raise error.Abort(MSG_BAD_INDEX)
    except TypeError:
        raise error.Abort(MSG_BAD_INDEX)

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
        raise error.Abort(b"You must be in the repository root directory.")

    subcmds = ('run', 'createconfig', 'apply')
    required = sum([bool(opts[cmd]) for cmd in subcmds])
    if required == 0:
        raise error.Abort(b"You must specify either --run, " +
                          b"--createconfig, --apply")
    if required > 1:
        raise error.Abort(b"Use either --run, --createconfig, or --apply")

    if opts['apply']:
        params, stored_cfg = get_config_info(ui_, opts)
        params['REQUEST_URI'] = opts['apply']
        execute_wiki_apply(ui_, repo, params, stored_cfg)
        return

    if opts['fcphost'] or opts['fcpport']:
        raise error.Abort(b"--fcphost, --fcpport only for --apply")

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
        ui_.status(b"Skipped FMS configuration because --nofms was set.\n")

    if not opts['nowot']:
        infocalypse_setupwot(ui_, **opts)
    else:
        ui_.status(b"Skipped WoT configuration because --nowot was set.\n")


def infocalypse_setupfms(ui_, **opts):
    """ Setup or modify the fms configuration. """
    # REQUIRES config file.
    execute_setupfms(ui_, opts)


# TODO: Why ui with trailing underscore? Is there a global "ui" somewhere?
def infocalypse_setupwot(ui_, **opts):
    if not opts['truster']:
        raise error.Abort(b"Specify default truster with --truster")

    from . import wot
    from .wot_id import Local_WoT_ID
    fcpopts = wot.get_fcpopts(ui_, fcphost=opts["fcphost"], fcpport=opts["fcpport"])
    wot.execute_setup_wot(ui_, Local_WoT_ID(opts['truster'].decode("utf-8"), fcpopts=fcpopts))


def infocalypse_setupfreemail(ui, repo, **opts):
    """
    Set a Freemail password. If --truster is not given uses the default
    truster.
    """
    from . import wot
    # TODO: Here --truster doesn't make sense. There is no trust involved.
    # TODO: Should this be part of the normal fn-setup?
    wot.execute_setup_freemail(ui, get_truster(ui, repo, opts['truster'],
                                               fcpport=opts["fcpport"], fcphost=opts["fcphost"]),
                               mailhost=opts["mailhost"], smtpport=opts["smtpport"])


def get_truster(ui, repo=None, truster_identifier=None, fcpport=None, fcphost=None):
    """
    Return a local WoT ID.

    Search for a local identity from most to least specific:
    1. truster_identifier (if given)
    2. identity that published this respository (if repo is given and an
                                                 identity is set)
    3. default truster

    TODO: Accept fcp port and fcp host parameters.

    :rtype : Local_WoT_ID
    """
    from . import wot
    from . import wot_id
    cfg = config.Config.from_ui(ui)
    
    fcpopts = wot.get_fcpopts(ui, fcphost=fcphost or cfg.defaults['HOST'], fcpport=fcpport or cfg.defaults['PORT'])
    if truster_identifier:
        return wot_id.Local_WoT_ID(truster_identifier, fcpopts=fcpopts)
    else:

        # Value is identity ID, so '@' prefix makes it an identifier with an
        # empty nickname.
        identity = None
        default = False
        if repo:
            identity = cfg.get_wot_identity(cfg.get_request_uri(repo.root))

        # Either repo is not given or there is no associated identity.
        if not identity:
            identity = cfg.defaults['DEFAULT_TRUSTER']
            default = True

        try:
            return wot_id.Local_WoT_ID('@' + identity, fcpopts=fcpopts)
        except error.Abort:
            if default:
                raise error.Abort((b"Cannot resolve the default truster with "
                                   b"public key hash '%b'. Set it with hg"
                                   b" fn-setupwot --truster") % identity.encode("utf-8"))
            else:
                # TODO: Is this suggestion appropriate?
                # TODO: Ensure that fn-create on an existing repo does not
                # leave isolated insert_usks or wot_identities entries in the
                # config file.
                raise error.Abort((("Cannot resolve the identity with public key "
                                    "hash '%b' that published this repository. "
                                    "To create this repository under a different "
                                    "identity run hg fn-create") % identity).encode("utf-8"))

#----------------------------------------------------------"


def do_archive_create(ui_, opts, params, stored_cfg):
    """ fn-archive --create."""
    insert_uri = opts['uri']
    if not insert_uri:
        raise error.Abort(b"Please set the insert URI with --uri.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()
    execute_arc_create(ui_, params, stored_cfg)


def do_archive_push(ui_, opts, params, stored_cfg):
    """ fn-archive --push."""
    insert_uri = opts['uri']
    if not insert_uri:
        insert_uri = (
            stored_cfg.get_dir_insert_uri(params['ARCHIVE_CACHE_DIR']))
        if not insert_uri:
            ui_.warn("There is no stored insert URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise error.Abort(b"No Insert URI.")

    params['INSERT_URI'] = insert_uri
    params['FROM_DIR'] = os.getcwd()

    execute_arc_push(ui_, params, stored_cfg)


def do_archive_pull(ui_, opts, params, stored_cfg):
    """ fn-archive --pull."""
    request_uri = opts['uri']

    if not request_uri:
        request_uri = (
            stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR']))
        if not request_uri:
            ui_.warn("There is no stored request URI for this archive.\n"
                     "Please set one with the --uri option.\n")
            raise error.Abort(b"No request URI.")

    params['REQUEST_URI'] = request_uri
    params['TO_DIR'] = os.getcwd()
    execute_arc_pull(ui_,  params, stored_cfg)

ILLEGAL_FOR_REINSERT = ('uri', 'aggressive', 'nosearch')


def do_archive_reinsert(ui_, opts, params, stored_cfg):
    """ fn-archive --reinsert."""
    illegal = [value for value in ILLEGAL_FOR_REINSERT
               if value in opts and opts[value]]
    if illegal:
        raise error.Abort(b"--uri, --aggressive, --nosearch illegal " +
                          b"for reinsert.")
    request_uri = stored_cfg.get_request_uri(params['ARCHIVE_CACHE_DIR'])
    if request_uri is None:
        ui_.warn("There is no stored request URI for this archive.\n" +
                 "Run fn-archive --pull first!.\n")
        raise error.Abort(b" No request URI, can't re-insert")

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
        raise error.Abort(b"--create, --pull, --push are mutally exclusive. " +
                          b"Only specify one.")
    if len(subcmd) > 0:
        subcmd = subcmd[0]
    else:
        subcmd = "pull"

    params, stored_cfg = get_config_info(ui_, opts)
    params['ARCHIVE_CACHE_DIR'] = os.path.join(os.getcwd(), ARCHIVE_CACHE_DIR)

    if not subcmd in ARCHIVE_SUBCMDS:
        raise error.Abort(("Unhandled subcommand: " + subcmd).encode("utf-8"))

    # 2 qt?
    ARCHIVE_SUBCMDS[subcmd](ui_, opts, params, stored_cfg)
