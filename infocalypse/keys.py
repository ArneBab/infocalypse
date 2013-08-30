from string import split
from mercurial import util


class USK:
    def __init__(self, path):
        components = split(path, '/')
        # Expecting USK@key/name/edition
        assert len(components) == 3

        self.key = components[0].split('@')[1]
        self.name = components[1]
        self.edition = int(components[2])

        # TODO: Is stripping "freenet://" appropriate?
        self.key = strip_protocol(self.key)

    def get_repo_name(self):
        """
        Return name with the redundancy level, if any, removed.

        >>> USK('USK@.../name/5').get_repo_name()
        'name'
        >>> USK('USK@.../name.R1/5').get_repo_name()
        'name'
        >>> USK('USK@.../name.R0/5').get_repo_name()
        'name'
        >>> USK('USK@.../name.something/5').get_repo_name()
        'name.something'
        >>> USK('USK@.../name.R2/5').get_repo_name()
        'name.R2'
        """
        if self.name.endswith('.R1') or self.name.endswith('.R0'):
            return self.name[:-3]
        return self.name

    def get_public_key_hash(self):
        """
        Return the public key hash component of the key.

        >>> USK('USK@wHllqvhRlGLZZrXwqgsFFGbv2V9S33lq~-MTIN2FvOw,mN0trI6Yx1W6ecyERrVxANHQmA3vJwk88UEHW3qCsRA,AQACAAE/vcs/22').get_public_key_hash()
        'wHllqvhRlGLZZrXwqgsFFGbv2V9S33lq~-MTIN2FvOw'
        """
        return self.key.split(',')[0]

    def clone(self):
        return USK(str(self))

    def __str__(self):
        return 'USK@%s/%s/%s' % (self.key, self.name, self.edition)

    def __repr__(self):
        return "USK('%s')" % str(self)


# Method instead of class because the existing code expects keys to be strings.
# TODO: Would assuming edition / redundancy be better suited as arguments to
# the USK __init__()? WoT paths are not USKs though. Once again RepoPath
# might be nice. It would especially avoid repeated string operations to work
# with redundancy level.
def parse_repo_path(path, assume_redundancy=False):
    """
    Return the given path to a repo - either USK or WoT path -
    assuming if unspecified:
    * edition 0
    * optionally, not by default (assume_redundancy) R1 redundancy

    >>> parse_repo_path('USK@.../name')
    'USK@.../name/0'
    >>> parse_repo_path('USK@.../name/')
    'USK@.../name/0'
    >>> parse_repo_path('USK@.../name', assume_redundancy=True)
    'USK@.../name.R1/0'
    >>> parse_repo_path('USK@.../name.R0/5', assume_redundancy=True)
    'USK@.../name.R0/5'
    >>> parse_repo_path('not a key')
    Traceback (most recent call last):
        ...
    Abort: Cannot parse 'not a key' as repository path.
    """
    parts = path.split('/')

    if len(parts) == 2:
        # Assuming USK@..,/name: '/edition' omitted.
        parts.append('0')

    if not len(parts) == 3:
        raise util.Abort("Cannot parse '{0}' as repository path.".format(path))

    if not parts[2]:
        # Assuming USK@../name/: 'edition' omitted
        parts[2] = '0'

    if assume_redundancy:
        # Assuming USK@.../repo_name/edition
        repo_name = parts[1]
        if not repo_name.endswith('.R1') and not repo_name.endswith('.R0'):
            parts[1] += '.R1'

    return '/'.join(parts)


def strip_protocol(uri):
    """
    Return the uri without "freenet:" or "freenet://" at the start, if present.

    >>> strip_protocol('freenet:USK@.../test/0')
    'USK@.../test/0'
    >>> strip_protocol('freenet://someone/test')
    'someone/test'
    >>> strip_protocol('another/testing')
    'another/testing'
    """
    if uri.startswith('freenet://'):
        return uri[len('freenet://'):]
    elif uri.startswith('freenet:'):
        return uri[len('freenet:'):]
    return uri
