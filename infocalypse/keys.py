from mercurial import util


class USK:
    def __init__(self, path):
        components = path.split(b'/')
        # Expecting USK@key/name/edition
        assert len(components) == 3

        self.key = components[0].split(b'@')[1]
        self.name = components[1]
        self.edition = int(components[2])

        # TODO: Is stripping "freenet://" appropriate?
        self.key = strip_protocol(self.key)

    def get_repo_name(self):
        """
        Return name with the redundancy level, if any, removed.

        >>> USK(b'USK@.../name/5').get_repo_name()
        'name'
        >>> USK(b'USK@.../name.R1/5').get_repo_name()
        'name'
        >>> USK(b'USK@.../name.R0/5').get_repo_name()
        'name'
        >>> USK(b'USK@.../name.something/5').get_repo_name()
        'name.something'
        >>> USK(b'USK@.../name.R2/5').get_repo_name()
        'name.R2'
        """
        if self.name.endswith(b'.R1') or self.name.endswith(b'.R0'):
            return self.name[:-3]
        return self.name

    def get_public_key_hash(self):
        """
        Return the public key hash component of the key.

        >>> USK(b'USK@wHllqvhRlGLZZrXwqgsFFGbv2V9S33lq~-MTIN2FvOw,mN0trI6Yx1W6ecyERrVxANHQmA3vJwk88UEHW3qCsRA,AQACAAE/vcs/22').get_public_key_hash()
        'wHllqvhRlGLZZrXwqgsFFGbv2V9S33lq~-MTIN2FvOw'
        """
        return self.key.split(b',')[0]

    def clone(self):
        return USK(str(self).encode("utf-8"))

    def __str__(self):
        return 'USK@%s/%s/%s' % (self.key.decode("utf-8"), self.name.decode("utf-8"), self.edition)

    def __repr__(self):
        return "USK(b'%s')" % str(self)


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

    >>> parse_repo_path(b'USK@.../name')
    'USK@.../name/0'
    >>> parse_repo_path(b'USK@.../name/')
    'USK@.../name/0'
    >>> parse_repo_path(b'USK@.../name/10')
    'USK@.../name/10'
    >>> parse_repo_path(b'USK@.../name/10/')
    'USK@.../name/10'
    >>> parse_repo_path(b'USK@.../name', assume_redundancy=True)
    'USK@.../name.R1/0'
    >>> parse_repo_path(b'USK@.../name.R0/5', assume_redundancy=True)
    'USK@.../name.R0/5'
    >>> parse_repo_path(b'not a key')
    Traceback (most recent call last):
        ...
    Abort: Cannot parse 'not a key' as repository path.
    """
    parts = path.split(b'/')

    if len(parts) == 2:
        # Assuming USK@..,/name: '/edition' omitted.
        parts.append(b'0')

    if len(parts) == 4:
        # Assuming trailing slash - the part after it should be empty.
        if parts[3]:
            raise util.Abort(b"Found unexpected '{0}' trailing the edition "
                             b"number.".format(parts[3]))
        parts.pop()

    if not len(parts) == 3:
        raise util.Abort("Cannot parse '{0}' as repository path.".format(path))

    if not parts[2]:
        # Assuming USK@../name/: 'edition' omitted
        parts[2] = b'0'

    if assume_redundancy:
        # Assuming USK@.../repo_name/edition
        repo_name = parts[1]
        if not repo_name.endswith(b'.R1') and not repo_name.endswith(b'.R0'):
            parts[1] += b'.R1'

    return b'/'.join(parts)


def strip_protocol(uri):
    """
    Return the uri without "freenet:" or "freenet://" at the start, if present.

    >>> strip_protocol(b'freenet:USK@.../test/0')
    'USK@.../test/0'
    >>> strip_protocol(b'freenet://someone/test')
    'someone/test'
    >>> strip_protocol(b'another/testing')
    'another/testing'
    """
    if uri.startswith(b'freenet://'):
        return uri[len(b'freenet://'):]
    elif uri.startswith(b'freenet:'):
        return uri[len(b'freenet:'):]
    return uri
