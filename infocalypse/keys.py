from string import split


class USK:
    def __init__(self, path):
        components = split(path, '/')
        # Expecting USK@key/name/edition
        assert len(components) == 3

        self.key = components[0]
        self.name = components[1]
        self.edition = int(components[2])

        # TODO: Is stripping "freenet://" appropriate?
        if self.key.startswith('freenet:'):
            self.key = self.key[len('freenet:'):]
        elif self.key.startswith('freenet://'):
            self.key = self.key[len('freenet://'):]

    def __str__(self):
        return '%s/%s/%s' % (self.key, self.name, self.edition)

    def __repr__(self):
        return "USK(%s)" % str(self)