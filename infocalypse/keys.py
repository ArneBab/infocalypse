from string import split


class USK:
    def __init__(self, path):
        components = split(path, '/')
        # Expecting USK@key/name/edition
        assert len(components) == 3

        self.key = components[0]
        self.name = components[1]
        self.edition = components[2]

    def __str__(self):
        return self.key + '/' + self.name + '/' + self.edition