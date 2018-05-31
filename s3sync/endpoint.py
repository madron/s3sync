class BaseEndpoint(object):
    def __init__(self, includes=[], excludes=[], log_prefix='', verbosity=0):
        self.includes = includes
        self.excludes = excludes
        self.log_prefix = log_prefix
        self.verbosity = verbosity
        self.etag = dict()
        self.key_data = dict()

    def log_info(self, message):
        if self.verbosity > 0:
            print('<{}> {}'.format(self.log_prefix, message))

    def is_excluded(self, key):
        for exclude in self.excludes:
            if key.startswith(exclude):
                return True
        return False
