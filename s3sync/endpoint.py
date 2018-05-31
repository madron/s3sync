class BaseEndpoint(object):
    def __init__(self, includes=[], excludes=[], log_prefix='', verbosity=0):
        self.includes = includes
        self.excludes = excludes
        self.log_prefix = log_prefix
        self.verbosity = verbosity
        self.key_data = dict()
        self.etag = dict()
        self.total_files = 0
        self.total_bytes = 0

    def log_info(self, message):
        if self.verbosity > 0:
            print('<{}> {}'.format(self.log_prefix, message))

    def is_excluded(self, key):
        for exclude in self.excludes:
            if key.startswith(exclude):
                return True
        return False

    def update_etag(self):
        self.etag = dict((key, data['etag']) for key, data in self.key_data.items())

    def update_totals(self):
        self.total_files = len(self.key_data)
        self.total_bytes = sum([x['size'] for x in self.key_data.values()])
