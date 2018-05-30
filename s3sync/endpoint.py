class BaseEndpoint(object):
    def __init__(self, log_prefix='', verbosity=0):
        self.log_prefix = log_prefix
        self.verbosity = verbosity

    def log_info(self, message):
        if self.verbosity > 0:
            print('<{}> {}'.format(self.log_prefix, message))
