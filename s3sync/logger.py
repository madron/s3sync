class Logger(object):
    def __init__(self, log_prefix='', verbosity=0):
        self.log_prefix = log_prefix
        self.verbosity = verbosity

    def log_info(self, message, log_prefix=None):
        if self.verbosity > 0:
            log_prefix = log_prefix or self.log_prefix
            print('INFO <{}> {}'.format(log_prefix, message))

    def log_debug(self, message, log_prefix=None):
        if self.verbosity > 1:
            log_prefix = log_prefix or self.log_prefix
            print('DEBUG <{}> {}'.format(log_prefix, message))

    def log_error(self, message, log_prefix=None):
        log_prefix = log_prefix or self.log_prefix
        print('ERROR <{}> {}'.format(log_prefix, message))
