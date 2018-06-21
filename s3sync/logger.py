import sys
import traceback
from . import metrics

class Logger(object):
    def __init__(self, log_prefix='', verbosity=0):
        self.log_prefix = log_prefix
        self.verbosity = verbosity

    def log_info(self, message, log_prefix=None):
        if self.verbosity > 0:
            log_prefix = log_prefix or self.log_prefix
            msg = message.encode('ascii', 'replace').decode()
            print('INFO <{}> {}'.format(log_prefix, msg))
            sys.stdout.flush()

    def log_debug(self, message, log_prefix=None):
        if self.verbosity > 1:
            log_prefix = log_prefix or self.log_prefix
            msg = message.encode('ascii', 'replace').decode()
            print('DEBUG <{}> {}'.format(log_prefix, msg))
            sys.stdout.flush()

    def log_error(self, message, error=None, log_prefix=None):
        metrics.errors.inc(1)
        log_prefix = log_prefix or self.log_prefix
        msg = message.encode('ascii', 'replace').decode()
        print('ERROR <{}> {}'.format(log_prefix, msg))
        if error:
            print(traceback.format_exc())
        sys.stdout.flush()
