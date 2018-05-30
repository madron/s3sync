class BaseEndpoint(object):
    def __init__(self, log_prefix=''):
        self.log_prefix = log_prefix

    def log_info(self, message):
        print('<{}> {}'.format(self.log_prefix, message))
