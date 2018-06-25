from botocore.utils import percent_encode
from .counter import FileByteCounter
from .logger import Logger


class BaseEndpoint(Logger):
    def __init__(self, name='', includes=[], excludes=[], log_prefix='', verbosity=0):
        super().__init__(log_prefix=log_prefix, verbosity=verbosity)
        self.name = name
        self.includes = includes
        self.excludes = excludes
        self.key_data = dict()
        self.etag = dict()
        self.counter = FileByteCounter(name, verbosity=verbosity)

    def is_excluded(self, key):
        for exclude in self.excludes:
            if key.startswith(exclude):
                return True
        try:
            percent_encode(key)
        except UnicodeEncodeError as e:
            self.log_error('{} - {}'.format(key, str(e)))
            return True
        return False

    def get_include(self, key):
        for include in self.includes:
            if key.startswith(include):
                return include

    def update_single_key_data(self, key):
        pass

    def update_etag(self):
        self.etag = dict((key, data['etag']) for key, data in self.key_data.items())

    def transfer(self, key, destination):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def write_cache(self):
        pass

    def observer_start(self, events_queue):
        pass

    def observer_stop(self):
        pass
