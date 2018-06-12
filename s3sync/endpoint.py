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
        self.total_files = 0
        self.total_bytes = 0
        self.counter = FileByteCounter(name, verbosity=verbosity)

    def is_excluded(self, key):
        for exclude in self.excludes:
            if key.startswith(exclude):
                return True
        return False

    def update_single_key_data(self, key):
        pass

    def update_etag(self):
        self.etag = dict((key, data['etag']) for key, data in self.key_data.items())

    def update_totals(self):
        self.total_files = len(self.key_data)
        self.total_bytes = sum([x['size'] for x in self.key_data.values()])
        self.counter.set(self.total_files, self.total_bytes)

    def transfer(self, key, destination):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def observer_start(self, events_queue):
        pass

    def observer_stop(self):
        pass
