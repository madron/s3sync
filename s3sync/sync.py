import time
from queue import Queue
from .import utils
from .fs import FSEndpoint
from .logger import Logger
from .s3 import S3Endpoint


class SyncManager(Logger):
    def __init__(self, fake=False, verbosity=0, **kwargs):
        super().__init__(log_prefix='SyncManager', verbosity=verbosity)
        self.fake = fake
        self.source = self.get_endpoint(kwargs['source'], 'source', **kwargs)
        self.destination = self.get_endpoint(kwargs['destination'], 'destination', **kwargs)
        self.source_queue = Queue()
        self.destination_queue = Queue()

    def get_endpoint(self, path, name, **kwargs):
        keys = ['includes', 'excludes', 'verbosity']
        options = {k: kwargs[k] for k in keys if k in kwargs}
        options['verbosity'] = self.verbosity
        if path.startswith('/'):
            options['name'] = name
            options['base_path'] = path
            options['cache_dir'] = kwargs.get('cache_dir')
            return FSEndpoint(**options)
        else:
            options['base_url'] = path
            return S3Endpoint(**options)

    def get_operations(self):
        return utils.get_operations(
            self.source.etag,
            self.destination.etag,
        )

    def sync(self):
        self.source.update_key_data()
        self.destination.update_key_data()
        operations = self.get_operations()
        for key in operations['transfer']:
            self.transfer(key, fake=self.fake)
        for key in operations['delete']:
            self.destination.delete(key, fake=self.fake)

    def transfer(self, key, fake=False):
        if not fake:
            if self.source.type == 'fs' and self.destination.type == 'fs':
                self.source.copy(key, self.destination)
            elif self.source.type == 'fs' and self.destination.type == 's3':
                source_path = self.source.get_path(key)
                self.destination.upload(key, source_path)
            elif self.source.type == 's3' and self.destination.type == 'fs':
                destination_path = self.destination.get_destination_path(key)
                self.source.download(key, destination_path)
            else:
                raise NotImplementedError()
        self.log_info(key, log_prefix='transfer')

    def watch(self):
        self.source.observer_start(self.source_queue)
        self.destination.observer_start(self.destination_queue)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        self.source.observer_stop()
