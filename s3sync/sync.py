from .import utils
from .fs import FilesystemEndpoint
from .s3 import S3Endpoint


class SyncManager(object):
    def __init__(self, fake=False, **kwargs):
        self.fake = fake
        self.log_prefix = 'sync'
        self.source = self.get_endpoint(kwargs['source'], 'source', **kwargs)
        self.destination = self.get_endpoint(kwargs['destination'], 'destination', **kwargs)

    def get_endpoint(self, path, name, **kwargs):
        keys = ['includes', 'excludes', 'verbosity']
        options = {k: kwargs[k] for k in keys if k in kwargs}
        if path.startswith('/'):
            options['name'] = name
            options['base_path'] = path
            options['cache_dir'] = kwargs.get('cache_dir')
            return FilesystemEndpoint(**options)
        else:
            options['base_url'] = path
            return S3Endpoint(**options)

    def get_operations(self):
        return utils.get_operations(
            self.source.key_data,
            self.destination.key_data,
        )

    def sync(self):
        self.source.update_key_data()
        self.destination.update_key_data()
        operations = self.get_operations()
        for key in operations['transfer']:
            self.destination.transfer_from(key, self.source, fake=self.fake)
        for key in operations['delete']:
            self.destination.delete(key, fake=self.fake)
