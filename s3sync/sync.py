from .fs import FilesystemEndpoint
from .s3 import S3Endpoint


class SyncManager(object):
    def __init__(self, **kwargs):
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
