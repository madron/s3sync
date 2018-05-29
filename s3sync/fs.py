import json
import os
from datetime import datetime
from operator import itemgetter
from . import utils

DEFAULT_CACHE_DIR = os.path.join('/', 'var', 'cache', 's3sync')


class FilesystemEndpoint(object):
    def __init__(self, name='source', base_path='/', includes=[], excludes=[], cache_file=None):
        self.name = name
        self.base_path = base_path
        self.includes = includes
        self.excludes = excludes
        self.key_data = dict()
        self.etag = dict()
        cache_file_name = os.path.join(DEFAULT_CACHE_DIR, '{}.json'.format(name))
        self.cache_file = cache_file or open(cache_file_name, '+')
        self.read_cache()

    def is_excluded(self, key):
        for exclude in self.excludes:
            if key.startswith(exclude):
                return True
        return False

    def get_path_data(self, include):
        path_data = dict()
        path = os.path.join(self.base_path, include)
        if os.path.isfile(path):
            file_path = path
            stat = os.stat(file_path)
            key = file_path.replace(self.base_path, '', 1).lstrip('/')
            if not self.is_excluded(key):
                path_data[key] = dict(
                    size=stat.st_size,
                    last_modified=stat.st_mtime,
                )
        else:
            for prefix, directories, filenames in os.walk(path):
                for filename in filenames:
                    file_path = os.path.join(prefix, filename)
                    stat = os.stat(file_path)
                    key = file_path.replace(self.base_path, '', 1).lstrip('/')
                    if not self.is_excluded(key):
                        path_data[key] = dict(
                            size=stat.st_size,
                            last_modified=stat.st_mtime,
                        )
        return path_data

    def get_fs_key_data(self):
        key_data = dict()
        for include in self.includes:
            key_data.update(self.get_path_data(include))
        return key_data

    def read_cache(self):
        self.cache_file.seek(0)
        try:
            self.key_data = json.load(self.cache_file)
        except json.decoder.JSONDecodeError:
            pass

    def write_cache(self):
        self.cache_file.seek(0)
        json.dump(self.key_data, self.cache_file)

    def update_key_data(self):
        fs_data = self.get_fs_key_data()
        for key, data in fs_data.items():
            old_data = self.key_data.get(key, dict())
            if data['size'] == old_data.get('size') and data['last_modified'] == old_data.get('last_modified'):
                data['etag'] = old_data['etag']
            else:
                path = os.path.join(self.base_path, key)
                data['etag'] = utils.get_etag(path)
        if self.key_data == fs_data:
            return False
        self.key_data = fs_data
        self.write_cache()
        self.etag = dict((key, data['etag']) for key, data in self.key_data.items())
        return True
