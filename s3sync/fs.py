import json
import os
from datetime import datetime
from operator import itemgetter
from . import utils
from .cache import Cache
from .constants import HASHED_BYTES_THRESHOLD
from .endpoint import BaseEndpoint


class FilesystemEndpoint(BaseEndpoint):
    def __init__(self, name='source', base_path='/', cache_dir=None, cache_file=None,
                 hashed_bytes_threshold=HASHED_BYTES_THRESHOLD, **kwargs):
        super().__init__(log_prefix=name, **kwargs)
        self.name = name
        self.base_path = base_path
        self.hashed_bytes_threshold = hashed_bytes_threshold
        self.cache = Cache(name=name, cache_dir=cache_dir, cache_file=cache_file)
        self.key_data = self.cache.read()

    def get_path_data(self, include):
        path_data = dict()
        path = os.path.join(self.base_path, include)
        path_list = []
        if os.path.isfile(path):
            path_list.append(path)
        else:
            for prefix, directories, filenames in os.walk(path):
                for filename in filenames:
                    file_path = os.path.join(prefix, filename)
                    if os.path.isfile(file_path):
                        path_list.append(file_path)
        for path in path_list:
            stat = os.stat(path)
            key = path.replace(self.base_path, '', 1).lstrip('/')
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

    def update_key_data(self):
        fs_data = self.get_fs_key_data()
        hashed_bytes = 0
        hashed_files = 0
        total_files = len(fs_data)
        for key, data in fs_data.items():
            hashed_files += 1
            self.total_bytes += data['size']
            old_data = self.key_data.get(key, dict())
            if      data['size'] == old_data.get('size') \
                and data['last_modified'] == old_data.get('last_modified') \
                and 'etag' in old_data:
                data['etag'] = old_data.get('etag')
            else:
                path = os.path.join(self.base_path, key)
                data['etag'] = utils.get_etag(path)
                hashed_bytes += data['size']
                if hashed_bytes > self.hashed_bytes_threshold:
                    self.cache.write(fs_data)
                    hashed_bytes = 0
                    self.log_info('Hashed files: {}/{}'.format(hashed_files, total_files))
        self.key_data = fs_data
        self.cache.write(self.key_data)
        self.update_etag()
        self.update_totals()
        self.log_info('Total files: {}'.format(self.total_files))
        self.log_info('Total bytes: {}'.format(self.total_bytes))
