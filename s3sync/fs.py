import json
import os
import shutil
from datetime import datetime
from operator import itemgetter
from shutil import copy2
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from . import utils
from .cache import Cache
from .constants import HASHED_BYTES_THRESHOLD
from .endpoint import BaseEndpoint


class FSEndpoint(BaseEndpoint, FileSystemEventHandler):
    def __init__(self, name='source', base_path='/', cache_dir=None, cache_file=None,
                 hashed_bytes_threshold=HASHED_BYTES_THRESHOLD, **kwargs):
        super().__init__(name=name, log_prefix=name, **kwargs)
        self.type = 'fs'
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
        key_errors = []
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
                try:
                    data['etag'] = utils.get_etag(path)
                except Exception as e:
                    key_errors.append(key)
                    self.log_error('File: {}'.format(path), error=e)
                    continue
                hashed_bytes += data['size']
                if hashed_bytes > self.hashed_bytes_threshold:
                    self.cache.write(fs_data)
                    hashed_bytes = 0
                    self.log_info('Hashed files: {}/{}'.format(hashed_files, total_files))
        for key in key_errors:
            del fs_data[key]
        self.key_data = fs_data
        self.write_cache()
        self.update_etag()
        self.update_totals()
        self.counter.log_totals()

    def write_cache(self):
        self.cache.write(self.key_data)

    def update_single_key_data(self, key):
        if not self.is_excluded(key):
            path = self.get_path(key)
            try:
                stat = os.stat(path)
                etag = utils.get_etag(path)
                self.key_data[key] = dict(
                    size=stat.st_size,
                    last_modified=stat.st_mtime,
                    etag=etag,
                )
                self.etag[key] = etag
            except FileNotFoundError:
                if key in self.key_data: del self.key_data[key]
                if key in self.etag: del self.etag[key]
            self.update_totals()

    def get_path(self, key):
        return os.path.join(self.base_path, key)

    def get_destination_path(self, key):
        path = self.get_path(key)
        dir_name = os.path.dirname(path)
        # Create destination directory
        if os.path.exists(dir_name):
            if os.path.isfile(dir_name):
                os.remove(dir_name)
                os.makedirs(dir_name)
        else:
            os.makedirs(dir_name)
        # Remove destination path if it's a directory
        if os.path.isdir(path):
            shutil.rmtree(path)
        return path

    def copy(self, key, destination_endpoint):
        assert(destination_endpoint.type == 'fs')
        source_path = self.get_path(key)
        destination_path = destination_endpoint.get_destination_path(key)
        copy2(source_path, destination_path)

    def delete(self, key, fake=False):
        self.log_debug(key, log_prefix='delete')
        destination = os.path.join(self.base_path, key)
        if not fake:
            os.remove(destination)
        self.log_info(key, log_prefix='delete')

    def on_any_event(self, event):
        self.log_debug(str(event))

    def on_created(self, event):
        if not event.is_directory:
            key = event.src_path.replace(self.base_path, '', 1).lstrip('/')
            if not self.is_excluded(key):
                event = dict(type='modified', key=key)
                self.events_queue.put(event)

    def on_modified(self, event):
        if not event.is_directory:
            key = event.src_path.replace(self.base_path, '', 1).lstrip('/')
            if not self.is_excluded(key):
                event = dict(type='modified', key=key)
                self.events_queue.put(event)

    def on_moved(self, event):
        if not event.is_directory:
            # Source
            key = event.src_path.replace(self.base_path, '', 1).lstrip('/')
            if not self.is_excluded(key):
                self.events_queue.put(dict(type='deleted', key=key))
            # Destination
            key = event.dest_path.replace(self.base_path, '', 1).lstrip('/')
            if not self.is_excluded(key):
                self.events_queue.put(dict(type='modified', key=key))

    def on_deleted(self, event):
        key = event.src_path.replace(self.base_path, '', 1).lstrip('/')
        if not self.is_excluded(key):
            if event.is_directory:
                keys = [k for k in self.key_data.keys() if k.startswith(key)]
                for k in keys:
                    event = dict(type='deleted', key=k)
                    self.events_queue.put(event)
            else:
                event = dict(type='deleted', key=key)
                self.events_queue.put(event)

    def observer_start(self, events_queue):
        self.events_queue = events_queue
        self.observer = Observer()
        for include in self.includes:
            path = os.path.join(self.base_path, include)
            if os.path.isdir(path):
                self.observer.schedule(self, path, recursive=True)
        self.observer.start()

    def observer_stop(self):
        self.observer.stop()
        self.observer.join()
