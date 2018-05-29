import json
import os

class Cache(object):
    def __init__(self, name='source', cache_dir=None, cache_file=None):
        self.cache_file = cache_file
        if not cache_file:
            self.cache_file_name = os.path.join(cache_dir, '.s3sync-{}.json'.format(name))

    def read(self):
        if self.cache_file:
            cache_file = self.cache_file
            return self.read_file(cache_file)
        else:
            try:
                with open(self.cache_file_name, 'r') as cache_file:
                    return self.read_file(cache_file)
            except FileNotFoundError:
                return dict()

    def read_file(self, cache_file):
        cache_file.seek(0)
        try:
            return json.load(cache_file)
        except json.decoder.JSONDecodeError:
            return dict()

    def write(self, data):
        if self.cache_file:
            cache_file = self.cache_file
            self.write_file(cache_file, data)
        else:
            with open(self.cache_file_name, 'w') as cache_file:
                self.write_file(cache_file, data)

    def write_file(self, cache_file, data):
        cache_file.seek(0)
        json.dump(data, cache_file, indent=2)

