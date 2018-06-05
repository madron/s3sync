import os
from io import StringIO
from tempfile import TemporaryDirectory
from unittest import TestCase
from ..cache import Cache


class CacheTest(TestCase):
    def test_read_dir(self):
        with TemporaryDirectory() as cache_dir:
            file_name = os.path.join(cache_dir, '.s3sync-source.json')
            with open(file_name, 'w') as cache_file:
                cache_file.write('{"name": "Fred"}')
            cache = Cache(name='source', cache_dir=cache_dir)
            data = cache.read()
            self.assertEqual(data, dict(name='Fred'))

    def test_read_dir_not_found(self):
        with TemporaryDirectory() as cache_dir:
            cache = Cache(name='source', cache_dir=cache_dir)
            data = cache.read()
            self.assertEqual(data, dict())

    def test_write_dir(self):
        with TemporaryDirectory() as cache_dir:
            cache = Cache(name='source', cache_dir=cache_dir)
            cache.write(dict(name='Bob'))
            file_name = os.path.join(cache_dir, '.s3sync-source.json')
            with open(file_name, 'r') as cache_file:
                content = cache_file.read()
                self.assertIn('"name": "Bob"', content)

    def test_read_file(self):
        cache_file = StringIO('{"name": "Fred"}')
        cache = Cache(name='source', cache_file=cache_file)
        data = cache.read()
        self.assertEqual(data, dict(name='Fred'))

    def test_write_file(self):
        cache_file = StringIO()
        cache = Cache(name='source', cache_file=cache_file)
        cache.write(dict(name='Bob'))
        content = cache_file.getvalue()
        self.assertIn('"name": "Bob"', content)
