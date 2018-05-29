import os
from datetime import datetime
from io import StringIO
from unittest import TestCase
from .. import fs


class FilesystemEndpointTest(TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(__file__)

    def test_get_path_data(self):
        endpoint = fs.FilesystemEndpoint(base_path=self.base_path, cache_file=StringIO())
        file_data = endpoint.get_path_data('files')
        f = file_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)

    def test_get_fs_key_data_1(self):
        base_path = self.base_path
        includes = [
            'files',
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(len(key_data), 1)
        file_data = key_data[includes[0]]
        f = file_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 5)

    def test_get_fs_key_data_2(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(len(key_data), 1)
        # os.path.join('files', 'd1')
        file_data = key_data[includes[0]]
        f = file_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 2)

    def test_get_fs_key_data_3(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'd2'),
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(len(key_data), 2)
        # os.path.join('files', 'd1')
        file_data = key_data[includes[0]]
        f = file_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 2)
        # os.path.join('files', 'd2')
        file_data = key_data[includes[1]]
        f = file_data['files/d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['files/d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 2)

    def test_get_fs_key_data_4(self):
        base_path = os.path.join(self.base_path, 'files')
        includes = [
            '',
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(len(key_data), 1)
        # ''
        file_data = key_data[includes[0]]
        f = file_data['d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = file_data['f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 5)

    def test_get_fs_key_data_5(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'f1'),
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(len(key_data), 1)
        # os.path.join('files', 'f1')
        file_data = key_data[includes[0]]
        f = file_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(file_data), 1)


class FilesystemEndpointReadCacheTest(TestCase):
    def test_empty_1(self):
        cache_file = StringIO()
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.read_cache()
        self.assertEqual(endpoint.key_data, dict())

    def test_empty_2(self):
        cache_file = StringIO('{}')
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.read_cache()
        self.assertEqual(endpoint.key_data, dict())

    def test_1(self):
        cache_file = StringIO('{"include": {"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}}')
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.read_cache()
        self.assertEqual(
            endpoint.key_data,
            dict(include={"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}),
        )


class FilesystemEndpointWriteCacheTest(TestCase):
    def test_empty(self):
        cache_file = StringIO()
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.write_cache()
        self.assertEqual(cache_file.getvalue() , '{}')

    def test_1(self):
        cache_file = StringIO()
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        endpoint.key_data = dict(include=dict())
        endpoint.write_cache()
        self.assertEqual(cache_file.getvalue() , '{"include": {}}')

    def test_2(self):
        cache_file = StringIO()
        endpoint = fs.FilesystemEndpoint(cache_file=cache_file)
        endpoint.key_data = dict(
            include={
                'files/f1': dict(size=8, etag='hash', last_modified=1527577755.3356848),
            }
        )
        endpoint.write_cache()
        self.assertEqual(
            cache_file.getvalue() ,
            '{"include": {"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}}',
        )


class FilesystemEndpointUpdateKeyDataTest(TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(__file__)

    def test_1(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'f1'),
        ]
        cache_file=StringIO()
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        changed = endpoint.update_key_data()
        self.assertTrue(changed)
        self.assertEqual(len(endpoint.key_data), 2)
        # os.path.join('files', 'd1')
        file_data = endpoint.key_data[includes[0]]
        f = file_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], 'c3f058778ae8b1cefa04425c2178b7a6')
        f = file_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '909a4146156421135da6b38e8efd3a3b')
        self.assertEqual(len(file_data), 2)
        # os.path.join('files', 'f1')
        file_data = endpoint.key_data[includes[1]]
        f = file_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '16fed0121505838f492d0295ba547558')
        self.assertEqual(len(file_data), 1)
        # cache file is updated
        content = cache_file.getvalue()
        self.assertIn('909a4146156421135da6b38e8efd3a3b', content)
        self.assertIn('c3f058778ae8b1cefa04425c2178b7a6', content)
        self.assertIn('16fed0121505838f492d0295ba547558', content)

    def test_unchanged(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'f1'),
        ]
        endpoint = fs.FilesystemEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        changed = endpoint.update_key_data()
        changed = endpoint.update_key_data()
        self.assertFalse(changed)
