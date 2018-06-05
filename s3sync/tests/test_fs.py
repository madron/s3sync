import os
from datetime import datetime
from io import StringIO
from tempfile import TemporaryDirectory
from unittest import TestCase
from .. import fs


class FSEndpointGetFsKeyTest(TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(__file__)

    def test_get_path_data(self):
        endpoint = fs.FSEndpoint(base_path=self.base_path, cache_file=StringIO())
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

    def test_1(self):
        base_path = self.base_path
        includes = [
            'files',
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        f = key_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(key_data), 5)

    def test_2(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        f = key_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(key_data), 2)

    def test_3(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'd2'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        f = key_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['files/d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(key_data), 4)

    def test_4(self):
        base_path = os.path.join(self.base_path, 'files')
        includes = [
            '',
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        f = key_data['d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['d2/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['d2/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        f = key_data['f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(key_data), 5)

    def test_5(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'f1'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        f = key_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(len(key_data), 1)

    def test_excludes_not_in_include(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'd2'),
        ]
        excludes = [
            os.path.join('files', 'f1'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(
            sorted(list(key_data.keys())),
            ['files/d1/f1', 'files/d1/f2', 'files/d2/f1', 'files/d2/f2'],
        )

    def test_excludes_1(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'd2'),
        ]
        excludes = [
            os.path.join('files', 'd2', 'f1'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, excludes=excludes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(
            sorted(list(key_data.keys())),
            ['files/d1/f1', 'files/d1/f2', 'files/d2/f2'],
        )


class FSEndpointReadCacheTest(TestCase):
    def test_empty_1(self):
        cache_file = StringIO()
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(endpoint.key_data, dict())

    def test_empty_2(self):
        cache_file = StringIO('{}')
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(endpoint.key_data, dict())

    def test_1(self):
        cache_file = StringIO('{"include": {"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}}')
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(
            endpoint.key_data,
            dict(include={"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}),
        )


class FilesystemEndpointWriteCacheTest(TestCase):
    def test_empty(self):
        cache_file = StringIO()
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.cache.write(endpoint.key_data)
        self.assertEqual(cache_file.getvalue() , '{}')

    def test_1(self):
        cache_file = StringIO()
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict(name='Bob')
        endpoint.cache.write(endpoint.key_data)
        self.assertIn('"name": "Bob"', cache_file.getvalue())

    def test_2(self):
        cache_file = StringIO()
        endpoint = fs.FSEndpoint(cache_file=cache_file)
        endpoint.key_data = {
            'files/f1': dict(size=8, etag='hash', last_modified=1527577755.3356848),
        }
        endpoint.cache.write(endpoint.key_data)
        self.assertIn('"files/f1": {', cache_file.getvalue())
        self.assertIn('"size": 8', cache_file.getvalue())
        self.assertIn('"etag": "hash"', cache_file.getvalue())
        self.assertIn('"last_modified": 1527577755.3356848', cache_file.getvalue())


class FSEndpointUpdateKeyDataTest(TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(__file__)

    def test_1(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'd1'),
            os.path.join('files', 'f1'),
        ]
        cache_file=StringIO()
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.update_key_data()
        key_data = endpoint.key_data
        f = key_data['files/d1/f1']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], 'c3f058778ae8b1cefa04425c2178b7a6')
        f = key_data['files/d1/f2']
        self.assertEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '909a4146156421135da6b38e8efd3a3b')
        f = key_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '16fed0121505838f492d0295ba547558')
        self.assertEqual(len(key_data), 3)
        # cache file is updated
        content = cache_file.getvalue()
        self.assertIn('909a4146156421135da6b38e8efd3a3b', content)
        self.assertIn('c3f058778ae8b1cefa04425c2178b7a6', content)
        self.assertIn('16fed0121505838f492d0295ba547558', content)
        # etag attribute is updated
        self.assertEqual(
            endpoint.etag,
            {
                'files/d1/f1': 'c3f058778ae8b1cefa04425c2178b7a6',
                'files/d1/f2': '909a4146156421135da6b38e8efd3a3b',
                'files/f1': '16fed0121505838f492d0295ba547558',
            },
        )

    def test_hashed_bytes_threshold(self):
        base_path = self.base_path
        includes = [
            os.path.join('files'),
        ]
        endpoint = fs.FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO(), hashed_bytes_threshold=20)
        endpoint.update_key_data()


class FSEndpointDeleteTest(TestCase):
    def test_file(self):
        with TemporaryDirectory() as dir:
            file_name = os.path.join(dir, 'f1')
            with open(file_name, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(file_name))
            endpoint = fs.FSEndpoint(base_path=dir, cache_file=StringIO())
            endpoint.delete('f1')
            self.assertFalse(os.path.exists(file_name))

    def test_empty_dir(self):
        with TemporaryDirectory() as backup_dir:
            dir_name = os.path.join(backup_dir, 'd1')
            file_name = os.path.join(dir_name, 'f1')
            os.makedirs(dir_name)
            with open(file_name, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(file_name))
            endpoint = fs.FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            endpoint.delete('d1/f1')
            self.assertFalse(os.path.exists(file_name))
            self.assertTrue(os.path.isdir(dir_name))
