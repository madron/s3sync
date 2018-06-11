import io
import os
import shutil
import time
import boto3
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from queue import Queue
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from moto import mock_s3
from ..fs import FSEndpoint
from ..s3 import S3Endpoint
from ..utils import get_queue_events


class FSEndpointGetFsKeyTest(TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(__file__)

    def test_get_path_data(self):
        endpoint = FSEndpoint(base_path=self.base_path, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, excludes=excludes, cache_file=StringIO())
        key_data = endpoint.get_fs_key_data()
        self.assertEqual(
            sorted(list(key_data.keys())),
            ['files/d1/f1', 'files/d1/f2', 'files/d2/f2'],
        )


class FSEndpointReadCacheTest(TestCase):
    def test_empty_1(self):
        cache_file = StringIO()
        endpoint = FSEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(endpoint.key_data, dict())

    def test_empty_2(self):
        cache_file = StringIO('{}')
        endpoint = FSEndpoint(cache_file=cache_file)
        self.assertEqual(endpoint.key_data, dict())
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(endpoint.key_data, dict())

    def test_1(self):
        cache_file = StringIO('{"include": {"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}}')
        endpoint = FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.key_data = endpoint.cache.read()
        self.assertEqual(
            endpoint.key_data,
            dict(include={"files/f1": {"size": 8, "etag": "hash", "last_modified": 1527577755.3356848}}),
        )


class FilesystemEndpointWriteCacheTest(TestCase):
    def test_empty(self):
        cache_file = StringIO()
        endpoint = FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict()
        endpoint.cache.write(endpoint.key_data)
        self.assertEqual(cache_file.getvalue() , '{}')

    def test_1(self):
        cache_file = StringIO()
        endpoint = FSEndpoint(cache_file=cache_file)
        endpoint.key_data = dict(name='Bob')
        endpoint.cache.write(endpoint.key_data)
        self.assertIn('"name": "Bob"', cache_file.getvalue())

    def test_2(self):
        cache_file = StringIO()
        endpoint = FSEndpoint(cache_file=cache_file)
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=cache_file)
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
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO(), hashed_bytes_threshold=20)
        endpoint.update_key_data()

    def test_cache(self):
        base_path = self.base_path
        includes = [
            os.path.join('files'),
        ]
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        self.assertEqual(endpoint.key_data, dict())
        # Update key data
        endpoint.update_key_data()
        key_data = endpoint.key_data
        f = key_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '16fed0121505838f492d0295ba547558')
        # Update again
        endpoint.update_key_data()
        key_data = endpoint.key_data
        f = key_data['files/f1']
        self.assertEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], float)
        self.assertEqual(f['etag'], '16fed0121505838f492d0295ba547558')

    def test_permission_denied(self):
        base_path = self.base_path
        includes = [
            os.path.join('files', 'f1'),
        ]
        endpoint = FSEndpoint(base_path=base_path, includes=includes, cache_file=StringIO())
        self.assertEqual(endpoint.key_data, dict())
        # Update key data
        stdout = io.StringIO()
        with patch('s3sync.utils.get_etag') as get_etag, redirect_stdout(stdout):
            get_etag.side_effect = OSError('Permission denied')
            endpoint.update_key_data()
        self.assertEqual(endpoint.key_data, dict())
        self.assertIn('ERROR <source> File:', stdout.getvalue())
        self.assertIn('files/f1 - Permission denied', stdout.getvalue())


class FSEndpointUpdateSingleKeyDataTest(TestCase):
    def test_1(self):
        with TemporaryDirectory() as backup_dir:
            endpoint = FSEndpoint(base_path=backup_dir, includes=[''], cache_file=StringIO())
            f1 = endpoint.get_path('f1')
            f2 = endpoint.get_path('f2')
            with open(f1, 'w') as f:
                f.write('content')
            endpoint.update_key_data()
            # key_data
            f = endpoint.key_data['f1']
            self.assertEqual(f['size'], 7)
            self.assertIsInstance(f['last_modified'], float)
            self.assertEqual(f['etag'], '9a0364b9e99bb480dd25e1f0284c8555')
            self.assertEqual(len(endpoint.key_data), 1)
            # Etag
            self.assertEqual(
                endpoint.etag['f1'],
                '9a0364b9e99bb480dd25e1f0284c8555',
            )
            self.assertEqual(len(endpoint.etag), 1)
            # Totals
            self.assertEqual(endpoint.total_files, 1)
            self.assertEqual(endpoint.total_bytes, 7)
            # Update single key data
            with open(f2, 'w') as f:
                f.write('contentcontent')
            endpoint.update_single_key_data('f2')
            # key_data
            f = endpoint.key_data['f2']
            self.assertEqual(f['size'], 14)
            self.assertIsInstance(f['last_modified'], float)
            self.assertEqual(f['etag'], '6858851eee0e05f318897984757b59dc')
            self.assertEqual(len(endpoint.key_data), 2)
            # Etag
            self.assertEqual(
                endpoint.etag['f2'],
                '6858851eee0e05f318897984757b59dc',
            )
            self.assertEqual(len(endpoint.etag), 2)
            # Totals
            self.assertEqual(endpoint.total_files, 2)
            self.assertEqual(endpoint.total_bytes, 21)




class FSEndpointGetDestinationPathTest(TestCase):
    def test_ok(self):
        with TemporaryDirectory() as backup_dir:
            endpoint = FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            path = endpoint.get_destination_path('f1')
            self.assertEqual(path, os.path.join(backup_dir, 'f1'))

    def test_create_missing_dir(self):
        with TemporaryDirectory() as backup_dir:
            dir_name = os.path.join(backup_dir, 'd1')
            self.assertFalse(os.path.exists(dir_name))
            endpoint = FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            path = endpoint.get_destination_path('d1/f1')
            self.assertEqual(path, os.path.join(backup_dir, 'd1/f1'))
            self.assertTrue(os.path.isdir(dir_name))

    def test_dir_to_file(self):
        ' Target path already exist but it is a directory '
        with TemporaryDirectory() as backup_dir:
            dir1 = os.path.join(backup_dir, 'd1')
            dir2 = os.path.join(backup_dir, 'd1', 'f1')
            os.makedirs(dir2)
            self.assertTrue(os.path.isdir(dir2))
            with open(os.path.join(dir2, 'f9'), 'w') as f:
                f.write('previous content')
            endpoint = FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            path = endpoint.get_destination_path('d1/f1')
            self.assertEqual(path, os.path.join(backup_dir, 'd1/f1'))
            self.assertTrue(os.path.isdir(dir1))
            # dir2 tree is removed
            self.assertFalse(os.path.exists(dir2))

    def test_file_to_dir(self):
        ' Target path directory already exist but it is file '
        with TemporaryDirectory() as backup_dir:
            dir_name = os.path.join(backup_dir, 'd1')
            with open(dir_name, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(dir_name))
            endpoint = FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            path = endpoint.get_destination_path('d1/f1')
            self.assertEqual(path, os.path.join(backup_dir, 'd1/f1'))
            # file is removed and directory is created
            self.assertTrue(os.path.isdir(dir_name))


class FSEndpointCopyTest(TestCase):
    def test_ok(self):
        with TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            destination_dir = os.path.join(temp_dir, 'destination', 'subdir')
            # Write source file
            os.makedirs(source_dir)
            source_path = os.path.join(source_dir, 'f1')
            destination_path = os.path.join(destination_dir, 'f1')
            with open(source_path, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(source_path))
            # Destination dir does not exist
            self.assertFalse(os.path.exists(os.path.join(temp_dir, 'destination')))
            # Endpoints
            source_endpoint = FSEndpoint(base_path=source_dir, cache_file=StringIO())
            destination_endpoint = FSEndpoint(base_path=destination_dir, cache_file=StringIO())
            source_endpoint.copy('f1', destination_endpoint)
            self.assertTrue(os.path.isfile(destination_path))
            with open(destination_path, 'r') as f:
                self.assertEqual(f.read(), 'content')

    def test_ko(self):
        with TemporaryDirectory() as temp_dir:
            source_dir = os.path.join(temp_dir, 'source')
            destination_dir = os.path.join(temp_dir, 'destination', 'subdir')
            source_path = os.path.join(source_dir, 'f1')
            destination_path = os.path.join(destination_dir, 'f1')
            # Source file does not exist
            self.assertFalse(os.path.exists(source_path))
            # Endpoints
            source_endpoint = FSEndpoint(base_path=source_dir, cache_file=StringIO())
            self.assertFalse(os.path.exists(source_path))
            destination_endpoint = FSEndpoint(base_path=destination_dir, cache_file=StringIO())
            self.assertFalse(os.path.exists(source_path))
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                source_endpoint.copy('f1', destination_endpoint)
            self.assertIn('ERROR <transfer> "f1" [Errno 2] No such file or directory', stdout.getvalue())
            self.assertFalse(os.path.isfile(destination_path))

    def test_wrong_destination_endpoint(self):
        source_endpoint = FSEndpoint(cache_file=StringIO())
        destination_endpoint = FSEndpoint(cache_file=StringIO())
        destination_endpoint.type = 'notfs'
        with self.assertRaises(AssertionError):
            source_endpoint.copy('f1', destination_endpoint)


class FSEndpointDeleteTest(TestCase):
    def test_file(self):
        with TemporaryDirectory() as dir:
            file_name = os.path.join(dir, 'f1')
            with open(file_name, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(file_name))
            endpoint = FSEndpoint(base_path=dir, cache_file=StringIO())
            endpoint.delete('f1')
            self.assertFalse(os.path.exists(file_name))

    def test_file_does_not_exist(self):
        with TemporaryDirectory() as dir:
            file_name = os.path.join(dir, 'f1')
            self.assertFalse(os.path.exists(file_name))
            endpoint = FSEndpoint(base_path=dir, cache_file=StringIO())
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                endpoint.delete('f1')
            self.assertIn('ERROR <delete> "f1" [Errno 2] No such file or directory', stdout.getvalue())

    def test_empty_dir(self):
        with TemporaryDirectory() as backup_dir:
            dir_name = os.path.join(backup_dir, 'd1')
            file_name = os.path.join(dir_name, 'f1')
            os.makedirs(dir_name)
            with open(file_name, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(file_name))
            endpoint = FSEndpoint(base_path=backup_dir, cache_file=StringIO())
            endpoint.delete('d1/f1')
            self.assertFalse(os.path.exists(file_name))
            self.assertTrue(os.path.isdir(dir_name))


class FSEndpointObserverTest(TestCase):
    def test_1(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            dir1 = os.path.join(backup_dir, 'd1')
            dir2 = os.path.join(backup_dir, 'd2')
            os.makedirs(dir1)
            os.makedirs(dir2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1', 'd2'],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = endpoint.get_path('d1/f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd1/f1')

    def test_2(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            dir1 = os.path.join(backup_dir, 'd1')
            dir2 = os.path.join(backup_dir, 'd2')
            os.makedirs(dir1)
            os.makedirs(dir2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1', 'd2'],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = endpoint.get_path('d1/f1')
            with open(file_path, 'w') as f:
                f.write('content')
            file_path = endpoint.get_path('d2/f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 2)
            event = events[0]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd1/f1')
            event = events[1]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd2/f1')

    def test_outside_base(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            base_path = os.path.join(backup_dir, 'd1')
            os.makedirs(base_path)
            endpoint = FSEndpoint(
                base_path=base_path,
                includes=[''],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = os.path.join(backup_dir, 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 0)

    def test_not_included(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1'],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = os.path.join(backup_dir, 'd2', 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 0)

    def test_excluded_1(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                excludes=['d2'],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = os.path.join(backup_dir, 'd2', 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 0)

    def test_excluded_2(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                excludes=['d1'],
                cache_file=StringIO(),
            )
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            file_path = os.path.join(backup_dir, 'd2', 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd2/f1')

    def test_delete_file(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                cache_file=StringIO(),
            )
            file_path = endpoint.get_path('f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Remove file
            os.remove(file_path)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'f1')

    def test_delete_directory(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            os.makedirs(d1)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                cache_file=StringIO(),
            )
            file_path = endpoint.get_path('d1/f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Remove directory
            shutil.rmtree(d1)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'd1/f1')

    def test_delete_excluded(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                excludes=['d2'],
                cache_file=StringIO(),
            )
            file_path = os.path.join(backup_dir, 'd2', 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Remove file
            os.remove(file_path)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 0)

    def test_move_file(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                cache_file=StringIO(),
            )
            file_path = endpoint.get_path('f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move file
            os.rename(file_path, endpoint.get_path('f2'))
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 2)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'f1')
            event = events[1]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'f2')

    def test_move_file_from_outside_to_inside(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1'],
                cache_file=StringIO(),
            )
            source_file_path = endpoint.get_path('d2/f1')
            destination_file_path = endpoint.get_path('d1/f1')
            with open(source_file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move file
            os.rename(source_file_path, destination_file_path)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd1/f1')

    def test_move_file_from_inside_to_outside(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1'],
                cache_file=StringIO(),
            )
            source_file_path = endpoint.get_path('d1/f1')
            destination_file_path = endpoint.get_path('d2/f1')
            with open(source_file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move file
            os.rename(source_file_path, destination_file_path)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'd1/f1')

    def test_move_directory(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=[''],
                cache_file=StringIO(),
            )
            file_path = endpoint.get_path('d1/f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move directory
            os.rename(d1, d2)
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 2)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'd1/f1')
            event = events[1]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd2/f1')

    def test_move_directory_from_outside_to_inside(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1')
            d2 = os.path.join(backup_dir, 'd2', 'd3')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1'],
                cache_file=StringIO(),
            )
            file_path = os.path.join(d2, 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move directory
            os.rename(d2, os.path.join(d1, 'd4'))
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'modified')
            self.assertEqual(event['key'], 'd1/d4/f1')

    def test_move_directory_from_inside_to_outside(self):
        events_queue = Queue()
        with TemporaryDirectory() as backup_dir:
            d1 = os.path.join(backup_dir, 'd1', 'd3')
            d2 = os.path.join(backup_dir, 'd2')
            os.makedirs(d1)
            os.makedirs(d2)
            endpoint = FSEndpoint(
                base_path=backup_dir,
                includes=['d1'],
                cache_file=StringIO(),
            )
            endpoint.key_data = {'d1/d3/f1': dict()}
            file_path = os.path.join(d1, 'f1')
            with open(file_path, 'w') as f:
                f.write('content')
            endpoint.observer_start(events_queue)
            self.assertTrue(events_queue.empty())
            # Move directory
            os.rename(d1, os.path.join(d2, 'd4'))
            os.sync()
            time.sleep(0.5)
            endpoint.observer_stop()
            events = get_queue_events(events_queue)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(event['type'], 'deleted')
            self.assertEqual(event['key'], 'd1/d3/f1')
