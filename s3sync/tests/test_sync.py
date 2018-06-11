import io
import os
import boto3
from queue import Queue
from tempfile import TemporaryDirectory
from unittest import TestCase
from moto import mock_s3
from ..sync import SyncManager


class SyncManagerTest(TestCase):
    def test_init(self):
        options = dict(
            source='/tmp',
            destination='default:bucket/path',
            cache_file=io.StringIO(),
        )
        manager = SyncManager(**options)


class SyncManagerTransferTest(TestCase):
    def test_fs_to_fs(self):
        with TemporaryDirectory() as source_dir, TemporaryDirectory() as destination_dir:
            manager = SyncManager(
                source=source_dir,
                destination=destination_dir,
                includes=[''],
                cache_file=io.StringIO(),
            )
            source_path = os.path.join(manager.source.get_path('f1'))
            with open(source_path, 'w') as f:
                f.write('content')
            manager.transfer('f1')
            destination_path = os.path.join(manager.destination.get_path('f1'))
            self.assertTrue(os.path.isfile(destination_path))

    @mock_s3
    def test_fs_to_s3(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        with TemporaryDirectory() as source_dir:
            manager = SyncManager(
                source=source_dir,
                destination='default:bucket/path',
                includes=[''],
                cache_file=io.StringIO(),
            )
            source_path = os.path.join(manager.source.get_path('f1'))
            with open(source_path, 'w') as f:
                f.write('content')
            manager.transfer('f1')
        obj = bucket.Object('path/f1').get()
        self.assertEqual(obj['ETag'], '"9a0364b9e99bb480dd25e1f0284c8555"')
        self.assertEqual(obj['ContentLength'], 7)
        self.assertEqual(obj['Body'].read(), b'content')

    @mock_s3
    def test_s3_to_fs(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='path/f1', Body='content')
        with TemporaryDirectory() as destination_dir:
            manager = SyncManager(
                source='default:bucket/path',
                destination=destination_dir,
                includes=[''],
                cache_file=io.StringIO(),
            )
            manager.transfer('f1')
            destination_path = manager.destination.get_path('f1')
            self.assertTrue(os.path.isfile(destination_path))

    def test_not_supported(self):
        with TemporaryDirectory() as work_dir:
            manager = SyncManager(
                source=work_dir,
                destination=work_dir,
                includes=[''],
                cache_file=io.StringIO(),
            )
        manager.source.type = 'unsupported'
        with self.assertRaises(NotImplementedError):
            manager.transfer('f1')


class SyncManagerGetEventsOperationsTest(TestCase):
    def setUp(self):
        self.manager = SyncManager(
            source='/tmp/backup',
            destination='/tmp/restore',
            cache_file=io.StringIO(),
        )

    def test_no_events(self):
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=[], delete=[]))

    def test_source_modified_1(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_source_modified_2(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.source_queue.put(dict(type='modified', key='f2'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1', 'f2'], delete=[]))

    def test_source_modified_3(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_source_modified_destination_modified_1(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.destination_queue.put(dict(type='modified', key='f2'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1', 'f2'], delete=[]))

    def test_source_modified_destination_modified_2(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.destination_queue.put(dict(type='modified', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_source_modified_destination_deleted_1(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.destination_queue.put(dict(type='deleted', key='f2'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1', 'f2'], delete=[]))

    def test_source_modified_destination_deleted_2(self):
        self.manager.source_queue.put(dict(type='modified', key='f1'))
        self.manager.destination_queue.put(dict(type='deleted', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_source_deleted(self):
        self.manager.source_queue.put(dict(type='deleted', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=[], delete=['f1']))

    def test_source_deleted_destination_deleted_1(self):
        self.manager.source_queue.put(dict(type='deleted', key='f1'))
        self.manager.destination_queue.put(dict(type='deleted', key='f2'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f2'], delete=['f1']))

    def test_source_deleted_destination_deleted_2(self):
        self.manager.source_queue.put(dict(type='deleted', key='f1'))
        self.manager.destination_queue.put(dict(type='deleted', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=[], delete=['f1']))

    def test_source_deleted_destination_modified_1(self):
        self.manager.source_queue.put(dict(type='deleted', key='f1'))
        self.manager.destination_queue.put(dict(type='modified', key='f2'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f2'], delete=['f1']))

    def test_source_deleted_destination_modified_2(self):
        self.manager.source_queue.put(dict(type='deleted', key='f1'))
        self.manager.destination_queue.put(dict(type='modified', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=[], delete=['f1']))

    def test_destination_deleted(self):
        self.manager.destination_queue.put(dict(type='deleted', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_destination_modified_deleted(self):
        self.manager.destination_queue.put(dict(type='modified', key='f1'))
        self.manager.destination_queue.put(dict(type='deleted', key='f1'))
        operations = self.manager.get_events_operations()
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))
