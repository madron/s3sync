import os
from unittest import TestCase
from .. import utils


class GetEtagTest(TestCase):
    def setUp(self):
        self.files_path = os.path.join(os.path.dirname(__file__), 'files')

    def test_small(self):
        etag = utils.get_etag(os.path.join(self.files_path, 'f1'))
        self.assertEqual(etag, '16fed0121505838f492d0295ba547558')

    def test_multipart(self):
        etag = utils.get_etag(os.path.join(self.files_path, 'f1'), 2)
        self.assertEqual(etag, '040e6beb623b405da8d66016cfcf75ad-4')


class ParseS3UrlTest(TestCase):
    def test_profile_bucket(self):
        profile, bucket, path = utils.parse_s3_url('default:bucket1')
        self.assertEqual(profile, 'default')
        self.assertEqual(bucket, 'bucket1')
        self.assertEqual(path, '')

    def test_profile_bucket_path(self):
        profile, bucket, path = utils.parse_s3_url('default:bucket1/attic')
        self.assertEqual(profile, 'default')
        self.assertEqual(bucket, 'bucket1')
        self.assertEqual(path, 'attic')

    def test_profile_bucket_path_slash(self):
        profile, bucket, path = utils.parse_s3_url('minio:bck2/attic/')
        self.assertEqual(profile, 'minio')
        self.assertEqual(bucket, 'bck2')
        self.assertEqual(path, 'attic')


class GetOperationsTest(TestCase):
    def test_up_to_date(self):
        source = dict(f1='t1', f2='t2')
        destination = dict(f1='t1', f2='t2')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=[], delete=[]))

    def test_destination_missing(self):
        source = dict(f1='t1', f2='t2')
        destination = dict(f1='t1')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=['f2'], delete=[]))

    def test_etag_changed(self):
        source = dict(f1='t1', f2='t2')
        destination = dict(f1='changed', f2='t2')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=['f1'], delete=[]))

    def test_removed_file(self):
        source = dict(f1='t1',)
        destination = dict(f1='t1', f2='t2')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=[], delete=['f2']))

    def test_1(self):
        source = dict(f1='t1',)
        destination = dict(f1='new', f2='t2')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=['f1'], delete=['f2']))

    def test_2(self):
        source = dict(f1='t1', f2='t2')
        destination = dict(f1='new')
        operations = utils.get_operations(source, destination)
        self.assertEqual(operations, dict(transfer=['f1', 'f2'], delete=[]))
