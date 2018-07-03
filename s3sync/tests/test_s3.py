import io
import os
import boto3
from contextlib import redirect_stdout
from tempfile import TemporaryDirectory
from test.support import EnvironmentVarGuard
from unittest import TestCase
from moto import mock_s3
from .. import exceptions
from .. import utils
from ..fs import FSEndpoint
from ..s3 import S3Endpoint


class S3EndpointInitTest(TestCase):
    def test_ok(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        self.assertEqual(endpoint.profile_name, 'default')
        self.assertEqual(endpoint.bucket_name, 'bucket')
        self.assertEqual(endpoint.base_path, 'path')


class S3EndpointGetProfileTest(TestCase):
    def test_get_profile_1(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        with EnvironmentVarGuard() as env:
            env.set('AWS_ACCESS_KEY_ID', 'user')
            env.set('AWS_SECRET_ACCESS_KEY', 'pass')
            profile = endpoint.get_profile(env_first=True)
        self.assertEqual(profile['aws_access_key_id'], 'user')
        self.assertEqual(profile['aws_secret_access_key'], 'pass')

    def test_get_profile_2(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        with EnvironmentVarGuard() as env:
            env.set('AWS_ACCESS_KEY_ID', 'user')
            env.set('AWS_SECRET_ACCESS_KEY', 'pass')
            env.set('REGION_NAME', 'us-east-1')
            env.set('ENDPOINT_URL', 'http://localhost')
            profile = endpoint.get_profile(env_first=True)
        self.assertEqual(profile['aws_access_key_id'], 'user')
        self.assertEqual(profile['aws_secret_access_key'], 'pass')
        self.assertEqual(profile['region_name'], 'us-east-1')
        self.assertEqual(profile['endpoint_url'], 'http://localhost')


class S3EndpointGetBucketTest(TestCase):
    def test_get_bucket(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        with EnvironmentVarGuard() as env:
            env.set('AWS_ACCESS_KEY_ID', 'user')
            env.set('AWS_SECRET_ACCESS_KEY', 'pass')
            env.set('REGION_NAME', 'us-east-1')
            env.set('ENDPOINT_URL', 'http://localhost')
            bucket = endpoint.get_bucket()
        self.assertIsInstance(bucket, boto3.resources.base.ServiceResource)
        self.assertEqual(bucket.name, 'bucket')

    def test_get_bucket_cache(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        with EnvironmentVarGuard() as env:
            env.set('AWS_ACCESS_KEY_ID', 'user')
            env.set('AWS_SECRET_ACCESS_KEY', 'pass')
            env.set('REGION_NAME', 'us-east-1')
            env.set('ENDPOINT_URL', 'http://localhost')
            bucket = endpoint.get_bucket()
            bucket = endpoint.get_bucket()
        self.assertIsInstance(bucket, boto3.resources.base.ServiceResource)
        self.assertEqual(bucket.name, 'bucket')


class S3EndpointUpdateKeyDataTest(TestCase):
    @mock_s3
    def test_bucket(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket', includes=[''])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_path(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/d1', includes=[''])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_include(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket', includes=['d1'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_path_include(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='backup/d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/backup', includes=['d1'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_include_slash_1(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket', includes=['/d1'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_include_slash_2(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket', includes=['d1/'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_include_slash_3(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket', includes=['/d1/'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_path_include_slash_1(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='backup/d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/backup', includes=['/d1'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_path_include_slash_2(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='backup/d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/backup', includes=['d1/'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )

    @mock_s3
    def test_bucket_path_include_slash_3(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='backup/d1/d2/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/backup', includes=['/d1/'])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            {'d1/d2/f1': dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')},
        )


class S3EndpointUpdateSingleKeyDataTest(TestCase):
    @mock_s3
    def test_add(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='path/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.update_key_data()
        data = endpoint.key_data['f1']
        self.assertEqual(data['size'], 7)
        self.assertEqual(data['etag'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.key_data), 1)
        self.assertEqual(endpoint.etag['f1'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.etag), 1)
        self.assertEqual(endpoint.counter.total_files, 1)
        self.assertEqual(endpoint.counter.total_bytes, 7)
        # Add object
        bucket.put_object(Key='path/f2', Body='contentcontent')
        endpoint.update_single_key_data('f2')
        data = endpoint.key_data['f2']
        self.assertEqual(data['size'], 14)
        self.assertEqual(data['etag'], '6858851eee0e05f318897984757b59dc')
        self.assertEqual(len(endpoint.key_data), 2)
        self.assertEqual(endpoint.etag['f2'], '6858851eee0e05f318897984757b59dc')
        self.assertEqual(len(endpoint.etag), 2)
        self.assertEqual(endpoint.counter.total_files, 2)
        self.assertEqual(endpoint.counter.total_bytes, 21)

    @mock_s3
    def test_change(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='path/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.update_key_data()
        data = endpoint.key_data['f1']
        self.assertEqual(data['size'], 7)
        self.assertEqual(data['etag'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.key_data), 1)
        self.assertEqual(endpoint.etag['f1'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.etag), 1)
        self.assertEqual(endpoint.counter.total_files, 1)
        self.assertEqual(endpoint.counter.total_bytes, 7)
        # Change object
        bucket.put_object(Key='path/f1', Body='contentcontent')
        endpoint.update_single_key_data('f1')
        data = endpoint.key_data['f1']
        self.assertEqual(data['size'], 14)
        self.assertEqual(data['etag'], '6858851eee0e05f318897984757b59dc')
        self.assertEqual(len(endpoint.key_data), 1)
        self.assertEqual(endpoint.etag['f1'], '6858851eee0e05f318897984757b59dc')
        self.assertEqual(len(endpoint.etag), 1)
        self.assertEqual(endpoint.counter.total_files, 1)
        self.assertEqual(endpoint.counter.total_bytes, 14)

    @mock_s3
    def test_delete(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='path/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.update_key_data()
        data = endpoint.key_data['f1']
        self.assertEqual(data['size'], 7)
        self.assertEqual(data['etag'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.key_data), 1)
        self.assertEqual(endpoint.etag['f1'], '9a0364b9e99bb480dd25e1f0284c8555')
        self.assertEqual(len(endpoint.etag), 1)
        self.assertEqual(endpoint.counter.total_files, 1)
        self.assertEqual(endpoint.counter.total_bytes, 7)
        # Delete object
        bucket.Object('path/f1').delete()
        endpoint.update_single_key_data('f1')
        self.assertEqual(len(endpoint.key_data), 0)
        self.assertEqual(len(endpoint.etag), 0)
        self.assertEqual(endpoint.counter.total_files, 0)
        self.assertEqual(endpoint.counter.total_bytes, 0)



class S3EndpointGetPathTest(TestCase):
    def test_1(self):
        endpoint = S3Endpoint(base_url='default:bucket')
        path = endpoint.get_path('f1')
        self.assertEqual(path, 'f1')

    def test_2(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        path = endpoint.get_path('f1')
        self.assertEqual(path, 'path/f1')

    def test_3(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        path = endpoint.get_path('d1/f1')
        self.assertEqual(path, 'path/d1/f1')


class S3EndpointUploadTest(TestCase):
    @mock_s3
    def test_ok(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        with TemporaryDirectory() as source_dir:
            path = os.path.join(source_dir, 'f1')
            with open(path, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(path))
            endpoint.upload('f1', path)
        obj = bucket.Object('path/f1').get()
        self.assertEqual(obj['ETag'], '"9a0364b9e99bb480dd25e1f0284c8555"')
        self.assertEqual(obj['ContentLength'], 7)
        self.assertEqual(obj['Body'].read(), b'content')

    @mock_s3
    def test_source_not_found(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        with TemporaryDirectory() as source_dir:
            path = os.path.join(source_dir, 'f1')
            self.assertFalse(os.path.isfile(path))
            with self.assertRaises(exceptions.SourceVanishedError):
                endpoint.upload('f1', path)


class S3EndpointDownloadTest(TestCase):
    @mock_s3
    def test_ok(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        obj = bucket.put_object(Key='path/f1', Body='content').get()
        self.assertEqual(obj['ETag'], '"9a0364b9e99bb480dd25e1f0284c8555"')
        source_endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        with TemporaryDirectory() as destination_dir:
            destination_path = os.path.join(destination_dir, 'f1')
            source_endpoint.download('f1', destination_path)
            self.assertTrue(os.path.isfile(destination_path))
            with open(destination_path, 'r') as f:
                self.assertEqual(f.read(), 'content')
            etag = utils.get_etag(destination_path)
            self.assertEqual(etag, '9a0364b9e99bb480dd25e1f0284c8555')


class S3EndpointDeleteTest(TestCase):
    @mock_s3
    def test_ok(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        obj = bucket.put_object(Key='path/f1', Body='content').get()
        objects = list(bucket.objects.filter(Prefix='path/f1'))
        self.assertEqual(len(objects), 1)
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.delete('f1')
        objects = list(bucket.objects.filter(Prefix='path/f1'))
        self.assertEqual(len(objects), 0)
