import io
import os
import boto3
from contextlib import redirect_stdout
from tempfile import TemporaryDirectory
from test.support import EnvironmentVarGuard
from unittest import TestCase
from moto import mock_s3
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
    def test_ok(self):
        bucket = boto3.resource('s3').create_bucket(Bucket='bucket')
        bucket.put_object(Key='path/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            dict(f1=dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')),
        )


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
    def test_no_bucket(self):
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        with TemporaryDirectory() as source_dir:
            path = os.path.join(source_dir, 'f1')
            with open(path, 'w') as f:
                f.write('content')
            self.assertTrue(os.path.isfile(path))
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                endpoint.upload('f1', path)
        self.assertIn('ERROR <transfer> "f1" Failed to upload', stdout.getvalue())
        self.assertIn('An error occurred (NoSuchBucket) when calling the PutObject operation', stdout.getvalue())
