import os
import boto3
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


@mock_s3
class S3EndpointUpdateKeyDataTest(TestCase):
    def setUp(self):
        s3 = boto3.resource('s3')
        self.bucket = s3.create_bucket(Bucket='bucket')

    def test_update_key_data(self):
        self.bucket.put_object(Key='path/f1', Body='content')
        endpoint = S3Endpoint(base_url='default:bucket/path', includes=[''])
        endpoint.update_key_data()
        self.assertEqual(
            endpoint.key_data,
            dict(f1=dict(size=7, etag='9a0364b9e99bb480dd25e1f0284c8555')),
        )
