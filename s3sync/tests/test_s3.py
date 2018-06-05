import os
import boto3
from unittest import TestCase
from ..s3 import S3Endpoint


class S3EndpointInitTest(TestCase):
    def test_ok(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        self.assertEqual(endpoint.profile_name, 'default')
        self.assertEqual(endpoint.bucket_name, 'bucket')
        self.assertEqual(endpoint.base_path, 'path')

    def test_get_profile_1(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        os.environ['AWS_ACCESS_KEY_ID'] = 'user'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'pass'
        profile = endpoint.get_profile(env_first=True)
        self.assertEqual(profile['aws_access_key_id'], 'user')
        self.assertEqual(profile['aws_secret_access_key'], 'pass')

    def test_get_profile_2(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        os.environ['AWS_ACCESS_KEY_ID'] = 'user'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'pass'
        os.environ['REGION_NAME'] = 'us-east-1'
        os.environ['ENDPOINT_URL'] = 'http://localhost'
        profile = endpoint.get_profile(env_first=True)
        self.assertEqual(profile['aws_access_key_id'], 'user')
        self.assertEqual(profile['aws_secret_access_key'], 'pass')
        self.assertEqual(profile['region_name'], 'us-east-1')
        self.assertEqual(profile['endpoint_url'], 'http://localhost')

    def test_get_bucket(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        os.environ['AWS_ACCESS_KEY_ID'] = 'user'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'pass'
        os.environ['REGION_NAME'] = 'us-east-1'
        os.environ['ENDPOINT_URL'] = 'http://localhost'
        bucket = endpoint.get_bucket()
        self.assertIsInstance(bucket, boto3.resources.base.ServiceResource)
        self.assertEqual(bucket.name, 'bucket')

    def test_get_bucket_cache(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        os.environ['AWS_ACCESS_KEY_ID'] = 'user'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'pass'
        os.environ['REGION_NAME'] = 'us-east-1'
        os.environ['ENDPOINT_URL'] = 'http://localhost'
        bucket = endpoint.get_bucket()
        bucket = endpoint.get_bucket()
        self.assertIsInstance(bucket, boto3.resources.base.ServiceResource)
        self.assertEqual(bucket.name, 'bucket')
