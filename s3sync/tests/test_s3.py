from unittest import TestCase
from ..s3 import S3Endpoint


class S3EndpointInitTest(TestCase):
    def test_ok(self):
        endpoint = S3Endpoint(base_url='default:bucket/path')
        self.assertEqual(endpoint.profile, 'default')
        self.assertEqual(endpoint.bucket_name, 'bucket')
        self.assertEqual(endpoint.base_path, 'path')
