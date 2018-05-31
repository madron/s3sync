from unittest import TestCase
from ..endpoint import BaseEndpoint


class BaseEndpointTest(TestCase):
    def test_update_etag(self):
        endpoint = BaseEndpoint()
        self.assertEqual(endpoint.etag, dict())
        endpoint.key_data = dict(
            f1=dict(size=1, etag='etag1'),
            f2=dict(size=2, etag='etag2'),
        )
        endpoint.update_etag()
        self.assertEqual(endpoint.etag, dict(f1='etag1', f2='etag2'))

    def test_update_totals(self):
        endpoint = BaseEndpoint()
        self.assertEqual(endpoint.total_files, 0)
        self.assertEqual(endpoint.total_bytes, 0)
        endpoint.key_data = dict(
            f1=dict(size=1, etag='etag1'),
            f2=dict(size=2, etag='etag2'),
        )
        endpoint.update_totals()
        self.assertEqual(endpoint.total_files, 2)
        self.assertEqual(endpoint.total_bytes, 3)
