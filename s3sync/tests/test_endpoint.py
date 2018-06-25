import io
from contextlib import redirect_stdout
from unittest import TestCase
from ..endpoint import BaseEndpoint


class BaseEndpointTest(TestCase):
    def test_is_excluded_false(self):
        endpoint = BaseEndpoint(includes=['files'], excludes=['files/trash'])
        self.assertFalse(endpoint.is_excluded('files/d1/f1'))

    def test_is_excluded_true(self):
        endpoint = BaseEndpoint(includes=['files'], excludes=['files/trash'])
        self.assertTrue(endpoint.is_excluded('files/trash/f1'))

    def test_is_excluded_surrogates_error(self):
        endpoint = BaseEndpoint(includes=['files'], excludes=['files/trash'])
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertTrue(endpoint.is_excluded('files/d1/f1\udcf9'))
        self.assertIn("ERROR <> files/d1/f1?", stdout.getvalue())
        self.assertIn("'utf-8' codec can't encode character", stdout.getvalue())
        self.assertIn("surrogates not allowed", stdout.getvalue())

    def test_update_single_key_data(self):
        endpoint = BaseEndpoint()
        endpoint.update_single_key_data('f1')

    def test_update_etag(self):
        endpoint = BaseEndpoint()
        self.assertEqual(endpoint.etag, dict())
        endpoint.key_data = dict(
            f1=dict(size=1, etag='etag1'),
            f2=dict(size=2, etag='etag2'),
        )
        endpoint.update_etag()
        self.assertEqual(endpoint.etag, dict(f1='etag1', f2='etag2'))

    def test_log_info(self):
        endpoint = BaseEndpoint(log_prefix='s3sync', verbosity=10)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            endpoint.log_info('text')
        self.assertEqual(stdout.getvalue(), 'INFO <s3sync> text\n')

    def test_log_debug(self):
        endpoint = BaseEndpoint(log_prefix='s3sync', verbosity=10)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            endpoint.log_debug('text')
        self.assertEqual(stdout.getvalue(), 'DEBUG <s3sync> text\n')

    def test_log_error(self):
        endpoint = BaseEndpoint(log_prefix='s3sync', verbosity=0)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            endpoint.log_error('text')
        self.assertEqual(stdout.getvalue(), 'ERROR <s3sync> text\n')

    def test_transfer(self):
        endpoint = BaseEndpoint()
        with self.assertRaises(NotImplementedError):
            endpoint.transfer('f1', None)

    def test_write_cache(self):
        endpoint = BaseEndpoint()
        endpoint.write_cache()

    def test_delete(self):
        endpoint = BaseEndpoint()
        with self.assertRaises(NotImplementedError):
            endpoint.delete('f1')

    def test_observer_start(self):
        endpoint = BaseEndpoint()
        endpoint.observer_start(None)

    def test_observer_stop(self):
        endpoint = BaseEndpoint()
        endpoint.observer_stop()
