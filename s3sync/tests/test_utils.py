import os
from unittest import TestCase
from s3sync import utils


class GetEtagTest(TestCase):
    def setUp(self):
        self.files_path = os.path.join(os.path.dirname(__file__), 'files')

    def test_small(self):
        etag = utils.get_etag(os.path.join(self.files_path, 'f1'))
        self.assertEqual(etag, '16fed0121505838f492d0295ba547558')

    def test_multipart(self):
        etag = utils.get_etag(os.path.join(self.files_path, 'f1'), 2)
        self.assertEqual(etag, '040e6beb623b405da8d66016cfcf75ad-4')
