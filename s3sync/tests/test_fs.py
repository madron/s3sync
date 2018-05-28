import os
from datetime import datetime
from unittest import TestCase
from .. import fs


class FilesystemEndpointTest(TestCase):
    def setUp(self):
        self.files_path = os.path.join(os.path.dirname(__file__), 'files')

    def test_get_file_list(self):
        file_list = fs.FilesystemEndpoint().get_file_list(self.files_path)
        f = file_list[0]
        self.assertTrue(f['name'].endswith('tests/files/d1/f1'))
        self.assertAlmostEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], datetime)
        f = file_list[1]
        self.assertTrue(f['name'].endswith('tests/files/d1/f2'))
        self.assertAlmostEqual(f['size'], 14)
        self.assertIsInstance(f['last_modified'], datetime)
        f = file_list[2]
        self.assertTrue(f['name'].endswith('tests/files/f1'))
        self.assertAlmostEqual(f['size'], 8)
        self.assertIsInstance(f['last_modified'], datetime)
