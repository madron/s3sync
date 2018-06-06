import io
import os
from contextlib import redirect_stdout
from tempfile import TemporaryDirectory
from unittest import TestCase
from ..main import main


class MainTest(TestCase):
    def test_help(self):
        with TemporaryDirectory() as source_dir, TemporaryDirectory() as destination_dir, TemporaryDirectory() as cache_dir:
            open(os.path.join(source_dir, 'f1'), 'w').close()
            open(os.path.join(destination_dir, 'f2'), 'w').close()
            args = [
                '--source', source_dir,
                '--destination', destination_dir,
                '--cache-dir', cache_dir,
            ]
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                rc = main(args)
        self.assertEqual(rc, 0)
        self.assertIn('INFO <source> Total files: 1', stdout.getvalue())
        self.assertIn('INFO <transfer> f1', stdout.getvalue())
        self.assertIn('INFO <delete> f2', stdout.getvalue())
