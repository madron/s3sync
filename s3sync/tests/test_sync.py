import io
from unittest import TestCase
from ..sync import SyncManager


class SyncManagerTest(TestCase):
    def test_init(self):
        options = dict(
            source='/tmp',
            destination='default:bucket/path',
            cache_file=io.StringIO(),
        )
        manager = SyncManager(**options)
