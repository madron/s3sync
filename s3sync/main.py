from argparse import ArgumentParser
from .constants import DEFAULT_SOURCE
from .constants import DEFAULT_CACHE_DIR
from .sync import SyncManager


def main():
    parser = ArgumentParser(description='S3 sync')
    parser.add_argument('--source', type=str, default=DEFAULT_SOURCE,
        help='Source endpoint, default: {}'.format(DEFAULT_SOURCE))
    parser.add_argument('--destination', type=str, default=DEFAULT_SOURCE,
        help='Destination endpoint, default: {}'.format(DEFAULT_SOURCE))
    parser.add_argument('--cache-dir', dest='cache_dir', type=str,
        default=DEFAULT_CACHE_DIR, metavar='DIR',
        help='Cache directory, default: {}'.format(DEFAULT_CACHE_DIR))
    parser.add_argument('--include', dest='includes', type=str, nargs='+', metavar='PATH',
        default=[''], help='Paths to include, if not specified it will sync everything in source path')
    parser.add_argument('--exclude', dest='excludes', type=str, nargs='*',
        default=[], help='paths to exclude', metavar='PATH')
    parser.add_argument('-v', '--verbosity', dest='verbosity', type=int,
        default=1, help='Verbosity level', metavar='N')
    parser.add_argument('--fake', dest='fake', action='store_true', help='Fake run')

    options = vars(parser.parse_args())

    manager = SyncManager(**options)
    manager.sync()
