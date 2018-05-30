import pathlib
from argparse import ArgumentParser
from .fs import FilesystemEndpoint
from .s3 import S3Endpoint

DEFAULT_SOURCE = DEFAULT_CACHE_DIR = pathlib.Path.home().as_posix()


def main():
    parser = ArgumentParser(description='S3 sync')
    parser.add_argument('--source', type=str, default=DEFAULT_SOURCE,
        help='Source endpoint, default: {}'.format(DEFAULT_SOURCE))
    parser.add_argument('--cache-dir', dest='cache_dir', type=str,
        default=DEFAULT_CACHE_DIR, metavar='DIR',
        help='Cache directory, default: {}'.format(DEFAULT_CACHE_DIR))
    parser.add_argument('--include', dest='includes', type=str, nargs='+', metavar='PATH',
        default=[''], help='Paths to include, if not specified it will sync everything in source path')
    parser.add_argument('--exclude', dest='excludes', type=str, nargs='*',
        default=[], help='paths to exclude', metavar='PATH')
    parser.add_argument('-v', '--verbosity', dest='verbosity', type=int,
        default=1, help='Verbosity level', metavar='N')

    options = vars(parser.parse_args())
    print(options)

    kwargs = dict(
            includes=options['includes'],
            excludes=options['excludes'],
            verbosity=options['verbosity'],
    )
    if options['source'].startswith('/'):
        source = FilesystemEndpoint(
            name='source',
            base_path=options['source'],
            cache_dir=options['cache_dir'],
            **kwargs,
        )
    else:
        source = S3Endpoint(
            base_url=options['source'],
            **kwargs,
        )
    source.update_key_data()
