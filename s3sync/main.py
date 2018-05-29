import pathlib
from argparse import ArgumentParser
from .fs import FilesystemEndpoint

DEFAULT_SOURCE = DEFAULT_CACHE_DIR = pathlib.Path.home()


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

    options = vars(parser.parse_args())
    print(options)

    if options['source'].startswith('/'):
        source = FilesystemEndpoint(
            name='source',
            base_path=options['source'],
            includes=options['includes'],
            excludes=options['excludes'],
            cache_dir=options['cache_dir'],
        )
        source.update_key_data()
        total_size = sum([d['size'] for d in source.key_data.values()])
        print('files:', len(source.key_data))
        print('bytes:', total_size)
