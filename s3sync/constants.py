import pathlib

DEFAULT_SOURCE = pathlib.Path.home().as_posix()
DEFAULT_CACHE_DIR = pathlib.Path.home().as_posix()
HASHED_BYTES_THRESHOLD = 1024 * 1024 * 100
