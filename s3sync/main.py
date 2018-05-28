from . import utils

LOCAL_FILENAME = '/etc/hosts'


def main():
    print(utils.get_etag(LOCAL_FILENAME))
