import hashlib

CHUNK_SIZE = 8 * 1024 * 1024


def get_etag(filename, chunk_size=CHUNK_SIZE):
    md5s = []
    with open(filename, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    if len(md5s) == 1:
        return '"{}"'.format(md5s[0].hexdigest())

    digests = b"".join(m.digest() for m in md5s)
    md5_hash = hashlib.md5(digests)
    return '"{}-{}"'.format(md5_hash.hexdigest(), len(md5s))
