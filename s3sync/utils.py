import hashlib
import queue
from operator import itemgetter

CHUNK_SIZE = 8 * 1024 * 1024


def get_etag(filename, chunk_size=CHUNK_SIZE):
    md5s = []
    with open(filename, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    if len(md5s) == 0:
        return hashlib.md5(b"").hexdigest()
    if len(md5s) == 1:
        return md5s[0].hexdigest()

    digests = b"".join(m.digest() for m in md5s)
    md5_hash = hashlib.md5(digests)
    return '{}-{}'.format(md5_hash.hexdigest(), len(md5s))


def parse_s3_url(url):
    profile_name, bucket_path = url.split(':', 1)
    parts = bucket_path.split('/', 1)
    if len(parts) == 1:
        bucket_name = parts[0]
        base_path = ''
    else:
        bucket_name = parts[0]
        base_path = parts[1].rstrip('/')
    return profile_name, bucket_name, base_path


def get_operations(source, destination):
    source_keys = set(source.keys())
    destination_keys = set(destination.keys())
    intersection = source_keys & destination_keys
    changed = [k for k in intersection if not source[k] == destination[k]]
    transfer = sorted(list(source_keys - destination_keys) + changed)
    delete = sorted(list(destination_keys - source_keys))
    return dict(transfer=transfer, delete=delete)


def get_queue_events(events_queue):
    events = []
    while True:
        try:
            event = events_queue.get(block=False)
        except queue.Empty:
            event = None
        if event is None:
            break
        events.append(event)
        events_queue.task_done()
    # keep only last event type for the same key
    event_dict = dict()
    for event in events:
        event_dict[event['key']] = event['type']
    events = [dict(key=key, type=event_type) for key, event_type in event_dict.items()]
    return sorted(events, key=itemgetter('key'))
