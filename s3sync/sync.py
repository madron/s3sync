import time
from datetime import datetime
from datetime import timedelta
from queue import Queue
from .import metrics
from .import utils
from .counter import FileByteCounter
from .fs import FSEndpoint
from .logger import Logger
from .s3 import S3Endpoint


class SyncManager(Logger):
    def __init__(self, fake=False, verbosity=0, **kwargs):
        super().__init__(log_prefix='SyncManager', verbosity=verbosity)
        self.fake = fake
        restore = kwargs.get('restore', False)
        source = kwargs['destination'] if restore else kwargs['source']
        destination = kwargs['source'] if restore else kwargs['destination']
        self.source = self.get_endpoint(source, 'source', **kwargs)
        self.destination = self.get_endpoint(destination, 'destination', **kwargs)
        self.operations = dict(transfer=[], delete=[])
        self.rescan_delay = kwargs.get('rescan_delay', 0)
        self.rescan_time = None
        self.source_queue = Queue()
        self.destination_queue = Queue()
        self.queue_counter = FileByteCounter(name='queue', verbosity=verbosity)
        self.transferred_counter = FileByteCounter(name='transferred', verbosity=verbosity)

    def get_endpoint(self, path, name, **kwargs):
        keys = ['includes', 'excludes', 'verbosity']
        options = {k: kwargs[k] for k in keys if k in kwargs}
        options['verbosity'] = self.verbosity
        options['name'] = name
        if path.startswith('/'):
            options['base_path'] = path
            options['cache_dir'] = kwargs.get('cache_dir')
            options['cache_file'] = kwargs.get('cache_file')
            return FSEndpoint(**options)
        else:
            options['base_url'] = path
            return S3Endpoint(**options)

    def get_operations(self):
        return utils.get_operations(
            self.source.etag,
            self.destination.etag,
        )

    def sync(self, rescan=False):
        finished = False
        while not finished:
            try:
                self.source.update_key_data()
                finished = True
            except Exception as e:
                self.log_error(e)
                time.sleep(5)
        finished = False
        while not finished:
            try:
                self.destination.update_key_data()
                finished = True
            except Exception as e:
                self.log_error(e)
                time.sleep(5)
        self.rescan_time = datetime.now()
        operations = self.get_operations()
        if rescan:
            total_files = len(operations['delete']) + len(operations['transfer'])
            metrics.rescan_files.set(total_files)
        self.sync_operations(operations)
        self.source.write_cache()
        self.destination.write_cache()

    def sync_operations(self, operations):
        # Update source key data
        for key in operations['delete']:
            self.source.update_single_key_data(key)
        for key in operations['transfer']:
            self.source.update_single_key_data(key)
        # Update queue counter
        total_files = len(operations['delete']) + len(operations['transfer'])
        total_bytes = 0
        for key in operations['transfer']:
            total_bytes += self.source.key_data[key]['size']
        self.queue_counter.set(total_files, total_bytes)
        self.operations = operations
        finished = False
        while not finished:
            try:
                self.execute_operations()
                finished = True
            except Exception as e:
                self.log_error(e)
                time.sleep(5)

    def execute_operations(self):
        #  Operations
        while len(self.operations['delete']) > 0:
            key = self.operations['delete'][0]
            self.destination.delete(key, fake=self.fake)
            self.operations['delete'].pop(0)
            self.queue_counter.add(-1, 0)
            self.transferred_counter.add(1, 0)
            self.destination.update_single_key_data(key)
        while len(self.operations['transfer']) > 0:
            key = self.operations['transfer'][0]
            self.transfer(key, fake=self.fake)
            self.operations['transfer'].pop(0)
            size = self.source.key_data[key]['size']
            self.queue_counter.add(-1, -size)
            self.transferred_counter.add(1, size)
            self.destination.update_single_key_data(key)

    def get_events_operations(self):
        transfer = []
        delete = []
        for event in utils.get_queue_events(self.source_queue):
            if event['type'] == 'modified':
                transfer.append(event['key'])
            if event['type'] == 'deleted':
                delete.append(event['key'])
        transfer = list(set(transfer))
        delete = list(set(delete))
        for event in utils.get_queue_events(self.destination_queue):
            key = event['key']
            if not key in transfer and not key in delete:
                transfer.append(key)
        return dict(transfer=sorted(transfer), delete=sorted(delete))

    def transfer(self, key, fake=False):
        if not fake:
            if self.source.type == 'fs' and self.destination.type == 'fs':
                self.source.copy(key, self.destination)
            elif self.source.type == 'fs' and self.destination.type == 's3':
                source_path = self.source.get_path(key)
                self.destination.upload(key, source_path)
            elif self.source.type == 's3' and self.destination.type == 'fs':
                destination_path = self.destination.get_destination_path(key)
                self.source.download(key, destination_path)
            else:
                raise NotImplementedError()
        self.log_info(key, log_prefix='transfer')

    def watch(self):
        self.source.observer_start(self.source_queue)
        try:
            while True:
                if self.rescan_delay:
                    delay = timedelta(seconds=self.rescan_delay)
                    if datetime.now() > self.rescan_time + delay:
                        self.sync()
                time.sleep(1)
                operations = self.get_events_operations()
                self.sync_operations(operations)
        except KeyboardInterrupt:
            pass
        self.source.observer_stop()
