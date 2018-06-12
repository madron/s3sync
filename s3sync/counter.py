from prometheus_client import Gauge
from .logger import Logger


source_files = Gauge('s3sync_source_files', 'Number of source files')
source_bytes = Gauge('s3sync_source_bytes', 'Size of source files in bytes')
destination_files = Gauge('s3sync_destination_files', 'Number of destination files')
destination_bytes = Gauge('s3sync_destination_bytes', 'Size of destination files in bytes')
queue_files = Gauge('s3sync_queue_files', 'Number of queued files')
queue_bytes = Gauge('s3sync_queue_bytes', 'Size of queued files in bytes')
transferred_files = Gauge('s3sync_transferred_files', 'Number of transferred files')
transferred_bytes = Gauge('s3sync_transferred_bytes', 'Size of transferred files in bytes')


class FileByteCounter(Logger):
    def __init__(self, name=None, verbosity=0):
        super().__init__(log_prefix=name, verbosity=verbosity)
        self.name = name
        self.prev_files = 0
        self.prev_bytes = 0
        self.files = 0
        self.bytes = 0

    def set(self, files_val, bytes_val):
        self.prev_files = self.files
        self.prev_bytes = self.bytes
        self.files = files_val
        self.bytes = bytes_val
        self.update_metrics()

    def add(self, files_val, bytes_val):
        self.prev_files = self.files
        self.prev_bytes = self.bytes
        self.files += files_val
        self.bytes += bytes_val
        self.update_metrics()

    def update_metrics(self):
        if self.name == 'source':
            source_files.set(self.files)
            source_bytes.set(self.bytes)
        elif self.name == 'destination':
            destination_files.set(self.files)
            destination_bytes.set(self.bytes)
        elif self.name == 'queue':
            queue_files.set(self.files)
            queue_bytes.set(self.bytes)
        elif self.name == 'transferred':
            transferred_files.set(self.files)
            transferred_bytes.set(self.bytes)
        if not (self.files == self.prev_files and self.prev_bytes == self.prev_bytes):
            self.log_debug('Files: {}'.format(self.files))
            self.log_debug('Bytes: {}'.format(self.bytes))

    def log_totals(self):
        self.log_info('Files: {}'.format(self.files))
        self.log_info('Bytes: {}'.format(self.bytes))
