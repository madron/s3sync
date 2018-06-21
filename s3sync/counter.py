from prometheus_client import Gauge
from . import metrics
from .logger import Logger


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
            metrics.source_files.set(self.files)
            metrics.source_bytes.set(self.bytes)
        elif self.name == 'destination':
            metrics.destination_files.set(self.files)
            metrics.destination_bytes.set(self.bytes)
        elif self.name == 'queue':
            metrics.queue_files.set(self.files)
            metrics.queue_bytes.set(self.bytes)
        elif self.name == 'transferred':
            metrics.transferred_files.set(self.files)
            metrics.transferred_bytes.set(self.bytes)
        if not (self.files == self.prev_files and self.prev_bytes == self.prev_bytes):
            if self.name == 'queue':
                self.log_info('Files: {}'.format(self.files))
                self.log_info('Bytes: {}'.format(self.bytes))
            else:
                self.log_debug('Files: {}'.format(self.files))
                self.log_debug('Bytes: {}'.format(self.bytes))

    def log_totals(self):
        self.log_info('Files: {}'.format(self.files))
        self.log_info('Bytes: {}'.format(self.bytes))
