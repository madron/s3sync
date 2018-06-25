from prometheus_client import Gauge
from . import metrics
from .logger import Logger


class FileByteCounter(Logger):
    def __init__(self, name=None, verbosity=0):
        super().__init__(log_prefix=name, verbosity=verbosity)
        self.name = name
        self.prev_files = 0
        self.prev_bytes = 0
        self.total_files = 0
        self.total_bytes = 0
        self.files = dict()
        self.bytes = dict()

    def set(self, files_val, bytes_val, include):
        self.prev_files = self.total_files
        self.prev_bytes = self.total_bytes
        self.files[include] = files_val
        self.bytes[include] = bytes_val
        self.update_totals()
        self.update_metrics(include)

    def add(self, files_val, bytes_val, include):
        self.prev_files = self.total_files
        self.prev_bytes = self.total_bytes
        self.files[include] = self.files.get(include, 0) + files_val
        self.bytes[include] = self.bytes.get(include, 0) + bytes_val
        self.update_totals()
        self.update_metrics(include)

    def update_totals(self):
        self.total_files = sum([x for x in self.files.values()])
        self.total_bytes = sum([x for x in self.bytes.values()])

    def update_metrics(self, include):
        if self.name == 'source':
            metrics.source_files.labels(include=include).set(self.files[include])
            metrics.source_bytes.labels(include=include).set(self.bytes[include])
            metrics.source_total_files.set(self.total_files)
            metrics.source_total_bytes.set(self.total_bytes)
        elif self.name == 'destination':
            metrics.destination_files.labels(include=include).set(self.files[include])
            metrics.destination_bytes.labels(include=include).set(self.bytes[include])
            metrics.destination_total_files.set(self.total_files)
            metrics.destination_total_bytes.set(self.total_bytes)
        elif self.name == 'queue':
            metrics.queue_files.labels(include=include).set(self.files[include])
            metrics.queue_bytes.labels(include=include).set(self.bytes[include])
            metrics.queue_total_files.set(self.total_files)
            metrics.queue_total_bytes.set(self.total_bytes)
        elif self.name == 'transferred':
            metrics.transferred_files.labels(include=include).set(self.files[include])
            metrics.transferred_bytes.labels(include=include).set(self.bytes[include])
            metrics.transferred_total_files.set(self.total_files)
            metrics.transferred_total_bytes.set(self.total_bytes)
        if not (self.total_files == self.prev_files and self.prev_bytes == self.prev_bytes):
            if self.name == 'queue':
                self.log_info('Files: {}'.format(self.total_files))
                self.log_info('Bytes: {}'.format(self.total_bytes))
            else:
                self.log_debug('Files: {}'.format(self.total_files))
                self.log_debug('Bytes: {}'.format(self.total_bytes))

    def log_totals(self):
        self.log_info('Files: {}'.format(self.total_files))
        self.log_info('Bytes: {}'.format(self.total_bytes))
