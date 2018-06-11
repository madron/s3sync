from prometheus_client import Gauge
from .logger import Logger


source_files = Gauge('s3sync_source_files', 'Number of source files')
source_bytes = Gauge('s3sync_source_bytes', 'Size of source files in bytes')
destination_files = Gauge('s3sync_destination_files', 'Number of destination files')
destination_bytes = Gauge('s3sync_destination_bytes', 'Size of destination files in bytes')


class FileByteCounter(Logger):
    def __init__(self, name=None, verbosity=0):
        super().__init__(log_prefix=name, verbosity=verbosity)
        self.name = name
        self.files = 0
        self.bytes = 0

    def update(self, files_val, bytes_val):
        self.files = files_val
        self.bytes = bytes_val
        self.update_metrics()

    def update_metrics(self):
        if self.name == 'source':
            source_files.set(self.files)
            source_bytes.set(self.bytes)
        if self.name == 'destination':
            destination_files.set(self.files)
            destination_bytes.set(self.bytes)

    def log_totals(self):
        self.log_info('Total files: {}'.format(self.files))
        self.log_info('Total bytes: {}'.format(self.bytes))
