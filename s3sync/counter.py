from .logger import Logger


class FileByteCounter(Logger):
    def __init__(self, name=None, verbosity=0):
        super().__init__(log_prefix=name, verbosity=verbosity)
        self.name = name
        self.files = 0
        self.bytes = 0

    def update(self, files_val, bytes_val):
        self.files = files_val
        self.bytes = bytes_val

    def log_totals(self):
        self.log_info('Total files: {}'.format(self.files))
        self.log_info('Total bytes: {}'.format(self.bytes))
