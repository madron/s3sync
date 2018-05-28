import os
from datetime import datetime
from operator import itemgetter


class FilesystemEndpoint(object):
    def get_file_list(self, path):
        file_list = []
        for prefix, directories, filenames in os.walk(path):
            for filename in filenames:
                file_path = os.path.join(prefix, filename)
                stat = os.stat(file_path)
                file_list.append(dict(
                    name=file_path,
                    size=stat.st_size,
                    last_modified=datetime.fromtimestamp(stat.st_mtime),
                ))
        return sorted(file_list, key=itemgetter('name'))
