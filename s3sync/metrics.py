from prometheus_client import Gauge


source_files = Gauge('s3sync_source_files', 'Number of source files')
source_bytes = Gauge('s3sync_source_bytes', 'Size of source files in bytes')
destination_files = Gauge('s3sync_destination_files', 'Number of destination files')
destination_bytes = Gauge('s3sync_destination_bytes', 'Size of destination files in bytes')
queue_files = Gauge('s3sync_queue_files', 'Number of queued files')
queue_bytes = Gauge('s3sync_queue_bytes', 'Size of queued files in bytes')
transferred_files = Gauge('s3sync_transferred_files', 'Number of transferred files')
transferred_bytes = Gauge('s3sync_transferred_bytes', 'Size of transferred files in bytes')
