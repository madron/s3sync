import os
import boto3
import botocore
from . import utils
from .endpoint import BaseEndpoint


class S3Endpoint(BaseEndpoint):
    def __init__(self, base_url=None, **kwargs):
        self.profile, self.bucket_name, self.base_path = utils.parse_s3_url( base_url)
        super().__init__(log_prefix=self.profile, **kwargs)
        self.bucket = None
        self.type = 's3'
        self.log_info('profile: "{}"  bucket: "{}"  path: "{}"'.format( self.profile, self.bucket_name, self.base_path))

    def get_bucket(self):
        if self.bucket:
            return self.bucket
        profile = botocore.session.get_session().full_config['profiles'][self.profile]
        self.bucket = boto3.resource('s3', **profile).Bucket(self.bucket_name)
        return self.bucket

    def update_key_data(self):
        bucket = self.get_bucket()
        self.key_data = dict()
        for include in self.includes:
            prefix = '{}/{}'.format(self.base_path, include)
            for obj in bucket.objects.filter(Prefix=prefix):
                data = obj.meta.data
                key = data['Key'].replace(prefix, '', 1)
                if not self.is_excluded(key):
                    self.key_data[key] = dict(
                        size=data['Size'],
                        etag=data['ETag'].strip('"'),
                    )
        self.update_etag()
        self.update_totals()
        self.log_info('Total files: {}'.format(self.total_files))
        self.log_info('Total bytes: {}'.format(self.total_bytes))

    def get_path(self, key):
        return os.path.join(self.base_path, key)

    def transfer(self, key, destination_endpoint, fake=True):
        source_path = self.get_path(key)
        if destination_endpoint.type == 'fs':
            if not fake:
                self.download(key, destination_endpoint.get_path(key))
        else:
            raise NotImplementedError()
        self.log_info(key, log_prefix='transfer')

    def upload(self, key, source_path):
        s3_path = self.get_path(key)
        try:
            self.get_bucket().Object(s3_path).upload_file(source_path)
        except Exception as e:
            self.log_error('"{}" {}'.format(key, e), log_prefix='transfer')

    def download(self, key, destination_path):
        s3_path = self.get_path(key)
        try:
            destination_dir = os.path.dirname(destination_path)
            if not os.path.isdir(destination_dir):
                os.makedirs(destination_dir)
            self.get_bucket().Object(s3_path).download_file(destination_path)
        except Exception as e:
            self.log_error('"{}" {}'.format(key, e), log_prefix='transfer')

    def delete(self, key, fake=True):
        if not fake:
            destination = '{}/{}'.format(self.base_path, key)
            try:
                self.get_bucket().Object(destination).delete()
            except Exception as e:
                self.log_error('"{}" {}'.format(key, e), log_prefix='delete')
                return
        self.log_info(key, log_prefix='delete')
