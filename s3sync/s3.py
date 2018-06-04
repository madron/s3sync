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
                key = data['Key'].lstrip('/')
                if not self.is_excluded(key):
                    self.key_data[key] = dict(
                        size=data['Size'],
                        etag=data['ETag'].strip('"'),
                    )
        self.update_etag()
        self.update_totals()
        self.log_info('Total files: {}'.format(self.total_files))
        self.log_info('Total bytes: {}'.format(self.total_bytes))

    def transfer_from(self, key, source_endpoint, fake=True):
        if source_endpoint.type == 'fs':
            source = os.path.join(source_endpoint.base_path, key)
            destination = os.path.join(self.base_path, key)
            if not fake:
                try:
                    self.get_bucket().Object(destination).upload_file(source)
                except Exception as e:
                    self.log_error('"{}" {}'.format(key, e), log_prefix='transfer')
        else:
            raise NotImplementedError()
        self.log_info(key, log_prefix='transfer')

    def delete(self, key, fake=True):
        if not fake:
            destination = '{}/{}'.format(self.base_path, key)
            try:
                self.get_bucket().Object(destination).delete()
            except Exception as e:
                self.log_error('"{}" {}'.format(key, e), log_prefix='delete')
                return
        self.log_info(key, log_prefix='delete')
