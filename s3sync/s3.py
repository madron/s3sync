import os
import boto3
import botocore
from . import utils
from .endpoint import BaseEndpoint


class S3Endpoint(BaseEndpoint):
    def __init__(self, base_url=None, **kwargs):
        self.profile_name, self.bucket_name, self.base_path = utils.parse_s3_url( base_url)
        super().__init__(log_prefix=self.profile_name, **kwargs)
        self.type = 's3'
        self.bucket = None

    def get_profile(self, env_first=False):
        env = dict()
        if self.profile_name == 'default':
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            region_name = os.getenv('REGION_NAME')
            endpoint_url = os.getenv('ENDPOINT_URL')
            if aws_access_key_id:
                env['aws_access_key_id'] = aws_access_key_id
            if aws_secret_access_key:
                env['aws_secret_access_key'] = aws_secret_access_key
            if region_name:
                env['region_name'] = region_name
            if endpoint_url:
                env['endpoint_url'] = endpoint_url
        config_profiles = botocore.session.get_session().full_config['profiles']
        cfg = config_profiles.get(self.profile_name, dict())
        first, second = (env, cfg) if env_first else (cfg, env)
        second.update(first)
        return second

    def get_bucket(self):
        if self.bucket:
            return self.bucket
        profile = self.get_profile()
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

    def transfer(self, key, destination_endpoint, fake=False):
        source_path = self.get_path(key)
        if destination_endpoint.type == 'fs':
            if not fake:
                self.download(key, destination_endpoint.get_destination_path(key))
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
            self.get_bucket().Object(s3_path).download_file(destination_path)
        except Exception as e:
            self.log_error('"{}" {}'.format(key, e), log_prefix='transfer')

    def delete(self, key, fake=False):
        if not fake:
            destination = '{}/{}'.format(self.base_path, key)
            try:
                self.get_bucket().Object(destination).delete()
            except Exception as e:
                self.log_error('"{}" {}'.format(key, e), log_prefix='delete')
                return
        self.log_info(key, log_prefix='delete')
