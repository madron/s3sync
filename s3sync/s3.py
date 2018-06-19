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

    def get_prefix(self, include):
        base_path = self.base_path.rstrip('/')
        include = include.lstrip('/')
        prefix = '{}/{}'.format(self.base_path, include).lstrip('/')
        return prefix

    def get_key(self, s3key):
        key = s3key.replace(self.base_path, '', 1).lstrip('/')
        return key

    def update_key_data(self):
        bucket = self.get_bucket()
        self.key_data = dict()
        for include in self.includes:
            prefix = self.get_prefix(include)
            for obj in bucket.objects.filter(Prefix=prefix):
                data = obj.meta.data
                key = self.get_key(data['Key'])
                if not self.is_excluded(key):
                    self.key_data[key] = dict(
                        size=data['Size'],
                        etag=data['ETag'].strip('"'),
                    )
        self.update_etag()
        self.update_totals()
        self.counter.log_totals()

    def update_single_key_data(self, key):
        bucket = self.get_bucket()
        try:
            base_path = self.base_path.rstrip('/')
            data = bucket.Object('{}/{}'.format(base_path, key)).get()
            etag = data['ETag'].strip('"')
            self.key_data[key] = dict(
                size=data['ContentLength'],
                etag=etag,
            )
            self.etag[key] = etag
        except bucket.meta.client.exceptions.NoSuchKey:
            if key in self.key_data: del self.key_data[key]
            if key in self.etag: del self.etag[key]
        self.update_totals()

    def get_path(self, key):
        return os.path.join(self.base_path, key)

    def upload(self, key, source_path):
        s3_path = self.get_path(key)
        try:
            self.get_bucket().Object(s3_path).upload_file(source_path)
        except:
            self.log_error('upload - key: {} - source_path: {}'.format(key, source_path))
            raise

    def download(self, key, destination_path):
        s3_path = self.get_path(key)
        self.get_bucket().Object(s3_path).download_file(destination_path)

    def delete(self, key, fake=False):
        if not fake:
            destination = '{}/{}'.format(self.base_path, key)
            self.get_bucket().Object(destination).delete()
        self.log_info(key, log_prefix='delete')
