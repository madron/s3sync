import boto3
import botocore
from . import utils
from .endpoint import BaseEndpoint


class S3Endpoint(BaseEndpoint):
    def __init__(self, base_url=None, **kwargs):
        self.profile, self.bucket, self.base_path = utils.parse_s3_url( base_url)
        super().__init__(log_prefix=self.profile, **kwargs)
        self.log_info('profile: "{}"  bucket: "{}"  path: "{}"'.format( self.profile, self.bucket, self.base_path))

    def update_key_data(self):
        profile = botocore.session.get_session().full_config['profiles'][self.profile]
        bucket = boto3.resource('s3', **profile).Bucket(self.bucket)
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
