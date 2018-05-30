import boto3
import botocore
from . import utils
from .endpoint import BaseEndpoint


class S3Endpoint(BaseEndpoint):
    def __init__(self, base_url=None, base_path='', includes=[], excludes=[], **kwargs):
        self.profile, self.bucket_name, self.base_path = utils.parse_s3_url(
            base_url)
        super().__init__(log_prefix=self.profile, **kwargs)
        profile = botocore.session.get_session().full_config['profiles'][self.profile]
        self.bucket = boto3.resource('s3', **profile).Bucket(self.bucket_name)
        self.includes = includes
        self.excludes = excludes
        self.etag = dict()
        self.key_data = dict()
        self.log_info('profile: "{}"  bucket: "{}"  path: "{}"'.format( self.profile, self.bucket, self.base_path))

    def update_key_data(self):
        for include in self.includes:
            prefix = '{}/{}'.format(self.base_path, include)
            for obj in self.bucket.objects.filter(Prefix=prefix):
                print(obj.meta.data)
