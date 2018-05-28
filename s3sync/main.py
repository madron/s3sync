import logging
import hashlib
import sys
import boto3
from . import utils


# LOCAL_FILENAME = '/tmp/big'
LOCAL_FILENAME = '/etc/hosts'

endpoints = dict(
    # s3=boto3.resource('s3',
    #                   aws_access_key_id='AKIAJMMRXDBKJIAH6KIA',
    #                   aws_secret_access_key='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',
    #                   region_name='us-east-1',
    #                   ),
    minio=boto3.resource('s3',
                         endpoint_url='http://localhost',
                         aws_access_key_id='admin',
                         aws_secret_access_key='password',
                         region_name='us-east-1',
                         ),
)


def main():
    print(utils.get_etag(LOCAL_FILENAME))

    for key, endpoint in endpoints.items():
        bucket = endpoint.Bucket('mastercom-files')
        # print(key, bucket)
        result = bucket.upload_file(LOCAL_FILENAME, 'mastercom-demo1/fabfile1.py')
        # bucket.upload_file(LOCAL_FILENAME, 'mastercom-demo1/fabfile2.py')

        for obj in bucket.objects.filter(Prefix='mastercom-demo1'):
            print(obj.meta.data)

