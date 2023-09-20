"""S3 Store Class"""
from dol import Store

from s3dol.base import S3BucketDol, S3ClientDol
from s3dol.utility import S3DolException


def S3Store(bucket_name: str, *, make_bucket=False, path=None, **kwargs) -> Store:
    """S3 Bucket Store

    :param bucket_name: name of bucket to store data in
    :param make_bucket: create bucket if it does not exist
    :param path: prefix to use for bucket keys
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS session token
    :param endpoint_url: URL of S3 endpoint
    :param region_name: AWS region name
    :param profile_name: AWS profile name
    :return: S3BucketDol
    """

    validate_kwargs(kwargs)
    s3cr = S3ClientDol(**kwargs)
    validate_bucket(bucket_name, s3cr, make_bucket)
    return S3BucketDol(client=s3cr.client, bucket_name=bucket_name, prefix=path)


def validate_kwargs(kwargs):
    """Validate kwargs"""
    if kwargs.get('profile_name') is None and kwargs.get('aws_access_key_id') is None:
        raise S3DolException(
            'Either profile_name or aws_access_key_id must be specified'
        )


def validate_bucket(bucket_name: str, s3_client: S3ClientDol, make_bucket: bool):
    """Validate bucket name"""
    if make_bucket is True and bucket_name not in s3_client:
        s3_client[bucket_name] = {}
    return s3_client[bucket_name]
