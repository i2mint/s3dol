"""S3 Store Class"""

from typing import Optional
from dol import Store

from s3dol.base import S3BucketDol, S3ClientDol
from s3dol.utility import S3DolException


def S3Store(
    bucket_name: str,
    *,
    make_bucket: Optional[bool] = None,
    path: Optional[str] = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_session_token: str = None,
    endpoint_url: str = None,
    region_name: str = None,
    profile_name: str = None,
) -> Store:
    """S3 Bucket Store

    :param bucket_name: name of bucket to store data in
    :param make_bucket: if True, create bucket if it does not exist.
                        If None, do nothing regarding bucket existence.
    :param path: prefix to use for bucket keys
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS session token
    :param endpoint_url: URL of S3 endpoint
    :param region_name: AWS region name
    :param profile_name: AWS profile name
    :return: S3BucketDol
    """

    s3cr = S3ClientDol(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        endpoint_url=endpoint_url,
        region_name=region_name,
        profile_name=profile_name,
    )
    validate_bucket(bucket_name, s3cr, make_bucket)
    return S3BucketDol(client=s3cr.client, bucket_name=bucket_name, prefix=path)


def validate_bucket(bucket_name: str, s3_client: S3ClientDol, make_bucket: bool):
    """Validate bucket name"""
    if make_bucket is True and bucket_name not in s3_client:
        s3_client[bucket_name] = {}
    return s3_client[bucket_name]
