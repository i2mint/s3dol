import configparser
import functools
import os
from typing import Union
import warnings

import boto3
import dol


class S3DolException(Exception):
    pass


noCredentialsFound = S3DolException(
    'No AWS credentials found. Configure your AWS credentials as environment variables or in ~/.aws/credentials. '
    'See https://github.com/i2mint/s3dol/#set-up-credentials for more information.'
)


def get_aws_credentials():
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION')
    aws_session_token = os.environ.get('AWS_SESSION_TOKEN')

    return {
        'aws_access_key_id': aws_access_key,
        'aws_secret_access_key': aws_secret_key,
        'region_name': aws_region,
        'aws_session_token': aws_session_token,
    }


def list_profile_names():
    """
    Return a list of available AWS profiles
    """
    session = boto3.session.Session()
    available_profiles = session.available_profiles
    # Check environment variables for default profile
    if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
        available_profiles.append('environment variables')

    return available_profiles


def get_client(profile_name=None, endpoint_url=None, **session_kwargs):
    """
    Return a boto3 client for the specified profile
    """
    if profile_name is None:
        return _find_default_credentials(endpoint_url=endpoint_url, **session_kwargs)

    if profile_name == 'environment variables':
        aws_credentials = get_aws_credentials()
        if (
            not aws_credentials['aws_access_key_id']
            or not aws_credentials['aws_secret_access_key']
        ):
            raise S3DolException(
                'Missing AWS credentials in environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY'
            )
        _skw = {**session_kwargs, **aws_credentials}
        session = boto3.Session(**_skw)
    else:
        # Get credentials from profile
        session = boto3.Session(profile_name=profile_name, **session_kwargs)
    client = session.client('s3', endpoint_url=endpoint_url)
    return client


def _find_default_credentials(endpoint_url=None, **session_kwargs):
    try:
        return get_client('environment variables', **session_kwargs)
    except S3DolException:
        pass
    session = boto3.Session(**session_kwargs)
    if session.get_credentials() is None:
        raise noCredentialsFound
    client = session.client('s3', endpoint_url=endpoint_url)
    return client


class S3BucketReader(dol.base.KvReader):
    def __init__(self, *, client, bucket_name):
        self.client = client
        self.bucket_name = bucket_name

    def __iter__(self):
        _obj_list = self.client.list_objects(Bucket=self.bucket_name)
        return (o['Key'] for o in _obj_list.get('Contents', []))

    def __getitem__(self, k: str):
        return self.client.get_object(Bucket=self.bucket_name, Key=k)['Body'].read()


class S3BucketDol(S3BucketReader, dol.base.KvPersister):
    def __setitem__(self, k, v):
        self.client.put_object(Bucket=self.bucket_name, Key=k, Body=v)

    def __delitem__(self, k):
        self.client.delete_object(Bucket=self.bucket_name, Key=k)


class S3ClientReader(dol.base.KvReader):
    def __init__(
        self, *, s3_bucket_dol=S3BucketDol, profile_name=None, **session_kwargs
    ):
        self.client = get_client(profile_name=profile_name, **session_kwargs)
        self.s3_bucket_dol = s3_bucket_dol

    def __iter__(self):
        return (b['Name'] for b in self.client.list_buckets().get('Buckets', []))

    def __getitem__(self, k: str):
        return self.s3_bucket_dol(client=self.client, bucket_name=k)


class S3ClientDol(S3ClientReader, dol.base.KvPersister):
    def __init__(
        self, *, s3_bucket_dol=S3BucketDol, profile_name=None, **session_kwargs
    ):
        super().__init__(
            s3_bucket_dol=s3_bucket_dol, profile_name=profile_name, **session_kwargs
        )

    def __getitem__(self, k: str):
        """Get bucket. If bucket does not exist, create it.

        :param k: bucket name
        :type k: str
        :return: S3 bucket dol
        """
        self.client.create_bucket(Bucket=k)
        return super().__getitem__(k)

    def __setitem__(self, k, v=None):
        """Only creates bucket. Value is ignored.
        Buckets are storage containers for objects. It is not possible to store a value directly as a bucket.

        Prefer s3[bucket][key]=value and the bucket will be created automatically if it does not exist.

        :param k: bucket name
        :type k: str
        :param v: None
        :type v: None
        """
        self.client.create_bucket(Bucket=k)
        if v is not None:
            warnings.warn(
                'Bucket created successfully. Value is not set. Try s3[bucket][key]=value.',
                category=UserWarning,
            )

    def __delitem__(self, k):
        self.client.delete_bucket(Bucket=k)


class S3Dol(dol.base.KvReader):
    """S3 profiles -> buckets -> keys -> objects"""

    def __init__(self, s3_client_dol=S3ClientDol, s3_bucket_dol=S3BucketDol):
        if len(list_profile_names()) == 0:
            raise noCredentialsFound
        self.s3_client_dol = s3_client_dol
        self.s3_bucket_dol = s3_bucket_dol

    def __iter__(self):
        return iter(list_profile_names())

    def __getitem__(self, k: Union[str, dict]):
        if isinstance(k, str):
            return self.s3_client_dol(profile_name=k, s3_bucket_dol=self.s3_bucket_dol)
        return self.s3_client_dol(s3_bucket_dol=self.s3_bucket_dol, **k)


S3DolReadOnly = functools.partial(
    S3Dol, s3_client_dol=S3ClientReader, s3_bucket_dol=S3BucketReader
)
