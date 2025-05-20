"""S3 Store Class"""

from typing import Optional
from dol import Store

import boto3
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
    skip_bucket_check: Optional[bool] = None,
) -> Store:
    """S3 Bucket Store

    :param bucket_name: name of bucket to store data in
    :param make_bucket: if True, create bucket if it does not exist.
                        If None, skip bucket existence checks completely.
                        If False, check for existence but don't create.
    :param path: prefix to use for bucket keys
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS session token
    :param endpoint_url: URL of S3 endpoint
    :param region_name: AWS region name
    :param profile_name: AWS profile name
    :return: S3BucketDol
    """
    if skip_bucket_check is None:
        if endpoint_url and '.supabase.' in endpoint_url:
            skip_bucket_check = True
    # For Supabase endpoints, create a direct client without session token
    if endpoint_url and '.supabase.' in endpoint_url:

        # Create a direct boto3 client without the session token
        client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

        return S3BucketDolWithouBucketCheck(
            client=client, bucket_name=bucket_name, prefix=path
        )

    # For standard AWS, use the regular flow

    s3cr = S3ClientDol(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        endpoint_url=endpoint_url,
        region_name=region_name,
        profile_name=profile_name,
    )

    bucket = s3cr[bucket_name]

    if make_bucket is None:
        bucket.skip_bucket_exists_check = True
    elif make_bucket is True and bucket_name not in s3cr:
        s3cr[bucket_name] = {}

    return bucket


def validate_bucket(
    bucket_name: str, s3_client: S3ClientDol, make_bucket: Optional[bool]
):
    """Validate bucket name and create if needed

    If make_bucket is None, skip existence check entirely.
    If make_bucket is True, create bucket if it doesn't exist.
    If make_bucket is False, check existence but don't create.
    """
    if make_bucket is None:
        # Skip validation - just return a bucket object without checking existence
        return s3_client[bucket_name]
    elif make_bucket is True and bucket_name not in s3_client:
        s3_client[bucket_name] = {}
    return s3_client[bucket_name]


class S3BucketDolWithouBucketCheck(S3BucketDol):
    """A S3BucketDol that completely avoids the bucket exists check.
    This is needed for Supabase endpoints where the bucket is not created
    until the first object is uploaded, for example.
    """

    def __setitem__(self, k, v):
        _id = self._id_of_key(k)
        self.client.put_object(Bucket=self.bucket_name, Key=_id, Body=v)

    def _bucket_exists(self):
        return True

    @classmethod
    def from_params(
        cls,
        bucket_name: str,
        *,
        path: Optional[str] = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        region_name: str = None,
        endpoint_url: str = None,
    ):
        """Create a S3BucketDol without session token"""
        client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )

        return cls(client=client, bucket_name=bucket_name, prefix=path)


class SupabaseS3Store(S3BucketDolWithouBucketCheck):
    def __getitem__(self, k):
        _id = self._id_of_key(k)
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=_id)
            raw_data = response['Body'].read()

            # Process HTTP chunked encoding directly on bytes
            # Look for the pattern: [chunk size in hex]\r\n[data]\r\n0\r\n[optional headers]\r\n\r\n
            if (
                raw_data.startswith(b'0')
                or raw_data[0:2].isdigit()
                or (
                    raw_data[0:1].isdigit()
                    and raw_data[1:2].isalpha()
                    and raw_data[1:2].lower() in b'abcdef'
                )
            ):
                # Find the first CRLF
                first_crlf_pos = raw_data.find(b'\r\n')
                if first_crlf_pos != -1:
                    # Extract what should be the hex chunk size
                    hex_size = raw_data[:first_crlf_pos]
                    try:
                        # Skip the chunk size and the CRLF
                        content_start = first_crlf_pos + 2
                        # Find the end of the chunk (marked by another CRLF)
                        content_end = raw_data.find(b'\r\n', content_start)
                        if content_end != -1:
                            # Extract just the content between the CRLFs
                            return raw_data[content_start:content_end]
                    except ValueError:
                        pass  # Not valid hex, continue to return raw data

            # If we couldn't parse it as chunked encoding or any step failed,
            # return the raw data as a fallback
            return raw_data

        except self.client.exceptions.NoSuchKey as ex:
            raise KeyError(f'Key {k} does not exist') from ex
