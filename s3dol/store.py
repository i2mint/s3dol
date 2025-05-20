"""S3 Store Class"""

from typing import Optional
from dol import Store

import boto3
from s3dol.base import S3BucketDol, S3ClientDol
from s3dol.utility import S3DolException


# TODO: Add support for key-based grouping (store['folder/subfolder/'] is the store with path = original_path + 'folder/subfolder/'
# TODO: Make profile_name handling work for SupabaseS3BucketDol and S3BucketDolWithouBucketCheck
# TODO: Review this S3Store (with SupabaseS3BucketDol etc.) setup and see if we can make it simpler/cleaner
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
    is_supabase_endpoint: Optional[bool] = None,
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

    if is_supabase_endpoint is None:
        is_supabase_endpoint = _is_supabase_endpoint(endpoint_url)
    if skip_bucket_check is None and is_supabase_endpoint:
        skip_bucket_check = True
    # For Supabase endpoints, create a direct client without session token

    params = dict(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        endpoint_url=endpoint_url,
        region_name=region_name,
    )

    if is_supabase_endpoint:
        # For Supabase endpoints, use a custom store that doesn't check for bucket existence
        return SupabaseS3BucketDol.from_params(
            **params,
            bucket_name=bucket_name,
            path=path,
        )
    elif skip_bucket_check:
        return S3BucketDolWithouBucketCheck.from_params(
            **params,
            bucket_name=bucket_name,
            prefix=path,
        )

    # For standard AWS, use the regular flow

    s3cr = S3ClientDol(
        **params,
        profile_name=profile_name,
    )

    bucket = s3cr[bucket_name]

    if make_bucket is None:
        bucket.skip_bucket_exists_check = True
    elif make_bucket is True and bucket_name not in s3cr:
        s3cr[bucket_name] = {}

    return bucket


def _is_supabase_endpoint(endpoint_url: str) -> bool:
    """Check if the endpoint URL is a Supabase endpoint"""
    return endpoint_url and '.supabase.' in endpoint_url


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
        _id = self._id_of_key(k)  # TODO: Smelly. use trans tools
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


# TODO: Use wrap_kvs?
class SupabaseS3BucketDol(S3BucketDolWithouBucketCheck):
    """
    S3BucketDol for Supabase endpoints.
    Over the S3BucketDolWithouBucketCheck, it adds support to unravel the returned
    content from the stuff (number of bytes, check sum, etc.) superbase adds to the
    content.
    """

    def __getitem__(self, k):
        _id = self._id_of_key(k)  # TODO: Smelly. use trans tools
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
