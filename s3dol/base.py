"""S3Dol with a dict-like interface for AWS S3 buckets.
Supports relative paths to access files under a prefix ending with slash.
Hints added to help setup credentials.

bucket = S3Dol()[profile][bucket_name]
bucket['level1/level2/test-key'] == bucket['level1/']['level2/']['test-key']
"""

from dataclasses import dataclass
import functools
import os
from typing import Iterable, Mapping, Union

from botocore.exceptions import ClientError
import boto3
import dol

from s3dol.utility import KeyNotValidError, Resp, S3DolException, S3KeyError


noCredentialsFound = S3DolException(
    "No AWS credentials found. Configure your AWS credentials as environment variables or in ~/.aws/credentials. "
    "See https://github.com/i2mint/s3dol/#set-up-credentials for more information."
)


def get_aws_credentials():
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_DEFAULT_REGION")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

    return {
        "aws_access_key_id": aws_access_key,
        "aws_secret_access_key": aws_secret_key,
        "region_name": aws_region,
        "aws_session_token": aws_session_token,
    }


def list_profile_names():
    """
    Return a list of available AWS profiles
    """
    session = boto3.session.Session()
    available_profiles = session.available_profiles
    # Check environment variables for default profile
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        available_profiles.append("environment variables")

    return available_profiles


def get_client(profile_name=None, endpoint_url=None, **session_kwargs):
    """
    Return a boto3 client for the specified profile
    """
    if profile_name is None:
        return _find_default_credentials(endpoint_url=endpoint_url, **session_kwargs)

    if profile_name == "environment variables":
        aws_credentials = get_aws_credentials()
        if (
            not aws_credentials["aws_access_key_id"]
            or not aws_credentials["aws_secret_access_key"]
        ):
            raise S3DolException(
                "Missing AWS credentials in environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
            )
        _skw = {**session_kwargs, **aws_credentials}
        session = boto3.Session(**_skw)
    else:
        # Get credentials from profile
        session = boto3.Session(profile_name=profile_name, **session_kwargs)
    client = session.client("s3", endpoint_url=endpoint_url)
    return client


def _find_default_credentials(endpoint_url=None, **session_kwargs):
    try:
        return get_client("environment variables", **session_kwargs)
    except S3DolException:
        pass
    session = boto3.Session(**session_kwargs)
    if session.get_credentials() is None:
        raise noCredentialsFound
    client = session.client("s3", endpoint_url=endpoint_url)
    return client


@dataclass
class BaseS3BucketReader(dol.base.KvReader):
    """Dict-like interface for AWS S3 buckets"""

    client: boto3.client
    bucket_name: str
    prefix: str = None
    delimiter: str = "/"

    def __post_init__(self):
        self.prefix = (
            f"{self.prefix.strip(self.delimiter)}{self.delimiter}"
            if self.prefix
            else ""
        )

    def object_list_pages(self) -> Iterable[dict]:
        if self._bucket_exists():
            yield from self.client.get_paginator("list_objects").paginate(
                Bucket=self.bucket_name, Prefix=self.prefix
            )

    def __iter__(self) -> Iterable[str]:
        for resp in self.object_list_pages():
            Resp.ascertain_200_status_code(resp)
            yield from filter(
                lambda k: not k.endswith(self.delimiter),
                map(Resp.key, Resp.contents(resp)),
            )

    def __getitem__(self, k: str):
        try:
            return self.client.get_object(Bucket=self.bucket_name, Key=k)["Body"].read()
        except self.client.exceptions.NoSuchKey as ex:
            raise KeyError(f"Key {k} does not exist") from ex

    def __contains__(self, k) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=k)
            return True  # if all went well
        except KeyNotValidError as e:
            raise
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise

    def _bucket_exists(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False


class BaseS3BucketDol(BaseS3BucketReader, dol.base.KvPersister):
    def __setitem__(self, k, v):
        if not self._bucket_exists():
            # create_bucket will not be silent if permission to create bucket is denied
            self.client.create_bucket(Bucket=self.bucket_name)
        self.client.put_object(Bucket=self.bucket_name, Key=k, Body=v)

    def __delitem__(self, k):
        self.client.delete_object(Bucket=self.bucket_name, Key=k)


# TODO: Use wrap_kvs and filt_iter from dol to handle key and id conversions
@dataclass
class S3BucketReader(BaseS3BucketReader):
    """
    Enables path delimitations to access objects under a prefix ending with slash such as 'level1/level2/'.

    s3_bucket['level1/']['level2/']['test-key'] == s3_bucket['level1/level2/test-key']
    """

    def _id_of_key(self, k):
        return f"{self.prefix}{k}"

    def _key_of_id(self, id):
        return id[len(self.prefix) :]

    def __iter__(self):
        return (
            self._key_of_id(_id)
            for _id in super().__iter__()
            if _id.startswith(self.prefix)
        )

    def __getitem__(self, k: str):
        _id = self._id_of_key(k)
        if _id.endswith(self.delimiter):
            _kw = {**self.__dict__, "prefix": _id}
            return type(self)(**_kw)
        return super().__getitem__(_id)

    def __contains__(self, k) -> bool:
        _id = self._id_of_key(k)
        return super().__contains__(_id)


class S3BucketDol(S3BucketReader, BaseS3BucketDol):
    def __setitem__(self, k, v):
        _id = self._id_of_key(k)
        return super().__setitem__(_id, v)

    def __delitem__(self, k):
        _id = self._id_of_key(k)
        return super().__delitem__(_id)


class S3ClientReader(dol.base.KvReader):
    ignore_404_when_endpoint_has_substring = ".supabase."

    def __init__(
        self, *, s3_bucket_dol=S3BucketDol, profile_name=None, **session_kwargs
    ):
        self.client = get_client(profile_name=profile_name, **session_kwargs)
        self.s3_bucket_dol = s3_bucket_dol

    def __iter__(self):
        return (b["Name"] for b in self.client.list_buckets().get("Buckets", []))

    def __contains__(self, k):
        try:
            self.client.head_bucket(Bucket=k)
            return True
        except Exception as e:
            # Check for a 400 error code in the response attributes
            if (
                hasattr(e, "response")
                and e.response.get("Error", {}).get("Code") == "400"
            ):
                # Use the endpoint URL to decide if it's Supabase
                endpoint = getattr(self.client.meta, "endpoint_url", "")
                if "supabase" in endpoint:
                    return True
            elif "404" in str(e):
                return False
            raise S3DolException(f"Error checking bucket existence: {e}") from e

    def __getitem__(self, k: str):
        # if k not in self:
        #     raise S3KeyError(f'Bucket {k} does not exist')

        return self.s3_bucket_dol(client=self.client, bucket_name=k)


class S3ClientDol(S3ClientReader, dol.base.KvPersister):
    def __init__(
        self, *, s3_bucket_dol=S3BucketDol, profile_name=None, **session_kwargs
    ):
        super().__init__(
            s3_bucket_dol=s3_bucket_dol, profile_name=profile_name, **session_kwargs
        )

    def __setitem__(self, k: str, v: Mapping):
        """Populate a bucket with a mapping of keys to objects. Create bucket if it does not exist.

        :param k: bucket name
        :type k: str
        :param v: mapping of keys to objects to populate bucket with
        :type v: Mapping
        """
        if not isinstance(v, Mapping):
            raise TypeError(
                f"Value must be a mapping (dict-like) object. Got {type(v)}"
            )
        self.client.create_bucket(Bucket=k)
        bucket = self[k]
        for bucket_item_key, bucket_item_value in v.items():
            bucket[bucket_item_key] = bucket_item_value

    def __delitem__(self, k):
        res = self.client.list_objects_v2(Bucket=k)
        if "Contents" in res:
            objects = [{"Key": obj["Key"]} for obj in res["Contents"]]
            self.client.delete_objects(Bucket=k, Delete={"Objects": objects})
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
