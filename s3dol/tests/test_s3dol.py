from s3dol.new_s3 import get_s3_client, S3BinaryStore
from s3dol.tests.util import get_s3_test_access_info_from_env_vars

s3_access_info = get_s3_test_access_info_from_env_vars(perm='RW')
aws_access_key_id = s3_access_info['aws_access_key_id']
aws_secret_access_key = s3_access_info['aws_secret_access_key']
bucket_name = s3_access_info['bucket_name']
endpoint_url = s3_access_info['endpoint_url']


def ensure_bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:
        s3_client.create_bucket(Bucket=bucket_name)


def test_s3_store_crud():
    s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, endpoint_url)
    ensure_bucket_exists(s3_client, bucket_name)
    s3_store = S3BinaryStore(client=s3_client, bucket=bucket_name)
    key = 'test_key'
    if key in s3_store:
        del s3_store[key]
    assert key not in s3_store
    n = len(list(s3_store))
    s3_store[key] = b'test_value'
    assert key in s3_store
    assert n + 1 == len(list(s3_store))
    assert s3_store[key] == b'test_value'
    s3_store[key] = b'test_value2'
    assert s3_store[key] == b'test_value2'
    del s3_store[key]
    assert key not in s3_store
    assert n == len(list(s3_store))
