import os
from s3dol.base import S3Dol
import pytest


@pytest.mark.parametrize(
    'aws_access_key_id, aws_secret_access_key, endpoint_url',
    [('localstack', 'localstack', 'http://localhost:4566')],
)
def test_s3_store_crud(aws_access_key_id, aws_secret_access_key, endpoint_url):
    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    os.environ['AWS_ENDPOINT_URL'] = endpoint_url

    bucket_name = 'test-bucket'
    key = 'test-key'
    value = b'test-value'

    s3 = S3Dol()
    assert 'environment variables' in s3
    s3_client = s3['environment variables']

    if bucket_name in s3_client:
        del s3_client[bucket_name]
        assert bucket_name not in s3_client
    s3_bucket = s3_client[bucket_name]
    assert bucket_name in s3_client
    assert bucket_name in list(s3_client)
    n_obj = len(list(s3_bucket))

    assert key not in s3_bucket
    s3_bucket[key] = value
    assert key in s3_bucket
    assert key in list(s3_bucket)
    assert s3_bucket[key] == value
    assert len(list(s3_bucket)) == n_obj + 1
    del s3_bucket[key]
    assert key not in s3_bucket
    assert list(s3_bucket) == []
    assert len(list(s3_bucket)) == n_obj

    del s3_client[bucket_name]
    assert bucket_name not in s3_client
    assert bucket_name not in list(s3_client)


if __name__ == '__main__':
    test_s3_store_crud(
        aws_access_key_id='localstack',
        aws_secret_access_key='localstack',
        endpoint_url='http://localhost:4566',
    )
