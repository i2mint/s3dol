import pytest
from s3dol.base import S3ClientDol

from s3dol.store import S3Store
from s3dol.tests.test_base import assert_bucket_key_value
from s3dol.utility import S3DolException


def setup_test_bucket(aws_access_key_id, aws_secret_access_key, endpoint_url):
    s3_client = S3ClientDol(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )
    if "test-bucket" not in s3_client:
        s3_client["test-bucket"] = {}

    if "level1/level2/test-key" in s3_client["test-bucket"]:
        del s3_client["test-bucket"]["level1/level2/test-key"]


@pytest.mark.parametrize(
    "aws_access_key_id, aws_secret_access_key, endpoint_url",
    [("localstack", "localstack", "http://localhost:4566")],
)
def test_s3_store_crud(aws_access_key_id, aws_secret_access_key, endpoint_url):
    setup_test_bucket(aws_access_key_id, aws_secret_access_key, endpoint_url)

    s3_store = S3Store(
        bucket_name="test-bucket",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )
    test_value = b"test-store-value"
    s3_store["level1/level2/test-key"] = test_value
    assert_bucket_key_value(s3_store, "level1/level2/test-key", test_value)
    assert_bucket_key_value(s3_store["level1/level2/"], "test-key", test_value)
    assert_bucket_key_value(s3_store["level1/"]["level2/"], "test-key", test_value)
    assert_bucket_key_value(s3_store["level1/"], "level2/test-key", test_value)

    del s3_store["level1/"]["level2/"]["test-key"]
    assert "level1/level2/test-key" not in s3_store


if __name__ == "__main__":
    pytest.main([__file__])
