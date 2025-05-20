import os
import pytest

from s3dol.base import S3BucketDol, S3ClientReader, S3Dol, S3ClientDol, S3DolReadOnly


def overwrite_aws_environment_variables(
    aws_access_key_id, aws_secret_access_key, endpoint_url
):
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
    os.environ["AWS_ENDPOINT_URL"] = endpoint_url


@pytest.mark.parametrize(
    "aws_access_key_id, aws_secret_access_key, endpoint_url",
    [("localstack", "localstack", "http://localhost:4566")],
)
def test_s3_dol_crud(aws_access_key_id, aws_secret_access_key, endpoint_url):
    overwrite_aws_environment_variables(
        aws_access_key_id, aws_secret_access_key, endpoint_url
    )

    test_bucket_name = "test-bucket"
    test_key = "test-key"
    test_value = b"test-value"

    s3 = S3Dol()
    assert "environment variables" in s3
    profile_names = list(s3)
    assert "environment variables" in profile_names

    # create client from profile
    s3_client = s3["environment variables"]
    # create bucket
    if test_bucket_name in s3_client:
        del s3_client[test_bucket_name]
        assert test_bucket_name not in s3_client

    s3_client[test_bucket_name] = {}
    s3_bucket = s3_client[test_bucket_name]
    assert test_bucket_name in s3_client
    bucket_names = list(s3_client)
    assert test_bucket_name in bucket_names
    n_obj = len(list(s3_bucket))

    # create and delete object
    assert test_key not in s3_bucket
    s3_bucket[test_key] = test_value
    assert test_key in s3_bucket
    assert test_key in list(s3_bucket)
    assert s3_bucket[test_key] == test_value
    assert s3["environment variables"][test_bucket_name][test_key] == test_value
    assert len(list(s3_bucket)) == n_obj + 1
    del s3_bucket[test_key]
    assert test_key not in s3_bucket
    assert list(s3_bucket) == []
    assert len(list(s3_bucket)) == n_obj

    # delete bucket
    del s3_client[test_bucket_name]
    assert test_bucket_name not in s3_client
    assert test_bucket_name not in list(s3_client)

    # delete bucket with object
    s3_client[test_bucket_name] = {}
    s3_bucket = s3_client[test_bucket_name]
    s3_bucket[test_key] = test_value
    del s3_client[test_bucket_name]
    assert test_bucket_name not in s3_client

    # access bucket object with path delimiations
    s3_client[test_bucket_name] = {}
    s3_bucket = s3_client[test_bucket_name]
    s3_bucket["level1/level2/test-key"] = test_value

    assert_bucket_key_value(s3_bucket, "level1/level2/test-key", test_value)
    assert_bucket_key_value(s3_bucket["level1/level2/"], "test-key", test_value)
    assert_bucket_key_value(s3_bucket["level1/"]["level2/"], "test-key", test_value)
    assert_bucket_key_value(s3_bucket["level1/"], "level2/test-key", test_value)

    # delete object with path delimiations
    del s3_bucket["level1/"]["level2/"]["test-key"]
    assert "level1/level2/test-key" not in s3_bucket


def assert_bucket_key_value(s3_bucket, key, value):
    assert key in s3_bucket, f"failed contains test with key: {key}"
    assert key in list(s3_bucket), f"failed iter test with key: {key}"
    assert s3_bucket[key] == value, f"failed get test with key: {key}"


def mk_s3_client_from_env(
    aws_access_key_id, aws_secret_access_key, endpoint_url, s3_client_class
):
    overwrite_aws_environment_variables(
        aws_access_key_id, aws_secret_access_key, endpoint_url
    )
    return s3_client_class()


def mk_s3_client_from_params(
    aws_access_key_id, aws_secret_access_key, endpoint_url, s3_client_class
):
    return s3_client_class(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )


@pytest.mark.parametrize(
    "aws_access_key_id, aws_secret_access_key, endpoint_url",
    [("localstack", "localstack", "http://localhost:4566")],
)
@pytest.mark.parametrize(
    "mk_s3_client", [mk_s3_client_from_env, mk_s3_client_from_params]
)
def test_s3_client(
    aws_access_key_id, aws_secret_access_key, endpoint_url, mk_s3_client
):
    test_bucket_name = "test-bucket"

    s3_client = mk_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        s3_client_class=S3ClientDol,
    )

    if test_bucket_name in s3_client:
        del s3_client[test_bucket_name]
        assert test_bucket_name not in s3_client

    s3_client[test_bucket_name] = {}
    s3_bucket = s3_client[test_bucket_name]
    assert isinstance(s3_bucket, S3BucketDol)

    s3_client_reader = mk_s3_client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        s3_client_class=S3ClientReader,
    )
    s3_bucket = s3_client_reader[test_bucket_name]
    assert isinstance(s3_bucket, S3BucketDol)

    if test_bucket_name in s3_client:
        del s3_client[test_bucket_name]
        assert test_bucket_name not in s3_client_reader


@pytest.mark.parametrize(
    "aws_access_key_id, aws_secret_access_key, endpoint_url",
    [("localstack", "localstack", "http://localhost:4566")],
)
def test_s3_dol_readonly(aws_access_key_id, aws_secret_access_key, endpoint_url):
    overwrite_aws_environment_variables(
        aws_access_key_id, aws_secret_access_key, endpoint_url
    )

    test_bucket_name = "test-bucket"
    test_key = "test-key"
    test_key2 = "test-key2"
    test_value = b"test-value"

    s3 = S3Dol()
    s3["environment variables"][test_bucket_name] = {
        test_key: test_value,
        test_key2: test_value,
    }

    s3_readonly = S3DolReadOnly()["environment variables"]
    assert test_bucket_name in s3_readonly
    assert test_key in s3_readonly[test_bucket_name]
    assert test_key2 in s3_readonly[test_bucket_name]
    assert s3_readonly[test_bucket_name][test_key] == test_value

    with pytest.raises(TypeError):
        s3_readonly[test_bucket_name][test_key] = test_value

    with pytest.raises(TypeError):
        del s3_readonly[test_bucket_name][test_key]

    del s3["environment variables"][test_bucket_name][test_key]
    assert test_key not in s3_readonly[test_bucket_name]


if __name__ == "__main__":
    pytest.main([__file__])
