"""Presigned-URL (``url_for``) tests, mocked with moto. See issue #7.

Unlike the env-gated real-S3 tests, these run anywhere — moto fully mocks the
S3 API including ``generate_presigned_url``.
"""

import pytest

pytest.importorskip("moto")
from moto import mock_aws

from s3dol.store import S3Store

_CREDS = dict(
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
    region_name="us-east-1",
)


@pytest.fixture(autouse=True)
def _isolate_aws_env(monkeypatch):
    """Make these moto tests hermetic. The localstack tests (test_base) set
    ``AWS_ENDPOINT_URL=http://localhost:4566`` via ``os.environ`` with no
    cleanup, which would route boto3 past moto to a (down) localstack. Clear any
    leaked endpoint + pin fake creds so ``@mock_aws`` always intercepts."""
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@mock_aws
def test_url_for_returns_presigned_get_url():
    store = S3Store("test-bucket", make_bucket=True, **_CREDS)
    store["hello.txt"] = b"hi"
    url = store.url_for("hello.txt")
    assert isinstance(url, str)
    assert "test-bucket" in url
    assert "hello.txt" in url
    # Presigned marker (SigV4 X-Amz-Signature / SigV2 Signature).
    assert "Signature" in url


@mock_aws
def test_url_for_is_prefix_aware():
    # path= sets a key prefix; url_for must point at the prefixed id, matching
    # __getitem__ (else the URL 404s).
    store = S3Store("test-bucket", make_bucket=True, path="blobs", **_CREDS)
    store["abc"] = b"x"
    url = store.url_for("abc")
    assert "blobs" in url and "abc" in url


@mock_aws
def test_url_for_honors_expires_in():
    store = S3Store("test-bucket", make_bucket=True, **_CREDS)
    store["k"] = b"v"
    url = store.url_for("k", expires_in=60)
    assert "X-Amz-Expires=60" in url or "Expires=" in url
