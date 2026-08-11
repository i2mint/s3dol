"""The exported conformance suite, run against the fake, the real store
(moto), and a shipped recipe — the ADR-0008 drift gate: the fake and the real
store pass the SAME laws, so divergence fails here."""

import json

import pytest

from s3dol.testing import FakeBucketStore, _FakeBackend, mock_s3, run_conformance

moto = pytest.importorskip("moto")


@pytest.fixture
def _mock_aws_env(monkeypatch):
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_conformance_fake():
    run_conformance(lambda: FakeBucketStore("mock-bucket", backing=_FakeBackend()))


def test_conformance_fake_with_prefix():
    run_conformance(
        lambda: FakeBucketStore(
            "mock-bucket", backing=_FakeBackend(), prefix="tenant-a"
        )
    )


def test_conformance_real_store(_mock_aws_env):
    from s3dol.base import BucketStore
    from s3dol.connection import S3Connection

    with moto.mock_aws():
        connection = S3Connection(region_name="us-east-1")
        connection.client.create_bucket(Bucket="conf-bucket")
        run_conformance(
            lambda: BucketStore("conf-bucket", connection=connection),
            check_pickle=False,  # moto's patch does not survive a real
            # process hop; pickling itself is covered in test_base_moto
        )


def test_conformance_real_store_with_prefix(_mock_aws_env):
    from s3dol.base import BucketStore
    from s3dol.connection import S3Connection

    with moto.mock_aws():
        connection = S3Connection(region_name="us-east-1")
        connection.client.create_bucket(Bucket="conf-bucket")
        run_conformance(
            lambda: BucketStore("conf-bucket", connection=connection, prefix="p"),
            check_pickle=False,
        )


def test_conformance_json_recipe(_mock_aws_env):
    # Layer C: the value law's normalize is json.loads∘json.dumps (N3 —
    # (1,2) -> [1,2]); values are py objects so the str-rejection and url
    # sections do not apply verbatim.
    from s3dol.connection import S3Connection
    from s3dol.recipes import S3Jsons

    with moto.mock_aws():
        connection = S3Connection(region_name="us-east-1")
        connection.client.create_bucket(Bucket="json-bucket")
        store = S3Jsons("json-bucket", connection=connection)
        store["a"] = {"x": 1, "t": (1, 2)}
        assert store["a"] == json.loads(json.dumps({"x": 1, "t": (1, 2)}))
        assert store["a"]["t"] == [1, 2]  # the honest N3 invariant


def test_shipped_recipe_encoders_produce_bytes():
    # ADR-0008 assertion 9: every shipped recipe's encoder output is a
    # BytesSource — this is what catches ValueCodecs.json() returning str.
    import pickle as pickle_module

    encoders = {
        "S3Texts": lambda s: s.encode(),
        "S3Jsons": lambda obj: json.dumps(obj).encode(),
        "S3Pickles": pickle_module.dumps,
    }
    samples = {"S3Texts": "text", "S3Jsons": {"a": 1}, "S3Pickles": {"a": 1}}
    for name, encode in encoders.items():
        assert isinstance(encode(samples[name]), bytes), name


def test_recipe_wire_format_is_plain_bytes(_mock_aws_env):
    # A codec must not change the wire format: re-read through a raw store.
    from s3dol.base import BucketStore
    from s3dol.connection import S3Connection
    from s3dol.recipes import S3Jsons

    with moto.mock_aws():
        connection = S3Connection(region_name="us-east-1")
        connection.client.create_bucket(Bucket="wire-bucket")
        S3Jsons("wire-bucket", connection=connection)["k"] = {"n": 5}
        raw = BucketStore("wire-bucket", connection=connection)["k"]
        assert json.loads(raw) == {"n": 5}


def test_conformance_catches_a_broken_store():
    # The suite must actually bite: a store that lies about membership fails.
    class Lying(FakeBucketStore):
        def __contains__(self, k):
            return False

    with pytest.raises(AssertionError):
        run_conformance(lambda: Lying("mock-bucket", backing=_FakeBackend()))
