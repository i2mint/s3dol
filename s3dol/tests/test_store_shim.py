"""The v0 ``S3Store`` compat shim (ADR-0007 §3/§4, ADR-0012 D7).

The shim's justification is "existing users keep working", so these tests are
written from the dependents' *actual* usage: `lacing` calls
``S3Store(bucket, path=prefix, **kw)`` positionally and passes
``make_bucket=True``; `http_cosmo_prep` passes the bucket by keyword with an
explicit ``endpoint_url``, writes ``str`` values, uses ``store['folder/']`` as
a sub-store, and relies on ``del`` being a silent no-op.
"""

import inspect
import warnings

import pytest

from s3dol.errors import S3DolResolutionChanged
from s3dol.store import (
    S3Store,
    _V0CompatStore,
    dechunk_aws_chunked,
    detect_chunked_framing,
)
from s3dol.testing import _FakeBackend, mock_s3


def compat(data=None, prefix="", dechunk=False):
    """A shim store over the in-memory fake (no network)."""
    return _V0CompatStore(mock_s3(data=data, prefix=prefix), dechunk=dechunk)


# --------------------------------------------------------------------------- #
# the signature must not move (both dependents import s3dol.store.S3Store)
# --------------------------------------------------------------------------- #


def test_module_survives_as_an_importable_path():
    import s3dol.store  # noqa: F401  — the fully-qualified import both use

    assert callable(S3Store)


def test_signature_is_v0s_exactly():
    parameters = inspect.signature(S3Store).parameters
    assert list(parameters)[0] == "bucket_name"
    assert parameters["bucket_name"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    for name in (
        "make_bucket",
        "path",  # NOT renamed to prefix on the shim
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "endpoint_url",
        "region_name",
        "profile_name",
        "skip_bucket_check",
        "is_supabase_endpoint",  # v0 accepts it; omitting it = TypeError on upgrade
    ):
        assert name in parameters, name


def test_test_utils_still_importable_for_py2store():
    # py2store imports these under suppress(ImportError) — breakage is SILENT.
    from s3dol.tests.util import (  # noqa: F401
        extract_s3_access_info,
        get_s3_test_access_info_from_env_vars,
    )


def test_calling_it_warns_about_the_v2_removal():
    with pytest.warns(DeprecationWarning, match="s3_store"):
        with pytest.raises(Exception):  # no credentials/bucket here; the warning is the point
            S3Store("some-bucket", make_bucket=False)


# --------------------------------------------------------------------------- #
# the three behaviours a dependent's PASSING tests rely on (ADR-0007 §4)
# --------------------------------------------------------------------------- #


def test_str_values_are_accepted_with_a_deprecation_warning():
    store = compat()
    with pytest.warns(DeprecationWarning, match="utf-8"):
        store["k"] = "a string"
    assert store["k"] == b"a string"


def test_trailing_slash_returns_a_sub_store():
    store = compat(data={"level1/level2/k": b"v"})
    assert store["level1/level2/"]["k"] == b"v"
    assert store["level1/"]["level2/"]["k"] == b"v"
    assert store["level1/"]["level2/k"] == b"v"


def test_delete_of_an_absent_key_is_a_silent_no_op():
    store = compat(data={"a": b"1"})
    del store["a"]
    del store["a"]  # v0 relied on this; v1 deletes are idempotent anyway


def test_len_still_counts_on_the_shim():
    # The v1 store raises TypeError; the shim keeps v0's behaviour (and cost).
    assert len(compat(data={"a": b"1", "b": b"2"})) == 2


def test_url_for_is_forwarded_and_prefix_aware():
    store = compat(data={"blobs/abc": b"v"}, prefix="blobs")
    url = store.url_for("abc")
    assert "blobs/abc" in url
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url  # the #10 fix
    assert "AWSAccessKeyId" not in url  # the SigV2 tell


def test_shim_iterates_and_contains_like_a_mapping():
    store = compat(data={"a": b"1", "b": b"2"})
    assert sorted(store) == ["a", "b"]
    assert "a" in store and "zz" not in store


# --------------------------------------------------------------------------- #
# the corrected de-chunker (ADR-0003 §3) — v0's truncated multi-chunk payloads
# --------------------------------------------------------------------------- #


def test_dechunk_single_chunk():
    assert dechunk_aws_chunked(b"5\r\nhello\r\n0\r\n\r\n") == b"hello"


def test_dechunk_multi_chunk_where_v0_truncated():
    framed = b"3\r\nabc\r\n2\r\nde\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n"
    assert dechunk_aws_chunked(framed) == b"abcde"


@pytest.mark.parametrize(
    "payload",
    [
        b"cafe is hex-ish but not framed",  # v0 misfired on this shape
        b"0123456789",
        b"",
        b"deadbeef",
        b"5\r\nhello",  # truncated framing: passthrough, never a partial parse
        b"5\r\ntoo-long-for-its-header\r\n0\r\n\r\n",
    ],
)
def test_dechunk_is_full_parse_or_passthrough(payload):
    assert dechunk_aws_chunked(payload) == payload


def test_detect_chunked_framing_finds_the_corrupted_keys():
    store = mock_s3(
        data={"good": b"plain bytes", "bad": b"5\r\nhello\r\n0\r\n\r\n"}
    )
    assert detect_chunked_framing(store) == ["bad"]


def test_dechunking_store_repairs_reads_only_when_enabled():
    framed = b"5\r\nhello\r\n0\r\n\r\n"
    assert compat(data={"k": framed}, dechunk=True)["k"] == b"hello"
    assert compat(data={"k": framed})["k"] == framed  # never a default


# --------------------------------------------------------------------------- #
# the D7 migration warning
# --------------------------------------------------------------------------- #


def test_warns_when_v1_resolves_differently_than_v0(monkeypatch):
    # v0 DROPPED an explicit endpoint_url whenever env credentials existed —
    # the store then talked to AWS. v1 honours it; that moves the data target,
    # so it must announce itself.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        S3Store(
            "b",
            endpoint_url="https://minio.example:9000",
            aws_access_key_id="AKIAEXPLICIT",
            aws_secret_access_key="explicitsecret",
        )
    messages = [str(w.message) for w in caught if w.category is S3DolResolutionChanged]
    assert messages and "diagnose" in messages[0]


def test_no_warning_when_nothing_moves(monkeypatch):
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(var, raising=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        S3Store("b", endpoint_url="https://minio.example:9000")
    assert not [w for w in caught if w.category is S3DolResolutionChanged]


def test_strict_mode_is_filterwarnings_not_a_kwarg(monkeypatch):
    # ADR-0012 D1: no strict= kwarg; filterwarnings('error', S3DolWarning) is
    # strict mode, with zero API surface.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    from s3dol.errors import S3DolWarning

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        warnings.filterwarnings("error", category=S3DolWarning)
        with pytest.raises(S3DolResolutionChanged):
            S3Store(
                "b",
                endpoint_url="https://minio.example:9000",
                aws_access_key_id="AKIAEXPLICIT",
                aws_secret_access_key="explicitsecret",
            )


def test_make_bucket_tristate_maps_to_on_missing_bucket():
    from s3dol.base import BucketStore

    captured = {}
    original = BucketStore.__init__

    def spy(self, *args, **kwargs):
        captured["on_missing_bucket"] = kwargs.get("on_missing_bucket")
        captured["prefix"] = kwargs.get("prefix")
        original(self, *args, **kwargs)

    BucketStore.__init__ = spy
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            S3Store("b", make_bucket=True, path="logs")
            assert captured == {"on_missing_bucket": "create", "prefix": "logs"}
            S3Store("b", make_bucket=None)
            assert captured["on_missing_bucket"] == "assume"
    finally:
        BucketStore.__init__ = original
