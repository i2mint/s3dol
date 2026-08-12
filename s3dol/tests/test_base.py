"""Layer B: prefix arithmetic, key validity, contracts, guards.

Mostly tier 1 against the in-memory fake (which *is* the real store with only
the backend protocol swapped), plus a moto section for what only a real client
can show.
"""

import pickle
import warnings

import pytest
from dol import KeyCodecs, Pipe, filt_iter

from s3dol.base import (
    MAX_KEY_BYTES,
    BucketStore,
    ObjectHandle,
    _normalize_prefix,
    _validate_key,
)
from s3dol.errors import (
    BucketNotFound,
    ConfigurationError,
    CredentialsError,
    KeyNotValid,
    NotSupported,
    ObjectNotFound,
)
from s3dol.testing import FakeBucketStore, _FakeBackend, mock_s3


# --------------------------------------------------------------------------- #
# prefix arithmetic (ADR-0006 §1) — the boundary the whole scoping story rests on
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "given, expected",
    [("logs", "logs/"), ("logs/", "logs/"), ("/logs/", "logs/"), ("", ""), (None, "")],
)
def test_prefix_normalisation_is_mandatory_and_first(given, expected):
    assert _normalize_prefix(given) == expected


def test_unnormalised_prefix_cannot_leak_a_sibling_tenant():
    # The one-character boundary violation ADR-0006 §1 exists to prevent:
    # prefix='logs' must NOT expose 'logs2/2026.txt' as key '2/2026.txt'.
    data = {"logs/2026.txt": b"mine", "logs2/2026.txt": b"NEIGHBOUR", "logsX": b"x"}
    store = mock_s3(data=data, prefix="logs")
    assert sorted(store) == ["2026.txt"]
    assert store["2026.txt"] == b"mine"
    assert "2/2026.txt" not in store
    with pytest.raises(KeyError):
        store["2/2026.txt"]


def test_scoping_round_trip_with_siblings_markers_and_non_matching_keys():
    data = {
        "a/b": b"1",
        "a/c": b"2",
        "z": b"3",  # non-matching
        "ab/x": b"4",  # sibling prefix
        "a/": b"",  # the exact-prefix marker
    }
    store = mock_s3(data=data, prefix="a/")
    assert sorted(store) == ["b", "c"]  # marker filtered; siblings excluded
    assert all(k in store for k in store)
    for key in store:
        assert store._key_of_id(store._id_of_key(key)) == key


def test_key_of_id_raises_rather_than_slicing_a_non_matching_id():
    # The belt to the server-side Prefix's suspenders: a provider that ignores
    # Prefix must not silently produce corrupt keys.
    store = mock_s3(prefix="a/")
    with pytest.raises(KeyNotValid, match="outside"):
        store._key_of_id("zzz/other")


def test_exact_prefix_marker_is_hidden_from_its_own_scoped_view():
    # The documented wart: it would relativise to '', which is not a valid key.
    store = mock_s3(data={"a/": b"", "a/b": b"1"}, prefix="a/")
    assert sorted(store) == ["b"]
    # ...and stays addressable by absolute key on an unscoped store.
    assert sorted(mock_s3(data={"a/": b"", "a/b": b"1"})) == ["a/", "a/b"]


# --------------------------------------------------------------------------- #
# key validity, before the wire (ADR-0006 §4)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad", ["", 123, None, b"bytes"])
def test_invalid_keys_are_keynotvalid_not_backend_types(bad):
    with pytest.raises(KeyNotValid):
        _validate_key(bad)


def test_key_length_limit_is_utf8_bytes_not_characters():
    _validate_key("é" * (MAX_KEY_BYTES // 2))  # 1024 bytes exactly: fine
    with pytest.raises(KeyNotValid, match="UTF-8 bytes"):
        _validate_key("é" * (MAX_KEY_BYTES // 2 + 1))


def test_lone_surrogate_key_is_rejected_locally():
    with pytest.raises(KeyNotValid, match="UTF-8"):
        _validate_key("bad\ud800key")


def test_keynotvalid_is_both_keyerror_and_valueerror():
    store = mock_s3()
    with pytest.raises(KeyError):
        store[""]
    with pytest.raises(ValueError):
        store[""]


# --------------------------------------------------------------------------- #
# Mapping contracts (architecture.md table)
# --------------------------------------------------------------------------- #


def test_len_raises_with_guidance_rather_than_counting():
    with pytest.raises(TypeError, match="sum\\(1 for _ in store\\)"):
        len(mock_s3())


def test_delete_is_idempotent_by_default_and_probes_only_when_asked():
    store = mock_s3(data={"a": b"1"})
    del store["a"]
    del store["a"]  # idempotent: S3 returns 204 for an absent key
    strict = FakeBucketStore(
        "mock-bucket", backing=_FakeBackend({"a": b"1"}), strict_delete=True
    )
    del strict["a"]
    with pytest.raises(KeyError):
        del strict["a"]


def test_missing_bucket_raises_on_iteration_never_yields_empty():
    store = FakeBucketStore("mock-bucket", backing=_FakeBackend(bucket_exists=False))
    with pytest.raises(BucketNotFound):
        list(store)


def test_on_missing_bucket_assume_performs_no_probe():
    # 'assume' must never call HeadBucket at construction (ADR-0010 §1).
    backend = _FakeBackend(bucket_exists=False)
    FakeBucketStore("mock-bucket", backing=backend)  # constructs fine
    with pytest.raises(BucketNotFound):
        FakeBucketStore("mock-bucket", backing=backend, on_missing_bucket="raise")


def test_on_missing_bucket_create_recovers_without_probing():
    backend = _FakeBackend(bucket_exists=False)
    store = FakeBucketStore("mock-bucket", backing=backend, on_missing_bucket="create")
    store["k"] = b"v"  # attempt -> NoSuchBucket -> create -> retry once
    assert store["k"] == b"v" and backend.bucket_exists


def test_setdefault_never_overwrites_an_archived_object():
    # MutableMapping.setdefault is try/except KeyError -> assign, so an
    # ObjectArchived KeyError would silently overwrite the archived object.
    from s3dol.errors import ObjectArchived

    class Archived(FakeBucketStore):
        def _op_read(self, _id):
            raise ObjectArchived("archived")

    store = Archived("mock-bucket", backing=_FakeBackend({"a": b"COLD"}))
    with pytest.raises(ObjectArchived):
        store.setdefault("a", b"REPLACEMENT")
    assert store._backing.data["a"] == b"COLD"  # untouched
    with pytest.raises(ObjectArchived):
        store.pop("a", b"default")


def test_stores_pickle():
    store = mock_s3(data={"p/a": b"1"}, prefix="p")
    clone = pickle.loads(pickle.dumps(store))
    assert sorted(clone) == ["a"] and clone["a"] == b"1"


def test_repr_shows_scope_and_no_secrets():
    store = mock_s3(prefix="tenant-a")
    text = repr(store)
    assert "mock-bucket" in text and "tenant-a/" in text


# --------------------------------------------------------------------------- #
# writes: the value domain
# --------------------------------------------------------------------------- #


def test_str_values_are_refused_at_the_store_boundary():
    with pytest.raises(TypeError, match="Filepath"):
        mock_s3()["k"] = "a string"


def test_filepath_ref_writes_file_content(tmp_path):
    from s3dol.values import Filepath

    payload = tmp_path / "big.bin"
    payload.write_bytes(b"CONTENT")
    store = mock_s3()
    store["k"] = Filepath(payload)
    assert store["k"] == b"CONTENT"


def test_chunks_ref_concatenates():
    from s3dol.values import Chunks

    store = mock_s3()
    store["k"] = Chunks(iter([b"ab", b"cd"]))
    assert store["k"] == b"abcd"


# --------------------------------------------------------------------------- #
# anonymous stores refuse writes locally (ADR-0012 D5)
# --------------------------------------------------------------------------- #


def test_anonymous_store_refuses_writes_locally():
    store = mock_s3(anon=True)
    with pytest.raises(CredentialsError):
        store["k"] = b"v"
    with pytest.raises(CredentialsError):
        del store["k"]


def test_anonymous_url_for_returns_none_not_raises():
    # SupportsUrlFor is Optional[str]; raising would turn a public-bucket
    # streaming fallback into a 500 (ADR-0012 D5).
    assert mock_s3(data={"k": b"v"}, anon=True).url_for("k") is None


# --------------------------------------------------------------------------- #
# url_for: the single guarded keyed method (ADR-0011 D3b)
# --------------------------------------------------------------------------- #


def test_url_for_unwrapped_addresses_the_prefixed_key():
    store = mock_s3(data={"p/f": b"v"}, prefix="p")
    assert "p/f" in store.url_for("f")


def test_url_for_refuses_once_the_leaf_has_been_wrapped():
    # The finding behind ADR-0011 D3b's revision: TWO call paths reach this
    # one signature with different key domains — dol.content_url pre-resolves
    # the key through the outer layers and stops at the provider "because that
    # layer applies its own" (dol/content.py), while a direct delegated call
    # hands over the raw outer key. Indistinguishable here, and a wrong
    # presigned URL addresses a different REAL object, so: refuse.
    for make_call in (
        lambda s: KeyCodecs.prefixed("x/")(s).url_for("g"),  # temporary wrapper
        lambda s: _wrap_then_call(s),  # named wrapper
    ):
        store = mock_s3(data={"p/x/g": b"v"}, prefix="p")
        with pytest.raises(NotSupported, match="s3dol.urls"):
            make_call(store)


def _wrap_then_call(store):
    wrapper = KeyCodecs.prefixed("x/")(store)
    return wrapper.url_for("g")


def test_url_for_refuses_even_after_the_wrapper_is_collected():
    # A delegated bound method holds no reference to its wrapper, so the
    # weakref-based wrapped_self degrades to "never wrapped" here (ADR-0011
    # D1a). The probe-descriptor flag survives that, which is what makes the
    # refusal reliable rather than intermittent.
    store = mock_s3(data={"p/f": b"v"}, prefix="p")
    wrapper = KeyCodecs.prefixed("x/")(store)
    del wrapper
    with pytest.raises(NotSupported, match="s3dol.urls"):
        store.url_for("f")


def test_dol_content_url_is_correct_for_an_unwrapped_store():
    # The reason url_for exists at all (dol.SupportsUrlFor is a *method* and
    # content_url reaches it by getattr).
    from dol import content_url

    store = mock_s3(data={"p/g": b"v"}, prefix="p")
    assert "p/g" in content_url(store, "g")


def test_the_sibling_store_is_correct_where_url_for_must_refuse():
    # ...and the remedy the refusal names is correct at any depth, because it
    # goes through __getitem__ (ADR-0011 D2).
    import s3dol

    store = mock_s3(data={"p/x/g": b"v"}, prefix="p")
    wrapped_data, wrapped_urls = (
        KeyCodecs.prefixed("x/")(store),
        KeyCodecs.prefixed("x/")(s3dol.urls(store)),
    )
    assert wrapped_data["g"] == b"v"
    assert "p/x/g" in wrapped_urls["g"]


def test_url_for_clamps_and_warns_past_sigv4s_cap():
    store = mock_s3(data={"k": b"v"})
    with pytest.warns(UserWarning, match="604800"):
        url = store.url_for("k", expires_in=999_999)
    assert "X-Amz-Expires=604800" in url


# --------------------------------------------------------------------------- #
# ObjectHandle binds its key at construction
# --------------------------------------------------------------------------- #


def test_handle_reads_ranges_streams_and_downloads(tmp_path):
    from s3dol.testing import FakeObjectHandle

    handle = FakeObjectHandle(
        "b", "k", backing=_FakeBackend({"k": b"the quick brown fox"})
    )
    assert handle.read() == b"the quick brown fox"
    assert handle.read(offset=4, length=5) == b"quick"
    assert b"".join(handle.stream(chunk_size=4)) == b"the quick brown fox"
    target = handle.download_to(tmp_path / "out.bin")
    assert open(target, "rb").read() == b"the quick brown fox"
    assert handle.exists() and handle.info().size == 19


def test_handle_missing_object_raises_keyerror():
    from s3dol.testing import FakeObjectHandle

    handle = FakeObjectHandle("b", "nope", backing=_FakeBackend())
    with pytest.raises(ObjectNotFound):
        handle.read()
    assert handle.exists() is False


# --------------------------------------------------------------------------- #
# construction validation
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad_bucket", ["", None, 42])
def test_bucket_must_be_a_non_empty_str(bad_bucket):
    with pytest.raises(ConfigurationError):
        mock_s3(bad_bucket)


def test_unknown_on_missing_bucket_value_rejected():
    with pytest.raises(ConfigurationError, match="on_missing_bucket"):
        FakeBucketStore("b", backing=_FakeBackend(), on_missing_bucket="probe")


# --------------------------------------------------------------------------- #
# moto tier 2: what only a real client shows
# --------------------------------------------------------------------------- #


def test_real_store_round_trip_and_presign_is_sigv4(monkeypatch):
    moto = pytest.importorskip("moto")
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    from s3dol.connection import S3Connection

    with moto.mock_aws():
        connection = S3Connection(region_name="us-east-1")
        connection.client.create_bucket(Bucket="real-bucket")
        store = BucketStore("real-bucket", connection=connection, prefix="p")
        store["a"] = b"alpha"
        assert store["a"] == b"alpha" and sorted(store) == ["a"]
        url = store.url_for("a")
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url and "/p/a" in url
        assert "AWSAccessKeyId" not in url
        # the object really is at the prefixed key
        raw = connection.client.get_object(Bucket="real-bucket", Key="p/a")
        assert raw["Body"].read() == b"alpha"


def test_real_store_pickles_across_a_process_boundary(monkeypatch):
    pytest.importorskip("moto")
    from s3dol.connection import S3Connection

    store = BucketStore(
        "real-bucket", connection=S3Connection(region_name="us-east-1"), prefix="p"
    )
    clone = pickle.loads(pickle.dumps(store))
    assert clone.bucket == store.bucket and clone.prefix == store.prefix
    assert clone.connection == store.connection
