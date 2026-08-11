"""Sibling capability stores and free functions (ADR-0011 D2/D3/D4)."""

import pytest
from dol import KeyCodecs, Pipe, cached_keys, filt_iter

import s3dol
from s3dol.errors import ConfigurationError, CredentialsError, S3PartialFailure
from s3dol.testing import mock_s3

# --------------------------------------------------------------------------- #
# D2 — sibling stores are correct BY CONSTRUCTION (they route through
# __getitem__, the one thing dol maps correctly at every depth)
# --------------------------------------------------------------------------- #


def test_siblings_share_the_stores_key_space():
    store = mock_s3(data={"p/f": b"value"}, prefix="p")
    assert s3dol.handles(store)["f"].key == "p/f"
    assert "p/f" in s3dol.urls(store)["f"]
    assert s3dol.info(store)["f"].size == 5


@pytest.mark.parametrize(
    "wrap",
    [
        lambda s: KeyCodecs.prefixed("x/")(s),
        lambda s: Pipe(filt_iter.prefixes("x/"), KeyCodecs.prefixed("x/"))(s),
        lambda s: cached_keys(KeyCodecs.prefixed("x/")(s)),
    ],
    ids=["key-codec", "pipe", "cached_keys-over-codec"],
)
def test_parallel_wrapping_is_correct_at_any_depth_and_lifetime(wrap):
    # The property no resolution primitive can offer: no inner_most_key, no
    # wrapped_self, no live reference needed.
    store = mock_s3(data={"p/x/f": b"D"}, prefix="p")
    assert wrap(store)["f"] == b"D"
    assert wrap(s3dol.handles(store))["f"].key == "p/x/f"
    assert "p/x/f" in wrap(s3dol.urls(store))["f"]
    # ...including when nothing holds the wrapper (a temporary in one expr):
    assert KeyCodecs.prefixed("x/")(s3dol.handles(store))["f"].key == "p/x/f"


def test_accessors_refuse_a_wrapped_store_and_name_the_remedy():
    store = mock_s3(data={"p/f": b"v"}, prefix="p")
    wrapped = KeyCodecs.prefixed("x/")(store)
    for accessor in (s3dol.handles, s3dol.urls, s3dol.info):
        with pytest.raises(ConfigurationError) as info:
            accessor(wrapped)
        message = str(info.value)
        assert "unwrapped" in message and "codec(" in message


def test_accessors_reject_a_non_store():
    with pytest.raises(ConfigurationError, match="s3dol.handles"):
        s3dol.handles({"a": b"1"})


def test_url_sibling_takes_presign_options():
    store = mock_s3(data={"k": b"v"})
    assert "X-Amz-Expires=60" in s3dol.urls(store, expires_in=60)["k"]


def test_siblings_iterate_the_same_keys_as_the_data_store():
    store = mock_s3(data={"p/a": b"1", "p/b": b"2", "other": b"3"}, prefix="p")
    assert sorted(s3dol.handles(store)) == sorted(store) == ["a", "b"]


def test_anonymous_urls_sibling_yields_none():
    assert s3dol.urls(mock_s3(data={"k": b"v"}, anon=True))["k"] is None


# --------------------------------------------------------------------------- #
# D3 — free functions
# --------------------------------------------------------------------------- #


def test_sub_on_an_unwrapped_store_extends_the_prefix_and_keeps_pushdown():
    store = mock_s3(data={"a/b/c": b"1", "a/d": b"2"}, prefix="a")
    child = s3dol.sub(store, "b")
    assert list(child) == ["c"] and child["c"] == b"1"
    assert type(child) is type(store)  # same class; the prefix moved
    assert child.prefix == "a/b/"


def test_sub_on_a_wrapped_store_composes_filter_first():
    # Filter FIRST is the only safe composition (ADR-0006 §1): reversed, the
    # store is silently empty — and bare prefixed() corrupts out-of-band keys.
    store = mock_s3(data={"p/x/b/c": b"1", "p/x/d": b"2"}, prefix="p")
    wrapped = KeyCodecs.prefixed("x/")(store)
    child = s3dol.sub(wrapped, "b")
    assert list(child) == ["c"] and child["c"] == b"1"


def test_sub_rejects_a_foreign_object():
    with pytest.raises(ConfigurationError, match="s3dol.sub"):
        s3dol.sub({"a": 1}, "p")


def test_prefixes_lists_one_level_in_the_callers_key_space():
    store = mock_s3(
        data={"a/b/c": b"1", "a/d": b"2", "a/e/f": b"3"}, prefix="a"
    )
    assert sorted(s3dol.prefixes(store)) == ["b/", "e/"]


def test_prefixes_maps_outward_through_a_wrapper():
    store = mock_s3(data={"p/x/dir/a": b"1", "p/x/e": b"2", "p/other/z": b"3"}, prefix="p")
    wrapped = KeyCodecs.prefixed("x/")(store)
    # scoped to the caller's key space: 'other/' is NOT theirs to see
    assert s3dol.prefixes(wrapped) == ["dir/"]


def test_prefixes_refuses_a_wrapper_whose_codec_has_no_directory_structure():
    store = mock_s3(data={"p/a/b": b"1"}, prefix="p")
    wrapped = KeyCodecs.suffixed(".json")(store)  # maps '' -> '.json'
    with pytest.raises(ConfigurationError, match="directory structure"):
        s3dol.prefixes(wrapped)


def test_free_functions_refuse_a_hand_rolled_non_store_layer():
    # D3a's documented hole: a passthrough __getattr__ layer breaks the walk.
    # Refuse rather than return a plausible wrong key.
    class Passthrough:
        def __init__(self, store):
            self.store = store

        def __getattr__(self, name):
            return getattr(self.store, name)

    layered = Passthrough(mock_s3(data={"p/a": b"1"}, prefix="p"))
    with pytest.raises(ConfigurationError, match="refuse rather than guess"):
        s3dol.prefixes(layered)


# --------------------------------------------------------------------------- #
# D4 — destructive operations are free functions
# --------------------------------------------------------------------------- #


def test_delete_many_resolves_the_callers_keys_through_the_chain():
    store = mock_s3(data={"p/x/a": b"1", "p/x/b": b"2", "p/other": b"3"}, prefix="p")
    wrapped = KeyCodecs.prefixed("x/")(store)
    s3dol.delete_many(wrapped, ["a", "b"])
    assert sorted(store._backing.data) == ["p/other"]  # nothing outside scope


def test_delete_many_on_an_unwrapped_store():
    store = mock_s3(data={"p/a": b"1", "p/b": b"2"}, prefix="p")
    s3dol.delete_many(store, ["a"])
    assert sorted(store) == ["b"]


def test_delete_many_is_idempotent_for_absent_keys():
    # S3 reports absent keys as Deleted; delete_many does not pretend otherwise.
    store = mock_s3(data={"a": b"1"})
    s3dol.delete_many(store, ["a", "never-existed"])
    assert list(store) == []


def test_delete_many_chunks_at_a_thousand():
    from s3dol.capabilities import DELETE_MANY_CHUNK

    assert DELETE_MANY_CHUNK == 1000  # AWS's cap; moto accepts 1001
    calls = []

    class Counting(type(mock_s3())):
        def _op_bulk_delete(self, ids):
            calls.append(len(ids))
            return super()._op_bulk_delete(ids)

    from s3dol.testing import _FakeBackend

    data = {f"k{i}": b"v" for i in range(2500)}
    store = Counting("mock-bucket", backing=_FakeBackend(data))
    s3dol.delete_many(store, list(data))
    assert calls == [1000, 1000, 500]


def test_delete_many_partial_failure_carries_the_split():
    from s3dol.testing import FakeBucketStore, _FakeBackend

    class Failing(FakeBucketStore):
        def _op_bulk_delete(self, ids):
            return [ids[0]], [(ids[1], "AccessDenied", "nope")]

    store = Failing("b", backing=_FakeBackend({"a": b"1", "b": b"2"}))
    with pytest.raises(S3PartialFailure) as info:
        s3dol.delete_many(store, ["a", "b"])
    assert info.value.succeeded == ["a"] and set(info.value.failures) == {"b"}


def test_partial_failure_reports_keys_in_the_callers_key_space():
    # "Keys embedded in an exception payload" is one of the shapes a codec
    # boundary cannot map (the raise site has no boundary frame) — but a free
    # function holds the store, so the inverse walk IS available here.
    from s3dol.testing import FakeBucketStore, _FakeBackend

    class Failing(FakeBucketStore):
        def _op_bulk_delete(self, ids):
            return [], [(ids[0], "AccessDenied", "nope")]

    store = Failing("b", backing=_FakeBackend({"p/x/a": b"1"}), prefix="p")
    wrapped = KeyCodecs.prefixed("x/")(store)
    with pytest.raises(S3PartialFailure) as info:
        s3dol.delete_many(wrapped, ["a"])
    # 'a' — the key the caller passed — not the wire key 'p/x/a'
    assert set(info.value.failures) == {"a"}


def test_delete_many_emulates_when_batch_delete_is_unsupported():
    # GCS and any provider that rejects the mandatory DeleteObjects checksum:
    # loop DeleteObject; identical observable result, only cost differs.
    from s3dol.connection import S3Connection
    from s3dol.testing import FakeBucketStore, _FakeBackend

    store = FakeBucketStore(
        "b",
        backing=_FakeBackend({"a": b"1", "b": b"2"}),
        connection=S3Connection(endpoint_url="https://storage.googleapis.com"),
    )
    assert store._resolution().capabilities.batch_delete is False
    s3dol.delete_many(store, ["a", "b"])
    assert list(store) == []


def test_delete_many_refuses_a_foreign_first_argument():
    # The honest caveat: a free function accepts anything — so validate.
    with pytest.raises(ConfigurationError):
        s3dol.delete_many({"a": b"1"}, ["a"])


def test_delete_many_refuses_anonymous_writes():
    with pytest.raises(CredentialsError):
        s3dol.delete_many(mock_s3(data={"a": b"1"}, anon=True), ["a"])


def test_delete_bucket_requires_an_endpoint_store():
    with pytest.raises(ConfigurationError, match="delete_bucket"):
        s3dol.delete_bucket(mock_s3(), "b", force=True)
