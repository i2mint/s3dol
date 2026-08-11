"""``s3dol.testing`` — a shipped in-memory fake and the exported conformance
suite (ADR-0008). Downstream users can test services that take a store as a
dependency with **no network, no docker, no moto**::

    from s3dol.testing import mock_s3

    def test_my_service():
        assert MediaService(mock_s3()).url('a')

The fake is not a re-implementation: :class:`FakeBucketStore` *is* the real
:class:`~s3dol.base.BucketStore` with only the backend protocol (the ``_op_*``
seam of ADR-0002) swapped for a dict — so the prefix arithmetic, key validity,
error taxonomy (the fake raises *synthesized botocore ClientErrors* through
the same translation seam), guards and Mapping semantics are structurally
identical, and drift fails the same conformance suite that gates the real
store.

The suite is parameterized by capability flags: ``mock_s3()`` runs the
structural presign assertion (2a) but not the fetch-it assertion (2b — there
is no HTTP server), so the honest claim is *"the same suite, parameterized by
an ``endpoint_is_fetchable`` capability"* (ADR-0008).
"""

from __future__ import annotations

import pickle
from datetime import datetime, timezone
from typing import Callable, Optional
from urllib.parse import quote

from s3dol.base import BucketStore, ObjectInfo, clamp_expires_in
from s3dol.capabilities import BucketHandles, BucketInfo, BucketUrls
from s3dol.errors import KeyNotValid, ObjectNotFound, S3Error
from s3dol.values import as_fileobj, is_bytes_like, reject_str_value


def _client_error(code: str, status: int, operation: str):
    """A synthesized botocore ClientError, exactly like the wire produces."""
    from botocore.exceptions import ClientError

    return ClientError(
        {
            "Error": {"Code": code, "Message": "synthesized by s3dol.testing"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )


class _FakeBackend:
    """The shared in-memory 'bucket': absolute (wire) keys -> bytes."""

    def __init__(self, data: Optional[dict] = None, *, bucket_exists: bool = True):
        self.data: dict = dict(data or {})
        self.bucket_exists = bucket_exists


class _FakeOps:
    """Mixin overriding exactly the ``_op_*`` backend protocol with dict
    operations that raise synthesized ``ClientError``s — everything above the
    seam (guards, taxonomy, key arithmetic) runs unchanged."""

    def __init__(self, *args, backing: Optional[_FakeBackend] = None, **kwargs):
        self._backing = backing if backing is not None else _FakeBackend()
        super().__init__(*args, **kwargs)

    # the fake never builds a client
    @property
    def client(self):
        raise S3Error(
            "s3dol.testing fakes have no boto3 client — a code path reached "
            "the backend outside the _op_* protocol seam (that is a bug worth "
            "reporting)."
        )

    def _check_bucket(self, operation):
        if not self._backing.bucket_exists:
            raise _client_error("NoSuchBucket", 404, operation)

    def _op_list_pages(
        self, *, delimiter: Optional[str] = None, prefix: Optional[str] = None
    ):
        self._check_bucket("ListObjectsV2")
        base = self.prefix if prefix is None else prefix
        keys = sorted(k for k in self._backing.data if k.startswith(base))
        if delimiter is None:
            contents = [
                {
                    "Key": key,
                    "Size": len(self._backing.data[key]),
                    "LastModified": datetime.now(timezone.utc),
                    "ETag": '"fake"',
                }
                for key in keys
            ]
            yield {"Contents": contents}
            return
        seen, prefixes = set(), []
        for key in keys:
            rest = key[len(base) :]
            if delimiter in rest.rstrip(delimiter):
                head = rest.split(delimiter, 1)[0] + delimiter
                if head not in seen:
                    seen.add(head)
                    prefixes.append({"Prefix": f"{base}{head}"})
        yield {"CommonPrefixes": prefixes}

    def _op_head_object(self, _id):
        self._check_bucket("HeadObject")
        if _id not in self._backing.data:
            # moto returns a body here; real AWS does not. The fake mimics
            # REAL AWS (code synthesized from the status) so the ambiguity
            # path of ADR-0004 §2 is exercised, which moto cannot do.
            raise _client_error("404", 404, "HeadObject")
        value = self._backing.data[_id]
        return {
            "ContentLength": len(value),
            "LastModified": datetime.now(timezone.utc),
            "ETag": '"fake"',
            "ContentType": "binary/octet-stream",
        }

    def _op_head_bucket(self):
        if not self._backing.bucket_exists:
            raise _client_error("404", 404, "HeadBucket")

    def _op_read(self, _id):
        self._check_bucket("GetObject")
        if _id not in self._backing.data:
            raise _client_error("NoSuchKey", 404, "GetObject")
        return self._backing.data[_id]

    def _op_write(self, _id, value):
        self._check_bucket("PutObject")
        if isinstance(value, str):
            reject_str_value(value)
        if is_bytes_like(value):
            self._backing.data[_id] = bytes(value)
        else:
            with as_fileobj(value) as fileobj:
                self._backing.data[_id] = fileobj.read()

    def _op_delete(self, _id):
        self._check_bucket("DeleteObject")
        self._backing.data.pop(_id, None)  # idempotent, like the wire

    def _op_bulk_delete(self, _ids):
        self._check_bucket("DeleteObjects")
        deleted = []
        for _id in _ids:
            self._backing.data.pop(_id, None)
            deleted.append(_id)  # absent keys report as Deleted, like S3
        return deleted, []

    def _op_create_bucket(self):
        self._backing.bucket_exists = True

    def _op_presign(self, _id, *, expires_in, client_method, **params):
        if self.connection.anon:
            return None
        expires_in = clamp_expires_in(expires_in)  # same guard as the real one
        return (
            f"https://{self.bucket}.mock-s3.invalid/{quote(_id)}"
            f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires={expires_in}"
            f"&X-Amz-Signature=mock"
        )

    def _sibling_kwargs(self):
        return super()._sibling_kwargs()

    def _make_sibling(self, kind, **options):
        sibling_class = {
            "handles": FakeBucketHandles,
            "urls": FakeBucketUrls,
            "info": FakeBucketInfo,
        }[kind]
        return sibling_class(
            **{**super()._sibling_kwargs(), **options}, backing=self._backing
        )

    def _with(self, **overrides):
        kwargs = super()._sibling_kwargs()
        kwargs["on_missing_bucket"] = self.on_missing_bucket
        kwargs.update(overrides)
        return type(self)(kwargs.pop("bucket"), backing=self._backing, **kwargs)


class FakeObjectHandle:
    """The fake's per-object escape hatch (mirrors ``ObjectHandle``)."""

    def __init__(self, bucket: str, key: str, *, backing: _FakeBackend, anon=False):
        self.bucket = bucket
        self.key = key
        self._backing = backing
        self._anon = anon

    def __repr__(self):
        return f"{type(self).__name__}({self.bucket!r}, {self.key!r})"

    def _bytes(self) -> bytes:
        if self.key not in self._backing.data:
            raise ObjectNotFound(f"GetObject failed: key={self.key!r}")
        return self._backing.data[self.key]

    def read(self, *, offset=None, length=None) -> bytes:
        data = self._bytes()
        if offset is None and length is None:
            return data
        start = offset or 0
        return data[start : None if length is None else start + length]

    def stream(self, *, chunk_size=8 * 2**20):
        data = self._bytes()
        for start in range(0, len(data), chunk_size):
            yield data[start : start + chunk_size]

    def download_to(self, path) -> str:
        from s3dol.values import Filepath

        target = path.path if isinstance(path, Filepath) else str(path)
        with open(target, "wb") as f:
            f.write(self._bytes())
        return target

    def write(self, value) -> None:
        if isinstance(value, str):
            reject_str_value(value)
        if is_bytes_like(value):
            self._backing.data[self.key] = bytes(value)
        else:
            with as_fileobj(value) as fileobj:
                self._backing.data[self.key] = fileobj.read()

    def delete(self) -> None:
        self._backing.data.pop(self.key, None)

    def exists(self) -> bool:
        return self.key in self._backing.data

    def info(self) -> ObjectInfo:
        return ObjectInfo(
            key=self.key,
            size=len(self._bytes()),
            last_modified=datetime.now(timezone.utc),
            etag='"fake"',
            content_type="binary/octet-stream",
            storage_class="STANDARD",
        )

    def url(self, *, expires_in=3600, client_method="get_object", **params):
        if self._anon:
            return None
        return (
            f"https://{self.bucket}.mock-s3.invalid/{quote(self.key)}"
            f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires={expires_in}"
            f"&X-Amz-Signature=mock"
        )


class FakeBucketStore(_FakeOps, BucketStore):
    """The real ``BucketStore`` over a dict backend."""


class FakeBucketHandles(_FakeOps, BucketHandles):
    def __getitem__(self, k):
        return FakeObjectHandle(
            self.bucket,
            self._id_of_key(k),
            backing=self._backing,
            anon=self.connection.anon,
        )


class FakeBucketUrls(_FakeOps, BucketUrls):
    def __getitem__(self, k):
        return self._op_presign(
            self._id_of_key(k),
            expires_in=self.expires_in,
            client_method=self.client_method,
        )


class FakeBucketInfo(_FakeOps, BucketInfo):
    def __getitem__(self, k):
        _id = self._id_of_key(k)
        if _id not in self._backing.data:
            raise ObjectNotFound(f"HeadObject failed: key={k!r}")
        return ObjectInfo(
            key=_id,
            size=len(self._backing.data[_id]),
            last_modified=datetime.now(timezone.utc),
            etag='"fake"',
            content_type="binary/octet-stream",
            storage_class="STANDARD",
        )


def mock_s3(
    bucket: str = "mock-bucket",
    *,
    prefix: str = "",
    data: Optional[dict] = None,
    bucket_exists: bool = True,
    anon: bool = False,
) -> FakeBucketStore:
    """An in-process fake ``BucketStore``: no network, no docker, no moto.

    ``data`` seeds the bucket with *absolute* (wire) keys -> bytes.

    >>> s = mock_s3(data={'logs/a.txt': b'hi', 'other/z': b'no'}, prefix='logs')
    >>> list(s), s['a.txt']
    (['a.txt'], b'hi')
    >>> s['b'] = b'new'; sorted(s)
    ['a.txt', 'b']
    """
    from s3dol.connection import S3Connection

    return FakeBucketStore(
        bucket,
        backing=_FakeBackend(data, bucket_exists=bucket_exists),
        connection=S3Connection(anon=anon) if anon else S3Connection(),
        prefix=prefix,
    )


# --------------------------------------------------------------------------- #
# The exported conformance suite (ADR-0008)
# --------------------------------------------------------------------------- #


def run_conformance(
    make_store: Callable[[], object],
    *,
    endpoint_is_fetchable: bool = False,
    normalize: Callable = lambda v: v,
    check_pickle: bool = True,
    check_urls: bool = True,
    secret: Optional[str] = None,
) -> None:
    """Assert the s3dol store laws against a fresh store from ``make_store``.

    ``make_store`` must return an EMPTY, writable store (each call may return
    a new one over the same empty backend). ``normalize`` is the per-layer
    canonical form of ADR-0005 N3 (identity for Layer B; ``json.loads ∘
    json.dumps`` for a json recipe, …). ``secret``, when given, is asserted
    absent from every repr this suite touches.

    Sibling packages and user code are welcome to run this against their own
    stores — that is why it ships in the package rather than in ``tests/``.
    """
    from urllib.parse import urlsplit

    store = make_store()

    # -- Mapping laws + the value law (N3)
    store["a"] = b"alpha"
    store["b/c"] = b"beta"
    assert store["a"] == normalize(b"alpha")
    assert sorted(store) == ["a", "b/c"]
    assert "a" in store and "b/c" in store
    assert "missing" not in store
    # iter/contains agreement — the law EncodingType breaks (ADR-0006 §4)
    assert all(k in store for k in store)
    try:
        store["missing"]
        raise AssertionError("reading an absent key must raise KeyError")
    except KeyError:
        pass
    assert store.get("missing") is None  # the defaulted accessor degrades
    del store["b/c"]
    assert "b/c" not in store
    del store["b/c"]  # idempotent (documented; costs no probe)

    # -- len() raises with guidance (ADR-0008 cost model)
    try:
        len(store)
        raise AssertionError("len(store) must raise TypeError")
    except TypeError:
        pass

    # -- keys that must round-trip (and KeyNotValid before the wire)
    for key in ["plain", "a b", "café", "a+b", "a\rb", "p/x y", "a%20b"]:
        store[key] = b"v"
    assert all(k in store for k in store)
    for bad in ["", 123, "x" * 2000]:
        try:
            store[bad] = b"v"
            raise AssertionError(f"key {bad!r} must be rejected")
        except (KeyNotValid, KeyError, ValueError, TypeError):
            pass

    # -- str values are rejected, loudly (ADR-0005 §2)
    try:
        store["s"] = "a string"
        raise AssertionError("str values must be rejected")
    except TypeError:
        pass

    # -- url assertions (2a: structural, never substring — ADR-0008)
    if check_urls:
        url_for = getattr(store, "url_for", None)
        if callable(url_for):
            url = url_for("a")
            if url is not None:
                parts = urlsplit(url)
                assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in (parts.query or ""), url
                assert "AWSAccessKeyId" not in url, f"SigV2 tell in {url!r}"
                assert parts.path.split("/")[-1] in ("a", quote("a")), (
                    f"URL path must address the key: {url!r}"
                )
                if endpoint_is_fetchable:
                    import requests

                    assert requests.get(url).content == normalize(b"alpha")

    # -- pickling (ADR-0008 §6): a store must cross a process boundary
    if check_pickle:
        clone = pickle.loads(pickle.dumps(store))
        assert sorted(clone) == sorted(store)

    # -- no secret in repr (ADR-0008 §7)
    if secret is not None:
        assert secret not in repr(store)
        connection = getattr(store, "connection", None)
        if connection is not None:
            assert secret not in repr(connection)
