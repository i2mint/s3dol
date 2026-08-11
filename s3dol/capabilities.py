"""Keyed capabilities as sibling stores; non-keyed operations as free
functions (ADR-0011 D2/D3/D4).

A keyed capability is a Layer B ``KvReader`` over the **same key space** whose
``__getitem__`` returns the capability::

    BucketStore                     k -> bytes
    BucketHandles  s3dol.handles(s) k -> ObjectHandle
    BucketUrls     s3dol.urls(s)    k -> presigned URL (str) | None
    BucketInfo     s3dol.info(s)    k -> ObjectInfo

``__getitem__`` is the one thing `dol` maps correctly at every wrapper depth,
so sibling stores are correct *by construction*: no ``inner_most_key``, no
``wrapped_self``, no guard — immune to both the lost-reference hole (D1a) and
the non-``Store``-layer hole (D3a), the only form in ADR-0011 that is.

**The cost, stated plainly** (D2): a user who wraps the data store must wrap
the sibling in parallel::

    s = s3_store('bucket', prefix='p/')
    h = s3dol.handles(s)
    c = KeyCodecs.prefixed('x/')      # a user-applied key codec
    s2, h2 = c(s), c(h)               # wrap BOTH
    h2['f']                           # -> handle on 'p/x/f'; correct at any depth

The accessors therefore derive a sibling from an **unwrapped** store and
**raise** on a wrapped one, naming that remedy — s3dol will not guess a user's
codec chain (that is dol#10).

Free functions (D3) resolve keys with the store held as an argument (alive, so
no weakref registry is consulted). Per D3a they are *more* reliable than
methods, not reliable: a hand-rolled non-``Store`` passthrough layer in the
chain can still defeat the resolution — no docstring here claims "correct at
any wrapper depth"; only the sibling stores may claim that.
"""

from __future__ import annotations

from typing import Iterable, Iterator, Optional

from dol.base import KvReader, Store as DolStore

from s3dol.base import (
    BucketCollection,
    ObjectHandle,
    ObjectInfo,
    _BucketBase,
    _EndpointBase,
    _presign,
)
from s3dol.errors import (
    BucketNotEmpty,
    ConfigurationError,
    CredentialsError,
    S3PartialFailure,
    code_of,
    translate_s3_errors,
    translating_s3_errors,
)

# --------------------------------------------------------------------------- #
# The sibling capability stores (D2)
# --------------------------------------------------------------------------- #


class BucketHandles(BucketCollection, KvReader):
    """``k -> ObjectHandle`` (key bound at construction; zero round-trips)."""

    def __getitem__(self, k) -> ObjectHandle:
        return ObjectHandle(self.bucket, self._id_of_key(k), connection=self.connection)


class BucketUrls(BucketCollection, KvReader):
    """``k -> presigned URL`` (zero object requests; ``None`` when anonymous).
    The canonical spelling of what ``url_for`` shims (ADR-0011 D3b)."""

    def __init__(
        self, *args, expires_in: int = 3600, client_method: str = "get_object", **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.expires_in = expires_in
        self.client_method = client_method

    def __getitem__(self, k) -> Optional[str]:
        return _presign(
            self.connection,
            bucket=self.bucket,
            key=self._id_of_key(k),
            expires_in=self.expires_in,
            client_method=self.client_method,
        )


class BucketInfo(BucketCollection, KvReader):
    """``k -> ObjectInfo`` (one ``HeadObject``)."""

    @translate_s3_errors(operation="HeadObject", key_arg="k")
    def __getitem__(self, k) -> ObjectInfo:
        _id = self._id_of_key(k)
        head = self.client.head_object(Bucket=self.bucket, Key=_id)
        self._mark_bucket_reachable()
        return ObjectInfo(
            key=_id,
            size=head.get("ContentLength"),
            last_modified=head.get("LastModified"),
            etag=head.get("ETag"),
            content_type=head.get("ContentType"),
            storage_class=head.get("StorageClass") or "STANDARD",
            restore_status=head.get("Restore"),
            version_id=head.get("VersionId"),
        )


# --------------------------------------------------------------------------- #
# The accessors: derive a sibling from an UNWRAPPED store; refuse a wrapped one
# --------------------------------------------------------------------------- #

_PARALLEL_WRAP_REMEDY = (
    "derive the sibling from the *unwrapped* store and apply the same "
    "wrapper to both:  s2, sib2 = codec(s), codec(s3dol.{fn}(s)). "
    "s3dol will not guess your codec chain (that is dol#10)."
)


def _leaf_of(store, *, fn: str) -> _BucketBase:
    if isinstance(store, _BucketBase):
        return store
    if isinstance(store, DolStore):
        raise ConfigurationError(
            f"s3dol.{fn}() needs an unwrapped s3dol store, but got a dol "
            f"wrapper ({type(store).__name__}). Returning an unwrapped "
            f"sibling here would silently ignore your key/value transforms — "
            f"instead, {_PARALLEL_WRAP_REMEDY.format(fn=fn)}"
        )
    raise ConfigurationError(
        f"s3dol.{fn}() takes an s3dol bucket store; got {type(store).__name__}."
    )


def handles(store) -> BucketHandles:
    """The sibling store of :class:`ObjectHandle`\\ s: ``handles(s)[k]``."""
    return _leaf_of(store, fn="handles")._make_sibling("handles")


def urls(
    store, *, expires_in: int = 3600, client_method: str = "get_object"
) -> BucketUrls:
    """The sibling store of presigned URLs: ``urls(s)[k]``."""
    return _leaf_of(store, fn="urls")._make_sibling(
        "urls", expires_in=expires_in, client_method=client_method
    )


def info(store) -> BucketInfo:
    """The sibling store of :class:`ObjectInfo`: ``info(s)[k]``."""
    return _leaf_of(store, fn="info")._make_sibling("info")


# --------------------------------------------------------------------------- #
# Free functions (D3/D4)
# --------------------------------------------------------------------------- #


def _outward_key_chain(store) -> list:
    """The wrapper layers outermost-first, ending at the s3dol leaf.
    Refuses (rather than guesses) on a non-``Store`` middle layer — D3a's
    documented hole."""
    chain = [store]
    current = store
    while not isinstance(current, _BucketBase):
        if not isinstance(current, DolStore):
            raise ConfigurationError(
                f"Cannot resolve keys through {type(current).__name__}: it is "
                f"neither an s3dol leaf nor a dol Store wrapper. Free "
                f"functions refuse rather than guess on hand-rolled layers "
                f"(ADR-0011 D3a)."
            )
        inner = current.store
        chain.append(inner)
        current = inner
    return chain


def _map_key_outward(store, wire_relative_key: str) -> str:
    """Map a key from the LEAF's relative key space out to the caller's —
    the inverse walk ``prefixes`` needs (each layer's ``_key_of_id``,
    innermost first). Property-tested round trip:
    ``inner_most_key(store, _map_key_outward(store, k_rel))`` recovers the
    wire key for pure key-codec chains."""
    chain = _outward_key_chain(store)
    key = wire_relative_key
    for layer in reversed(chain[:-1]):  # innermost wrapper -> outermost
        key_of_id = getattr(layer, "_key_of_id", None)
        if callable(key_of_id):
            key = key_of_id(key)
    return key


def sub(store, prefix: str):
    """A store scoped to ``prefix``, **in the caller's key space** (D3).

    Two branches, deliberately different (each documented in the ADR):

    - **unwrapped s3dol store**: ``leaf._with(prefix=...)`` — cheap, keeps
      server-side prefix pushdown, returns the same class;
    - **dol-wrapped store**: composes ``Pipe(filt_iter.prefixes(p),
      KeyCodecs.prefixed(p))`` over the *outer* store (filter FIRST — the only
      safe composition, ADR-0006 §1), which preserves the user's codecs but
      loses pushdown (a full scan filtered client-side; D8 closed this: the
      cheap path is the unwrapped branch) and returns a different type.
    """
    if isinstance(store, _BucketBase):
        return store._with(prefix=f"{store.prefix}{_norm(prefix, store.delimiter)}")
    if isinstance(store, DolStore):
        from dol import KeyCodecs, Pipe, filt_iter

        normalized = _norm(prefix, "/")
        return Pipe(filt_iter.prefixes(normalized), KeyCodecs.prefixed(normalized))(
            store
        )
    raise ConfigurationError(
        f"s3dol.sub() takes an s3dol store or a dol wrapper over one; got "
        f"{type(store).__name__}."
    )


def _norm(prefix: str, delimiter: str) -> str:
    from s3dol.base import _normalize_prefix

    return _normalize_prefix(prefix, delimiter)


def _wire_scope_base(chain: list) -> str:
    """The absolute (wire) prefix the *caller's* key space is rooted at:
    the leaf's own prefix extended by each wrapper layer's contribution
    (``layer._id_of_key('')``, outermost first). Only delimiter-shaped
    contributions are accepted — a codec that maps ``''`` to anything else
    (a suffix codec, say) has no well-defined directory structure and is
    refused loudly rather than listed wrongly."""
    leaf: _BucketBase = chain[-1]
    mapped = ""
    for layer in chain[:-1]:  # outermost wrapper -> innermost
        id_of_key = getattr(layer, "_id_of_key", None)
        if not callable(id_of_key):
            continue
        try:
            mapped = id_of_key(mapped)
        except Exception as error:
            raise ConfigurationError(
                f"s3dol.prefixes(): the wrapper layer {type(layer).__name__} "
                f"cannot map the key-space root ({type(error).__name__}); its "
                f"codec has no well-defined directory structure. List on the "
                f"unwrapped store instead."
            ) from error
        if not isinstance(mapped, str) or (
            mapped and not mapped.endswith(leaf.delimiter)
        ):
            raise ConfigurationError(
                f"s3dol.prefixes(): the wrapper layer {type(layer).__name__} "
                f"maps the key-space root to {mapped!r}, which is not a "
                f"delimiter-terminated scope — its codec has no well-defined "
                f"directory structure. List on the unwrapped store instead."
            )
    return f"{leaf.prefix}{mapped}"


def prefixes(store) -> list:
    """The 'directories' one level under the store's scope, **relative to the
    caller's key space** — one ``ListObjectsV2(Delimiter='/')`` reading
    ``CommonPrefixes`` (which v0 parsed and then called from nowhere;
    ADR-0009). Through a dol wrapper chain, the listing is rooted at the
    caller's scope (each prefix layer's contribution) and results are mapped
    back **outward** (``_key_of_id`` per layer, innermost first — the inverse
    walk `dol` does not provide; ADR-0011 D3). A wrapper whose codec cannot
    express the scope raises — loud, not wrong."""
    chain = _outward_key_chain(store)
    leaf: _BucketBase = chain[-1]
    base = _wire_scope_base(chain)
    with translating_s3_errors(leaf, operation="ListObjectsV2"):
        found = []
        for page in leaf._op_list_pages(delimiter=leaf.delimiter, prefix=base):
            for row in page.get("CommonPrefixes", ()):
                relative = leaf._key_of_id(row["Prefix"])
                found.append(
                    _map_key_outward(store, relative) if len(chain) > 1 else relative
                )
    return found


#: AWS's DeleteObjects cap. moto accepts 1001, so tier 2 can never catch a
#: missing chunker (ADR-0010 §2) — hence the constant is also unit-tested.
DELETE_MANY_CHUNK = 1000


def delete_many(store, keys: Iterable[str]) -> None:
    """Bulk delete (ADR-0010 §2; a free function per ADR-0011 D4 — keyed +
    destructive + delegated is the census's data-destroying shape).

    Chunks at 1000 (AWS's cap), parses the ``Errors`` list out of the HTTP
    200 response, and on partial failure raises one :class:`S3PartialFailure`
    carrying ``.succeeded`` and ``.failures``. Absent keys are reported by S3
    as ``Deleted`` — this function does **not** distinguish them (same
    idempotency as ``del store[k]``).

    Keys are the *caller's* keys: with the store held as an argument the
    chain is resolved via ``dol.inner_most_key`` (alive store — no weakref
    hole). Validate-the-target caveat (D4): the first argument must be an
    s3dol store or a dol wrapper over one.
    """
    from dol import inner_most_key

    chain = _outward_key_chain(store)  # validates the target
    leaf: _BucketBase = chain[-1]
    if leaf.connection.anon:
        raise CredentialsError("Anonymous credentials cannot sign deletes.")
    if len(chain) > 1:
        wire_keys = [inner_most_key(store, k) for k in list(keys)]
        bad = [k for k in wire_keys if not isinstance(k, str)]
        if bad:
            raise ConfigurationError(
                f"Key resolution through the wrapper chain produced non-str "
                f"values ({bad[:3]!r}…): refusing to delete."
            )
    else:
        wire_keys = [leaf._id_of_key(k) for k in keys]

    row = leaf._resolution().preset
    batch_ok = row.capabilities.batch_delete if row is not None else True
    succeeded: list = []
    failures: dict = {}
    with translating_s3_errors(leaf, operation="DeleteObjects"):
        if batch_ok:
            for start in range(0, len(wire_keys), DELETE_MANY_CHUNK):
                chunk = wire_keys[start : start + DELETE_MANY_CHUNK]
                deleted, errors = leaf._op_bulk_delete(chunk)
                succeeded.extend(deleted)
                for err_key, err_code, err_message in errors:  # in an HTTP 200!
                    failures[err_key] = S3PartialFailure(
                        f"{err_code}: {err_message}", succeeded=[], failures={}
                    )
        else:
            # Exact, cheap emulation (GCS & checksum-rejecting providers):
            # identical observable result, only cost differs (ADR-0003 §2).
            from botocore.exceptions import ClientError

            for key in wire_keys:
                try:
                    leaf._op_delete(key)
                    succeeded.append(key)
                except ClientError as error:
                    failures[key] = S3PartialFailure(
                        str(code_of(error)), succeeded=[], failures={}
                    )
    if failures:
        raise S3PartialFailure(
            f"delete_many: {len(failures)} of {len(wire_keys)} deletions "
            f"failed ({len(succeeded)} succeeded).",
            succeeded=succeeded,
            failures=failures,
        )


def delete_bucket(endpoint, name: str, *, force: bool = False) -> None:
    """Delete bucket ``name``. Refuses a non-empty bucket unless
    ``force=True``, in which case the cascade **paginates** — v0 listed one
    page, deleted ≤1000 objects, then failed: a partial, non-idempotent
    destruction (ADR-0010 §3). A free function, not a method (ADR-0011 D4)."""
    if not isinstance(endpoint, _EndpointBase):
        raise ConfigurationError(
            f"s3dol.delete_bucket() takes an EndpointStore/Reader first; got "
            f"{type(endpoint).__name__}."
        )
    if endpoint.connection.anon:
        raise CredentialsError("Anonymous credentials cannot delete buckets.")
    client = endpoint.client
    with translating_s3_errors(endpoint, operation="DeleteBucket", key=name):
        if force:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=name):
                contents = page.get("Contents", ())
                if contents:
                    client.delete_objects(
                        Bucket=name,
                        Delete={"Objects": [{"Key": row["Key"]} for row in contents]},
                    )
        from botocore.exceptions import ClientError

        try:
            client.delete_bucket(Bucket=name)
        except ClientError as error:
            if code_of(error) == "BucketNotEmpty":
                raise BucketNotEmpty(
                    f"Bucket {name!r} is not empty; pass force=True for the "
                    f"explicit, paginated cascade."
                ) from error
            raise
