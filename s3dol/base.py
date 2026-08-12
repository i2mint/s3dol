"""Layer B — close-to-metal stores over one bucket or one endpoint.

Design record: `misc/docs/architecture.md` (the contract tables) on
ADR-0001 (layering; the prefix lives HERE, in the leaf), ADR-0004 (one error
seam), ADR-0005 (write domain / strategies), ADR-0006 (prefix arithmetic and
key validity), ADR-0008 (`__len__` absent by design), ADR-0010 (bucket policy),
ADR-0011 (**no key-taking public methods** — one guarded exception, `url_for`),
ADR-0012 (connection).

The class triads mirror ``dol.filesys`` and ``azuredol``::

    BucketCollection   -> BucketReader   -> BucketStore     (keys: object keys)
    EndpointCollection -> EndpointReader -> EndpointStore   (keys: bucket names)
    ObjectHandle                                            (not a Mapping)

Reader-only classes are *real classes* (a type checker catches
``reader[k] = v``; a read-scoped credential never even attempts a write) —
``dol.mk_read_only`` is non-functional on a dol store (verified, ADR-0001).

**The rule for what may join a class here: nothing that takes a key**
(ADR-0011 D1). `dol` wrappers hand any non-dunder method the outer, unmapped
key, and the standard escape (``wrapped_self``) is itself silently wrong when
nothing references the wrapper. Keyed capabilities are sibling stores
(``s3dol.handles(store)[k]`` — `capabilities.py`); per-object richness lives on
:class:`ObjectHandle` (key bound at construction); everything else is a free
function taking the store first. The single exception is
:meth:`BucketReader.url_for`, kept for ``dol.SupportsUrlFor``, guarded to be
correct-or-loud (D3b). A reflective conformance test enforces the rule (D5).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Mapping, Optional, Union

from dol.base import Collection as DolCollection, KvPersister, KvReader

from s3dol.connection import S3Connection
from s3dol.errors import (
    BucketNotEmpty,
    BucketNotFound,
    ConfigurationError,
    CredentialsError,
    KeyNotValid,
    NotSupported,
    ObjectArchived,
    S3Error,
    classify_client_error,
    code_of,
    status_of,
    translate_s3_errors,
    translating_s3_errors,
)
from s3dol.reads import DEFAULT_READS, ReadStrategy
from s3dol.writes import DEFAULT_WRITES, WriteStrategy

#: S3's real limit is 1024 **UTF-8 bytes** (enforced client-side so moto and
#: AWS agree — moto accepts longer keys; ADR-0006 §4).
MAX_KEY_BYTES = 1024

#: SigV4's presigned-URL lifetime cap (botocore does not enforce it).
MAX_PRESIGN_EXPIRES = 604_800


def _validate_key(k, *, delimiter: str = "/") -> str:
    """Normalise key-validity failures to :class:`KeyNotValid` **before the
    wire** (ADR-0006 §4) — otherwise botocore types escape ``__getitem__``
    (``''`` -> ``ParamValidationError``; a lone surrogate ->
    ``UnicodeEncodeError``; a 1025-byte key -> fine on moto, error on AWS)."""
    if not isinstance(k, str):
        raise KeyNotValid(
            f"S3 keys are str; got {type(k).__name__}. "
            f"(If you need structured keys, put a dol key codec on top.)"
        )
    if k == "":
        raise KeyNotValid("The empty string is not a valid S3 key.")
    try:
        encoded = k.encode("utf-8")
    except UnicodeEncodeError as error:
        raise KeyNotValid(
            f"Key contains code points that cannot encode to UTF-8 "
            f"(lone surrogate?): {k!r}"
        ) from error
    if len(encoded) > MAX_KEY_BYTES:
        raise KeyNotValid(
            f"Key is {len(encoded)} UTF-8 bytes; S3's limit is {MAX_KEY_BYTES}. "
            f"(Checked client-side so moto and AWS agree.)"
        )
    return k


@dataclass(frozen=True)
class ObjectInfo:
    """Object metadata — from ``HeadObject`` or from a LIST row (which already
    carries size/mtime/etag/storage class that v0 threw away; ADR-0009)."""

    key: str
    size: Optional[int] = None
    last_modified: Optional[datetime] = None
    etag: Optional[str] = None
    content_type: Optional[str] = None
    storage_class: Optional[str] = None
    restore_status: Optional[str] = None
    version_id: Optional[str] = None


def _normalize_prefix(prefix: Optional[str], delimiter: str = "/") -> str:
    """The mandatory normalisation, first (ADR-0006 §1): a prefix that does
    not terminate in the delimiter is normalised, never accepted as-is —
    ``prefix='logs'`` would otherwise expose ``logs2/…`` as readable *and
    writable* keys one character away.

    >>> _normalize_prefix('logs'), _normalize_prefix('/logs/'), _normalize_prefix('')
    ('logs/', 'logs/', '')
    """
    if not prefix:
        return ""
    return f"{prefix.strip(delimiter)}{delimiter}"


class _WrapProbe:
    """Descriptor recording dol's wrap-time ``hasattr(store, 'KeysView')``
    probe on the instance, then reporting the attribute absent. Class-level
    access (``getattr(cls, 'KeysView')``) records nothing."""

    def __get__(self, instance, owner=None):
        if instance is not None:
            instance.__dict__["_ever_wrapped"] = True
        raise AttributeError("KeysView")


class _BucketBase:
    """Shared base of the bucket-level classes: owns the normalised prefix,
    the connection, the key arithmetic, and the error context — so those exist
    exactly once (ADR-0011 D2). Plain class; the dol ABCs are mixed in by the
    concrete triad classes."""

    def __init__(
        self,
        bucket: str,
        *,
        connection: Union[S3Connection, None] = None,
        prefix: str = "",
        delimiter: str = "/",
        reads: Optional[ReadStrategy] = None,
        writes: Optional[WriteStrategy] = None,
        on_missing_bucket: str = "assume",
        strict_delete: bool = False,
    ):
        if not isinstance(bucket, str) or not bucket:
            raise ConfigurationError(f"bucket must be a non-empty str; got {bucket!r}")
        if on_missing_bucket not in ("assume", "create", "raise"):
            raise ConfigurationError(
                f"on_missing_bucket must be 'assume', 'create' or 'raise'; got "
                f"{on_missing_bucket!r} (ADR-0010 §1)."
            )
        self.bucket = bucket
        self.connection = connection if connection is not None else S3Connection()
        self.delimiter = delimiter
        self.prefix = _normalize_prefix(prefix, delimiter)
        self.reads = reads if reads is not None else DEFAULT_READS
        self.writes = writes if writes is not None else DEFAULT_WRITES
        self.on_missing_bucket = on_missing_bucket
        self.strict_delete = strict_delete
        self._bucket_reachable = False
        if on_missing_bucket == "raise":
            # The explicit opt-in that performs ONE HeadBucket at construction
            # (documented I/O — the only construction-time request; ADR-0010).
            self._probe_bucket_or_raise()

    # -- key arithmetic (the leaf owns the prefix — ADR-0001/0006) --------- #

    def _id_of_key(self, k: str) -> str:
        return f"{self.prefix}{_validate_key(k, delimiter=self.delimiter)}"

    def _key_of_id(self, _id: str) -> str:
        # RAISE rather than slice a non-matching id: the server-side Prefix
        # makes out-of-scope ids unreachable on the common path; this is the
        # belt to that suspenders, because a provider that ignores Prefix must
        # not silently produce corrupt keys (ADR-0006 §1).
        if self.prefix and not _id.startswith(self.prefix):
            raise KeyNotValid(
                f"Backend returned id {_id!r} outside this store's prefix "
                f"{self.prefix!r} — refusing to relativise it (the provider "
                f"may be ignoring ListObjectsV2's Prefix)."
            )
        return _id[len(self.prefix) :]

    # -- plumbing ---------------------------------------------------------- #

    @property
    def client(self):
        return self.connection.client

    def _error_context(self) -> dict:
        row = self._resolution().preset
        return dict(
            bucket=self.bucket,
            endpoint=self._endpoint_label(),
            overrides=row.error_overrides if row is not None else (),
            deny_means_absent=self.connection.deny_means_absent,
            head_ambiguity_resolver=self._head_ambiguity_resolver,
        )

    def _resolution(self):
        cached = self.__dict__.get("_resolution_cache")
        if cached is None:
            cached = self.connection.resolution()
            self.__dict__["_resolution_cache"] = cached
        return cached

    def _endpoint_label(self) -> Optional[str]:
        built = self.connection.__dict__.get("_client")
        if built is not None:
            return getattr(built.meta, "endpoint_url", None)
        return self._resolution().endpoint_url.value

    def _mark_bucket_reachable(self):
        self._bucket_reachable = True

    def _head_ambiguity_resolver(self) -> bool:
        """Resolve the body-less ``HeadObject`` 404 (ADR-0004 §2): ``True``
        iff the bucket is proven reachable (so the miss is the object's).
        Costs one ``HeadBucket``, once — cached. An auth failure here raises
        ``AccessDenied`` rather than masquerading as a missing bucket."""
        if self._bucket_reachable:
            return True
        from botocore.exceptions import ClientError

        try:
            self._op_head_bucket()
        except ClientError as error:
            classified = classify_client_error(
                "HeadBucket",
                code_of(error),
                status_of(error),
                overrides=self._error_context()["overrides"],
            )
            if classified.kind == "bucket_absent":
                return False
            raise  # auth/transport: never disguise as absence (ADR-0004 §1)
        self._mark_bucket_reachable()
        return True

    def _probe_bucket_or_raise(self):
        if not self._head_ambiguity_resolver():
            raise BucketNotFound(
                f"Bucket {self.bucket!r} not found (on_missing_bucket='raise'). "
                f"Note only ('404', 404) means missing; a 403 would have raised "
                f"AccessDenied naming s3:ListBucket."
            )

    def _refuse_anonymous_write(self, operation: str):
        # botocore offers nothing here — moto even accepts an unsigned PUT —
        # so the refusal is local (ADR-0012 D5).
        if self.connection.anon:
            raise CredentialsError(
                f"{operation} refused: this store is anonymous (anon=True). "
                f"Anonymous credentials cannot sign writes."
            )

    def _sibling_kwargs(self) -> dict:
        """Constructor kwargs for a sibling capability store over the same
        key space (ADR-0011 D2) — the key arithmetic exists once, here."""
        return dict(
            bucket=self.bucket,
            connection=self.connection,
            prefix=self.prefix,
            delimiter=self.delimiter,
            reads=self.reads,
            writes=self.writes,
            on_missing_bucket="assume",
            strict_delete=self.strict_delete,
        )

    def _make_sibling(self, kind: str, **options):
        """Build the sibling capability store of ``kind`` over the same key
        space. Overridable so `s3dol.testing`'s fake hands out fake siblings."""
        from s3dol import capabilities as _capabilities

        sibling_class = {
            "handles": _capabilities.BucketHandles,
            "urls": _capabilities.BucketUrls,
            "info": _capabilities.BucketInfo,
        }[kind]
        return sibling_class(**{**self._sibling_kwargs(), **options})

    # -- the backend protocol (ADR-0002's module-internal seam) ------------ #
    # Layer B talks to the backend ONLY through these. Implemented once by
    # botocore here; s3dol.testing's in-memory fake overrides exactly these
    # (raising synthesized botocore ClientErrors, so the error taxonomy and
    # every guard above this line is exercised identically). Deliberately NOT
    # a public extension point in v1: a protocol with one real implementer
    # would be an abstraction shaped like its implementation.

    def _op_list_pages(
        self, *, delimiter: Optional[str] = None, prefix: Optional[str] = None
    ):
        row = self._resolution().preset
        use_v2 = row.capabilities.list_objects_v2 if row is not None else True
        paginator = self.client.get_paginator(
            "list_objects_v2" if use_v2 else "list_objects"
        )
        kwargs: dict = dict(
            Bucket=self.bucket, Prefix=self.prefix if prefix is None else prefix
        )
        if delimiter:
            kwargs["Delimiter"] = delimiter
        return paginator.paginate(**kwargs)

    def _op_head_object(self, _id: str) -> dict:
        return self.client.head_object(Bucket=self.bucket, Key=_id)

    def _op_head_bucket(self) -> None:
        self.client.head_bucket(Bucket=self.bucket)

    def _op_read(self, _id: str):
        return self.reads(self.client, self.bucket, _id)

    def _op_write(self, _id: str, value) -> None:
        self.writes(self.client, self.bucket, _id, value)

    def _op_delete(self, _id: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=_id)

    def _op_bulk_delete(self, _ids: list) -> tuple:
        """One DeleteObjects call (≤1000 ids). Returns ``(deleted_ids,
        errors)`` where errors is ``[(id, code, message), ...]`` — parsed out
        of the HTTP **200** response (ADR-0010 §2)."""
        response = self.client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": k} for k in _ids], "Quiet": False},
        )
        deleted = [d["Key"] for d in response.get("Deleted", ())]
        errors = [
            (e.get("Key"), e.get("Code"), e.get("Message"))
            for e in response.get("Errors", ())
        ]
        return deleted, errors

    def _op_create_bucket(self) -> None:
        _create_bucket(
            self.client, self.bucket, region=self._resolution().region_name.value
        )

    def _op_presign(
        self, _id: str, *, expires_in: int, client_method: str, **params
    ) -> Optional[str]:
        return _presign(
            self.connection,
            bucket=self.bucket,
            key=_id,
            expires_in=expires_in,
            client_method=client_method,
            **params,
        )

    def _with(self, **overrides) -> "_BucketBase":
        """An explicit-fields sub-store builder (never
        ``type(self)(**self.__dict__)`` — ADR-0001). Overridden prefixes are
        re-normalised by ``__init__``."""
        kwargs = self._sibling_kwargs()
        kwargs["on_missing_bucket"] = self.on_missing_bucket
        kwargs.update(overrides)
        return type(self)(kwargs.pop("bucket"), **kwargs)

    #: The wrap probe (ADR-0011 D1a §3, adapted). dol.base.Store.__init__
    #: probes its wrapped store with ``hasattr(store, 'KeysView')``. On a
    #: plain leaf that reaches ``__getattr__``; on a KvReader-derived leaf it
    #: would be satisfied by MappingViewMixin's class attribute and the probe
    #: would be undetectable — so we shadow it with a descriptor that records
    #: the touch and raises AttributeError. keys()/values()/items() below
    #: bypass it. (Documented false positive: a once-wrapped leaf later used
    #: bare will refuse url_for — accepted, D1a.)
    KeysView = _WrapProbe()

    def keys(self):
        from dol.base import BaseKeysView

        return BaseKeysView(self)

    def values(self):
        from dol.base import BaseValuesView

        return BaseValuesView(self)

    def items(self):
        from dol.base import BaseItemsView

        return BaseItemsView(self)

    def __repr__(self):
        # bucket, prefix, endpoint host, mode. No secrets. (architecture.md)
        bits = [repr(self.bucket)]
        if self.prefix:
            bits.append(f"prefix={self.prefix!r}")
        endpoint = self._endpoint_label()
        if endpoint:
            from urllib.parse import urlsplit

            bits.append(f"endpoint={urlsplit(endpoint).hostname!r}")
        if self.connection.anon:
            bits.append("anon=True")
        return f"{type(self).__name__}({', '.join(bits)})"

    # -- pickling: every s3dol store pickles (ADR-0008 §6) ----------------- #

    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop("_resolution_cache", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


class BucketCollection(_BucketBase, DolCollection):
    """Keys of one bucket (under the prefix). ``Collection`` only —
    iteration and membership, no reads."""

    def __iter__(self) -> Iterator[str]:
        # Lazy, paginated, prefix pushed down to the server. RAISES if the
        # bucket is missing or unlistable — never yields empty (goal 4).
        with translating_s3_errors(self, operation="ListObjectsV2"):
            for page in self._op_list_pages():
                self._mark_bucket_reachable()
                for entry in page.get("Contents", ()):
                    _id = entry["Key"]
                    if _id == self.prefix:
                        # The exact-prefix marker object would relativise to
                        # the forbidden key '' — the scoped view filters it
                        # out; it stays addressable by absolute key on an
                        # unscoped store (ADR-0006 §4).
                        continue
                    yield self._key_of_id(_id)

    @translate_s3_errors(operation="HeadObject", key_arg="k")
    def __contains__(self, k) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._op_head_object(self._id_of_key(k))
        except ClientError as error:
            classified = classify_client_error(
                "HeadObject",
                code_of(error),
                status_of(error),
                overrides=self._error_context()["overrides"],
            )
            if classified.kind == "ambiguous_head":
                if self._head_ambiguity_resolver():
                    return False  # bucket proven reachable: the object is absent
                raise  # bucket absent: let the seam raise BucketNotFound
            if classified.kind == "object_absent":
                return False
            if classified.kind == "access_denied" and self.connection.deny_means_absent:
                return False
            raise  # auth/config/transport: never a silent False (ADR-0004)
        self._mark_bucket_reachable()
        return True

    def __len__(self):
        # Explicit raise: dol's Collection.__len__ counts by iterating, so
        # len(store) would be a full paginated listing and list(store) would
        # cost TWO listings via the length hint (ADR-0008 §cost model).
        raise TypeError(
            f"{type(self).__name__} does not implement __len__ (a length is a "
            f"full paginated listing, and list() would take it as a hint and "
            f"pay twice). Use sum(1 for _ in store) if you accept the scan."
        )


class BucketReader(BucketCollection, KvReader):
    """+ ``__getitem__`` -> bytes (or the read strategy's value type)."""

    @translate_s3_errors(operation="GetObject", key_arg="k")
    def __getitem__(self, k):
        value = self._op_read(self._id_of_key(k))
        self._mark_bucket_reachable()
        return value

    def url_for(
        self,
        k: str,
        *,
        expires_in: int = 3600,
        client_method: str = "get_object",
        **params,
    ) -> Optional[str]:
        """Presigned URL for ``k`` — **the one keyed method** (ADR-0011 D3b),
        kept solely because ``dol.SupportsUrlFor`` requires a *method* and
        ``dol.content_url`` reaches it by ``getattr``. The canonical form is
        ``s3dol.urls(store)[k]``.

        Guarded to be correct-or-loud, never silently wrong:

        - **unwrapped store**: ``k`` is a key in this store's own key space,
          so the URL addresses ``self._id_of_key(k)``. Correct, and this is
          also the contract ``dol.content_url`` calls under — it resolves the
          key through the *outer* layers and stops at the layer owning
          ``url_for``, "because that layer applies its own"
          (``dol/content.py``);
        - **this leaf has ever been wrapped**: **raises**. Two call paths then
          reach this one signature with *different key domains* and nothing
          distinguishes them: ``dol.content_url`` hands over a key already
          mapped through the wrapper layers, while a direct
          ``wrapped.url_for(k)`` (delegation) hands over the raw outer key.
          Applying ``_id_of_key`` is right for the first and wrong for the
          second; re-resolving through the chain is right for the second and
          double-transforms the first. Since a wrong presigned URL addresses a
          *different real object*, the only honest answer is refusal, naming
          the sibling store — which is correct by construction because it goes
          through ``__getitem__`` (ADR-0011 D2/D3b, and see the note below);
        - **anonymous connection**: returns ``None`` (``SupportsUrlFor`` is
          ``Optional[str]``; raising would turn a public-bucket streaming
          fallback into a 500 — ADR-0012 D5). The check reads the built
          client's signature version, never ``spec.anon``.

        .. note:: This refusal is a *finding*, not a limitation we chose.
           ADR-0011 D3b expected a guarded method to serve both paths; it
           cannot, on a leaf that owns its prefix. Reported upstream on
           dol#86/#83 as evidence that a keyed capability *method* is
           unfixable in the general case, and that the sibling-store form is
           the only one correct by construction.
        """
        if self.__dict__.get("_ever_wrapped"):
            raise NotSupported(
                "url_for refused: this store has been wrapped by a dol "
                "key/value wrapper, and the two call paths that reach this "
                "method (dol.content_url, which pre-resolves the key, and a "
                "direct delegated call, which does not) are indistinguishable "
                "here — one of them would get a URL for the WRONG object. Use "
                "the sibling store, which is correct by construction: "
                "s3dol.urls(store)[key] (derive it from the unwrapped store "
                "and apply the same wrapper to both)."
            )
        return self._op_presign(
            self._id_of_key(k),
            expires_in=expires_in,
            client_method=client_method,
            **params,
        )


class BucketStore(BucketReader, KvPersister):
    """+ ``__setitem__`` / ``__delitem__``. The user-facing read-write store."""

    @translate_s3_errors(operation="PutObject", key_arg="k")
    def __setitem__(self, k, v):
        self._refuse_anonymous_write("__setitem__")
        _id = self._id_of_key(k)
        from botocore.exceptions import ClientError

        try:
            self._op_write(_id, v)
        except ClientError as error:
            # 'create' recovers, never probes: attempt, and on NoSuchBucket
            # create-then-retry once (ADR-0010 §1).
            if self.on_missing_bucket == "create" and code_of(error) == "NoSuchBucket":
                self._op_create_bucket()
                self._mark_bucket_reachable()
                self._op_write(_id, v)
            else:
                raise
        self._mark_bucket_reachable()

    @translate_s3_errors(operation="DeleteObject", key_arg="k")
    def __delitem__(self, k):
        self._refuse_anonymous_write("__delitem__")
        _id = self._id_of_key(k)
        if self.strict_delete:
            # The opt-in probe (one extra request, TOCTOU-racy — documented).
            if k not in self:
                raise ObjectNotFoundForDelete(k)
        self._op_delete(_id)
        # Idempotent by default: S3's DeleteObject returns 204 for an absent
        # key; raising KeyError would require a HeadObject probe on EVERY
        # delete (architecture.md contract table).

    # -- ObjectArchived must not be swallowed by the defaulted accessors --- #
    # MutableMapping.setdefault is `try: self[k] except KeyError: self[k] =
    # default` — a Glacier KeyError would let setdefault OVERWRITE the
    # archived object with the default, silently (measured; ADR-0004 §4).

    def setdefault(self, k, default=None):
        try:
            return self[k]
        except ObjectArchived:
            raise
        except KeyError:
            self[k] = default
            return default

    def pop(self, k, *default):
        try:
            value = self[k]
        except ObjectArchived:
            raise
        except KeyError:
            if default:
                return default[0]
            raise
        del self[k]
        return value

    def update(self, other=(), /, **kwargs) -> None:
        # Explicit override for the type: MutableMapping's value type is
        # invariant, so `dst.update(src)` with refs (Filepath/Chunks) fails a
        # type checker without this widened signature (ADR-0005 §3).
        super().update(other, **kwargs)


class ObjectNotFoundForDelete(S3Error, KeyError):
    """Raised by ``strict_delete=True`` deletes of absent keys."""

    def __str__(self):
        return f"Key {self.args[0]!r} was absent (strict_delete=True)."


def _create_bucket(client, bucket: str, *, region: Optional[str]):
    """CreateBucket with the region rules that bite (ADR-0010 §1):
    ``CreateBucketConfiguration`` everywhere except us-east-1 (where passing
    it is an error), and ``BucketAlreadyOwnedByYou`` tolerated (re-create is
    only idempotent in us-east-1)."""
    from botocore.exceptions import ClientError

    kwargs: dict = {"Bucket": bucket}
    if region and region not in ("us-east-1", "auto"):
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        client.create_bucket(**kwargs)
    except ClientError as error:
        if code_of(error) not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def clamp_expires_in(expires_in: int) -> int:
    """Clamp to SigV4's 604800 s cap, warning. botocore does not enforce it —
    it just signs a URL that fails after 7 days."""
    import warnings

    if expires_in > MAX_PRESIGN_EXPIRES:
        warnings.warn(
            f"expires_in={expires_in} exceeds SigV4's hard cap of "
            f"{MAX_PRESIGN_EXPIRES}s (botocore does not enforce it; the URL "
            f"would simply fail after 7 days). Clamping.",
            stacklevel=3,
        )
        return MAX_PRESIGN_EXPIRES
    return expires_in


def _presign(
    connection: S3Connection,
    *,
    bucket: str,
    key: str,
    expires_in: int,
    client_method: str = "get_object",
    **params,
) -> Optional[str]:
    """Shared presigning core: SigV4 (the connection guarantees it), the
    anonymous ``None``, the 604800 s cap, and the credential-expiry clamp."""
    import warnings

    from botocore import UNSIGNED

    client = connection.presign_client
    if client.meta.config.signature_version is UNSIGNED:
        return None  # no capability, not an error (ADR-0012 D5)
    expires_in = clamp_expires_in(expires_in)
    credentials = client._request_signer._credentials
    expiry = getattr(credentials, "_expiry_time", None)
    if expiry is not None:
        from datetime import timezone

        remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
        if 0 < remaining < expires_in:
            warnings.warn(
                f"The signing credentials expire in ~{int(remaining)}s, "
                f"before expires_in={expires_in}: a presigned URL cannot "
                f"outlive the credential that signed it (STS/SSO/instance "
                f"sessions). The URL will die with the session.",
                stacklevel=3,
            )
    return client.generate_presigned_url(
        client_method,
        Params={"Bucket": bucket, "Key": key, **params},
        ExpiresIn=expires_in,
    )


# --------------------------------------------------------------------------- #
# ObjectHandle — the per-object escape hatch (NOT a Mapping)
# --------------------------------------------------------------------------- #


class ObjectHandle:
    """One object, key bound **at construction** — which is what makes it
    immune to the delegation trap (a key codec over the store cannot corrupt a
    key that was resolved before the handle existed; ADR-0011 D1, following
    ``azuredol.BlobHandle``). This is where ranged reads, streaming,
    download-to-disk, metadata and presigned URLs live."""

    def __init__(
        self, bucket: str, key: str, *, connection: Optional[S3Connection] = None
    ):
        self.bucket = bucket
        self.key = key  # the ABSOLUTE (wire) key
        self.connection = connection if connection is not None else S3Connection()

    @property
    def client(self):
        return self.connection.client

    def _error_context(self) -> dict:
        return dict(bucket=self.bucket, endpoint=None)

    def __repr__(self):
        return f"{type(self).__name__}({self.bucket!r}, {self.key!r})"

    def __eq__(self, other):
        return isinstance(other, ObjectHandle) and (
            self.bucket,
            self.key,
            self.connection,
        ) == (other.bucket, other.key, other.connection)

    def __hash__(self):
        return hash((self.bucket, self.key, self.connection))

    @translate_s3_errors(operation="GetObject")
    def read(
        self, *, offset: Optional[int] = None, length: Optional[int] = None
    ) -> bytes:
        """The object's bytes; a ranged read when ``offset``/``length`` given."""
        kwargs: dict = {"Bucket": self.bucket, "Key": self.key}
        if offset is not None or length is not None:
            start = offset or 0
            end = "" if length is None else start + length - 1
            kwargs["Range"] = f"bytes={start}-{end}"
        return self.client.get_object(**kwargs)["Body"].read()

    @translate_s3_errors(operation="GetObject")
    def stream(self, *, chunk_size: int = 8 * 2**20) -> Iterator[bytes]:
        """Lazy chunk iterator over the body."""
        from functools import partial

        body = self.client.get_object(Bucket=self.bucket, Key=self.key)["Body"]
        return iter(partial(body.read, chunk_size), b"")

    @translate_s3_errors(operation="GetObject")
    def download_to(self, path) -> str:
        """Download to disk (never fully in memory). Accepts a path or
        :class:`~s3dol.values.Filepath`; returns the path written."""
        from s3dol.values import Filepath

        target = path.path if isinstance(path, Filepath) else str(path)
        self.client.download_file(self.bucket, self.key, target)
        return target

    @translate_s3_errors(operation="PutObject")
    def write(self, value) -> None:
        """Write the object (same value domain as ``store[k] = v``)."""
        if self.connection.anon:
            raise CredentialsError("Anonymous credentials cannot sign writes.")
        DEFAULT_WRITES(self.client, self.bucket, self.key, value)

    @translate_s3_errors(operation="DeleteObject")
    def delete(self) -> None:
        if self.connection.anon:
            raise CredentialsError("Anonymous credentials cannot sign writes.")
        self.client.delete_object(Bucket=self.bucket, Key=self.key)

    def exists(self) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self.key)
        except ClientError as error:
            if classify_client_error(
                "HeadObject", code_of(error), status_of(error)
            ).kind in ("object_absent", "ambiguous_head"):
                return False
            raise
        return True

    @translate_s3_errors(operation="HeadObject")
    def info(self) -> ObjectInfo:
        """One ``HeadObject`` -> :class:`ObjectInfo`."""
        head = self.client.head_object(Bucket=self.bucket, Key=self.key)
        return ObjectInfo(
            key=self.key,
            size=head.get("ContentLength"),
            last_modified=head.get("LastModified"),
            etag=head.get("ETag"),
            content_type=head.get("ContentType"),
            storage_class=head.get("StorageClass") or "STANDARD",
            restore_status=head.get("Restore"),
            version_id=head.get("VersionId"),
        )

    def url(
        self,
        *,
        expires_in: int = 3600,
        client_method: str = "get_object",
        **params,
    ) -> Optional[str]:
        """Presigned URL. Always SigV4; ``None`` when anonymous; capped at
        SigV4's 604800 s; warns when the signing credential expires sooner
        (the URL cannot outlive it)."""
        return _presign(
            self.connection,
            bucket=self.bucket,
            key=self.key,
            expires_in=expires_in,
            client_method=client_method,
            **params,
        )

    @translate_s3_errors(operation="RestoreObject")
    def restore(self, days: int = 1, *, tier: str = "Standard") -> None:
        """Request restoration of an archived (Glacier) object."""
        self.client.restore_object(
            Bucket=self.bucket,
            Key=self.key,
            RestoreRequest={"Days": days, "GlacierJobParameters": {"Tier": tier}},
        )


# --------------------------------------------------------------------------- #
# Endpoint level (keys = bucket names)
# --------------------------------------------------------------------------- #


class _EndpointBase:
    """Shared base of the endpoint-level classes."""

    def __init__(self, connection: Optional[S3Connection] = None, **bucket_kwargs):
        if connection is not None and not isinstance(connection, S3Connection):
            raise ConfigurationError(
                f"connection must be an S3Connection (got "
                f"{type(connection).__name__}). Live boto3 objects are not "
                f"accepted anywhere in v1 (ADR-0012 D1)."
            )
        self.connection = connection if connection is not None else S3Connection()
        #: kwargs forwarded to the bucket stores this endpoint hands out.
        self.bucket_kwargs = bucket_kwargs

    @property
    def client(self):
        return self.connection.client

    def _error_context(self) -> dict:
        row = self.connection.resolution().preset
        return dict(
            bucket=None,
            endpoint=None,
            overrides=row.error_overrides if row is not None else (),
            deny_means_absent=self.connection.deny_means_absent,
        )

    def __repr__(self):
        endpoint = self.connection.resolution().endpoint_url.value
        inner = "" if not endpoint else f"endpoint={endpoint!r}"
        return f"{type(self).__name__}({inner})"


class EndpointCollection(_EndpointBase, DolCollection):
    """Bucket names at one endpoint."""

    def __iter__(self) -> Iterator[str]:
        with translating_s3_errors(self, operation="ListBuckets"):
            for bucket in self.client.list_buckets().get("Buckets", ()):
                yield bucket["Name"]

    @translate_s3_errors(operation="HeadBucket", key_arg="k")
    def __contains__(self, k) -> bool:
        if not isinstance(k, str) or not k:
            return False
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=k)
        except ClientError as error:
            classified = classify_client_error(
                "HeadBucket",
                code_of(error),
                status_of(error),
                overrides=self._error_context()["overrides"],
            )
            if classified.kind == "bucket_absent":
                return False
            raise  # auth/config: never "the bucket doesn't exist" (ADR-0004)
        return True

    def __len__(self) -> int:
        # Bucket counts are small — an administrative resource (ADR-0008).
        return sum(1 for _ in self)


class EndpointReader(EndpointCollection, KvReader):
    """+ ``__getitem__`` -> :class:`BucketReader`."""

    _bucket_class = BucketReader

    def __getitem__(self, k: str):
        if not isinstance(k, str) or not k:
            raise KeyNotValid(f"Bucket names are non-empty str; got {k!r}")
        return self._bucket_class(k, connection=self.connection, **self.bucket_kwargs)


class EndpointStore(EndpointReader, KvPersister):
    """+ bucket creation/deletion. ``del endpoint[name]`` refuses a non-empty
    bucket; the explicit cascading form is the free function
    ``s3dol.delete_bucket(endpoint, name, force=True)`` (ADR-0010 §3 as
    amended by ADR-0011 D4 — a public keyed destructive *method* is the shape
    the family census found destroying wrong data)."""

    _bucket_class = BucketStore

    @translate_s3_errors(operation="CreateBucket", key_arg="k")
    def __setitem__(self, k: str, v: Mapping):
        """Create bucket ``k`` and populate it from mapping ``v`` (pass ``{}``
        to just create)."""
        if self.connection.anon:
            raise CredentialsError("Anonymous credentials cannot create buckets.")
        if not isinstance(v, Mapping):
            raise TypeError(
                f"EndpointStore values are Mappings of objects to populate the "
                f"bucket with (use {{}} to just create it); got {type(v).__name__}."
            )
        _create_bucket(
            self.client, k, region=self.connection.resolution().region_name.value
        )
        bucket = self[k]
        for key, value in v.items():
            bucket[key] = value

    @translate_s3_errors(operation="DeleteBucket", key_arg="k")
    def __delitem__(self, k: str):
        if self.connection.anon:
            raise CredentialsError("Anonymous credentials cannot delete buckets.")
        from botocore.exceptions import ClientError

        try:
            self.client.delete_bucket(Bucket=k)
        except ClientError as error:
            if code_of(error) == "BucketNotEmpty":
                raise BucketNotEmpty(
                    f"Bucket {k!r} is not empty. Deleting it would destroy "
                    f"its objects; that never happens implicitly. Use "
                    f"s3dol.delete_bucket(endpoint, {k!r}, force=True) for "
                    f"the explicit, paginated cascade."
                ) from error
            raise


class S3Profiles(KvReader):
    """AWS **profile** names -> endpoint stores (the v0 ``S3Dol``, renamed —
    its keys are verifiably profile names, never endpoints; ADR-0007 §1).

    >>> # S3Profiles()['prod']  ->  EndpointStore over the 'prod' profile
    """

    def __init__(self, *, bucket_kwargs: Optional[dict] = None, readonly: bool = False):
        self.bucket_kwargs = bucket_kwargs or {}
        self.readonly = readonly

    def __iter__(self) -> Iterator[str]:
        import botocore.session

        yield from botocore.session.Session().available_profiles

    def __contains__(self, k) -> bool:
        return isinstance(k, str) and k in set(self)

    def __getitem__(self, k: str):
        endpoint_class = EndpointReader if self.readonly else EndpointStore
        return endpoint_class(connection=S3Connection(profile=k), **self.bucket_kwargs)
