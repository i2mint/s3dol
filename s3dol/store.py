"""COMPAT SHIM — the v0 ``S3Store``, forwarded onto the v1 machinery.

Removed in v2. This module **must survive as an importable module**: external
dependents do ``from s3dol.store import S3Store`` (fully qualified), not
``from s3dol import S3Store`` (ADR-0007 §3).

The shim keeps v0's exact signature (``bucket_name`` positional-or-keyword,
``path=`` not renamed, the ``make_bucket`` tri-state, and
``is_supabase_endpoint=`` which an early draft forgot) and is **a fix delivery
mechanism**: callers get the corrected endpoint/credential resolution without
changing a line. Three v0 behaviours a dependent's *passing tests* rely on are
deliberately preserved here (and only here — ADR-0007 §4):

- ``store[k] = 'a str'`` is accepted (encoded utf-8, with a
  ``DeprecationWarning``);
- ``store['folder/']`` returns a sub-store;
- ``del store[absent]`` silently succeeds (v1 deletes are idempotent anyway).

And one v0 behaviour is preserved on the Supabase branch only: the read-side
de-chunker for objects that v0's misconfigured writes stored with their
``aws-chunked`` HTTP framing verbatim (ADR-0003 §3). It is implemented
*correctly* here (multi-chunk; full-parse-or-passthrough) where v0's truncated
multi-chunk payloads. Per ADR-0003's sequencing rule the codec is not deleted
until a repair path ships — :func:`detect_chunked_framing` is that path's
diagnostic.

When v1 resolves a call differently than v0 *effectively* did (v0 dropped an
explicit ``endpoint_url`` whenever env credentials were set, and let env
credentials override explicit ones), the shim warns with
:class:`~s3dol.errors.S3DolResolutionChanged` — the ADR-0012 D7 differential.
This module and that warning die together in v2.
"""

from __future__ import annotations

import os
import re
import warnings
from typing import MutableMapping, Optional

from dol.base import KvPersister

from s3dol._diagnose import _spec_from_v0_args, _v0_mangle
from s3dol.base import BucketStore
from s3dol.connection import resolve
from s3dol.errors import S3DolResolutionChanged

# --------------------------------------------------------------------------- #
# The corrected aws-chunked de-framer (Supabase legacy objects)
# --------------------------------------------------------------------------- #

_CHUNK_HEAD = re.compile(rb"^[0-9a-fA-F]{1,16}(;|\r\n)")


def dechunk_aws_chunked(data: bytes) -> bytes:
    """Undo verbatim-persisted ``aws-chunked`` framing, if present.

    Full-parse-or-passthrough: either the ENTIRE payload parses as chunked
    framing (every chunk header, every CRLF, through the terminal ``0`` chunk)
    and the concatenated chunks are returned, or the payload is returned
    untouched. v0's parser truncated any payload larger than one chunk and
    misfired on payloads that merely *started* with hex digits; requiring a
    complete valid parse removes both failure modes (a random payload that
    happens to be a complete chunked document is astronomically unlikely).

    >>> dechunk_aws_chunked(b'5\\r\\nhello\\r\\n0\\r\\n\\r\\n')
    b'hello'
    >>> framed = b'3\\r\\nabc\\r\\n2\\r\\nde\\r\\n0\\r\\nx-amz-checksum-crc32:AAAA\\r\\n\\r\\n'
    >>> dechunk_aws_chunked(framed)  # multi-chunk (v0 truncated this)
    b'abcde'
    >>> dechunk_aws_chunked(b'cafe is hex-ish but not framed')
    b'cafe is hex-ish but not framed'
    """
    if not _CHUNK_HEAD.match(data):
        return data
    parts = []
    position = 0
    while True:
        crlf = data.find(b"\r\n", position)
        if crlf < 0:
            return data
        size_token = data[position:crlf].split(b";")[0]
        try:
            size = int(size_token, 16)
        except ValueError:
            return data
        body_start = crlf + 2
        if size == 0:
            # Trailer (e.g. x-amz-checksum-crc32:…) then blank line — accept
            # anything through the final CRLFCRLF (or end of payload).
            rest = data[body_start:]
            if rest and not rest.endswith(b"\r\n"):
                return data
            return b"".join(parts)
        chunk = data[body_start : body_start + size]
        if (
            len(chunk) < size
            or data[body_start + size : body_start + size + 2] != b"\r\n"
        ):
            return data
        parts.append(chunk)
        position = body_start + size + 2


def detect_chunked_framing(store, *, sample: int = 20) -> list:
    """Which of the first ``sample`` keys hold verbatim ``aws-chunked``
    framing (objects corrupted by v0 Supabase writes). The repair path:
    ``store[k] = dechunk_aws_chunked(raw[k])`` per flagged key, reading raw
    bytes through an un-codec'd store."""
    flagged = []
    for i, key in enumerate(store):
        if i >= sample:
            break
        raw = store[key]
        if isinstance(raw, bytes) and dechunk_aws_chunked(raw) != raw:
            flagged.append(key)
    return flagged


# --------------------------------------------------------------------------- #
# The shim store: v0 conveniences over a v1 BucketStore
# --------------------------------------------------------------------------- #


class _V0CompatStore(KvPersister):
    """The v0 ergonomics (str writes, ``'folder/'`` sub-stores, counting
    ``len()``) over a v1 :class:`BucketStore`. Shim-only; not public API."""

    def __init__(self, inner: BucketStore, *, dechunk: bool = False):
        self._inner = inner
        self._dechunk = dechunk

    def __getitem__(self, k):
        if isinstance(k, str) and k.endswith(self._inner.delimiter):
            return type(self)(
                self._inner._with(prefix=f"{self._inner.prefix}{k}"),
                dechunk=self._dechunk,
            )
        value = self._inner[k]
        if self._dechunk and isinstance(value, bytes):
            value = dechunk_aws_chunked(value)
        return value

    def __setitem__(self, k, v):
        if isinstance(v, str):
            warnings.warn(
                "Writing a str to an s3dol store is deprecated (removed in "
                "v2): it will be stored as its utf-8 bytes. Be explicit: "
                "s[k] = v.encode() — or s3dol.Filepath(v) if you meant a "
                "file's contents.",
                DeprecationWarning,
                stacklevel=2,
            )
            v = v.encode()
        self._inner[k] = v

    def __delitem__(self, k):
        # v1 deletes are idempotent (no probe), which is exactly v0's
        # "del store[absent] silently succeeds" — nothing to shim.
        del self._inner[k]

    def __iter__(self):
        return iter(self._inner)

    def __contains__(self, k):
        if isinstance(k, str) and k.endswith(self._inner.delimiter):
            # v0: `'d/' in s` answered via HeadObject on the marker; keep it.
            return self._inner.__contains__(k) if k else False
        return k in self._inner

    def __len__(self):
        # v0's len() worked (by counting — two listings via the length hint).
        # The v1 store raises; the shim keeps v0's behaviour, cost and all.
        return sum(1 for _ in self._inner)

    def url_for(self, k: str, **kwargs) -> Optional[str]:
        # lacing calls store.url_for(k) on the v0 store; forward to the v1
        # guarded shim (prefix-aware, SigV4 — this is what closes issue #10).
        return self._inner.url_for(k, **kwargs)

    def __repr__(self):
        return f"S3Store({self._inner!r})"


def _warn_if_resolution_changed(v0_args: dict) -> None:
    """The ADR-0012 D7 runtime differential: one v0 argument-mangler run
    through v1's own resolver twice; warn iff the outcomes differ. Cost when
    env credentials are absent: two environ lookups."""
    branch, effective = _v0_mangle(v0_args, os.environ)
    if effective is None or effective == v0_args:
        return  # excluded branches, or v0 didn't mangle anything
    try:
        environ = dict(os.environ)
        v1_now = resolve(_spec_from_v0_args(v0_args), environ)
        v0_was = resolve(_spec_from_v0_args(effective), environ)
    except Exception:
        return  # the differential must never break the construction
    moved = [
        axis
        for axis in ("endpoint_url", "region_name")
        if getattr(v1_now, axis).value != getattr(v0_was, axis).value
    ]
    if v1_now.credential_provenance != v0_was.credential_provenance:
        moved.append("credential identity")
    if moved:
        warnings.warn(
            f"s3dol v1 resolves this S3Store(...) call differently than v0 "
            f"did (v0 branch: {branch}; changed: {', '.join(moved)}). v0 was "
            f"silently discarding an explicit argument; v1 honours it. Run "
            f"s3dol.diagnose(**your_S3Store_kwargs) for the full table.",
            S3DolResolutionChanged,
            stacklevel=3,
        )


def S3Store(
    bucket_name: str,
    *,
    make_bucket: Optional[bool] = None,
    path: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    region_name: Optional[str] = None,
    profile_name: Optional[str] = None,
    skip_bucket_check: Optional[bool] = None,
    is_supabase_endpoint: Optional[bool] = None,
) -> MutableMapping:
    """DEPRECATED — the v0 entry point, kept working until v2.

    Use :func:`s3dol.s3_store` instead: ``s3_store(bucket, prefix=...)``.

    Signature is v0's exactly (``path=`` not renamed; ``make_bucket``
    tri-state maps ``True -> on_missing_bucket='create'``, ``False ->
    'raise'``, ``None -> 'assume'``). Behaviour differences vs v0 are bug
    fixes only (explicit endpoint/credentials honoured; listing errors raise
    instead of returning ``[]``; writes no longer create buckets unless asked)
    and announce themselves via ``S3DolResolutionChanged`` when they change
    this call's resolution.
    """
    warnings.warn(
        "s3dol.store.S3Store is deprecated and will be removed in s3dol 2.0: "
        "use s3dol.s3_store(bucket, prefix=..., ...) (note: prefix=, not "
        "path=).",
        DeprecationWarning,
        stacklevel=2,
    )
    v0_args = dict(
        bucket_name=bucket_name,
        make_bucket=make_bucket,
        path=path,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        endpoint_url=endpoint_url,
        region_name=region_name,
        profile_name=profile_name,
        skip_bucket_check=skip_bucket_check,
        is_supabase_endpoint=is_supabase_endpoint,
    )
    _warn_if_resolution_changed(v0_args)

    if is_supabase_endpoint is None:
        is_supabase_endpoint = bool(endpoint_url and ".supabase." in endpoint_url)

    connection = _spec_from_v0_args(v0_args)
    if is_supabase_endpoint:
        # Route through the preset row: path addressing + checksum
        # when_required — the WRITE-side cure for what the read-side
        # de-chunker treats (ADR-0003 §3).
        from s3dol.connection import S3Connection

        connection = S3Connection(
            preset="supabase",
            endpoint_url=connection.endpoint_url,
            region_name=connection.region_name,
            profile=connection.profile,
            credentials=connection.credentials,
        )
    on_missing_bucket = {True: "create", False: "raise", None: "assume"}[make_bucket]
    inner = BucketStore(
        bucket_name,
        connection=connection,
        prefix=path or "",
        on_missing_bucket=on_missing_bucket,
    )
    return _V0CompatStore(inner, dechunk=is_supabase_endpoint)
