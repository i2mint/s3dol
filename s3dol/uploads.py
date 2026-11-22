"""Direct-to-S3 browser uploads: a presigned POST scoped to a key prefix.

A browser can upload straight to S3 without the server relaying bytes: the
server issues a presigned POST form whose signed policy pins

- the **exact key** (server-generated ``<prefix><random token>``: the client
  never picks a filename, so it can neither overwrite a sibling object nor
  smuggle ``..`` segments; a form holds one object, and re-posting with it only
  overwrites that object, so the byte budget cannot be multiplied),
- a **content-length range** (``min_bytes``..``max_bytes``),
- the **content type** (optional), and
- an **expiry** (the 7-day SigV4 cap; an STS session's own expiry still ends it sooner).

Before signing, an optional ``budget`` callable is asked to reserve the bytes
(``budget(max_bytes)`` raises :class:`UploadBudgetExceeded` to refuse, and may return a ``release()`` callable that is called if signing fails), so a
daily per-tenant byte budget is enforced *before* any form exists.
:class:`DailyByteBudget` is the ready-made one; any callable works.

The function is host-agnostic: it takes the store, the prefix and the limits
as arguments, and the web app mounts it behind its own auth::

    form = s3dol.presigned_upload(
        store, key_prefix='tenant-7/', max_bytes=5_000_000, content_type='image/png')
    form.url, form.fields      # hand both to the browser's <form>/FormData

>>> from s3dol.uploads import DailyByteBudget
>>> budget = DailyByteBudget(100, today=lambda: '2026-01-01')
>>> _ = budget(60); _ = budget(40)
>>> budget(1)
Traceback (most recent call last):
  ...
s3dol.uploads.UploadBudgetExceeded: daily upload budget exceeded: 100 of 100 bytes already reserved, 1 requested
"""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from dataclasses import dataclass, field
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from s3dol.base import clamp_expires_in
from s3dol.capabilities import _leaf_of
from s3dol.errors import ConfigurationError, S3Error

#: Default form lifetime (seconds): long enough to pick a file, short enough to
#: not be a standing upload credential.
DEFAULT_UPLOAD_EXPIRES_IN = 900


class UploadBudgetExceeded(S3Error):
    """The tenant's byte budget cannot cover this upload; no form was signed."""


@dataclass(frozen=True)
class PresignedUpload:
    """What the browser needs: POST ``fields`` + a ``file`` part to ``url``."""

    url: str
    fields: dict = field(default_factory=dict)
    key: str = ""
    key_prefix: str = ""
    max_bytes: int = 0


class DailyByteBudget:
    """A per-day byte budget (one instance per tenant); call it to reserve.

    ``ledger`` is any ``MutableMapping[str, int]`` (default: in-memory) keyed
    by day, so persistence is a one-argument swap (a ``dol`` store, a file...).
    ``today`` is the clock seam. Reservation is optimistic (the *maximum* the
    form allows), and a form holds exactly one key, so re-posting cannot multiply the budget.
    """

    def __init__(
        self,
        limit_bytes: int,
        *,
        ledger: Optional[MutableMapping] = None,
        today: Callable[[], str] = lambda: datetime.now(timezone.utc).strftime(
            "%Y-%m-%d"
        ),
    ):
        self.limit_bytes = limit_bytes
        self.ledger = {} if ledger is None else ledger
        self.today = today
        self._lock = (
            threading.Lock()
        )  # threads only; a shared ledger needs its own atomicity

    def __call__(self, nbytes: int) -> Callable[[], None]:
        """Reserve ``nbytes``; returns a ``release()`` that gives them back."""
        day = self.today()
        with self._lock:
            used = self.ledger.get(day, 0)
            if used + nbytes > self.limit_bytes:
                raise UploadBudgetExceeded(
                    f"daily upload budget exceeded: {used} of {self.limit_bytes} "
                    f"bytes already reserved, {nbytes} requested"
                )
            self.ledger[day] = used + nbytes

        def release() -> None:
            with self._lock:
                self.ledger[day] = max(0, self.ledger.get(day, 0) - nbytes)

        return release


def _check_prefix(key_prefix: str, delimiter: str) -> None:
    if not key_prefix.endswith(delimiter) or key_prefix.startswith(delimiter):
        raise ConfigurationError(
            f"key_prefix must end with {delimiter!r} (so 'tenant-7{delimiter}' "
            f"cannot match 'tenant-77{delimiter}') and not start with it; got "
            f"{key_prefix!r}."
        )
    if "$" in key_prefix or any(
        seg in (".", "..") for seg in key_prefix.split(delimiter)
    ):
        raise ConfigurationError(
            f"key_prefix may not contain '$' or '.'/'..' segments; got {key_prefix!r}."
        )


def presigned_upload(
    store,
    *,
    key_prefix: str,
    max_bytes: int,
    content_type: Optional[str],
    min_bytes: int = 0,
    expires_in: int = DEFAULT_UPLOAD_EXPIRES_IN,
    budget: Optional[Callable[[int], Optional[Callable[[], None]]]] = None,
) -> Optional[PresignedUpload]:
    """A presigned POST form for ONE new object under ``key_prefix``.

    ``content_type`` is required: pass the one type to accept, or ``None`` to
    explicitly accept any (then a holder can store ``text/html``: do not serve
    such objects from a trusted origin).

    ``key_prefix`` must end with the store's delimiter (``tenant-7/`` cannot
    match ``tenant-77/``) and is in the *store's* key space (the store's own
    prefix is prepended, like every other s3dol key). The object key is
    server-generated (``PresignedUpload.key``). Returns ``None`` for an
    anonymous store (no signing capability, as :func:`s3dol.urls` does).

    Raises :class:`ConfigurationError` for a bad prefix, size range or
    ``expires_in``, and :class:`UploadBudgetExceeded` if ``budget`` refuses.
    """
    from botocore import UNSIGNED

    leaf = _leaf_of(store, fn="presigned_upload")
    _check_prefix(key_prefix, leaf.delimiter)
    if not (0 <= min_bytes <= max_bytes) or max_bytes <= 0:
        raise ConfigurationError(
            f"need 0 <= min_bytes <= max_bytes and max_bytes > 0; got "
            f"min_bytes={min_bytes}, max_bytes={max_bytes}"
        )
    if (
        isinstance(expires_in, bool)
        or not isinstance(expires_in, int)
        or expires_in <= 0
    ):
        raise ConfigurationError(
            f"expires_in must be a positive int, got {expires_in!r}"
        )
    client = leaf.connection.presign_client
    if client.meta.config.signature_version is UNSIGNED:
        return None
    wire_prefix = leaf._id_of_key(key_prefix)
    key = wire_prefix + uuid.uuid4().hex
    release = budget(max_bytes) if budget is not None else None  # refuse BEFORE signing
    fields: dict = {}
    conditions: list = [["content-length-range", min_bytes, max_bytes]]
    if content_type is not None:
        fields["Content-Type"] = content_type
        conditions.append({"Content-Type": content_type})
    try:
        post = client.generate_presigned_post(
            leaf.bucket,
            key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=clamp_expires_in(expires_in),
        )
    except BaseException:
        if callable(release):
            release()
        raise
    return PresignedUpload(
        url=post["url"],
        fields=post["fields"],
        key=key,
        key_prefix=wire_prefix,
        max_bytes=max_bytes,
    )
