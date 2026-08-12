"""Write strategies (ADR-0005 §5): how ``s[k] = v`` reaches the wire.

A strategy is a callable ``(client, bucket, key, value) -> None``, injected at
store construction — ``__setitem__`` stays ``__setitem__``; *how* it uploads is
a constructor parameter. This is the answer to "infra-specific optimisation
without polluting the interface": multipart is *transport*, not storage-model.

Strategies are frozen dataclasses, not closures, because every s3dol store
must pickle (ADR-0008 §6 / ADR-0012 D1) and a store carries its strategy.

Every non-``bytes`` source routes through ``upload_fileobj``/``TransferManager``,
never ``PutObject`` — so seekability is s3transfer's problem, and the size
threshold is never consulted for a non-seekable source (where it is undecidable
anyway). Note ``io.UnsupportedOperation`` and botocore's
``UnseekableStreamError`` raise *before the request is built*, so they reach the
caller directly (they are not ``ClientError``s and the error seam does not —
and must not — convert them to ``KeyError``).

>>> strategy = transfer_writes(multipart_threshold=16 * 2**20)
>>> import pickle; pickle.loads(pickle.dumps(strategy)) == strategy
True
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from s3dol.values import as_fileobj, is_bytes_like, reject_str_value


class WriteStrategy(Protocol):
    def __call__(self, client: Any, bucket: str, key: str, value: Any) -> None: ...


#: boto3's own default multipart threshold (8 MiB).
DEFAULT_MULTIPART_THRESHOLD = 8 * 2**20


@dataclass(frozen=True)
class TransferWrites:
    """The default: small in-memory values take a single ``PutObject``; file
    objects and refs go through ``upload_fileobj`` (which goes multipart above
    the threshold, transparently). The overhead on small objects is one branch.

    Providers that cap multipart parts (Scaleway: 1000 — see the preset's
    ``Capabilities.max_multipart_parts``) may need a larger
    ``multipart_chunksize`` for very large objects: parts ≈ size / chunksize.
    """

    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    multipart_chunksize: int = DEFAULT_MULTIPART_THRESHOLD
    max_concurrency: int = 10
    use_threads: bool = True

    def __call__(self, client, bucket: str, key: str, value) -> None:
        if isinstance(value, str):
            reject_str_value(value)
        if is_bytes_like(value) and len(value) < self.multipart_threshold:
            client.put_object(Bucket=bucket, Key=key, Body=bytes(value))
            return
        from boto3.s3.transfer import TransferConfig

        config = TransferConfig(
            multipart_threshold=self.multipart_threshold,
            multipart_chunksize=self.multipart_chunksize,
            max_concurrency=self.max_concurrency,
            use_threads=self.use_threads,
        )
        with as_fileobj(value) as fileobj:
            client.upload_fileobj(fileobj, bucket, key, Config=config)


@dataclass(frozen=True)
class SimpleWrites:
    """Everything through a single ``PutObject`` — refs are materialised in
    memory first. For small-object workloads and providers with no multipart
    at all (``Capabilities.multipart=False``)."""

    def __call__(self, client, bucket: str, key: str, value) -> None:
        if isinstance(value, str):
            reject_str_value(value)
        if not is_bytes_like(value):
            with as_fileobj(value) as fileobj:
                value = fileobj.read()
        client.put_object(Bucket=bucket, Key=key, Body=bytes(value))


def transfer_writes(**kwargs) -> TransferWrites:
    """Factory spelling of :class:`TransferWrites` (the constructor-injection
    idiom of ADR-0005 §5: ``BucketStore(..., writes=transfer_writes(...))``)."""
    return TransferWrites(**kwargs)


def simple_writes() -> SimpleWrites:
    return SimpleWrites()


DEFAULT_WRITES = TransferWrites()
