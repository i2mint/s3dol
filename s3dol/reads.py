"""Read strategies (ADR-0005 §6): what ``s[k]`` returns.

``s[k]`` returns ``bytes`` — always, under the default strategy, because the
N1 canonical form demands it. ``stream_reads`` is the documented *runtime*
variation (the store's values become chunk iterators); it does not change the
class's static value type — honest typing there would need a generic store,
which v1 does not do.

Ranged reads and download-to-disk are per-object concerns and live on
``ObjectHandle`` (``handle.read(offset=, length=)``, ``handle.download_to``),
not here — a store-level "ranged" strategy would need a per-key range, which a
Mapping cannot express.

Strategies are frozen dataclasses, not closures, because every s3dol store
must pickle (ADR-0008 §6) and a store carries its strategy.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Any, Iterator, Protocol


class ReadStrategy(Protocol):
    def __call__(self, client: Any, bucket: str, key: str) -> Any: ...


#: Default streaming chunk size (8 MiB, matching the write side).
DEFAULT_CHUNK_SIZE = 8 * 2**20


@dataclass(frozen=True)
class BytesReads:
    """The default: one ``GetObject``, fully read -> ``bytes``."""

    def __call__(self, client, bucket: str, key: str) -> bytes:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()


@dataclass(frozen=True)
class StreamReads:
    """Values become lazy chunk iterators (the body streams; nothing is held
    in memory). The ``GetObject`` itself — and its ``KeyError`` — still
    happens eagerly at ``s[k]``; only the byte transfer is lazy."""

    chunk_size: int = DEFAULT_CHUNK_SIZE

    def __call__(self, client, bucket: str, key: str) -> Iterator[bytes]:
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        return iter(partial(body.read, self.chunk_size), b"")


def bytes_reads() -> BytesReads:
    return BytesReads()


def stream_reads(**kwargs) -> StreamReads:
    return StreamReads(**kwargs)


DEFAULT_READS = BytesReads()
