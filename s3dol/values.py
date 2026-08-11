"""Value refs and the write domain (ADR-0005).

The write domain is a small, closed, explicitly-named union::

    BytesSource = bytes | bytearray | memoryview | BinaryIO | Filepath | Chunks | Streamable

``Filepath``/``Chunks``/``Streamable`` are tiny frozen dataclasses — value
*refs*, not values (h5py's ``SoftLink``/``ExternalLink`` are the precedent:
purpose-built objects meaning "the value is a reference to content elsewhere",
assigned through an ordinary ``__setitem__``).

The law that makes the asymmetric read/write types safe (ADR-0005 §3):

- **N1 (canonical form)**: ``normalize: WriteDomain -> bytes`` is total on
  ``bytes | bytearray | memoryview | Filepath`` and **one-shot** on
  ``BinaryIO | Chunks | Streamable`` — a stream ref is consumed by its first
  write; assigning the same ref twice is a documented error, not a second copy.
- **N3 (the honest invariant)**: ``s[k] = v  =>  s[k] == normalize(v)`` — for
  the re-readable half.

``str`` is rejected, loudly, on **decidability** (§2): ``s['config'] = '{"a":1}'``
and ``s['video'] = '/tmp/big.mp4'`` are both overwhelmingly plausible, and any
rule distinguishing them is a latent data-corruption bug. One word —
``Filepath(p)`` — removes the ambiguity. Bare ``os.PathLike`` is rejected too:
accepting it while rejecting ``str`` would be a confusing half-rule.

>>> reject_str_value('{"a": 1}')
Traceback (most recent call last):
...
TypeError: ...
"""

from __future__ import annotations

import io
import os
from dataclasses import dataclass, field
from functools import singledispatch
from typing import BinaryIO, Iterable, Union

# --------------------------------------------------------------------------- #
# The refs
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Filepath:
    """The value *is the content of this file*. Re-readable (N1-total)."""

    path: str

    def __post_init__(self):
        # A Path is fine; normalise it to str so the ref stays picklable/hashable.
        object.__setattr__(self, "path", os.fspath(self.path))


@dataclass(frozen=True)
class Chunks:
    """The value is the concatenation of an iterable of ``bytes`` chunks.
    **One-shot** unless the iterable is re-iterable (a list is; a generator is
    not — and a consumed generator writes ``b''``, which is why assigning the
    same one-shot ref twice is a documented error)."""

    chunks: Iterable[bytes]
    consumed: list = field(default_factory=list, compare=False, repr=False)

    def _consume_once(self):
        if self.consumed:
            raise ValueError(
                "This Chunks ref was already written once. A stream ref is "
                "one-shot (ADR-0005 N1): re-create the ref (or pass a list of "
                "chunks, which is re-readable)."
            )
        self.consumed.append(True)
        return self.chunks


@dataclass(frozen=True)
class Streamable:
    """The value is whatever a zero-arg factory's stream yields — for sources
    that must be (re)opened at write time, e.g. an HTTP download. The factory
    is called once per write, so unlike :class:`Chunks` this ref IS safely
    re-writable (each write gets a fresh stream)."""

    open_stream: object  # zero-arg callable -> BinaryIO | Iterable[bytes]


#: The static write domain (``BinaryIO`` is for mypy; runtime dispatch uses
#: ``io.IOBase`` — see :func:`as_fileobj`).
BytesSource = Union[
    bytes, bytearray, memoryview, BinaryIO, Filepath, Chunks, Streamable
]


def reject_str_value(v):
    """The one error message for the ``str`` ambiguity (ADR-0005 §2)."""
    raise TypeError(
        "A str value is ambiguous here: did you mean its utf-8 bytes, or a "
        "filepath?\n"
        "  content : s[k] = v.encode()   (or wrap the store: "
        "dol.wrap_kvs(store, data_of_obj=lambda s: s.encode(), "
        "obj_of_data=lambda b: b.decode()))\n"
        "  filepath: s[k] = s3dol.Filepath(v)"
    )


class _IterableFileObj(io.RawIOBase):
    """A minimal read-only file object over an iterable of bytes chunks."""

    def __init__(self, chunks: Iterable[bytes]):
        self._iterator = iter(chunks)
        self._buffer = b""

    def readable(self):
        return True

    def readinto(self, target):
        while len(self._buffer) < len(target):
            try:
                self._buffer += next(self._iterator)
            except StopIteration:
                break
        n = min(len(target), len(self._buffer))
        target[:n] = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return n


# --------------------------------------------------------------------------- #
# as_fileobj — the open extension point (ADR-0005 §1)
# --------------------------------------------------------------------------- #


@singledispatch
def as_fileobj(v) -> BinaryIO:
    """A binary file object for any member of the write domain.

    ``functools.singledispatch``: the union is open for extension (users
    register their own ref types) and closed for modification. Everything
    non-``bytes`` routes through ``upload_fileobj`` downstream, so seekability
    is s3transfer's problem.
    """
    if isinstance(v, str):
        reject_str_value(v)
    if isinstance(v, os.PathLike):
        raise TypeError(
            f"Got a bare path object ({type(v).__name__}). Wrap it: "
            f"s3dol.Filepath({v!r}) — one word, zero ambiguity (ADR-0005 §2)."
        )
    raise TypeError(
        f"Cannot write a {type(v).__name__} to an s3dol store. Accepted: "
        f"bytes, bytearray, memoryview, an open binary file, "
        f"s3dol.Filepath(path), s3dol.Chunks(iterable_of_bytes), or "
        f"s3dol.Streamable(open_stream). Extend with "
        f"s3dol.values.as_fileobj.register(YourType)."
    )


@as_fileobj.register(bytes)
def _(v: bytes):
    return io.BytesIO(v)


@as_fileobj.register(bytearray)
def _(v: bytearray):
    return io.BytesIO(bytes(v))


@as_fileobj.register(memoryview)
def _(v: memoryview):
    return io.BytesIO(bytes(v))


# Register on io.IOBase, NEVER typing.BinaryIO: @register(BinaryIO) is accepted
# at definition time and then never fires — io.BytesIO is not in
# typing.BinaryIO's MRO (measured, ADR-0005 §1). io.IOBase covers BytesIO,
# BufferedReader, botocore.response.StreamingBody, SpooledTemporaryFile and
# urllib3 responses.
@as_fileobj.register(io.IOBase)
def _(v: io.IOBase):
    return v


@as_fileobj.register(Filepath)
def _(v: Filepath):
    return open(v.path, "rb")


@as_fileobj.register(Chunks)
def _(v: Chunks):
    return _IterableFileObj(v._consume_once())


@as_fileobj.register(Streamable)
def _(v: Streamable):
    stream = v.open_stream()
    if isinstance(stream, io.IOBase):
        return stream
    return _IterableFileObj(stream)


def is_bytes_like(v) -> bool:
    """Whether ``v`` is an in-memory bytes value (the ``PutObject`` fast path)."""
    return isinstance(v, (bytes, bytearray, memoryview))
