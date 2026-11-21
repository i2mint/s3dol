# s3dol.values

Value refs and the write domain (ADR-0005).

The write domain is a small, closed, explicitly-named union:

```default
BytesSource = bytes | bytearray | memoryview | BinaryIO | Filepath | Chunks | Streamable
```

`Filepath`/`Chunks`/`Streamable` are tiny frozen dataclasses — value
*refs*, not values (h5py’s `SoftLink`/`ExternalLink` are the precedent:
purpose-built objects meaning “the value is a reference to content elsewhere”,
assigned through an ordinary `__setitem__`).

The law that makes the asymmetric read/write types safe (ADR-0005 §3):

- **N1 (canonical form)**: `normalize: WriteDomain -> bytes` is total on
  `bytes | bytearray | memoryview | Filepath` and **one-shot** on
  `BinaryIO | Chunks | Streamable` — a stream ref is consumed by its first
  write; assigning the same ref twice is a documented error, not a second copy.
- **N3 (the honest invariant)**: `s[k] = v  =>  s[k] == normalize(v)` — for
  the re-readable half.

`str` is rejected, loudly, on **decidability** (§2): `s['config'] = '{"a":1}'`
and `s['video'] = '/tmp/big.mp4'` are both overwhelmingly plausible, and any
rule distinguishing them is a latent data-corruption bug. One word —
`Filepath(p)` — removes the ambiguity. Bare `os.PathLike` is rejected too:
accepting it while rejecting `str` would be a confusing half-rule.

```pycon
>>> reject_str_value('{"a": 1}')
Traceback (most recent call last):
...
TypeError: ...
```

### Module Attributes

| [`BytesSource`](#s3dol.values.BytesSource)   | The static write domain (`BinaryIO` is for mypy; runtime dispatch uses `io.IOBase` — see [`as_fileobj()`](#s3dol.values.as_fileobj)).   |
|----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|

### Functions

| [`as_fileobj`](#s3dol.values.as_fileobj)()        | A binary file object for any member of the write domain.             |
|----------------------------------------------------------------------|----------------------------------------------------------------------|
| [`is_bytes_like`](#s3dol.values.is_bytes_like)(v)    | Whether `v` is an in-memory bytes value (the `PutObject` fast path). |
| [`reject_str_value`](#s3dol.values.reject_str_value)(v) | The one error message for the `str` ambiguity (ADR-0005 §2).         |

### Classes

| [`Chunks`](#s3dol.values.Chunks)(chunks[, consumed])   | The value is the concatenation of an iterable of `bytes` chunks.                                                                     |
|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [`Filepath`](#s3dol.values.Filepath)(path)               | The value *is the content of this file*.                                                                                             |
| [`Streamable`](#s3dol.values.Streamable)(open_stream)      | The value is whatever a zero-arg factory's stream yields — for sources that must be (re)opened at write time, e.g. an HTTP download. |

### s3dol.values.BytesSource

The static write domain (`BinaryIO` is for mypy; runtime dispatch uses
`io.IOBase` — see [`as_fileobj()`](#s3dol.values.as_fileobj)).

alias of [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes) | [`bytearray`](https://docs.python.org/3/builtins/stdtypes.html#bytearray) | [`memoryview`](https://docs.python.org/3/builtins/stdtypes.html#memoryview) | [`BinaryIO`](https://docs.python.org/3/library/typing.html#typing.BinaryIO) | [`Filepath`](#s3dol.values.Filepath) | [`Chunks`](#s3dol.values.Chunks) | [`Streamable`](#s3dol.values.Streamable)

### *class* s3dol.values.Chunks(chunks, consumed=list())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value is the concatenation of an iterable of `bytes` chunks.
**One-shot** unless the iterable is re-iterable (a list is; a generator is
not — and a consumed generator writes `b''`, which is why assigning the
same one-shot ref twice is a documented error).

### *class* s3dol.values.Filepath(path)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value *is the content of this file*. Re-readable (N1-total).

### *class* s3dol.values.Streamable(open_stream)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value is whatever a zero-arg factory’s stream yields — for sources
that must be (re)opened at write time, e.g. an HTTP download. The factory
is called once per write, so unlike [`Chunks`](#s3dol.values.Chunks) this ref IS safely
re-writable (each write gets a fresh stream).

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

### s3dol.values.as_fileobj(v)

A binary file object for any member of the write domain.

`functools.singledispatch`: the union is open for extension (users
register their own ref types) and closed for modification. Everything
non-`bytes` routes through `upload_fileobj` downstream, so seekability
is s3transfer’s problem.

* **Return type:**
  [`BinaryIO`](https://docs.python.org/3/library/typing.html#typing.BinaryIO)

### s3dol.values.is_bytes_like(v)

Whether `v` is an in-memory bytes value (the `PutObject` fast path).

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### s3dol.values.reject_str_value(v)

The one error message for the `str` ambiguity (ADR-0005 §2).
