# ADR-0005: Large objects through a pure Mapping — value refs + injected transfer strategies

- **Status:** Accepted
- **Date:** 2026-08-10
- **Addresses:** [issue #5](https://github.com/i2mint/s3dol/issues/5)

## Context

Multipart upload is used *precisely when the data is too big to hold in memory*. That makes
`s[k] = v` awkward: if `v` is `bytes`, we have already lost. So what is `v`? A filepath? A
file object? An iterator of bytes? And is it acceptable that `s[k] = v` accepts something
different from what `s[k]` returns?

A tempting escape is to split the store: a write-only multipart store that is Iterable +
Settable + Deletable but not Gettable, paired with a separate reader.

The hard constraint: infra capability must be reachable *through* `s[k] = v`, not by bolting
`s.upload_multipart(...)` onto a Mapping.

State that as a rule the next contributor can actually apply, because "keep the interface
pure" was being claimed while six methods were added:

> **The Mapping protocol is closed.** A method may be added iff it takes a key and is either
> a pure read of metadata or an address (`info`, `url_for`), or returns a store or handle
> (`sub`, `handle`, `prefixes`). Anything that mutates, batches, or takes non-key arguments
> belongs on `ObjectHandle` or a recipe. `delete_many` is an explicit, named exception,
> admitted only because the cost difference against a `__delitem__` loop is an order of
> magnitude.

For calibration: `azuredol` has exactly one such method (`walk`) and pushes everything
per-object onto `BlobHandle`. Six unexplained exceptions is how a surface grows to twenty.

## Decision

### 1. The write domain is a small, closed, explicitly-named union; the read codomain is `bytes`

```python
BytesSource = bytes | bytearray | BinaryIO | Filepath | Chunks | Streamable
```

`Filepath`, `Chunks` and `Streamable` are tiny frozen dataclasses in `s3dol/values.py` — value
*refs*, not values. Dispatch is `functools.singledispatch`, so the union is open for
extension (users register their own ref types) and closed for modification.

**Register on `io.IOBase`, never `typing.BinaryIO`.** `@register(BinaryIO)` is accepted at
definition time and then **never fires** — `io.BytesIO` is not in `typing.BinaryIO`'s MRO, and
`isinstance(io.BytesIO(), typing.BinaryIO)` is `False`. So the second-most obvious thing a
user types, `s['big.mp4'] = open('big.mp4', 'rb')`, would raise `TypeError`. `io.IOBase`
covers `BytesIO`, `BufferedReader`, `botocore.response.StreamingBody` (which the store-to-store
streaming copy depends on), `SpooledTemporaryFile` and urllib3 responses. `BinaryIO` stays in
the *static* union for mypy only. `memoryview` and other buffer-protocol objects are also
accepted — `dol.Files` takes them, and the precedent table below credits it for that.

### 2. `str` is rejected, loudly

`s['config'] = '{"a": 1}'` and `s['video'] = '/tmp/big.mp4'` are both overwhelmingly
plausible. There is no rule distinguishing them that isn't a latent data-corruption bug —
`os.path.exists` is a heuristic whose behaviour depends on the filesystem.

Dispatch is not the crime; dispatching on `str` is. The test is **decidability**: a `Path` is
never content, an open file handle is never content, a `Chunks(...)` is never content. A
`str` is ambiguous, so it must not be guessed:

```
TypeError: A str value is ambiguous here: did you mean its utf-8 bytes, or a filepath?
  content : s[k] = v.encode()   (or wrap: dol.wrap_kvs(store, data_of_obj=str.encode, ...))
  filepath: s[k] = s3dol.Filepath(v)
```

Bare `os.PathLike` is rejected too — `Path` *is* unambiguous, but accepting it while
rejecting `str` is a confusing half-rule. One word, `Filepath(p)`, zero ambiguity.

### 3. Asymmetric read/write types are correct, subject to a law

The question "is it a real problem or just design ickiness?" has an answer: **it is normal,
and it is safe exactly when a canonical form exists.**

Precedent, verified in the installed environment rather than recalled:

| Library | write accepts | read returns | `MutableMapping`? |
|---|---|---|---|
| **h5py** | `list`, `ndarray`, scalars, `bytes`, **`SoftLink`/`ExternalLink` reference objects** | `Dataset`/`Group` (not an `ndarray`) | **yes** |
| **configparser** | a `Mapping` of anything | `SectionProxy`; values come back `str` | yes |
| **fsspec `FSMap`** | `bytes`, `bytearray`, `array`, anything with `__array__`; **`str` rejected** | `bytes` | yes |
| **`dol.Files`** | any buffer-protocol object | `bytes` | yes |
| shelve, numpy, zarr, pandas | broader than read | narrower | mostly |

h5py is the decisive one: `SoftLink`/`ExternalLink` are purpose-built objects meaning *"the
value is a reference to content elsewhere"*, assigned through an ordinary
`MutableMapping.__setitem__`, in the most widely used scientific-data library in Python.
`Filepath`/`Chunks` are that design.

The law that makes it safe — three conditions, all necessary:

> **N1 Canonical form.** `normalize: WriteDomain → bytes` is **total on
> `bytes | bytearray | Filepath`** and **one-shot on `BinaryIO | Chunks | Streamable`** — a
> stream ref is consumed by its first write; assigning the same ref twice is a documented
> error, not a second copy.
> **N2 Stability.** For the re-readable half, `s[k] = s[k]` is a no-op and `dst.update(src)`
> terminates.
> **N3 The honest invariant.** Not `s[k] = v ⟹ s[k] == v`, but
> **`s[k] = v ⟹ s[k] == normalize(v)`** — for the re-readable half.

Note what this correction costs the argument: `str` was **not** the only thing breaking N1.
The single-consumption stream refs break it too (`s['a'] = f; s['b'] = f` yields `b'payload'`
then `b''`). So "rejecting `str` is what keeps `normalize` a function" is a non-sequitur, and
is struck. The `str` rejection stands on the **decidability** argument in §2 alone, which is
sufficient.

The residual costs, stated honestly: `setdefault` becomes type-unstable, `pop`/`popitem`
become expensive, and — the larger loss — **refs only type-check against the concrete class**.
Through `MutableMapping[str, bytes]`, which is how dependents actually annotate the store
(`lacing/artifact_store.py:120`), `s[k] = Filepath(...)`, `update()` and `setdefault()` all
fail a type checker, because `MutableMapping`'s value type is invariant. `BucketStore`
declares an explicit `update(self, other: Mapping[str, BytesSource]) -> None` override.

### 4. Do NOT split into a write-only store

Three reasons, in order of force:

1. **The split solves a problem that doesn't exist.** After `complete_multipart_upload` the
   object is an ordinary S3 object; `GetObject` reads it with no knowledge that it arrived in
   parts. Multipart is a *transport* concern, not a storage-model one. There is no state in
   which the key is writable but not readable.
2. **It conflates two different things.** "This blob is huge, upload it efficiently" is one
   `__setitem__` and is fully solved by a strategy. "This is a live stream being appended to
   over minutes" is a *session* with a lifetime, an `UploadId` and a completion event — not a
   Mapping operation at all, and it should not be modelled as one.
3. **It costs the whole Mapping toolchain**: `dict(s)`, `s.items()`, `filt_iter`, `cache_vals`,
   `kv_walk`, `Mapping.__eq__`, and every write-then-read doctest.

For the record, on the typing question the issue raises: `collections.abc` offers **nothing**
for Iterable+Settable+Deletable-but-not-Gettable, and cannot cheaply — `MutableMapping`
inherits `__getitem__` as abstract from `Mapping`, and `pop`/`popitem`/`clear`/`setdefault`
are all defined in terms of it; only `update` survives. `dol` has `mk_read_only` /
`disable_setitem` / `disable_delitem` but **no `disable_getitem` and no `mk_write_only`** —
you can take writes away but not reads.

We **do not ship** `WriteOnlyStore` Protocols in v1. They would have zero implementers, which
violates [ADR-0009](0009-scope-and-deferrals.md)'s own "no new `Protocol` without two
implementers" rule, and they cannot express the constraint anyway: `@runtime_checkable` checks
method *presence* only, so `isinstance({}, WriteOnlyStore)` is `True` and "must not be
gettable" needs a separate predicate. (Relatedly: since Python 3.12 `isinstance` uses
`getattr_static`, which detects a **class**-wrapped capability but not an **instance**-wrapped
one — capability detection must never rely on it.) The shape is recorded here; the Protocols
and the `disable_getitem`/`mk_write_only` symmetry go on the dol upstream list.

### 5. How the capability reaches through the pure interface: injected strategies

```python
BucketStore(
    bucket,
    connection=conn,
    writes=transfer_writes(multipart_threshold=64 << 20, max_concurrency=8),
    reads=bytes_reads(),  # or stream_reads() / ranged_reads()
)
```

A strategy is a callable, injected at construction. This is the answer to "how do we get
infra-specific optimization without polluting the interface": `__setitem__` stays
`__setitem__`; *how* it uploads is a constructor parameter.

Why this cannot be a `dol` value codec — the accurate version, since the obvious reason is
wrong. It is *not* that a codec can't see the key and the store: `wrap_kvs(preset=…)` with the
`(self, k, v)` convention **is** handed both. The real reasons are that (a) `preset`'s return
value is still passed to the inner `__setitem__`, so it can transform the value but cannot
*replace* the write, and (b) inside it, `self` is the unwrapped leaf. So the strategy must
live in the leaf store, below `wrap_kvs`.

Default: `transfer_writes` with boto3's threshold (8 MiB). Small writes take a single
`PutObject`; large ones transparently go multipart. The overhead on small objects is one
branch.

Every non-`bytes` source routes through `upload_fileobj`/`TransferManager`, never
`PutObject` — so seekability is s3transfer's problem, and the size threshold is never
consulted for a non-seekable source (where it is undecidable anyway). `io.UnsupportedOperation`
and `botocore.exceptions.UnseekableStreamError` are part of the error seam: a non-seekable
stream raises from `botocore/httpchecksum.py` *before the request is built*, so a
`translate_s3_errors` that only catches `ClientError` would never see it.

### 6. The read side, symmetrically

`s[k]` returns `bytes` — always, because N1 demands it. Streaming is reached three ways,
in increasing explicitness: a `reads=stream_reads()` strategy at construction (the store's
values become chunk iterators — a **runtime** variation, *not* a static one: the class's value
type does not change, and honest typing would need `BucketStore(Generic[VT])` plus overloaded
construction, which v1 does not do); `store.handle(k)` for `.open()` / `.stream()` /
`.read(byte_range=...)`; or a `Filepath` destination for download-to-disk.

`ObjectHandle` is not a Mapping and is the documented escape hatch — the same role
`BlobHandle` plays in `azuredol`.

### 7. `s[k] += v` stays out

`+=` on a Mapping is `__getitem__` then `__setitem__` — it reads the whole object back,
which is the opposite of the point. The general design belongs in `dol` (see
[dol discussion #29](https://github.com/i2mint/dol/discussions/29) and `dol/appendable.py`'s
`Extender`), and `azuredol` reached the same conclusion from the other side: it *removed*
append-blob-as-default because `MutableMapping.__setitem__` semantically replaces.

## Consequences

**Buys.** Arbitrarily large objects through `s[k] = v`. Store-to-store streaming copy where
neither side is ever fully in memory. No new public methods on the Mapping. Users extend the
write domain for their own types without touching s3dol.

**Costs.** Users must learn one name (`Filepath`) for the filepath case. Refs are s3dol types
that a plain `dict` or `Files` doesn't understand — so they must not escape the leaf layer;
`as_fileobj` is a public extension point so other backends *can* register support, and
fan-out wrappers reject refs with a clear message rather than pickling them.

**What NOT to do.** Do not accept `str` as a filepath, ever, under any flag. Do not add
`upload_multipart` to the store. Do not implement `+=` here.
