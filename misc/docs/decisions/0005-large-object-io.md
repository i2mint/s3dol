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

The hard constraint from [ADR-0001](0001-layered-architecture.md): **base interfaces stay
pure.** No `s.upload_multipart(...)`. Infra capability must be reachable *through* `s[k] = v`.

## Decision

### 1. The write domain is a small, closed, explicitly-named union; the read codomain is `bytes`

```python
BytesSource = bytes | bytearray | BinaryIO | Filepath | Chunks | Streamable
```

`Filepath`, `Chunks` and `Streamable` are tiny frozen dataclasses in `s3dol/values.py` — value
*refs*, not values. Dispatch is `functools.singledispatch`, so the union is open for
extension (users register their own ref types) and closed for modification.

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

> **N1 Canonical form.** There is a total `normalize: WriteDomain → bytes`, identity on `bytes`.
> **N2 Stability.** Therefore `s[k] = s[k]` is a no-op and `dst.update(src)` terminates.
> **N3 The honest invariant.** Not `s[k] = v ⟹ s[k] == v`, but **`s[k] = v ⟹ s[k] == normalize(v)`**.

Rejecting `str` is exactly what keeps `normalize` a *function* — with `str` admitted it would
have two candidate results, and N1 would fail. The rejection isn't fussiness; it's what makes
the rest sound.

The residual cost is real and small: `setdefault` becomes type-unstable, `pop`/`popitem`
become expensive. Documented, not removed.

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
you can take writes away but not reads. We define the `Protocol`s anyway (~15 lines, they
document the shape and serve genuinely write-only sinks) and propose the missing dol
symmetry upstream. Note `@runtime_checkable` checks method *presence* only, so
`isinstance(d, WriteOnlyStore)` is `True` for a `dict`; "must not be gettable" needs an
explicit predicate.

### 5. How the capability reaches through the pure interface: injected strategies

```python
BucketStore(
    bucket, connection=conn,
    writes=transfer_writes(multipart_threshold=64<<20, max_concurrency=8),
    reads=bytes_reads(),            # or stream_reads() / ranged_reads()
)
```

A strategy is a callable, injected at construction. This is the answer to "how do we get
infra-specific optimization without polluting the interface": `__setitem__` stays
`__setitem__`; *how* it uploads is a constructor parameter.

Note a **structural** reason this cannot be a `dol` value codec: a codec is a pure
`obj -> data` transformation applied by `Store.__setitem__` before the inner write. A
multipart upload needs the *key* and the *client*, and it is a side effect, not a
transformation. So the strategy must live in the leaf store, below `wrap_kvs`.

Default: `transfer_writes` with boto3's threshold (8 MiB). Small writes take a single
`PutObject`; large ones transparently go multipart. The overhead on small objects is one
branch.

### 6. The read side, symmetrically

`s[k]` returns `bytes` — always, because N1 demands it. Streaming is reached three ways,
in increasing explicitness: a `reads=stream_reads()` strategy at construction (the store's
values become chunk iterators — a *different store*, honestly typed); `store.handle(k)` for
`.open()` / `.stream()` / `.read(byte_range=...)`; or a `Filepath` destination for
download-to-disk.

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
