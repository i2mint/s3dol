# s3dol.base

Layer B — close-to-metal stores over one bucket or one endpoint.

Design record: `misc/docs/architecture.md` (the contract tables) on
ADR-0001 (layering; the prefix lives HERE, in the leaf), ADR-0004 (one error
seam), ADR-0005 (write domain / strategies), ADR-0006 (prefix arithmetic and
key validity), ADR-0008 (`__len__` absent by design), ADR-0010 (bucket policy),
ADR-0011 (**no key-taking public methods** — one guarded exception, `url_for`),
ADR-0012 (connection).

The class triads mirror `dol.filesys` and `azuredol`:

```default
BucketCollection   -> BucketReader   -> BucketStore     (keys: object keys)
EndpointCollection -> EndpointReader -> EndpointStore   (keys: bucket names)
ObjectHandle                                            (not a Mapping)
```

Reader-only classes are *real classes* (a type checker catches
`reader[k] = v`; a read-scoped credential never even attempts a write) —
`dol.mk_read_only` is non-functional on a dol store (verified, ADR-0001).

**The rule for what may join a class here: nothing that takes a key**
(ADR-0011 D1). `dol` wrappers hand any non-dunder method the outer, unmapped
key, and the standard escape (`wrapped_self`) is itself silently wrong when
nothing references the wrapper. Keyed capabilities are sibling stores
(`s3dol.handles(store)[k]` — `capabilities.py`); per-object richness lives on
[`ObjectHandle`](#s3dol.base.ObjectHandle) (key bound at construction); everything else is a free
function taking the store first. The single exception is
[`BucketReader.url_for()`](#s3dol.base.BucketReader.url_for), kept for `dol.SupportsUrlFor`, guarded to be
correct-or-loud (D3b). A reflective conformance test enforces the rule (D5).

### Module Attributes

| [`MAX_KEY_BYTES`](#s3dol.base.MAX_KEY_BYTES)       | S3's real limit is 1024 **UTF-8 bytes** (enforced client-side so moto and AWS agree — moto accepts longer keys; ADR-0006 §4).   |
|----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| [`MAX_PRESIGN_EXPIRES`](#s3dol.base.MAX_PRESIGN_EXPIRES) | SigV4's presigned-URL lifetime cap (botocore does not enforce it).                                                              |

### Functions

| [`clamp_expires_in`](#s3dol.base.clamp_expires_in)(expires_in)   | Clamp to SigV4's 604800 s cap, warning.   |
|---------------------------------------------------------------------------------|-------------------------------------------|

### Classes

| [`BucketCollection`](#s3dol.base.BucketCollection)(bucket, \*[, connection, ...])    | Keys of one bucket (under the prefix).                                                                                                                                                                                                        |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`BucketReader`](#s3dol.base.BucketReader)(bucket, \*[, connection, ...])        |                                                                                                                                                                                                                                               |
| [`BucketStore`](#s3dol.base.BucketStore)(bucket, \*[, connection, prefix, ...]) |                                                                                                                                                                                                                                               |
| [`EndpointCollection`](#s3dol.base.EndpointCollection)([connection])                   | Bucket names at one endpoint.                                                                                                                                                                                                                 |
| [`EndpointReader`](#s3dol.base.EndpointReader)([connection])                       |                                                                                                                                                                                                                                               |
| [`EndpointStore`](#s3dol.base.EndpointStore)([connection])                        |                                                                                                                                                                                                                                               |
| [`ObjectHandle`](#s3dol.base.ObjectHandle)(bucket, key, \*[, connection])        | One object, key bound **at construction** — which is what makes it immune to the delegation trap (a key codec over the store cannot corrupt a key that was resolved before the handle existed; ADR-0011 D1, following `azuredol.BlobHandle`). |
| [`ObjectInfo`](#s3dol.base.ObjectInfo)(key[, size, last_modified, etag, ...])  | Object metadata — from `HeadObject` or from a LIST row (which already carries size/mtime/etag/storage class that v0 threw away; ADR-0009).                                                                                                    |
| [`S3Profiles`](#s3dol.base.S3Profiles)(\*[, bucket_kwargs, readonly])          | AWS **profile** names -> endpoint stores (the v0 `S3Dol`, renamed — its keys are verifiably profile names, never endpoints; ADR-0007 §1).                                                                                                     |

### Exceptions

| [`ObjectNotFoundForDelete`](#s3dol.base.ObjectNotFoundForDelete)   | Raised by `strict_delete=True` deletes of absent keys.   |
|----------------------------------------------------------------------------|----------------------------------------------------------|

### *class* s3dol.base.BucketCollection(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: `_BucketBase`, `Collection`

Keys of one bucket (under the prefix). `Collection` only —
iteration and membership, no reads.

### *class* s3dol.base.BucketReader(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](#s3dol.base.BucketCollection), `KvReader`

+ `__getitem__` -> bytes (or the read strategy’s value type).

#### url_for(k, , expires_in=3600, client_method='get_object', \*\*params)

Presigned URL for `k` — **the one keyed method** (ADR-0011 D3b),
kept solely because `dol.SupportsUrlFor` requires a *method* and
`dol.content_url` reaches it by `getattr`. The canonical form is
`s3dol.urls(store)[k]`.

Guarded to be correct-or-loud, never silently wrong:

- **unwrapped store**: `k` is a key in this store’s own key space,
  so the URL addresses `self._id_of_key(k)`. Correct, and this is
  also the contract `dol.content_url` calls under — it resolves the
  key through the *outer* layers and stops at the layer owning
  `url_for`, “because that layer applies its own”
  (`dol/content.py`);
- **this leaf has ever been wrapped**: **raises**. Two call paths then
  reach this one signature with *different key domains* and nothing
  distinguishes them: `dol.content_url` hands over a key already
  mapped through the wrapper layers, while a direct
  `wrapped.url_for(k)` (delegation) hands over the raw outer key.
  Applying `_id_of_key` is right for the first and wrong for the
  second; re-resolving through the chain is right for the second and
  double-transforms the first. Since a wrong presigned URL addresses a
  *different real object*, the only honest answer is refusal, naming
  the sibling store — which is correct by construction because it goes
  through `__getitem__` (ADR-0011 D2/D3b, and see the note below);
- **anonymous connection**: returns `None` (`SupportsUrlFor` is
  `Optional[str]`; raising would turn a public-bucket streaming
  fallback into a 500 — ADR-0012 D5). The check reads the built
  client’s signature version, never `spec.anon`.

#### NOTE
This refusal is a *finding*, not a limitation we chose.
ADR-0011 D3b expected a guarded method to serve both paths; it
cannot, on a leaf that owns its prefix. Reported upstream on
dol#86/#83 as evidence that a keyed capability *method* is
unfixable in the general case, and that the sibling-store form is
the only one correct by construction.

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

### *class* s3dol.base.BucketStore(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketReader`](#s3dol.base.BucketReader), `KvPersister`

+ `__setitem__` / `__delitem__`. The user-facing read-write store.

#### pop(k) → v, remove specified key and return the corresponding value.

If key is not found, d is returned if given, otherwise KeyError is raised.

#### setdefault(k) → D.get(k,d), also set D[k]=d if k not in D

#### update(other=(), , \*\*kwargs)

If E present and has a .keys() method, does:     for k in E.keys(): D[k] = E[k]
If E present and lacks .keys() method, does:     for (k, v) in E: D[k] = v
In either case, this is followed by: for k, v in F.items(): D[k] = v

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* s3dol.base.EndpointCollection(connection=None, \*\*bucket_kwargs)

Bases: `_EndpointBase`, `Collection`

Bucket names at one endpoint.

### *class* s3dol.base.EndpointReader(connection=None, \*\*bucket_kwargs)

Bases: [`EndpointCollection`](#s3dol.base.EndpointCollection), `KvReader`

+ `__getitem__` -> [`BucketReader`](#s3dol.base.BucketReader).

### *class* s3dol.base.EndpointStore(connection=None, \*\*bucket_kwargs)

Bases: [`EndpointReader`](#s3dol.base.EndpointReader), `KvPersister`

+ bucket creation/deletion. `del endpoint[name]` refuses a non-empty
  bucket; the explicit cascading form is the free function
  `s3dol.delete_bucket(endpoint, name, force=True)` (ADR-0010 §3 as
  amended by ADR-0011 D4 — a public keyed destructive *method* is the shape
  the family census found destroying wrong data).

### s3dol.base.MAX_KEY_BYTES *= 1024*

S3’s real limit is 1024 **UTF-8 bytes** (enforced client-side so moto and
AWS agree — moto accepts longer keys; ADR-0006 §4).

### s3dol.base.MAX_PRESIGN_EXPIRES *= 604800*

SigV4’s presigned-URL lifetime cap (botocore does not enforce it).

### *class* s3dol.base.ObjectHandle(bucket, key, , connection=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

One object, key bound **at construction** — which is what makes it
immune to the delegation trap (a key codec over the store cannot corrupt a
key that was resolved before the handle existed; ADR-0011 D1, following
`azuredol.BlobHandle`). This is where ranged reads, streaming,
download-to-disk, metadata and presigned URLs live.

#### download_to(path)

Download to disk (never fully in memory). Accepts a path or
[`Filepath`](s3dol.values.html.md#s3dol.values.Filepath); returns the path written.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### info()

One `HeadObject` -> [`ObjectInfo`](#s3dol.base.ObjectInfo).

* **Return type:**
  [`ObjectInfo`](#s3dol.base.ObjectInfo)

#### read(, offset=None, length=None)

The object’s bytes; a ranged read when `offset`/`length` given.

* **Return type:**
  [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)

#### restore(days=1, , tier='Standard')

Request restoration of an archived (Glacier) object.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### stream(, chunk_size=8388608)

Lazy chunk iterator over the body.

* **Return type:**
  [`Iterator`](https://docs.python.org/3/library/typing.html#typing.Iterator)[[`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)]

#### url(, expires_in=3600, client_method='get_object', \*\*params)

Presigned URL. Always SigV4; `None` when anonymous; capped at
SigV4’s 604800 s; warns when the signing credential expires sooner
(the URL cannot outlive it).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### write(value)

Write the object (same value domain as `store[k] = v`).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* s3dol.base.ObjectInfo(key, size=None, last_modified=None, etag=None, content_type=None, storage_class=None, restore_status=None, version_id=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Object metadata — from `HeadObject` or from a LIST row (which already
carries size/mtime/etag/storage class that v0 threw away; ADR-0009).

### *exception* s3dol.base.ObjectNotFoundForDelete

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), [`KeyError`](https://docs.python.org/3/builtins/exceptions.html#KeyError)

Raised by `strict_delete=True` deletes of absent keys.

### *class* s3dol.base.S3Profiles(, bucket_kwargs=None, readonly=False)

Bases: `KvReader`

AWS **profile** names -> endpoint stores (the v0 `S3Dol`, renamed —
its keys are verifiably profile names, never endpoints; ADR-0007 §1).

```pycon
>>> # S3Profiles()['prod']  ->  EndpointStore over the 'prod' profile
```

### s3dol.base.clamp_expires_in(expires_in)

Clamp to SigV4’s 604800 s cap, warning. botocore does not enforce it —
it just signs a URL that fails after 7 days.

* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)
