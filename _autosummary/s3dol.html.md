# s3dol

s3dol — S3 and S3-compatible object storage behind a dict-like interface.

The one-liner:

```default
import s3dol
s = s3dol.s3_store('my-bucket')          # MutableMapping[str, bytes]
s['k'] = b'v'; s['k']; list(s); del s['k']
```

Keyed capabilities are **sibling stores you index** (never methods — a dol key
wrapper would hand a method the unmapped key; ADR-0011):

```default
s3dol.handles(s)['k']    # ObjectHandle (ranged reads, streams, metadata)
s3dol.urls(s)['k']       # presigned URL
s3dol.info(s)['k']       # ObjectInfo (one HeadObject)
s3dol.listed_info(s)['k']  # ObjectInfo from one LIST (a snapshot)
```

Non-keyed operations are free functions taking the store first:

```default
s3dol.sub(s, 'folder/')          s3dol.prefixes(s)
s3dol.delete_many(s, keys)       s3dol.delete_bucket(endpoint, name, force=True)
```

Test without a cloud: `from s3dol.testing import mock_s3, run_conformance`.
Migrating from v0? `s3dol.diagnose(**your_S3Store_kwargs)` prints what
resolves, from where, and whether v1 moves it. The legacy `S3Store` keeps
working via `s3dol.store` (deprecated, removed in v2).

Everything here loads lazily (PEP 562): `import s3dol` does not import boto3.

### Functions

| [`S3Store`](#s3dol.S3Store)(bucket_name, \*[, make_bucket, path, ...])   | DEPRECATED — the v0 entry point, kept working until v2.                                                                                                                                                                       |
|-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`delete_bucket`](#s3dol.delete_bucket)(endpoint, name, \*[, force])           | Delete bucket `name`.                                                                                                                                                                                                         |
| [`delete_many`](#s3dol.delete_many)(store, keys)                             | Bulk delete (ADR-0010 §2; a free function per ADR-0011 D4 — keyed + destructive + delegated is the census's data-destroying shape).                                                                                           |
| [`diagnose`](#s3dol.diagnose)([connection, environ, aws_config, file])    | Print (and return) the resolution report.                                                                                                                                                                                     |
| [`handles`](#s3dol.handles)(store)                                       | The sibling store of [`ObjectHandle`](#s3dol.ObjectHandle)s: `handles(s)[k]`.                                                                                                                         |
| [`info`](#s3dol.info)(store)                                          | The sibling store of [`ObjectInfo`](#s3dol.ObjectInfo): `info(s)[k]`.                                                                                                                               |
| [`listed_info`](#s3dol.listed_info)(store)                                   | The sibling store of [`ObjectInfo`](#s3dol.ObjectInfo) built from one paginated LIST: `listed_info(s)[k]` (a lazy snapshot; see [`BucketListedInfo`](#s3dol.BucketListedInfo)). |
| [`prefixes`](#s3dol.prefixes)(store)                                      | The 'directories' one level under the store's scope, \*\*relative to the caller's key space\*\* — one `ListObjectsV2(Delimiter='/')` reading `CommonPrefixes` (which v0 parsed and then called from nowhere; ADR-0009).       |
| [`presigned_upload`](#s3dol.presigned_upload)(store, \*, key_prefix, ...)         | A presigned POST form for ONE new object under `key_prefix`.                                                                                                                                                                  |
| [`register_preset`](#s3dol.register_preset)(preset, \*[, overwrite])             | Add a row.                                                                                                                                                                                                                    |
| [`resolve`](#s3dol.resolve)(spec, environ[, aws_config])                 | Pure resolution: no `os.environ`, no disk, no network (ADR-0012 D2).                                                                                                                                                          |
| [`s3_store`](#s3dol.s3_store)(bucket, \*[, prefix, preset, ...])          | One bucket as a `MutableMapping[str, bytes]` (a `Mapping` when `readonly=True`).                                                                                                                                              |
| [`sub`](#s3dol.sub)(store, prefix)                                   | A store scoped to `prefix`, **in the caller's key space** (D3).                                                                                                                                                               |
| [`urls`](#s3dol.urls)(store, \*[, expires_in, client_method])         | The sibling store of presigned URLs: `urls(s)[k]`.                                                                                                                                                                            |

### Classes

| [`BucketCollection`](#s3dol.BucketCollection)(bucket, \*[, connection, ...])    | Keys of one bucket (under the prefix).                                                                                                                                                                                                        |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`BucketHandles`](#s3dol.BucketHandles)(bucket, \*[, connection, ...])       | `k -> ObjectHandle` (key bound at construction; zero round-trips).                                                                                                                                                                            |
| [`BucketInfo`](#s3dol.BucketInfo)(bucket, \*[, connection, prefix, ...])  | `k -> ObjectInfo` (one `HeadObject`).                                                                                                                                                                                                         |
| [`BucketListedInfo`](#s3dol.BucketListedInfo)(bucket, \*[, connection, ...])    | `k -> ObjectInfo` from the listing rows, not one `HeadObject` per key.                                                                                                                                                                        |
| [`BucketReader`](#s3dol.BucketReader)(bucket, \*[, connection, ...])        |                                                                                                                                                                                                                                               |
| [`BucketStore`](#s3dol.BucketStore)(bucket, \*[, connection, prefix, ...]) |                                                                                                                                                                                                                                               |
| [`BucketUrls`](#s3dol.BucketUrls)(\*args[, expires_in, client_method])    | `k -> presigned URL` (zero object requests; `None` when anonymous).                                                                                                                                                                           |
| [`CallableCredentials`](#s3dol.CallableCredentials)(fetch)                         | A picklable zero-arg callable returning a mapping with keys `access_key`/`secret_key` (aliases: `key`/`secret`), optional `token`, optional `expires_at`/`expiry_time` (ISO 8601).                                                            |
| [`Capabilities`](#s3dol.Capabilities)([list_objects_v2, ...])               | What a backend supports.                                                                                                                                                                                                                      |
| [`Chunks`](#s3dol.Chunks)(chunks[, consumed])                         | The value is the concatenation of an iterable of `bytes` chunks.                                                                                                                                                                              |
| [`DailyByteBudget`](#s3dol.DailyByteBudget)(limit_bytes, \*[, ledger, today])  | A per-day byte budget (one instance per tenant); call it to reserve.                                                                                                                                                                          |
| [`EndpointCollection`](#s3dol.EndpointCollection)([connection])                   | Bucket names at one endpoint.                                                                                                                                                                                                                 |
| [`EndpointReader`](#s3dol.EndpointReader)([connection])                       |                                                                                                                                                                                                                                               |
| [`EndpointStore`](#s3dol.EndpointStore)([connection])                        |                                                                                                                                                                                                                                               |
| [`Filepath`](#s3dol.Filepath)(path)                                     | The value *is the content of this file*.                                                                                                                                                                                                      |
| [`ObjectHandle`](#s3dol.ObjectHandle)(bucket, key, \*[, connection])        | One object, key bound **at construction** — which is what makes it immune to the delegation trap (a key codec over the store cannot corrupt a key that was resolved before the handle existed; ADR-0011 D1, following `azuredol.BlobHandle`). |
| [`ObjectInfo`](#s3dol.ObjectInfo)(key[, size, last_modified, etag, ...])  | Object metadata — from `HeadObject` or from a LIST row (which already carries size/mtime/etag/storage class that v0 threw away; ADR-0009).                                                                                                    |
| [`Preset`](#s3dol.Preset)(name, addressing_style[, ...])              | One provider's facts.                                                                                                                                                                                                                         |
| [`ProfileCredentials`](#s3dol.ProfileCredentials)(profile)                        | Credentials from a named profile — *credentials only*; the profile's other config is not selected by this (that is what `profile=` on the connection is for).                                                                                 |
| [`Resolution`](#s3dol.Resolution)(endpoint_url, region_name, ...)         | The result of [`resolve()`](#s3dol.resolve) — everything `diagnose()` prints and everything client-build consumes.                                                                                               |
| [`S3Connection`](#s3dol.S3Connection)([preset, endpoint_url, ...])          | A frozen, picklable connection spec.                                                                                                                                                                                                          |
| [`S3Jsons`](#s3dol.S3Jsons)                                            |                                                                                                                                                                                                                                               |
| [`S3Pickles`](#s3dol.S3Pickles)                                          |                                                                                                                                                                                                                                               |
| [`S3Profiles`](#s3dol.S3Profiles)(\*[, bucket_kwargs, readonly])          | AWS **profile** names -> endpoint stores (the v0 `S3Dol`, renamed — its keys are verifiably profile names, never endpoints; ADR-0007 §1).                                                                                                     |
| [`S3Texts`](#s3dol.S3Texts)                                            |                                                                                                                                                                                                                                               |
| [`StaticCredentials`](#s3dol.StaticCredentials)(access_key, secret_key[, ...])   | An explicit key pair (optionally a session token).                                                                                                                                                                                            |
| [`Streamable`](#s3dol.Streamable)(open_stream)                            | The value is whatever a zero-arg factory's stream yields — for sources that must be (re)opened at write time, e.g. an HTTP download.                                                                                                          |
| [`S3Dol`](#s3dol.S3Dol)                                              |                                                                                                                                                                                                                                               |

### Exceptions

| [`AccessDenied`](#s3dol.AccessDenied)                                       | Permission denied.                                                       |
|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`BucketNotEmpty`](#s3dol.BucketNotEmpty)                                     | Refusal to delete a non-empty bucket.                                    |
| [`BucketNotFound`](#s3dol.BucketNotFound)                                     | The bucket is absent.                                                    |
| [`ConfigurationError`](#s3dol.ConfigurationError)                                 | A construction- or resolution-time misconfiguration (ADR-0012 D1).       |
| [`CredentialsError`](#s3dol.CredentialsError)                                   | Credentials are missing, expired, or invalid.                            |
| [`KeyNotValid`](#s3dol.KeyNotValid)                                        | The key is syntactically invalid (checked before the wire).              |
| [`NotSupported`](#s3dol.NotSupported)                                       | The operation is not supported by this provider (names both).            |
| [`ObjectArchived`](#s3dol.ObjectArchived)(message, \*[, storage_class, ...])  | The object exists but is archived (e.g. Glacier) and cannot be read now. |
| [`ObjectNotFound`](#s3dol.ObjectNotFound)                                     | The object is absent.                                                    |
| [`S3DolWarning`](#s3dol.S3DolWarning)                                       | Base of every s3dol warning.                                             |
| [`S3Error`](#s3dol.S3Error)                                            | Base of every s3dol exception — `except S3Error` catches the package.    |
| [`S3PartialFailure`](#s3dol.S3PartialFailure)(message, \*, succeeded, failures) | A bulk operation partially failed (ADR-0010 §2).                         |
| [`UploadBudgetExceeded`](#s3dol.UploadBudgetExceeded)                               | The tenant's byte budget cannot cover this upload; no form was signed.   |

### *exception* s3dol.AccessDenied

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

Permission denied. **Not** a `KeyError` — a missing permission must
not look like a missing key (ADR-0004 §1/§3).

### *class* s3dol.BucketCollection(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: `_BucketBase`, `Collection`

Keys of one bucket (under the prefix). `Collection` only —
iteration and membership, no reads.

### *class* s3dol.BucketHandles(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectHandle` (key bound at construction; zero round-trips).

### *class* s3dol.BucketInfo(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectInfo` (one `HeadObject`).

### *class* s3dol.BucketListedInfo(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectInfo` from the listing rows, not one `HeadObject` per key.

The first access (a lookup or an iteration) pages through `ListObjectsV2`
once and keeps `{key: ObjectInfo}` (`size`, `last_modified`, `etag`,
`storage_class`; `content_type` and the other HEAD-only fields stay
`None`). It is **lazy and a snapshot**: later writes are not seen; make a
new `listed_info(store)` to refresh. A whole-bucket ETag comparison costs
one LIST per 1000 keys.

### *exception* s3dol.BucketNotEmpty

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

Refusal to delete a non-empty bucket. Use
`s3dol.delete_bucket(endpoint, name, force=True)` for the explicit
cascading form (ADR-0010 §3).

### *exception* s3dol.BucketNotFound

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), `_InformativeKeyError`

The bucket is absent.

Still a `KeyError`: for an object operation it is a key-space problem for
the caller; for a bucket operation it is the key of the endpoint mapping.

### *class* s3dol.BucketReader(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

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

### *class* s3dol.BucketStore(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketReader`](s3dol.base.html.md#s3dol.base.BucketReader), `KvPersister`

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

### *class* s3dol.BucketUrls(\*args, expires_in=3600, client_method='get_object', \*\*kwargs)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> presigned URL` (zero object requests; `None` when anonymous).
The canonical spelling of what `url_for` shims (ADR-0011 D3b).

### *class* s3dol.CallableCredentials(fetch)

Bases: [`CredentialProvider`](s3dol.connection.html.md#s3dol.connection.CredentialProvider)

A picklable zero-arg callable returning a mapping with keys
`access_key`/`secret_key` (aliases: `key`/`secret`), optional
`token`, optional `expires_at`/`expiry_time` (ISO 8601). With an
expiry, credentials are refreshable — the callable is re-invoked by
botocore’s refresh machinery (advisory 900 s / mandatory 600 s before
expiry).

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### *class* s3dol.Capabilities(list_objects_v2=True, batch_delete=True, presigned_post=True, multipart=True, max_multipart_parts=10000, min_part_size=5242880, object_tagging=True, versioning=True, conditional_writes=True, consistency='strong')

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

What a backend supports. A *static declared table*, never a probe
(a probe costs a round-trip, is wrong under partial permissions, and can’t
be trusted anyway — ADR-0003 §2). Missing capabilities are handled
per-capability: emulate when exact (batch delete -> loop), substitute when
equivalent (ListObjectsV2 -> V1), raise `NotSupported` when there is no
honest fallback. Never silently degrade correctness.

### *class* s3dol.Chunks(chunks, consumed=list())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value is the concatenation of an iterable of `bytes` chunks.
**One-shot** unless the iterable is re-iterable (a list is; a generator is
not — and a consumed generator writes `b''`, which is why assigning the
same one-shot ref twice is a documented error).

### *exception* s3dol.ConfigurationError

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError)

A construction- or resolution-time misconfiguration (ADR-0012 D1).

### *exception* s3dol.CredentialsError

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

Credentials are missing, expired, or invalid.

### *class* s3dol.DailyByteBudget(limit_bytes, \*, ledger=None, today=<function DailyByteBudget.<lambda>>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A per-day byte budget (one instance per tenant); call it to reserve.

`ledger` is any `MutableMapping[str, int]` (default: in-memory) keyed
by day, so persistence is a one-argument swap (a `dol` store, a file…).
`today` is the clock seam. Reservation is optimistic (the *maximum* the
form allows), and a form holds exactly one key, so re-posting cannot multiply
the budget on an unversioned bucket (on a versioned one each re-post stores a
new version: see the module docstring).

### *class* s3dol.EndpointCollection(connection=None, \*\*bucket_kwargs)

Bases: `_EndpointBase`, `Collection`

Bucket names at one endpoint.

### *class* s3dol.EndpointReader(connection=None, \*\*bucket_kwargs)

Bases: [`EndpointCollection`](s3dol.base.html.md#s3dol.base.EndpointCollection), `KvReader`

+ `__getitem__` -> [`BucketReader`](#s3dol.BucketReader).

### *class* s3dol.EndpointStore(connection=None, \*\*bucket_kwargs)

Bases: [`EndpointReader`](s3dol.base.html.md#s3dol.base.EndpointReader), `KvPersister`

+ bucket creation/deletion. `del endpoint[name]` refuses a non-empty
  bucket; the explicit cascading form is the free function
  `s3dol.delete_bucket(endpoint, name, force=True)` (ADR-0010 §3 as
  amended by ADR-0011 D4 — a public keyed destructive *method* is the shape
  the family census found destroying wrong data).

### *class* s3dol.Filepath(path)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value *is the content of this file*. Re-readable (N1-total).

### *exception* s3dol.KeyNotValid

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), `_InformativeKeyError`, [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError)

The key is syntactically invalid (checked before the wire).

Inherits from both `KeyError` and `ValueError` deliberately, so either
`except` works. Named `KeyNotValid` (not `KeyValidationError`) because
dol already ships two *different* classes called `KeyValidationError`
(ADR-0004 §5).

### *exception* s3dol.NotSupported

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

The operation is not supported by this provider (names both).

### *exception* s3dol.ObjectArchived(message, , storage_class=None, restore_status=None, restore=None)

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), `_InformativeKeyError`

The object exists but is archived (e.g. Glacier) and cannot be read now.

A `KeyError`, deliberately and arguably (ADR-0004 §4): `k in store`
stays `True`, but `store[k]` fails as a `KeyError` so that
`store.get(k, default)` degrades to the not-available branch. Note that
only the *defaulted accessors* degrade — `dict(store)`, `.items()` and
`Mapping.__eq__` still raise. `setdefault`/`pop` are overridden in the
store to re-raise this instead of silently overwriting the archived object.

#### restore(days=1, , tier='Standard')

Request restoration of the archived object (if a handle was bound).

### *class* s3dol.ObjectHandle(bucket, key, , connection=None)

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

One `HeadObject` -> [`ObjectInfo`](#s3dol.ObjectInfo).

* **Return type:**
  [`ObjectInfo`](s3dol.base.html.md#s3dol.base.ObjectInfo)

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

### *class* s3dol.ObjectInfo(key, size=None, last_modified=None, etag=None, content_type=None, storage_class=None, restore_status=None, version_id=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Object metadata — from `HeadObject` or from a LIST row (which already
carries size/mtime/etag/storage class that v0 threw away; ADR-0009).

### *exception* s3dol.ObjectNotFound

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error), `_InformativeKeyError`

The object is absent. The Mapping contract’s `KeyError`.

### *class* s3dol.Preset(name, addressing_style, endpoint_url=None, region_name=None, checksum=None, payload_signing_enabled=None, capabilities=Capabilities(list_objects_v2=True, batch_delete=True, presigned_post=True, multipart=True, max_multipart_parts=10000, min_part_size=5242880, object_tagging=True, versioning=True, conditional_writes=True, consistency='strong'), presign_endpoint_url=None, presign_addressing_style=None, params=(), host_patterns=(), requires_endpoint=False, soft=False, config_kwargs=(), error_overrides=(), verified=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

One provider’s facts. Public, shareable, committable config — which is
why there is **no credential slot**, structurally (ADR-0012 D4).

`endpoint_url`/`region_name` may contain `{placeholders}`; bind them
with [`bind()`](#s3dol.Preset.bind). A row whose endpoint template has an unbound *required*
placeholder contributes no endpoint rung (ADR-0012 C1).

#### addressing_style *: [Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['path', 'virtual']*

Mandatory; no default; `'auto'` is illegal (ADR-0012 D4).

#### bind(\*\*params)

A copy of this row with template `params` bound.

```pycon
>>> r2 = get_preset('r2').bind(account_id='abc123')
>>> r2.bound_endpoint()
'https://abc123.r2.cloudflarestorage.com'
```

* **Return type:**
  [`Preset`](s3dol.presets.html.md#s3dol.presets.Preset)

#### bound_endpoint(, region_name=None)

The endpoint URL with params bound, or `None` if the template has
unbound placeholders (C1: an unbound row contributes no rung —
`preset='minio'` silently becoming AWS is structurally impossible).

`region_name` binds a `{region_name}` placeholder, per the region
ladder’s first rung (ADR-0012 D3).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### checksum *: [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[Literal](https://docs.python.org/3/library/typing.html#typing.Literal)['when_supported', 'when_required']]* *= None*

`None` == the row makes no claim (the ladder continues — ADR-0012 D3).

#### config_kwargs *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Any](https://docs.python.org/3/library/typing.html#typing.Any)], ...]* *= ()*

Extra `botocore.Config` kwargs — allowlisted, scalars only.

#### error_overrides *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[int](https://docs.python.org/3/builtins/functions.html#int)]], [str](https://docs.python.org/3/builtins/stdtypes.html#str)], ...]* *= ()*

Error-classification overrides, consulted before the default table
(ADR-0004 §2/§6): rows of `((operation, code, status), kind)`.

#### host_patterns *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), ...]* *= ()*

`fnmatch` patterns over the endpoint *hostname*; used for detection
(pass 2) and the C2 named-vs-resolved guard.

#### matches_host(endpoint_url)

Whether `endpoint_url`’s hostname matches this row’s patterns.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

#### missing_endpoint_params(, region_name=None)

Which placeholders keep [`bound_endpoint()`](#s3dol.Preset.bound_endpoint) from producing a URL.

* **Return type:**
  [`set`](https://docs.python.org/3/builtins/stdtypes.html#set)

#### params *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)], ...]* *= ()*

Bound template parameters, e.g. `(('account_id', 'abc'),)`.

#### *property* pinned *: [bool](https://docs.python.org/3/builtins/functions.html#bool)*

Whether the row’s region literal is authoritative enough to make a
conflicting explicit `region_name=` raise `PresetConflict`.
Derived, and gated on `verified` — dormant while rows are
doc-sourced (ADR-0012 D4).

#### presign_endpoint_url *: [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]* *= None*

Presigning sometimes genuinely differs from the API config (Hetzner
presigns virtual while the API is path; R2 presigns only on the S3 API
domain) — ADR-0003 §1.

#### requires_endpoint *: [bool](https://docs.python.org/3/builtins/functions.html#bool)* *= False*

The provider cannot be reached without an explicit endpoint.

#### soft *: [bool](https://docs.python.org/3/builtins/functions.html#bool)* *= False*

they
never win detection over a non-soft row and never trigger the C2 guard.

* **Type:**
  Soft rows are last-resort fallbacks (`aws`, `generic-s3`)

#### template_params()

All placeholder names across this row’s templates.

* **Return type:**
  [`set`](https://docs.python.org/3/builtins/stdtypes.html#set)

#### verified *: [bool](https://docs.python.org/3/builtins/functions.html#bool)* *= False*

`True` only when the row was verified against a live endpoint (with a
date in the row’s docstring/comment). Builtin rows are doc-sourced.

### *class* s3dol.ProfileCredentials(profile)

Bases: [`CredentialProvider`](s3dol.connection.html.md#s3dol.connection.CredentialProvider)

Credentials from a named profile — *credentials only*; the profile’s
other config is not selected by this (that is what `profile=` on the
connection is for). Rebuilds live, still-refreshable credentials after
unpickling (a frozen dataclass holding only a profile name round-trips).

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### *class* s3dol.Resolution(endpoint_url, region_name, signature_version, addressing_style, checksum, payload_signing_enabled, verify, preset, preset_source, credential_provenance, capabilities, environ_consulted, notes)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The result of [`resolve()`](#s3dol.resolve) — everything `diagnose()` prints and
everything client-build consumes. Carries provenance labels, never a
credential value.

#### environ_consulted *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]], ...]*

The non-secret environment keys consulted, with their values.

#### notes *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[type](https://docs.python.org/3/builtins/functions.html#type), [str](https://docs.python.org/3/builtins/stdtypes.html#str)], ...]*

Collected (warning class, message) pairs. resolve() never emits them —
the impure caller decides (client build warns; diagnose prints rows).

### *class* s3dol.S3Connection(preset=None, endpoint_url=None, region_name=None, profile=None, credentials=None, anon=False, signature_version='s3v4', addressing_style=None, checksum=None, payload_signing_enabled=None, verify=None, client_kwargs=(), deny_means_absent=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A frozen, picklable connection spec. No live objects — ever.

Construction performs no I/O and does not import boto3 (ADR-0002); the
client is built lazily, once, under a per-instance lock (CPython ≥3.12
removed `cached_property`’s lock deliberately — measured: 8 racing
threads → 8 clients without one).

`__post_init__` is the only door: every normalisation and contradiction
check happens here, so a bad shape dies at construction with a message,
not later inside botocore.

```pycon
>>> S3Connection(anon=True, profile='prod')
Traceback (most recent call last):
...
s3dol.errors.ConfigurationError: ...
```

#### *property* client

The boto3 S3 client — built lazily, once, under the lock.

#### *property* presign_client

The client presigned URLs are generated with.

For most providers this is [`client`](#s3dol.S3Connection.client). Two providers genuinely
need different config for presigning than for the API (ADR-0003 §1):
Hetzner presigns virtual-hosted while the API is path-style; R2
presigns only on the S3 API domain. When the resolved preset carries
`presign_endpoint_url`/`presign_addressing_style`, a second client
is built (lazily, once, under the same lock) from the merged config.

#### resolution(environ=None, aws_config=<function load_aws_config>)

Resolve against the real environment by default (a thin impure
wrapper over the pure [`resolve()`](#s3dol.resolve)).

* **Return type:**
  [`Resolution`](s3dol.connection.html.md#s3dol.connection.Resolution)

### s3dol.S3Dol

alias of [`S3Profiles`](s3dol.base.html.md#s3dol.base.S3Profiles)

### *exception* s3dol.S3DolWarning

Bases: [`UserWarning`](https://docs.python.org/3/builtins/exceptions.html#UserWarning)

Base of every s3dol warning.

### *exception* s3dol.S3Error

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base of every s3dol exception — `except S3Error` catches the package.

### s3dol.S3Jsons

alias of [`BucketStore`](s3dol.base.html.md#s3dol.base.BucketStore)

### *exception* s3dol.S3PartialFailure(message, , succeeded, failures)

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

A bulk operation partially failed (ADR-0010 §2).

Not an `ExceptionGroup` — the package supports Python 3.10.

#### succeeded

keys the backend reported as deleted (absent keys are
reported as deleted too — S3’s semantics, documented, not hidden).

#### failures

mapping of key -> the error for that key.

### s3dol.S3Pickles

alias of [`BucketStore`](s3dol.base.html.md#s3dol.base.BucketStore)

### *class* s3dol.S3Profiles(, bucket_kwargs=None, readonly=False)

Bases: `KvReader`

AWS **profile** names -> endpoint stores (the v0 `S3Dol`, renamed —
its keys are verifiably profile names, never endpoints; ADR-0007 §1).

```pycon
>>> # S3Profiles()['prod']  ->  EndpointStore over the 'prod' profile
```

### s3dol.S3Store(bucket_name, , make_bucket=None, path=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, endpoint_url=None, region_name=None, profile_name=None, skip_bucket_check=None, is_supabase_endpoint=None)

DEPRECATED — the v0 entry point, kept working until v2.

Use [`s3dol.s3_store()`](#s3dol.s3_store) instead: `s3_store(bucket, prefix=...)`.

Signature is v0’s exactly (`path=` not renamed; `make_bucket`
tri-state maps `True -> on_missing_bucket='create'`, `False ->
'raise'`, `None -> 'assume'`). Behaviour differences vs v0 are bug
fixes only (explicit endpoint/credentials honoured; listing errors raise
instead of returning `[]`; writes no longer create buckets unless asked)
and announce themselves via `S3DolResolutionChanged` when they change
this call’s resolution.

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/typing.html#typing.MutableMapping)

### s3dol.S3Texts

alias of [`BucketStore`](s3dol.base.html.md#s3dol.base.BucketStore)

### *class* s3dol.StaticCredentials(access_key, secret_key, token=None)

Bases: [`CredentialProvider`](s3dol.connection.html.md#s3dol.connection.CredentialProvider)

An explicit key pair (optionally a session token).

#### NOTE
a *static* token expires and cannot refresh — prefer `profile=` or
a callable provider for STS/SSO. Pickling this object carries the secret
(that is what “explicit credentials” means); its repr does not.

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### *class* s3dol.Streamable(open_stream)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The value is whatever a zero-arg factory’s stream yields — for sources
that must be (re)opened at write time, e.g. an HTTP download. The factory
is called once per write, so unlike [`Chunks`](#s3dol.Chunks) this ref IS safely
re-writable (each write gets a fresh stream).

### *exception* s3dol.UploadBudgetExceeded

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

The tenant’s byte budget cannot cover this upload; no form was signed.

### s3dol.delete_bucket(endpoint, name, , force=False)

Delete bucket `name`. Refuses a non-empty bucket unless
`force=True`, in which case the cascade **paginates** — v0 listed one
page, deleted ≤1000 objects, then failed: a partial, non-idempotent
destruction (ADR-0010 §3). A free function, not a method (ADR-0011 D4).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.delete_many(store, keys)

Bulk delete (ADR-0010 §2; a free function per ADR-0011 D4 — keyed +
destructive + delegated is the census’s data-destroying shape).

Chunks at 1000 (AWS’s cap), parses the `Errors` list out of the HTTP
200 response, and on partial failure raises one [`S3PartialFailure`](#s3dol.S3PartialFailure)
carrying `.succeeded` and `.failures`. Absent keys are reported by S3
as `Deleted` — this function does **not** distinguish them (same
idempotency as `del store[k]`).

Keys are the *caller’s* keys: with the store held as an argument the
chain is resolved via `dol.inner_most_key` (alive store — no weakref
hole). Validate-the-target caveat (D4): the first argument must be an
s3dol store or a dol wrapper over one.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.diagnose(connection=None, , environ=None, aws_config=None, file=None, \*\*kwargs)

Print (and return) the resolution report. Never raises.

Call it either with a ready [`S3Connection`](s3dol.connection.html.md#s3dol.connection.S3Connection), with
`S3Connection` kwargs, or — the migration case — with exactly the kwargs
you pass `s3dol.store.S3Store` today (`bucket_name=`, `path=`,
`profile_name=`, …), which additionally prints the v0-vs-v1
divergence table.

```pycon
>>> report = diagnose(endpoint_url='http://localhost:9000',
...                   environ={}, file=io.StringIO())
>>> 'endpoint_url' in report and 'resolution' in report.lower()
True
```

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### s3dol.handles(store)

The sibling store of [`ObjectHandle`](#s3dol.ObjectHandle)s: `handles(s)[k]`.

* **Return type:**
  [`BucketHandles`](s3dol.capabilities.html.md#s3dol.capabilities.BucketHandles)

### s3dol.info(store)

The sibling store of [`ObjectInfo`](#s3dol.ObjectInfo): `info(s)[k]`.

* **Return type:**
  [`BucketInfo`](s3dol.capabilities.html.md#s3dol.capabilities.BucketInfo)

### s3dol.listed_info(store)

The sibling store of [`ObjectInfo`](#s3dol.ObjectInfo) built from one paginated LIST:
`listed_info(s)[k]` (a lazy snapshot; see [`BucketListedInfo`](#s3dol.BucketListedInfo)).

* **Return type:**
  [`BucketListedInfo`](s3dol.capabilities.html.md#s3dol.capabilities.BucketListedInfo)

### s3dol.prefixes(store)

The ‘directories’ one level under the store’s scope, \*\*relative to the
caller’s key space\*\* — one `ListObjectsV2(Delimiter='/')` reading
`CommonPrefixes` (which v0 parsed and then called from nowhere;
ADR-0009). Through a dol wrapper chain, the listing is rooted at the
caller’s scope (each prefix layer’s contribution) and results are mapped
back **outward** (`_key_of_id` per layer, innermost first — the inverse
walk `dol` does not provide; ADR-0011 D3). A wrapper whose codec cannot
express the scope raises — loud, not wrong.

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)

### s3dol.presigned_upload(store, , key_prefix, max_bytes, content_type, min_bytes=0, expires_in=900, budget=None, on_versioned_bucket='warn')

A presigned POST form for ONE new object under `key_prefix`.

`content_type` is required: pass the one type to accept, or `None` to
explicitly accept any (then a holder can store `text/html`: do not serve
such objects from a trusted origin).

`key_prefix` must end with the store’s delimiter (`tenant-7/` cannot
match `tenant-77/`) and is in the *store’s* key space (the store’s own
prefix is prepended, like every other s3dol key). The object key is
server-generated (`PresignedUpload.key`). Returns `None` for an
anonymous store (no signing capability, as [`s3dol.urls()`](#s3dol.urls) does).

When `budget` is given, the bucket’s versioning status is read first (one
`GetBucketVersioning` call): with versioning enabled a re-posted form
stores a new version each time, so `on_versioned_bucket` decides:
`'warn'` (default) emits `VersionedBucketWarning`, `'raise'`
refuses with [`ConfigurationError`](#s3dol.ConfigurationError), `'ignore'` skips the call.

Raises [`ConfigurationError`](#s3dol.ConfigurationError) for a bad prefix, size range,
`expires_in` or `on_versioned_bucket`, and [`UploadBudgetExceeded`](#s3dol.UploadBudgetExceeded)
if `budget` refuses.

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`PresignedUpload`](s3dol.uploads.html.md#s3dol.uploads.PresignedUpload)]

### s3dol.register_preset(preset, , overwrite=False)

Add a row. Open-closed: adding a provider is adding a row.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.resolve(spec, environ, aws_config=None)

Pure resolution: no `os.environ`, no disk, no network (ADR-0012 D2).

`environ` is a plain mapping (pass `os.environ` at real call sites,
a dict in tests). `aws_config` is a pre-loaded, credential-scrubbed
config mapping, or a thunk for one (`load_aws_config()`) — called only
if a rung needs it.

* **Return type:**
  [`Resolution`](s3dol.connection.html.md#s3dol.connection.Resolution)

### s3dol.s3_store(bucket, , prefix='', preset=None, connection=None, endpoint_url=None, region_name=None, profile=None, credentials=None, anon=False, on_missing_bucket='assume', readonly=False, value_codec=None, reads=None, writes=None)

One bucket as a `MutableMapping[str, bytes]` (a `Mapping` when
`readonly=True`).

Annotated (deliberately) as the abstract Mapping, **not** `-> BucketStore`:
with `value_codec` the concrete type is a generated `dol` wrapper class
that is *not* a `BucketStore` subclass. (v0 annotated its factory
`-> Store` and returned something that was not one; inverting that lie
is not an improvement — architecture.md.)

Connection axes (`preset`/`endpoint_url`/`region_name`/`profile`/
`credentials`/`anon`) are conveniences that build an
[`S3Connection`](s3dol.connection.html.md#s3dol.connection.S3Connection); pass `connection=` instead for
full control (they are mutually exclusive with it).

### s3dol.sub(store, prefix)

A store scoped to `prefix`, **in the caller’s key space** (D3).

Two branches, deliberately different (each documented in the ADR):

- **unwrapped s3dol store**: `leaf._with(prefix=...)` — cheap, keeps
  server-side prefix pushdown, returns the same class;
- **dol-wrapped store**: composes `Pipe(filt_iter.prefixes(p),
  KeyCodecs.prefixed(p))` over the *outer* store (filter FIRST — the only
  safe composition, ADR-0006 §1), which preserves the user’s codecs but
  loses pushdown (a full scan filtered client-side; D8 closed this: the
  cheap path is the unwrapped branch) and returns a different type.

### s3dol.urls(store, , expires_in=3600, client_method='get_object')

The sibling store of presigned URLs: `urls(s)[k]`.

* **Return type:**
  [`BucketUrls`](s3dol.capabilities.html.md#s3dol.capabilities.BucketUrls)

### Modules

| [`base`](s3dol.base.html.md#module-s3dol.base)                 | Layer B — close-to-metal stores over one bucket or one endpoint.                                                                                                                                          |
|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`capabilities`](s3dol.capabilities.html.md#module-s3dol.capabilities) | Keyed capabilities as sibling stores; non-keyed operations as free functions (ADR-0011 D2/D3/D4).                                                                                                         |
| [`connection`](s3dol.connection.html.md#module-s3dol.connection)     | `S3Connection` — the credential + endpoint SSOT, and the pure resolver.                                                                                                                                   |
| [`errors`](s3dol.errors.html.md#module-s3dol.errors)             | The s3dol error taxonomy and the single error-translation seam.                                                                                                                                           |
| [`presets`](s3dol.presets.html.md#module-s3dol.presets)           | Provider presets: config rows, not subclasses.                                                                                                                                                            |
| [`reads`](s3dol.reads.html.md#module-s3dol.reads)               | Read strategies (ADR-0005 §6): what `s[k]` returns.                                                                                                                                                       |
| [`recipes`](s3dol.recipes.html.md#module-s3dol.recipes)           | Layer C — the one-liner factory and codec facades.                                                                                                                                                        |
| [`store`](s3dol.store.html.md#module-s3dol.store)               | COMPAT SHIM — the v0 `S3Store`, forwarded onto the v1 machinery.                                                                                                                                          |
| [`testing`](s3dol.testing.html.md#module-s3dol.testing)           | `s3dol.testing` — a shipped in-memory fake and the exported conformance suite (ADR-0008). Downstream users can test services that take a store as a dependency with **no network, no docker, no moto**::. |
| [`uploads`](s3dol.uploads.html.md#module-s3dol.uploads)           | Direct-to-S3 browser uploads: a presigned POST scoped to a key prefix.                                                                                                                                    |
| [`values`](s3dol.values.html.md#module-s3dol.values)             | Value refs and the write domain (ADR-0005).                                                                                                                                                               |
| [`writes`](s3dol.writes.html.md#module-s3dol.writes)             | Write strategies (ADR-0005 §5): how `s[k] = v` reaches the wire.                                                                                                                                          |
