# s3dol — Architecture (v1 target)

`s3dol` exposes **S3 and S3-compatible object storage** as `dol`-style `Mapping` /
`MutableMapping` interfaces. This document is the single source of truth for the package's
layering. The *why* behind each defaulted choice lives in [decisions/](decisions/).

> **Status: design.** This describes the v1 target, not the shipped code. v0.1.x has a
> different (and in places broken) shape — see
> [decisions/0009-scope-and-deferrals.md](decisions/0009-scope-and-deferrals.md) for what
> lands when, and [decisions/0007-naming-and-compatibility.md](decisions/0007-naming-and-compatibility.md)
> for the migration.

---

## Goals

1. **Pythonic.** `s[k]`, `s[k] = v`, `del s[k]`, `k in s`, `for k in s:` is the surface.
   Everything else is opt-in.
2. **`dol` is the base.** Key transforms, prefix scoping, codecs, caching and filtering come
   from `dol`. s3dol writes S3 knowledge only.
3. **Portable across S3-compatible backends** — AWS, MinIO, R2, Scaleway, Hetzner, Backblaze,
   Wasabi, Ceph, Supabase, DigitalOcean, Tigris — with AWS as the reference semantics.
4. **Never silently wrong.** A wrong answer is worse than an error in every use case we
   serve. No operation returns empty-on-failure, and no explicit argument is silently
   ignored.
5. **Big objects are ordinary.** Reading and writing must never require the whole value in
   memory, and that must be reachable *through the Mapping interface*.
6. **Testable without a cloud**, by us and by our users.

## Non-goals for v1

Async, an fsspec filesystem adapter, an obstore engine, and Mapping interfaces over
non-blob S3 resources (versions, tags, bucket config, in-flight uploads). All are tracked;
see [decisions/0009](decisions/0009-scope-and-deferrals.md).

---

## The four layers

```
┌────────────────────────────────────────────────────────────────────┐
│ Layer D — recipes (s3dol.recipes)                                  │
│ s3_store(...) / s3(...) factories, codec stacks (S3Jsons, ...)     │
│ Built ONLY by wrap_kvs / Pipe composition. Never by subclassing.   │
├────────────────────────────────────────────────────────────────────┤
│ Layer C — relative-key stores (s3dol.stores)                       │
│ Prefix scoping via dol. Sub-stores. This is what users hold.       │
├────────────────────────────────────────────────────────────────────┤
│ Layer B — close-to-metal (s3dol.base)                              │
│ BucketCollection/Reader/Store, Buckets, ObjectHandle.              │
│ ABSOLUTE keys, bytes in / bytes out, one error seam, no codecs.    │
├────────────────────────────────────────────────────────────────────┤
│ Layer A — connection (s3dol.connection)                            │
│ S3Connection: the credential + endpoint SSOT. Lazy, picklable,     │
│ redacting. The dependency-injection seam.                          │
├────────────────────────────────────────────────────────────────────┤
│                    boto3 / botocore                                │
└────────────────────────────────────────────────────────────────────┘
```

Every public class belongs to exactly one layer. No mixing. Two rules make the layering
load-bearing rather than decorative:

- **Layer B keys are absolute.** All prefix arithmetic happens in Layer C, in `dol`. A
  Layer B store addresses the bucket's real keyspace, which is what makes `url_for`,
  `info`, and the transfer strategies correct by construction — they operate on the key S3
  actually sees.
- **Layer D never subclasses.** If a recipe cannot be expressed as a composition of Layer C
  plus `dol` wrappers, that is a signal the capability belongs in Layer B as a parameter,
  not in Layer D as a subclass. This is the rule that keeps per-vendor classes
  (`SupabaseS3BucketDol`) from reappearing.

### Layer A — `s3dol.connection`

Owns the expensive resource (the botocore client) and *all* credential/endpoint
resolution. See [decisions/0002](decisions/0002-boto3-as-engine.md) and
[decisions/0003](decisions/0003-provider-presets-and-capabilities.md).

```python
@dataclass(frozen=True)
class S3Connection:
    preset: str | Preset | None = None      # 'aws' | 'minio' | 'r2' | ...
    profile_name: str | None = None
    endpoint_url: str | None = None
    region_name: str | None = None
    credentials: Credentials | None = None  # explicit; None => resolve the chain
    anon: bool | Literal['auto'] = False
    client_config: dict = field(default_factory=dict)
```

Three properties matter and are each tested:

- **Lazy.** The client is a `cached_property`; constructing a connection performs no I/O and
  never raises for missing credentials.
- **Picklable.** The connection carries a *spec*, not a client. `__getstate__` drops the
  cached client so stores survive `ProcessPoolExecutor` and Dask.
- **Redacting.** No secret appears in `repr`, `str`, or any surviving local.

### Layer B — `s3dol.base`

Follows `dol.filesys`' triangle, in S3 vocabulary:

```
BucketCollection      (Collection — __iter__ over object keys)
   └── BucketReader   (+ __getitem__ -> bytes, url_for, info, handle)
        └── BucketStore  (+ __setitem__ / __delitem__)

BucketsCollection     (Collection — __iter__ over bucket names)
   └── BucketsReader  (+ __getitem__ -> BucketReader)
        └── Buckets    (+ __setitem__ / __delitem__ for buckets)
```

plus `ObjectHandle` — the escape hatch for one object, which is **not** a Mapping and is
where ranged reads, streaming, multipart and object metadata live.

| Operation | Contract |
|---|---|
| `__getitem__(k)` | Returns `bytes`. `KeyError` iff absent. Auth/config errors re-raised untouched. |
| `__setitem__(k, v)` | `v` in a closed, documented union (see [0005](decisions/0005-large-object-io.md)). Replaces. |
| `__delitem__(k)` | `KeyError` iff absent. |
| `__contains__(k)` | One `HeadObject`. `False` iff absent; **raises** on auth/config failure. |
| `__iter__()` | Lazy paginated `ListObjectsV2`. **Raises** if the bucket is missing or unlistable — never yields empty. |
| `__len__()` | **Not implemented.** Raises `TypeError` with guidance. See [0008](decisions/0008-testing-architecture.md) §cost model. |
| `__repr__` | bucket, prefix, endpoint host, mode. No secrets, no addresses. |
| `url_for(k, ...)` | Presigned URL. Zero object requests. Prefix-aware. |

### Layer C — `s3dol.stores`

Prefix scoping, delegated to `dol` — but **only in its safe composition**. This is a
correctness requirement, not an optimization; see
[decisions/0006](decisions/0006-key-scoping-and-dol-fixes.md), which is the most important
document here.

### Layer D — `s3dol.recipes`

```python
s3_store(bucket, *, prefix='', preset=None, connection=None, codec=None, ...) -> BucketStore
S3Jsons  = wrap_kvs(BucketStore, value_codec=ValueCodecs.json())
S3Texts  = wrap_kvs(BucketStore, value_codec=ValueCodecs.str_to_bytes())
```

---

## Module layout

```
s3dol/
  __init__.py     lazy PEP-562 __getattr__, explicit __all__, TYPE_CHECKING imports
  connection.py   S3Connection, credential/endpoint resolution + precedence
  presets.py      Preset + Capabilities frozen dataclasses; the provider registry
  errors.py       translate_s3_errors seam; the exception taxonomy
  values.py       Filepath / Chunks / Streamable refs; as_fileobj singledispatch
  writes.py       write strategies (simple / transfer / multipart)
  reads.py        read strategies (bytes / stream / ranged / to-file)
  base.py         Layer B
  stores.py       Layer C + Layer D codec facades
  recipes.py      s3_store(...) and friends
  store.py        COMPAT SHIM — legacy S3Store, DeprecationWarning, removed in v2
  testing.py      in-memory fake + the exported conformance suite
  tests/
    util.py       KEEP — py2store imports two functions from here
```

`store.py` must survive as an importable module: both external dependents do
`from s3dol.store import S3Store`, not `from s3dol import S3Store`.

---

## What we explicitly do NOT do

- **Per-vendor subclasses.** A provider is a row in `presets.py`, never a class. The v0
  `SupabaseS3BucketDol` is the anti-pattern; its behaviour becomes client configuration
  ([0003](decisions/0003-provider-presets-and-capabilities.md) §Supabase).
- **`type(self)(**self.__dict__)` for sub-stores.** Fragile the moment any attribute isn't
  an `__init__` arg. Sub-stores come from `dol`.
- **Probe-then-act.** No `head_bucket` before a write. Bucket policy is decided once, at
  construction.
- **Cascading deletes as a side effect.** `del buckets[name]` refuses a non-empty bucket;
  `buckets.delete(name, force=True)` is the explicit form.
- **`__len__` on a bucket store.** Unbounded pagination cost.
- **Silent empties.** Anywhere.

## Prior art

`azuredol` went through this refactor first; its
[design_decisions.md](https://github.com/i2mint/azuredol) is the direct ancestor of this
layout, and several of its sections cite s3dol v0 as the pattern being rejected. Where the
two packages face the same question, **we deliberately give the same answer** — the
`*dol` family's value is that one adapter reads like the next.
