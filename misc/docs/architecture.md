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
│ Layer C — recipes (s3dol.recipes)                                  │
│ s3_store(...) factory, codec stacks (S3Jsons, S3Texts, ...)        │
│ Built ONLY by wrap_kvs / Pipe composition. Never by subclassing.   │
├────────────────────────────────────────────────────────────────────┤
│ Layer B — close-to-metal (s3dol.base)                              │
│ BucketCollection/Reader/Store, Endpoint*, ObjectHandle.            │
│ Owns a normalized `prefix`. bytes in / bytes out, one error seam.  │
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

- **Layer B owns the prefix.** A bucket store carries a *normalized* `prefix` and addresses
  keys relative to it: `_id_of_key`/`_key_of_id` live in the leaf, the prefix is pushed into
  `ListObjectsV2(Prefix=…)`, it appears in `__repr__`, and sub-stores are built by
  `self._with(prefix=…)`. `dol` is used for **codecs, filtering and caching — not for prefix
  arithmetic.**

  > This reverses an earlier draft of this document, which put prefixing in a `dol` wrapper
  > above an absolute-keyed leaf. That does not work, and the reasons are recorded in
  > [ADR-0001](decisions/0001-layered-architecture.md) §"Why the prefix lives in the leaf".
  > In short: `dol` wrappers delegate unknown attributes with the **outer, unmapped** key, so
  > every capability method (`url_for`, `info`, `handle`, `sub`, `prefixes`, `delete_many`)
  > silently addresses the wrong object; and there is no channel for a wrapper to push its
  > prefix into the leaf's listing call, so every scoped listing becomes a full-bucket scan.

- **Layer C never subclasses.** If a recipe cannot be expressed as a composition of Layer B
  plus `dol` wrappers, that is a signal the capability belongs in Layer B as a parameter,
  not in Layer C as a subclass. This is the rule that keeps per-vendor classes
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
   └── BucketReader   (+ __getitem__ -> bytes, url_for, info, handle, sub, prefixes)
        └── BucketStore  (+ __setitem__ / __delitem__ / delete_many)

EndpointCollection    (Collection — __iter__ over bucket names)
   └── EndpointReader (+ __getitem__ -> BucketReader)
        └── EndpointStore  (+ __setitem__ / __delitem__ for buckets)
```

plus `ObjectHandle` — the escape hatch for one object, which is **not** a Mapping and is
where ranged reads, streaming, multipart and object metadata live.

`EndpointStore`, not `BucketsStore`: naming the *containing* resource (as `azuredol` does with
`ContainerStore`/`AccountStore`) avoids shipping `BucketStore` and `BucketsStore` — two of the
package's most-used classes, one silent `s` apart, with **opposite key spaces**, where a typo
yields a working, silently-wrong store.

| Operation | Contract |
|---|---|
| `__getitem__(k)` | Returns `bytes`. `KeyError` iff absent. Auth/config errors re-raised untouched. |
| `__setitem__(k, v)` | `v` in a closed, documented union (see [0005](decisions/0005-large-object-io.md)). Replaces. |
| `__delitem__(k)` | **Idempotent.** S3's `DeleteObject` returns 204 for an absent key; raising `KeyError` would require a `HeadObject` probe — banned below, doubles the cost of every delete, and is TOCTOU-racy. `strict_delete=True` opts into the probe and is documented as costing an extra request. |
| `__contains__(k)` | One `HeadObject`. `False` iff absent; **raises** on auth/config failure. See [0004](decisions/0004-error-taxonomy.md) §2 for the HEAD ambiguity. |
| `__iter__()` | Lazy paginated `ListObjectsV2(Prefix=self.prefix)`. **Raises** if the bucket is missing or unlistable — never yields empty. |
| `__len__()` | **Not implemented.** Raises `TypeError` with guidance. See [0008](decisions/0008-testing-architecture.md) §cost model. |
| `__repr__` | bucket, prefix, endpoint host, mode. No secrets, no addresses. |
| `url_for(k, ...)` | Presigned URL. Zero object requests. **Always SigV4** — see [0003](decisions/0003-provider-presets-and-capabilities.md) §4. **The URL cannot outlive the signing credential**: with STS/SSO/instance-profile credentials it dies with the session (default 1 h) regardless of `expires_in`. Capped at 604800 s (SigV4's limit, which botocore does not enforce); clamps and warns when the resolved credentials expire sooner. |
| `sub(prefix)` | A `BucketReader`/`BucketStore` with an extended, normalized prefix. Zero round-trips. |
| `info(k)` | One `HeadObject` → size, mtime, etag, content-type, storage class, restore status. |
| `prefixes(p='')` | One `ListObjectsV2(Delimiter='/')` → `CommonPrefixes`, relative to the store's own prefix. |
| `delete_many(keys)` | See [0010](decisions/0010-bucket-and-bulk-operations.md). |

**The rule for what may join this table** (it is otherwise how a surface grows to twenty): a
method may be added iff it takes a key and is either a pure read of metadata or an address
(`info`, `url_for`), or returns a store or handle (`sub`, `handle`, `prefixes`). Anything that
mutates, batches, or takes non-key arguments belongs on `ObjectHandle` or a recipe.
`delete_many` is an explicit, named exception, admitted only because the cost difference
against a `__delitem__` loop is an order of magnitude.

### Layer C — `s3dol.recipes`

```python
s3_store(
    bucket, *, prefix='', preset=None, connection=None,
    value_codec=None, on_missing_bucket='assume', anon=False, readonly=False,
) -> MutableMapping[str, bytes]

S3Texts = wrap_kvs(BucketStore, value_codec=ValueCodecs.str_to_bytes())
S3Jsons = wrap_kvs(
    BucketStore,
    value_encoder=lambda o: json.dumps(o).encode(),
    value_decoder=json.loads,
)
```

Two footnotes that cost real debugging time if forgotten:

- `s3_store` is annotated `-> MutableMapping[str, bytes]`, **not** `-> BucketStore`: a `dol`
  wrap returns a new class that is not a `BucketStore` subclass, and the concrete type varies
  with the arguments. (v0 annotated its factory `-> Store` and returned something that was
  not one; inverting that lie is not an improvement.)
- `S3Jsons` uses `value_encoder`/`value_decoder`, not `value_codec=ValueCodecs.json()` —
  `ValueCodecs.json()` encodes to **`str`**, which Layer B rejects ([0005](decisions/0005-large-object-io.md) §2),
  and `value_codec=` does not compose via `Pipe` (`'Pipe' object has no attribute 'decoder'`).
  A conformance assertion checks that every shipped recipe's encoder output is a member of
  `BytesSource`.

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
  base.py         Layer B (owns prefix)
  recipes.py      Layer C — s3_store(...), codec facades
  diagnose.py     s3dol.diagnose() — prints resolved endpoint/region/credential SOURCE
  store.py        COMPAT SHIM — legacy S3Store, DeprecationWarning, removed in v2
  testing.py      in-memory fake + the exported conformance suite
  tests/
    util.py       KEEP — py2store imports two functions from here
```

`diagnose.py` is small but not optional: it is the step-0 safety mechanism for a change that
can silently move a live data target ([ADR-0007](decisions/0007-naming-and-compatibility.md) §5).

**Public API** — `s3_store`, `BucketStore`, `BucketReader`, `EndpointStore`, `ObjectHandle`,
`S3Connection`, `Filepath`/`Chunks`/`Streamable`, the error classes, `diagnose`. Everything
else is implementation.

`store.py` must survive as an importable module: both external dependents do
`from s3dol.store import S3Store`, not `from s3dol import S3Store`.

---

## What we explicitly do NOT do

- **Per-vendor subclasses.** A provider is a row in `presets.py`, never a class. The v0
  `SupabaseS3BucketDol` is the anti-pattern; its behaviour becomes client configuration
  ([0003](decisions/0003-provider-presets-and-capabilities.md) §Supabase).
- **`type(self)(**self.__dict__)` for sub-stores.** Fragile the moment any attribute isn't
  an `__init__` arg. Sub-stores come from an explicit `self._with(prefix=…)` that names the
  fields it carries (`azuredol/base.py:125` is the reference).
- **Probe-then-act.** No `head_bucket` before a write. Bucket policy is a construction-time
  *parameter*; only `on_missing_bucket='raise'` performs I/O, and it says so
  ([0010](decisions/0010-bucket-and-bulk-operations.md)).
- **Cascading deletes as a side effect.** `del endpoint[name]` refuses a non-empty bucket;
  `endpoint.delete(name, force=True)` is the explicit form.
- **`__len__` on a bucket store.** Unbounded pagination cost.
- **Silent empties.** Anywhere.
- **Passing `EncodingType` to a list call.** botocore sets it itself *and* decodes the
  response — but only when it set it. Passing it explicitly disables the decoder and returns
  percent-encoded keys that no longer address their objects
  ([0006](decisions/0006-key-scoping-and-dol-fixes.md) §4).

## Prior art

`azuredol` went through this refactor first, and where the two packages face the same
question **we deliberately give the same answer** — the `*dol` family's value is that one
adapter reads like the next. Concretely inherited: the layering, the Collection→Reader→Store
triads, the single error-translation decorator, no `__len__`, no global client cache, real
reader classes, and the refusal to cascade-delete a container (its `design_decisions.md` §12
cites s3dol v0 by name: *"This is convenient and dangerous. We refuse it."*).

**Read `azuredol`'s code, not only its docs.** Its `architecture.md` says container stores
are wrapped with `dol`'s `mk_relative_path_store`; its actual `base.py` does prefix
arithmetic in the leaf and uses `mk_relative_path_store` **zero times**. An earlier draft of
this document adopted the documented design rather than the shipped one, and that error is
the origin of the rewrite recorded in
[ADR-0001](decisions/0001-layered-architecture.md) §"Why the prefix lives in the leaf".
