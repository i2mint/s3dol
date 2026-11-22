# s3dol.capabilities

Keyed capabilities as sibling stores; non-keyed operations as free
functions (ADR-0011 D2/D3/D4).

A keyed capability is a Layer B `KvReader` over the **same key space** whose
`__getitem__` returns the capability:

```default
BucketStore                     k -> bytes
BucketHandles  s3dol.handles(s) k -> ObjectHandle
BucketUrls     s3dol.urls(s)    k -> presigned URL (str) | None
BucketInfo     s3dol.info(s)    k -> ObjectInfo (one HeadObject per key)
BucketListedInfo  s3dol.listed_info(s)  k -> ObjectInfo (one LIST, a snapshot)
```

`__getitem__` is the one thing `dol` maps correctly at every wrapper depth,
so sibling stores are correct *by construction*: no `inner_most_key`, no
`wrapped_self`, no guard — immune to both the lost-reference hole (D1a) and
the non-`Store`-layer hole (D3a), the only form in ADR-0011 that is.

**The cost, stated plainly** (D2): a user who wraps the data store must wrap
the sibling in parallel:

```default
s = s3_store('bucket', prefix='p/')
h = s3dol.handles(s)
c = KeyCodecs.prefixed('x/')      # a user-applied key codec
s2, h2 = c(s), c(h)               # wrap BOTH
h2['f']                           # -> handle on 'p/x/f'; correct at any depth
```

The accessors therefore derive a sibling from an **unwrapped** store and
**raise** on a wrapped one, naming that remedy — s3dol will not guess a user’s
codec chain (that is dol#10).

Free functions (D3) resolve keys with the store held as an argument (alive, so
no weakref registry is consulted). Per D3a they are *more* reliable than
methods, not reliable: a hand-rolled non-`Store` passthrough layer in the
chain can still defeat the resolution — no docstring here claims “correct at
any wrapper depth”; only the sibling stores may claim that.

### Module Attributes

| [`DELETE_MANY_CHUNK`](#s3dol.capabilities.DELETE_MANY_CHUNK)   | AWS's DeleteObjects cap.   |
|----------------------------------------------------------------------|----------------------------|

### Functions

| [`delete_bucket`](#s3dol.capabilities.delete_bucket)(endpoint, name, \*[, force])   | Delete bucket `name`.                                                                                                                                                                                                   |
|-----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`delete_many`](#s3dol.capabilities.delete_many)(store, keys)                     | Bulk delete (ADR-0010 §2; a free function per ADR-0011 D4 — keyed + destructive + delegated is the census's data-destroying shape).                                                                                     |
| [`handles`](#s3dol.capabilities.handles)(store)                               | The sibling store of `ObjectHandle`s: `handles(s)[k]`.                                                                                                                                                                  |
| [`info`](#s3dol.capabilities.info)(store)                                  | The sibling store of `ObjectInfo`: `info(s)[k]`.                                                                                                                                                                        |
| [`listed_info`](#s3dol.capabilities.listed_info)(store)                           | The sibling store of `ObjectInfo` built from one paginated LIST: `listed_info(s)[k]` (a lazy snapshot; see [`BucketListedInfo`](#s3dol.capabilities.BucketListedInfo)).                                          |
| [`prefixes`](#s3dol.capabilities.prefixes)(store)                              | The 'directories' one level under the store's scope, \*\*relative to the caller's key space\*\* — one `ListObjectsV2(Delimiter='/')` reading `CommonPrefixes` (which v0 parsed and then called from nowhere; ADR-0009). |
| [`sub`](#s3dol.capabilities.sub)(store, prefix)                           | A store scoped to `prefix`, **in the caller's key space** (D3).                                                                                                                                                         |
| [`urls`](#s3dol.capabilities.urls)(store, \*[, expires_in, client_method]) | The sibling store of presigned URLs: `urls(s)[k]`.                                                                                                                                                                      |

### Classes

| [`BucketHandles`](#s3dol.capabilities.BucketHandles)(bucket, \*[, connection, ...])      | `k -> ObjectHandle` (key bound at construction; zero round-trips).     |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`BucketInfo`](#s3dol.capabilities.BucketInfo)(bucket, \*[, connection, prefix, ...]) | `k -> ObjectInfo` (one `HeadObject`).                                  |
| [`BucketListedInfo`](#s3dol.capabilities.BucketListedInfo)(bucket, \*[, connection, ...])   | `k -> ObjectInfo` from the listing rows, not one `HeadObject` per key. |
| [`BucketUrls`](#s3dol.capabilities.BucketUrls)(\*args[, expires_in, client_method])   | `k -> presigned URL` (zero object requests; `None` when anonymous).    |

### *class* s3dol.capabilities.BucketHandles(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectHandle` (key bound at construction; zero round-trips).

### *class* s3dol.capabilities.BucketInfo(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectInfo` (one `HeadObject`).

### *class* s3dol.capabilities.BucketListedInfo(bucket, , connection=None, prefix='', delimiter='/', reads=None, writes=None, on_missing_bucket='assume', strict_delete=False)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> ObjectInfo` from the listing rows, not one `HeadObject` per key.

The first access (a lookup or an iteration) pages through `ListObjectsV2`
once and keeps `{key: ObjectInfo}` (`size`, `last_modified`, `etag`,
`storage_class`; `content_type` and the other HEAD-only fields stay
`None`). It is **lazy and a snapshot**: later writes are not seen; make a
new `listed_info(store)` to refresh. A whole-bucket ETag comparison costs
one LIST per 1000 keys.

### *class* s3dol.capabilities.BucketUrls(\*args, expires_in=3600, client_method='get_object', \*\*kwargs)

Bases: [`BucketCollection`](s3dol.base.html.md#s3dol.base.BucketCollection), `KvReader`

`k -> presigned URL` (zero object requests; `None` when anonymous).
The canonical spelling of what `url_for` shims (ADR-0011 D3b).

### s3dol.capabilities.DELETE_MANY_CHUNK *= 1000*

AWS’s DeleteObjects cap. moto accepts 1001, so tier 2 can never catch a
missing chunker (ADR-0010 §2) — hence the constant is also unit-tested.

### s3dol.capabilities.delete_bucket(endpoint, name, , force=False)

Delete bucket `name`. Refuses a non-empty bucket unless
`force=True`, in which case the cascade **paginates** — v0 listed one
page, deleted ≤1000 objects, then failed: a partial, non-idempotent
destruction (ADR-0010 §3). A free function, not a method (ADR-0011 D4).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.capabilities.delete_many(store, keys)

Bulk delete (ADR-0010 §2; a free function per ADR-0011 D4 — keyed +
destructive + delegated is the census’s data-destroying shape).

Chunks at 1000 (AWS’s cap), parses the `Errors` list out of the HTTP
200 response, and on partial failure raises one `S3PartialFailure`
carrying `.succeeded` and `.failures`. Absent keys are reported by S3
as `Deleted` — this function does **not** distinguish them (same
idempotency as `del store[k]`).

Keys are the *caller’s* keys: with the store held as an argument the
chain is resolved via `dol.inner_most_key` (alive store — no weakref
hole). Validate-the-target caveat (D4): the first argument must be an
s3dol store or a dol wrapper over one.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.capabilities.handles(store)

The sibling store of `ObjectHandle`s: `handles(s)[k]`.

* **Return type:**
  [`BucketHandles`](#s3dol.capabilities.BucketHandles)

### s3dol.capabilities.info(store)

The sibling store of `ObjectInfo`: `info(s)[k]`.

* **Return type:**
  [`BucketInfo`](#s3dol.capabilities.BucketInfo)

### s3dol.capabilities.listed_info(store)

The sibling store of `ObjectInfo` built from one paginated LIST:
`listed_info(s)[k]` (a lazy snapshot; see [`BucketListedInfo`](#s3dol.capabilities.BucketListedInfo)).

* **Return type:**
  [`BucketListedInfo`](#s3dol.capabilities.BucketListedInfo)

### s3dol.capabilities.prefixes(store)

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

### s3dol.capabilities.sub(store, prefix)

A store scoped to `prefix`, **in the caller’s key space** (D3).

Two branches, deliberately different (each documented in the ADR):

- **unwrapped s3dol store**: `leaf._with(prefix=...)` — cheap, keeps
  server-side prefix pushdown, returns the same class;
- **dol-wrapped store**: composes `Pipe(filt_iter.prefixes(p),
  KeyCodecs.prefixed(p))` over the *outer* store (filter FIRST — the only
  safe composition, ADR-0006 §1), which preserves the user’s codecs but
  loses pushdown (a full scan filtered client-side; D8 closed this: the
  cheap path is the unwrapped branch) and returns a different type.

### s3dol.capabilities.urls(store, , expires_in=3600, client_method='get_object')

The sibling store of presigned URLs: `urls(s)[k]`.

* **Return type:**
  [`BucketUrls`](#s3dol.capabilities.BucketUrls)
