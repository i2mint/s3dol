# ADR-0009: What v1 contains, what waits, and where the `s3dol` / `botodol` line runs

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

Two forces pull opposite ways.

**Pull toward more:** S3 has ~108 operations, and a surprising number map beautifully onto
`collections.abc`. In-flight multipart uploads are the single best fit in the whole API —
`ListMultipartUploads` + `AbortMultipartUpload` is literally a `Mapping` whose `__delitem__`
aborts, and orphaned uploads are the classic silent S3 bill. Object versions, tags, user
metadata and bucket configuration all have clean Get/Put/Delete triples.

**Pull toward less:** zarr is the cautionary tale. Its v3 rewrite replaced a
`MutableMapping` store interface with a 15-method async ABC plus capability flags, and in
doing so **killed five of its own backends** — `DBMStore`, `LMDBStore`, `SQLiteStore`,
`MongoDBStore`, `RedisStore` all "do not have an equivalent in Zarr-Python 3" — while
leaving vestigial flags like `supports_partial_writes -> Literal[False]`.

The eight research passes collectively proposed ~6 Protocols, 4 strategy slots, 16 presets,
an async submodule, 8 new store families and 6 upstream `dol` changes. Shipping that at once
is how a package becomes unimplementable.

## Decision

### v1.0 — the scope

```
connection.py   S3Connection: credential + endpoint SSOT; lazy, picklable, redacting
presets.py      Preset + Capabilities registry
errors.py       one translate_s3_errors seam + the taxonomy
values.py       Filepath / Chunks / Streamable + as_fileobj
writes.py       write strategies (simple / transfer / multipart)
reads.py        read strategies (bytes / stream / ranged / to-file)
base.py         BucketCollection/Reader/Store, BucketsCollection/Reader/Buckets, ObjectHandle
stores.py       relative-key stores + codec facades
recipes.py      s3_store(...) and friends
store.py        deprecated shim
testing.py      in-memory fake + exported conformance suite
```

Plus, from the "beyond blobs" analysis, the items that are **free or nearly so** because they
fall out of work already being done:

- **`ObjectInfo` from LIST metadata.** Every `ListObjectsV2` response already carries size,
  mtime, ETag and storage class, and v0 throws them away. A `store.info(k)` (one HEAD) and a
  cheap listing-derived metadata view cost almost nothing and serve nearly every use case.
- **Prefix tree via `CommonPrefixes`.** `Resp.common_prefixes` has existed in `utility.py`
  since 2023 and is **called from nowhere**. `store.prefixes()` is one LIST with a delimiter.
- **A presigned-URL view.** `url_for` already exists; a `Mapping` face over it is trivial.

The multipart **parts** store (`MutableMapping[int, bytes]`, filled then committed) is built
as an *internal mechanism* of the write strategy in v1 — it is how `s[k] = v` stays pure —
but it is **not** exported as a public Mapping until v1.x. There is no `DeletePart`
operation, so its `__delitem__` would be a lie.

### v1.x — deferred, tracked, in this order

1. **In-flight multipart uploads as a `Mapping`** (`del uploads[(key, upload_id)]` aborts).
   Highest portability of the deferred set, real cost savings, cleanest fit.
2. **Object versions** (+ delete-marker view).
3. **Object tags** and **user metadata** — natural, but portability 2/5 and 4/5; each needs
   capability detection. Note user-metadata *writes* are a `CopyObject`, which is surprising
   enough to document loudly.
4. **Bucket configuration as one `Mapping`** — `cfg['lifecycle'] = {...}` over the
   Get/Put/Delete triples.
5. **Object annotations** — natively a per-object `MutableMapping[str, bytes]`, and the first
   S3 feature that is a nested store rather than a flat blob store. Deferred not for design
   reasons but for two hard facts: **no S3-compatible provider implements it** (portability
   0/5), and it needs `botocore>=1.43.31`, too fresh for a storage library's hard floor.
   Feature-detect with `hasattr(client, 'put_object_annotation')`.
6. **fsspec adapter** (`to_fsspec` / `from_fsspec`) — one adapter buys pandas, dask, pyarrow
   and zarr-v3-via-`FsspecStore`.
7. **`s3dol[fast]`** — obstore for the object level only, if re-measurement justifies it.
8. **`s3dol.aio`** — async, mirroring the sync surface.

### Never

- `GetObjectTorrent` — dead.
- `SelectObjectContent` — AWS closed it to new customers in July 2024 and points at Athena /
  Object Lambda instead. Building a Mapping over a service new users cannot enable is
  negative value.
- **Native async `Store` ABC / zarr-v3 store.** An s3dol store is already a working **zarr
  v2** store for free. zarr v3 deliberately left `MutableMapping` behind for async and
  byte-range coalescing; chasing it means becoming a different package. We adapt (via fsspec)
  rather than compete.

### The `s3dol` / `botodol` line

Three mechanical tests, applied in order.

**Test 1 — the endpoint test (decisive).** *Can it be reached through the same
`boto3.client('s3', endpoint_url=...)` a MinIO or R2 user already holds?* If yes → s3dol.

This is a fact about botocore, not a taste judgement: `s3control` is a **separate service
model** with a different endpoint, every operation takes a required `AccountId`, and **no
S3-compatible provider implements any of it**. Same for `s3tables`, `s3vectors`,
`s3outposts`, `glacier`. Drawing the line here yields exactly "everything portable is
inside", and no case ever needs arguing.

**Test 2 — the key-shape test.** If the natural key is an ARN or an
`(AccountId, Region, Name)` triple rather than a bucket name or object key → botodol. Storage
keys are strings a user typed; control-plane keys are identities AWS minted.

**Test 3 — the value test.** If the value is a resource with a lifecycle you *poll* rather
than data or a config document → botodol. There is no `DeleteJob` for a Batch Operations job,
only `UpdateJobStatus(Cancelled)` — a `MutableMapping` whose `__delitem__` means "please try
to cancel" is a lie.

**→ botodol, never s3dol:** all `s3control` operations (access points, batch jobs, Storage
Lens, Access Grants, MRAP), `s3tables`, `s3vectors`, `s3outposts`, `glacier`, Object Lambda.

**Two apparent exceptions that the tests resolve correctly:**

- **Access points** are *consumed* as bucket names — the API docs say to pass the ARN in
  place of the bucket. So s3dol accepts an access-point ARN wherever it accepts a bucket
  name (one line in the bucket-name normalizer). *Administering* them needs `s3control` →
  botodol. The line runs between using and administering, and Test 1 puts it there.
- **S3 Express / directory buckets** need a zonal endpoint, `CreateSession`, `/`-only
  delimiters and no `UploadIdMarker` pagination. That is one row in the preset registry — the
  same mechanism that describes R2 and Supabase. No new abc, no new module.

**The obligation this creates:** the spec-driven sub-resource store and the paginated-list
store are **boto-generic, not S3-specific** — the same code will serve DynamoDB tables, SQS
queues, Secrets Manager secrets. When they get built (v1.x, item 4), they go in a documented
public module and **botodol depends on s3dol** for them. Do not build a shared base package
speculatively; do not let botodol reimplement them. If they prove fully boto-agnostic,
promote to `dol` later — cheap in that direction, expensive in reverse.

## Consequences

**Buys.** A v1 that can actually ship, whose every element is 4/5 or 5/5 on portability. A
written, mechanical rule for future scope arguments.

**Costs.** Several genuinely nice interfaces wait. The `Capabilities` table exists in v1 but
is barely exercised until the low-portability families arrive in v1.x — accepted, because
retrofitting capability declaration later is much worse.

**Enforcement.** A line budget, and one rule: **no new `Protocol` without two implementers.**
