# ADR-0002: boto3 stays the engine; alternatives are optional, narrow, and later

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

The brief asked whether s3dol should keep `boto3` or move to something lighter — `minio`,
`s3fs`, `smart_open`, `aioboto3`, or `obstore` (Rust `object_store` bindings). The measured
case against boto3 is real: `import s3dol` costs **~167 ms** cumulative — boto3 ~141 ms and
`dol` ~61 ms (they overlap in shared stdlib), s3dol's own code ~2 ms.
One research pass recommended making `obstore` the default engine.

## Decision

**boto3/botocore remains the default and only required engine for v1.** Alternatives are
deferred behind a narrow protocol.

### Why

1. **The alternatives cannot back the whole surface.** `obstore` has no bucket
   create/delete/list operations at all, so the bucket level (`EndpointStore`, keys = bucket
   names) cannot be implemented on it. A "default engine" that can't serve a documented
   layer isn't a default.
2. **Everything portability-related is botocore-shaped.** The checksum fix that makes
   half the S3-compatible providers work is `botocore.Config(request_checksum_calculation=...)`.
   The error taxonomy keys on `ClientError.response['Error']['Code']`. The transfer
   strategies are `TransferConfig`/`upload_fileobj`. Choosing another engine discards the
   research that makes the package correct.
3. **The blocker cited for boto3 was not load-bearing.** The claim was that s3dol's
   `url_for(k, **params)` can't be honoured by obstore. The only external consumer calls it
   with one positional argument and no params. The genuine blockers are (1) above.
4. **A `botodol` is planned.** The owner intends a unified collections interface over AWS
   generally. Standardising s3dol on botocore keeps that path open; standardising on a
   Rust binding closes it.

### The import cost is addressed directly, not by switching engines

boto3 is imported **lazily**: `from __future__ import annotations`, `if TYPE_CHECKING:` for
types, and the client as a `functools.cached_property` on `S3Connection`. Constructing a
store performs no import of boto3 and no I/O.

Budget: **`import s3dol` adds < 10 ms on top of `import dol`**, enforced by a test that
measures the *delta*. An absolute budget is not achievable: measured cumulative import cost is
`dol` **61 ms**, boto3 141 ms, `s3dol` 167 ms — `dol` alone is twice any sub-30 ms target, and
it is a hard dependency of Layer B (which subclasses `dol.base.KvReader`) and of the
module-scope `wrap_kvs` recipes. Deferring `dol` too would mean making `S3Jsons`/`S3Texts`
`__getattr__`-lazy, which is not worth it. dol's own import cost is raised as an upstream
issue — it benefits every `*dol` package.

This is strictly better than switching engines, because it also gives us the
lazy/picklable connection that [ADR-0003](0003-provider-presets-and-capabilities.md)
and the multiprocessing use case need anyway.

### The seam for later

Layer B talks to the backend through a small set of operations (get, get-range, put,
put-stream, delete, delete-many, list, head, presign). v1 defines that boundary as a
module-internal protocol, implemented once by botocore. It is deliberately **not** a public
extension point in v1 — publishing a protocol with one implementer is how you get an
abstraction shaped like its only implementation.

`s3dol[fast]` (obstore for the object level only) and `s3dol.aio` are tracked in
[ADR-0009](0009-scope-and-deferrals.md).

## Consequences

**Buys.** All the provider-compatibility work stays applicable. No 0.x single-vendor
dependency in the required set. A fast import anyway.

**Costs.** We carry boto3's weight for users who only ever touch one object, and we inherit
botocore's config surface — including the checksum default that broke half the ecosystem
(see [ADR-0003](0003-provider-presets-and-capabilities.md)).

**Revisit when:** obstore grows bucket operations *and* an independent re-measurement in a
real environment confirms the import/throughput claims. The numbers behind the original
recommendation came from a throwaway venv and were not reproduced.

**What NOT to do.** Do not add a second engine "just for benchmarking". Two engines means
two error tables, two checksum stories, and two sets of provider quirks — the cost is not
in the adapter, it's in the compatibility matrix.
