# ADR-0010: Bucket-existence policy and bulk operations

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

Three surfaces were load-bearing in earlier ADRs but appeared only as prose, with no default,
no value domain and no owner: `on_missing_bucket`, `delete_many`, and (until
[ADR-0003](0003-provider-presets-and-capabilities.md) §5) `anon`. Each is reachable from the
README's first ten lines, so "unspecified" is not a neutral state.

`on_missing_bucket` was additionally **self-contradictory** across three documents: it replaces
v0's `make_bucket` tri-state and is said to be "decided once, at construction", but `'raise'`
requires *knowing* the bucket is missing, which costs a `HeadBucket` — I/O in a constructor
that [ADR-0002](0002-boto3-as-engine.md) promises is I/O-free, or else the probe-then-act that
the architecture bans.

Compounding it: `HeadBucket` requires `s3:ListBucket`, which the canonical least-privilege
policy omits ([ADR-0004](0004-error-taxonomy.md) §3), so a probing default would 403 for users
who can read and write every object perfectly well.

## Decision

### 1. `on_missing_bucket='assume'` is the default, and it performs no I/O

| value | behaviour |
|---|---|
| **`'assume'`** (default) | Never probes. A missing bucket surfaces as `BucketNotFound` on the first real operation. Costs nothing, races nothing. |
| `'create'` | **Recover, don't probe.** Attempt the write; on `NoSuchBucket`, `CreateBucket` then retry once. |
| `'raise'` | Explicit opt-in that performs **one `HeadBucket` at construction**, documented as such. Only `('404', 404)` means missing; 403 raises `AccessDenied` naming `s3:ListBucket`. |

`'assume'` is right because the loud failure is already free: every real operation returns
`NoSuchBucket`, so a probe buys nothing but latency and a permission requirement. This also
resolves the constructor contradiction — only `'raise'` does I/O, and its docstring says so.

`'create'` has two details that bite:

- `CreateBucket` needs `CreateBucketConfiguration={'LocationConstraint': region}` **everywhere
  except `us-east-1`**, where passing it is an error.
- It is idempotent only in `us-east-1`; elsewhere a re-create raises `BucketAlreadyOwnedByYou`
  (409), which must be tolerated.

The compat shim maps `make_bucket=True → 'create'`, `make_bucket=False → 'raise'`,
`make_bucket=None → 'assume'`. (`lacing` passes `make_bucket=True` today, so this mapping is
exercised by a real dependent.)

**README consequence, stated so it isn't rediscovered:** with `'assume'`, the two-line
quickstart `s = s3_store('my-bucket'); s['k'] = b'v'` raises on a bucket that doesn't exist
yet. That is correct — silently creating a bucket from a typo is the v0 behaviour this whole
ADR set exists to remove — but the README must show `on_missing_bucket='create'` in the
"start from nothing" example rather than pretending the happy path needs no argument.

### 2. `delete_many(keys)`

Chunks at **1000** (AWS's cap; moto accepts 1001, so tier 2 cannot catch a missing chunker),
parses the `Errors` list out of what is an **HTTP 200** response, and on partial failure raises
a single `S3PartialFailure(S3Error)` carrying `.succeeded: list[str]` and
`.failures: dict[str, S3Error]`.

**Not an `ExceptionGroup`** — that is 3.11+, and `requires-python` is `>=3.10`.

`DeleteObjects` reports **absent keys as `Deleted`**, so `delete_many` does not distinguish
them. That differs from `__delitem__` only in that neither raises — both are idempotent
([architecture.md](../architecture.md) contract table).

When `Capabilities.batch_delete=False` (GCS, and any provider that rejects the mandatory
`x-amz-checksum-crc32` on `DeleteObjects`), the emulation loops `DeleteObject` and produces the
**identical exception object**. [ADR-0003](0003-provider-presets-and-capabilities.md) §2's
"the observable result is identical; only cost differs" holds for present keys; neither form
distinguishes absent ones.

### 3. Cascading bucket deletion stays explicit

`del endpoint[name]` raises `BucketNotEmpty` if the bucket has objects.
`endpoint.delete(name, force=True)` is the documented cascading form, and it **paginates** —
v0's version listed one page and deleted at most 1000 objects before failing on
`delete_bucket`, i.e. a partial, non-idempotent destruction.

## Consequences

**Buys.** A default that is safe, free, and needs no permission the caller doesn't already
have. Bulk delete with honest partial-failure semantics on a 3.10 floor. No probe-then-act
anywhere.

**Costs.** `'assume'` means a mistyped bucket name is not caught until the first operation —
a slightly later, but still loud, failure. The `S3PartialFailure` type is one more exception
for users to learn, justified because the alternative (an HTTP 200 whose failures are silently
in a list nobody reads) is exactly the class of bug this ADR set targets.

**What NOT to do.** Do not make `'raise'` the default "to be safe" — it is the option that
costs a round-trip and a permission. Do not use `ExceptionGroup` while 3.10 is supported. Do
not let `delete_many` claim it reports absent keys.
