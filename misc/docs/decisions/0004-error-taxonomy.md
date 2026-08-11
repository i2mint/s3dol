# ADR-0004: One error-translation seam, and a taxonomy that never lies

- **Status:** Accepted
- **Date:** 2026-08-10
- **Addresses:** [discussion #6](https://github.com/i2mint/s3dol/discussions/6)

## Context

v0.1.x defines seven exception classes in `utility.py`. Five are never raised. One
(`KeyNotValidError`) is *caught* at `base.py:132` and raised nowhere, so that `except` clause
is dead code. Meanwhile the real error handling is string-sniffing spread across the
codebase: `e.response["Error"]["Code"] == "404"`, `"404" in str(e)`, and a bare
`except ClientError: return False` in `_bucket_exists` that reports *every* failure —
expired token, missing permission, wrong endpoint — as "the bucket doesn't exist".

Discussion #6 proposes an `S3KeyError(KeyError)` carrying a class-level registry of backend
exception types plus a `register` classmethod.

### The measured failure landscape

Verified against botocore 1.40.53 and moto 5.2.2:

| Operation | missing target | `Error.Code` | HTTP | caught by `client.exceptions.NoSuchKey`? |
|---|---|---|---|---|
| `GetObject` | key | `NoSuchKey` | 404 | **yes** |
| `HeadObject` | key | `404` | 404 | **no** |
| `HeadBucket` | bucket | `404` | 404 | **no** |
| `DeleteObject` | key | — | 204 | *no error at all* |
| `DeleteObjects` | keys | — | 200 | reported as `Deleted`, not an error |

`HEAD` has no response body, so there is no XML error document for botocore to model — which
is why the modelled exception never fires and code that catches `NoSuchKey` around a
`head_object` silently never matches. This is the single most common S3 error-handling bug
and v0 has it.

Provider divergence compounds it: **Supabase returns 400 where AWS returns 404** on
`HeadBucket`, and several providers return 403 to hide 404.

## Decision

### 1. One seam

A single `@translate_s3_errors(...)` decorator in `errors.py` is the **only** place that
catches botocore exceptions in Layer B. Everything else propagates. This is `azuredol` §4,
and it is what makes the auth-vs-not-found distinction auditable from one file.

**Auth, config and transport errors are never translated to `KeyError`.** A missing
permission must not look like a missing key — that is exactly the confusion that makes
`_bucket_exists` dangerous today.

### 2. Classification is a table keyed on `(operation, code, status)`, not on exception type

Because the modelled exceptions are unreliable (above), classification reads
`ClientError.response['Error']['Code']` and the HTTP status, through two tiny predicates
(`code_of`, `status_of`). The table is per-provider-overridable, which is how Supabase's
400-means-404 is handled without any code branch.

**The operation is part of the key because HEAD has no body.** botocore synthesizes the code
from the HTTP status, so on real AWS `HeadObject` against a missing *key* and against a
missing *bucket* are **both** `('404', 404)` — indistinguishable. AWS documents that the exact
error is not retrievable for HEAD.

This ambiguity must not be resolved by guessing, and moto will not warn you: moto returns an
error **body** on HEAD where AWS returns none, so `head_object` against a missing bucket gives
`Code='NoSuchBucket'` under moto and `Code='404'` in production. `'k' in store` against a
typo'd bucket is therefore green in tiers 1–2 and silently **`False`** in production — the
exact silent-empty this package bans, produced by the taxonomy itself.

Resolution: on `('HeadObject', '404', 404)`, `__contains__` returns `False` only if the
connection has already proven the bucket reachable this session; otherwise it performs one
`HeadBucket` disambiguation, cached on the connection. Add `405` (delete-marker HEAD on a
versioned bucket) and `301` (wrong region) to the table.

Testable without a network: the classifier takes a synthesized `ClientError`, so the bulk of
error tests are pure unit tests — including body-less HEAD errors, **which moto cannot
produce**.

### 3. The taxonomy

> **Extended by [ADR-0012](0012-credential-and-endpoint-resolution.md) §D1** with the
> configuration/spec-time branch, which resolves before any request is made and therefore has no
> `(operation, code, status)` row: `ConfigurationError(S3Error, ValueError)` with
> `PresetConflict`, `PresetHostMismatch`, `MissingEndpoint`, `MissingPresetParam`,
> `UnknownPresetParam`, `InvalidEndpoint`. Plus a warning tree, `S3DolWarning(UserWarning)` with
> `S3DolResolutionChanged`, `AmbiguousResolution`, `AnonymousFallback` —
> `filterwarnings('error', category=S3DolWarning)` is the strict mode, so there is no `strict=`
> kwarg. Two bans belong here too: `anon=True` with credentials or a profile, and `anon=True`
> with `deny_means_absent=True` (measured under moto, anonymous HEAD gives 403 on a present key
> and 404 on an absent one, so every key reads as absent — a silently empty store).

| Condition | Raises | Rationale |
|---|---|---|
| object absent | `ObjectNotFound(KeyError)` | Mapping contract |
| bucket absent (object op) | `BucketNotFound(KeyError)` | still a key-space problem for the caller |
| bucket absent (bucket op) | `BucketNotFound(KeyError)` | key of the `EndpointStore` mapping |
| key syntactically invalid | `KeyNotValid(KeyError, ValueError)` | both, deliberately — see §5 |
| object archived (Glacier) | `ObjectArchived(KeyError)` | see §4 |
| permission denied | `AccessDenied(S3Error)` — **not** a `KeyError` | but see below |
| credentials missing/expired | `CredentialsError(S3Error)` | |
| operation unsupported by provider | `NotSupported(S3Error)` | names provider + operation |
| transient / throttled | propagate (botocore retries) | |

Everything derives from `S3Error(Exception)` so `except S3Error` catches the package.

**403-means-absent is a real, common ambiguity and needs a stated policy.** AWS's
`HeadObject` docs: *"If you have `s3:ListBucket` … 404. If you don't have `s3:ListBucket`,
Amazon S3 returns 403 Forbidden."* The canonical least-privilege policy grants
`s3:GetObject`/`s3:PutObject` on `bucket/*` and omits `s3:ListBucket` on `bucket` — so under
the **most common production IAM policy, every miss is a 403**, which is not a `KeyError`,
which means `store.get(k, default)` and `k in store` *raise* and a cache-lookup service 500s
instead of taking the miss branch.

Default: raise `AccessDenied`, with a message naming `s3:ListBucket` as the likely cause.
`S3Connection(deny_means_absent=True)` opts into classifying it as `ObjectNotFound` for
deployments that knowingly run that policy. This is a decision, not an omission.

### 4. `ObjectArchived` is a `KeyError`, deliberately and arguably

A Glacier object exists but `GetObject` returns `InvalidObjectState` / 403. `k in store` must
stay `True` — the key *does* exist, and any other answer breaks generic algorithms. But
`store[k]` must fail, and it must fail as a `KeyError` so that `store.get(k, default)` and
`store.pop(k, default)` degrade to the not-available branch rather than exploding.

Be precise about how far that degradation goes, because the obvious claim is wrong:
`dict(store)`, `store.items()` and `Mapping.__eq__` still **raise** — only the defaulted
accessors degrade. Verified.

And one genuinely dangerous interaction: `MutableMapping.setdefault` is
`try: self[k] except KeyError: self[k] = default`, so a `KeyError` here lets `setdefault`
**overwrite the archived object with the default** — verified, silently, no exception.
`BucketStore` therefore overrides `setdefault` and `pop(k, default)` to re-raise
`ObjectArchived` rather than swallow it, and the conformance suite asserts that `setdefault`
on an archived key does not mutate the object.

So it is a `KeyError` carrying `.storage_class`, `.restore_status` and `.restore(days, tier)`.
This is the one place we knowingly let a `KeyError` mean something other than "absent", and it
is recorded here as a considered choice rather than an accident.

### 5. Two collisions to avoid

- **`dol` already has two different `KeyValidationError`s.** `dol.errors.KeyValidationError`
  derives from `(NotValid, ValueError, TypeError)`; `dol.filesys.KeyValidationError` derives
  from `(KeyError, LookupError)`. They are *not the same class*. s3dol therefore names its
  own `KeyNotValid` rather than adding a third thing called `KeyValidationError`, and
  inherits from both `KeyError` and `ValueError` so either `except` works.
- **`KeyError.__str__` uses `repr` of its args.** A multi-line, helpful message becomes a
  single line full of `\n` escapes. Our `KeyError` subclasses therefore override `__str__`.
  Without this, every "informative error" in the package is silently mangled — verified.

### 6. On discussion #6's registry

The proposed class-attribute registry with `register()` works, but it is **global mutable
state**: any import can change how every store in the process classifies errors, and the
effect is order-dependent and untestable in isolation. We take the same *intent* —
third-party extensibility — with a different mechanism: the translation table is a value on
the connection's preset, so extension is per-connection, explicit, and inspectable.

**This belongs in `dol`, and that is a separate change.** Every `*dol` adapter has the same
"which backend exception means no-such-key" problem, and each has solved it differently (or
not at all). The proposed `dol` primitive is a `map_errors` decorator plus a `NotFound`
convention. It is *not* a blocker for s3dol: we implement it locally in a drop-in shape with
a linked issue, per [ADR-0006](0006-key-scoping-and-dol-fixes.md)'s policy on upstreams.

### 7. Message quality, and the one thing they must not contain

An error names the operation, the bucket, the key, the resolved endpoint, and the underlying
code. It **never** contains a credential, a token, or a signed URL — `url_for` returns URLs
carrying `X-Amz-Signature`, so redaction is a tested function, not a convention.

## Consequences

**Buys.** `except KeyError` keeps working for Mapping consumers — non-negotiable and
preserved. Auth failures stop masquerading as missing data. Provider quirks are table rows.
Most error tests need no network.

**Costs.** The table needs maintenance as providers change, and `ObjectArchived` being a
`KeyError` will surprise someone eventually — which is why it is documented here and in the
class docstring.

**What NOT to do.** Do not catch `ClientError` broadly anywhere outside `errors.py`. Do not
return `False`/`[]`/`None` on an exception path — the v0 `_bucket_exists` pattern is
banned by [ADR-0001](0001-layered-architecture.md) goal 4.
