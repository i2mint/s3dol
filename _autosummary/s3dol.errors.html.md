# s3dol.errors

The s3dol error taxonomy and the single error-translation seam.

Design record: `misc/docs/decisions/0004-error-taxonomy.md` (the taxonomy, the
`(operation, code, status)` classification, the 403 policy, `ObjectArchived`)
and `misc/docs/decisions/0012-credential-and-endpoint-resolution.md` (the
`ConfigurationError` tree and the `S3DolWarning` tree).

Three rules govern this module:

1. **One seam.** [`translate_s3_errors()`](#s3dol.errors.translate_s3_errors) is the only place in the package
   that catches botocore exceptions. Everything else propagates.
2. \*\*Auth, config and transport errors are never translated to `KeyError`.\*\*
   A missing permission must not look like a missing key.
3. \*\*Classification keys on `(operation, code, status)`, never on exception
   type.\*\* HEAD responses have no body, so botocore’s modelled exceptions
   (`client.exceptions.NoSuchKey` …) never fire for them.

Everything classification-related here is pure: [`classify_client_error()`](#s3dol.errors.classify_client_error)
takes plain values (or a synthesized `ClientError`) and touches no network,
so the bulk of error tests are tier-1 unit tests — including body-less HEAD
errors, which moto cannot produce.

```pycon
>>> classify_client_error('GetObject', 'NoSuchKey', 404).kind
'object_absent'
>>> classify_client_error('HeadObject', '404', 404).kind  # HEAD is ambiguous
'ambiguous_head'
>>> classify_client_error('GetObject', 'AccessDenied', 403).kind
'access_denied'
```

### Module Attributes

| [`KINDS`](#s3dol.errors.KINDS)         | Kinds a classified error can be.                                           |
|----------------------------------------------------------------|----------------------------------------------------------------------------|
| [`Rule`](#s3dol.errors.Rule)          | `((operation, code, status), kind)`.                                       |
| [`DEFAULT_RULES`](#s3dol.errors.DEFAULT_RULES) | The default classification table (ADR-0004 §2, measured against botocore). |

### Functions

| [`classify_client_error`](#s3dol.errors.classify_client_error)(operation, code, status, \*)   | Classify a backend error by `(operation, code, status)`.                                                                                                                                                       |
|-------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`code_of`](#s3dol.errors.code_of)(client_error)                                | The `Error.Code` of a botocore `ClientError` (or `None`).                                                                                                                                                      |
| [`redact`](#s3dol.errors.redact)(text)                                         | Redact signed-URL query secrets from `text`.                                                                                                                                                                   |
| [`redact_url`](#s3dol.errors.redact_url)(url)                                      | A URL safe to display: userinfo, query and fragment removed.                                                                                                                                                   |
| [`status_of`](#s3dol.errors.status_of)(client_error)                              | The HTTP status of a botocore `ClientError` (or `None`).                                                                                                                                                       |
| [`translate_client_error`](#s3dol.errors.translate_client_error)(error, \*, operation)         | Translate a botocore `ClientError` into the s3dol taxonomy.                                                                                                                                                    |
| [`translate_s3_errors`](#s3dol.errors.translate_s3_errors)(\*, operation[, key_arg])        | The one seam: decorate a Layer B method so botocore's `ClientError` becomes the s3dol taxonomy (ADR-0004 §1).                                                                                                  |
| [`translating_s3_errors`](#s3dol.errors.translating_s3_errors)(store, \*, operation[, key])   | Generator/inline form of the seam, for code the decorator cannot wrap (`__iter__` bodies: a decorated generator function returns its generator before the body runs, so the decorator's `except` never fires). |

### Classes

| [`Classified`](#s3dol.errors.Classified)(kind[, exception_type])   | The result of classifying a backend error.   |
|---------------------------------------------------------------------------------------|----------------------------------------------|

### Exceptions

| [`AccessDenied`](#s3dol.errors.AccessDenied)                                       | Permission denied.                                                                                                                    |
|-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [`AmbiguousResolution`](#s3dol.errors.AmbiguousResolution)                                | Resolution is legal but ambiguous (e.g. a named preset whose endpoint, supplied from the environment, matches no known host pattern). |
| [`AnonymousFallback`](#s3dol.errors.AnonymousFallback)                                  | Anonymous access was used as a fallback (reserved for a future `anon='auto'` — ADR-0012 D5 defers that feature to v1.x).              |
| [`BucketNotEmpty`](#s3dol.errors.BucketNotEmpty)                                     | Refusal to delete a non-empty bucket.                                                                                                 |
| [`BucketNotFound`](#s3dol.errors.BucketNotFound)                                     | The bucket is absent.                                                                                                                 |
| [`ConfigurationError`](#s3dol.errors.ConfigurationError)                                 | A construction- or resolution-time misconfiguration (ADR-0012 D1).                                                                    |
| [`CredentialsError`](#s3dol.errors.CredentialsError)                                   | Credentials are missing, expired, or invalid.                                                                                         |
| [`InvalidEndpoint`](#s3dol.errors.InvalidEndpoint)                                    | An endpoint URL failed hygiene checks (userinfo/query/fragment — ADR-0012 D4: a URL is the residual credential carrier).              |
| [`KeyNotValid`](#s3dol.errors.KeyNotValid)                                        | The key is syntactically invalid (checked before the wire).                                                                           |
| [`MissingEndpoint`](#s3dol.errors.MissingEndpoint)                                    | The named preset needs an endpoint (or endpoint parameters) that nothing supplied (ADR-0012 C1).                                      |
| [`MissingPresetParam`](#s3dol.errors.MissingPresetParam)                                 | A preset's endpoint template has an unbound required parameter, so the row contributes no endpoint rung (ADR-0012 C1).                |
| [`NotSupported`](#s3dol.errors.NotSupported)                                       | The operation is not supported by this provider (names both).                                                                         |
| [`ObjectArchived`](#s3dol.errors.ObjectArchived)(message, \*[, storage_class, ...])  | The object exists but is archived (e.g. Glacier) and cannot be read now.                                                              |
| [`ObjectNotFound`](#s3dol.errors.ObjectNotFound)                                     | The object is absent.                                                                                                                 |
| [`PresetConflict`](#s3dol.errors.PresetConflict)                                     | An explicit argument contradicts a *pinned* (verified) preset value.                                                                  |
| [`PresetHostMismatch`](#s3dol.errors.PresetHostMismatch)                                 | The resolved endpoint's host matches a *different* provider's patterns than the preset you named (ADR-0012 C2).                       |
| [`S3DolResolutionChanged`](#s3dol.errors.S3DolResolutionChanged)                             | v1 resolves this call differently than v0 would have (ADR-0012 D7).                                                                   |
| [`S3DolWarning`](#s3dol.errors.S3DolWarning)                                       | Base of every s3dol warning.                                                                                                          |
| [`S3Error`](#s3dol.errors.S3Error)                                            | Base of every s3dol exception — `except S3Error` catches the package.                                                                 |
| [`S3PartialFailure`](#s3dol.errors.S3PartialFailure)(message, \*, succeeded, failures) | A bulk operation partially failed (ADR-0010 §2).                                                                                      |
| [`UnknownPresetParam`](#s3dol.errors.UnknownPresetParam)                                 | A parameter was bound that the preset's templates do not use.                                                                         |

### *exception* s3dol.errors.AccessDenied

Bases: [`S3Error`](#s3dol.errors.S3Error)

Permission denied. **Not** a `KeyError` — a missing permission must
not look like a missing key (ADR-0004 §1/§3).

### *exception* s3dol.errors.AmbiguousResolution

Bases: [`S3DolWarning`](#s3dol.errors.S3DolWarning)

Resolution is legal but ambiguous (e.g. a named preset whose endpoint,
supplied from the environment, matches no known host pattern).

### *exception* s3dol.errors.AnonymousFallback

Bases: [`S3DolWarning`](#s3dol.errors.S3DolWarning)

Anonymous access was used as a fallback (reserved for a future
`anon='auto'` — ADR-0012 D5 defers that feature to v1.x).

### *exception* s3dol.errors.BucketNotEmpty

Bases: [`S3Error`](#s3dol.errors.S3Error)

Refusal to delete a non-empty bucket. Use
`s3dol.delete_bucket(endpoint, name, force=True)` for the explicit
cascading form (ADR-0010 §3).

### *exception* s3dol.errors.BucketNotFound

Bases: [`S3Error`](#s3dol.errors.S3Error), `_InformativeKeyError`

The bucket is absent.

Still a `KeyError`: for an object operation it is a key-space problem for
the caller; for a bucket operation it is the key of the endpoint mapping.

### *class* s3dol.errors.Classified(kind, exception_type=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The result of classifying a backend error.

### *exception* s3dol.errors.ConfigurationError

Bases: [`S3Error`](#s3dol.errors.S3Error), [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError)

A construction- or resolution-time misconfiguration (ADR-0012 D1).

### *exception* s3dol.errors.CredentialsError

Bases: [`S3Error`](#s3dol.errors.S3Error)

Credentials are missing, expired, or invalid.

### s3dol.errors.DEFAULT_RULES *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[int](https://docs.python.org/3/builtins/functions.html#int)]], [str](https://docs.python.org/3/builtins/stdtypes.html#str)], ...]* *= (((None, 'NoSuchKey', None), 'object_absent'), ((None, 'NoSuchBucket', None), 'bucket_absent'), ((None, 'NoSuchUpload', None), 'object_absent'), (('HeadObject', '404', 404), 'ambiguous_head'), (('HeadBucket', '404', 404), 'bucket_absent'), (('HeadObject', '404', None), 'ambiguous_head'), (('HeadBucket', '404', None), 'bucket_absent'), (('HeadObject', None, 405), 'delete_marker'), ((None, None, 301), 'wrong_region'), ((None, 'PermanentRedirect', None), 'wrong_region'), ((None, 'AuthorizationHeaderMalformed', None), 'wrong_region'), ((None, 'InvalidObjectState', None), 'archived'), ((None, 'RequestTimeTooSkewed', None), 'credentials'), ((None, 'RequestExpired', None), 'credentials'), ((None, 'AuthorizationQueryParametersError', None), 'credentials'), ((None, 'ExpiredToken', None), 'credentials'), ((None, 'InvalidAccessKeyId', None), 'credentials'), ((None, 'SignatureDoesNotMatch', None), 'credentials'), ((None, 'TokenRefreshRequired', None), 'credentials'), ((None, 'AccessDenied', None), 'access_denied'), ((None, 'AllAccessDisabled', None), 'access_denied'), ((None, '403', 403), 'access_denied'), ((None, None, 403), 'access_denied'), ((None, 'KeyTooLongError', None), 'invalid_key'), ((None, 'SlowDown', None), 'transient'), ((None, 'RequestTimeout', None), 'transient'), ((None, 'ServiceUnavailable', None), 'transient'), ((None, 'InternalError', None), 'transient'), ((None, None, 503), 'transient'))*

The default classification table (ADR-0004 §2, measured against botocore).

### *exception* s3dol.errors.InvalidEndpoint

Bases: [`ConfigurationError`](#s3dol.errors.ConfigurationError)

An endpoint URL failed hygiene checks (userinfo/query/fragment —
ADR-0012 D4: a URL is the residual credential carrier).

### s3dol.errors.KINDS *= ('object_absent', 'bucket_absent', 'ambiguous_head', 'access_denied', 'credentials', 'invalid_key', 'archived', 'not_supported', 'wrong_region', 'delete_marker', 'transient', 'unknown')*

Kinds a classified error can be. Layer B maps kinds to behaviour; in
particular `ambiguous_head` triggers the one-`HeadBucket`
disambiguation of ADR-0004 §2 (HEAD has no body, so a missing key and a
missing bucket are both `('404', 404)` on real AWS).

### *exception* s3dol.errors.KeyNotValid

Bases: [`S3Error`](#s3dol.errors.S3Error), `_InformativeKeyError`, [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError)

The key is syntactically invalid (checked before the wire).

Inherits from both `KeyError` and `ValueError` deliberately, so either
`except` works. Named `KeyNotValid` (not `KeyValidationError`) because
dol already ships two *different* classes called `KeyValidationError`
(ADR-0004 §5).

### *exception* s3dol.errors.MissingEndpoint

Bases: [`ConfigurationError`](#s3dol.errors.ConfigurationError)

The named preset needs an endpoint (or endpoint parameters) that nothing
supplied (ADR-0012 C1).

### *exception* s3dol.errors.MissingPresetParam

Bases: [`MissingEndpoint`](#s3dol.errors.MissingEndpoint)

A preset’s endpoint template has an unbound required parameter, so the
row contributes no endpoint rung (ADR-0012 C1). A `MissingEndpoint`, so
either `except` works.

### *exception* s3dol.errors.NotSupported

Bases: [`S3Error`](#s3dol.errors.S3Error)

The operation is not supported by this provider (names both).

### *exception* s3dol.errors.ObjectArchived(message, , storage_class=None, restore_status=None, restore=None)

Bases: [`S3Error`](#s3dol.errors.S3Error), `_InformativeKeyError`

The object exists but is archived (e.g. Glacier) and cannot be read now.

A `KeyError`, deliberately and arguably (ADR-0004 §4): `k in store`
stays `True`, but `store[k]` fails as a `KeyError` so that
`store.get(k, default)` degrades to the not-available branch. Note that
only the *defaulted accessors* degrade — `dict(store)`, `.items()` and
`Mapping.__eq__` still raise. `setdefault`/`pop` are overridden in the
store to re-raise this instead of silently overwriting the archived object.

#### restore(days=1, , tier='Standard')

Request restoration of the archived object (if a handle was bound).

### *exception* s3dol.errors.ObjectNotFound

Bases: [`S3Error`](#s3dol.errors.S3Error), `_InformativeKeyError`

The object is absent. The Mapping contract’s `KeyError`.

### *exception* s3dol.errors.PresetConflict

Bases: [`ConfigurationError`](#s3dol.errors.ConfigurationError)

An explicit argument contradicts a *pinned* (verified) preset value.

### *exception* s3dol.errors.PresetHostMismatch

Bases: [`ConfigurationError`](#s3dol.errors.ConfigurationError)

The resolved endpoint’s host matches a *different* provider’s patterns
than the preset you named (ADR-0012 C2).

### s3dol.errors.Rule

`((operation, code, status), kind)`. `None` is a wildcard.
First match wins; more specific rows come first.

* **Type:**
  Rule rows

alias of [`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`None`](https://docs.python.org/3/builtins/constants.html#None), [`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`None`](https://docs.python.org/3/builtins/constants.html#None), [`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)], [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

### *exception* s3dol.errors.S3DolResolutionChanged

Bases: [`S3DolWarning`](#s3dol.errors.S3DolWarning)

v1 resolves this call differently than v0 would have (ADR-0012 D7).
Emitted by the compat shim; dies with it.

### *exception* s3dol.errors.S3DolWarning

Bases: [`UserWarning`](https://docs.python.org/3/builtins/exceptions.html#UserWarning)

Base of every s3dol warning.

### *exception* s3dol.errors.S3Error

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base of every s3dol exception — `except S3Error` catches the package.

### *exception* s3dol.errors.S3PartialFailure(message, , succeeded, failures)

Bases: [`S3Error`](#s3dol.errors.S3Error)

A bulk operation partially failed (ADR-0010 §2).

Not an `ExceptionGroup` — the package supports Python 3.10.

#### succeeded

keys the backend reported as deleted (absent keys are
reported as deleted too — S3’s semantics, documented, not hidden).

#### failures

mapping of key -> the error for that key.

### *exception* s3dol.errors.UnknownPresetParam

Bases: [`ConfigurationError`](#s3dol.errors.ConfigurationError)

A parameter was bound that the preset’s templates do not use.

### s3dol.errors.classify_client_error(operation, code, status, , overrides=())

Classify a backend error by `(operation, code, status)`.

`overrides` rows (typically from a preset) are consulted first, so a
provider divergence is a table row, never a code branch.

```pycon
>>> classify_client_error('HeadBucket', None, 400,
...     overrides=((('HeadBucket', None, 400), 'bucket_absent'),)).kind
'bucket_absent'
```

* **Return type:**
  [`Classified`](#s3dol.errors.Classified)

### s3dol.errors.code_of(client_error)

The `Error.Code` of a botocore `ClientError` (or `None`).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

### s3dol.errors.redact(text)

Redact signed-URL query secrets from `text`.

Error messages may name the operation, bucket, key, endpoint and code, but
never a credential, token, or signed URL — redaction is a tested function,
not a convention (ADR-0004 §7).

```pycon
>>> redact('https://b.s3.amazonaws.com/k?X-Amz-Signature=abc123&x=1')
'https://b.s3.amazonaws.com/k?X-Amz-Signature=REDACTED&x=1'
```

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### s3dol.errors.redact_url(url)

A URL safe to display: userinfo, query and fragment removed.

A URL is the residual credential carrier (ADR-0012 D4). Endpoint hygiene
refuses those shapes for values *s3dol* is given, but an endpoint read
from `AWS_ENDPOINT_URL_S3` or an aws-config profile is not ours to
refuse — it must merely never be echoed intact.

```pycon
>>> redact_url('https://user:pw@minio.internal:9000/x?sig=abc#f')
'https://REDACTED@minio.internal:9000/x?<redacted>'
```

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

### s3dol.errors.status_of(client_error)

The HTTP status of a botocore `ClientError` (or `None`).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`int`](https://docs.python.org/3/builtins/functions.html#int)]

### s3dol.errors.translate_client_error(error, , operation, bucket=None, key=None, endpoint=None, overrides=(), deny_means_absent=False, head_ambiguity_resolver=None)

Translate a botocore `ClientError` into the s3dol taxonomy.

Returns the replacement exception, or `None` meaning \*propagate the
original\* (transient/unknown — rule 2 of the module docstring).

`head_ambiguity_resolver` resolves the body-less `HeadObject` 404
(ADR-0004 §2): it returns `True` iff the bucket is known reachable, in
which case the miss really is the object’s. It is only called for the
ambiguous case, so the common path costs nothing.

`deny_means_absent=True` classifies an object-op 403 as
`ObjectNotFound` — for deployments knowingly running the canonical
least-privilege policy that omits `s3:ListBucket` (ADR-0004 §3).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`S3Error`](#s3dol.errors.S3Error)]

### s3dol.errors.translate_s3_errors(, operation, key_arg=None, \*\*translate_kwargs)

The one seam: decorate a Layer B method so botocore’s `ClientError`
becomes the s3dol taxonomy (ADR-0004 §1).

`key_arg` names the decorated function’s key *parameter* (resolved via
the call’s kwargs/positionals — never an attribute, so the key is not lost
the way azuredol’s `key_arg='blob'` loses it). The store instance is
expected to provide `_error_context()` returning
`dict(bucket=..., endpoint=..., overrides=..., deny_means_absent=...,
head_ambiguity_resolver=...)` — Layer B owns that context.

### s3dol.errors.translating_s3_errors(store, , operation, key=None, \*\*translate_kwargs)

Generator/inline form of the seam, for code the decorator cannot wrap
(`__iter__` bodies: a decorated generator function returns its generator
before the body runs, so the decorator’s `except` never fires).

Same contract as [`translate_s3_errors()`](#s3dol.errors.translate_s3_errors); the store supplies
`_error_context()`.
