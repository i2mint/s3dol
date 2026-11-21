# s3dol.connection

`S3Connection` — the credential + endpoint SSOT, and the pure resolver.

Design record: `misc/docs/decisions/0012-credential-and-endpoint-resolution.md`
(all of it) on top of `0002-boto3-as-engine.md` (lazy boto3) and
`0003-provider-presets-and-capabilities.md` (presets, SigV4 mandate).

The two sentences the module follows from (ADR-0012):

1. **A preset is the caller’s explicit endpoint, spelled by name.** It sits
   exactly where `endpoint_url=` sits — above every environment and
   config-file channel. s3dol owns two rungs of the endpoint ladder, not six.
2. \*\*A `Config` value s3dol does not set is a channel it leaves open to the
   user.\*\* Every field is a ladder with a documented bottom, and “the row makes
   no claim” is spelled by *omitting the key*, never by passing `None`.

Structure:

- [`S3Connection`](#s3dol.connection.S3Connection) — a frozen, picklable spec. No live objects, no
  escape hatch for one. Construction performs no I/O and imports no boto3.
- [`resolve()`](#s3dol.connection.resolve) — a **pure** function `(spec, environ, aws_config) ->
  Resolution` where every field carries its provenance. Tier-1 tests cover
  the whole ladder with a fake environ dict and zero I/O.
- [`load_aws_config()`](#s3dol.connection.load_aws_config) — the separate, *named impure* loader for the
  aws-config rungs; pass it (or its result) to [`resolve()`](#s3dol.connection.resolve).
- Credential shapes (ADR-0012 D5) — normalised in `__post_init__` to small
  frozen, picklable providers; live botocore objects are rejected with
  shape-specific messages.

```pycon
>>> spec = S3Connection(endpoint_url='http://localhost:9000',
...                     credentials=('AKIAEXAMPLE', 'sEkR3tVALUE'))
>>> res = resolve(spec, {})
>>> res.endpoint_url.value, res.endpoint_url.source
('http://localhost:9000', 'endpoint_url=')
>>> res.signature_version.value  # never left to botocore (ADR-0003 §4)
's3v4'
>>> 'sEkR3tVALUE' in repr(spec)  # secrets never in repr (ADR-0012 D1)
False
```

### Functions

| [`load_aws_config`](#s3dol.connection.load_aws_config)()                    | The named, *impure* aws-config loader (ADR-0012 D2): loads the shared config via botocore (~16 ms — never on the common path; pass this function as a thunk and [`resolve()`](#s3dol.connection.resolve) calls it only if a rung needs it), scrubbed of anything credential-shaped.   |
|---------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`normalize_credentials`](#s3dol.connection.normalize_credentials)(value)         | Normalise every accepted credential shape; reject the rest, each with its own message (ADR-0012 D5).                                                                                                                                                                                                    |
| [`resolve`](#s3dol.connection.resolve)(spec, environ[, aws_config]) | Pure resolution: no `os.environ`, no disk, no network (ADR-0012 D2).                                                                                                                                                                                                                                    |

### Classes

| [`CallableCredentials`](#s3dol.connection.CallableCredentials)(fetch)                       | A picklable zero-arg callable returning a mapping with keys `access_key`/`secret_key` (aliases: `key`/`secret`), optional `token`, optional `expires_at`/`expiry_time` (ISO 8601).    |
|---------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`CredentialProvider`](#s3dol.connection.CredentialProvider)()                             | Base of s3dol's picklable credential specs.                                                                                                                                           |
| [`ProfileCredentials`](#s3dol.connection.ProfileCredentials)(profile)                      | Credentials from a named profile — *credentials only*; the profile's other config is not selected by this (that is what `profile=` on the connection is for).                         |
| [`Resolution`](#s3dol.connection.Resolution)(endpoint_url, region_name, ...)       | The result of [`resolve()`](#s3dol.connection.resolve) — everything `diagnose()` prints and everything client-build consumes.                                       |
| [`S3Connection`](#s3dol.connection.S3Connection)([preset, endpoint_url, ...])        | A frozen, picklable connection spec.                                                                                                                                                  |
| [`Sourced`](#s3dol.connection.Sourced)(value, source[, passed])                 | A resolved value, where it came from, and whether s3dol passes it to botocore explicitly (`passed=False` == the key is omitted so the channel below stays open — "omit, never None"). |
| [`StaticCredentials`](#s3dol.connection.StaticCredentials)(access_key, secret_key[, ...]) | An explicit key pair (optionally a session token).                                                                                                                                    |

### *class* s3dol.connection.CallableCredentials(fetch)

Bases: [`CredentialProvider`](#s3dol.connection.CredentialProvider)

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

### *class* s3dol.connection.CredentialProvider

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Base of s3dol’s picklable credential specs.

Subclasses are small frozen dataclasses that *describe* how to obtain
credentials; nothing live is held (nothing live pickles — measured, ADR-0012
D5). At client-build time each is adapted into a botocore provider and
**replaces** botocore’s resolver via
`session.register_component('credential_provider', ...)` — never
prepended (silent fallthrough) and never `insert_before` (raises when a
profile is set).

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### *class* s3dol.connection.ProfileCredentials(profile)

Bases: [`CredentialProvider`](#s3dol.connection.CredentialProvider)

Credentials from a named profile — *credentials only*; the profile’s
other config is not selected by this (that is what `profile=` on the
connection is for). Rebuilds live, still-refreshable credentials after
unpickling (a frozen dataclass holding only a profile name round-trips).

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### *class* s3dol.connection.Resolution(endpoint_url, region_name, signature_version, addressing_style, checksum, payload_signing_enabled, verify, preset, preset_source, credential_provenance, capabilities, environ_consulted, notes)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The result of [`resolve()`](#s3dol.connection.resolve) — everything `diagnose()` prints and
everything client-build consumes. Carries provenance labels, never a
credential value.

#### environ_consulted *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Optional](https://docs.python.org/3/library/typing.html#typing.Optional)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]], ...]*

The non-secret environment keys consulted, with their values.

#### notes *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[type](https://docs.python.org/3/builtins/functions.html#type), [str](https://docs.python.org/3/builtins/stdtypes.html#str)], ...]*

Collected (warning class, message) pairs. resolve() never emits them —
the impure caller decides (client build warns; diagnose prints rows).

### *class* s3dol.connection.S3Connection(preset=None, endpoint_url=None, region_name=None, profile=None, credentials=None, anon=False, signature_version='s3v4', addressing_style=None, checksum=None, payload_signing_enabled=None, verify=None, client_kwargs=(), deny_means_absent=False)

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

For most providers this is [`client`](#s3dol.connection.S3Connection.client). Two providers genuinely
need different config for presigning than for the API (ADR-0003 §1):
Hetzner presigns virtual-hosted while the API is path-style; R2
presigns only on the S3 API domain. When the resolved preset carries
`presign_endpoint_url`/`presign_addressing_style`, a second client
is built (lazily, once, under the same lock) from the merged config.

#### resolution(environ=None, aws_config=<function load_aws_config>)

Resolve against the real environment by default (a thin impure
wrapper over the pure [`resolve()`](#s3dol.connection.resolve)).

* **Return type:**
  [`Resolution`](#s3dol.connection.Resolution)

### *class* s3dol.connection.Sourced(value, source, passed=False)

Bases: [`NamedTuple`](https://docs.python.org/3/library/typing.html#typing.NamedTuple)

A resolved value, where it came from, and whether s3dol passes it to
botocore explicitly (`passed=False` == the key is omitted so the channel
below stays open — “omit, never None”).

#### passed *: [bool](https://docs.python.org/3/builtins/functions.html#bool)*

Alias for field number 2

#### source *: [str](https://docs.python.org/3/builtins/stdtypes.html#str)*

Alias for field number 1

#### value *: [Any](https://docs.python.org/3/library/typing.html#typing.Any)*

Alias for field number 0

### *class* s3dol.connection.StaticCredentials(access_key, secret_key, token=None)

Bases: [`CredentialProvider`](#s3dol.connection.CredentialProvider)

An explicit key pair (optionally a session token).

#### NOTE
a *static* token expires and cannot refresh — prefer `profile=` or
a callable provider for STS/SSO. Pickling this object carries the secret
(that is what “explicit credentials” means); its repr does not.

#### provenance()

A label for diagnose() — never a credential value.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### s3dol.connection.load_aws_config()

The named, *impure* aws-config loader (ADR-0012 D2): loads the shared
config via botocore (~16 ms — never on the common path; pass this function
as a thunk and [`resolve()`](#s3dol.connection.resolve) calls it only if a rung needs it), scrubbed
of anything credential-shaped.

* **Return type:**
  [`Mapping`](https://docs.python.org/3/library/typing.html#typing.Mapping)

### s3dol.connection.normalize_credentials(value)

Normalise every accepted credential shape; reject the rest, each with
its own message (ADR-0012 D5). The only door is `S3Connection.__post_init__`.

```pycon
>>> normalize_credentials(None) is None
True
>>> normalize_credentials('prod')
ProfileCredentials(profile='prod')
>>> normalize_credentials(('AKIAXX', 'sec'))
StaticCredentials(access_key='AKIA'…, secret_key=***)
```

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`CredentialProvider`](#s3dol.connection.CredentialProvider)]

### s3dol.connection.resolve(spec, environ, aws_config=None)

Pure resolution: no `os.environ`, no disk, no network (ADR-0012 D2).

`environ` is a plain mapping (pass `os.environ` at real call sites,
a dict in tests). `aws_config` is a pre-loaded, credential-scrubbed
config mapping, or a thunk for one ([`load_aws_config()`](#s3dol.connection.load_aws_config)) — called only
if a rung needs it.

* **Return type:**
  [`Resolution`](#s3dol.connection.Resolution)
