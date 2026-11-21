# s3dol.presets

Provider presets: config rows, not subclasses.

Design record: `misc/docs/decisions/0003-provider-presets-and-capabilities.md`
(the survey of 16 backends, the checksum analysis, capabilities policy) amended
by `misc/docs/decisions/0012-credential-and-endpoint-resolution.md` §D4
(`params`/`host_patterns`/`requires_endpoint`/`soft`/`config_kwargs`,
derived `pinned`; `signature_version` and `client_kwargs` moved to the
connection; `addressing_style` mandatory with `'auto'` illegal; the
structural no-credential rule).

A provider divergence is a row in this registry — never a subclass, never an
endpoint-substring branch in the hot path. Users register their own rows:

```pycon
>>> row = Preset(name='mycorp', addressing_style='path',
...              endpoint_url='https://s3.mycorp.example', requires_endpoint=False)
>>> register(row)
>>> get_preset('mycorp').endpoint_url
'https://s3.mycorp.example'
>>> unregister('mycorp')  # (cleanup for this doctest)
```

Most builtin rows are **doc-sourced, not verified against a live endpoint** —
they say so via `verified=False`. Wrong values mostly fail loudly, which is
the acceptable failure mode; `pinned` (which makes a row’s region literal
conflict-raising) is derived and *gated on* `verified`, so it is dormant
until a row earns evidence (ADR-0012 D4).

### Module Attributes

| [`CONFIG_KWARGS_ALLOWLIST`](#s3dol.presets.CONFIG_KWARGS_ALLOWLIST)   | `Config`-constructor kwargs a preset row may carry.   |
|----------------------------------------------------------------------------|-------------------------------------------------------|

### Functions

| `available_presets`()                                                                       |                                                                                                                                                                                                                                                                      |
|---------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`detect_preset`](#s3dol.presets.detect_preset)(endpoint_url)                | Detection (resolution pass 2): the non-soft row whose `host_patterns` match the resolved endpoint's host.                                                                                                                                                            |
| [`get_preset`](#s3dol.presets.get_preset)(name)                           | Look a row up by name; unknown names raise, naming the registry.                                                                                                                                                                                                     |
| [`register`](#s3dol.presets.register)(preset, \*[, overwrite])          | Add a row.                                                                                                                                                                                                                                                           |
| [`unregister`](#s3dol.presets.unregister)(name)                           | Remove a row (mostly for tests).                                                                                                                                                                                                                                     |
| [`validate_endpoint_hygiene`](#s3dol.presets.validate_endpoint_hygiene)(url, \*[, what]) | Refuse credential-carrying URL shapes: userinfo in the netloc, a query string, or a fragment (ADR-0012 D4 — a query-string endpoint is accepted by botocore and retained verbatim in `client.meta.endpoint_url`, so it would leak into `diagnose()` and tracebacks). |

### Classes

| [`Capabilities`](#s3dol.presets.Capabilities)([list_objects_v2, ...])   | What a backend supports.   |
|-----------------------------------------------------------------------------------------|----------------------------|
| [`Preset`](#s3dol.presets.Preset)(name, addressing_style[, ...])  | One provider's facts.      |

### s3dol.presets.CONFIG_KWARGS_ALLOWLIST *= frozenset({'connect_timeout', 'max_pool_connections', 'read_timeout', 'tcp_keepalive'})*

`Config`-constructor kwargs a preset row may carry. Scalars only, and
`proxies`/`proxies_config`/`client_context_params` are structurally
excluded — a preset must not be able to smuggle a credential-bearing proxy
URL (ADR-0012 D4).

### *class* s3dol.presets.Capabilities(list_objects_v2=True, batch_delete=True, presigned_post=True, multipart=True, max_multipart_parts=10000, min_part_size=5242880, object_tagging=True, versioning=True, conditional_writes=True, consistency='strong')

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

What a backend supports. A *static declared table*, never a probe
(a probe costs a round-trip, is wrong under partial permissions, and can’t
be trusted anyway — ADR-0003 §2). Missing capabilities are handled
per-capability: emulate when exact (batch delete -> loop), substitute when
equivalent (ListObjectsV2 -> V1), raise `NotSupported` when there is no
honest fallback. Never silently degrade correctness.

### *class* s3dol.presets.Preset(name, addressing_style, endpoint_url=None, region_name=None, checksum=None, payload_signing_enabled=None, capabilities=Capabilities(list_objects_v2=True, batch_delete=True, presigned_post=True, multipart=True, max_multipart_parts=10000, min_part_size=5242880, object_tagging=True, versioning=True, conditional_writes=True, consistency='strong'), presign_endpoint_url=None, presign_addressing_style=None, params=(), host_patterns=(), requires_endpoint=False, soft=False, config_kwargs=(), error_overrides=(), verified=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

One provider’s facts. Public, shareable, committable config — which is
why there is **no credential slot**, structurally (ADR-0012 D4).

`endpoint_url`/`region_name` may contain `{placeholders}`; bind them
with [`bind()`](#s3dol.presets.Preset.bind). A row whose endpoint template has an unbound *required*
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
  [`Preset`](#s3dol.presets.Preset)

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

Which placeholders keep [`bound_endpoint()`](#s3dol.presets.Preset.bound_endpoint) from producing a URL.

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

### s3dol.presets.detect_preset(endpoint_url)

Detection (resolution pass 2): the non-soft row whose `host_patterns`
match the resolved endpoint’s host. Two matching rows is genuinely
ambiguous and raises; zero yields `None` (the caller falls back to
`generic-s3`/`aws` — ADR-0012 D4).

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Preset`](#s3dol.presets.Preset)]

### s3dol.presets.get_preset(name)

Look a row up by name; unknown names raise, naming the registry.

* **Return type:**
  [`Preset`](#s3dol.presets.Preset)

### s3dol.presets.register(preset, , overwrite=False)

Add a row. Open-closed: adding a provider is adding a row.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.presets.unregister(name)

Remove a row (mostly for tests).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### s3dol.presets.validate_endpoint_hygiene(url, , what='endpoint_url')

Refuse credential-carrying URL shapes: userinfo in the netloc, a query
string, or a fragment (ADR-0012 D4 — a query-string endpoint is accepted by
botocore and retained verbatim in `client.meta.endpoint_url`, so it would
leak into `diagnose()` and tracebacks).

```pycon
>>> validate_endpoint_hygiene('https://s3.example.com')
>>> validate_endpoint_hygiene('https://user:pw@s3.example.com')
Traceback (most recent call last):
...
s3dol.errors.InvalidEndpoint: ...
```

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)
