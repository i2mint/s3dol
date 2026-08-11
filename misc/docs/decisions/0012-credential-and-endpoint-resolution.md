# ADR-0012: Credential and endpoint resolution

- **Status:** Accepted
- **Date:** 2026-08-11
- **Discussion:** [#15](https://github.com/i2mint/s3dol/discussions/15)
- **Amends:** [ADR-0003](0003-provider-presets-and-capabilities.md) §1/§4/§5/§6,
  [ADR-0004](0004-error-taxonomy.md) §3, [ADR-0007](0007-naming-and-compatibility.md) §3/§4/§5,
  [ADR-0008](0008-testing-architecture.md), [ADR-0011](0011-keyed-capability-surface.md) §D5
- **Fixes:** findings 1, 2, 5 of [state-of-play](../state-of-play.md) §4; issue
  [#10](https://github.com/i2mint/s3dol/issues/10)

Everything here is either measured (botocore **1.43.68**, offline, network-blocked) or traceable
to an Accepted ADR. Measurements were produced by four independent probes and then re-verified
by an adversarial pass that overturned several of them; where a claim in #15 or an earlier ADR
turned out to be wrong, this ADR says so rather than quietly correcting it.

## The two sentences the rest follows from

1. **A preset is the caller's explicit endpoint, spelled by name.** It sits exactly where
   `endpoint_url=` sits — above every environment and config-file channel. s3dol owns **two**
   rungs of the endpoint ladder, not six.
2. **A `Config` value s3dol does not set is a channel it leaves open to the user.** So every
   field is a *ladder with a documented bottom*, and "the row makes no claim" is spelled by
   **omitting the key**, never by passing `None`.

## Context — three premises in #15 that did not survive measurement

**(a) "The obvious fix inverts v0's precedence."** #15 (and [ADR-0007](0007-naming-and-compatibility.md) §4)
say v0's effective order is `AWS_ENDPOINT_URL_S3 > AWS_ENDPOINT_URL > explicit kwarg`, so
"explicit wins" inverts the top of it. Measured: **botocore already ranks
`explicit > AWS_ENDPOINT_URL_S3 > AWS_ENDPOINT_URL > config file > resolver`.** v0 behaves
otherwise because it *drops the argument* — `_find_default_credentials(endpoint_url=…)` calls
`get_client("environment variables", **session_kwargs)` and never forwards it. **v1 re-ranks
nothing; it stops discarding an argument.** That deletes the "vendor a precedence ladder" work
item entirely.

**(b) "SigV2 for `us-east-1` and every custom endpoint."** Measured across 16 regions: the
downgrade fires in exactly the regions `endpoints.json` still lists as v2-capable — **12 region
strings**, not 8, once `aws-global` and three FIPS regions are counted. A custom endpoint is
irrelevant: `us-east-1` + MinIO → SigV2, `eu-south-2` + MinIO → SigV4. **Region is the
discriminator.** The practical impact stands only because `us-east-1` is the conventional MinIO
default. `client.meta.config.signature_version` reports `'s3v4'` throughout — it lies.

**(c) "`anon='auto'` needs a cheap credential probe."** Measured: a `None` credential result is
**not cached**, so probe-then-build-signed runs the whole chain twice (~4.2 s where IMDS traffic
is dropped rather than refused). An UNSIGNED client short-circuits the chain entirely — zero
credential I/O.

## Decision

### D1 — `S3Connection` is a frozen, picklable spec. No live objects.

```python
@dataclass(frozen=True)
class S3Connection:
    # WHERE
    endpoint_url: str | None = None
    region_name: str | None = None
    # WHO
    profile: str | None = None
    credentials: CredentialProvider | None = None  # normalised in __post_init__
    anon: bool = False  # see D5
    # HOW  (None == "do not override" — see D3's ladders)
    signature_version: str = "s3v4"  # ADR-0003 §4: never left to botocore
    addressing_style: Literal["path", "virtual"] | None = None
    checksum: Literal["when_supported", "when_required"] | None = None
    payload_signing_enabled: bool | None = None
    verify: bool | str | None = None  # self-signed MinIO / corporate CA
    client_kwargs: tuple[tuple[str, Any], ...] = ()
    # POLICY
    deny_means_absent: bool = False  # ADR-0004 §3
```

**There is no `_live` escape hatch and no `connection=<boto3 client>`.** Checked: no dependent
passes one — `lacing` uses `S3Store(bucket, path=prefix, **s3_kwargs)`, `reelee` reaches s3dol
*through* `lacing`. A bespoke session is expressed as
`credentials=<picklable CredentialProvider>` + `profile=` + `client_kwargs=`.

That buys a flat, unconditional guarantee: **every s3dol store pickles**, which
[ADR-0008](0008-testing-architecture.md) §6 can assert without a special case. The alternative
was two permanent classes of connection — one picklable, one raising `TypeError` and unable to
cross a `ProcessPoolExecutor`, Dask, or a `dol` disk cache. It also deletes a measured hazard:
a live client held in a `field(compare=False)` makes two connections to **different accounts**
compare and hash equal, so an `lru_cache` hands back the wrong account's client.

Four properties that are not optional:

1. **`__post_init__` is the only door.** Every normalisation and contradiction check happens on
   the public dataclass, not in a `from_anything` classmethod — otherwise
   `S3Connection(credentials=('AK', 'sec'))` dies later inside botocore with
   `AttributeError: 'tuple' object has no attribute 'METHOD'`.
2. **The client is built under a per-instance lock, not a bare `cached_property`.** Measured:
   8 barrier-synchronised threads → **8 body executions and 8 distinct surviving clients**, every
   trial. CPython 3.12 removed `cached_property`'s lock deliberately. For a callable credential
   that is 8 token fetches; for `credential_process`, 8 subprocess spawns.
3. **`__getstate__`/`__setstate__` are mandatory for every connection**, not only anonymous ones
   — the boto3 client is unpicklable, full stop. Enumerate `dataclasses.fields` (drops all caches
   by construction) and re-create the lock in `__setstate__`. *This corrects
   [ADR-0003](0003-provider-presets-and-capabilities.md) §5, which attributes the pickle problem
   to `UNSIGNED` alone.*
4. **Secrets never in `repr`.** Measured hazard: `repr()` of a botocore provider chain dumps the
   entire `os.environ`. A `str` subclass is **not** sufficient redaction — it holds for display
   and leaks through `json`, concatenation and `join`.

One error tree and one warning tree, rooted in ADR-0004's `S3Error`:

```
S3Error                                   S3DolWarning(UserWarning)
└── ConfigurationError(.., ValueError)    ├── S3DolResolutionChanged   (D7; dies with the shim)
    ├── PresetConflict                    ├── AmbiguousResolution
    ├── PresetHostMismatch                └── AnonymousFallback
    ├── MissingEndpoint / MissingPresetParam / UnknownPresetParam
    └── InvalidEndpoint
```

**There is no `strict=` kwarg.** `filterwarnings('error', category=s3dol.errors.S3DolWarning)`
is strict mode with zero API surface, escalates per-subcategory and per-module, and works in
pytest's ini.

### D2 — `resolve(spec, environ, aws_config) -> Resolution` is pure

No `os.environ`, no disk, no network. `aws_config` is a pre-loaded, **credential-scrubbed**
profile dict from a separate, named impure loader passed as a **thunk** — `botocore.session.Session()`
costs 16 ms and must never sit on the common path.

`Resolution` carries a `Sourced(value, source)` per field, the `environ` it was computed
against, the chosen `preset` and how it was chosen, `capabilities`, and a credential
**provenance label** — never a credential value. Tier-1 tests cover the whole ladder with a fake
environ dict and zero I/O.

### D3 — The endpoint ladder, and the field ladders below it

```
1  endpoint_url= kwarg
2  the bound preset's endpoint          [no rung if a required param is unbound — C1]
   ---- s3dol hands botocore ONE value (or None) and stops ----
   AWS_ENDPOINT_URL_S3 > AWS_ENDPOINT_URL
   > aws-config [services X] s3.endpoint_url > aws-config [profile P] endpoint_url
   > the SDK's own resolver
```

**Never pass `Config(ignore_configured_endpoint_urls=True)`.** Measured: with an explicit
endpoint it changes nothing; with no explicit endpoint it retargets an `AWS_ENDPOINT_URL_S3`
user to `https://s3.amazonaws.com`. **It is a no-op where it is safe and harmful where it is
not.** Two independent drafts of this design passed it unconditionally; that would have been a
silent data-target move of exactly the kind this ADR exists to prevent.

s3dol *reads* the lower rungs — purely, via
`botocore.configprovider.ConfiguredEndpointProvider(full_config=…, scoped_config=…, client_name='s3', environ=…)`,
verified constructible from plain dicts with zero network — for `diagnose()` and the guards
**only**, never to override.

Two guards:

- **C1** — a row whose endpoint template has an unbound *required* param contributes no rung. If
  nothing below supplies an endpoint and the row is `requires_endpoint`, raise `MissingEndpoint`
  naming both the missing params and `AWS_ENDPOINT_URL_S3`. `preset='minio'` silently becoming
  `https://s3.amazonaws.com` is structurally impossible.
- **C2** — a *named* row with `host_patterns` whose endpoint arrived from below s3dol's two rungs
  and matches no pattern: **warn**; if it matches a *different* row's patterns, **raise**
  `PresetHostMismatch`. Matching nothing is genuinely ambiguous (a proxy, a vanity CNAME and a
  self-hosted gateway all look like this) and hard-failing punishes a legitimate deployment;
  matching another provider is unambiguous. Anyone wanting hard-fail everywhere gets it from
  `filterwarnings`. The documented escape is `endpoint_url=`, which is rung 1 and never triggers
  C2.

**Per-field ladders**, each bottoming out in *omit the key*:

| field | ladder |
|---|---|
| `region_name` | kwarg (**binds** the row's `{region_name}` template) > row literal (`PresetConflict` iff pinned) > region captured from the resolved host > `AWS_DEFAULT_REGION` > aws-config profile > `'us-east-1'` |
| `addressing_style` | kwarg > row > profile `s3.addressing_style` > **omit** |
| `checksum` | kwarg > row > `AWS_{REQUEST,RESPONSE}_CHECKSUM_*` > `'when_supported'` |
| `payload_signing_enabled` | kwarg > row > **omit** (no env channel exists) |
| `signature_version` | kwarg > `'s3v4'` (no env channel; never from a row) |

Three measurements force these shapes:

- **Omitting is not the same as `None`.** `Config(s3={'addressing_style': None})` *overrides* a
  profile's value with `None`; omitting the key preserves it. Config's `s3` dict merges per-key.
- **The checksum must ladder, not be forced.** `Config(request_checksum_calculation=…)` beats env
  in both directions, **and omitting the key lets env through**. So "always set `when_supported`
  unless a preset says otherwise" would silently break the user who sets
  `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` to work around Supabase — the exact
  population ADR-0003 §3 exists for.
- **botocore does not read `AWS_REGION`**, only `AWS_DEFAULT_REGION`. v1 must not "helpfully" add
  it. And `addressing_style` has **no env var at all**; its default is endpoint-dependent (path
  for any configured endpoint, virtual for the SDK-resolved one).

### D4 — Presets are provider facts, resolved in two passes

**Pass 1** resolves the endpoint. **Pass 2** picks the row: *named* > *detected* by
`host_patterns` against the resolved endpoint (ambiguity raises) > `generic-s3` if any endpoint
resolved > `aws` if none did. Not circular: with no preset named there is no preset rung; with
one named, pass 2 is already determined.

**Detection supplies the full row** — addressing style, checksum, payload-signing and
capabilities — so a bare R2/Backblaze/Supabase URL works with no new argument, and ADR-0003 §3's
checksum fix reaches people who have never heard of presets. The cost is one announced
path→virtual flip for an endpoint-passing caller whose URL matches a virtual-style row. All
three known endpoint-passing dependents resolve to `generic-s3`, which is byte-identical to
today; ADR-0007 §5 step 0 exists to confirm that before shipping.

`Preset` gains `params`, `host_patterns`, `requires_endpoint`, `soft`, `config_kwargs`, and a
**derived** `pinned`; `signature_version` and `client_kwargs` **leave** `Preset` for
`S3Connection`. `addressing_style` has **no default** so every row must state it, and `'auto'` is
illegal.

**No credential slot, and the exclusion is structural** — not a check. `proxies`,
`proxies_config` and `client_context_params` are off the `config_kwargs` allowlist and values are
scalars-only, which closes `config_kwargs['proxies'] = {'https': 'http://user:pw@…'}`. The
residual carrier is a URL field, checked at registration and on the resolved endpoint: **no `@`
in netloc, no query, no fragment**. Measured: a query-string endpoint is *accepted* by botocore
and retained verbatim in `client.meta.endpoint_url`, so it would leak into `diagnose()` and
tracebacks.

**`pinned` is derived and gated on `verified`.** Every builtin row is doc-sourced today, so
`PresetConflict` is dormant in v1.0 and pinning becomes a ratchet that turns on with evidence.

### D5 — `credentials=`, `profile=`, and `anon: bool`

**Accepted:** `None` (botocore's chain) · a bare `str` (**always** a profile name) ·
`(key, secret[, token])` · a mapping of `aws_*` keys · a `CredentialProvider` instance · a
**picklable** zero-arg callable.
**Rejected, each with its own message:** a `CredentialProvider` *class*; a resolved
`Credentials`/`RefreshableCredentials` (a snapshot cannot refresh and cannot pickle); a live
boto3 object; an unknown mapping key; an unpicklable callable.

**Mechanism:** normalise to a picklable frozen-dataclass `CredentialProvider` and inject by
**replacing** the component —
`bs.register_component('credential_provider', CredentialResolver([provider]))`. Never
`insert_before('env', …)` (raises `UnknownCredentialError` whenever a profile is set) and never
prepend (silent chain fallthrough).

Measured facts that force this:

- **Nothing live pickles** — not `boto3.Session`, not `botocore.session.Session`, not clients,
  not refreshable credentials, not the real chain's providers. #15's "a `botocore.Session`" is
  therefore impossible. A frozen dataclass holding **only a profile name** round-trips and
  rebuilds live, still-refreshable credentials.
- **`profile_name` plus *any* explicit credential kwarg silently discards the profile's
  credentials** — no error, no warning; a token-only kwarg leaves `Credentials(None, None, 'TOK')`.
  So `profile=` and `credentials=` cannot be naively independent parameters.
- **Reading `.access_key` on a refreshable credential triggers a network refresh.** `diagnose()`
  must never touch it.
- **Env credentials are not always static**: with `AWS_CREDENTIAL_EXPIRATION` set, `EnvProvider`
  returns `RefreshableCredentials`.
- Refresh thresholds are advisory **900 s** / mandatory **600 s**, so a snapshot dies ~10 minutes
  before its nominal expiry.

**`profile=` stays its own parameter** because it selects *config*, not only credentials — and
because `addressing_style` defaults to `None` and `ignore_configured_endpoint_urls` is never
passed, four of five profile-supplied settings survive.

**`anon` is `bool` in v1. `'auto'` is deferred to v1.x.** Widening `bool → bool | 'auto'` later
is fully compatible, and nothing in the ecosystem uses it today. The reason to defer is the shape
of the evidence: across three review rounds the guard count went **4 → 6 → 7**, each round
finding another silent-credential-downgrade path — the signal a feature whose *purpose* is to
guess emits. `'auto'` is the only path on which s3dol can silently read the **public view** of
data the caller believed was authenticated; every other credential path either resolves or fails
loudly. The honest use case is served by `anon=bool(os.getenv(...))` today, and the decision can
be re-taken on real `diagnose()` telemetry.

`anon=True` still ships, and:

- `anon=True` + credentials/profile **raises** at construction. Measured: `UNSIGNED` sets
  `_request_signer._credentials` to `None`, so they would be silently discarded.
- `anon=True` + `deny_means_absent=True` **raises**. Measured under moto: an anonymous
  `head_object` gives 403 on an existing key and 404 on a missing one, so **every key reads as
  absent** — a silently empty store, banned by ADR-0001 goal 4.
- **`url_for` returns `None` when anonymous** — *amends ADR-0003 §5, which says it raises*.
  `dol.SupportsUrlFor.url_for` is typed `-> Optional[str]`, and raising turns `lacing`'s
  documented streaming fallback into a 500 on a **public** bucket, the one case `anon` exists
  for. Two refusal conditions coexist and must not be merged: ADR-0011 D3b's unreachable-wrapper
  guard still **raises** (wrong-key risk); anonymity returns `None` (no capability). The guard
  reads `client.meta.config.signature_version is UNSIGNED`, never `spec.anon`.
- Anonymous **writes** refuse locally in `__setitem__`/`__delitem__`/`create_bucket` — botocore
  offers nothing here, and moto even accepts an unsigned PUT.
- `UNSIGNED` never enters the dataclass, and — measured — a bare `Config` holding it is
  unpicklable too, so it is constructed only at client-build time.

### D6 — `diagnose()` ships first, and never raises

Non-negotiable: it **never raises** (a failed resolution is a *row*; it exists to report exactly
the misconfigurations that raise); it holds **no credential value**, only a provenance label; it
never reads `.access_key`; credential *identity* is compared by access-key prefix only.

It prints both resolution passes, every resolved field with its source, the botocore version, the
`anon` state and `can_presign`, and the **v0-vs-v1 divergence table** — which is what
ADR-0007 §5 step 0 is for.

### D7 — The migration warning is a differential, not a frozen simulation

Compute it as **one v0 argument-mangler run through v1's own resolver twice**:
`v0_mangle(args, environ) -> (branch, args')`, then `compare(resolve(args'), resolve(args))`.
Both sides go through the same resolver, so imprecision cancels and the whole class of
value-gap false positives disappears. `v0_mangle` is ~20 frozen lines mapping 1:1 onto v0, and it
dies in the same commit as the shim. This replaces a frozen ~60-line copy of v0's resolution,
which an earlier draft proposed and which could rot undetected.

Facts that shape it:

- **The gate is the `is_supabase_endpoint` parameter, not a `'.supabase.'` substring** — and
  ADR-0007 §3's shim signature **omits `is_supabase_endpoint=` although v0 accepts it**
  (`s3dol/store.py:26`), which is a `TypeError` on upgrade for anyone passing it.
- **Two v0 branches are not functions of their arguments** — the supabase and skip paths call
  module-level `boto3.client(...)`, resolving through the process-global cached `DEFAULT_SESSION`.
  Excluded from the golden table, with the reason in the file, and given its own release-note
  line: *a long-lived process that builds one Supabase store pins its credentials for the life of
  the interpreter.*
- **Credentials are compared by a boolean predicate, never by snapshot.** v0 also silently nulls
  an explicitly-passed `aws_session_token` and silently changes an explicitly-passed
  `region_name` — two behaviour changes neither #15 nor ADR-0007 records.
- **Cost:** two `os.environ` lookups (~2 µs) in the common case. Never call
  `session.get_credentials()` in the check.

**Who moves:** `http_cosmo_prep` (not ours — one row, three axes: endpoint honoured, credential
identity moves, checksum may change) · `lacing` (verbatim `**s3_kwargs` forwarding) · `reelee`
**transitively through `lacing`** — the pair moves together · `py2store` unaffected.

## Consequences

**Buys.** The endpoint fix is a deletion, not a re-ranking, so there is no vendored ladder and no
conformance test guarding it. Every store pickles, unconditionally. Resolution is a pure function
with per-field provenance, exhaustively testable with a fake environ and no I/O. The one path
that could silently read a public view is deferred out of v1.

**Costs.**

- Users with a bespoke `boto3.Session` must re-express it as a `CredentialProvider`. Nobody known
  does this, but `BaseS3BucketReader.client` is public in v0, so an unmeasured population may.
- Detection supplying the full row means one announced path→virtual flip is possible for an
  endpoint-passing caller. Gated on step-0 telemetry.
- A preset author must now state `addressing_style` explicitly and think about `pinned`/`soft`.
- The `S3DolWarning` category must not cry wolf: if it fires spuriously, users filter the whole
  category and silence the migration warning with it.

**What NOT to do.**

1. Do not pass `Config(ignore_configured_endpoint_urls=True)`. Re-read D3.
2. Do not add a `strict=` kwarg. Use `filterwarnings`.
3. Do not store a `Credentials` snapshot, a `Session`, or a live client anywhere in the spec.
4. Do not read `.access_key` in `diagnose()` — it performs network I/O.
5. Do not spell "no claim" as `None` in a `Config` sub-dict; omit the key.
6. Do not re-add `AWS_REGION` support. botocore does not read it.
7. Do not assert on `client.meta.config.signature_version` — it reports `'s3v4'` while emitting
   SigV2. Assert on `'X-Amz-Algorithm'` in the URL.
