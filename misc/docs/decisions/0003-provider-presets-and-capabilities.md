# ADR-0003: Providers are config rows, not subclasses — presets + declared capabilities

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

s3dol's reason to exist is *S3 and things that speak S3*. v0.1.x handles that with per-vendor
subclasses (`SupabaseS3BucketDol`, `S3BucketDolWithouBucketCheck` [sic]) and endpoint
string-sniffing scattered through the code (`".supabase." in endpoint` appears in three
places, in the hot path of every user, including those who have never heard of Supabase).

A survey of 16 backends — AWS, MinIO, R2, Scaleway, Hetzner, Backblaze B2, Wasabi,
DigitalOcean Spaces, Ceph RadosGW, GCS XML interop, Supabase, Tigris, Oracle OCI, LocalStack,
moto, and Azure Blob — produced a clear result:

> **Every divergence is expressible as (a) botocore `Config` + client kwargs, (b) one of a
> small set of named strategies, or (c) a declared capability. Nothing needs a subclass.**

## Decision

### 1. A preset registry, and a `Preset` is data

```python
@dataclass(frozen=True)
class Preset:
    name: str
    endpoint_url: str | None = None  # may contain {placeholders}
    region_name: str | None = None
    addressing_style: str = "auto"  # 'auto' | 'path' | 'virtual'
    signature_version: str = "s3v4"
    checksum: str = "when_supported"  # 'when_supported' | 'when_required'
    payload_signing_enabled: bool | None = None
    capabilities: Capabilities = DEFAULT_CAPABILITIES
    client_kwargs: tuple[tuple[str, Any], ...] = ()  # hashable: Preset must key a cache
    # presign-specific overrides; default to the API values above
    presign_endpoint_url: str | None = None
    presign_addressing_style: str | None = None
    verified: bool = False  # against a live endpoint? with a date in the row
```

Adding a provider is adding a row. Open-closed. Users register their own:
`s3dol.presets.register(Preset(name='mycorp', ...))`.

`client_kwargs` is a hashable tuple, not a `Mapping`: a frozen dataclass holding a
`MappingProxyType` is unhashable, so it could not key an `lru_cache` or be compared in
`diagnose()`.

**Presigning needs its own config slots** because for two providers it genuinely differs from
the API config. Hetzner configures **path** style for normal use and documents *"uncomment
before you create presigned URLs"* for virtual — virtual-hosted style breaks ordinary
Get/Put there because the TLS certificate doesn't cover bucket subdomains. R2 requires
presigning against the S3 API domain even when objects are served from a custom domain.
`url_for` therefore builds and caches a **second client** from the merged presign config.

The registry is the SSOT for a set of facts nobody should have to rediscover:

| Provider | endpoint | region | addressing | notes |
|---|---|---|---|---|
| aws | SDK-resolved | real region | auto | reference semantics |
| minio | `http://{host}:{port}` | `us-east-1` conventional | **path** unless wildcard DNS | |
| r2 | `https://{account_id}.r2.cloudflarestorage.com` | **`auto`** | virtual | presign only on the S3 API domain, never a custom domain |
| scaleway | `https://s3.{region}.scw.cloud` | same string | virtual | multipart capped at **1000 parts** |
| hetzner | `https://{loc}.your-objectstorage.com` | **must repeat `{loc}`** | **path** (presign: virtual) | needs `payload_signing_enabled=False` |
| backblaze | `https://s3.{region}.backblazeb2.com` | same string | virtual | checksum `when_required` **mandatory** |
| wasabi | `https://s3.{region}.wasabisys.com` | region string | path (vendor's own advice) | `GetBucketLocation` always says `us-east` |
| gcs | `https://storage.googleapis.com` | ignored | virtual (path for dotted buckets) | **no ListObjectsV2**, no batch delete |
| supabase | `https://{ref}.storage.supabase.co/storage/v1/s3` | project region | **path, forced** | see §3 |
| localstack / moto | `http://localhost:{port}` | `us-east-1` | path | test doubles |

**Azure Blob is not in the registry.** It is not S3-compatible; reaching it requires a
translating proxy. It is a different backend (`azuredol`), not a preset. Recording this
explicitly because "S3-compatible" is often assumed to include it.

### 2. Capabilities are *declared*, and unsupported means a loud, specific error

```python
@dataclass(frozen=True)
class Capabilities:
    list_objects_v2: bool = True
    batch_delete: bool = True
    presigned_post: bool = True
    multipart: bool = True
    max_multipart_parts: int = 10_000
    min_part_size: int = 5 * 2**20
    object_tagging: bool = True
    versioning: bool = True
    conditional_writes: bool = True
    consistency: Literal["strong", "read-after-write", "eventual"] = "strong"
```

Four options were considered for handling a missing capability — silent emulation,
fail-fast at construction, `NotSupportedError` at call time, and capability introspection.
We take a **combination**, chosen per capability:

- **Emulate when the emulation is exact and cheap.** `batch_delete=False` (GCS) loops
  `DeleteObject`. The observable result is identical; only cost differs. Emulate, and say so
  in the docs.
- **Substitute when a documented equivalent exists.** `list_objects_v2=False` (GCS) uses the
  V1 paginator. Same keys, same order.
- **Raise `NotSupportedError` when there is no honest fallback.** `presigned_post=False`
  (Backblaze) cannot be emulated — raise, naming the provider and the operation.
- **Never silently degrade correctness.** Emulation is allowed only where the result is
  indistinguishable.

Capabilities are a *static table*, not a probe. A probe costs a round-trip per store, is
wrong under partial permissions, and cannot be trusted anyway (providers change). Users can
override per-connection when their deployment differs from the table — which matters most
for Ceph, whose behaviour varies more by release than by vendor.

### 3. The checksum change is the single most important entry in this ADR

Since **botocore ≥ 1.36**, integrity checksums are calculated by default
(`request_checksum_calculation="when_supported"`). Over HTTPS, `PutObject` takes the
*trailer* branch: the wire body becomes `aws-chunked` framed, `Content-Length` is deleted,
and `X-Amz-Decoded-Content-Length` carries the real size.

Providers that don't implement it react in two ways:

- **Loud** — Backblaze: `InvalidArgument: Unsupported header 'x-amz-sdk-checksum-algorithm'`.
- **Silent and destructive** — Supabase ignores `Content-Encoding: aws-chunked` and
  **persists the framing verbatim**. Objects come back as
  `<hexlen>\r\n<data>\r\n0\r\nx-amz-checksum-crc32:<b64>\r\n\r\n`.

That silent case is what v0's `SupabaseS3BucketDol.__getitem__` de-chunker exists to undo. It
is a **read-side workaround for a write-side misconfiguration**, and it is itself buggy —
it truncates at the first `\r\n` after the header, so it mangles any payload larger than one
chunk, and it misfires on any payload whose first bytes look like hex digits.

**The fix is configuration:**

```python
Config(
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required",
    s3={"addressing_style": "path"},
)
```

with **`s3transfer >= 0.11.2`** pinned — 0.11.0 unconditionally re-enabled the default
checksum from inside `TransferManager`, defeating the setting for `upload_file`/`upload_fileobj`
(boto/s3transfer#327).

Two caveats we must not forget:

1. `when_required` does **not** silence everything. `DeleteObjects` *always* ships
   `x-amz-checksum-crc32`. Providers that reject it need `batch_delete=False` — which is why
   that capability exists.
2. **Fixing this does not un-corrupt already-written objects.** See Consequences.

### 4. `signature_version` is never left to botocore — this is the single most load-bearing default in the package

botocore silently **downgrades presigning to SigV2** whenever the user has not *explicitly*
set `signature_version` (`botocore/client.py::_set_s3_presign_signature_version`). Measured:

```
no Config                        -> SigV2   (meta.config.signature_version reports 's3v4')
Config(s3={'addressing_style':'path'})  -> SigV2   (a Config that omits signature_version does NOT help)
Config(signature_version='s3v4') -> SigV4
custom endpoint_url              -> SigV2   <- every MinIO / LocalStack / moto user
us-east-1                        -> SigV2
eu-central-1                     -> SigV4
```

AWS rejects SigV2 on any bucket created after 2020-06-24; R2, Backblaze, Scaleway and modern
MinIO reject it outright. And `client.meta.config.signature_version` **reports `'s3v4'` while
producing SigV2**, so introspection cannot catch it.

`S3Connection` therefore always constructs a `botocore.Config` with an explicit
`signature_version` (default `'s3v4'`), **including when `preset is None`**.

This is a live bug in v0, not merely a design gap: `s3dol` today presigns with SigV2 for
`us-east-1` and for every custom endpoint. `test_url_for.py` cannot see it because it asserts
`"Signature" in url` — and a SigV2 URL contains `Signature=` while SigV4 contains
`X-Amz-Signature=`. The tier-1 assertion is therefore
`'X-Amz-Algorithm=AWS4-HMAC-SHA256' in url and 'AWSAccessKeyId' not in url`.

### 5. Anonymous access

`anon` is `bool | 'auto'` on the connection **and surfaced directly on `s3_store(...)`** —
reading a public bucket must not require learning Layer A. It is translated to
`botocore.UNSIGNED` *inside* the `cached_property` that builds the client: the singleton is
**unpicklable**, so it must never enter the dataclass or every anonymous store would fail
[ADR-0008](0008-testing-architecture.md)'s pickle conformance.

`url_for` raises `NotSupported('url_for', reason='anonymous credentials cannot sign a URL')`
when the resolved signer is `UNSIGNED` — botocore otherwise returns a plain unsigned URL with
no error, which is a wrong answer under the never-silently-wrong goal.

`'auto'` means *"try unsigned if no credentials resolve at all"*. It explicitly does **not**
mean "retry unsigned after `AccessDenied`": an expired token would then silently downgrade to
a different, public view of the data. It warns, naming what it did. Requester-pays buckets
forbid anonymous access outright, so a silent fallback would confuse there regardless.

### 6. Per-vendor classes are deleted

`SupabaseS3BucketDol` → `preset='supabase'`. `S3BucketDolWithouBucketCheck` → the default
(no probe-then-act, see [ADR-0001](0001-layered-architecture.md)). Provider detection from
an endpoint URL happens **once, at connection construction**, never in `__getitem__`.

## Consequences

**Buys.** A new provider is a row and a test. No vendor logic in the read/write path. The
matrix is documented where a user can find it.

**Costs.** The table will drift; it needs a dated review and an escape hatch (it has one:
users override any field per-connection). And most rows are **doc-sourced, not verified
against a live endpoint** — they are marked as such in `presets.py`. Wrong values mostly fail
loudly, which is the acceptable failure mode, but the Supabase entry in particular is
inferred from behaviour rather than confirmed.

**What NOT to do.**

1. **Do not delete the Supabase de-chunker in the same change that adds the checksum fix.**
   Sequence it: (1) ship `preset='supabase'`; (2) validate against a live project; (3) ship
   a `detect_chunked_framing(store)` diagnostic and a documented repair path for objects
   already written wrong; (4) *only then* remove the codec — and keep it available as an
   explicitly-chosen value codec, never a default. Deleting it early converts silent
   corruption into silently-unreadable data.
2. Do not probe capabilities at construction.
3. Do not let a preset carry credentials. Presets are public, shareable, committable config.
