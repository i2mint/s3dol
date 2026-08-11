# s3dol design docs

Hand-written design material. (Generated API docs go elsewhere — `docs/` is Sphinx/epythet
output; don't put prose here that a build step will overwrite.)

| Document | Contents |
|---|---|
| [architecture.md](architecture.md) | **Start here.** The v1 four-layer design, module layout, and the contracts each layer enforces. |
| [decisions/](decisions/) | ADRs — one defaulted choice per document, with the evidence behind it. |

## ADR index

| # | Decision | Read it when |
|---|---|---|
| [0001](decisions/0001-layered-architecture.md) | Four layers, adopted from `azuredol` | adding any class or module |
| [0002](decisions/0002-boto3-as-engine.md) | boto3 stays the engine; alternatives are optional and later | tempted by obstore / minio / async |
| [0003](decisions/0003-provider-presets-and-capabilities.md) | Providers are config rows, not subclasses | adding a backend, or hitting "works on AWS, breaks on X" |
| [0004](decisions/0004-error-taxonomy.md) | One error seam; a taxonomy that never lies | touching exception handling |
| [0005](decisions/0005-large-object-io.md) | Value refs + injected transfer strategies | anything about big objects, streaming, or `s[k] = v` types |
| [0006](decisions/0006-key-scoping-and-dol-fixes.md) | **Prefix normalization, key validity, `dol` traps** | always, if you touch keys |
| [0007](decisions/0007-naming-and-compatibility.md) | Names, public API, deprecation path | renaming anything, or planning the release |
| [0008](decisions/0008-testing-architecture.md) | Four tiers, shipped fake, exported conformance | writing a test |
| [0009](decisions/0009-scope-and-deferrals.md) | v1 scope, deferrals, the `s3dol`/`botodol` line | proposing a feature |
| [0010](decisions/0010-bucket-and-bulk-operations.md) | Bucket-existence policy, `delete_many`, cascading delete | touching bucket lifecycle or bulk ops |
| [0011](decisions/0011-keyed-capability-surface.md) | **No key-taking methods; capabilities are sibling stores** | adding any capability, or wondering where `url_for` went |
| [0012](decisions/0012-credential-and-endpoint-resolution.md) | **Credential + endpoint resolution**: a pure `resolve()`, a picklable spec, two endpoint rungs | touching credentials, endpoints, presets, `anon`, or the migration warning |

## The five things most likely to bite you

1. **`url_for` presigns with SigV2 unless you set `signature_version` explicitly** — for
   `us-east-1` and for *every custom endpoint* (MinIO, LocalStack, R2). AWS rejects SigV2 on
   buckets created after June 2020. `client.meta.config.signature_version` reports `'s3v4'`
   while doing it, and a `"Signature" in url` assertion cannot tell the two apart (SigV2 has
   `Signature=`, SigV4 has `X-Amz-Signature=`). This is a live bug in v0.1.x.
   [ADR-0003 §4](decisions/0003-provider-presets-and-capabilities.md).
2. **`dol`'s prefix machinery silently corrupts non-matching keys.** `mk_relative_path_store`,
   `KeyCodecs.prefixed` and `prefixless_view` turn a sibling key `ab/x` into `/x` and a
   non-matching key `z` into `''`. This is why the prefix lives in the leaf; if you stack a
   `dol` codec anyway, only `Pipe(filt_iter.prefixes(p), KeyCodecs.prefixed(p))` is safe — and
   only after normalizing `p` to end in the delimiter.
   [ADR-0006 §1](decisions/0006-key-scoping-and-dol-fixes.md).
3. **A `dol` wrapper delegates methods with the outer, unmapped key**, so a keyed method like
   `url_for` silently addresses the wrong object. Capability detection can't see it, and
   **the obvious escape is also broken**: `inner_most_key(wrapped_self(self), k)` is silently
   wrong when nothing holds a reference to the wrapper (`inner_most_key(self, k)` returns
   `None`, which at least fails loudly). So s3dol has **no key-taking methods**: capabilities
   are sibling stores keyed through `__getitem__` — the one thing `dol` maps correctly at every
   depth. [ADR-0011](decisions/0011-keyed-capability-surface.md).
4. **botocore ≥1.36 sends checksums by default**, and several S3-compatible providers either
   reject them loudly or persist the `aws-chunked` framing *into the object body*. The fix is
   client config plus `s3transfer>=0.11.2`.
   [ADR-0003 §3](decisions/0003-provider-presets-and-capabilities.md).
5. **Never pass `EncodingType` to a list call.** botocore sets it *and* decodes the response,
   but only when it set it itself — passing it explicitly drops 5 of 7 test keys out of
   round-trip. [ADR-0006 §4](decisions/0006-key-scoping-and-dol-fixes.md).

## A note on how these docs were revised

ADRs 0001 and 0006 were substantially rewritten after an adversarial review executed their
claims against the real `dol`, `botocore` and `moto`. The original design put prefix scoping in
a `dol` wrapper above an absolute-keyed leaf, citing `azuredol` as precedent — but `azuredol`'s
*code* does the opposite of its *documentation*, and the wrapper design produces six
silently-wrong methods plus full-bucket scans. The lesson is recorded in ADR-0001: **a sibling
package's design doc is a claim; its source is the evidence.**

## Convention

When you change a default, update the ADR that documents it **in the same PR**. When a
decision is reversed, add a new ADR that supersedes the old one rather than editing history.
