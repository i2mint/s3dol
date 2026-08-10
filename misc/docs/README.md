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
| [0006](decisions/0006-key-scoping-and-dol-fixes.md) | **Prefix scoping — read before writing key code** | always, if you touch keys |
| [0007](decisions/0007-naming-and-compatibility.md) | Names, public API, deprecation path | renaming anything, or planning the release |
| [0008](decisions/0008-testing-architecture.md) | Four tiers, shipped fake, exported conformance | writing a test |
| [0009](decisions/0009-scope-and-deferrals.md) | v1 scope, deferrals, the `s3dol`/`botodol` line | proposing a feature |

## The three things most likely to bite you

1. **`dol`'s prefix machinery silently corrupts non-matching keys.** `mk_relative_path_store`,
   `KeyCodecs.prefixed` and `prefixless_view` turn a sibling key `ab/x` into `/x` and a
   non-matching key `z` into `''`. Only `Pipe(filt_iter.prefixes(p), KeyCodecs.prefixed(p))`
   is safe. [ADR-0006 §1](decisions/0006-key-scoping-and-dol-fixes.md).
2. **`url_for` through a `dol` key-wrap returns a URL for the wrong object, silently**, and
   `isinstance(store, SupportsUrlFor)` still says `True`.
   [ADR-0006 §2](decisions/0006-key-scoping-and-dol-fixes.md).
3. **botocore ≥1.36 sends checksums by default**, and several S3-compatible providers either
   reject them loudly or persist the `aws-chunked` framing *into the object body*. The fix is
   client config plus `s3transfer>=0.11.2`.
   [ADR-0003 §3](decisions/0003-provider-presets-and-capabilities.md).

## Convention

When you change a default, update the ADR that documents it **in the same PR**. When a
decision is reversed, add a new ADR that supersedes the old one rather than editing history.
