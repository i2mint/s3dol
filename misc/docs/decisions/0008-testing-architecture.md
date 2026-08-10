# ADR-0008: Four test tiers, a shipped in-memory fake, and an exported conformance suite

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

Today: **7 tests**, of which **5 need a live S3 endpoint**. `pytest -q` on a developer
machine gives `3 passed, 5 deselected`. The three that always run are the presigned-URL
tests, and they assert only substring presence — `"test-bucket" in url`, `"Signature" in url`
— so a URL pointing at the **wrong key** passes ([ADR-0006](0006-key-scoping-and-dol-fixes.md) §2).

`conftest.py` is genuinely thoughtful (it TCP-probes the endpoint and *deselects* rather than
skips), but the mechanism hides the problem: the suite is green while barely testing
anything, and `S3DOL_S3_PROBE_ENDPOINT` can only *enable* tests, never *redirect* them,
because the tests hardcode `localhost:4566`.

s3dol also ships **nothing** for downstream users. A user whose service takes a store as a
dependency has no way to test their code without S3.

## Decision

### Four tiers, with tier 3 as the merge gate

| Tier | What | Runs |
|---|---|---|
| 1 | **Pure unit, no I/O.** Error classification over *synthesized* `ClientError`s; key-codec property tests; preset merging; the prefix round-trip law. Should be the majority. | always |
| 2 | **In-process `@mock_aws` (moto).** The hermetic default. | always |
| 3 | **Container: moto-server + MinIO.** One conformance suite parameterized over endpoints. | merge gate |
| 4 | **Live providers** (R2, B2, Supabase), `@pytest.mark.live`, credentialed. | opt-in / nightly |

Tier 1 matters more than it sounds: the error taxonomy, the key laws and the preset registry
are where the correctness lives, and none of them needs a network. Building tier 1 first is
what lets the rewrite be red/green rather than hopeful.

### `s3dol.testing` is shipped, not just `tests/`

```python
from s3dol.testing import mock_s3, conformance


def test_my_service():
    assert MediaService(mock_s3()).url("a")  # no network, no docker, no moto
```

`mock_s3()` is an in-process fake that passes **the same conformance suite** as the real
store — including `url_for`, `info`, ranged reads, `delete_many` and the error taxonomy. This
is the tier the ecosystem is missing: `azuredol` ships an Azurite context manager, but
nothing in the family ships an in-memory tier, and that is what downstream users actually
need.

`conformance` is exported so sibling `*dol` packages and user code can run it against their
own stores.

### What the conformance suite must assert

Beyond the obvious Mapping laws, these are the ones that would have caught real bugs:

1. **Prefix scoping round-trip.** Fixture must include sibling, non-matching, marker and
   un-normalized cases: `{'a/b','a/c','z','ab/x','a/'}` scoped to `a/`, **and**
   `{'logs/2026.txt','logs2/2026.txt','logsX'}` scoped to the un-terminated `logs` — which
   must normalize to `logs/` and expose exactly one key. This is
   [ADR-0006](0006-key-scoping-and-dol-fixes.md) §1, enforced.
2. **`url_for` correctness**, split by tier:
   - **2a (all tiers, structural):** parse the URL; the path must equal the fully-prefixed
     key, and the query must carry `X-Amz-Algorithm=AWS4-HMAC-SHA256` and **not**
     `AWSAccessKeyId` (the SigV2 tell — see [ADR-0003](0003-provider-presets-and-capabilities.md) §4).
   - **2b (tier 2+):** fetch it and get the object back. Use `requests`, which moto patches —
     `urllib` is **not** patched and would leave the machine. This proves path correctness
     only: moto validates neither signature nor expiry. Add `requests` to the `test` extra
     (today it is only transitive via `moto[s3]`).
3. **`iter`/`contains` agreement**: `all(k in s for k in s)`. Fixture includes
   `plain, 'a b', café, 'a+b', 'a\rb', 'p/x y', 'a%20b'` — the keys that break if anyone
   reintroduces `EncodingType`.
4. **Never silently empty**: listing a missing/unlistable bucket raises.
5. **Value law**: `s[k] = v ⟹ s[k] == normalize(v)`, **Layer B only and re-readable sources
   only** — stream refs are one-shot ([ADR-0005](0005-large-object-io.md) N1), and through a
   Layer C json codec `normalize` is `json.loads ∘ json.dumps`, which is not identity
   (`(1,2) → [1,2]`, `{1:'a'} → {'1':'a'}`). Per-layer `normalize` is a parameter of the suite.
6. **Picklability**: `pickle.loads(pickle.dumps(store))` works, and a `ProcessPoolExecutor`
   round-trip works. Today `pickle` raises `PicklingError` and `deepcopy` raises
   `RecursionError` — a store that cannot cross a process boundary is unusable with Dask or
   multiprocessing, and nothing currently notices.
7. **No secret in `repr`**: `assert SECRET not in repr(conn)`, plus a traceback-locals scan.
8. **`setdefault` on an archived key does not mutate the object**
   ([ADR-0004](0004-error-taxonomy.md) §4).
9. **Every shipped recipe's encoder output is a member of `BytesSource`** — this is what
   catches `ValueCodecs.json()` returning `str`.

The suite is parameterized by capability flags, because not every tier can run every
assertion: `mock_s3()` runs 2a but not 2b (no HTTP server to fetch from). The claim is
therefore *"`mock_s3()` passes the same suite, parameterized by an `endpoint_is_fetchable`
capability"* — not *"the same suite"* unqualified.

The `len()`-raises assertion belongs on the **user-facing** store, which is the Layer B store
itself now that prefixing lives there.

### Cost model: `__len__` is not implemented

`dol.base.Collection.__len__` counts by iterating, so `len(store)` is a full paginated
listing — and worse, `list(store)` currently costs **two** listings because `list()` takes a
length hint from `__len__`. Following `azuredol` §2, `BucketStore` does not implement
`__len__` at all; `len(store)` raises `TypeError` with guidance to `sum(1 for _ in store)`.
`EndpointStore.__len__` is fine — bucket counts are small.

Listing caches are opt-in (`dol.cached_keys`), never default: the notebook explorer wants
them and the pipeline is actively harmed by them.

### Fixing the harness

- Delete the deselect-by-module-stem allowlist — it silently swallows any new hermetic test.
- Every test gets a unique bucket via fixture; `monkeypatch.setenv` only (tests currently
  mutate `os.environ` process-wide).
- Make the endpoint env var able to **redirect**, not just enable.
- Doctests run in the default gate against the in-memory fake, so every documented example is
  executable with no endpoint. `--doctest-modules` on; `py.typed` shipped.
- Add `lacing`'s `tests/test_artifact_store_s3.py` as a required cross-repo gate — it is the
  tightest existing contract test for s3dol.

### On moto's fidelity

moto is good enough for tier 2 but diverges, and two of the divergences are exactly where this
package's correctness lives:

- **HEAD error responses.** moto returns an error **body** on HEAD where AWS returns none, so
  `head_object` against a missing bucket yields `Code='NoSuchBucket'` under moto and a
  body-less `Code='404'` in production. This is the single divergence most likely to ship a
  bug — see [ADR-0004](0004-error-taxonomy.md) §2. Tier 1 must feed the classifier synthesized
  body-less HEAD errors, because **moto cannot produce them**.
- **`DeleteObjects` limits.** moto accepts 1001 keys where AWS caps at 1000, so tier 2 will
  never catch a missing chunker.

Also: it accepts >1024-byte keys, and has open `aws-chunked` and composite-checksum bugs. It
*is* faithful on `CreateBucket`/`LocationConstraint` (checked — a claim to the contrary in the
research was wrong, and it was being used to justify a non-moto tier). And it validates no
presigned-URL signature at all, which is why assertion 2a is structural.

## Consequences

**Buys.** A suite that is green because it passes, not because it deselected. Downstream
users can test. Provider compatibility is actually exercised. The rewrite gets a safety net
before it starts.

**Costs.** An in-memory fake is code that must itself stay faithful — mitigated by running
the *same* conformance suite against it and the real thing, so drift fails a test. Tier 3
needs containers in CI.

**What NOT to do.** Do not assert on presigned URLs by substring. Do not let a test depend on
a bucket another test created. Do not add a tier-2-only test for behaviour that only tier 3
can distinguish.
