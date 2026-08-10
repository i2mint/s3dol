# ADR-0007: Names, public API, and the deprecation path

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

The v0.1.x names are the least consistent in the `*dol` family:

- `S3BucketDolWithouBucketCheck` ships a typo.
- `S3Store` is a **function** annotated `-> Store` that never returns a `dol.Store`
  (`isinstance(S3Store(...), dol.Store)` is `False` — verified).
- The `Dol` suffix (`S3Dol`, `S3ClientDol`, `BaseS3BucketDol`) is used by **no other** blob
  adapter: `azuredol` has `ContainerCollection`/`ContainerReader`/`ContainerStore`; `cosmodol`
  has `CosmosItems`/`CosmosAccount`.
- Six overlapping entry points exist, two are exported, and none is the one a first-time
  user should type.
- `S3DolReadOnly` is a `functools.partial`, so it is a function — `isinstance` and
  subclassing don't work on it.

There are real dependents, which bounds how freely we rename:

| Dependent | Usage | Ours to merge? |
|---|---|---|
| `lacing` | `from s3dol.store import S3Store`; `S3Store(bucket_name, path=prefix, **kw)` — bucket **positional** | yes |
| `http_cosmo_prep` | `from s3dol.store import S3Store`; `S3Store(path=..., bucket_name=..., endpoint_url=...)` — bucket **by keyword** | **no** (cosmograph-org, needs peer review) |
| `reelee` | declares `s3dol` in `pyproject.toml` | yes |
| `py2store` | `from s3dol.tests.util import extract_s3_access_info, get_s3_test_access_info_from_env_vars`, wrapped in `suppress(ImportError)` — so breakage is **silent** | yes |

And merging auto-publishes to PyPI (`[tool.wads.ci.publish] enabled = true`); version numbers
burn permanently.

## Decision

### 1. New names, mirroring `dol.filesys` and `azuredol`

| v0 | v1 | Why |
|---|---|---|
| `BaseS3BucketReader` | `BucketCollection` + `BucketReader` | splits two conflated responsibilities; mirrors `FileCollection`/`FileBytesReader` |
| `BaseS3BucketDol` | `BucketStore` | "Dol" carries no meaning |
| `S3BucketReader` / `S3BucketDol` | (Layer C) `BucketReader` / `BucketStore` with `prefix=` | one class, prefix is a parameter |
| `S3ClientReader` / `S3ClientDol` | `EndpointReader` / `EndpointStore` | the key *is* a bucket name; "Client" names the implementation, not the mapping |
| `S3Dol` | `S3Profiles` | it maps AWS **profile** names → bucket collections |
| `S3DolReadOnly` | *deleted* | use the `*Reader` classes |
| `S3BucketDolWithouBucketCheck` | *deleted* | typo, and the behaviour is now the default |
| `SupabaseS3BucketDol` | *deleted* | → `preset='supabase'` ([ADR-0003](0003-provider-presets-and-capabilities.md)) |
| `s3dol.utility` | `s3dol.errors` | 5 of 7 exception classes and 5 of 9 `Resp` methods are dead |
| — | `ObjectHandle` | new; the per-object escape hatch |
| — | `s3_store(...)` | the lowercase factory, matching `azuredol.azure_store` |

**`BucketStore` and `BucketsStore` are rejected as a pair** — the package's two most-used
classes, one silent `s` apart, with *opposite* key spaces (object keys vs bucket names), where
a typo yields a working, silently-wrong store. `azuredol` avoids this by naming the
*containing* resource (`ContainerStore` inside one container, `AccountStore` of containers);
`BucketStore` / `EndpointStore` is the same move. `S3Profiles`, not `S3Endpoints`, because its
keys are verifiably AWS profile names (plus the literal `'environment variables'`), never
endpoints — and its values are bucket *collections*, not bucket stores.

Vocabulary alignment with the family, since ADR-0001 claims "a reader of `azuredol` can read
`s3dol`": use `value_codec=` (not `codec=`), and keep `prefix=`.

**Public API:** `s3_store`, `BucketStore`, `BucketReader`, `EndpointStore`, `ObjectHandle`,
`S3Connection`, `Filepath`/`Chunks`/`Streamable`, the error classes, `diagnose`. Everything
else is implementation. (A layered library legitimately has more constructors than a flat one;
what v0 lacked was a statement of which were *public*.)

### 2. The one-liner

`s3dol.s3_store(bucket)` is what line 1 of the README shows. Zero credential ceremony, no
`make_bucket`/`skip_bucket_check`/`profile_name` in sight.

### 3. `s3dol.store.S3Store` becomes a deprecated shim, removed in v2

It keeps its current signature exactly — `bucket_name` accepted **both positionally and by
keyword**, `path=` (not renamed), plus `aws_access_key_id`, `aws_secret_access_key`,
`aws_session_token`, `endpoint_url`, `region_name`, `profile_name`, `make_bucket`,
`skip_bucket_check` — forwards to the new API, and emits `DeprecationWarning`.

`s3dol/store.py` **must survive as a module**: both external dependents import the
fully-qualified path, not the package root.

The shim is not merely compatible, it is a **fix delivery mechanism**: dependents get the
corrected endpoint/credential resolution ([ADR-0002](0002-boto3-as-engine.md),
[ADR-0003](0003-provider-presets-and-capabilities.md)) without changing a line. That matters
most for `http_cosmo_prep`, which passes an explicit `endpoint_url` for a non-AWS endpoint and
is therefore **at risk**: whenever `AWS_ACCESS_KEY_ID` is exported, `base.py:82` drops its
endpoint and the store aims at AWS instead.

Be careful how strongly that is stated. Bucket names are globally unique on AWS, so if they do
not own that name the misrouted call fails loudly and someone would have noticed. The genuinely
*silent* variant is `AWS_ENDPOINT_URL_S3` — and it has a consequence worth naming: v0's
**effective** precedence when env credentials exist is
`AWS_ENDPOINT_URL_S3 > AWS_ENDPOINT_URL > explicit kwarg`, and the corrected ladder **inverts
the top of it**. Any deployment currently relying on `AWS_ENDPOINT_URL_S3` to override a stale
hard-coded endpoint will silently redirect on upgrade. That is why §5 has a step 0.

`s3dol/tests/util.py` keeps `extract_s3_access_info` and
`get_s3_test_access_info_from_env_vars`, because `py2store`'s import of them fails silently.

### 4. Behaviour changes that the shim deliberately does NOT preserve

These are bug fixes, and preserving them would mean preserving data-misrouting:

| v0 behaviour | v1 | shim keeps v0? |
|---|---|---|
| explicit `endpoint_url` dropped when env credentials exist | honoured | no — this is the fix |
| explicit credentials overridden by env | explicit wins | no — this is the fix |
| write to a missing bucket **creates** it (even with `make_bucket=False`) | per `on_missing_bucket` ([ADR-0010](0010-bucket-and-bulk-operations.md)) | no |
| `list(store)` returns `[]` on any error | raises | no |
| `del endpoint[name]` cascades, unpaginated | refuses non-empty; `force=True` is explicit | no |
| `store[k] = 'a str'` accepted | `TypeError` ([ADR-0005](0005-large-object-io.md) §2) | **yes** — shim wraps with `str.encode` + `DeprecationWarning` |
| `store['folder/']` returns a sub-store | removed; use `s3dol.sub(store, …)` ([ADR-0011](0011-keyed-capability-surface.md)) | **yes** |
| `del store[absent]` silently succeeds | still idempotent ([ADR-0010](0010-bucket-and-bulk-operations.md)) | n/a — unchanged |

The last three rows exist because `http_cosmo_prep`'s **currently-passing** tests do all
three: `s['current_time.txt'] = str(...)` (`tests/test_prod_resources.py:26-27`),
`s['scrap/test_s3_store/']` as a sub-store, and `del s[...]` relying on the no-op. A shim whose
justification is "existing users keep working" must actually keep them working.

Each remaining row is called out in the release notes as behaviour-changing. The resolution ladder —
explicit kwargs > preset > `AWS_ENDPOINT_URL_S3` > `AWS_ENDPOINT_URL` > profile > chain — is
documented and tested, and `s3dol.diagnose()` prints what resolved and from where (never the
secret).

`AWS_ENDPOINT_URL_S3` deserves its own note: it silently outranks everything, is
service-specific so it beats the generic `AWS_ENDPOINT_URL`, and it is **not** in botocore's
`BOTOCORE_DEFAUT_SESSION_VARIABLES`, so it is invisible to naive introspection. It is also
why CI is currently green on a data-misrouting bug — the test env re-supplies the endpoint
that the code throws away.

### 5. Release mechanics

`s3dol.__version__` is added (absent today). Deprecations name their removal version. Order
of operations, because publishing is automatic and irreversible:

0. **Ship `s3dol.diagnose()` in a non-breaking 0.1.x patch.** It prints the resolved endpoint,
   region and credential *source* (never the secret). Have each dependent run it in their real
   environment and record the answer. Only then flip the resolution order — and ship the
   endpoint/credential fix and the naming/API change as **separate releases**, so a dependent
   who sees their data target move can bisect it to one change.
1. Land the `dol` fixes ([ADR-0006](0006-key-scoping-and-dol-fixes.md) §3), and raise the dol
   floor to ≥0.3.58 for `wrapped_self`.
2. Pin `s3dol<1` in `lacing` and `reelee`, and open the `http_cosmo_prep` PR. Also **re-enable
   `lacing`'s `tests/test_artifact_store_s3.py`**, which is currently excluded in
   `lacing/pyproject.toml:217-223` with the comment *"fail in CI's clean env (s3dol url_for →
   None)"* — [ADR-0008](0008-testing-architecture.md) proposes it as a cross-repo gate, and a
   disabled test cannot gate anything.
3. Merge s3dol v1. Release notes lead with the behaviour changes.

Step 2 before step 3 is not optional: a stalled cross-org PR must not be able to strand that
repo on a broken line.

## Consequences

**Buys.** Names that read like the rest of the family. Existing users keep working *and* get
the bug fixes. A documented migration.

**Costs.** A compat module to carry until v2, and a legacy signature (`path=`, the
`make_bucket` tri-state) that must keep working while the new API uses better vocabulary
(`prefix=`, `on_missing_bucket=`). Two vocabularies coexist for one major version.

**What NOT to do.** Do not remove `s3dol/store.py`. Do not rename `path=` on the shim. Do not
merge before the dependents are pinned.
