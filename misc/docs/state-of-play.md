# s3dol v1 redesign — state of play

**Date:** 2026-08-10 · **Status:** design complete and merged (#13); **no code written yet**

This document exists so the next person (or agent) picking this up does not have to redo the
analysis. It carries the intent, the findings, the dead ends, and the two design questions
that are still genuinely open.

Read order: this file → [architecture.md](architecture.md) → [decisions/](decisions/) as needed.

---

## 1. The original intent

Verbatim in spirit, from the request that started this:

> Major refactor of s3dol — modernize it, optimize it, make the design as clean, SOLID and
> flexible as possible. Breaking changes are acceptable (indicate with the version number and
> README commentary).
>
> s3dol should be for S3 **or any S3-compatible interface** to blob storage (MinIO, R2,
> Scaleway, Hetzner…). When they conflict, **S3 is the base**, but we should be able to
> extend/adapt to the alternatives — ready-made objects or configs that handle their
> particulars: prefilling and/or validating instance parameters, doing something sensible
> about functionality that isn't present in an alternative, so there are minimal surprises
> downstream.
>
> The main role of s3dol is to offer base `collections.abc` interfaces (Collection, Mapping,
> MutableMapping) to S3 blob storage and compatible alternatives.
>
> **DOL IS THE BASE: KNOW IT AND USE IT.** Avoid reimplementing what dol already handles
> (key and value transformation, path formatting, etc.).
>
> On multipart uploads: not as an extra method of a MutableMapping (that violates the purity
> we seek in our base interfaces), but as *a way to do* `s[k] = v`. Though we don't want to
> violate the purity of the base interfaces (no extra methods, except hidden underscore
> helpers), we do want to maximize how many infra-specific functionalities and optimizations
> are offered — **but via the simple interface.** This could be a parameter (possibly a
> strategy pattern) at instantiation and/or a subclass and/or a decorator that transforms HOW
> we do it.

A follow-up clarification that materially changed the design, and which should keep governing
it:

> The infra research describes a current need for one particular project. We should
> accommodate it, but **not let it define what we do**, since our use of s3dol is more
> general. Sometimes it will be for data scientists in a notebook who want to poke around
> some data. Sometimes a straight migration from a filesystem to S3, or back, or a sync
> between both. Use the infra research as **a point of a line/plane, not the line or plane
> itself.**

That clarification is why the design is driven by a use-case matrix (notebook explorer,
filesystem↔S3 migration/sync, production app backend, large media, local/CI testing,
analytics interop, ETL, archival, multi-account, credential hygiene) rather than by any single
consumer's requirements.

## 2. Current state

| | |
|---|---|
| Merged | **#13** — `misc/docs/architecture.md` + ADRs 0001–0010. Docs only; no code touched. |
| Tracking | **#11** — phases P0–P8 |
| Deferred scope | **#12** — v1.x interfaces + the `s3dol`/`botodol` line |
| Live bug found | **#10** — `url_for` emits SigV2 |
| Answered | **#5** (multipart / value types), **discussion #6** (extendable KeyError) |
| Upstream blockers | **i2mint/dol#82** (prefix corruption), **i2mint/dol#83** (delegation with unmapped key) |
| Open design questions | **§7** and **§8** of this document |

**Nothing has been implemented.** The repo still ships v0.1.9 unchanged.

## 3. The ten ADRs, in one paragraph each

**[0001 — Four-layer architecture](decisions/0001-layered-architecture.md).** Three layers
(`connection` → `base` → `recipes`), mirroring `azuredol` so one adapter in the family reads
like the next. Layer B owns a normalized `prefix`; `dol` supplies codecs, filtering and
caching but **not** prefix arithmetic. Contains the §"Why the prefix lives in the leaf"
post-mortem of the abandoned design — see §5 and §7 below. Reader classes are real classes,
not instances with methods deleted (`dol.mk_read_only` is *non-functional* on a `dol` store:
`ro['a'] = b'2'` succeeds silently).

**[0002 — boto3 stays the engine](decisions/0002-boto3-as-engine.md).** Alternatives (obstore,
minio SDK, s3fs, aioboto3) are deferred behind a module-internal protocol. Decisive fact:
`obstore` has **no bucket create/delete/list operations**, so it cannot back the bucket level;
and every portability fix we need (checksums, error codes, transfer config) is botocore-shaped.
Import cost is addressed by lazy-importing boto3, not by switching engines. Budget is a
**delta** over `import dol` (~10 ms), because `dol` alone is ~61 ms and an absolute sub-30 ms
target is arithmetically impossible.

**[0003 — Providers are config rows](decisions/0003-provider-presets-and-capabilities.md).** A
`Preset` frozen dataclass + registry covering 16 backends; adding a provider is adding a row.
`Capabilities` declares what a backend lacks, handled per-capability: emulate when exact
(batch delete → loop), substitute when equivalent (ListObjectsV2 → V1), raise
`NotSupported` when there is no honest fallback. Contains §3, the botocore-≥1.36 checksum
analysis (the single most valuable piece of research in the set), §4 the SigV4 mandate, and §5
anonymous access.

**[0004 — Error taxonomy](decisions/0004-error-taxonomy.md).** One `@translate_s3_errors` seam;
classification on `(operation, code, status)`, not exception type, because HEAD has no body so
the modelled exceptions never fire. Auth/config/transport errors are **never** translated to
`KeyError`. Includes the 403-means-absent policy and the `ObjectArchived(KeyError)` decision
with its `setdefault` guard.

**[0005 — Large-object I/O](decisions/0005-large-object-io.md).** Answers #5. Closed write
domain `bytes | bytearray | BinaryIO | Filepath | Chunks | Streamable`; read codomain `bytes`;
`str` rejected on decidability. Asymmetry is legitimate under the N1/N2/N3 law. **No write-only
store split.** Transfer strategy injected at construction. Also states the rule for what may
join the Mapping surface at all.

**[0006 — Prefix normalization, key validity, dol traps](decisions/0006-key-scoping-and-dol-fixes.md).**
Prefix normalization is mandatory and comes first. The dol corruption table. The delegation
trap and the `inner_most_key(wrapped_self(self), k)` form. The `EncodingType` prohibition. The
reduced dol upstream list.

**[0007 — Naming and compatibility](decisions/0007-naming-and-compatibility.md).** New names;
`s3dol.store.S3Store` as a deprecated shim removed in v2, doubling as the fix-delivery
mechanism; the behaviour-change table including three rows a dependent's passing tests depend
on; and the release ordering (step 0 = ship `diagnose()` first).

**[0008 — Testing architecture](decisions/0008-testing-architecture.md).** Four tiers with
tier 3 as the merge gate; `s3dol.testing` shipped with an in-memory fake **and an exported
conformance suite**; nine conformance assertions; the moto fidelity list.

**[0009 — Scope and deferrals](decisions/0009-scope-and-deferrals.md).** v1 = core +
large-object I/O. The **endpoint test** as the mechanical `s3dol`/`botodol` line. The zarr
cautionary tale (its v3 async ABC killed five of its own backends) as the reason for
restraint. Rule: no new `Protocol` without two implementers.

**[0010 — Bucket and bulk operations](decisions/0010-bucket-and-bulk-operations.md).**
`on_missing_bucket='assume'` default (no probe, ever); `delete_many` chunking at 1000 with
`S3PartialFailure` (not `ExceptionGroup` — 3.10 floor); cascading delete stays explicit and
paginates.

## 4. Verified findings — what is actually wrong with v0.1.9

All executed, not inferred. Repro snippets in §9.

| # | Finding | Impact |
|---|---|---|
| 1 | Explicit `endpoint_url` **silently dropped** when env credentials exist (`base.py:82` doesn't forward it) | store configured for R2/MinIO talks to **AWS** |
| 2 | Explicit credentials **overridden by env** (`base.py:71` merges env over kwargs) | wrong identity, silently |
| 3 | `list(store)` returns `[]` on **any** error (`base.py:109` wraps listing in `_bucket_exists`, which swallows every `ClientError`) | wrong bucket / expired token / missing permission → empty list |
| 4 | Writing to a missing bucket **creates** it, even with `make_bucket=False` | a typo mints a bucket |
| 5 | `url_for` presigns with **SigV2** for `us-east-1` and every custom endpoint | rejected by modern S3, R2, B2, Scaleway, MinIO (#10) |
| 6 | Stores are **unpicklable** (`PicklingError`), `deepcopy` → `RecursionError` | unusable with `ProcessPoolExecutor`, Dask |
| 7 | Anonymous access to public buckets **impossible** (raises before any request) | the whole open-data use case |
| 8 | `del store[missing]` silently succeeds; `del client[bucket]` cascades **unpaginated** (deletes ≤1000 then fails) | partial, non-idempotent destruction |
| 9 | `__iter__` hides trailing-delimiter keys but `__contains__` doesn't | `'d/' in s` is `True`, `list(s)` omits it; `dict(s)` inconsistent |
| 10 | `SupabaseS3BucketDol.__getitem__` hand-parses HTTP chunked framing out of object bodies | truncates payloads >1 chunk; misfires on payloads starting with hex digits |
| 11 | `list(s)` costs **two** full listings (`list()` takes a length hint from `__len__`, which counts by iterating) | 2× request cost on every listing |
| 12 | 5 of 7 tests need a live endpoint; the 3 that always run assert `"Signature" in url` | a URL for the **wrong key** passes; a SigV2 URL passes |

Findings 1, 2 and 5 share a root cause: credential/endpoint resolution is spread across three
functions instead of living in one connection object. Finding 10 is a read-side workaround for
a write-side client misconfiguration (the checksum default — ADR-0003 §3).

## 5. What we tried and abandoned

Recorded so nobody re-proposes them without new information.

**(a) Prefix scoping as a `dol` wrapper above an absolute-keyed leaf.** This was the first
draft's central architectural bet, and it survived three research passes before an adversarial
review executed it. It fails for two independent mechanical reasons (§7). **The prior art
cited for it was misread**: `azuredol`'s `architecture.md` says container stores are wrapped
with `mk_relative_path_store`; its `base.py` does prefix arithmetic in the leaf and uses that
function **zero times**. Lesson recorded in ADR-0001: *a sibling package's design doc is a
claim; its source is the evidence.*

**(b) `EncodingType='url'` on list calls.** Proposed so keys with control characters survive
the XML listing. It **causes** the corruption it was meant to prevent: botocore already sets it
*and* URL-decodes the response, but the decode is gated on a flag it sets only when the caller
didn't pass the parameter. Measured: 5 of 7 test keys stop round-tripping.

**(c) `obstore` as the default engine.** Recommended by one research pass on import-time and
throughput grounds. Rejected: it has no bucket operations, its cited blocker for boto3
(presign `**params`) is used by nobody, and its numbers came from a throwaway venv and were
never reproduced. Revisit only if it grows bucket ops *and* the numbers are re-measured.

**(d) The class-attribute `S3KeyError.register()` registry** from discussion #6. Works
mechanically, but it is process-global mutable state, and — more importantly — it classifies on
exception *type*, which for S3 carries almost no information (`HeadObject` on a missing key
gives code `'404'`, not `NoSuchKey`). Replaced by an `(operation, code, status)` table on the
connection's preset.

**(e) A write-only multipart store** (Iterable + Settable + Deletable, not Gettable). Rejected:
after `complete_multipart_upload` the object is an ordinary S3 object, so there is no state in
which the key is writable but not readable — multipart is *transport*, not storage-model. It
would also cost the whole Mapping toolchain, and break `dst.update(src)` for the
filesystem↔S3 sync use case.

**(f) `WriteOnlyStore` Protocols shipped in v1.** Zero implementers, which violates ADR-0009's
own rule. Moved to the deferral list.

**(g) An absolute `import s3dol < 30 ms` budget.** Arithmetically impossible: `dol` alone is
~61 ms and is a hard dependency of Layer B. Replaced by a delta budget.

## 6. The two open design questions

These are the ones worth discussing before any code is written. Each has a GitHub discussion
with concrete options; this section is the short framing.

- **§7 — Layered transformation and unmapped keys.** Discussion: **#14**
- **§8 — Credential and endpoint resolution.** Discussion: **#15**

## 7. Open question 1 — layered transformation and the unmapped key

### The problem, precisely

`dol`'s model is "wrap a base store with layers of transformations". Key transforms are applied
by the wrapper's `__getitem__`/`__setitem__`/`__delitem__`/`__contains__`/`__iter__`. **Every
other method is invisible to the transformation layer**: `dol.base.Store.__getattr__` returns
the *bound leaf method*, so it receives the outer, unmapped key.

For a backend adapter this is severe, because the interesting capabilities are exactly the
non-dunder methods. Measured, store scoped to `logs/` in a bucket that also holds `logs2/leak`
and a root `a.txt`:

```
w['a.txt']              -> b'IN'                  correct
url_for('a.txt')        -> https://s3/a.txt       WRONG (want /logs/a.txt) — signed, silent
sub('x/')               -> store over bucket ROOT  scope escape
handle('a.txt')         -> handle on root a.txt    wrong object
info('a.txt')           -> KeyError                 for a key that IS present
prefixes()              -> ['logs/', 'logs2/']      leaks the sibling it exists to hide
delete_many(['a.txt'])  -> ROOT a.txt DESTROYED     silent destruction of the wrong object
len(w)                  -> 1                        a __len__ we deliberately omitted
```

Two independent failure modes:

1. **Delegation loses the mapping.** Above.
2. **Pushdown is impossible.** `Store.__iter__` calls `self.store.__iter__()`; there is no
   channel by which a key-wrapper hands its prefix to the leaf's `ListObjectsV2(Prefix=…)`.
   Every scoped listing becomes a full-bucket scan — measured at 22 LIST requests where a
   leaf-owned prefix costs 1.

And nothing raises. `isinstance(w, SupportsUrlFor)` stays `True`, because `@runtime_checkable`
Protocols check method *presence* only.

### What v1 does about it, and why that is not a solution

v1 sidesteps it: the prefix lives in the leaf, so there is no key-mapping seam between a
capability method and the wire. **That fixes the prefix case only.** A user who wraps an s3dol
store with any `dol` key codec still gets a silently wrong `url_for`. So the general problem
is unsolved and is worth solving properly — it affects every `*dol` adapter that has a keyed
capability method.

### Options (for discussion — see #14)

**A. Status quo + documentation.** Prefix in the leaf; document the trap; use
`inner_most_key(wrapped_self(self), k)` in any capability method that might be wrapped.
*Cheap; leaves the footgun loaded for users and for sibling packages.*

**B. Declarative key-method registration.** The leaf declares which methods take a key and
where; dol's wrapper machinery generates mapped delegates.
```python
class BucketReader(...):
    _key_methods = {
        "url_for": 0,
        "info": 0,
        "handle": 0,
        "sub": 0,
        "delete_many": ("iter", 0),
    }
```
*General; fixes the family at once. But a method the author forgets to declare fails the same
silent way — mitigable with a reflective test that enumerates public methods and fails on any
undeclared one.*

**C. Free functions instead of methods.** `s3dol.url_for(store, k)` rather than
`store.url_for(k)`. The function resolves the key through the whole wrapper chain once, then
calls the leaf. **This is already dol's own idiom** — `dol.content_url(store, ref_or_key)`,
`get_content`, `put_content`, `add_content` all take the store as first argument, and
`dol/content.py` explicitly frames it as solving the problem *beside* the interface rather than
inside it. *Composes at any wrapper depth, no delegation at all. Costs ergonomics
(`store.url_for(k)` reads better) and `dol.SupportsUrlFor` currently expects a method — though
both can coexist: method on the leaf, free function as the safe general form.*

**D. Bind delegated methods to the outer store.** Change `Store.__getattr__` to return a
delegate bound to the *wrapper*, so `self._id_of_key(k)` inside the leaf method resolves
through the full chain. *Addresses the root cause rather than each symptom; but it is a
behavioural change in dol's core with a wide blast radius (~42k downloads/month, many
dependents), and it only works if leaf methods consistently call `self._id_of_key`.* Closely
related to dol issue #18.

**E. Capabilities as parallel Mappings.** Turn each keyed capability into a Mapping view
sharing the key space — `store.urls[k]`, `store.info[k]`, `store.handles[k]` — so key
transformation happens through the Mapping protocol the wrapper **already** handles correctly.
Requires the wrapper to re-wrap returned Mapping-valued attributes with the same key codec
(declarative, à la B: `_key_mapped_attrs = ('urls', 'info', 'handles')`).
*Most dol-native of the options: it turns delegation into composition. Also collapses three
ADR-0009 deferrals (presigned-URL store, ObjectInfo-from-LIST, handles) into the same
mechanism. Costs: more objects, and it doesn't cover non-keyed methods like `delete_many` or
`prefixes`.*

My provisional lean: **C for the general safe form + E for the capabilities that are naturally
keyed**, with **B** as the dol-level mechanism that makes E's re-wrapping declarative. D is the
most principled but the riskiest to land in dol. Worth arguing.

The pushdown half (failure mode 2) is separate and probably needs its own answer — a hint
protocol (`__iter__(self, *, prefix_hint=…)`) or an explicit `iter_prefix` on the leaf that the
wrapper knows to route to.

## 8. Open question 2 — credential and endpoint resolution

### The problem

Findings 1, 2 and 5 of §4, plus two facts that make a naive fix dangerous:

- **`AWS_ENDPOINT_URL_S3` silently outranks everything**, is service-specific so it beats the
  generic `AWS_ENDPOINT_URL`, and is **not** in botocore's
  `BOTOCORE_DEFAUT_SESSION_VARIABLES` — so it is invisible to naive introspection. It is also
  why CI is currently green on a data-misrouting bug: the test environment re-supplies the
  endpoint the code throws away.
- **v0's *effective* precedence when env credentials exist is
  `AWS_ENDPOINT_URL_S3 > AWS_ENDPOINT_URL > explicit kwarg`** — and the obvious fix
  (explicit wins) **inverts the top of it**. Any deployment relying on that env var to override
  a stale hard-coded endpoint silently redirects on upgrade.

There is also a correctness trap in the other direction: boto3 clients hold a `Credentials`
*object* and call `get_frozen_credentials()` per request, so SSO/STS/IMDS refresh works — **if
you let the chain resolve**. v0 defeats this twice, by merging env keys into session kwargs and
by building a raw client from a credential *snapshot* that can never refresh (and which drops
`profile_name` entirely).

### Design constraints, ranked

1. `except`-free correctness: never silently ignore an argument the caller passed.
2. Never snapshot credentials — carry a profile/session/spec so refresh works.
3. Resolution must be **inspectable** (`diagnose()`) and **testable without I/O**.
4. Progressive disclosure: zero-arg works, a preset name works, an explicit connection works.
5. Picklable: the connection carries a spec, not a client (and `botocore.UNSIGNED` is
   unpicklable, so it must never enter the dataclass).
6. Migration-safe: upgrading must not silently move anyone's data target.

### Options (for discussion — see #15)

**Resolution shape.** The strong idea, and the one I'd build on: make resolution a **pure
function** of `(spec, environ, aws_config)` returning a `Resolution` record where **every field
carries its provenance**:

```python
resolve(spec, env) -> Resolution(
    endpoint_url=('https://…', source='preset:r2'),
    region_name=('auto',      source='preset:r2'),
    signature_version=('s3v4', source='default'),
    credentials=(<spec>,       source='profile:prod'),
)
```

No I/O, so tier-1 tests can exhaustively cover the ladder with a fake env dict — which is
exactly what makes a precedence rule trustworthy rather than aspirational. `diagnose()` prints
this table (never the secret). This part I think is uncontroversial; the questions are below.

**Q1 — what wins?** (a) Explicit kwargs always win, full stop. (b) Explicit wins but **warn
once** when an env var that would have won under v0 is present and differs — the
migration-safe variant. (c) A `strict=True` mode that *raises* on any ambiguity, off by
default. My lean: (b) as default + (c) available, with the warning removed in v2.

**Q2 — what shape is `credentials=`?** Options: raw kwargs (v0's mistake — defeats refresh); a
profile name; a `botocore.Session`; a callable credential provider returning
`{key, secret, token, expires_at}` (obstore's shape, and the one that composes with SSO); or a
polymorphic "thing or spec for it" à la `azuredol.AzureConnection.from_anything`. My lean:
polymorphic accept, **normalize to a session/provider internally, never to frozen keys**.

**Q3 — how much does the preset get to set?** A preset today can set endpoint, region,
addressing, signature version, checksum policy. Should it be allowed to carry credentials?
(Proposed: **no** — presets are public, shareable, committable config.) Should
`AWS_ENDPOINT_URL_S3` outrank a preset the user explicitly named? (Proposed: no — an explicitly
named preset is an explicit argument.)

**Q4 — where does `anon` live?** It must be surfaced on the top-level factory
(`s3_store(bucket, anon=True)`), not just on the connection, or the open-data notebook case
requires learning Layer A. And `'auto'` needs a precise meaning — proposed: "try unsigned if no
credentials resolve **at all**", explicitly *not* "retry unsigned after `AccessDenied`", because
an expired token would then silently downgrade to a different, public view of the data.

## 9. Re-verification snippets

The raw research (13 agents, ~1.4M tokens) was local-only and is **not** recoverable. Every
load-bearing claim is reproducible from these. Use an interpreter that resolves `dol` to the
local editable checkout — check `dol.__file__` before trusting a result, since a different
interpreter may pick up an older wheel.

```python
# dol prefix corruption (dol#82)
from dol import KeyCodecs, Pipe, filt_iter

base = {"a/b": 1, "a/c": 2, "z": 3, "ab/x": 4}
sorted(KeyCodecs.prefixed("a/")(dict(base)))  # ['', '/x', 'b', 'c']
sorted(
    Pipe(filt_iter.prefixes("a/"), KeyCodecs.prefixed("a/"))(dict(base))
)  # ['b', 'c']

# un-normalized prefix escapes the scope
store = {"logs/2026.txt": 1, "logs2/2026.txt": 2, "logsX": 3}
safe = Pipe(filt_iter.prefixes("logs"), KeyCodecs.prefixed("logs"))(store)
safe["2/2026.txt"]  # -> 2   OTHER TENANT, READ
safe["2/hacked.txt"] = 99  # -> writes 'logs2/hacked.txt'   OTHER TENANT, WRITE
```

```python
# SigV2 presigning (#10)
import boto3
from botocore.config import Config


def presign(cfg=None, **kw):
    c = boto3.client(
        "s3",
        region_name=kw.pop("region", "us-east-1"),
        aws_access_key_id="AK",
        aws_secret_access_key="SK",
        config=cfg,
        **kw,
    )
    u = c.generate_presigned_url(
        "get_object", Params={"Bucket": "mybucket", "Key": "a/b"}, ExpiresIn=3600
    )
    return (
        "SigV4" if "X-Amz-Algorithm" in u else "SigV2"
    ), c.meta.config.signature_version


presign()  # ('SigV2', 's3v4')   <- and it lies
presign(Config(signature_version="s3v4"))  # ('SigV4', 's3v4')
presign(endpoint_url="http://localhost:9000")  # ('SigV2', 's3v4')   <- every MinIO user
```

```python
# EncodingType corruption
import boto3
from moto import mock_aws

KEYS = ["plain", "a b", "café", "a+b", "a\rb", "p/x y", "a%20b"]


@mock_aws
def run(explicit):
    c = boto3.client("s3", region_name="us-east-1")
    c.create_bucket(Bucket="test-bucket")
    for k in KEYS:
        c.put_object(Bucket="test-bucket", Key=k, Body=b"v")
    kw = {"EncodingType": "url"} if explicit else {}
    return sorted(
        o["Key"]
        for o in c.list_objects_v2(Bucket="test-bucket", **kw).get("Contents", [])
    )


run(False)  # all 7 round-trip
run(True)  # 2 of 7 — ['a%0Db','a%20b','a%2520b','a%2Bb','caf%C3%A9','p/x%20y','plain']
```

```python
# HEAD has no body, so the modelled exception never fires
#   GetObject  missing key -> Code='NoSuchKey'  caught by client.exceptions.NoSuchKey: True
#   HeadObject missing key -> Code='404'        caught: False
#   HeadBucket missing     -> Code='404'        caught: False
# and moto returns an error BODY on HEAD where AWS returns none:
#   moto head_object missing bucket -> Code='NoSuchBucket'; real AWS -> Code='404'
```

`azuredol` is the reference implementation for the layering — read its `base.py` (not its
`architecture.md`): normalized prefix in the leaf, `_id_of_key`/`_key_of_id` in the leaf,
pushdown via `list_blobs(name_starts_with=…)`, prefix in `__repr__`, sub-stores via
`_with(prefix=…)`.

## 10. Dependents — the real compatibility constraint

| Dependent | Usage | Ours to merge? |
|---|---|---|
| `lacing` | `from s3dol.store import S3Store`; `S3Store(bucket_name, path=prefix, **kw)` — bucket **positional**; passes `make_bucket=True` in tests | yes |
| `http_cosmo_prep` | `from s3dol.store import S3Store`; bucket **by keyword**, explicit `endpoint_url`; its passing tests write `str` values, use `store['folder/']` as a sub-store, and rely on `del` being a silent no-op | **no** — cosmograph-org, needs peer review |
| `reelee` | declares `s3dol` in `pyproject.toml` | yes |
| `py2store` | imports two functions from `s3dol.tests.util` under `suppress(ImportError)` — breakage is **silent** | yes |

Merging auto-publishes to PyPI (`[tool.wads.ci.publish] enabled = true`) and version numbers
burn permanently, so release ordering (ADR-0007 §5) is load-bearing, not ceremony.
`lacing`'s `tests/test_artifact_store_s3.py` is currently **excluded** in its `pyproject.toml`
with the comment *"fail in CI's clean env (s3dol url_for → None)"* — it must be re-enabled
before it can serve as the cross-repo gate ADR-0008 proposes.

## 11. Suggested next steps

1. **Resolve §7 and §8** (discussions #14, #15) — both shape the module boundaries, so they
   are worth settling before code.
2. **#10 (SigV2)** as a standalone 0.1.x patch. Independent, strictly a fix, immediate value.
3. **`s3dol.diagnose()`** in the same 0.1.x line, so dependents can record what their
   environment resolves to *before* the resolution order changes.
4. **dol#82 / dol#83** — both block the implementation and benefit every `*dol` adapter.
5. **P0 tier-1 test scaffolding**, then module-by-module per #11.
