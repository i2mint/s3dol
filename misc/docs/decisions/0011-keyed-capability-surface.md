# ADR-0011: The keyed capability surface and the unmapped-key problem

- **Status:** Accepted
- **Date:** 2026-08-10
- **Discussion:** [#14](https://github.com/i2mint/s3dol/discussions/14)
- **Amends:** [ADR-0001](0001-layered-architecture.md) (Layer B method table),
  [ADR-0005](0005-large-object-io.md) §2 (the surface-growth rule),
  [ADR-0006](0006-key-scoping-and-dol-fixes.md) §2 (the escape form),
  [ADR-0007](0007-naming-and-compatibility.md) (sub-store migration row),
  [ADR-0009](0009-scope-and-deferrals.md) (v1.0 scope: `info`/`prefixes` shapes),
  [ADR-0010](0010-bucket-and-bulk-operations.md) §2–§3 (`delete_many`, cascading delete)
- **Upstream:** [dol#83](https://github.com/i2mint/dol/issues/83) (this problem),
  [dol#18](https://github.com/i2mint/dol/issues/18) (its root)

## Context

[ADR-0001](0001-layered-architecture.md) puts the prefix in the leaf, which removes the
key-mapping seam between a capability method and the wire **for the prefix s3dol owns**. It does
not remove the seam in general: a user who wraps an s3dol store with a `dol` **key** codec still
gets a silently wrong `url_for`. Discussion #14 asked how to fix that properly.

### The mechanism, precisely

`dol` wraps by **delegation (has-a)**. Key transforms are applied by the wrapper's
`__getitem__` / `__setitem__` / `__delitem__` / `__contains__` / `__iter__`. Every other method
is handed the **outer, unmapped** key. There are **two** delegation routes, not one:

| route | when | site |
|---|---|---|
| `Store.__getattr__` | instance-wraps, and `mk_relative_path_store` subclasses | `dol/base.py:742` |
| `DelegatedAttribute.__get__` | class-wraps (`delegate_to` installs one descriptor per attr of `dir(wrapped)`) | `dol/base.py:279`, installed at `dol/base.py:416-480` |

Both return the method **bound to the leaf**. #14's framing named only the first; a fix covering
one and not the other is a silent no-op on half the cases.

Capability detection cannot see it: a `@runtime_checkable` Protocol checks method *presence*
only. It is also wrap-dependent — since 3.12 `isinstance` uses `getattr_static`, so
`isinstance(w, SupportsUrlFor)` is `True` for a **class**-wrapped capability and `False` for an
**instance**-wrapped one. Either way it says nothing about whether the key is right.

### What the family census showed

13 sibling packages were surveyed and every claim re-verified by source read plus a runnable
repro. **Stated accurately, because an earlier draft of this ADR overstated it:** the defect is
overwhelmingly **latent** — it bites only when a user applies a key codec, and most of these
packages never wrap their own stores. Verified breakdown: `focal` confirmed-live throughout;
`azuredol`, `aiofiledol`, `chromadol`, `sqldol`, `mongodol`, `redisdol` mixed; `cosmodol`,
`pydrivedol`, `sshdol`, `dynamodol`, `hfdol` latent-only; `couchdol` essentially refuted. **12
survey claims were refuted outright.**

Latent is not harmless — the latent cases include `cosmodol.CosmosItems.replace`, which under a
key codec silently overwrites a *different, real* document in full, and
`pydrivedol.GDReader.get_url`, which returns a URL for the wrong file *and grants
anyone/reader permission on it*. But "already destroying data in production" is not what the
evidence supports, and this ADR does not claim it. Per-package detail and repros go to the
responsible repos, indexed from [dol#83](https://github.com/i2mint/dol/issues/83).

One case *is* confirmed-live in `dol` itself: `dol.filesys.Files(d).is_valid_key(k)` returns
`False` for a key that exists.

## Decision

### D1 — Layer B carries no key-taking public methods (one documented exception)

[ADR-0001](0001-layered-architecture.md)'s Layer B table proposed **six** keyed methods
(`url_for`, `info`, `handle`, `sub`, `prefixes`, `delete_many`). `azuredol` — the reference
implementation for our layering — has **~zero**: `ContainerStore` is deliberately method-free,
and the rich per-object surface lives on `BlobHandle` (`azuredol/base.py:233`), which takes its
blob **at construction**, so wrapping the store with a key codec cannot corrupt it. Its only
residual exposures are `ContainerCollection.walk` (`base.py:164`) and `AccountStore.delete`
(`base.py:460`).

**`azuredol` is not safe because the prefix lives in the leaf. It is safe because it has almost
no seam to get wrong.** Prefix-in-leaf is necessary, not sufficient. That is the finding, and
ADR-0001 previously drew the wrong lesson from the same source.

The single exception is **`url_for(k)`, kept solely to satisfy `dol.SupportsUrlFor`** — see D3b.
It is the only entry on D5's allowlist and it is guarded so it is correct-or-loud, never
silently wrong.

### D1a — Why a hardened *method* is not the answer

The first draft kept `handle(k)`/`sub(prefix)` as methods hardened with
`inner_most_key(wrapped_self(self), k)` — the form ADR-0006 §2 prescribed. Testing that plan
refuted it.

`wrapped_self` resolves the outer store through a weakref registry keyed by `id(inner)`. A
`DelegatedAttribute` returns a method bound to the **leaf**, holding no reference to the
wrapper. So when nothing else holds the wrapper, it is freed before the method body runs and the
registry entry is removed by the cleanup callback:

```python
s = KeyCodecs.prefixed("x/")(BucketReader(data, prefix="logs/"))
s.m_abs("b.txt")  # 'logs/x/b.txt'  correct
KeyCodecs.prefixed("x/")(BucketReader(data, "logs/")).m_abs("b.txt")
# 'logs/b.txt'    WRONG, silently
```

The wrong answer is a **plausible `str`** — precisely because ADR-0001 puts the prefix in the
leaf, the leaf's own `_id_of_key` still fires — so a type check cannot catch it.

**Four corrections to that draft, all from an adversarial review, all re-verified:**

1. **The predicate is "no live strong reference", not "temporary".** A temporary caught in a
   reference cycle silently starts working, and `operator.methodcaller('m', k)(obj)` is correct
   where `obj.m(k)` is not. The failure is **intermittent**.
2. **The trigger is narrower than claimed.** `s3_store('bucket', prefix='p').handle(k)` is
   **not** an instance of the bug: `s3_store` returns a bare leaf or a **value**-codec wrap, and
   both are correct. Nor is `filt_iter` alone. Only a user-applied **key** codec breaks it.
3. **It is detectable.** `Store.__init__` probes the leaf with `hasattr(self.store, "KeysView")`,
   so a leaf can record that it was ever wrapped and refuse when `wrapped_self(self) is self`
   but that flag is set. Verified against instance-wrap, class-wrap and `Pipe`. So the honest
   claim is *"a method can be made loud rather than silently wrong, at the cost of a hack"* —
   not *"a method is impossible"*.
4. **Reproduced on CPython 3.10–3.14**; not gc-, version- or bytecode-specific.

So the method form is rejected on **cost/benefit**, not impossibility: it needs a guard riding
on an incidental `hasattr` probe, which misses the unpickle path and false-positives on a
once-wrapped leaf used bare. D2 gets the same correctness with none of that.

This still demotes `wrapped_self` generally: it is a **best-effort guardrail with a silent
failure mode**, not "the correct escape". The hole goes upstream (D9).

### D2 — Keyed capabilities are sibling *stores*, keyed through `__getitem__`

A capability becomes a Layer B `KvReader` over the same key space whose `__getitem__` returns
the capability:

```
BucketCollection → BucketReader → BucketStore     k -> bytes
BucketHandles                                     k -> ObjectHandle
BucketUrls                                        k -> presigned URL (str)
BucketInfo                                        k -> ObjectInfo
```

All four share one private base holding `prefix`, connection and `_id_of_key`/`_key_of_id`, so
the key arithmetic exists once.

**`__getitem__` is the one thing `dol` maps correctly at every depth**, so this is correct *by
construction*: no `inner_most_key`, no `wrapped_self`, no private `dol.dig` import, no upstream
PR, no guard. Verified correct with no live reference, under `Pipe`, under `cached_keys`, and
under a hand-rolled non-`Store` passthrough layer — the last being a case where the
free-function form is **silently wrong** (D3a).

It also restores `[k]` ergonomics, and subsumes three capability features
[ADR-0009](0009-scope-and-deferrals.md) currently defers (presigned-URL store,
ObjectInfo-from-LIST, handles) into one mechanism rather than three.

**The cost, stated plainly:** a user who wraps the data store must wrap the sibling in parallel.

```python
s = s3_store("bucket", prefix="p/")
h = s3_handles("bucket", prefix="p/")
h["f"].url()  # correct

c = KeyCodecs.prefixed("x/")  # user-applied key codec
s2, h2 = c(s), c(h)  # wrap BOTH
h2["f"]  # -> p/x/f   correct at any depth, any lifetime
```

`s3dol.handles(store)` / `.urls(store)` / `.info(store)` derive a sibling from an **unwrapped**
store and **raise** on a wrapped one, naming the parallel-wrap remedy. That refusal is reliable:
these are free functions holding the store, so `isinstance(store, dol.base.Store)` is a real
test — no weakref, none of D1a's fragility. Re-deriving the user's codec chain onto a sibling is
`dol`'s recursive-wrap problem (dol#10) and is explicitly **not** attempted here.

### D3 — Free functions for everything not keyed through a store

Bulk and endpoint-level operations have no Mapping to ride on, so they are free functions:

```python
s3dol.delete_many(store, keys)  # D4; see ADR-0010 §2
s3dol.prefixes(store)  # relative to the caller's key space
s3dol.sub(store, prefix)  # a store in the CALLER's key space
s3dol.delete_bucket(endpoint, name, force=True)  # D4; see ADR-0010 §3
```

This matches `dol`'s own idiom (`content_url`, `get_content`, `put_content`, `add_content` all
take the store first).

**`sub` has an obligation:** resolving the absolute prefix is not sufficient, because a leaf
sub-store would speak the leaf's key space and silently drop the user's outer codec. On an
unwrapped store use `leaf._with(prefix=…)` (cheap, pushes down); on a wrapped store compose over
the **outer** store with `Pipe(filt_iter.prefixes(p), KeyCodecs.prefixed(p))`
([ADR-0006](0006-key-scoping-and-dol-fixes.md) §1) — correct, but it loses pushdown (D8) and
returns a different type than the unwrapped branch. Document both.

**`prefixes` has the mirror obligation:** its results must be mapped **outward** through the
chain (`_key_of_id` per layer, outermost last) — the inverse of `inner_most_key`, which
`dol.dig` does not provide. Implement locally with a round-trip property test.

#### D3a — Free functions are *more* reliable than methods, not reliable

`inner_most_key` walks `.store` applying each layer's `_id_of_key`, which breaks when a layer is
**not** a `dol` `Store`. Verified against a ground-truth oracle:

| chain | wire key | free function | method form |
|---|---|---|---|
| `KeyCodecs` / `Pipe` / `cached_keys` / `filt_iter` / `wrap_kvs` | — | correct | correct (if referenced) |
| hand-rolled `__getattr__`-passthrough over the leaf | `logs/b.txt` | **`logs/logs/b.txt`** | correct |
| same passthrough over a key codec | `logs/x/b.txt` | **`logs/x/x/b.txt`** | correct |
| key codec over a plain `.store`-holding middle layer | `logs/x/b.txt` | **`x/b.txt`** | also wrong |

A passthrough `__getattr__` resolves `_id_of_key` to the *leaf's* bound method, so the walk
applies it at the delegator and again at the leaf; a middle layer without `_id_of_key` truncates
the walk instead. Every `dol`-shipped wrapper is safe (every `Store` inherits an identity
`_id_of_key`) — but `dol/base.py` ships a documented hand-rolled `Delegator` recipe of exactly
the breaking shape.

**No docstring may claim "correct at any wrapper depth" for a free function.** D2's stores may:
they never call `inner_most_key`.

#### D3b — `url_for` survives as a method, for the protocol only

`dol.SupportsUrlFor` requires a `url_for` **method**, and `dol.content_url` reaches it with
`getattr(store, 'url_for', None)`. With no method anywhere, `content_url` would return `None`
for every s3dol store forever — and `dol/content.py`'s module docstring names an `s3dol` store
as its intended S3 backend.

So `BucketReader.url_for(k)` ships, with the D1a §3 guard: correct when unwrapped or when the
wrapper is referenced, and **raising** (naming `s3dol.urls(store)[k]`) when the wrapper is
unreachable. Verified correct-or-loud in all three cases, and `dol.content_url` returns the
right URL for both an unwrapped and a referenced-wrapped store.

`s3dol.urls(store)[k]` (D2) remains the canonical form. `url_for` is a compatibility shim with a
documented limitation, and the real fix is upstream: `content_url` must resolve through the
chain **and** call the innermost `url_for` with the resolved key — resolving without that second
half would double-transform.

#### D3b-amended (2026-08-11, during implementation) — the guarded method cannot serve both call paths

The guard above assumed one call path. There are **two**, and they hand the *same signature*
**different key domains**:

| caller | what `url_for` receives | correct action |
|---|---|---|
| `dol.content_url(wrapped, k)` | the key already mapped through the **outer** layers — because the walk stops at the layer owning `url_for` "*because that layer applies its own*" (`dol/content.py::_url_for_provider_and_key`) | apply `self._id_of_key` |
| `wrapped.url_for(k)` (delegation) | the raw **outer** key | resolve through the whole chain |

Applying `_id_of_key` is right for the first and wrong for the second; re-resolving through the
chain is right for the second and **double-transforms** the first (measured: a store scoped to
`p/` under a `x/` key codec produced `p/x/x/g`). Nothing at the callee distinguishes them —
this is dol#83's defect reaching the one method the ADR kept.

**Decision:** `url_for` applies `self._id_of_key(k)` (dol's stated contract), and **refuses**
— `NotSupported`, naming `s3dol.urls(store)[k]` — as soon as the leaf has ever been wrapped.
Consequences, stated plainly:

- `dol.content_url` is correct for an **unwrapped** s3dol store (the common case, and the one
  `dol/content.py`'s docstring shows) and **raises** for a wrapped one. Loud, never wrong.
- The refusal is driven by a *sticky flag* set when dol probes the leaf (`hasattr(store,
  'KeysView')` in `Store.__init__`), implemented as a descriptor because a `KvReader` leaf
  would otherwise satisfy the probe from `MappingViewMixin` and never see it. Unlike the
  `wrapped_self` weakref, the flag survives the wrapper's death — so the refusal is reliable,
  not intermittent. The documented false positive stands: a once-wrapped leaf later used bare
  also refuses.

**This is the P1 evidence for dol#86/#83**: a keyed capability *method* is unfixable in the
general case — not merely fragile, and not fixable by a better resolution primitive, because
the ambiguity is in the *call*, not the resolution. Sibling stores (D2) route through
`__getitem__`, which has one key domain and one caller, and so remain correct by construction.

### D4 — Destructive operations are free functions

`delete_many(keys)` and `EndpointStore.delete(name, force=True)` both leave Layer B.

Destructive **+** key-taking **+** delegated is the shape the census found in `cosmodol`
(`CosmosItems.replace`/`batch`, `CosmosDatabase.delete`, `CosmosAccount.delete`), `azuredol`
(`AccountStore.delete`) and `pydrivedol` (`GDStore.upload`). Latent or not, s3dol does not add
another. [ADR-0010](0010-bucket-and-bulk-operations.md)'s semantics are unchanged — 1000-key
chunking, the HTTP-200 `Errors` parse, `S3PartialFailure`, the refusal to cascade implicitly —
only the surface moves.

`del endpoint[name]` stays, and still refuses a non-empty bucket: it is a Mapping dunder, so
`dol` maps it correctly.

One honest caveat: a module-level `delete_many(store, keys)` accepts any object as its first
argument, where a method could not be called on a store lacking it. Free-function form fixes the
*unmapped key* problem, not the *wrong target* problem. Validate the first argument.

### D5 — A conformance test, not a registry

> **Amended by [ADR-0012](0012-credential-and-endpoint-resolution.md) §D5:** the allowlisted
> `url_for` returns `str | None`, where `None` means "no presign capability" (an anonymous
> connection). Its *other* refusal — the unreachable-wrapper guard in D3b — still **raises**.
> Two distinct conditions; do not merge them.

No `_key_methods` registry. A **reflective conformance test** enumerates the public methods of
every Layer B class and fails on any that takes a key — allowlist: `url_for` (D3b), and nothing
else.

The predicate cannot be "first argument is named like a key": that misses `delete(name, ...)`,
`delete_many(keys)`, `cosmodol`'s `batch(operations)` and `sshdol`'s `sync_to(target)` — several
of the shapes this ADR most cares about. It is "does this method accept anything that reaches
the wire as a key, at any argument position or nested in a structure", which is a judgement.
The test therefore asserts against an **explicit inventory of every public method** and fails on
any *new* one, forcing the judgement at review time rather than pretending a signature check
suffices.

#14 observed that a declarative registry fails the same silent way when an author forgets to
declare a method, and that a reflective test is "the kind of guard that actually holds". Note
also that `dol` already ships a hook of Option B's shape —
`wrap_kvs(ingoing_key_methods=…, outcoming_key_methods=…)` — which is untested and verified
broken for leaf-defined methods on both wrap paths (it fails loudly, at least). Option B would
mean replacing it, not building on it.

### D6 — Option E: rejected as an *attribute*, adopted as a *store*

#14's Option E argued that capabilities-as-Mappings work because "key transformation happens
through the Mapping protocol the wrapper already handles correctly."

- **As a Mapping-valued *attribute* (`store.urls`) that is false.** Verified: a wrapper does not
  re-wrap such an attribute; `store.urls` returns an inner-keyed mapping under both class- and
  instance-wrap. That form needs the same key resolution as everything else.
- **As a sibling *store* it is exactly true**, and that is D2.

The first draft of this ADR conflated the two and deferred Option E wholesale. That was wrong:
the provisional lean in #14 (C + E, with B) was right in substance — the correction is that E's
value comes from being a *store*, not an attribute, and that C's role shrinks to the
non-keyed operations.

Making `store.urls` work as an attribute would need `dol` to propagate and re-wrap
Mapping-valued attributes — dol#10, and the `.meta` sidecar design in
`dol/misc/docs/dol_content_metadata_bifurcation.md` §2.2, whose stated blocker is precisely
key-transform propagation. Deferred to v1.x; D2 does not depend on it.

### D7 — Option D (rebind delegated methods to the wrapper) is rejected

Not primarily for blast radius. `dol/misc/docs/dol_issue18_design.md` surveyed 26 ecosystem
sites and rejected the rebind family on three verified defects, the fatal one being that
rebinding binds `self` to the **innermost** `Wrap` — so under a `Pipe` stack it *does not fix the
case it exists to fix*, and stacked-codec writes gain a partial-transform corruption surface.
Plus statically-undetectable crashes: a leaf method calling `super().__getitem__(k)` compiles
`super(SomeClass, self)` with `self` now a `Wrap` → `TypeError`.

That document's Phase-1 instructions say: *"**Do not touch** `DelegatedAttribute.__get__`, the
`delegate_to` copy loop, or the `base.py:451` signature graft."* We comply.

**The terminal fix is `dol`'s is-a wrapping** (dol#18 Approach C), which would resolve dol#18 and
dol#6 together and make D1a moot. Note it is the doc's *recommended* terminal direction but
still an open question for the maintainer (§9 of that doc), and Phases 2–3 have not shipped. So
the selection criterion here was *correct on dol 0.3.x **and** harmlessly redundant under is-a* —
which D2 satisfies: sibling stores stay correct either way.

### D8 — Prefix pushdown is closed, not deferred

ADR-0001 already solves pushdown for the prefix s3dol owns: the leaf passes
`ListObjectsV2(Prefix=self.prefix)`. The residual case is a user stacking `filt_iter.prefixes(…)`
on top, which is **general predicate pushdown** — `dol` has no framework for it, and an
`__iter__(*, prefix_hint=…)` protocol would have exactly one implementer, violating
[ADR-0009](0009-scope-and-deferrals.md)'s own "no new `Protocol` without two implementers" rule.

A user-stacked filter **accepts a full scan**; `s3dol.sub(store, prefix)` is the documented cheap
path and pushes down on an unwrapped store. This is an answer, not a deferral; do not reopen it
without a second implementer.

### D9 — Upstream findings

Blocking for s3dol ([ADR-0006](0006-key-scoping-and-dol-fixes.md) §3):

1. **Export `dol.dig.inner_most_key`** and harden `store_trans_path` (raise instead of returning
   `None`; fix `dol/dig.py:41` hardcoding `unravel_key`). Needed by D3's free functions.
2. **Fix `dol.content_url`** (`dol/content.py:210-214`) — see D3b for the two-part fix.

Non-blocking, reported not fixed by us:

3. **`wrapped_self` has a lost-reference hole** (D1a). dol ships it as the *blessed* pattern and
   `xdol`, `unbox` and `lexis` have adopted it, so they inherit a silent failure mode. dol's own
   test suite does not cover it — all of `dol/tests/base_test.py` binds the wrapper to a name.
4. **`dol.filesys.is_valid_key`/`validate_key`** (`dol/filesys.py:422,425`) — confirmed-live, and
   the best regression sentinel for any future delegation fix. `dol/paths.py:1199-1206` already
   carries the hand-rolled fix for the same shape.
5. **`dol#83`'s own "Ask"** requests the two things this ADR rejects (bless `wrapped_self`; ship
   a declarative key-method helper). It needs correcting with D1a and D7.

## Consequences

**Buys.** The keyed surface is correct *by construction* rather than by a resolution primitive:
D2's stores never call `inner_most_key`, so they are immune to both D1a's lost-reference hole and
D3a's non-`Store`-layer hole — the only form in this ADR that is. `[k]` ergonomics survive. Three
ADR-0009 deferrals collapse into one mechanism. No destructive delegated method. Nothing to
unwind when `dol` lands is-a wrapping.

**Costs.**

- **Parallel wrapping.** Wrapping the data store does not wrap its siblings, and s3dol will not
  guess the chain. `s3dol.handles(store)` raises rather than silently returning an unwrapped
  sibling, so the cost is visible — but it is a real ergonomic tax on the one case (user-applied
  key codec) this whole ADR is about.
- **More classes.** Four Layer B readers where there was one, plus factories.
- `sub` and `prefixes` become non-trivial (D3's two obligations) where as leaf methods they were
  three lines, and `sub` returns different types on the wrapped and unwrapped branches.
- `url_for` survives as a guarded shim, which is one exception to an otherwise clean rule, and
  the guard rides on an incidental `dol` implementation detail.
- s3dol depends on `dol.dig.inner_most_key`, not currently public API.

**What NOT to do.**

1. **Do not add a key-taking method to a Layer B class** — not even hardened with `wrapped_self`
   (D1a). Add a sibling capability store (D2), put it on `ObjectHandle` (key bound at
   construction), or ship a free function (D3). The conformance test allows `url_for` and
   nothing else.
2. Do not compose `inner_most_key(store, k)` with `_id_of_key` — it already includes the leaf's
   prefix, and composing double-prefixes silently.
3. Do not claim a free function is correct at any wrapper depth (D3a).
4. Do not treat `wrapped_self` as a correctness mechanism (D1a, D9).
5. Do not rely on `isinstance(store, SupportsUrlFor)` to detect a capability.
6. Do not re-propose rebinding delegated methods (D7).
7. Do not invent a pushdown hint protocol for one implementer (D8).
8. Do not attempt to re-derive a user's codec chain onto a sibling store — that is dol#10.
