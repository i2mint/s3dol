# ADR-0011: The keyed capability surface and the unmapped-key problem

- **Status:** **Proposed** — D1/D2/D3 are under revision; see §"Open fork" below. D4–D9 are settled.
- **Date:** 2026-08-10
- **Discussion:** [#14](https://github.com/i2mint/s3dol/discussions/14)
- **Amends:** [ADR-0001](0001-layered-architecture.md) (Layer B method table),
  [ADR-0006](0006-key-scoping-and-dol-fixes.md) §2 (the escape form),
  [ADR-0010](0010-bucket-and-bulk-operations.md) §2 (`delete_many`'s surface)
- **Upstream:** [dol#83](https://github.com/i2mint/dol/issues/83) (this problem),
  [dol#18](https://github.com/i2mint/dol/issues/18) (its root)

## Context

[ADR-0001](0001-layered-architecture.md) puts the prefix in the leaf, which removes the
key-mapping seam between a capability method and the wire **for the prefix s3dol owns**. It does
not remove the seam in general: a user who wraps an s3dol store with any `dol` key codec still
gets a silently wrong `url_for`. Discussion #14 asked how to fix that properly.

### The mechanism, precisely

`dol` wraps by **delegation (has-a)**. Key transforms are applied by the wrapper's
`__getitem__` / `__setitem__` / `__delitem__` / `__contains__` / `__iter__`. Every other method
is handed the **outer, unmapped** key. There are **two** delegation routes, not one:

| route | when | site |
|---|---|---|
| `Store.__getattr__` | instance-wraps, and `mk_relative_path_store` subclasses | `dol/base.py:742` |
| `DelegatedAttribute.__get__` | class-wraps (`delegate_to` installs one descriptor per attr of `dir(wrapped)`) | `dol/base.py:279`, installed at `dol/base.py:416-480` |

Both return the method **bound to the leaf**. #14's original framing named only the first; any
fix that covers one and not the other is a silent no-op on half the cases.

Nothing raises, and capability detection cannot see it: a `@runtime_checkable` Protocol checks
method *presence* only. It is also wrap-dependent — since 3.12 `isinstance` uses
`getattr_static`, so `isinstance(w, SupportsUrlFor)` is `True` for a **class**-wrapped
capability but `False` for an **instance**-wrapped one. Either way it tells you nothing about
whether the key is right.

### What the family census showed

Surveyed across ~20 `*dol` packages, this is not a latent tidiness issue. Already shipping:
destructive key-taking delegated methods in `cosmodol` (`CosmosItems.replace`/`batch`,
`CosmosDatabase.delete`, `CosmosAccount.delete`), `azuredol` (`AccountStore.delete`),
`pydrivedol` (`GDStore.upload`, and `GDReader.get_url` which *also* grants anyone/reader
permission on the wrong file), `aiofiledol`, and `sshdol.sync_to` (rsync `--delete` over the
leaf's whole rootdir). And a **confirmed-live** bug in `dol` itself:
`dol.filesys.Files(d).is_valid_key(k)` returns `False` for a key that exists.

The full census, with per-symbol verdicts, lives on
[dol#83](https://github.com/i2mint/dol/issues/83).

## Decision

### D1 — Shrink the keyed surface. This is the primary decision.

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

So:

- `url_for` and `info` **move onto `ObjectHandle`** — `ObjectHandle` binds its key at
  construction, azuredol-style, so no key ever crosses a delegation seam to reach them.
- `delete_many` leaves the store entirely — see D4.
- **`handle` and `sub` become free functions too**, and Layer B ends up with **zero** key-taking
  public methods. That is stronger than this ADR originally proposed, and it is forced by the
  finding below rather than chosen for symmetry.

### D1a — Why zero and not two: a delegated method cannot be made reliably key-correct

The first draft of this ADR kept `handle(k)` and `sub(prefix)` as methods, hardened by D2's
`inner_most_key(wrapped_self(self), k)`. **Testing that plan refuted it.**

`wrapped_self` resolves the outer store through a weakref registry keyed by `id(inner)`
(`dol/base.py`, `_wrapper_backrefs`). A `DelegatedAttribute` returns a method bound to the
**leaf**, holding no reference to the wrapper. So in a chained expression the wrapper is a
temporary that is freed by the time the method body runs, its weakref dies, and the registry
entry is **removed by the cleanup callback** — leaving a state indistinguishable from "never
wrapped":

```python
s = KeyCodecs.prefixed('x/')(BucketReader(data, prefix='logs/'))
s.m_abs('b.txt')                                          # 'logs/x/b.txt'  correct
KeyCodecs.prefixed('x/')(BucketReader(data, 'logs/')).m_abs('b.txt')
                                                          # 'logs/b.txt'    WRONG, silently
```

**The wrong answer is a plausible `str`**, so a type check does not catch it. Precisely
*because* ADR-0001 puts the prefix in the leaf, the leaf's own `_id_of_key` still fires and
produces a well-formed key that addresses the wrong object. A leaf with no `_id_of_key` would at
least return `None`.

Measured, method form vs free function, across wrap shape × wrapper lifetime:

| form | unwrapped (named/temp) | key-codec + named | key-codec + **temp** | `Pipe` + named | `Pipe` + **temp** |
|---|---|---|---|---|---|
| method via `wrapped_self` | correct | correct | **silently wrong** | correct | **silently wrong** |
| free function | correct | correct | correct | correct | correct |

**Scope of the failure — stated precisely, because an earlier draft overstated it.** The
precondition is *a user-applied **key** codec* **and** *no live strong reference to the wrapper
at call time*. It is **not** triggered by `s3_store('bucket', prefix='p').handle(k)`: `s3_store`
returns a bare leaf or a **value**-codec wrap, and both are correct as temporaries (verified).
Nor by `filt_iter` alone. The failing shape is
`KeyCodecs.prefixed('x/')(s3_store(...)).handle(k)`.

"Temporary" is also the wrong word: the predicate is *no live strong reference*. A temporary
caught in a reference cycle silently starts working, and `operator.methodcaller('m', k)(obj)` is
correct where `obj.m(k)` is not — so the failure is **intermittent**, which is worse than
deterministic even though it is rarer.

**It is detectable, contrary to an earlier draft of this section.** `Store.__init__` probes the
leaf with `hasattr(self.store, "KeysView")`, so a leaf can record that it was ever wrapped and
then refuse when `wrapped_self(self) is self` but that flag is set. Verified: it catches the
instance-wrap, class-wrap and `Pipe` temporary cases and stays silent on a genuinely unwrapped
store. Caveats: it rides on an incidental probe, it does not fire on the unpickle path (which
re-registers via `__setstate__`, bypassing `__init__`), and it false-positives on a leaf that
was wrapped once and is later used bare. A two-line upstream change to
`_register_wrapper_backref` would do it properly.

So the honest conclusion is narrower than "the method form is impossible": **the method form can
be made loud rather than silently wrong, at the cost of a hack.** Whether that is worth keeping
is the open fork below.

This does still demote `wrapped_self` generally: it is a **best-effort guardrail with a silent
failure mode**, not "the correct escape" that
[ADR-0006](0006-key-scoping-and-dol-fixes.md) §2 called it. The hole goes upstream (D9).

### D2 — One key-resolution primitive, and it takes the store as an argument

```python
from dol.dig import inner_most_key  # NOT exported from `dol` — see ADR-0006 §3


def _abs_key(store, k: str) -> str:
    """The absolute S3 key for `k` in `store`. Correct at any wrapper depth."""
    _id = inner_most_key(store, k)
    if not isinstance(_id, str):
        raise KeyNotValid(...)  # never let a None reach the wire
    return _id
```

**`store` is a parameter, not `self`.** The caller's expression holds the store alive for the
duration of the call, `inner_most_key` walks the real `.store` chain, and the `wrapped_self`
weakref registry is never consulted — so D1a's hole cannot occur. Verified correct across
wrapped/unwrapped × named/temporary and a `Pipe` stack, where the method form fails two of
those.

**But it is not categorically safe, and this ADR must not claim it is.** `inner_most_key` walks
`.store` applying each layer's `_id_of_key`, which breaks when a layer in the chain is **not** a
`dol` `Store`. Verified against a ground-truth oracle:

| chain | wire key | free function | method form |
|---|---|---|---|
| `KeyCodecs` / `Pipe` / `cached_keys` / `filt_iter` / `wrap_kvs` over the leaf | — | correct | correct (if named) |
| hand-rolled `__getattr__`-passthrough delegator over the leaf | `logs/b.txt` | **`logs/logs/b.txt`** | correct |
| same delegator over a key codec | `logs/x/b.txt` | **`logs/x/x/b.txt`** | correct |
| key codec over a plain `.store`-holding middle layer | `logs/x/b.txt` | **`x/b.txt`** | also wrong |

Cause: a passthrough `__getattr__` resolves `_id_of_key` to the *leaf's* bound method, so the
walk applies it once at the delegator and again at the leaf; a middle layer with no `_id_of_key`
truncates the walk instead. Every `dol`-shipped wrapper is safe because every `Store` subclass
inherits an identity `_id_of_key` — but `dol/base.py` ships a documented hand-rolled `Delegator`
recipe of exactly the breaking shape.

So the free function is *more* reliable than the method form, not *reliable*. Any claim of
"correct at any wrapper depth" is false and must not appear in the docstring.

**It replaces `_id_of_key`. It never composes with it.** Because ADR-0001 puts the prefix in the
leaf, `inner_most_key` walks the whole chain *including the leaf's own `_id_of_key`*, so it
already returns the fully-prefixed absolute key. Composing the two double-prefixes, silently:

```python
# store scoped to 'logs/', outer key 'a.txt'
inner_most_key(store, k)                    # 'logs/a.txt'       correct
store._id_of_key(inner_most_key(store, k))  # 'logs/logs/a.txt'  WRONG
```

That instinct — reach for `_id_of_key` — is exactly what an author will have, so the rule is
restated in ADR-0006 §2.

The `str` check is **mandatory, not defensive**: `inner_most_key` returns `None` — silently, via
`last_element` over an empty generator — when no layer in the chain supplies `_id_of_key`.

**Two implementation obligations this creates**, because not every capability is a pure
key→address mapping:

- **`sub(store, prefix)`** must return a store *in the caller's key space*. Resolving the
  absolute prefix is not sufficient: a leaf sub-store built from it would speak the leaf's key
  space, silently dropping the user's outer codec. On an unwrapped s3dol store, use
  `leaf._with(prefix=…)` (cheap, pushes down). On a wrapped store, compose over the **outer**
  store with dol's own safe form, `Pipe(filt_iter.prefixes(p), KeyCodecs.prefixed(p))`
  ([ADR-0006](0006-key-scoping-and-dol-fixes.md) §1) — correct, but it loses pushdown, which is
  the accepted cost in D8.
- **`prefixes(store)`** returns keys relative to the store, so its results must be mapped
  **outward** through the chain (`_key_of_id` per layer, outermost last) — the inverse of
  `inner_most_key`, which `dol.dig` does not currently provide. Implement it locally and add a
  round-trip property test (`∀ p: abs → rel → abs` is identity).

### D3 — Free functions are the *only* reliable form, so they are the whole keyed API

```python
s3dol.handle(store, k)        # -> ObjectHandle  (then .url(), .info(), ranged reads, …)
s3dol.url_for(store, k)       # == s3dol.handle(store, k).url()
s3dol.info(store, k)          # == s3dol.handle(store, k).info()
s3dol.sub(store, prefix)      # -> a store in the CALLER's key space (see D2)
s3dol.prefixes(store)         # -> relative to the caller's key space (see D2)
s3dol.delete_many(store, keys)
```

The function resolves the key through the whole wrapper chain once, then acts. This is already
`dol`'s idiom (`dol.content_url`, `get_content`, `put_content`, `add_content` all take the store
first), it composes at any wrapper depth, it is unaffected by whether `dol` wraps by has-a or
is-a, and — per D1a — it is the only form that is correct when the store is a temporary.

D1a upgrades this from "the safe general form, offered alongside methods" to "the form". There
is no method variant to fall back to, because a method variant would be right most of the time
and silently wrong the rest, which is the worst available option.

**What remains ergonomic without a keyed method:** `__getitem__` *is* correctly key-mapped by
`dol` at every wrapper depth, so `store[k]`, `store[k] = v`, `del store[k]`, `k in store` and
iteration stay the primary surface and stay correct — which is the Mapping-first promise in
[architecture.md](../architecture.md) goal 1. Only the *capabilities* move out of method
position, and they were always the opt-in part.

**With one correction to the argument #14 made for it:** `dol.content_url` does *not* currently
resolve through the chain — it does a flat `getattr(store, 'url_for')(key)`
(`dol/content.py:210-214`) and returns a URL for the unmapped key. The *shape* is prior art; the
*resolution* is not. Fixing it upstream is a prerequisite for s3dol to serve as `dol.content`'s
S3 backend, which `dol/content.py`'s own module docstring names as the intended arrangement.

### D4 — `delete_many` is a free function only

No method on the store. Destructive **+** key-taking **+** delegated is the exact combination
the census found already destroying the wrong data in three sibling packages. We do not add a
fourth. [ADR-0010](0010-bucket-and-bulk-operations.md) §2's semantics — 1000-key chunking, the
HTTP-200 `Errors` parse, `S3PartialFailure` — are unchanged; only the surface moves.

### D5 — Option B's guard, without Option B's machinery

No `_key_methods` registry. Instead, a **reflective conformance test** that enumerates the
public methods of every Layer B class and fails on **any** that takes a key-shaped first
argument. Under D1a the allowlist is *empty*, which makes the test a simple, unarguable
invariant rather than a list someone has to curate.

#14 observed that a declarative registry fails the same silent way when an author forgets to
declare a method, and that the reflective test is "the kind of guard that actually holds". Since
the test is the part that holds and the registry is the part that can be forgotten, we ship the
test and skip the registry.

**D5 is not yet writable**, though: a "key-shaped first argument" heuristic misses
`delete(name, force=True)`, `delete_many(keys)` and `batch(operations)` — three of the shapes
this ADR is most worried about. The predicate has to be "does this method take anything that
gets used as a key", which is a judgement, not a signature check. Resolve with the open fork.

Note also that `dol` already ships a declarative hook of exactly Option B's shape —
`wrap_kvs(ingoing_key_methods=…, outcoming_key_methods=…)` — which is untested (`dol/trans.py`
carries the TODO) and **verified broken for leaf-defined methods** on both wrap paths. Option B
would mean replacing it, not building on it.

### D6 — Option E (capabilities as parallel Mappings) is deferred to v1.x

#14 argued E is the most dol-native option because "key transformation happens through the
Mapping protocol the wrapper already handles correctly." **That is not true at dol 0.3.58.** A
wrapper does not re-wrap a Mapping-valued attribute; `store.urls` returns an inner-keyed mapping
under both class- and instance-wrap. E only works if the view itself calls
`inner_most_key(wrapped_self(...))` — the same primitive as D2.

So E is a *surface* over D2's mechanism, not an alternative mechanism. It buys ergonomics and
collapses three [#12](https://github.com/i2mint/s3dol/issues/12) deferrals into one shape; it
does not buy correctness, and it costs new machinery. Revisit when `dol`'s `.meta` sidecar
design lands, which is the mechanism E actually wants.

### D7 — Option D (rebind delegated methods to the wrapper) is rejected

Not primarily for blast radius. `dol/misc/docs/dol_issue18_design.md` surveyed 26 ecosystem
sites and rejected the rebind family on three verified defects, the fatal one being that
rebinding binds `self` to the **innermost** `Wrap` — so under a `Pipe` stack it *does not fix
the case it exists to fix*, and stacked-codec writes gain a partial-transform corruption
surface. Plus statically-undetectable crashes: a leaf method calling `super().__getitem__(k)`
compiles `super(SomeClass, self)` with `self` now a `Wrap` → `TypeError`.

That document's Phase-1 instructions say verbatim: *do not touch `DelegatedAttribute.__get__`,
the `delegate_to` copy loop, or the signature graft.* We comply.

**The terminal fix is `dol`'s is-a wrapping** (dol#18 Approach C, committed for dol 0.4/1.0),
which resolves dol#18 and dol#6 together and makes this ADR's machinery redundant. That is why
the selection criterion here was *correct on dol 0.3.x **and** harmlessly redundant under is-a* —
not *permanent*. D2's helper degrades to a no-op; D3's free functions stay correct; D1's smaller
surface stays desirable on its own merits.

### D8 — Prefix pushdown is closed, not deferred

ADR-0001 already solves pushdown for the prefix s3dol owns: the leaf passes
`ListObjectsV2(Prefix=self.prefix)`. The residual case is a user stacking `filt_iter.prefixes(…)`
on top, which is **general predicate pushdown** — `dol` has no framework for it, and a
`__iter__(*, prefix_hint=…)` protocol would have exactly one implementer, violating
[ADR-0009](0009-scope-and-deferrals.md)'s own "no new `Protocol` without two implementers" rule.

Decision: a user-stacked filter **accepts a full scan**, and `s3dol.sub(store, prefix)` is the
documented cheap path. On an unwrapped store `sub` costs zero round-trips and pushes down, so
the fast route exists and is one call away. This is an answer, not a deferral; do not reopen it
without a second implementer.

### D9 — New upstream finding: `wrapped_self` has a temporary-wrapper hole

dol#18 shipped `wrapped_self` as the blessed pattern for delegation-wrapped classes. D1a shows
it silently degrades to the raw leaf whenever the wrapper is a temporary, because the delegated
bound method holds no reference to it and the weakref cleanup removes the evidence. Any
`*dol` package that adopted the blessed pattern — `xdol` and `unbox` have —
inherits this.

This is not a blocker for s3dol (D3 routes around it entirely), but it belongs upstream on
dol#18 with the repro, because the documented remedy for a *No Silent Failures* project
currently has a silent failure. Possible directions for dol, none of them s3dol's to choose:
have `DelegatedAttribute.__get__` return a wrapper-retaining bound method; keep a strong
reference for the duration of the call; or land is-a wrapping, which removes the registry
entirely.

## Open fork — how the keyed capabilities are actually surfaced

An adversarial review of the first draft refuted three of its supporting claims (all corrections
are folded in above) and surfaced a fourth option the draft never evaluated, because D6
conflated *capability as a Mapping-valued **attribute*** (broken — verified) with *capability as
a sibling **store***, which is a different design.

**Option S — capability stores.** A capability becomes a Layer B `KvReader` over the same key
space whose `__getitem__` returns the capability:

```python
class BucketHandles(KvReader):  # zero key-taking methods
    def _id_of_key(self, k):
        return self.prefix + k

    def __getitem__(self, k):
        return ObjectHandle(self.bucket, self._id_of_key(k))
```

`__getitem__` is the one thing `dol` maps correctly at **every** depth, so this is correct *by
construction*: no `inner_most_key`, no `wrapped_self`, no private `dol.dig` import, no upstream
PR. Verified 5/5 including a temporary under `Pipe`, under `cached_keys`, and under the
hand-rolled delegator **where the free function is silently wrong**. It also restores `[k]`
ergonomics and subsumes three of the capability features currently deferred in ADR-0009.

Its cost is real: a user who wraps the data store must wrap the sibling in parallel
(`KeyCodecs.prefixed('x/')` applied to both), because `store.handles` as an *attribute* is the
broken form. And it does not cover non-keyed bulk operations (`delete_many`, `prefixes`), which
stay free functions regardless.

Three further blockers must be resolved with this fork, in any option:

1. **`EndpointStore.delete(name, force=True)`** — still specified in
   [architecture.md](../architecture.md), [ADR-0010](0010-bucket-and-bulk-operations.md) §3 and
   [ADR-0007](0007-naming-and-compatibility.md). It is a public, key-taking, destructive,
   delegated Layer B method — structurally identical to `azuredol.AccountStore.delete`, which
   this ADR cites as a census exhibit. Either D5's empty allowlist fails on day one, or D5's
   "key-shaped first argument" heuristic misses it — and the same heuristic misses
   `delete_many(keys)`, `cosmodol`'s `batch(operations)` and `sshdol`'s `sync_to(target)`. D5
   needs a real predicate, not a name heuristic.
2. **`dol.SupportsUrlFor` requires a `url_for` *method***, and `dol.content_url` reaches it with
   `getattr(store, 'url_for', None)`. Under a zero-method Layer B, `content_url` returns `None`
   for every s3dol store forever. That makes the `dol.content` integration a **protocol change**
   upstream, not the "small PR" D3 implies.
3. **`url_for` needs `(endpoint, bucket, key)`, and only the key has a resolution primitive.**
   `recursive_get_attr(chain, 'bucket')` returns the *first* layer carrying a `bucket` attribute,
   so a middle layer with its own can pair a correctly-resolved key with the wrong bucket. Either
   add a `_leaf_of` primitive or state the limitation.

Also pending, independent of the fork: ADR-0011 must be added to `misc/docs/README.md` (which
still teaches the retired `inner_most_key(wrapped_self(self), k)` form), and the superseded
"a method may be added iff it takes a key…" rule survives verbatim in
[ADR-0005](0005-large-object-io.md) §2 and [ADR-0009](0009-scope-and-deferrals.md) §v1.0 scope.

## Consequences

**Buys.** Zero keyed seams instead of six, guarded by an invariant with an empty allowlist
rather than by discipline. A resolution primitive verified correct in 6/6 wrap × lifetime
shapes, where the obvious alternative is 2/4 and fails silently. No destructive delegated method
anywhere in the package. The Mapping surface — which `dol` maps correctly — stays the primary
API. Nothing that has to be unwound when `dol` lands is-a wrapping.

**Costs.** These are real and this ADR does not pretend otherwise.

- `store.url_for(k)` becomes `s3dol.url_for(store, k)`. That reads worse, and it is a
  divergence from v0 that `store.py`'s compat shim
  ([ADR-0007](0007-naming-and-compatibility.md)) must absorb — the shim can keep the method on
  the legacy class, since a legacy `S3Store` is not something users key-wrap.
- The capability API no longer tab-completes off a store, which is a genuine loss for the
  notebook-explorer use case that [state-of-play](../state-of-play.md) §1 names first. Mitigate
  in docs: `s3dol.<TAB>` is the discovery surface, and `__getitem__`/iteration still cover the
  common path.
- `sub` and `prefixes` become non-trivial to implement correctly (D2's two obligations), where
  as leaf methods they were three lines.
- s3dol depends on `dol.dig.inner_most_key`, which is not public API — a small upstream PR
  ([ADR-0006](0006-key-scoping-and-dol-fixes.md) §3).

**What NOT to do.**

1. **Do not add a key-taking method to a Layer B class** — not even "just this one", not even
   hardened with `wrapped_self`. D1a is why: the hardened form is silently wrong on temporaries
   and the failure is undetectable. Add it to `ObjectHandle` (key bound at construction) or ship
   a free function. The conformance test enforces an *empty* allowlist; do not add entries.
2. Do not compose `_abs_key` with `_id_of_key`. Re-read D2.
3. Do not treat `wrapped_self` as a correctness mechanism. It is a guardrail with a known
   silent failure mode (D1a, D9).
4. Do not rely on `isinstance(store, SupportsUrlFor)` to detect a capability — a
   `@runtime_checkable` Protocol checks presence, not correctness, and since 3.12 `isinstance`
   uses `getattr_static`, which sees a class-wrapped capability but not an instance-wrapped one.
5. Do not re-propose rebinding delegated methods. See D7 and the upstream evidence.
6. Do not invent a pushdown hint protocol for one implementer. See D8.
