# ADR-0001: Four-layer architecture, adopted from `azuredol`

- **Status:** Accepted
- **Date:** 2026-08-10
- **Supersedes:** the v0.1.x `base.py` / `store.py` / `utility.py` split
- **Amended by:** [ADR-0011](0011-keyed-capability-surface.md) — Layer B has **no** key-taking
  public methods. `url_for`/`info` move onto `ObjectHandle` (key bound at construction);
  `handle`, `sub`, `prefixes` and `delete_many` become free functions taking the store first.
  See §"Prefix-in-leaf is necessary, not sufficient" below.

## Context

v0.1.x has three modules and a class hierarchy that mixes concerns at every level:
`BaseS3BucketReader` holds a client *and* does prefix arithmetic *and* parses responses;
`S3Store` is a factory function that decides credentials, provider quirks, bucket policy and
prefix handling in one 80-line body; `SupabaseS3BucketDol` subclasses the store to work
around a *client configuration* problem by hand-parsing HTTP framing out of object bodies.

The code knows. There are five `TODO` comments saying so, including
`# TODO: Messy. Should use wrap_kvs.` and two `# TODO: Smelly. use trans tools`.

We are not the first `*dol` blob adapter to face this. **`azuredol` completed exactly this
refactor**, and its `misc/docs/design_decisions.md` cites s3dol by name as the pattern it
rejected — §5 on `type(self)(**self.__dict__)` sub-stores, §12 on the cascading container
delete (*"This is convenient and dangerous. We refuse it."*).

## Decision

Adopt `azuredol`'s layering — as its **code** implements it — in S3 vocabulary.

```
C  recipes     factories + codec stacks, by composition only
B  base        close-to-metal; owns a normalized `prefix`; one error seam
A  connection  the credential + endpoint SSOT; the DI seam
```

**Layer B owns the prefix.** A bucket store carries a normalized `prefix`;
`_id_of_key`/`_key_of_id` live in the leaf; the prefix is pushed into
`ListObjectsV2(Prefix=…)`; it shows in `__repr__`; sub-stores come from an explicit
`self._with(prefix=…)`. `dol` is used for **codecs, filtering and caching — not prefix
arithmetic**.

**Layer C never subclasses.** If a recipe can't be built from Layer B + `dol` wrappers, that
is evidence the capability belongs in Layer B as a *parameter*. This is the rule that stops
per-vendor classes from reappearing.

Class triads mirror `dol.filesys`:

```
BucketCollection   -> BucketReader   -> BucketStore     (keys: object keys)
EndpointCollection -> EndpointReader -> EndpointStore   (keys: bucket names)
ObjectHandle                                            (not a Mapping)
```

## Why the prefix lives in the leaf

An earlier draft of this ADR did the opposite: Layer B was absolute-keyed and knew nothing
about prefixes, with a separate Layer C applying prefix scoping through a `dol` wrapper. The
reasoning was that a leaf addressing the bucket's real keyspace makes `url_for` and friends
correct by construction. **That draft was wrong, and it was wrong in a way worth recording,
because the mistake is attractive.**

Two mechanical facts kill it.

**1. `dol` delegates unknown attributes with the outer, unmapped key.**
`dol.base.Store.__getattr__` returns the *bound leaf method*, so every non-dunder method the
package adds receives the user-facing key, not the mapped one. Verified against dol 0.3.58,
with a store scoped to `logs/` in a bucket that also holds `logs2/leak` and a root `a.txt`:

| method | result through the wrap | consequence |
|---|---|---|
| `url_for('a.txt')` | `https://…/a.txt` | signs a URL for the **wrong object** |
| `sub('x/')` | store over the bucket **root** `x/` | scope escape |
| `handle('a.txt')` | handle on root `a.txt` | wrong object |
| `info('a.txt')` | `KeyError` for a key that is present | manufactured "absent" |
| `prefixes()` | `['logs/', 'logs2/']` | leaks the sibling prefix the scope exists to hide |
| `delete_many(['a.txt'])` | **root `a.txt` destroyed**, `logs/a.txt` untouched | silent destruction of the wrong object |

`len()` also comes back from the dead: `dol.trans._filt_iter` assigns `store_cls.__len__`
**unconditionally, with no `hasattr` guard**, so the mandatory prefix filter undoes
[ADR-0008](0008-testing-architecture.md)'s deliberate omission of `__len__` *and* restores
the double-listing it diagnoses.

**2. Prefix pushdown is unimplementable across that seam.** `Store.__iter__` calls
`self.store.__iter__()`. There is no channel by which a key-wrapper hands its prefix to the
leaf's `ListObjectsV2(Prefix=…)`. Every prefix-scoped listing becomes a full-bucket scan —
measured at 22 LIST requests where a leaf-owned prefix costs 1.

**And the prior art cited for the draft was misread.** `azuredol`'s `architecture.md` says
container stores are wrapped with `mk_relative_path_store(prefix_attr='prefix')`. Its
**code** does no such thing: `base.py:88` normalizes the prefix in the leaf, `:100-103`
define `_id_of_key`/`_key_of_id` there, `:147`/`:171` push it into
`list_blobs(name_starts_with=…)`, `:109` puts it in `__repr__`, and `mk_relative_path_store`
appears **zero times** in the package. azuredol has none of the six bugs above precisely
because it does the thing this ADR now specifies. The lesson generalizes: **a sibling's
design doc is a claim; its source is the evidence.**

## Prefix-in-leaf is necessary, not sufficient

*(Added by [ADR-0011](0011-keyed-capability-surface.md). The section above is correct about why
the prefix must live in the leaf; it drew an incomplete lesson from `azuredol`.)*

Putting the prefix in the leaf removes the key-mapping seam **for the prefix s3dol owns**. It
does nothing for a user who wraps an s3dol store with a `dol` key codec — every capability
method is then delegated with the outer, unmapped key again, by one of *two* routes
(`Store.__getattr__` at `dol/base.py:742` for instance-wraps and `mk_relative_path_store`
subclasses; `DelegatedAttribute.__get__` at `dol/base.py:279` for class-wraps).

Re-reading `azuredol`'s code with that in mind gives the sharper finding: **`ContainerStore` has
essentially no key-taking public methods at all.** The rich per-object surface lives on
`BlobHandle` (`azuredol/base.py:233`), which binds its blob **at construction**, so a key codec
over the store cannot corrupt it. `azuredol`'s only residual exposures are
`ContainerCollection.walk` (`base.py:164`, leaf-keyed return) and `AccountStore.delete`
(`base.py:460`, keyed and destructive).

So `azuredol` is not safe because of where its prefix lives. It is safe because **it has almost
no seam to get wrong.** The table above lists six methods this ADR originally proposed for
Layer B; [ADR-0011](0011-keyed-capability-surface.md) reduces that to **zero** (plus one guarded
`url_for` shim for `dol.SupportsUrlFor`), turning keyed capabilities into **sibling stores** you
index — `s3dol.handles(store)[k]` — and the rest into free functions.

Before proposing that one keyed method be kept: the obvious hardening,
`inner_most_key(wrapped_self(self), k)`, is **itself silently wrong** when nothing holds a
reference to the wrapper, because the delegated bound method holds none and the weakref registry
entry is removed when the wrapper dies. Sibling stores avoid the question entirely by routing
through `__getitem__`, which `dol` maps correctly at every depth. Verified; see ADR-0011
§D1a/§D2.

The "Consequences" claim below that *"every capability method is key-correct by construction"*
holds only for an unwrapped store. Read it as: *there is no seam **we** introduce* — a user can
still add one.

Reader-only classes are **real classes**, not instances with methods deleted. `dol`'s
`mk_read_only` is not merely bypassable — it is **non-functional on a `dol` store**: verified,
`ro['a'] = b'2'` on `mk_read_only(Store({...}))` succeeds silently, no exception. Real classes
let a type checker catch `reader[k] = v`, and let an anonymous or read-scoped credential
refuse to even attempt a write.

## Consequences

**Buys.** One place to look for credential behaviour. Every capability method is key-correct
by construction, because there is no key-mapping seam between the method and the wire. Prefix
pushdown to `ListObjectsV2` is available. `__len__` stays absent. A reader of `azuredol` can
read `s3dol` — the family's main value.

**Costs.** s3dol owns prefix arithmetic rather than inheriting it, which means it owns the
correctness of that arithmetic — including the normalization rule that
[ADR-0006](0006-key-scoping-and-dol-fixes.md) §1 specifies, and a round-trip property test to
keep it honest. That is a real cost and it is the one v0 paid badly. We accept it because the
alternative is not "`dol` does it correctly for us" — it is "`dol` does it incorrectly for us,
across six methods, silently".

Users who *additionally* want a `dol` prefix codec on top are not prevented; they inherit the
delegation trap, which is why [ADR-0006](0006-key-scoping-and-dol-fixes.md) §2 documents it.

**What NOT to do.**

1. Do not move prefix handling out of the leaf. Re-read §"Why the prefix lives in the leaf".
2. Do not put provider knowledge anywhere but `presets.py`.
3. Do not let Layer C grow a class statement.
4. Do not treat a sibling package's design doc as evidence about its behaviour. Read its code.
