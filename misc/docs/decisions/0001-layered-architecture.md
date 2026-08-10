# ADR-0001: Four-layer architecture, adopted from `azuredol`

- **Status:** Accepted
- **Date:** 2026-08-10
- **Supersedes:** the v0.1.x `base.py` / `store.py` / `utility.py` split

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

Adopt `azuredol`'s layering, in S3 vocabulary, with one addition.

```
D  recipes     factories + codec stacks, by composition only
C  stores      relative keys / prefix scoping, delegated to dol
B  base        close-to-metal, ABSOLUTE keys, bytes<->bytes, one error seam
A  connection  the credential + endpoint SSOT; the DI seam
```

The addition is the **A/B split being strict about key space**: Layer B addresses the
bucket's real keyspace and knows nothing about prefixes. This is not tidiness — it is what
makes `url_for`, `info` and the transfer strategies correct by construction, because they
operate on the key S3 actually sees rather than a user-facing alias. v0's `url_for` had to
re-apply `_id_of_key` by hand (`base.py:222-225`) precisely because that split didn't exist,
and any future method would have had to remember to do the same.

**Layer D never subclasses.** If a recipe can't be built from Layer C + `dol` wrappers, that
is evidence the capability belongs in Layer B as a *parameter*. This is the rule that stops
per-vendor classes from reappearing.

Class triads mirror `dol.filesys`:

```
BucketCollection -> BucketReader -> BucketStore          (keys: object keys)
BucketsCollection -> BucketsReader -> Buckets            (keys: bucket names)
ObjectHandle                                             (not a Mapping)
```

Reader-only classes are **real classes**, not instances with methods deleted. `dol`'s
`mk_read_only` works by assigning `__delitem__`/`__setitem__` onto the object, which
`type(store).__setitem__(store, k, v)` bypasses and which static analysis cannot see. Real
classes let a type checker catch `reader[k] = v`, and let an anonymous or read-scoped
credential refuse to even attempt a write.

## Consequences

**Buys.** One place to look for credential behaviour. Sub-stores and codecs for free from
`dol`. A capability added at Layer B is automatically available through every Layer C/D
composition. A reader of `azuredol` can read `s3dol` — the family's main value.

**Costs.** More modules (7 vs 3) for a package this size, and one genuinely awkward
consequence: because Layer B is absolute-keyed and Layer C is a `dol` wrapper, a method
added to Layer B is **not automatically key-correct** when reached through Layer C. That is
the `url_for` delegation bug in [ADR-0006](0006-key-scoping-and-dol-fixes.md), and it is the
price of delegating prefixing to `dol` rather than owning it. We pay it because owning it is
what produced v0's bugs, and because the fix is upstreamable.

**What NOT to do.**

1. Do not add a method to Layer B without deciding how it behaves through a Layer C wrap.
   Every such method needs a key-mapping test.
2. Do not put provider knowledge anywhere but `presets.py`.
3. Do not let Layer D grow a class statement.
