# ADR-0006: Prefix scoping is delegated to `dol` — but only in its safe composition

- **Status:** Accepted
- **Date:** 2026-08-10
- **Severity:** This is the most important document in this set. Read it before writing any key-handling code.

## Context

v0.1.x does prefix scoping by hand:

```python
def _key_of_id(self, id):
    return id[len(self.prefix):]        # base.py:201
```

This is unguarded: if `id` doesn't start with `prefix`, it silently slices anyway. The
obvious fix — and the one every research pass recommended — is *"delete this and use `dol`'s
canonical mechanism, `mk_relative_path_store(prefix_attr='prefix')`"*.

**That fix is wrong.** `dol`'s prefix machinery has the same bug.

## The evidence

Store `{'a/b': 1, 'a/c': 2, 'z': 3, 'ab/x': 4}`, prefix `'a/'`, run against dol 0.3.58:

| Mechanism | keys produced | verdict |
|---|---|---|
| `KeyCodecs.prefixed('a/')` | `['', '/x', 'b', 'c']` | **CORRUPT** |
| `prefixless_view(store, prefix='a/')` | `['', '/x', 'b', 'c']` | **CORRUPT** |
| `mk_relative_path_store(cls, prefix_attr='prefix')` | `['', '/x', 'b', 'c']` | **CORRUPT** ← the recommended replacement |
| `handle_prefixes(store, prefix='a/')` | `['b', 'c']` | safe (filters first) |
| `Pipe(filt_iter.prefixes('a/'), KeyCodecs.prefixed('a/'))` | `['b', 'c']`, `len == 2`, `'' in p → False` | **safe** |

The non-matching key `z` becomes `''` (and `w['']` then raises `KeyError: 'a/'`), and the
*sibling* key `ab/x` becomes `/x`.

In S3 terms: a store scoped to `logs/` surfaces a neighbouring object `logs2/2026.txt` as a
plausible-looking, **writable** key `2/2026.txt`. Writing to it writes outside the store's
scope. For anyone using a prefix as a tenant or app boundary, that is a boundary violation
produced by the storage layer itself.

## Decision

### 1. `filt_iter.prefixes(p)` below every relativization is MANDATORY

It is a **correctness requirement, not an optimization**. The only sanctioned composition:

```python
relative = Pipe(
    filt_iter.prefixes(prefix),   # filter FIRST — not optional
    KeyCodecs.prefixed(prefix),   # then relativize
)
```

Bare `mk_relative_path_store` / `KeyCodecs.prefixed` / `prefixless_view` are **banned in
s3dol**, and the ban is enforced by a test in the conformance suite: a store containing
sibling and non-matching keys must expose exactly the in-scope ones, and round-trip them.

Where the prefix is also pushed down to `ListObjectsV2(Prefix=...)`, the client-side filter is
usually redundant — but "usually" is doing dangerous work there (a pushdown that silently
fails, a provider that ignores `Prefix`, a wrapper composed in a different order), so the
filter stays unconditionally.

### 2. `url_for` must reach the leaf with the fully-mapped key

Verified, and worse than the above because it is silent:

```python
w = KeyCodecs.prefixed('a/')(WithUrl)(...)
w['b']            # -> 1                     correct, prefix applied
w.url_for('b')    # -> https://x/b           WRONG: should be https://x/a/b
isinstance(w, SupportsUrlFor)   # -> True    the Protocol cannot detect this
```

`dol` wrappers delegate unknown attributes to the inner store **with the outer, unmapped
key**. So the moment prefixing moves into a `dol` wrap, every presigned URL points at the
wrong object — and nothing fails. The existing `test_url_for.py` asserts only substring
presence (`"test-bucket" in url`, `"Signature" in url`), so a URL for the wrong key **passes
today**.

Interim mechanism: route `url_for` through `dol.dig.inner_most_key`. Permanent mechanism: the
dol fix below.

Test requirement: parse the URL and assert the path equals the fully-prefixed key, and
actually fetch it against moto. Substring assertions are banned for this method.

### 3. Two fixes go upstream to `dol` first

Per the owner's decision, these land in `dol` as its own reviewed change, and s3dol then
requires that version. Every other `*dol` adapter almost certainly has the same latent bugs,
so fixing them once in `dol` is worth more than fixing them once in s3dol.

**dol fix 1 — strict prefix relativization.** A `strict=True` mode (proposed default in a
future major) on the prefix machinery: keys outside the prefix must **raise**, never be
silently sliced. Plus a property test:

```
∀ k in-scope:      key_of_id(id_of_key(k)) == k
∀ i out-of-scope:  key_of_id(i) raises          # never returns a corrupted key
```

**dol fix 2 — key-mapped delegation.** A mechanism so that delegated methods (`url_for`,
`info`, and anything a backend adds) receive the fully-mapped inner key. Without it, every
capability s3dol adds at Layer B is silently wrong through a Layer C wrap, and the package's
own layering becomes a trap.

Until both land, s3dol uses the safe local composition and `inner_most_key`, with `# TODO:
upstream to dol (dol#NN)` at each site and a linked issue. **Policy on upstreams:** never
block an s3dol release on a dol PR; never let a local copy diverge silently — raise the dol
floor the day each fix lands and delete the workaround in the same commit.

### 4. Key validity is checked before the wire

Probed behaviours that currently leak backend types through the Mapping:

| key | today |
|---|---|
| `''` | `ParamValidationError` — a botocore type escaping through `__getitem__` |
| `'folder/'` | returns a **sub-store**, not bytes; absent from `list(s)`; `in` says `True` — the object is permanently unreadable through the interface |
| `'bad\ud800key'` | `UnicodeEncodeError` |
| 1025-char key | fine on moto, `KeyTooLongError` on AWS |

Decisions: normalize all of these to `KeyNotValid` before the request; enforce the
1024-UTF-8-**byte** limit client-side so moto and AWS agree; and set `EncodingType='url'` by
default (per-preset opt-out — GCS rejects it) so keys containing control characters survive
the XML listing.

The trailing-`/` overload is removed: **`store[k]` always returns bytes**. Sub-stores come
from `store.sub('folder/')`. `store['folder/']` survives only on an explicitly-constructed
navigable reader for notebook use. This is what makes empty-directory markers (which a
filesystem migration creates) addressable at all.

## Consequences

**Buys.** Prefix scoping that is actually correct, and correct for every `*dol` adapter once
upstreamed. Presigned URLs that point at the right object. Sub-stores with zero round-trips
from `dol` rather than `type(self)(**self.__dict__)`.

**Costs.** A dependency on a `dol` release for the clean version, and an interim workaround
that must be deleted later — tracked, with the usual risk that it isn't.

**What NOT to do.**

1. **Never use `mk_relative_path_store`, `KeyCodecs.prefixed` or `prefixless_view` bare.**
2. Never assert on a presigned URL by substring.
3. Never add a Layer B method without a Layer C key-mapping test.
