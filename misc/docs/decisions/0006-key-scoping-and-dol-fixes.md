# ADR-0006: Prefix normalization, key validity, and the `dol` traps to avoid

- **Status:** Accepted
- **Date:** 2026-08-10
- **Severity:** Read before writing any key-handling code.
- **Note:** An earlier revision of this ADR made prefix scoping a `dol` wrapper above an
  absolute-keyed leaf. [ADR-0001](0001-layered-architecture.md) §"Why the prefix lives in the
  leaf" records why that was reversed. What remains here is the arithmetic the leaf must get
  right, plus the traps for anyone stacking `dol` codecs on top.

## Context

v0.1.x does prefix scoping by hand and unsafely:

```python
def _key_of_id(self, id):
    return id[len(self.prefix):]        # base.py:201 — slices even when it doesn't match
```

The natural fix is "use `dol`'s canonical mechanism". **`dol`'s mechanism has the same bug.**

Store `{'a/b': 1, 'a/c': 2, 'z': 3, 'ab/x': 4}`, prefix `'a/'`, dol 0.3.58:

| Mechanism | keys produced | verdict |
|---|---|---|
| `KeyCodecs.prefixed('a/')` | `['', '/x', 'b', 'c']` | **CORRUPT** |
| `prefixless_view(store, prefix='a/')` | `['', '/x', 'b', 'c']` | **CORRUPT** |
| `mk_relative_path_store(cls, prefix_attr='prefix')` | `['', '/x', 'b', 'c']` | **CORRUPT** |
| `Pipe(filt_iter.prefixes('a/'), KeyCodecs.prefixed('a/'))` | `['b', 'c']` | safe |

The non-matching key `z` becomes `''`; the *sibling* key `ab/x` becomes `/x`. A store scoped
to `logs/` surfaces a neighbouring `logs2/2026.txt` as a plausible, **writable** key
`2/2026.txt`. For anyone using a prefix as a tenant or app boundary, that is a boundary
violation produced by the storage layer.

## Decision

### 1. Normalize the prefix, then filter, then relativize — in that order

**Normalization is not optional and comes first:**

```python
prefix = f"{prefix.strip(delimiter)}{delimiter}" if prefix else ""
```

Without it, `prefix='logs'` (no trailing slash) exposes `logs2/2026.txt` as a readable **and
writable** key `2/2026.txt` — the same boundary violation this section exists to prevent, one
character away. Verified:

```python
safe = Pipe(filt_iter.prefixes('logs'), KeyCodecs.prefixed('logs'))(store)
safe['2/2026.txt']         # -> 2      OTHER TENANT, READ
safe['2/hacked.txt'] = 99  # -> writes 'logs2/hacked.txt'   OTHER TENANT, WRITE
```

Both v0 (`base.py:100-105`) and `azuredol` (`base.py:88`) normalize. A prefix that does not
terminate in the delimiter is normalized, never accepted as-is.

**In the leaf**, `_id_of_key`/`_key_of_id` operate on the normalized prefix, `__iter__`
passes `Prefix=self.prefix` to `ListObjectsV2`, and `_key_of_id` **raises** rather than
slicing a non-matching id. The server-side `Prefix` makes out-of-scope keys unreachable in
the common path; the raising `_key_of_id` is the belt to that suspenders, because a provider
that ignores `Prefix` must not silently produce corrupt keys.

**If you additionally stack a `dol` prefix codec**, the only safe composition is:

```python
Pipe(filt_iter.prefixes(prefix), KeyCodecs.prefixed(prefix))   # filter FIRST
```

Bare `mk_relative_path_store` / `KeyCodecs.prefixed` / `prefixless_view` are **banned in
s3dol**. The order is not a style preference — reversed, the store is silently empty.

Related upstream bug: `dol.trans.filter_prefixes(['logs/', 'tmp/'])` compiles to
`^logs/|tmp/` = `(^logs/)|(tmp/)`, so `zzz/tmp/c` matches. Multi-prefix scoping leaks.

### 2. The delegation trap (for anyone stacking `dol` on top)

`dol` wrappers delegate unknown attributes to the leaf **with the outer, unmapped key**:

```python
w = KeyCodecs.prefixed('a/')(WithUrl)(...)
w['b']            # -> 1                  correct
w.url_for('b')    # -> https://x/b        WRONG: should be https://x/a/b
```

`isinstance(w, SupportsUrlFor)` stays `True` — a `@runtime_checkable` Protocol checks method
*presence* only, so it cannot detect this. (It is also wrap-dependent: since Python 3.12
`isinstance` uses `getattr_static`, which sees a **class**-wrapped capability but not an
**instance**-wrapped one. Capability detection must not rely on it.)

The correct escape is **`inner_most_key(wrapped_self(self), k)`**.

> `inner_most_key(self, k)` — which an earlier revision of this ADR prescribed — returns
> **`None`**, silently: inside a delegated method `self` *is* the unwrapped leaf (dol issue
> #18), so the URL becomes `https://…/None` with no exception. Do not copy that form.

`dol.wrapped_self` **already ships in dol 0.3.58**, so this is a floor bump, not an upstream
project. `url_for` must additionally **raise** when the mapped key is not a `str`, so the
`None` failure mode is impossible rather than merely documented.

### 3. What still goes upstream to `dol`

Reduced, now that prefixing lives in the leaf and `wrapped_self` turns out to exist:

1. **Strict prefix relativization** — a `strict=True` mode where keys outside the prefix
   raise rather than being silently sliced, plus the property test
   (`∀ k in-scope: key_of_id(id_of_key(k)) == k`; `∀ i out-of-scope: key_of_id(i)` raises).
   Every `*dol` adapter that uses this machinery has the latent bug.
2. **`filter_prefixes` regex grouping** — `^logs/|tmp/` must be `^(?:logs/|tmp/)`.
3. **`_filt_iter` assigning `__len__` unconditionally** — it should not resurrect a `__len__`
   the wrapped class deliberately omits.
4. **Document `wrapped_self` as the delegation answer**, and make the family use it.

**Policy on upstreams:** never block an s3dol release on a `dol` PR; never let a local copy
diverge silently — raise the `dol` floor the day each lands and delete the workaround in the
same commit.

### 4. Key validity is checked before the wire

Probed behaviours that currently leak backend types through the Mapping:

| key | today |
|---|---|
| `''` | `ParamValidationError` — a botocore type escaping `__getitem__` |
| `'folder/'` | returns a **sub-store**, not bytes; absent from `list(s)`; `in` says `True` — permanently unreadable through the interface |
| `'bad\ud800key'` | `UnicodeEncodeError` |
| 1025-char key | fine on moto, `KeyTooLongError` on AWS |

Decisions: normalize all of these to `KeyNotValid` before the request, and enforce the
1024-UTF-8-**byte** limit client-side so moto and AWS agree.

**Never pass `EncodingType`.** botocore sets it on every `ListObjects*` *and* URL-decodes the
response — but the decode is gated on a flag it sets only when the caller did **not** pass the
parameter (`botocore/handlers.py`). Passing it explicitly disables botocore's decoder and
returns percent-encoded keys that no longer address their objects. Verified on moto with the
keys `plain, 'a b', café, 'a+b', 'a\rb', 'p/x y', 'a%20b'`:

```
DEFAULT (not passed)        -> 7/7 round-trip
EXPLICIT EncodingType='url' -> 2/7 round-trip
   ['a%0Db','a%20b','a%2520b','a%2Bb','caf%C3%A9','p/x%20y','plain']
```

An earlier revision of this ADR mandated `EncodingType='url'` "so keys with control characters
survive the XML listing". That decision **caused** the corruption it claimed to prevent, and
broke [ADR-0008](0008-testing-architecture.md)'s conformance law `all(k in s for k in s)`. Its
stated per-preset opt-out was also unimplementable: not passing the parameter doesn't remove
it, since botocore adds it. If a provider rejects botocore's auto-`EncodingType`, the preset
expresses that by unregistering botocore's handler pair — and s3dol then owns the decode.

**The trailing-`/` overload is removed**: `store[k]` always returns bytes; sub-stores come
from `store.sub('folder/')`. `store['folder/']` survives only on an explicitly-constructed
navigable reader for notebook use (`azuredol` keeps both, and so do we).

One consequence to handle explicitly: a view scoped to `a/` relativizes the **exact-prefix
marker object** `a/` to the key `''`, which this section forbids. Resolution: the scoped view
filters out the exact-prefix marker, and `store.sub(p)` documents that the parent's own marker
is not a key of the child. The marker stays addressable by its absolute key on an unscoped
store. (Directory markers matter: a filesystem migration creates them for empty directories.)

## Consequences

**Buys.** Prefix scoping that is correct, cheap (server-side `Prefix`), and visible in
`__repr__`. Keys that round-trip. A short upstream list that benefits every `*dol` adapter.

**Costs.** s3dol owns the arithmetic and therefore owns a property test for it. The
exact-prefix-marker rule is a genuine wart — an object *is* hidden from its own scoped view —
justified only because the alternative is a key the store iterates but refuses to read.

**What NOT to do.**

1. Never accept an un-normalized prefix.
2. Never use `mk_relative_path_store` / `KeyCodecs.prefixed` / `prefixless_view` bare.
3. Never pass `EncodingType`.
4. Never assert on a presigned URL by substring ([ADR-0008](0008-testing-architecture.md)).
5. Never write `inner_most_key(self, k)` inside a delegated method.
