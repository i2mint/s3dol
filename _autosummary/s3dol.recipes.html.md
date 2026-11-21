# s3dol.recipes

Layer C — the one-liner factory and codec facades. Composition ONLY.

If a recipe cannot be expressed as Layer B + `dol` wrappers, that is evidence
the capability belongs in Layer B as a *parameter* — this module never grows a
class statement (ADR-0001; the rule that keeps per-vendor classes from
reappearing).

The one-liner:

```default
s = s3dol.s3_store('my-bucket')                     # zero ceremony
s = s3dol.s3_store('my-bucket', prefix='logs/')     # scoped
s = s3dol.s3_store('pub-bucket', anon=True)         # public data
```

With `on_missing_bucket='assume'` (the default; no probe, ever — ADR-0010)
the two-line quickstart raises on a bucket that does not exist yet; start from
nothing with `on_missing_bucket='create'`.

### Functions

| [`s3_store`](#s3dol.recipes.s3_store)(bucket, \*[, prefix, preset, ...])   | One bucket as a `MutableMapping[str, bytes]` (a `Mapping` when `readonly=True`).   |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|

### s3dol.recipes.s3_store(bucket, , prefix='', preset=None, connection=None, endpoint_url=None, region_name=None, profile=None, credentials=None, anon=False, on_missing_bucket='assume', readonly=False, value_codec=None, reads=None, writes=None)

One bucket as a `MutableMapping[str, bytes]` (a `Mapping` when
`readonly=True`).

Annotated (deliberately) as the abstract Mapping, **not** `-> BucketStore`:
with `value_codec` the concrete type is a generated `dol` wrapper class
that is *not* a `BucketStore` subclass. (v0 annotated its factory
`-> Store` and returned something that was not one; inverting that lie
is not an improvement — architecture.md.)

Connection axes (`preset`/`endpoint_url`/`region_name`/`profile`/
`credentials`/`anon`) are conveniences that build an
[`S3Connection`](s3dol.connection.html.md#s3dol.connection.S3Connection); pass `connection=` instead for
full control (they are mutually exclusive with it).
