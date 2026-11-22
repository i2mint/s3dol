# s3dol.uploads

Direct-to-S3 browser uploads: a presigned POST scoped to a key prefix.

A browser can upload straight to S3 without the server relaying bytes: the
server issues a presigned POST form whose signed policy pins

- the **exact key** (server-generated `<prefix><random token>`: the client
  never picks a filename, so it can neither overwrite a sibling object nor
  smuggle `..` segments; a form holds one object, and re-posting with it only
  overwrites that object, so the byte budget cannot be multiplied),
- a **content-length range** (`min_bytes`..\`\`max_bytes\`\`),
- the **content type** (optional), and
- an **expiry** (the 7-day SigV4 cap; an STS session’s own expiry still ends it sooner).

Before signing, an optional `budget` callable is asked to reserve the bytes
(`budget(max_bytes)` raises [`UploadBudgetExceeded`](#s3dol.uploads.UploadBudgetExceeded) to refuse, and may return a `release()` callable that is called if signing fails), so a
daily per-tenant byte budget is enforced *before* any form exists.
[`DailyByteBudget`](#s3dol.uploads.DailyByteBudget) is the ready-made one; any callable works.

The function is host-agnostic: it takes the store, the prefix and the limits
as arguments, and the web app mounts it behind its own auth:

```default
form = s3dol.presigned_upload(
    store, key_prefix='tenant-7/', max_bytes=5_000_000, content_type='image/png')
form.url, form.fields      # hand both to the browser's <form>/FormData
```

```pycon
>>> from s3dol.uploads import DailyByteBudget
>>> budget = DailyByteBudget(100, today=lambda: '2026-01-01')
>>> _ = budget(60); _ = budget(40)
>>> budget(1)
Traceback (most recent call last):
  ...
s3dol.uploads.UploadBudgetExceeded: daily upload budget exceeded: 100 of 100 bytes already reserved, 1 requested
```

### Module Attributes

| [`DEFAULT_UPLOAD_EXPIRES_IN`](#s3dol.uploads.DEFAULT_UPLOAD_EXPIRES_IN)   | long enough to pick a file, short enough to not be a standing upload credential.   |
|------------------------------------------------------------------------------|------------------------------------------------------------------------------------|

### Functions

| [`presigned_upload`](#s3dol.uploads.presigned_upload)(store, \*, key_prefix, ...)   | A presigned POST form for ONE new object under `key_prefix`.   |
|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------|

### Classes

| [`DailyByteBudget`](#s3dol.uploads.DailyByteBudget)(limit_bytes, \*[, ledger, today])   | A per-day byte budget (one instance per tenant); call it to reserve.   |
|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`PresignedUpload`](#s3dol.uploads.PresignedUpload)(url[, fields, key, ...])            | What the browser needs: POST `fields` + a `file` part to `url`.        |

### Exceptions

| [`UploadBudgetExceeded`](#s3dol.uploads.UploadBudgetExceeded)   | The tenant's byte budget cannot cover this upload; no form was signed.   |
|-------------------------------------------------------------------------|--------------------------------------------------------------------------|

### s3dol.uploads.DEFAULT_UPLOAD_EXPIRES_IN *= 900*

long enough to pick a file, short enough to
not be a standing upload credential.

* **Type:**
  Default form lifetime (seconds)

### *class* s3dol.uploads.DailyByteBudget(limit_bytes, \*, ledger=None, today=<function DailyByteBudget.<lambda>>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A per-day byte budget (one instance per tenant); call it to reserve.

`ledger` is any `MutableMapping[str, int]` (default: in-memory) keyed
by day, so persistence is a one-argument swap (a `dol` store, a file…).
`today` is the clock seam. Reservation is optimistic (the *maximum* the
form allows), and a form holds exactly one key, so re-posting cannot multiply the budget.

### *class* s3dol.uploads.PresignedUpload(url, fields=dict(), key='', key_prefix='', max_bytes=0)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

What the browser needs: POST `fields` + a `file` part to `url`.

### *exception* s3dol.uploads.UploadBudgetExceeded

Bases: [`S3Error`](s3dol.errors.html.md#s3dol.errors.S3Error)

The tenant’s byte budget cannot cover this upload; no form was signed.

### s3dol.uploads.presigned_upload(store, , key_prefix, max_bytes, content_type, min_bytes=0, expires_in=900, budget=None)

A presigned POST form for ONE new object under `key_prefix`.

`content_type` is required: pass the one type to accept, or `None` to
explicitly accept any (then a holder can store `text/html`: do not serve
such objects from a trusted origin).

`key_prefix` must end with the store’s delimiter (`tenant-7/` cannot
match `tenant-77/`) and is in the *store’s* key space (the store’s own
prefix is prepended, like every other s3dol key). The object key is
server-generated (`PresignedUpload.key`). Returns `None` for an
anonymous store (no signing capability, as [`s3dol.urls()`](s3dol.html.md#s3dol.urls) does).

Raises `ConfigurationError` for a bad prefix, size range or
`expires_in`, and [`UploadBudgetExceeded`](#s3dol.uploads.UploadBudgetExceeded) if `budget` refuses.

* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`PresignedUpload`](#s3dol.uploads.PresignedUpload)]
