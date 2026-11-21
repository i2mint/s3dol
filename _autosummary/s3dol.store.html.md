# s3dol.store

COMPAT SHIM — the v0 `S3Store`, forwarded onto the v1 machinery.

Removed in v2. This module **must survive as an importable module**: external
dependents do `from s3dol.store import S3Store` (fully qualified), not
`from s3dol import S3Store` (ADR-0007 §3).

The shim keeps v0’s exact signature (`bucket_name` positional-or-keyword,
`path=` not renamed, the `make_bucket` tri-state, and
`is_supabase_endpoint=` which an early draft forgot) and is \*\*a fix delivery
mechanism\*\*: callers get the corrected endpoint/credential resolution without
changing a line. Three v0 behaviours a dependent’s *passing tests* rely on are
deliberately preserved here (and only here — ADR-0007 §4):

- `store[k] = 'a str'` is accepted (encoded utf-8, with a
  `DeprecationWarning`);
- `store['folder/']` returns a sub-store;
- `del store[absent]` silently succeeds (v1 deletes are idempotent anyway).

And one v0 behaviour is preserved on the Supabase branch only: the read-side
de-chunker for objects that v0’s misconfigured writes stored with their
`aws-chunked` HTTP framing verbatim (ADR-0003 §3). It is implemented
*correctly* here (multi-chunk; full-parse-or-passthrough) where v0’s truncated
multi-chunk payloads. Per ADR-0003’s sequencing rule the codec is not deleted
until a repair path ships — [`detect_chunked_framing()`](#s3dol.store.detect_chunked_framing) is that path’s
diagnostic.

When v1 resolves a call differently than v0 *effectively* did (v0 dropped an
explicit `endpoint_url` whenever env credentials were set, and let env
credentials override explicit ones), the shim warns with
[`S3DolResolutionChanged`](s3dol.errors.html.md#s3dol.errors.S3DolResolutionChanged) — the ADR-0012 D7 differential.
This module and that warning die together in v2.

### Functions

| [`S3Store`](#s3dol.store.S3Store)(bucket_name, \*[, make_bucket, path, ...])   | DEPRECATED — the v0 entry point, kept working until v2.                                                         |
|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| [`dechunk_aws_chunked`](#s3dol.store.dechunk_aws_chunked)(data)                            | Undo verbatim-persisted `aws-chunked` framing, if present.                                                      |
| [`detect_chunked_framing`](#s3dol.store.detect_chunked_framing)(store, \*[, sample])          | Which of the first `sample` keys hold verbatim `aws-chunked` framing (objects corrupted by v0 Supabase writes). |

### s3dol.store.S3Store(bucket_name, , make_bucket=None, path=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, endpoint_url=None, region_name=None, profile_name=None, skip_bucket_check=None, is_supabase_endpoint=None)

DEPRECATED — the v0 entry point, kept working until v2.

Use [`s3dol.s3_store()`](s3dol.html.md#s3dol.s3_store) instead: `s3_store(bucket, prefix=...)`.

Signature is v0’s exactly (`path=` not renamed; `make_bucket`
tri-state maps `True -> on_missing_bucket='create'`, `False ->
'raise'`, `None -> 'assume'`). Behaviour differences vs v0 are bug
fixes only (explicit endpoint/credentials honoured; listing errors raise
instead of returning `[]`; writes no longer create buckets unless asked)
and announce themselves via `S3DolResolutionChanged` when they change
this call’s resolution.

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/typing.html#typing.MutableMapping)

### s3dol.store.dechunk_aws_chunked(data)

Undo verbatim-persisted `aws-chunked` framing, if present.

Full-parse-or-passthrough: either the ENTIRE payload parses as chunked
framing (every chunk header, every CRLF, through the terminal `0` chunk)
and the concatenated chunks are returned, or the payload is returned
untouched. v0’s parser truncated any payload larger than one chunk and
misfired on payloads that merely *started* with hex digits; requiring a
complete valid parse removes both failure modes (a random payload that
happens to be a complete chunked document is astronomically unlikely).

```pycon
>>> dechunk_aws_chunked(b'5\r\nhello\r\n0\r\n\r\n')
b'hello'
>>> framed = b'3\r\nabc\r\n2\r\nde\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n'
>>> dechunk_aws_chunked(framed)  # multi-chunk (v0 truncated this)
b'abcde'
>>> dechunk_aws_chunked(b'cafe is hex-ish but not framed')
b'cafe is hex-ish but not framed'
```

* **Return type:**
  [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)

### s3dol.store.detect_chunked_framing(store, , sample=20)

Which of the first `sample` keys hold verbatim `aws-chunked`
framing (objects corrupted by v0 Supabase writes). The repair path:
`store[k] = dechunk_aws_chunked(raw[k])` per flagged key, reading raw
bytes through an un-codec’d store.

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)
