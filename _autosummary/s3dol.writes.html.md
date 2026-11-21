# s3dol.writes

Write strategies (ADR-0005 §5): how `s[k] = v` reaches the wire.

A strategy is a callable `(client, bucket, key, value) -> None`, injected at
store construction — `__setitem__` stays `__setitem__`; *how* it uploads is
a constructor parameter. This is the answer to “infra-specific optimisation
without polluting the interface”: multipart is *transport*, not storage-model.

Strategies are frozen dataclasses, not closures, because every s3dol store
must pickle (ADR-0008 §6 / ADR-0012 D1) and a store carries its strategy.

Every non-`bytes` source routes through `upload_fileobj`/`TransferManager`,
never `PutObject` — so seekability is s3transfer’s problem, and the size
threshold is never consulted for a non-seekable source (where it is undecidable
anyway). Note `io.UnsupportedOperation` and botocore’s
`UnseekableStreamError` raise *before the request is built*, so they reach the
caller directly (they are not `ClientError``s and the error seam does not —
and must not — convert them to ``KeyError`).

```pycon
>>> strategy = transfer_writes(multipart_threshold=16 * 2**20)
>>> import pickle; pickle.loads(pickle.dumps(strategy)) == strategy
True
```

### Module Attributes

| [`DEFAULT_MULTIPART_THRESHOLD`](#s3dol.writes.DEFAULT_MULTIPART_THRESHOLD)   | boto3's own default multipart threshold (8 MiB).   |
|--------------------------------------------------------------------------------|----------------------------------------------------|

### Functions

| `simple_writes`()                                                            |                                                                                                                                                                                        |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`transfer_writes`](#s3dol.writes.transfer_writes)(\*\*kwargs) | Factory spelling of [`TransferWrites`](#s3dol.writes.TransferWrites) (the constructor-injection idiom of ADR-0005 §5: `BucketStore(..., writes=transfer_writes(...))`). |

### Classes

| [`SimpleWrites`](#s3dol.writes.SimpleWrites)()                             | Everything through a single `PutObject` — refs are materialised in memory first.                                                                                            |
|---------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`TransferWrites`](#s3dol.writes.TransferWrites)([multipart_threshold, ...]) | The default: small in-memory values take a single `PutObject`; file objects and refs go through `upload_fileobj` (which goes multipart above the threshold, transparently). |
| [`WriteStrategy`](#s3dol.writes.WriteStrategy)(\*args, \*\*kwargs)          |                                                                                                                                                                             |

### s3dol.writes.DEFAULT_MULTIPART_THRESHOLD *= 8388608*

boto3’s own default multipart threshold (8 MiB).

### *class* s3dol.writes.SimpleWrites

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Everything through a single `PutObject` — refs are materialised in
memory first. For small-object workloads and providers with no multipart
at all (`Capabilities.multipart=False`).

### *class* s3dol.writes.TransferWrites(multipart_threshold=8388608, multipart_chunksize=8388608, max_concurrency=10, use_threads=True)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The default: small in-memory values take a single `PutObject`; file
objects and refs go through `upload_fileobj` (which goes multipart above
the threshold, transparently). The overhead on small objects is one branch.

Providers that cap multipart parts (Scaleway: 1000 — see the preset’s
`Capabilities.max_multipart_parts`) may need a larger
`multipart_chunksize` for very large objects: parts ≈ size / chunksize.

### *class* s3dol.writes.WriteStrategy(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

### s3dol.writes.transfer_writes(\*\*kwargs)

Factory spelling of [`TransferWrites`](#s3dol.writes.TransferWrites) (the constructor-injection
idiom of ADR-0005 §5: `BucketStore(..., writes=transfer_writes(...))`).

* **Return type:**
  [`TransferWrites`](#s3dol.writes.TransferWrites)
