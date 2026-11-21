# s3dol.reads

Read strategies (ADR-0005 §6): what `s[k]` returns.

`s[k]` returns `bytes` — always, under the default strategy, because the
N1 canonical form demands it. `stream_reads` is the documented *runtime*
variation (the store’s values become chunk iterators); it does not change the
class’s static value type — honest typing there would need a generic store,
which v1 does not do.

Ranged reads and download-to-disk are per-object concerns and live on
`ObjectHandle` (`handle.read(offset=, length=)`, `handle.download_to`),
not here — a store-level “ranged” strategy would need a per-key range, which a
Mapping cannot express.

Strategies are frozen dataclasses, not closures, because every s3dol store
must pickle (ADR-0008 §6) and a store carries its strategy.

### Module Attributes

| [`DEFAULT_CHUNK_SIZE`](#s3dol.reads.DEFAULT_CHUNK_SIZE)   | Default streaming chunk size (8 MiB, matching the write side).   |
|-----------------------------------------------------------------------|------------------------------------------------------------------|

### Functions

| `bytes_reads`()            |    |
|----------------------------|----|
| `stream_reads`(\*\*kwargs) |    |

### Classes

| [`BytesReads`](#s3dol.reads.BytesReads)()                     | The default: one `GetObject`, fully read -> `bytes`.                              |
|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`ReadStrategy`](#s3dol.reads.ReadStrategy)(\*args, \*\*kwargs) |                                                                                   |
| [`StreamReads`](#s3dol.reads.StreamReads)([chunk_size])        | Values become lazy chunk iterators (the body streams; nothing is held in memory). |

### *class* s3dol.reads.BytesReads

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The default: one `GetObject`, fully read -> `bytes`.

### s3dol.reads.DEFAULT_CHUNK_SIZE *= 8388608*

Default streaming chunk size (8 MiB, matching the write side).

### *class* s3dol.reads.ReadStrategy(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

### *class* s3dol.reads.StreamReads(chunk_size=8388608)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Values become lazy chunk iterators (the body streams; nothing is held
in memory). The `GetObject` itself — and its `KeyError` — still
happens eagerly at `s[k]`; only the byte transfer is lazy.
