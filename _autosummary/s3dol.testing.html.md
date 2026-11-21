# s3dol.testing

`s3dol.testing` — a shipped in-memory fake and the exported conformance
suite (ADR-0008). Downstream users can test services that take a store as a
dependency with **no network, no docker, no moto**:

```default
from s3dol.testing import mock_s3

def test_my_service():
    assert MediaService(mock_s3()).url('a')
```

The fake is not a re-implementation: [`FakeBucketStore`](#s3dol.testing.FakeBucketStore) *is* the real
[`BucketStore`](s3dol.base.html.md#s3dol.base.BucketStore) with only the backend protocol (the `_op_*`
seam of ADR-0002) swapped for a dict — so the prefix arithmetic, key validity,
error taxonomy (the fake raises *synthesized botocore ClientErrors* through
the same translation seam), guards and Mapping semantics are structurally
identical, and drift fails the same conformance suite that gates the real
store.

The suite is parameterized by capability flags: `mock_s3()` runs the
structural presign assertion (2a) but not the fetch-it assertion (2b — there
is no HTTP server), so the honest claim is  *“the same suite, parameterized by
an \`\`endpoint_is_fetchable\`\` capability”* (ADR-0008).

### Functions

| [`mock_s3`](#s3dol.testing.mock_s3)([bucket, prefix, data, ...])   | An in-process fake `BucketStore`: no network, no docker, no moto.    |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| [`run_conformance`](#s3dol.testing.run_conformance)(make_store, \*[, ...]) | Assert the s3dol store laws against a fresh store from `make_store`. |

### Classes

| [`FakeBucketHandles`](#s3dol.testing.FakeBucketHandles)(\*args[, backing])               |                                                              |
|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| [`FakeBucketInfo`](#s3dol.testing.FakeBucketInfo)(\*args[, backing])                  |                                                              |
| [`FakeBucketStore`](#s3dol.testing.FakeBucketStore)(\*args[, backing])                 | The real `BucketStore` over a dict backend.                  |
| [`FakeBucketUrls`](#s3dol.testing.FakeBucketUrls)(\*args[, backing])                  |                                                              |
| [`FakeObjectHandle`](#s3dol.testing.FakeObjectHandle)(bucket, key, \*, backing[, anon]) | The fake's per-object escape hatch (mirrors `ObjectHandle`). |

### *class* s3dol.testing.FakeBucketHandles(\*args, backing=None, \*\*kwargs)

Bases: `_FakeOps`, [`BucketHandles`](s3dol.capabilities.html.md#s3dol.capabilities.BucketHandles)

### *class* s3dol.testing.FakeBucketInfo(\*args, backing=None, \*\*kwargs)

Bases: `_FakeOps`, [`BucketInfo`](s3dol.capabilities.html.md#s3dol.capabilities.BucketInfo)

### *class* s3dol.testing.FakeBucketStore(\*args, backing=None, \*\*kwargs)

Bases: `_FakeOps`, [`BucketStore`](s3dol.base.html.md#s3dol.base.BucketStore)

The real `BucketStore` over a dict backend.

### *class* s3dol.testing.FakeBucketUrls(\*args, backing=None, \*\*kwargs)

Bases: `_FakeOps`, [`BucketUrls`](s3dol.capabilities.html.md#s3dol.capabilities.BucketUrls)

### *class* s3dol.testing.FakeObjectHandle(bucket, key, , backing, anon=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

The fake’s per-object escape hatch (mirrors `ObjectHandle`).

### s3dol.testing.mock_s3(bucket='mock-bucket', , prefix='', data=None, bucket_exists=True, anon=False)

An in-process fake `BucketStore`: no network, no docker, no moto.

`data` seeds the bucket with *absolute* (wire) keys -> bytes.

```pycon
>>> s = mock_s3(data={'logs/a.txt': b'hi', 'other/z': b'no'}, prefix='logs')
>>> list(s), s['a.txt']
(['a.txt'], b'hi')
>>> s['b'] = b'new'; sorted(s)
['a.txt', 'b']
```

* **Return type:**
  [`FakeBucketStore`](#s3dol.testing.FakeBucketStore)

### s3dol.testing.run_conformance(make_store, \*, endpoint_is_fetchable=False, normalize=<function <lambda>>, check_pickle=True, check_urls=True, secret=None)

Assert the s3dol store laws against a fresh store from `make_store`.

`make_store` must return an EMPTY, writable store (each call may return
a new one over the same empty backend). `normalize` is the per-layer
canonical form of ADR-0005 N3 (identity for Layer B; `json.loads ∘
json.dumps` for a json recipe, …). `secret`, when given, is asserted
absent from every repr this suite touches.

Sibling packages and user code are welcome to run this against their own
stores — that is why it ships in the package rather than in `tests/`.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)
