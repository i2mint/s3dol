# s3dol

s3 (through boto3) with a simple (dict-like or list-like) interface

To install:	```pip install s3dol```

[Documentation](https://i2mint.github.io/s3dol/)


## Quick start

```python
import s3dol

s = s3dol.s3_store('my-bucket')                  # MutableMapping[str, bytes]
s['hello.txt'] = b'world'
s['hello.txt']                                   # b'world'
list(s); 'hello.txt' in s; del s['hello.txt']

s = s3dol.s3_store('my-bucket', prefix='logs/')  # scoped to a prefix
s = s3dol.s3_store('open-data', anon=True)       # public bucket, no credentials
s = s3dol.s3_store('b', preset='minio', endpoint_url='http://localhost:9000')
```

Starting from nothing (the default never creates a bucket from a typo):

```python
s = s3dol.s3_store('brand-new-bucket', on_missing_bucket='create')
```

**Big objects go through the same interface** — no `upload_multipart` method:

```python
s['video.mp4'] = s3dol.Filepath('/tmp/video.mp4')   # streamed, multipart above 8 MiB
s['stream.bin'] = s3dol.Chunks(chunk_iterator)       # never fully in memory
```

**Keyed capabilities are stores you index**, not methods (a `dol` key wrapper
hands a method the *unmapped* key — so s3dol has none):

```python
s3dol.handles(s)['video.mp4'].read(offset=0, length=1024)   # ranged read
s3dol.handles(s)['video.mp4'].url(expires_in=3600)          # presigned URL
s3dol.urls(s)['video.mp4']                                  # ...or directly
s3dol.info(s)['video.mp4'].size                             # one HeadObject
```

Everything else is a free function taking the store first:

```python
s3dol.sub(s, 'folder/'); s3dol.prefixes(s)
s3dol.delete_many(s, ['a', 'b']); s3dol.delete_bucket(endpoint, 'name', force=True)
```

**Test without a cloud** — no network, no docker, no moto:

```python
from s3dol.testing import mock_s3, run_conformance

def test_my_service():
    assert MediaService(mock_s3(data={'a.mp4': b'...'})).play('a.mp4')
```

`run_conformance(make_store)` is exported too: run the same laws against your
own store (or a sibling `*dol` package's).

## Heads-up: v1 is coming — run `s3dol.diagnose()` now

A major, behaviour-clarifying v1 is in progress
(design: [`misc/docs/architecture.md`](misc/docs/architecture.md) and the ADRs in
[`misc/docs/decisions/`](misc/docs/decisions/)). Today's `S3Store` will keep
working through a compatibility shim, but a few v0 behaviours were bugs
(e.g. an explicit `endpoint_url=` being silently dropped whenever
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are exported) and v1 fixes them —
which can *move where your data goes*.

Before upgrading to v1, run the diagnosis with the same arguments you pass
`S3Store` today, in the environment you deploy in:

```python
import s3dol

s3dol.diagnose(bucket_name='my-bucket', endpoint_url='...', path='...')
```

It prints what resolves (endpoint, region, signing, credential *source* — never
a secret), where each value came from, and a **v0-vs-v1 divergence table**
telling you whether the upgrade changes anything for your call. It never
raises; a failing resolution is part of the report.


## Set up credentials

Recommended prerequisite to make getting started easier but not required.

### Option 1: [Environment Variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)
```
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

### Option 2: [Configure Default Profile in Credentials File](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
Add credentails in `~/.aws/credentials`
```
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Option 3: [Configure Default Profile with AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/configure/)
[Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
```bash
brew instal awscli
```
Set credentails with CLI
```bash
aws configure
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-west-2
Default output format [None]:
```
