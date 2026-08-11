"""Layer C — the one-liner factory and codec facades. Composition ONLY.

If a recipe cannot be expressed as Layer B + `dol` wrappers, that is evidence
the capability belongs in Layer B as a *parameter* — this module never grows a
class statement (ADR-0001; the rule that keeps per-vendor classes from
reappearing).

The one-liner::

    s = s3dol.s3_store('my-bucket')                     # zero ceremony
    s = s3dol.s3_store('my-bucket', prefix='logs/')     # scoped
    s = s3dol.s3_store('pub-bucket', anon=True)         # public data

With ``on_missing_bucket='assume'`` (the default; no probe, ever — ADR-0010)
the two-line quickstart raises on a bucket that does not exist yet; start from
nothing with ``on_missing_bucket='create'``.
"""

from __future__ import annotations

import json
from typing import Optional, Union

from dol import wrap_kvs

from s3dol.base import BucketReader, BucketStore
from s3dol.connection import S3Connection
from s3dol.errors import ConfigurationError
from s3dol.presets import Preset
from s3dol.reads import ReadStrategy
from s3dol.writes import WriteStrategy


def s3_store(
    bucket: str,
    *,
    prefix: str = "",
    preset: Union[str, Preset, None] = None,
    connection: Optional[S3Connection] = None,
    endpoint_url: Optional[str] = None,
    region_name: Optional[str] = None,
    profile: Optional[str] = None,
    credentials=None,
    anon: bool = False,
    on_missing_bucket: str = "assume",
    readonly: bool = False,
    value_codec=None,
    reads: Optional[ReadStrategy] = None,
    writes: Optional[WriteStrategy] = None,
):
    """One bucket as a ``MutableMapping[str, bytes]`` (a ``Mapping`` when
    ``readonly=True``).

    Annotated (deliberately) as the abstract Mapping, **not** ``-> BucketStore``:
    with ``value_codec`` the concrete type is a generated `dol` wrapper class
    that is *not* a ``BucketStore`` subclass. (v0 annotated its factory
    ``-> Store`` and returned something that was not one; inverting that lie
    is not an improvement — architecture.md.)

    Connection axes (``preset``/``endpoint_url``/``region_name``/``profile``/
    ``credentials``/``anon``) are conveniences that build an
    :class:`~s3dol.connection.S3Connection`; pass ``connection=`` instead for
    full control (they are mutually exclusive with it).
    """
    if connection is not None:
        if (
            any(
                x is not None
                for x in (preset, endpoint_url, region_name, profile, credentials)
            )
            or anon
        ):
            raise ConfigurationError(
                "Pass either connection= or the individual connection axes "
                "(preset/endpoint_url/region_name/profile/credentials/anon), "
                "not both — one SSOT for where the bytes go."
            )
    else:
        connection = S3Connection(
            preset=preset,
            endpoint_url=endpoint_url,
            region_name=region_name,
            profile=profile,
            credentials=credentials,
            anon=anon,
        )
    store_class = BucketReader if readonly else BucketStore
    store = store_class(
        bucket,
        connection=connection,
        prefix=prefix,
        reads=reads,
        writes=writes,
        on_missing_bucket=on_missing_bucket,
    )
    if value_codec is not None:
        store = value_codec(store)
    return store


# --------------------------------------------------------------------------- #
# Codec facades. Note S3Jsons uses value_encoder/decoder, NOT
# ValueCodecs.json(): that codec encodes to *str*, which Layer B rejects
# (ADR-0005 §2), and value_codec= does not compose via Pipe. A conformance
# assertion checks every shipped recipe's encoder output is a BytesSource.
# And never `bytes.decode` bare as a transform — dol's signature-inference
# would misread it (dol issue #9): wrap in a lambda.
# --------------------------------------------------------------------------- #

S3Texts = wrap_kvs(
    BucketStore,
    obj_of_data=lambda b: b.decode(),
    data_of_obj=lambda s: s.encode(),
)
S3Texts.__doc__ = """``BucketStore`` whose values are ``str`` (utf-8 on the wire)."""

S3Jsons = wrap_kvs(
    BucketStore,
    obj_of_data=json.loads,
    data_of_obj=lambda obj: json.dumps(obj).encode(),
)
S3Jsons.__doc__ = """``BucketStore`` whose values are JSON-able Python objects.

Note the canonical-form law (ADR-0005 N3): ``s[k] = v`` then ``s[k]`` gives
``json.loads(json.dumps(v))`` — tuples come back as lists, int dict keys as str.
"""


def _pickle_codec_store():
    import pickle

    return wrap_kvs(BucketStore, obj_of_data=pickle.loads, data_of_obj=pickle.dumps)


S3Pickles = _pickle_codec_store()
S3Pickles.__doc__ = (
    """``BucketStore`` whose values are arbitrary picklable Python objects."""
)
