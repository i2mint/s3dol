"""s3dol — S3 and S3-compatible object storage behind a dict-like interface.

The one-liner::

    import s3dol
    s = s3dol.s3_store('my-bucket')          # MutableMapping[str, bytes]
    s['k'] = b'v'; s['k']; list(s); del s['k']

Keyed capabilities are **sibling stores you index** (never methods — a dol key
wrapper would hand a method the unmapped key; ADR-0011)::

    s3dol.handles(s)['k']    # ObjectHandle (ranged reads, streams, metadata)
    s3dol.urls(s)['k']       # presigned URL
    s3dol.info(s)['k']       # ObjectInfo

Non-keyed operations are free functions taking the store first::

    s3dol.sub(s, 'folder/')          s3dol.prefixes(s)
    s3dol.delete_many(s, keys)       s3dol.delete_bucket(endpoint, name, force=True)

Test without a cloud: ``from s3dol.testing import mock_s3, run_conformance``.
Migrating from v0? ``s3dol.diagnose(**your_S3Store_kwargs)`` prints what
resolves, from where, and whether v1 moves it. The legacy ``S3Store`` keeps
working via ``s3dol.store`` (deprecated, removed in v2).

Everything here loads lazily (PEP 562): ``import s3dol`` does not import boto3.
"""

# name -> module path (attribute of the same name), or (module path, attribute)
_LAZY_ATTRIBUTES = {
    # Layer C — recipes
    "s3_store": "s3dol.recipes",
    "S3Texts": "s3dol.recipes",
    "S3Jsons": "s3dol.recipes",
    "S3Pickles": "s3dol.recipes",
    # Layer B — stores and the handle
    "BucketCollection": "s3dol.base",
    "BucketReader": "s3dol.base",
    "BucketStore": "s3dol.base",
    "EndpointCollection": "s3dol.base",
    "EndpointReader": "s3dol.base",
    "EndpointStore": "s3dol.base",
    "ObjectHandle": "s3dol.base",
    "ObjectInfo": "s3dol.base",
    "S3Profiles": "s3dol.base",
    # sibling capability stores + accessors (ADR-0011 D2)
    "BucketHandles": "s3dol.capabilities",
    "BucketUrls": "s3dol.capabilities",
    "BucketInfo": "s3dol.capabilities",
    "handles": "s3dol.capabilities",
    "urls": "s3dol.capabilities",
    "info": "s3dol.capabilities",
    # free functions (ADR-0011 D3/D4)
    "sub": "s3dol.capabilities",
    "prefixes": "s3dol.capabilities",
    "delete_many": "s3dol.capabilities",
    "delete_bucket": "s3dol.capabilities",
    # value refs (ADR-0005)
    "Filepath": "s3dol.values",
    "Chunks": "s3dol.values",
    "Streamable": "s3dol.values",
    # Layer A — connection + presets
    "S3Connection": "s3dol.connection",
    "resolve": "s3dol.connection",
    "Resolution": "s3dol.connection",
    "StaticCredentials": "s3dol.connection",
    "ProfileCredentials": "s3dol.connection",
    "CallableCredentials": "s3dol.connection",
    "Preset": "s3dol.presets",
    "Capabilities": "s3dol.presets",
    "register_preset": ("s3dol.presets", "register"),
    # errors (the taxonomy — `except s3dol.S3Error` catches the package)
    "S3Error": "s3dol.errors",
    "ObjectNotFound": "s3dol.errors",
    "BucketNotFound": "s3dol.errors",
    "KeyNotValid": "s3dol.errors",
    "ObjectArchived": "s3dol.errors",
    "AccessDenied": "s3dol.errors",
    "CredentialsError": "s3dol.errors",
    "NotSupported": "s3dol.errors",
    "BucketNotEmpty": "s3dol.errors",
    "S3PartialFailure": "s3dol.errors",
    "ConfigurationError": "s3dol.errors",
    "S3DolWarning": "s3dol.errors",
    # diagnostics
    "diagnose": ("s3dol._diagnose", "diagnose"),
    # legacy (deprecated; removed in v2)
    "S3Store": "s3dol.store",
}

__all__ = sorted(_LAZY_ATTRIBUTES) + ["S3Dol", "__version__"]


def __getattr__(name):
    if name == "__version__":
        import importlib.metadata

        try:
            return importlib.metadata.version("s3dol")
        except importlib.metadata.PackageNotFoundError:
            return "unknown"
    if name == "S3Dol":
        # v0's profiles->buckets->keys explorer, renamed (ADR-0007 §1).
        import warnings

        warnings.warn(
            "s3dol.S3Dol is deprecated (removed in v2): it is now "
            "s3dol.S3Profiles — its keys are AWS profile names.",
            DeprecationWarning,
            stacklevel=2,
        )
        from s3dol.base import S3Profiles

        return S3Profiles
    try:
        target = _LAZY_ATTRIBUTES[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    import importlib

    if isinstance(target, tuple):
        module_path, attribute = target
    else:
        module_path, attribute = target, name
    value = getattr(importlib.import_module(module_path), attribute)
    # Cache the resolved attribute. This ALSO deliberately overrides the
    # submodule binding a same-named submodule import creates on this package
    # (e.g. `info` vs a hypothetical info module) — without it, the second
    # access would resolve to the module and shadow the function.
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
