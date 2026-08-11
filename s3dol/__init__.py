"""
s3 (through boto3) with a simple (dict-like or list-like) interface
"""

from s3dol.base import S3Dol
from s3dol.store import S3Store

# New (v1) layer-A names, loaded lazily (PEP 562) so `import s3dol` does not
# pay for what a script does not use. `s3dol.diagnose(...)` is the ADR-0007 §5
# step-0 migration safety mechanism — run it with your current S3Store kwargs
# to see what resolves, from where, and whether the upcoming v1 moves it.
_LAZY_ATTRIBUTES = {
    "diagnose": ("s3dol._diagnose", "diagnose"),
    "S3Connection": "s3dol.connection",
    "resolve": "s3dol.connection",
    "Resolution": "s3dol.connection",
    "Preset": "s3dol.presets",
    "Capabilities": "s3dol.presets",
    "register_preset": ("s3dol.presets", "register"),
}


def __getattr__(name):
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
    # submodule binding that `import s3dol.diagnose` just created on this
    # package — without it, `s3dol.diagnose` would resolve to the module on
    # every access after the first, shadowing the function.
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY_ATTRIBUTES))
