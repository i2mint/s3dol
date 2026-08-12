"""The ADR-0011 D5 reflective conformance test: no keyed methods, enforced.

Not a registry — a registry fails silently when an author forgets to declare a
method. This test enumerates **every public attribute of every Layer B class**
against an explicit inventory and fails on any *new* one, forcing the
"does this take anything that reaches the wire as a key?" judgement at review
time. (A signature predicate cannot make that judgement: ``delete(name)``,
``delete_many(keys)``, ``batch(operations)`` and ``sync_to(target)`` all hide
keys in innocently-named parameters.)

Allowlist of keyed methods: ``url_for`` (the guarded ``dol.SupportsUrlFor``
shim, ADR-0011 D3b) — and NOTHING else. To add a capability:
sibling store (D2), ``ObjectHandle`` (key bound at construction), or a free
function taking the store first (D3). Do not grow this inventory with a keyed
method; the comment you are reading is the review gate.
"""

import pytest

from s3dol.base import (
    BucketCollection,
    BucketReader,
    BucketStore,
    EndpointCollection,
    EndpointReader,
    EndpointStore,
    S3Profiles,
)
from s3dol.capabilities import BucketHandles, BucketInfo, BucketUrls

#: The ONLY method allowed to take a key (guarded; see ADR-0011 D3b).
KEYED_METHOD_ALLOWLIST = {"url_for"}

#: Mapping-protocol members `dol` maps correctly at every wrapper depth —
#: the sanctioned keyed surface.
MAPPING_PROTOCOL = {
    "__getitem__",
    "__setitem__",
    "__delitem__",
    "__contains__",
    "__iter__",
    "__len__",
    "__eq__",
    "__ne__",
    "get",
    "keys",
    "values",
    "items",
    "pop",
    "popitem",
    "clear",
    "update",
    "setdefault",
    "head",  # dol.KvReader convenience (first item; not key-taking)
}

#: The full public (non-dunder, non-Mapping) inventory, per class. A test
#: failure here means the public surface changed: review the new attribute
#: against ADR-0011 D1 before extending the inventory.
PUBLIC_INVENTORY = {
    BucketCollection: {"bucket", "connection", "delimiter", "prefix", "reads",
                       "writes", "on_missing_bucket", "strict_delete", "client",
                       "KeysView", "ValuesView", "ItemsView"},
    BucketReader: {"url_for"},
    BucketStore: set(),
    BucketHandles: set(),
    BucketUrls: {"expires_in", "client_method"},
    BucketInfo: set(),
    EndpointCollection: {"connection", "bucket_kwargs", "client",
                         "KeysView", "ValuesView", "ItemsView"},
    EndpointReader: set(),
    EndpointStore: set(),
    S3Profiles: {"bucket_kwargs", "readonly", "KeysView", "ValuesView",
                 "ItemsView"},
}

#: Methods among the inventory that take a key-like argument. Must stay equal
#: to the allowlist.
KEYED_IN_INVENTORY = {"url_for"}


def _public_surface(cls) -> set:
    """Every public attribute the s3dol classes in ``cls``'s MRO define,
    minus the Mapping protocol."""
    names = set()
    for klass in cls.__mro__:
        if klass.__module__.startswith("s3dol"):
            names |= set(vars(klass))
    return {
        name
        for name in names
        if not name.startswith("_") and name not in MAPPING_PROTOCOL
    }


@pytest.mark.parametrize("cls", list(PUBLIC_INVENTORY), ids=lambda c: c.__name__)
def test_public_surface_matches_the_inventory(cls):
    surface = _public_surface(cls)
    all_declared = set()
    for klass, names in PUBLIC_INVENTORY.items():
        if klass in cls.__mro__:
            all_declared |= names
    unexpected = surface - all_declared
    assert not unexpected, (
        f"{cls.__name__} grew public attribute(s) {sorted(unexpected)} not in "
        f"the ADR-0011 D5 inventory. If any takes something that reaches the "
        f"wire as a key — at any argument position, or inside a structure — "
        f"it must become a sibling store, an ObjectHandle method, or a free "
        f"function. Otherwise, add it to PUBLIC_INVENTORY with a reviewer's "
        f"eyes on this message."
    )


def test_the_keyed_allowlist_is_exactly_url_for():
    assert KEYED_IN_INVENTORY == KEYED_METHOD_ALLOWLIST == {"url_for"}


def test_url_for_exists_only_where_declared():
    # BucketReader (and its subclasses) only — never the endpoint level.
    assert "url_for" in vars(BucketReader)
    for cls in (BucketCollection, EndpointCollection, EndpointReader, EndpointStore):
        assert "url_for" not in vars(cls)


def test_object_handle_binds_its_key_at_construction():
    # The handle may be method-rich precisely because no method takes a key.
    import inspect

    from s3dol.base import ObjectHandle

    for name, member in vars(ObjectHandle).items():
        if name.startswith("_") or not callable(member):
            continue
        params = list(inspect.signature(member).parameters)[1:]  # drop self
        assert "k" not in params and "key" not in params, (
            f"ObjectHandle.{name} takes a key parameter — the handle's "
            f"immunity to the delegation trap rests on the key being bound "
            f"at construction (ADR-0011 D1)."
        )


def test_free_functions_take_the_store_first():
    import inspect

    import s3dol.capabilities as capabilities

    for name in ("sub", "prefixes", "delete_many", "handles", "urls", "info"):
        first = next(iter(inspect.signature(getattr(capabilities, name)).parameters))
        assert first in ("store", "endpoint"), (name, first)
    first = next(
        iter(inspect.signature(capabilities.delete_bucket).parameters)
    )
    assert first == "endpoint"
