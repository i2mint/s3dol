"""Tier-1 tests for the preset registry (ADR-0003 as amended by ADR-0012 D4)."""

import pytest

from s3dol.errors import ConfigurationError, InvalidEndpoint, UnknownPresetParam
from s3dol.presets import (
    CONFIG_KWARGS_ALLOWLIST,
    Capabilities,
    Preset,
    _REGISTRY,
    available_presets,
    detect_preset,
    get_preset,
    register,
    unregister,
    validate_endpoint_hygiene,
)


@pytest.fixture
def scratch_registry():
    """Snapshot/restore the registry so tests can register freely."""
    snapshot = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(snapshot)


# --------------------------------------------------------------------------- #
# row validation
# --------------------------------------------------------------------------- #


def test_addressing_style_is_mandatory_and_auto_is_illegal():
    with pytest.raises(TypeError):
        Preset(name="x")  # no default: every row must state it
    with pytest.raises(ConfigurationError, match="auto"):
        Preset(name="x", addressing_style="auto")


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://user:pw@s3.example.com",  # userinfo: the credential carrier
        "https://s3.example.com?token=abc",  # query
        "https://s3.example.com#frag",  # fragment
    ],
)
def test_endpoint_hygiene(bad_url):
    with pytest.raises(InvalidEndpoint):
        validate_endpoint_hygiene(bad_url)
    with pytest.raises(InvalidEndpoint):
        Preset(name="x", addressing_style="path", endpoint_url=bad_url)


def test_config_kwargs_allowlist_is_closed():
    with pytest.raises(ConfigurationError, match="allowlisted"):
        Preset(
            name="x",
            addressing_style="path",
            config_kwargs=(("proxies", "http://user:pw@proxy"),),
        )
    with pytest.raises(ConfigurationError, match="scalar"):
        Preset(
            name="x",
            addressing_style="path",
            config_kwargs=(("connect_timeout", {"nested": 1}),),
        )
    ok = Preset(
        name="x", addressing_style="path", config_kwargs=(("connect_timeout", 5),)
    )
    assert dict(ok.config_kwargs)["connect_timeout"] == 5
    assert "proxies" not in CONFIG_KWARGS_ALLOWLIST


def test_unknown_bound_param_raises():
    with pytest.raises(UnknownPresetParam):
        Preset(
            name="x",
            addressing_style="path",
            endpoint_url="https://{a}.example.com",
            params=(("b", "1"),),
        )


# --------------------------------------------------------------------------- #
# template binding (C1's raw material)
# --------------------------------------------------------------------------- #


def test_bind_and_bound_endpoint():
    r2 = get_preset("r2")
    assert r2.bound_endpoint() is None  # unbound template -> no rung
    assert r2.missing_endpoint_params() == {"account_id"}
    bound = r2.bind(account_id="abc123")
    assert bound.bound_endpoint() == "https://abc123.r2.cloudflarestorage.com"
    assert bound.missing_endpoint_params() == set()


def test_bind_unknown_param_raises():
    with pytest.raises(UnknownPresetParam, match="account_id"):
        get_preset("r2").bind(nonsense="x")


def test_region_kwarg_binds_region_template():
    scaleway = get_preset("scaleway")
    assert scaleway.bound_endpoint() is None
    assert scaleway.bound_endpoint(region_name="fr-par") == (
        "https://s3.fr-par.scw.cloud"
    )


# --------------------------------------------------------------------------- #
# pinned is derived, gated on verified (dormant in v1.0)
# --------------------------------------------------------------------------- #


def test_pinned_is_dormant_while_doc_sourced():
    row = Preset(
        name="x", addressing_style="path", region_name="eu-west-1", verified=False
    )
    assert row.pinned is False
    verified = Preset(
        name="x", addressing_style="path", region_name="eu-west-1", verified=True
    )
    assert verified.pinned is True


def test_templated_region_never_pins():
    row = Preset(
        name="x", addressing_style="path", region_name="{location}", verified=True
    )
    assert row.pinned is False


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #


def test_register_refuses_silent_overwrite(scratch_registry):
    row = Preset(name="mine", addressing_style="path")
    register(row)
    with pytest.raises(ConfigurationError, match="already registered"):
        register(row)
    register(row, overwrite=True)  # explicit is fine
    unregister("mine")
    assert "mine" not in available_presets()


def test_get_preset_unknown_names_the_registry():
    with pytest.raises(ConfigurationError) as info:
        get_preset("no-such-provider")
    assert "register" in str(info.value).lower()
    assert "aws" in str(info.value)  # the row list is in the message


# --------------------------------------------------------------------------- #
# detection (resolution pass 2)
# --------------------------------------------------------------------------- #


def test_detection_by_host_pattern():
    detected = detect_preset("https://abc123.r2.cloudflarestorage.com")
    assert detected is not None and detected.name == "r2"
    assert detect_preset("https://storage.googleapis.com").name == "gcs"
    assert detect_preset("https://myref.storage.supabase.co/storage/v1/s3").name == (
        "supabase"
    )


def test_soft_rows_never_win_detection():
    # aws is a soft fallback; detection yielding None routes the caller to the
    # generic-s3/aws fallback logic instead.
    assert detect_preset("https://s3.eu-central-1.amazonaws.com") is None
    assert detect_preset("https://unknown.example.com") is None


def test_ambiguous_detection_raises(scratch_registry):
    register(
        Preset(
            name="clone-a", addressing_style="path", host_patterns=("*.clone.test",)
        )
    )
    register(
        Preset(
            name="clone-b", addressing_style="path", host_patterns=("*.clone.test",)
        )
    )
    with pytest.raises(ConfigurationError, match="more than one preset"):
        detect_preset("https://x.clone.test")


# --------------------------------------------------------------------------- #
# builtin rows sanity
# --------------------------------------------------------------------------- #


def test_every_builtin_row_is_well_formed():
    for name in available_presets():
        row = get_preset(name)
        assert row.addressing_style in ("path", "virtual")
        assert row.verified is False  # doc-sourced until proven live


def test_the_load_bearing_rows():
    assert get_preset("backblaze").checksum == "when_required"  # mandatory
    assert get_preset("supabase").checksum == "when_required"  # the §3 fix
    assert get_preset("supabase").error_overrides  # 400-means-404
    assert get_preset("hetzner").payload_signing_enabled is False
    assert get_preset("hetzner").presign_addressing_style == "virtual"
    assert get_preset("scaleway").capabilities.max_multipart_parts == 1000
    gcs = get_preset("gcs").capabilities
    assert gcs.list_objects_v2 is False and gcs.batch_delete is False
    assert get_preset("minio").requires_endpoint is True
    assert get_preset("ceph").requires_endpoint is True
    assert get_preset("r2").region_name == "auto"  # R2's literal region string
    for soft_name in ("aws", "generic-s3"):
        assert get_preset(soft_name).soft is True


def test_capabilities_defaults_are_aws_reference_semantics():
    caps = Capabilities()
    assert caps.list_objects_v2 and caps.batch_delete and caps.multipart
    assert caps.max_multipart_parts == 10_000
    assert caps.consistency == "strong"
