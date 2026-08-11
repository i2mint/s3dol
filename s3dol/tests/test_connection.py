"""Tests for ``S3Connection`` and the pure resolver (ADR-0012).

Tier 1 throughout (fake environ dicts, zero I/O), plus a small moto tier-2
section at the bottom proving the built client's presigning is SigV4 — the
regression test for issue #10, at the layer that fixes it.
"""

import pickle
import threading

import pytest

from s3dol.connection import (
    CallableCredentials,
    CredentialProvider,
    ProfileCredentials,
    Resolution,
    S3Connection,
    StaticCredentials,
    load_aws_config,
    normalize_credentials,
    resolve,
)
from s3dol.errors import (
    AmbiguousResolution,
    ConfigurationError,
    MissingEndpoint,
    PresetHostMismatch,
)
from s3dol.presets import get_preset


def fetch_test_credentials():
    """Module-level (picklable) credential callable for tests."""
    return {"access_key": "AKIATEST", "secret_key": "sEkR3t"}


# --------------------------------------------------------------------------- #
# credential normalisation (D5)
# --------------------------------------------------------------------------- #


def test_none_stays_none_meaning_the_chain():
    assert normalize_credentials(None) is None


def test_bare_str_is_always_a_profile_name():
    provider = normalize_credentials("prod")
    assert isinstance(provider, ProfileCredentials) and provider.profile == "prod"


@pytest.mark.parametrize("shape", [("AK", "SK"), ("AK", "SK", "TOK"), ["AK", "SK"]])
def test_tuple_shapes(shape):
    provider = normalize_credentials(shape)
    assert isinstance(provider, StaticCredentials)
    assert provider.access_key == "AK" and provider.secret_key == "SK"


def test_tuple_wrong_arity_rejected():
    with pytest.raises(ConfigurationError, match="tuple"):
        normalize_credentials(("only-one",))


def test_mapping_of_aws_keys():
    provider = normalize_credentials(
        {"aws_access_key_id": "AK", "aws_secret_access_key": "SK"}
    )
    assert isinstance(provider, StaticCredentials)


def test_mapping_unknown_key_rejected_with_the_allowed_list():
    with pytest.raises(ConfigurationError, match="aws_access_key_id"):
        normalize_credentials({"access_key": "AK"})


def test_token_only_mapping_rejected():
    # boto3 would leave Credentials(None, None, TOKEN) — a measured hazard.
    with pytest.raises(ConfigurationError, match="token"):
        normalize_credentials({"aws_session_token": "TOK"})


def test_provider_instance_passes_through():
    provider = StaticCredentials("AK", "SK")
    assert normalize_credentials(provider) is provider


def test_provider_class_rejected():
    with pytest.raises(ConfigurationError, match="instance"):
        normalize_credentials(StaticCredentials)


def test_live_botocore_object_rejected():
    from botocore.credentials import Credentials

    with pytest.raises(ConfigurationError, match="live"):
        normalize_credentials(Credentials("AK", "SK"))


def test_unpicklable_callable_rejected():
    with pytest.raises(ConfigurationError, match="picklable"):
        normalize_credentials(lambda: {"access_key": "a", "secret_key": "b"})


def test_picklable_callable_accepted():
    provider = normalize_credentials(fetch_test_credentials)
    assert isinstance(provider, CallableCredentials)
    assert isinstance(provider, CredentialProvider)


def test_alien_type_rejected():
    with pytest.raises(ConfigurationError, match="accepted"):
        normalize_credentials(42)


def test_static_credentials_repr_redacts():
    provider = StaticCredentials("AKIAEXAMPLE", "sEkR3tVALUE", "tOkEnVALUE")
    text = repr(provider)
    assert "sEkR3tVALUE" not in text and "tOkEnVALUE" not in text
    assert "AKIA" in text  # identity by prefix only


def test_callable_credentials_normalize_aliases_and_reject_garbage():
    assert CallableCredentials._normalize({"key": "a", "secret": "b"})["access_key"] == "a"
    with pytest.raises(ConfigurationError, match="access_key"):
        CallableCredentials._normalize({"nope": 1})


# --------------------------------------------------------------------------- #
# S3Connection construction (D1)
# --------------------------------------------------------------------------- #


def test_construction_is_lazy_no_client_no_io():
    spec = S3Connection(endpoint_url="http://localhost:9000")
    assert "_client" not in spec.__dict__


def test_anon_with_credentials_raises():
    with pytest.raises(ConfigurationError, match="anon"):
        S3Connection(anon=True, credentials=("AK", "SK"))
    with pytest.raises(ConfigurationError, match="anon"):
        S3Connection(anon=True, profile="prod")


def test_anon_with_deny_means_absent_raises():
    with pytest.raises(ConfigurationError, match="silently empty"):
        S3Connection(anon=True, deny_means_absent=True)


def test_auto_addressing_style_is_illegal():
    with pytest.raises(ConfigurationError, match="auto"):
        S3Connection(addressing_style="auto")


def test_bad_checksum_rejected():
    with pytest.raises(ConfigurationError, match="checksum"):
        S3Connection(checksum="always")


def test_unknown_preset_name_dies_at_construction():
    with pytest.raises(ConfigurationError, match="Unknown preset"):
        S3Connection(preset="no-such-provider")


def test_endpoint_hygiene_applies_to_the_kwarg():
    with pytest.raises(ConfigurationError):
        S3Connection(endpoint_url="https://user:pw@host")


def test_client_kwargs_mapping_normalised_to_tuple():
    spec = S3Connection(client_kwargs={"use_ssl": False})
    assert spec.client_kwargs == (("use_ssl", False),)
    hash(spec)  # stays hashable


def test_equal_specs_hash_equal_so_caches_are_safe():
    a = S3Connection(endpoint_url="http://x:1", credentials=("AK", "SK"))
    b = S3Connection(endpoint_url="http://x:1", credentials=("AK", "SK"))
    assert a == b and hash(a) == hash(b)
    assert a != S3Connection(endpoint_url="http://x:2", credentials=("AK", "SK"))


def test_repr_shows_only_non_defaults_and_no_secrets():
    spec = S3Connection(
        endpoint_url="http://x:1", credentials=("AKIAEXAMPLE", "sEkR3tVALUE")
    )
    text = repr(spec)
    assert "sEkR3tVALUE" not in text
    assert "http://x:1" in text
    assert "deny_means_absent" not in text  # default fields stay quiet


# --------------------------------------------------------------------------- #
# pickling (D1 §3: mandatory for every connection)
# --------------------------------------------------------------------------- #


def test_pickle_round_trip_preserves_equality():
    spec = S3Connection(
        preset="r2",
        credentials=("AK", "SK", "TOK"),
        region_name="auto",
        client_kwargs={"use_ssl": True},
    )
    clone = pickle.loads(pickle.dumps(spec))
    assert clone == spec
    assert isinstance(clone._lock, type(threading.Lock()))


def test_pickle_drops_a_cached_client():
    spec = S3Connection(endpoint_url="http://x:1")
    object.__setattr__(spec, "_client", object())  # simulate a built client
    clone = pickle.loads(pickle.dumps(spec))
    assert "_client" not in clone.__dict__


def test_callable_credentials_pickle():
    spec = S3Connection(credentials=fetch_test_credentials)
    clone = pickle.loads(pickle.dumps(spec))
    assert isinstance(clone.credentials, CallableCredentials)


# --------------------------------------------------------------------------- #
# the client is built once, under the lock (D1 §2)
# --------------------------------------------------------------------------- #


def test_client_build_races_produce_one_client(monkeypatch):
    builds = []
    barrier = threading.Barrier(8)

    def fake_build(self):
        builds.append(1)
        return object()

    monkeypatch.setattr(S3Connection, "_build_client", fake_build)
    spec = S3Connection(endpoint_url="http://x:1")
    clients = []

    def grab():
        barrier.wait()
        clients.append(spec.client)

    threads = [threading.Thread(target=grab) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(builds) == 1
    assert len({id(c) for c in clients}) == 1


# --------------------------------------------------------------------------- #
# resolve(): purity and the ladders (D2/D3/D4)
# --------------------------------------------------------------------------- #


def test_resolve_is_pure_wrt_the_process_environment(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://should-not-be-seen:1")
    resolution = resolve(S3Connection(), {})
    assert resolution.endpoint_url.value is None
    assert resolution.endpoint_url.source == "SDK resolver"


def test_endpoint_rung1_kwarg_wins_over_everything():
    spec = S3Connection(
        endpoint_url="http://explicit:1", preset=get_preset("r2").bind(account_id="a")
    )
    resolution = resolve(spec, {"AWS_ENDPOINT_URL_S3": "http://env:1"})
    assert resolution.endpoint_url == ("http://explicit:1", "endpoint_url=", True)


def test_endpoint_rung2_bound_preset():
    spec = S3Connection(preset=get_preset("r2").bind(account_id="abc"))
    resolution = resolve(spec, {})
    assert resolution.endpoint_url.value == "https://abc.r2.cloudflarestorage.com"
    assert resolution.endpoint_url.source == "preset:r2"
    assert resolution.endpoint_url.passed is True


def test_endpoint_env_rungs_are_reported_not_passed():
    resolution = resolve(S3Connection(), {"AWS_ENDPOINT_URL_S3": "http://env:1"})
    assert resolution.endpoint_url.value == "http://env:1"
    assert resolution.endpoint_url.source == "env:AWS_ENDPOINT_URL_S3"
    assert resolution.endpoint_url.passed is False  # botocore reads it itself
    lower = resolve(S3Connection(), {"AWS_ENDPOINT_URL": "http://gen:1"})
    assert lower.endpoint_url.source == "env:AWS_ENDPOINT_URL"
    priority = resolve(
        S3Connection(),
        {"AWS_ENDPOINT_URL_S3": "http://svc:1", "AWS_ENDPOINT_URL": "http://gen:1"},
    )
    assert priority.endpoint_url.value == "http://svc:1"  # service beats generic


def test_endpoint_aws_config_rung():
    aws_config = {"profiles": {"default": {"endpoint_url": "http://cfg:1"}}}
    resolution = resolve(S3Connection(), {}, aws_config)
    assert resolution.endpoint_url.value == "http://cfg:1"
    assert resolution.endpoint_url.source == "aws-config"
    assert resolution.endpoint_url.passed is False


def test_c1_requires_endpoint_raises_missing_endpoint():
    with pytest.raises(MissingEndpoint) as info:
        resolve(S3Connection(preset="minio"), {})
    assert "AWS_ENDPOINT_URL_S3" in str(info.value)


def test_c1_unbound_template_raises_and_names_the_params():
    with pytest.raises(MissingEndpoint) as info:
        resolve(S3Connection(preset="r2"), {})
    assert "account_id" in str(info.value)


def test_c1_env_endpoint_satisfies_a_requiring_preset():
    resolution = resolve(
        S3Connection(preset="minio"), {"AWS_ENDPOINT_URL_S3": "http://minio:9000"}
    )
    assert resolution.endpoint_url.value == "http://minio:9000"
    assert resolution.preset.name == "minio"


def test_c2_env_endpoint_matching_another_provider_raises():
    with pytest.raises(PresetHostMismatch, match="wasabi"):
        resolve(
            S3Connection(preset="r2"),
            {"AWS_ENDPOINT_URL_S3": "https://s3.eu-central-1.wasabisys.com"},
        )


def test_c2_env_endpoint_matching_nothing_warns_not_raises():
    resolution = resolve(
        S3Connection(preset="r2"),
        {"AWS_ENDPOINT_URL_S3": "https://proxy.mycorp.internal"},
    )
    assert any(cls is AmbiguousResolution for cls, _ in resolution.notes)


def test_c2_never_fires_for_rung1():
    # endpoint_url= is the documented escape (rung 1 never triggers C2).
    resolution = resolve(
        S3Connection(preset="r2", endpoint_url="https://proxy.mycorp.internal"), {}
    )
    assert resolution.notes == ()


def test_pass2_detection_supplies_the_full_row():
    resolution = resolve(
        S3Connection(endpoint_url="https://abc.r2.cloudflarestorage.com"), {}
    )
    assert resolution.preset.name == "r2"
    assert resolution.preset_source == "detected"
    assert resolution.addressing_style.value == "virtual"
    assert resolution.region_name.value == "auto"  # the row's literal


def test_pass2_fallbacks():
    with_endpoint = resolve(S3Connection(endpoint_url="http://unknown:1"), {})
    assert with_endpoint.preset.name == "generic-s3"
    assert with_endpoint.preset_source == "fallback:generic-s3"
    without = resolve(S3Connection(), {})
    assert without.preset.name == "aws"
    assert without.preset_source == "fallback:aws"


def test_region_ladder():
    kwarg = resolve(S3Connection(region_name="eu-west-3"), {})
    assert kwarg.region_name == ("eu-west-3", "region_name=", True)
    captured = resolve(
        S3Connection(endpoint_url="https://s3.eu-central-2.amazonaws.com"), {}
    )
    assert captured.region_name.value == "eu-central-2"
    assert "endpoint host" in captured.region_name.source
    env = resolve(S3Connection(), {"AWS_DEFAULT_REGION": "ap-south-1"})
    assert env.region_name == ("ap-south-1", "env:AWS_DEFAULT_REGION", True)
    profile = resolve(
        S3Connection(), {}, {"profiles": {"default": {"region": "sa-east-1"}}}
    )
    assert profile.region_name.value == "sa-east-1"
    bottom = resolve(S3Connection(), {})
    assert bottom.region_name == ("us-east-1", "default", True)


def test_aws_region_env_var_is_deliberately_ignored():
    # botocore does not read AWS_REGION; v1 must not "helpfully" add it.
    resolution = resolve(S3Connection(), {"AWS_REGION": "eu-north-1"})
    assert resolution.region_name.value == "us-east-1"


def test_addressing_ladder_soft_rows_omit():
    hard = resolve(S3Connection(endpoint_url="https://storage.googleapis.com"), {})
    assert hard.addressing_style.passed is True  # gcs is a real claim
    soft = resolve(S3Connection(), {})  # aws fallback: omit, channel open
    assert soft.addressing_style.passed is False
    profile = resolve(
        S3Connection(),
        {},
        {"profiles": {"default": {"s3": {"addressing_style": "virtual"}}}},
    )
    assert profile.addressing_style.value == "virtual"
    assert profile.addressing_style.source == "aws-config profile"
    assert profile.addressing_style.passed is False  # omit preserves the channel
    kwarg = resolve(S3Connection(addressing_style="path"), {})
    assert kwarg.addressing_style == ("path", "addressing_style=", True)


def test_checksum_ladder_env_stays_open():
    row = resolve(
        S3Connection(preset=get_preset("backblaze").bind()),
        {"AWS_ENDPOINT_URL_S3": "https://s3.us-west-004.backblazeb2.com"},
    )
    assert row.checksum == ("when_required", "preset:backblaze", True)
    env = resolve(
        S3Connection(), {"AWS_REQUEST_CHECKSUM_CALCULATION": "when_required"}
    )
    assert env.checksum.value == "when_required"
    assert env.checksum.passed is False  # omit the key: env keeps working
    bottom = resolve(S3Connection(), {})
    assert bottom.checksum == ("when_supported", "botocore default", False)
    kwarg = resolve(S3Connection(checksum="when_required"), {})
    assert kwarg.checksum.passed is True


def test_payload_signing_ladder():
    hetzner = resolve(
        S3Connection(preset="hetzner", endpoint_url="https://fsn1.your-objectstorage.com"),
        {},
    )
    assert hetzner.payload_signing_enabled == (False, "preset:hetzner", True)
    default = resolve(S3Connection(), {})
    assert default.payload_signing_enabled == (None, "omitted", False)


def test_signature_version_always_passed():
    resolution = resolve(S3Connection(), {})
    assert resolution.signature_version.value == "s3v4"
    assert resolution.signature_version.passed is True


def test_credential_provenance_labels_never_values():
    explicit = resolve(S3Connection(credentials=("AKIAXYZ", "sEkR3t")), {})
    assert "sEkR3t" not in str(explicit.credential_provenance)
    assert "AKIA" in explicit.credential_provenance
    profile = resolve(S3Connection(profile="prod"), {})
    assert profile.credential_provenance == "profile:prod"
    env = resolve(S3Connection(), {"AWS_ACCESS_KEY_ID": "AK"})
    assert "env" in env.credential_provenance
    anon = resolve(S3Connection(anon=True), {})
    assert "anonymous" in anon.credential_provenance


def test_resolution_is_a_value_and_consults_named_env_keys():
    resolution = resolve(S3Connection(), {"AWS_DEFAULT_REGION": "eu-west-1"})
    assert isinstance(resolution, Resolution)
    consulted = dict(resolution.environ_consulted)
    assert consulted["AWS_DEFAULT_REGION"] == "eu-west-1"
    assert "AWS_ENDPOINT_URL_S3" in consulted


def test_load_aws_config_is_credential_scrubbed():
    config = load_aws_config()  # impure by design; must still be safe to print
    flat = repr(config).lower()
    assert "secret" not in flat
    assert "aws_access_key_id" not in flat
    assert "session_token" not in flat


# --------------------------------------------------------------------------- #
# tier 2 (moto): the built client — SigV4 presigning is issue #10's regression
# --------------------------------------------------------------------------- #

moto = pytest.importorskip("moto")


@pytest.fixture
def _mock_aws_env(monkeypatch):
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_client_presigns_sigv4_even_in_us_east_1(_mock_aws_env):
    # us-east-1 is precisely a region where botocore, left to itself, silently
    # downgrades presigning to SigV2 (ADR-0003 §4). Assert on the URL, never on
    # client.meta.config.signature_version — it reports 's3v4' while emitting
    # SigV2 (measured).
    with moto.mock_aws():
        spec = S3Connection(region_name="us-east-1")
        client = spec.client
        client.create_bucket(Bucket="test-bucket")
        client.put_object(Bucket="test-bucket", Key="k", Body=b"v")
        url = client.generate_presigned_url(
            "get_object", Params={"Bucket": "test-bucket", "Key": "k"}, ExpiresIn=60
        )
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
    assert "AWSAccessKeyId" not in url  # the SigV2 tell


def test_anon_client_is_unsigned_and_still_builds(_mock_aws_env):
    from botocore import UNSIGNED

    with moto.mock_aws():
        spec = S3Connection(anon=True, region_name="us-east-1")
        assert spec.client.meta.config.signature_version is UNSIGNED


def test_static_credentials_reach_the_request(_mock_aws_env):
    with moto.mock_aws():
        spec = S3Connection(
            credentials=("AKIAINJECTED", "sEkR3t"), region_name="us-east-1"
        )
        client = spec.client
        client.create_bucket(Bucket="cred-bucket")
        url = client.generate_presigned_url(
            "get_object", Params={"Bucket": "cred-bucket", "Key": "k"}, ExpiresIn=60
        )
    assert "AKIAINJECTED" in url  # the injected identity signed, not the env one
