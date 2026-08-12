"""``S3Connection`` — the credential + endpoint SSOT, and the pure resolver.

Design record: `misc/docs/decisions/0012-credential-and-endpoint-resolution.md`
(all of it) on top of `0002-boto3-as-engine.md` (lazy boto3) and
`0003-provider-presets-and-capabilities.md` (presets, SigV4 mandate).

The two sentences the module follows from (ADR-0012):

1. **A preset is the caller's explicit endpoint, spelled by name.** It sits
   exactly where ``endpoint_url=`` sits — above every environment and
   config-file channel. s3dol owns two rungs of the endpoint ladder, not six.
2. **A ``Config`` value s3dol does not set is a channel it leaves open to the
   user.** Every field is a ladder with a documented bottom, and "the row makes
   no claim" is spelled by *omitting the key*, never by passing ``None``.

Structure:

- :class:`S3Connection` — a frozen, picklable spec. No live objects, no
  escape hatch for one. Construction performs no I/O and imports no boto3.
- :func:`resolve` — a **pure** function ``(spec, environ, aws_config) ->
  Resolution`` where every field carries its provenance. Tier-1 tests cover
  the whole ladder with a fake environ dict and zero I/O.
- :func:`load_aws_config` — the separate, *named impure* loader for the
  aws-config rungs; pass it (or its result) to :func:`resolve`.
- Credential shapes (ADR-0012 D5) — normalised in ``__post_init__`` to small
  frozen, picklable providers; live botocore objects are rejected with
  shape-specific messages.

>>> spec = S3Connection(endpoint_url='http://localhost:9000',
...                     credentials=('AKIAEXAMPLE', 'sEkR3tVALUE'))
>>> res = resolve(spec, {})
>>> res.endpoint_url.value, res.endpoint_url.source
('http://localhost:9000', 'endpoint_url=')
>>> res.signature_version.value  # never left to botocore (ADR-0003 §4)
's3v4'
>>> 'sEkR3tVALUE' in repr(spec)  # secrets never in repr (ADR-0012 D1)
False
"""

from __future__ import annotations

import pickle
import re
import threading
from dataclasses import dataclass, fields
from typing import (
    Any,
    Callable,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Union,
)

from s3dol.errors import (
    AmbiguousResolution,
    ConfigurationError,
    MissingEndpoint,
    MissingPresetParam,
    PresetConflict,
    PresetHostMismatch,
)
from s3dol.presets import (
    Capabilities,
    Preset,
    detect_preset,
    get_preset,
    validate_endpoint_hygiene,
)

# --------------------------------------------------------------------------- #
# Credential shapes (ADR-0012 D5)
# --------------------------------------------------------------------------- #


class CredentialProvider:
    """Base of s3dol's picklable credential specs.

    Subclasses are small frozen dataclasses that *describe* how to obtain
    credentials; nothing live is held (nothing live pickles — measured, ADR-0012
    D5). At client-build time each is adapted into a botocore provider and
    **replaces** botocore's resolver via
    ``session.register_component('credential_provider', ...)`` — never
    prepended (silent fallthrough) and never ``insert_before`` (raises when a
    profile is set).
    """

    def provenance(self) -> str:
        """A label for diagnose() — never a credential value."""
        raise NotImplementedError

    def _load_botocore_credentials(self):
        raise NotImplementedError

    def botocore_provider(self):
        from botocore.credentials import CredentialProvider as _BotocoreBase

        spec = self

        class _Adapter(_BotocoreBase):
            METHOD = f"s3dol-{type(spec).__name__}"
            CANONICAL_NAME = METHOD

            def load(self):
                return spec._load_botocore_credentials()

        return _Adapter()


@dataclass(frozen=True)
class StaticCredentials(CredentialProvider):
    """An explicit key pair (optionally a session token).

    Note: a *static* token expires and cannot refresh — prefer ``profile=`` or
    a callable provider for STS/SSO. Pickling this object carries the secret
    (that is what "explicit credentials" means); its repr does not.
    """

    access_key: str
    secret_key: str
    token: Optional[str] = None

    def provenance(self) -> str:
        return f"explicit static (access key {self.access_key[:4]}…)"

    def __repr__(self):  # secrets never in repr (ADR-0012 D1 §4)
        token = ", token=***" if self.token else ""
        return (
            f"{type(self).__name__}(access_key={self.access_key[:4]!r}…, "
            f"secret_key=***{token})"
        )

    def _load_botocore_credentials(self):
        from botocore.credentials import Credentials

        return Credentials(
            self.access_key, self.secret_key, self.token, method="s3dol-static"
        )


@dataclass(frozen=True)
class ProfileCredentials(CredentialProvider):
    """Credentials from a named profile — *credentials only*; the profile's
    other config is not selected by this (that is what ``profile=`` on the
    connection is for). Rebuilds live, still-refreshable credentials after
    unpickling (a frozen dataclass holding only a profile name round-trips)."""

    profile: str

    def provenance(self) -> str:
        return f"profile:{self.profile} (via credentials=)"

    def _load_botocore_credentials(self):
        import botocore.session

        return botocore.session.Session(profile=self.profile).get_credentials()


@dataclass(frozen=True)
class CallableCredentials(CredentialProvider):
    """A picklable zero-arg callable returning a mapping with keys
    ``access_key``/``secret_key`` (aliases: ``key``/``secret``), optional
    ``token``, optional ``expires_at``/``expiry_time`` (ISO 8601). With an
    expiry, credentials are refreshable — the callable is re-invoked by
    botocore's refresh machinery (advisory 900 s / mandatory 600 s before
    expiry)."""

    fetch: Callable[[], Mapping]

    def _name(self) -> str:
        # NEVER repr(self.fetch): functools.partial's repr prints its bound
        # arguments, and the rejection message below recommends a partial —
        # a partial closing over a vault token would leak it (D1 §4).
        target = getattr(self.fetch, "func", self.fetch)  # unwrap a partial
        return getattr(target, "__qualname__", None) or type(target).__name__

    def __repr__(self):
        return f"{type(self).__name__}(fetch={self._name()})"

    def provenance(self) -> str:
        return f"callable:{self._name()}"

    @staticmethod
    def _normalize(result: Mapping) -> dict:
        def pick(*names):
            for name in names:
                if name in result:
                    return result[name]
            return None

        creds = dict(
            access_key=pick("access_key", "key", "aws_access_key_id"),
            secret_key=pick("secret_key", "secret", "aws_secret_access_key"),
            token=pick("token", "aws_session_token"),
            expiry_time=pick("expires_at", "expiry_time"),
        )
        if not creds["access_key"] or not creds["secret_key"]:
            raise ConfigurationError(
                "The credentials callable must return a mapping with "
                "'access_key' and 'secret_key' (aliases: 'key'/'secret'); got "
                f"keys {sorted(result)}."
            )
        return creds

    def _load_botocore_credentials(self):
        from botocore.credentials import Credentials, RefreshableCredentials

        def refresh():
            fresh = self._normalize(self.fetch())
            return {
                "access_key": fresh["access_key"],
                "secret_key": fresh["secret_key"],
                "token": fresh["token"],
                "expiry_time": fresh["expiry_time"],
            }

        # ONE fetch on the first load — a token vendor may be rate-limited or
        # single-use, so the initial credential is the one we keep.
        first = refresh()
        if first["expiry_time"] is None:
            return Credentials(
                first["access_key"],
                first["secret_key"],
                first["token"],
                method="s3dol-callable",
            )
        return RefreshableCredentials.create_from_metadata(
            metadata=first, refresh_using=refresh, method="s3dol-callable"
        )


CredentialsInput = Union[
    None, str, tuple, list, Mapping, CredentialProvider, Callable[[], Mapping]
]

_AWS_CRED_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")


def _is_live_botocore_object(value: Any) -> bool:
    """Detect live boto3/botocore objects *without importing them* —
    construction must not import boto3 (ADR-0002)."""
    module = type(value).__module__ or ""
    return module.startswith(("botocore", "boto3"))


def normalize_credentials(value: CredentialsInput) -> Optional[CredentialProvider]:
    """Normalise every accepted credential shape; reject the rest, each with
    its own message (ADR-0012 D5). The only door is ``S3Connection.__post_init__``.

    >>> normalize_credentials(None) is None
    True
    >>> normalize_credentials('prod')
    ProfileCredentials(profile='prod')
    >>> normalize_credentials(('AKIAXX', 'sec'))
    StaticCredentials(access_key='AKIA'…, secret_key=***)
    """
    if value is None:
        return None
    if isinstance(value, CredentialProvider):
        return value
    if isinstance(value, type) and issubclass(value, CredentialProvider):
        raise ConfigurationError(
            f"credentials= got the class {value.__name__}; pass an *instance* "
            f"(e.g. {value.__name__}(...))."
        )
    if _is_live_botocore_object(value):
        raise ConfigurationError(
            f"credentials= got a live boto3/botocore object "
            f"({type(value).__name__}). A resolved credential snapshot cannot "
            f"refresh and cannot pickle, and sessions/clients hold sockets. "
            f"Pass a profile name, a (key, secret[, token]) tuple, a mapping of "
            f"aws_* keys, or a picklable zero-arg callable."
        )
    if isinstance(value, str):
        return ProfileCredentials(value)  # a bare str is ALWAYS a profile name
    if isinstance(value, (tuple, list)):
        if len(value) not in (2, 3):
            raise ConfigurationError(
                f"credentials= tuple must be (key, secret) or "
                f"(key, secret, token); got {len(value)} items."
            )
        access, secret, *rest = value
        token = rest[0] if rest else None
        # str()-ing a None token would yield the truthy string 'None' and sign
        # every request with X-Amz-Security-Token: None — the natural
        # `credentials=(k, s, maybe_token)` shape must behave like the 2-tuple.
        return StaticCredentials(
            str(access), str(secret), None if token is None else str(token)
        )
    if isinstance(value, Mapping):
        unknown = set(value) - set(_AWS_CRED_KEYS)
        if unknown:
            raise ConfigurationError(
                f"credentials= mapping has unknown key(s) {sorted(unknown)}; "
                f"allowed: {list(_AWS_CRED_KEYS)}."
            )
        if not value.get("aws_access_key_id") or not value.get("aws_secret_access_key"):
            raise ConfigurationError(
                "credentials= mapping needs both 'aws_access_key_id' and "
                "'aws_secret_access_key' (a token alone leaves "
                "Credentials(None, None, TOKEN) — a measured boto3 hazard)."
            )
        return StaticCredentials(
            access_key=value["aws_access_key_id"],
            secret_key=value["aws_secret_access_key"],
            token=value.get("aws_session_token"),
        )
    if callable(value):
        try:
            pickle.dumps(value)
        except Exception as error:
            target = getattr(value, "func", value)
            name = getattr(target, "__qualname__", None) or type(target).__name__
            raise ConfigurationError(
                f"credentials= callable {name} is not picklable "
                f"({type(error).__name__}). Every s3dol store pickles "
                f"(ADR-0012 D1); use a module-level function or a "
                f"functools.partial of one. (Its repr is deliberately not "
                f"shown — a bound argument may be a secret.)"
            ) from error
        return CallableCredentials(value)
    raise ConfigurationError(
        f"credentials= got a {type(value).__name__}; accepted: None, a profile "
        f"name (str), (key, secret[, token]), a mapping of aws_* keys, a "
        f"CredentialProvider instance, or a picklable zero-arg callable."
    )


# --------------------------------------------------------------------------- #
# Resolution — pure, provenance-carrying (ADR-0012 D2/D3/D4)
# --------------------------------------------------------------------------- #


class Sourced(NamedTuple):
    """A resolved value, where it came from, and whether s3dol passes it to
    botocore explicitly (``passed=False`` == the key is omitted so the channel
    below stays open — "omit, never None")."""

    value: Any
    source: str
    passed: bool = False


@dataclass(frozen=True)
class Resolution:
    """The result of :func:`resolve` — everything ``diagnose()`` prints and
    everything client-build consumes. Carries provenance labels, never a
    credential value."""

    endpoint_url: Sourced
    region_name: Sourced
    signature_version: Sourced
    addressing_style: Sourced
    checksum: Sourced
    payload_signing_enabled: Sourced
    verify: Sourced
    preset: Optional[Preset]
    preset_source: str  # 'named' | 'detected' | 'fallback:<name>' | 'none'
    credential_provenance: str
    capabilities: Capabilities
    #: The non-secret environment keys consulted, with their values.
    environ_consulted: tuple[tuple[str, Optional[str]], ...]
    #: Collected (warning class, message) pairs. resolve() never emits them —
    #: the impure caller decides (client build warns; diagnose prints rows).
    notes: tuple[tuple[type, str], ...]


_ENV_ENDPOINT_KEYS = ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
_ENV_CONSULTED_KEYS = _ENV_ENDPOINT_KEYS + (
    "AWS_DEFAULT_REGION",  # botocore does NOT read AWS_REGION; neither do we
    "AWS_PROFILE",
    "AWS_REQUEST_CHECKSUM_CALCULATION",
    "AWS_RESPONSE_CHECKSUM_VALIDATION",
)

#: Region-shaped tokens, deliberately NARROW: ``us-east-1``, ``fr-par``,
#: ``us-west-004``. A looser pattern (an earlier draft also accepted any
#: ``xxxN`` first label, for hetzner's ``fsn1``) captures ordinary hostname
#: labels — ``srv1.corp``, ``eu-storage.corp`` — and because host capture
#: outranks ``AWS_DEFAULT_REGION`` in the ladder, that garbage would be handed
#: to botocore as the SigV4 credential scope and silently override the user's
#: exported region. Providers whose region is genuinely part of the host state
#: it in their preset row instead (hetzner binds ``{location}``).
_REGION_TOKEN = re.compile(
    r"[a-z]{2}-[a-z]+-\d+|[a-z]{2}-[a-z]{3,}-\d{2,}|[a-z]{2}-[a-z]{3,}"
)


def _region_from_host(endpoint_url: Optional[str]) -> Optional[str]:
    """Best-effort region capture — ONLY from the canonical ``s3.<region>.``
    (or ``s3-<region>.``) host shape, never from an arbitrary label."""
    if not endpoint_url:
        return None
    from urllib.parse import urlsplit

    host = urlsplit(endpoint_url).hostname or ""
    match = re.search(r"(?:^|\.)s3[.-]([a-z0-9-]+)\.", host)
    if match and _REGION_TOKEN.fullmatch(match.group(1)):
        return match.group(1)
    return None


def _looks_secret(name: str) -> bool:
    """Whether an option name plausibly carries a secret (repr scrubbing)."""
    lowered = str(name).lower()
    return any(
        marker in lowered
        for marker in ("secret", "token", "password", "passwd", "credential")
    )


_CREDENTIAL_KEY_MARKERS = (
    "access_key",
    "secret",
    "token",
    "password",
    "credential_process",
    "sso_",
)


def _redact_env_value(key: str, value: Optional[str]) -> Optional[str]:
    """A display-safe form of a consulted environment value."""
    from s3dol.errors import redact_url

    if value and "ENDPOINT" in key:
        return redact_url(value)
    return value


def _scrub_credentials(config: Mapping) -> dict:
    """Recursively drop credential-shaped keys from an aws-config mapping."""

    def scrub(node):
        if isinstance(node, Mapping):
            return {
                k: scrub(v)
                for k, v in node.items()
                if not any(
                    marker in str(k).lower() for marker in _CREDENTIAL_KEY_MARKERS
                )
            }
        return node

    return scrub(config)


def load_aws_config() -> Mapping:
    """The named, *impure* aws-config loader (ADR-0012 D2): loads the shared
    config via botocore (~16 ms — never on the common path; pass this function
    as a thunk and :func:`resolve` calls it only if a rung needs it), scrubbed
    of anything credential-shaped."""
    import botocore.session

    return _scrub_credentials(botocore.session.Session().full_config)


def _profile_section(
    aws_config: Mapping, environ: Mapping, profile: Optional[str]
) -> Mapping:
    profiles = aws_config.get("profiles", {}) if aws_config else {}
    name = profile or environ.get("AWS_PROFILE") or "default"
    return profiles.get(name, {})


def _lower_rung_endpoint(
    environ: Mapping, aws_config: Mapping, profile: Optional[str]
) -> Optional[tuple[str, str]]:
    """The endpoint the rungs *below* s3dol's two would supply — read purely,
    for diagnose() and the C1/C2 guards **only**, never to override (ADR-0012
    D3; and never pass ``Config(ignore_configured_endpoint_urls=True)``)."""
    from s3dol.errors import redact_url

    for env_key in _ENV_ENDPOINT_KEYS:
        value = environ.get(env_key)
        if value:
            # Redact at capture. This rung is never PASSED to botocore (which
            # reads the env itself), only reported — so the Resolution, the C2
            # messages and diagnose all become leak-proof at once, while the
            # host survives for detection and the guards.
            return redact_url(value), f"env:{env_key}"
    if aws_config:
        try:
            from botocore.configprovider import ConfiguredEndpointProvider

            provider = ConfiguredEndpointProvider(
                full_config=aws_config,
                scoped_config=_profile_section(aws_config, environ, profile),
                client_name="s3",
                environ={},  # env rungs already handled above
            )
            value = provider.provide()
            if value:
                return redact_url(str(value)), "aws-config"
        except Exception:  # config parsing must never fail resolution
            return None
    return None


def _row_region(row: Preset) -> Optional[str]:
    """The row's region literal with params bound; ``None`` if it makes no
    claim (absent, or a template with unbound placeholders)."""
    if not row.region_name:
        return None
    value, missing = row._bound(row.region_name)
    return None if missing else value


def resolve(
    spec: "S3Connection",
    environ: Mapping[str, str],
    aws_config: Union[Mapping, Callable[[], Mapping], None] = None,
) -> Resolution:
    """Pure resolution: no ``os.environ``, no disk, no network (ADR-0012 D2).

    ``environ`` is a plain mapping (pass ``os.environ`` at real call sites,
    a dict in tests). ``aws_config`` is a pre-loaded, credential-scrubbed
    config mapping, or a thunk for one (:func:`load_aws_config`) — called only
    if a rung needs it.
    """
    notes: list[tuple[type, str]] = []
    aws_config_cache: dict = {}

    def config() -> Mapping:
        if "value" not in aws_config_cache:
            if aws_config is None:
                aws_config_cache["value"] = {}
            elif callable(aws_config):
                aws_config_cache["value"] = aws_config() or {}
            else:
                aws_config_cache["value"] = aws_config
        return aws_config_cache["value"]

    # -- the named row (pass 1 input)
    if spec.preset is None:
        named = None
    elif isinstance(spec.preset, Preset):
        named = spec.preset
    else:
        named = get_preset(spec.preset)

    # -- endpoint ladder (D3): s3dol owns two rungs, then stops
    endpoint: Optional[Sourced] = None
    if spec.endpoint_url:
        endpoint = Sourced(spec.endpoint_url, "endpoint_url=", passed=True)
    elif named is not None:
        bound = named.bound_endpoint(region_name=spec.region_name)
        if bound is not None:
            validate_endpoint_hygiene(bound, what=f"preset:{named.name} endpoint")
            endpoint = Sourced(bound, f"preset:{named.name}", passed=True)

    lower = None
    if endpoint is None:
        lower = _lower_rung_endpoint(environ, config(), spec.profile)
        if lower is not None:
            endpoint = Sourced(lower[0], lower[1], passed=False)
        elif named is not None:
            missing = named.missing_endpoint_params(region_name=spec.region_name)
            if missing:
                raise MissingPresetParam(
                    f"Preset {named.name!r} has unbound endpoint template "
                    f"parameter(s) {sorted(missing)}, so it supplies no "
                    f"endpoint — and nothing below supplied one either. Bind "
                    f"them (preset=get_preset({named.name!r}).bind(...)), or "
                    f"pass endpoint_url=. Falling back to AWS silently is "
                    f"structurally impossible (ADR-0012 C1)."
                )
            if named.requires_endpoint:
                raise MissingEndpoint(
                    f"Preset {named.name!r} cannot be reached without an "
                    f"explicit endpoint and nothing supplied one. Pass "
                    f"endpoint_url=, or set AWS_ENDPOINT_URL_S3 (ADR-0012 C1)."
                )
            endpoint = Sourced(None, "SDK resolver", passed=False)
        else:
            endpoint = Sourced(None, "SDK resolver", passed=False)

    # -- C2 guard: a named, patterned row whose endpoint arrived from below
    # ADR-0012 C2 exempts no *named* row: `soft` governs detection (pass 2),
    # not this guard. Exempting 'aws' would silence it for exactly the
    # population that declared "I want AWS" and was then retargeted elsewhere
    # by an environment variable.
    if (
        named is not None
        and named.host_patterns
        and not endpoint.passed
        and endpoint.value
    ):
        if not named.matches_host(endpoint.value):
            other = detect_preset(endpoint.value)
            if other is not None and other.name != named.name:
                raise PresetHostMismatch(
                    f"preset={named.name!r} was named, but the endpoint that "
                    f"resolved from the environment ({endpoint.source}) matches "
                    f"{other.name!r}'s host patterns: {endpoint.value!r}. "
                    f"Pass endpoint_url= explicitly (rung 1 never triggers "
                    f"this guard), or name the right preset."
                )
            notes.append(
                (
                    AmbiguousResolution,
                    f"preset={named.name!r} was named, but the endpoint that "
                    f"resolved from the environment ({endpoint.source}: "
                    f"{endpoint.value!r}) matches none of its host patterns "
                    f"{named.host_patterns}. Proceeding; pass endpoint_url= to "
                    f"silence this.",
                )
            )

    # -- pass 2: pick the settings row (named > detected > fallback)
    if named is not None:
        row, preset_source = named, "named"
    elif endpoint.value:
        detected = detect_preset(endpoint.value)
        if detected is not None:
            row, preset_source = detected, "detected"
        else:
            row, preset_source = get_preset("generic-s3"), "fallback:generic-s3"
    else:
        row, preset_source = get_preset("aws"), "fallback:aws"

    # -- region ladder
    row_region = _row_region(row)
    if spec.region_name:
        if row.pinned and row_region and spec.region_name != row_region:
            raise PresetConflict(
                f"region_name={spec.region_name!r} contradicts the verified "
                f"preset {row.name!r}, which pins region {row_region!r}."
            )
        region = Sourced(spec.region_name, "region_name=", passed=True)
    elif row_region:
        region = Sourced(row_region, f"preset:{row.name}", passed=True)
    else:
        captured = _region_from_host(endpoint.value)
        env_region = environ.get("AWS_DEFAULT_REGION")
        if captured:
            region = Sourced(captured, "endpoint host (best-effort)", passed=True)
        elif env_region:
            region = Sourced(env_region, "env:AWS_DEFAULT_REGION", passed=True)
        elif _profile_section(config(), environ, spec.profile).get("region"):
            region = Sourced(
                _profile_section(config(), environ, spec.profile)["region"],
                "aws-config profile",
                passed=True,
            )
        else:
            region = Sourced("us-east-1", "default", passed=True)

    # -- addressing style ladder: kwarg > row > profile > omit.
    #    Soft rows (aws, generic-s3) document the expected behaviour but do NOT
    #    pass it — omitting the key preserves the profile channel and botocore's
    #    endpoint-dependent default (ADR-0012 D3: omit, never None).
    profile_addressing = (
        _profile_section(config(), environ, spec.profile).get("s3", {}) or {}
    ).get("addressing_style")
    if spec.addressing_style:
        addressing = Sourced(spec.addressing_style, "addressing_style=", passed=True)
    elif not row.soft:
        addressing = Sourced(row.addressing_style, f"preset:{row.name}", passed=True)
    elif profile_addressing:
        addressing = Sourced(profile_addressing, "aws-config profile", passed=False)
    else:
        addressing = Sourced(
            row.addressing_style,
            f"botocore default (≈{row.addressing_style})",
            passed=False,
        )

    # -- checksum ladder: kwarg > row > env > botocore default. The env and
    #    default rungs OMIT the keys, so a user's AWS_REQUEST_CHECKSUM_* env
    #    workaround keeps working (measured: Config beats env both ways).
    env_checksum = environ.get("AWS_REQUEST_CHECKSUM_CALCULATION") or environ.get(
        "AWS_RESPONSE_CHECKSUM_VALIDATION"
    )
    if spec.checksum:
        checksum = Sourced(spec.checksum, "checksum=", passed=True)
    elif row.checksum:
        checksum = Sourced(row.checksum, f"preset:{row.name}", passed=True)
    elif env_checksum:
        checksum = Sourced(env_checksum, "env:AWS_*_CHECKSUM_*", passed=False)
    else:
        checksum = Sourced("when_supported", "botocore default", passed=False)

    # -- payload signing: kwarg > row > omit (no env channel exists)
    if spec.payload_signing_enabled is not None:
        payload_signing = Sourced(
            spec.payload_signing_enabled, "payload_signing_enabled=", passed=True
        )
    elif row.payload_signing_enabled is not None:
        payload_signing = Sourced(
            row.payload_signing_enabled, f"preset:{row.name}", passed=True
        )
    else:
        payload_signing = Sourced(None, "omitted", passed=False)

    # -- signature version: kwarg > 's3v4'; never from a row, never from env,
    #    never left to botocore (which silently downgrades to SigV2 in the 12
    #    regions endpoints.json lists as v2-capable — ADR-0003 §4 / ADR-0012).
    signature = Sourced(
        spec.signature_version,
        "signature_version=" if spec.signature_version != "s3v4" else "s3dol default",
        passed=True,
    )

    verify = Sourced(
        spec.verify,
        "verify=" if spec.verify is not None else "omitted",
        passed=spec.verify is not None,
    )

    # -- credential provenance label (never a value)
    if spec.anon:
        provenance = "anonymous (UNSIGNED)"
    elif spec.credentials is not None:
        provenance = spec.credentials.provenance()
    elif spec.profile:
        provenance = f"profile:{spec.profile}"
    elif environ.get("AWS_ACCESS_KEY_ID"):
        provenance = "chain (env credentials present)"
    else:
        provenance = "chain (config file / SSO / IMDS / none)"

    return Resolution(
        endpoint_url=endpoint,
        region_name=region,
        signature_version=signature,
        addressing_style=addressing,
        checksum=checksum,
        payload_signing_enabled=payload_signing,
        verify=verify,
        preset=row,
        preset_source=preset_source,
        credential_provenance=provenance,
        capabilities=row.capabilities,
        # Redacted at capture: these are display values (diagnose prints them,
        # and a Resolution is the natural thing to log), and an endpoint env
        # var may carry userinfo we neither refuse nor pass on.
        environ_consulted=tuple(
            (
                key,
                _redact_env_value(key, environ.get(key)),
            )
            for key in _ENV_CONSULTED_KEYS
        ),
        notes=tuple(notes),
    )


# --------------------------------------------------------------------------- #
# S3Connection (ADR-0012 D1)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class S3Connection:
    """A frozen, picklable connection spec. No live objects — ever.

    Construction performs no I/O and does not import boto3 (ADR-0002); the
    client is built lazily, once, under a per-instance lock (CPython ≥3.12
    removed ``cached_property``'s lock deliberately — measured: 8 racing
    threads → 8 clients without one).

    ``__post_init__`` is the only door: every normalisation and contradiction
    check happens here, so a bad shape dies at construction with a message,
    not later inside botocore.

    >>> S3Connection(anon=True, profile='prod')
    Traceback (most recent call last):
    ...
    s3dol.errors.ConfigurationError: ...
    """

    # WHERE
    preset: Union[str, Preset, None] = None
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None
    # WHO
    profile: Optional[str] = None
    credentials: Optional[CredentialProvider] = None  # normalised in __post_init__
    anon: bool = False
    # HOW (None == "do not override" — the ladder continues; see resolve())
    signature_version: str = "s3v4"  # never left to botocore (ADR-0003 §4)
    addressing_style: Optional[Literal["path", "virtual"]] = None
    checksum: Optional[Literal["when_supported", "when_required"]] = None
    payload_signing_enabled: Optional[bool] = None
    verify: Union[bool, str, None] = None  # self-signed MinIO / corporate CA
    client_kwargs: tuple = ()
    # POLICY
    deny_means_absent: bool = False  # ADR-0004 §3

    def __post_init__(self):
        object.__setattr__(self, "credentials", normalize_credentials(self.credentials))
        if isinstance(self.preset, str):
            get_preset(self.preset)  # unknown names die here, loudly
        if self.endpoint_url is not None:
            validate_endpoint_hygiene(self.endpoint_url)
        if self.addressing_style not in (None, "path", "virtual"):
            raise ConfigurationError(
                f"addressing_style must be 'path', 'virtual', or None (ladder "
                f"continues); got {self.addressing_style!r}. 'auto' is illegal."
            )
        if self.checksum not in (None, "when_supported", "when_required"):
            raise ConfigurationError(
                f"checksum must be 'when_supported', 'when_required', or None; "
                f"got {self.checksum!r}."
            )
        if not isinstance(self.signature_version, str) or not self.signature_version:
            # NOT a "None == do not override" field, unlike its neighbours:
            # ADR-0003 §4 requires signature_version to be set explicitly on
            # every client. Leaving it to botocore silently downgrades
            # presigning to SigV2 in the regions endpoints.json still lists as
            # v2-capable (us-east-1 among them — every conventional MinIO
            # setup), and client.meta.config reports 's3v4' while doing it.
            raise ConfigurationError(
                f"signature_version must be a non-empty str (default 's3v4'); "
                f"got {self.signature_version!r}. It is never left to botocore: "
                f"that silently re-enables SigV2 presigning (ADR-0003 §4, "
                f"issue #10). Pass anon=True for unsigned access."
            )
        if isinstance(self.client_kwargs, Mapping):
            object.__setattr__(
                self, "client_kwargs", tuple(sorted(self.client_kwargs.items()))
            )
        smuggled = sorted(
            key
            for key, _ in self.client_kwargs
            if key
            in (
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "config",
            )
        )
        if smuggled:
            raise ConfigurationError(
                f"client_kwargs must not carry {smuggled}: boto3 would accept "
                f"them, bypassing credential normalisation, the anon "
                f"contradiction check, and repr redaction (the secret would "
                f"then print in every log line that reprs this spec). Use "
                f"credentials=/profile=, and the connection's own fields for "
                f"Config (addressing_style, checksum, signature_version, "
                f"payload_signing_enabled, verify)."
            )
        if self.anon:
            if self.credentials is not None or self.profile:
                raise ConfigurationError(
                    "anon=True with credentials/profile: UNSIGNED would "
                    "silently discard them (measured — the request signer's "
                    "credentials become None). Drop anon, or drop the "
                    "credentials."
                )
            if self.deny_means_absent:
                raise ConfigurationError(
                    "anon=True with deny_means_absent=True: an anonymous HEAD "
                    "gives 403 on existing keys and 404 on missing ones, so "
                    "every key would read as absent — a silently empty store "
                    "(measured under moto; banned by ADR-0001 goal 4)."
                )
        object.__setattr__(self, "_lock", threading.Lock())

    # -- the client -------------------------------------------------------- #

    @property
    def client(self):
        """The boto3 S3 client — built lazily, once, under the lock."""
        built = self.__dict__.get("_client")
        if built is None:
            with self._lock:
                built = self.__dict__.get("_client")
                if built is None:
                    built = self._build_client()
                    object.__setattr__(self, "_client", built)
        return built

    def resolution(
        self,
        environ: Optional[Mapping[str, str]] = None,
        aws_config: Union[Mapping, Callable, None] = load_aws_config,
    ) -> Resolution:
        """Resolve against the real environment by default (a thin impure
        wrapper over the pure :func:`resolve`)."""
        import os

        return resolve(self, environ if environ is not None else os.environ, aws_config)

    def _build_client(
        self, *, _override_s3_settings: Optional[dict] = None, _override_endpoint=None
    ):
        import warnings

        import boto3
        import botocore.session
        from botocore import UNSIGNED
        from botocore.config import Config
        from botocore.credentials import CredentialResolver

        resolution = self.resolution()
        for warning_class, message in resolution.notes:
            warnings.warn(message, warning_class, stacklevel=4)

        s3_settings: dict = {}
        if resolution.addressing_style.passed:
            s3_settings["addressing_style"] = resolution.addressing_style.value
        if resolution.payload_signing_enabled.passed:
            s3_settings["payload_signing_enabled"] = (
                resolution.payload_signing_enabled.value
            )
        if _override_s3_settings:
            s3_settings.update(_override_s3_settings)
        config_kwargs: dict = dict(resolution.preset.config_kwargs or ())
        if resolution.checksum.passed:
            config_kwargs["request_checksum_calculation"] = resolution.checksum.value
            config_kwargs["response_checksum_validation"] = resolution.checksum.value
        if s3_settings:
            config_kwargs["s3"] = s3_settings
        config = Config(
            # UNSIGNED never enters the dataclass (unpicklable) — build-time only
            signature_version=(
                UNSIGNED if self.anon else resolution.signature_version.value
            ),
            **config_kwargs,
        )

        botocore_session = (
            botocore.session.Session(profile=self.profile)
            if self.profile
            else botocore.session.Session()
        )
        if self.credentials is not None:
            botocore_session.register_component(
                "credential_provider",
                CredentialResolver(providers=[self.credentials.botocore_provider()]),
            )

        client_kwargs = dict(self.client_kwargs)
        if self.verify is not None:
            client_kwargs["verify"] = self.verify
        endpoint_to_pass = (
            resolution.endpoint_url.value if resolution.endpoint_url.passed else None
        )
        if _override_endpoint is not None:
            endpoint_to_pass = _override_endpoint
        return boto3.Session(botocore_session=botocore_session).client(
            "s3",
            endpoint_url=endpoint_to_pass,
            region_name=resolution.region_name.value,
            config=config,
            **client_kwargs,
        )

    @property
    def presign_client(self):
        """The client presigned URLs are generated with.

        For most providers this is :attr:`client`. Two providers genuinely
        need different config for presigning than for the API (ADR-0003 §1):
        Hetzner presigns virtual-hosted while the API is path-style; R2
        presigns only on the S3 API domain. When the resolved preset carries
        ``presign_endpoint_url``/``presign_addressing_style``, a second client
        is built (lazily, once, under the same lock) from the merged config.
        """
        resolution = self.resolution()
        row = resolution.preset
        if row is None or (
            row.presign_endpoint_url is None and row.presign_addressing_style is None
        ):
            return self.client
        built = self.__dict__.get("_presign_client")
        if built is None:
            with self._lock:
                built = self.__dict__.get("_presign_client")
                if built is None:
                    overrides = {}
                    if row.presign_addressing_style is not None:
                        overrides["addressing_style"] = row.presign_addressing_style
                    endpoint = row.presign_endpoint_url
                    built = self._build_client(
                        _override_s3_settings=overrides,
                        _override_endpoint=endpoint,
                    )
                    object.__setattr__(self, "_presign_client", built)
        return built

    # -- pickling: mandatory for every connection (ADR-0012 D1 §3) --------- #

    def __getstate__(self):
        # Enumerating dataclass fields drops every cache by construction
        # (the boto3 client is unpicklable, full stop).
        return {f.name: getattr(self, f.name) for f in fields(self)}

    def __setstate__(self, state):
        for name, value in state.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_lock", threading.Lock())

    # -- repr: non-default fields only; secrets never (D1 §4) -------------- #

    def __repr__(self):
        from s3dol.errors import redact_url

        shown = []
        for f in fields(self):
            value = getattr(self, f.name)
            if value == f.default:
                continue  # only non-defaults, so the repr stays readable
            if f.name == "client_kwargs":
                value = tuple(
                    (key, "***" if _looks_secret(key) else item) for key, item in value
                )
            elif f.name == "endpoint_url":
                value = redact_url(value)
            shown.append(f"{f.name}={value!r}")
        return f"{type(self).__name__}({', '.join(shown)})"
