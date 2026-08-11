"""Provider presets: config rows, not subclasses.

Design record: `misc/docs/decisions/0003-provider-presets-and-capabilities.md`
(the survey of 16 backends, the checksum analysis, capabilities policy) amended
by `misc/docs/decisions/0012-credential-and-endpoint-resolution.md` §D4
(``params``/``host_patterns``/``requires_endpoint``/``soft``/``config_kwargs``,
derived ``pinned``; ``signature_version`` and ``client_kwargs`` moved to the
connection; ``addressing_style`` mandatory with ``'auto'`` illegal; the
structural no-credential rule).

A provider divergence is a row in this registry — never a subclass, never an
endpoint-substring branch in the hot path. Users register their own rows:

>>> row = Preset(name='mycorp', addressing_style='path',
...              endpoint_url='https://s3.mycorp.example', requires_endpoint=False)
>>> register(row)
>>> get_preset('mycorp').endpoint_url
'https://s3.mycorp.example'
>>> unregister('mycorp')  # (cleanup for this doctest)

Most builtin rows are **doc-sourced, not verified against a live endpoint** —
they say so via ``verified=False``. Wrong values mostly fail loudly, which is
the acceptable failure mode; ``pinned`` (which makes a row's region literal
conflict-raising) is derived and *gated on* ``verified``, so it is dormant
until a row earns evidence (ADR-0012 D4).
"""

from __future__ import annotations

import string
from dataclasses import dataclass, replace
from fnmatch import fnmatch
from typing import Any, Literal, Optional
from urllib.parse import urlsplit

from s3dol.errors import (
    ConfigurationError,
    InvalidEndpoint,
    Rule,
    UnknownPresetParam,
)

# --------------------------------------------------------------------------- #
# Capabilities
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Capabilities:
    """What a backend supports. A *static declared table*, never a probe
    (a probe costs a round-trip, is wrong under partial permissions, and can't
    be trusted anyway — ADR-0003 §2). Missing capabilities are handled
    per-capability: emulate when exact (batch delete -> loop), substitute when
    equivalent (ListObjectsV2 -> V1), raise ``NotSupported`` when there is no
    honest fallback. Never silently degrade correctness."""

    list_objects_v2: bool = True
    batch_delete: bool = True
    presigned_post: bool = True
    multipart: bool = True
    max_multipart_parts: int = 10_000
    min_part_size: int = 5 * 2**20
    object_tagging: bool = True
    versioning: bool = True
    conditional_writes: bool = True
    consistency: Literal["strong", "read-after-write", "eventual"] = "strong"


DEFAULT_CAPABILITIES = Capabilities()


# --------------------------------------------------------------------------- #
# Preset
# --------------------------------------------------------------------------- #

#: ``Config``-constructor kwargs a preset row may carry. Scalars only, and
#: ``proxies``/``proxies_config``/``client_context_params`` are structurally
#: excluded — a preset must not be able to smuggle a credential-bearing proxy
#: URL (ADR-0012 D4).
CONFIG_KWARGS_ALLOWLIST = frozenset(
    {"connect_timeout", "read_timeout", "max_pool_connections", "tcp_keepalive"}
)

_SCALAR_TYPES = (str, int, float, bool, type(None))


def _template_params(template: str) -> set:
    """Placeholder names in a ``str.format``-style template.

    >>> sorted(_template_params('https://{account_id}.r2.example/{x}'))
    ['account_id', 'x']
    """
    return {
        name for _, name, _, _ in string.Formatter().parse(template) if name is not None
    }


def validate_endpoint_hygiene(url: str, *, what: str = "endpoint_url") -> None:
    """Refuse credential-carrying URL shapes: userinfo in the netloc, a query
    string, or a fragment (ADR-0012 D4 — a query-string endpoint is accepted by
    botocore and retained verbatim in ``client.meta.endpoint_url``, so it would
    leak into ``diagnose()`` and tracebacks).

    >>> validate_endpoint_hygiene('https://s3.example.com')
    >>> validate_endpoint_hygiene('https://user:pw@s3.example.com')
    Traceback (most recent call last):
    ...
    s3dol.errors.InvalidEndpoint: ...
    """
    parts = urlsplit(url)
    if "@" in parts.netloc:
        raise InvalidEndpoint(
            f"{what} must not carry userinfo (user[:password]@host): got a URL "
            f"with '@' in its netloc. Pass credentials via "
            f"S3Connection(credentials=...), never in a URL."
        )
    # The refusal messages deliberately do NOT echo the URL: its query/
    # fragment is exactly the secret carrier this check exists to stop, and an
    # exception message lands in tracebacks, logs and diagnose reports.
    if parts.query:
        raise InvalidEndpoint(
            f"{what} must not carry a query string (host "
            f"{parts.hostname!r}): the query is where signed-URL and SAS-style "
            f"credentials live, and botocore would retain it verbatim in "
            f"client.meta.endpoint_url. Pass the bare endpoint."
        )
    if parts.fragment:
        raise InvalidEndpoint(
            f"{what} must not carry a fragment (host {parts.hostname!r})."
        )


@dataclass(frozen=True)
class Preset:
    """One provider's facts. Public, shareable, committable config — which is
    why there is **no credential slot**, structurally (ADR-0012 D4).

    ``endpoint_url``/``region_name`` may contain ``{placeholders}``; bind them
    with :meth:`bind`. A row whose endpoint template has an unbound *required*
    placeholder contributes no endpoint rung (ADR-0012 C1).
    """

    name: str
    #: Mandatory; no default; ``'auto'`` is illegal (ADR-0012 D4).
    addressing_style: Literal["path", "virtual"]
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None
    #: ``None`` == the row makes no claim (the ladder continues — ADR-0012 D3).
    checksum: Optional[Literal["when_supported", "when_required"]] = None
    payload_signing_enabled: Optional[bool] = None
    capabilities: Capabilities = DEFAULT_CAPABILITIES
    #: Presigning sometimes genuinely differs from the API config (Hetzner
    #: presigns virtual while the API is path; R2 presigns only on the S3 API
    #: domain) — ADR-0003 §1.
    presign_endpoint_url: Optional[str] = None
    presign_addressing_style: Optional[Literal["path", "virtual"]] = None
    #: Bound template parameters, e.g. ``(('account_id', 'abc'),)``.
    params: tuple[tuple[str, str], ...] = ()
    #: ``fnmatch`` patterns over the endpoint *hostname*; used for detection
    #: (pass 2) and the C2 named-vs-resolved guard.
    host_patterns: tuple[str, ...] = ()
    #: The provider cannot be reached without an explicit endpoint.
    requires_endpoint: bool = False
    #: Soft rows are last-resort fallbacks (``aws``, ``generic-s3``): they
    #: never win detection over a non-soft row and never trigger the C2 guard.
    soft: bool = False
    #: Extra ``botocore.Config`` kwargs — allowlisted, scalars only.
    config_kwargs: tuple[tuple[str, Any], ...] = ()
    #: Error-classification overrides, consulted before the default table
    #: (ADR-0004 §2/§6): rows of ``((operation, code, status), kind)``.
    error_overrides: tuple[Rule, ...] = ()
    #: ``True`` only when the row was verified against a live endpoint (with a
    #: date in the row's docstring/comment). Builtin rows are doc-sourced.
    verified: bool = False

    @property
    def pinned(self) -> bool:
        """Whether the row's region literal is authoritative enough to make a
        conflicting explicit ``region_name=`` raise ``PresetConflict``.
        Derived, and gated on ``verified`` — dormant while rows are
        doc-sourced (ADR-0012 D4)."""
        return (
            self.verified
            and self.region_name is not None
            and not _template_params(self.region_name)
        )

    def __post_init__(self):
        if self.addressing_style not in ("path", "virtual"):
            raise ConfigurationError(
                f"Preset {self.name!r}: addressing_style must be 'path' or "
                f"'virtual' (got {self.addressing_style!r}). 'auto' is illegal: "
                f"every row must state what the provider actually needs "
                f"(ADR-0012 D4)."
            )
        for url_field in ("endpoint_url", "presign_endpoint_url"):
            url = getattr(self, url_field)
            if url is not None:
                validate_endpoint_hygiene(url, what=f"Preset.{url_field}")
        for key, value in self.config_kwargs:
            if key not in CONFIG_KWARGS_ALLOWLIST:
                raise ConfigurationError(
                    f"Preset {self.name!r}: config_kwargs key {key!r} is not "
                    f"allowlisted (allowed: {sorted(CONFIG_KWARGS_ALLOWLIST)}). "
                    f"This list is closed to keep presets credential-free."
                )
            if not isinstance(value, _SCALAR_TYPES):
                raise ConfigurationError(
                    f"Preset {self.name!r}: config_kwargs values must be "
                    f"scalars; {key!r} is a {type(value).__name__}."
                )
        from s3dol.errors import KINDS

        for rule in self.error_overrides:
            try:
                (_operation, _code, _status), kind = rule
            except (TypeError, ValueError):
                raise ConfigurationError(
                    f"Preset {self.name!r}: each error_overrides row is "
                    f"((operation, code, status), kind); got {rule!r}."
                ) from None
            if kind not in KINDS:
                raise ConfigurationError(
                    f"Preset {self.name!r}: error_overrides kind {kind!r} is "
                    f"not one of {list(KINDS)}. An unvalidated kind would "
                    f"detonate inside the error-translation seam at request "
                    f"time, masking the real backend error."
                )
        unknown = set(dict(self.params)) - self.template_params()
        if unknown:
            raise UnknownPresetParam(
                f"Preset {self.name!r}: bound params {sorted(unknown)} appear in "
                f"no template. Templates use: {sorted(self.template_params())}."
            )

    def template_params(self) -> set:
        """All placeholder names across this row's templates."""
        names = set()
        for template in (
            self.endpoint_url,
            self.region_name,
            self.presign_endpoint_url,
        ):
            if template:
                names |= _template_params(template)
        return names

    def bind(self, **params: str) -> "Preset":
        """A copy of this row with template ``params`` bound.

        >>> r2 = get_preset('r2').bind(account_id='abc123')
        >>> r2.bound_endpoint()
        'https://abc123.r2.cloudflarestorage.com'
        """
        unknown = set(params) - self.template_params()
        if unknown:
            raise UnknownPresetParam(
                f"Preset {self.name!r} has no template parameter(s) "
                f"{sorted(unknown)}; it uses {sorted(self.template_params())}."
            )
        merged = {**dict(self.params), **{k: str(v) for k, v in params.items()}}
        return replace(self, params=tuple(sorted(merged.items())))

    def _bound(self, template: Optional[str], *, extra: Optional[dict] = None):
        """``(value, missing_params)`` for a template under current bindings."""
        if not template:
            return None, set()
        bindings = {**dict(self.params), **(extra or {})}
        missing = _template_params(template) - set(bindings)
        if missing:
            return None, missing
        return template.format(**bindings), set()

    def bound_endpoint(self, *, region_name: Optional[str] = None) -> Optional[str]:
        """The endpoint URL with params bound, or ``None`` if the template has
        unbound placeholders (C1: an unbound row contributes no rung —
        ``preset='minio'`` silently becoming AWS is structurally impossible).

        ``region_name`` binds a ``{region_name}`` placeholder, per the region
        ladder's first rung (ADR-0012 D3)."""
        extra = {"region_name": region_name} if region_name else None
        value, _missing = self._bound(self.endpoint_url, extra=extra)
        return value

    def missing_endpoint_params(self, *, region_name: Optional[str] = None) -> set:
        """Which placeholders keep :meth:`bound_endpoint` from producing a URL."""
        extra = {"region_name": region_name} if region_name else None
        _value, missing = self._bound(self.endpoint_url, extra=extra)
        return missing

    def matches_host(self, endpoint_url: str) -> bool:
        """Whether ``endpoint_url``'s hostname matches this row's patterns."""
        host = urlsplit(endpoint_url).hostname or ""
        return any(fnmatch(host, pattern) for pattern in self.host_patterns)


# --------------------------------------------------------------------------- #
# The registry
# --------------------------------------------------------------------------- #

_REGISTRY: dict[str, Preset] = {}


def register(preset: Preset, *, overwrite: bool = False) -> None:
    """Add a row. Open-closed: adding a provider is adding a row."""
    if not overwrite and preset.name in _REGISTRY:
        raise ConfigurationError(
            f"A preset named {preset.name!r} is already registered. "
            f"Pass overwrite=True to replace it."
        )
    _REGISTRY[preset.name] = preset


def unregister(name: str) -> None:
    """Remove a row (mostly for tests)."""
    _REGISTRY.pop(name, None)


def get_preset(name: str) -> Preset:
    """Look a row up by name; unknown names raise, naming the registry."""
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ConfigurationError(
            f"Unknown preset {name!r}. Registered: {sorted(_REGISTRY)}. "
            f"Register your own with s3dol.presets.register(Preset(...))."
        ) from None


def available_presets() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


def detect_preset(endpoint_url: str) -> Optional[Preset]:
    """Detection (resolution pass 2): the non-soft row whose ``host_patterns``
    match the resolved endpoint's host. Two matching rows is genuinely
    ambiguous and raises; zero yields ``None`` (the caller falls back to
    ``generic-s3``/``aws`` — ADR-0012 D4)."""
    matches = [
        p for p in _REGISTRY.values() if not p.soft and p.matches_host(endpoint_url)
    ]
    if len(matches) > 1:
        names = sorted(p.name for p in matches)
        raise ConfigurationError(
            f"Endpoint {endpoint_url!r} matches more than one preset's host "
            f"patterns: {names}. Name one explicitly with preset=<name>."
        )
    return matches[0] if matches else None


# --------------------------------------------------------------------------- #
# Builtin rows (ADR-0003 §1's table; doc-sourced — verified=False throughout)
# --------------------------------------------------------------------------- #

_SUPABASE_ERROR_OVERRIDES: tuple[Rule, ...] = (
    # Supabase returns 400 where AWS returns 404 on HeadBucket (ADR-0004 §2).
    (("HeadBucket", None, 400), "bucket_absent"),
)

_BUILTIN_PRESETS = (
    # AWS: the reference semantics. SDK-resolved endpoint; soft fallback row.
    Preset(
        name="aws",
        addressing_style="virtual",
        soft=True,
        host_patterns=("*.amazonaws.com",),
    ),
    # Fallback for "some endpoint resolved, nothing matched": makes no claim
    # beyond path addressing (botocore's own behaviour for custom endpoints),
    # so it is byte-identical to today for endpoint-passing callers.
    Preset(name="generic-s3", addressing_style="path", soft=True),
    Preset(
        name="minio",
        addressing_style="path",
        region_name="us-east-1",  # conventional
        requires_endpoint=True,
    ),
    Preset(
        name="r2",
        addressing_style="virtual",
        endpoint_url="https://{account_id}.r2.cloudflarestorage.com",
        region_name="auto",  # R2's literal region string
        host_patterns=("*.r2.cloudflarestorage.com",),
        # Presign only on the S3 API domain, never a custom domain (docs).
    ),
    Preset(
        name="scaleway",
        addressing_style="virtual",
        endpoint_url="https://s3.{region_name}.scw.cloud",
        host_patterns=("s3.*.scw.cloud",),
        capabilities=Capabilities(max_multipart_parts=1000),
    ),
    Preset(
        name="hetzner",
        addressing_style="path",  # virtual breaks plain Get/Put (TLS cert)
        presign_addressing_style="virtual",  # docs: needed for presigned URLs
        endpoint_url="https://{location}.your-objectstorage.com",
        region_name="{location}",  # must repeat the location
        payload_signing_enabled=False,
        host_patterns=("*.your-objectstorage.com",),
    ),
    Preset(
        name="backblaze",
        addressing_style="virtual",
        endpoint_url="https://s3.{region_name}.backblazeb2.com",
        checksum="when_required",  # mandatory: rejects default SDK checksums
        host_patterns=("s3.*.backblazeb2.com",),
        capabilities=Capabilities(presigned_post=False),
    ),
    Preset(
        name="wasabi",
        addressing_style="path",  # the vendor's own advice
        endpoint_url="https://s3.{region_name}.wasabisys.com",
        host_patterns=("s3.*.wasabisys.com",),
    ),
    Preset(
        name="gcs",
        addressing_style="virtual",
        endpoint_url="https://storage.googleapis.com",
        host_patterns=("storage.googleapis.com",),
        capabilities=Capabilities(list_objects_v2=False, batch_delete=False),
    ),
    Preset(
        name="supabase",
        addressing_style="path",  # forced
        endpoint_url="https://{project_ref}.storage.supabase.co/storage/v1/s3",
        checksum="when_required",  # the ADR-0003 §3 corruption fix
        host_patterns=("*.supabase.co", "*.supabase.in"),
        error_overrides=_SUPABASE_ERROR_OVERRIDES,
    ),
    Preset(
        name="digitalocean",
        addressing_style="virtual",
        endpoint_url="https://{region_name}.digitaloceanspaces.com",
        host_patterns=("*.digitaloceanspaces.com",),
    ),
    Preset(
        name="tigris",
        addressing_style="virtual",
        endpoint_url="https://fly.storage.tigris.dev",
        host_patterns=("fly.storage.tigris.dev", "*.storage.tigris.dev"),
    ),
    Preset(
        name="ceph",
        addressing_style="path",
        requires_endpoint=True,  # behaviour varies more by release than vendor
    ),
    # Test doubles
    Preset(
        name="localstack",
        addressing_style="path",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
    ),
    Preset(
        name="moto",
        addressing_style="path",
        endpoint_url="http://localhost:5000",
        region_name="us-east-1",
    ),
)

for _preset in _BUILTIN_PRESETS:
    register(_preset)
del _preset
