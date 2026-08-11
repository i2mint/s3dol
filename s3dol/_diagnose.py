"""``s3dol.diagnose()`` — print what would resolve, from where, and whether v1
moves it. The step-0 migration safety mechanism (ADR-0007 §5, ADR-0012 D6/D7).

Run it with the same arguments you pass ``s3dol.store.S3Store`` today::

    import s3dol
    s3dol.diagnose(bucket_name='my-bucket', endpoint_url='...', path='logs')

Non-negotiables (ADR-0012 D6):

- it **never raises** — a failed resolution is a *row*; this function exists to
  report exactly the misconfigurations that raise;
- it holds **no credential value**, only a provenance label; it never reads
  ``.access_key`` (which can trigger a network refresh); credential identity is
  discussed as a predicate, never a snapshot.

The v0-vs-v1 divergence table is a **differential, not a frozen simulation**
(ADR-0012 D7): one ~20-line argument-mangler (:func:`_v0_mangle`, mapping 1:1
onto v0's ``get_client``/``_find_default_credentials``) is run through v1's own
resolver twice, so resolver imprecision cancels. It dies in the same commit as
the compat shim.
"""

from __future__ import annotations

import io
import re
import sys
from typing import Mapping, Optional

from s3dol.connection import S3Connection, Resolution, load_aws_config, resolve
from s3dol.errors import redact

#: v0 ``S3Store`` parameter names (the shim signature, ADR-0007 §3 — including
#: ``is_supabase_endpoint``, which v0 accepts and an early shim draft forgot).
_V0_ARG_NAMES = frozenset(
    {
        "bucket_name",
        "make_bucket",
        "path",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "endpoint_url",
        "region_name",
        "profile_name",
        "skip_bucket_check",
        "is_supabase_endpoint",
    }
)

_SECRET_ARG_NAMES = frozenset({"aws_secret_access_key", "aws_session_token"})

#: Reasons a v0 branch is excluded from the differential (ADR-0012 D7): these
#: two branches build clients through the module-level ``boto3.client(...)``,
#: which resolves through the process-global cached DEFAULT_SESSION — they are
#: not functions of their arguments.
_EXCLUDED_BRANCHES = {
    "supabase": (
        "v0's Supabase branch builds its client through boto3's process-global "
        "session, so its behaviour is not a function of these arguments. Note: "
        "a long-lived v0 process that builds one Supabase store pins its "
        "credentials for the life of the interpreter."
    ),
    "skip-bucket-check": (
        "v0's skip_bucket_check branch builds its client through boto3's "
        "process-global session, so its behaviour is not a function of these "
        "arguments."
    ),
}


def _v0_mangle(args: Mapping, environ: Mapping) -> tuple[str, Optional[dict]]:
    """What v0's ``S3Store`` *effectively* does with these arguments.

    ~20 frozen lines mapping 1:1 onto v0 (``store.py`` + ``base.py:get_client``
    / ``_find_default_credentials``). Returns ``(branch, effective_args)``;
    ``effective_args=None`` means the branch is excluded (see
    :data:`_EXCLUDED_BRANCHES`).
    """
    endpoint = args.get("endpoint_url")
    is_supabase = args.get("is_supabase_endpoint")
    if is_supabase is None:
        is_supabase = bool(endpoint and ".supabase." in endpoint)
    if is_supabase:
        return "supabase", None
    if args.get("skip_bucket_check"):
        return "skip-bucket-check", None
    effective = dict(args)
    profile = args.get("profile_name")
    env_creds = bool(
        environ.get("AWS_ACCESS_KEY_ID") and environ.get("AWS_SECRET_ACCESS_KEY")
    )
    if profile is None and env_creds:
        # get_client(None) -> _find_default_credentials -> the env branch:
        # env creds OVERRIDE explicit kwargs ({**kwargs, **env}), env's (maybe
        # unset) token/region NULL explicit ones, and endpoint_url is DROPPED
        # (never forwarded to get_client('environment variables', ...)).
        effective.update(
            aws_access_key_id=environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=environ.get("AWS_SESSION_TOKEN"),
            region_name=environ.get("AWS_DEFAULT_REGION"),
            endpoint_url=None,
        )
        return "env-credentials (endpoint dropped)", effective
    if profile == "environment variables":
        effective.update(
            aws_access_key_id=environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=environ.get("AWS_SESSION_TOKEN"),
            region_name=environ.get("AWS_DEFAULT_REGION"),
        )
        return "env-profile", effective
    if profile is not None and any(
        args.get(k)
        for k in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")
    ):
        # boto3: profile + any explicit credential kwarg silently discards the
        # profile's credentials (measured, ADR-0012 D5).
        return "profile+explicit-creds (profile credentials discarded)", effective
    return ("profile" if profile else "explicit"), effective


def _spec_from_v0_args(args: Mapping) -> S3Connection:
    """The v1 spec equivalent of a v0 ``S3Store`` call (connection axes only)."""
    access = args.get("aws_access_key_id")
    secret = args.get("aws_secret_access_key")
    token = args.get("aws_session_token")
    credentials = None
    if access and secret:
        credentials = (access, secret, token) if token else (access, secret)
    profile = args.get("profile_name")
    if profile == "environment variables":
        profile = None  # env credentials are the top of botocore's own chain
    return S3Connection(
        endpoint_url=args.get("endpoint_url") or None,
        region_name=args.get("region_name") or None,
        profile=profile,
        credentials=credentials,
    )


def _redact_url(url: Optional[str]) -> Optional[str]:
    """Redact query secrets and any userinfo from a URL for display."""
    if url is None:
        return None
    return re.sub(r"://[^@/]+@", "://REDACTED@", redact(url))


def _field_rows(resolution: Resolution):
    for name in (
        "endpoint_url",
        "region_name",
        "signature_version",
        "addressing_style",
        "checksum",
        "payload_signing_enabled",
        "verify",
    ):
        sourced = getattr(resolution, name)
        value = sourced.value
        if name == "endpoint_url":
            value = _redact_url(value)
        handed = "passed" if sourced.passed else "omitted (channel open)"
        yield name, value, sourced.source, handed


def _divergence_rows(args: Mapping, environ: Mapping, aws_config) -> list[str]:
    branch, effective = _v0_mangle(args, environ)
    if effective is None:
        return [
            f"v0 branch: {branch} — EXCLUDED from the differential.",
            _EXCLUDED_BRANCHES[branch],
        ]
    rows = [f"v0 branch: {branch}"]
    v1_res = resolve(_spec_from_v0_args(args), environ, aws_config)
    v0_res = resolve(_spec_from_v0_args(effective), environ, aws_config)
    moved = False
    for axis in ("endpoint_url", "region_name"):
        v0_value = getattr(v0_res, axis).value
        v1_value = getattr(v1_res, axis).value
        if v0_value != v1_value:
            moved = True
            rows.append(
                f"  {axis}: v0 -> {_redact_url(v0_value)!r}, "
                f"v1 -> {_redact_url(v1_value)!r}   ** CHANGES ON UPGRADE **"
            )
    if v0_res.credential_provenance != v1_res.credential_provenance:
        moved = True
        rows.append(
            f"  credentials: v0 -> {v0_res.credential_provenance}, "
            f"v1 -> {v1_res.credential_provenance}   ** CHANGES ON UPGRADE **"
        )
    if not moved:
        rows.append("  no divergence: v1 resolves this call exactly as v0 does.")
    return rows


def diagnose(
    connection: Optional[S3Connection] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
    file=None,
    **kwargs,
) -> str:
    """Print (and return) the resolution report. Never raises.

    Call it either with a ready :class:`~s3dol.connection.S3Connection`, with
    ``S3Connection`` kwargs, or — the migration case — with exactly the kwargs
    you pass ``s3dol.store.S3Store`` today (``bucket_name=``, ``path=``,
    ``profile_name=``, ...), which additionally prints the v0-vs-v1
    divergence table.

    >>> report = diagnose(endpoint_url='http://localhost:9000',
    ...                   environ={}, file=io.StringIO())
    >>> 'endpoint_url' in report and 'resolution' in report.lower()
    True
    """
    out = io.StringIO()

    def emit(line=""):
        print(line, file=out)

    try:
        import importlib.metadata

        try:
            own_version = importlib.metadata.version("s3dol")
        except Exception:
            own_version = "unknown"
        try:
            import botocore

            botocore_version = botocore.__version__
        except Exception:
            botocore_version = "NOT IMPORTABLE"
        emit("s3dol diagnose")
        emit("=" * 60)
        emit(
            f"s3dol {own_version} | botocore {botocore_version} | "
            f"python {sys.version.split()[0]}"
        )

        if environ is None:
            import os

            environ = dict(os.environ)
        aws_config: object
        try:
            aws_config = load_aws_config()
        except Exception as error:
            aws_config = {}
            emit(f"note: aws config unreadable ({type(error).__name__}: {error})")

        v0_args = {k: v for k, v in kwargs.items() if k in _V0_ARG_NAMES}
        v1_kwargs = {k: v for k, v in kwargs.items() if k not in _V0_ARG_NAMES}
        v0_mode = bool(v0_args) and not v1_kwargs and connection is None

        if kwargs:
            emit()
            emit("arguments:")
            for key in sorted(kwargs):
                shown = "<provided>" if key in _SECRET_ARG_NAMES else repr(kwargs[key])
                emit(f"  {key} = {shown}")

        emit()
        emit("resolution:")
        try:
            if connection is None:
                if v0_mode:
                    spec = _spec_from_v0_args(v0_args)
                else:
                    spec = S3Connection(**{**v0_args, **v1_kwargs})
            else:
                if kwargs:
                    emit("note: connection= given; extra kwargs ignored.")
                spec = connection
            resolution = resolve(spec, environ, aws_config)
        except Exception as error:
            emit(f"  RESOLUTION FAILS: {type(error).__name__}: {error}")
            emit()
            emit("(That failure is the diagnosis: the same arguments would")
            emit(" raise at first use. Fix the row above and re-run.)")
            return _finish(out, file)
        emit(
            f"  preset: {resolution.preset.name!r} ({resolution.preset_source})"
            + ("" if resolution.preset.verified else "  [doc-sourced row]")
        )
        for name, value, source, handed in _field_rows(resolution):
            emit(f"  {name:<24} = {value!r:<28} [{source}; {handed}]")
        emit(f"  credentials: {resolution.credential_provenance}")
        emit(f"  can_presign: {not spec.anon}")
        emit()
        emit("environment consulted (non-secret keys only):")
        for key, value in resolution.environ_consulted:
            emit(f"  {key} = {_redact_url(value)!r}")
        for warning_class, message in resolution.notes:
            emit(f"warning [{warning_class.__name__}]: {message}")

        if v0_mode:
            emit()
            emit("v0 (S3Store) vs v1 divergence:")
            for row in _divergence_rows(v0_args, environ, aws_config):
                emit(f"  {row}")
    except Exception as error:  # never raises — D6
        emit(
            f"diagnose itself hit an unexpected error: {type(error).__name__}: {error}"
        )
        emit("Please report this at https://github.com/i2mint/s3dol/issues")
    return _finish(out, file)


def _finish(out: io.StringIO, file) -> str:
    text = out.getvalue()
    print(text, file=file if file is not None else sys.stdout, end="")
    return text
