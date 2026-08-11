"""The s3dol error taxonomy and the single error-translation seam.

Design record: `misc/docs/decisions/0004-error-taxonomy.md` (the taxonomy, the
``(operation, code, status)`` classification, the 403 policy, ``ObjectArchived``)
and `misc/docs/decisions/0012-credential-and-endpoint-resolution.md` (the
``ConfigurationError`` tree and the ``S3DolWarning`` tree).

Three rules govern this module:

1. **One seam.** :func:`translate_s3_errors` is the only place in the package
   that catches botocore exceptions. Everything else propagates.
2. **Auth, config and transport errors are never translated to ``KeyError``.**
   A missing permission must not look like a missing key.
3. **Classification keys on ``(operation, code, status)``, never on exception
   type.** HEAD responses have no body, so botocore's modelled exceptions
   (``client.exceptions.NoSuchKey`` …) never fire for them.

Everything classification-related here is pure: :func:`classify_client_error`
takes plain values (or a synthesized ``ClientError``) and touches no network,
so the bulk of error tests are tier-1 unit tests — including body-less HEAD
errors, which moto cannot produce.

>>> classify_client_error('GetObject', 'NoSuchKey', 404).kind
'object_absent'
>>> classify_client_error('HeadObject', '404', 404).kind  # HEAD is ambiguous
'ambiguous_head'
>>> classify_client_error('GetObject', 'AccessDenied', 403).kind
'access_denied'
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Iterable, Mapping, Optional

# --------------------------------------------------------------------------- #
# The exception tree
# --------------------------------------------------------------------------- #


class S3Error(Exception):
    """Base of every s3dol exception — ``except S3Error`` catches the package."""


class _InformativeKeyError(KeyError):
    """A ``KeyError`` whose message is not repr-mangled.

    ``KeyError.__str__`` uses ``repr`` of its args, so a multi-line, helpful
    message becomes one line full of ``\\n`` escapes. Every s3dol ``KeyError``
    subclass therefore overrides ``__str__`` (ADR-0004 §5).

    >>> raw = KeyError('line one\\nline two')
    >>> '\\\\n' in str(raw)  # stdlib mangles
    True
    >>> ours = _InformativeKeyError('line one\\nline two')
    >>> '\\\\n' in str(ours)
    False
    """

    def __str__(self):
        return str(self.args[0]) if self.args else ""


class ObjectNotFound(S3Error, _InformativeKeyError):
    """The object is absent. The Mapping contract's ``KeyError``."""


class BucketNotFound(S3Error, _InformativeKeyError):
    """The bucket is absent.

    Still a ``KeyError``: for an object operation it is a key-space problem for
    the caller; for a bucket operation it is the key of the endpoint mapping.
    """


class KeyNotValid(S3Error, _InformativeKeyError, ValueError):
    """The key is syntactically invalid (checked before the wire).

    Inherits from both ``KeyError`` and ``ValueError`` deliberately, so either
    ``except`` works. Named ``KeyNotValid`` (not ``KeyValidationError``) because
    dol already ships two *different* classes called ``KeyValidationError``
    (ADR-0004 §5).
    """


class ObjectArchived(S3Error, _InformativeKeyError):
    """The object exists but is archived (e.g. Glacier) and cannot be read now.

    A ``KeyError``, deliberately and arguably (ADR-0004 §4): ``k in store``
    stays ``True``, but ``store[k]`` fails as a ``KeyError`` so that
    ``store.get(k, default)`` degrades to the not-available branch. Note that
    only the *defaulted accessors* degrade — ``dict(store)``, ``.items()`` and
    ``Mapping.__eq__`` still raise. ``setdefault``/``pop`` are overridden in the
    store to re-raise this instead of silently overwriting the archived object.
    """

    def __init__(
        self,
        message: str,
        *,
        storage_class: Optional[str] = None,
        restore_status: Optional[str] = None,
        restore: Optional[Callable] = None,
    ):
        super().__init__(message)
        self.storage_class = storage_class
        self.restore_status = restore_status
        self._restore = restore

    def restore(self, days: int = 1, *, tier: str = "Standard"):
        """Request restoration of the archived object (if a handle was bound)."""
        if self._restore is None:
            raise NotSupported(
                "No restore capability was bound to this error. "
                "Use s3dol.handles(store)[key].restore(...) instead."
            )
        return self._restore(days=days, tier=tier)


class AccessDenied(S3Error):
    """Permission denied. **Not** a ``KeyError`` — a missing permission must
    not look like a missing key (ADR-0004 §1/§3)."""


class CredentialsError(S3Error):
    """Credentials are missing, expired, or invalid."""


class NotSupported(S3Error):
    """The operation is not supported by this provider (names both)."""


class BucketNotEmpty(S3Error):
    """Refusal to delete a non-empty bucket. Use
    ``s3dol.delete_bucket(endpoint, name, force=True)`` for the explicit
    cascading form (ADR-0010 §3)."""


class S3PartialFailure(S3Error):
    """A bulk operation partially failed (ADR-0010 §2).

    Not an ``ExceptionGroup`` — the package supports Python 3.10.

    Attributes:
        succeeded: keys the backend reported as deleted (absent keys are
            reported as deleted too — S3's semantics, documented, not hidden).
        failures: mapping of key -> the error for that key.
    """

    def __init__(self, message: str, *, succeeded: list, failures: dict):
        super().__init__(message)
        self.succeeded = succeeded
        self.failures = failures


class ConfigurationError(S3Error, ValueError):
    """A construction- or resolution-time misconfiguration (ADR-0012 D1)."""


class PresetConflict(ConfigurationError):
    """An explicit argument contradicts a *pinned* (verified) preset value."""


class PresetHostMismatch(ConfigurationError):
    """The resolved endpoint's host matches a *different* provider's patterns
    than the preset you named (ADR-0012 C2)."""


class MissingEndpoint(ConfigurationError):
    """The named preset needs an endpoint (or endpoint parameters) that nothing
    supplied (ADR-0012 C1)."""


class MissingPresetParam(ConfigurationError):
    """A preset endpoint template has an unbound required parameter."""


class UnknownPresetParam(ConfigurationError):
    """A parameter was bound that the preset's templates do not use."""


class InvalidEndpoint(ConfigurationError):
    """An endpoint URL failed hygiene checks (userinfo/query/fragment —
    ADR-0012 D4: a URL is the residual credential carrier)."""


# --------------------------------------------------------------------------- #
# The warning tree (ADR-0012 D1). Strict mode is
# ``filterwarnings('error', category=s3dol.errors.S3DolWarning)`` — there is
# deliberately no ``strict=`` kwarg.
# --------------------------------------------------------------------------- #


class S3DolWarning(UserWarning):
    """Base of every s3dol warning."""


class S3DolResolutionChanged(S3DolWarning):
    """v1 resolves this call differently than v0 would have (ADR-0012 D7).
    Emitted by the compat shim; dies with it."""


class AmbiguousResolution(S3DolWarning):
    """Resolution is legal but ambiguous (e.g. a named preset whose endpoint,
    supplied from the environment, matches no known host pattern)."""


class AnonymousFallback(S3DolWarning):
    """Anonymous access was used as a fallback (reserved for a future
    ``anon='auto'`` — ADR-0012 D5 defers that feature to v1.x)."""


# --------------------------------------------------------------------------- #
# Redaction
# --------------------------------------------------------------------------- #

#: Query parameters whose values are secrets (presigned-URL machinery).
_SECRET_QUERY_PARAMS = (
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
    "Signature",
    "AWSAccessKeyId",
)

_SECRET_PARAM_RE = re.compile(
    r"(?P<name>{})=[^&\s]*".format("|".join(map(re.escape, _SECRET_QUERY_PARAMS))),
    flags=re.IGNORECASE,
)


def redact(text: str) -> str:
    """Redact signed-URL query secrets from ``text``.

    Error messages may name the operation, bucket, key, endpoint and code, but
    never a credential, token, or signed URL — redaction is a tested function,
    not a convention (ADR-0004 §7).

    >>> redact('https://b.s3.amazonaws.com/k?X-Amz-Signature=abc123&x=1')
    'https://b.s3.amazonaws.com/k?X-Amz-Signature=REDACTED&x=1'
    """
    return _SECRET_PARAM_RE.sub(lambda m: f"{m.group('name')}=REDACTED", text)


# --------------------------------------------------------------------------- #
# Classification — pure, table-driven, per-provider-overridable
# --------------------------------------------------------------------------- #

#: Kinds a classified error can be. Layer B maps kinds to behaviour; in
#: particular ``ambiguous_head`` triggers the one-``HeadBucket``
#: disambiguation of ADR-0004 §2 (HEAD has no body, so a missing key and a
#: missing bucket are both ``('404', 404)`` on real AWS).
KINDS = (
    "object_absent",
    "bucket_absent",
    "ambiguous_head",  # HeadObject 404: missing key OR missing bucket
    "access_denied",
    "credentials",
    "invalid_key",
    "archived",
    "not_supported",
    "wrong_region",
    "delete_marker",  # 405 on HEAD of a delete marker (versioned bucket)
    "transient",
    "unknown",
)


@dataclass(frozen=True)
class Classified:
    """The result of classifying a backend error."""

    kind: str
    exception_type: Optional[type] = None  # None => re-raise the original


#: Rule rows: ``((operation, code, status), kind)``. ``None`` is a wildcard.
#: First match wins; more specific rows come first.
Rule = tuple[tuple[Optional[str], Optional[str], Optional[int]], str]

_KIND_TO_EXC: dict[str, Optional[type]] = {
    "object_absent": ObjectNotFound,
    "bucket_absent": BucketNotFound,
    "ambiguous_head": ObjectNotFound,  # resolved by Layer B before raising
    "access_denied": AccessDenied,
    "credentials": CredentialsError,
    "invalid_key": KeyNotValid,
    "archived": ObjectArchived,
    "not_supported": NotSupported,
    "wrong_region": ConfigurationError,
    "delete_marker": ObjectNotFound,
    "transient": None,  # propagate; botocore retries
    "unknown": None,  # propagate untouched
}

#: The default classification table (ADR-0004 §2, measured against botocore).
DEFAULT_RULES: tuple[Rule, ...] = (
    # -- modelled codes (GET/DELETE have bodies, so codes are real words)
    ((None, "NoSuchKey", None), "object_absent"),
    ((None, "NoSuchBucket", None), "bucket_absent"),
    ((None, "NoSuchUpload", None), "object_absent"),
    # -- HEAD has no body: botocore synthesizes the code from the HTTP status.
    #    On real AWS, HeadObject-on-missing-key and HeadObject-on-missing-bucket
    #    are BOTH ('404', 404) — indistinguishable. moto returns a body
    #    (Code='NoSuchBucket'/'NoSuchKey'), which the modelled rows above catch,
    #    so this row only fires on real, body-less responses.
    (("HeadObject", "404", 404), "ambiguous_head"),
    (("HeadBucket", "404", 404), "bucket_absent"),
    (("HeadObject", "404", None), "ambiguous_head"),
    (("HeadBucket", "404", None), "bucket_absent"),
    # -- versioned-bucket delete marker: HEAD gives 405 Method Not Allowed
    (("HeadObject", None, 405), "delete_marker"),
    # -- wrong region
    ((None, None, 301), "wrong_region"),
    ((None, "PermanentRedirect", None), "wrong_region"),
    ((None, "AuthorizationHeaderMalformed", None), "wrong_region"),
    # -- archived objects
    ((None, "InvalidObjectState", None), "archived"),
    # -- credentials
    ((None, "ExpiredToken", None), "credentials"),
    ((None, "InvalidAccessKeyId", None), "credentials"),
    ((None, "SignatureDoesNotMatch", None), "credentials"),
    ((None, "TokenRefreshRequired", None), "credentials"),
    # -- permissions (see ADR-0004 §3 for the deny_means_absent policy)
    ((None, "AccessDenied", None), "access_denied"),
    ((None, "AllAccessDisabled", None), "access_denied"),
    ((None, "403", 403), "access_denied"),
    ((None, None, 403), "access_denied"),
    # -- key validity that escaped the client-side check
    ((None, "KeyTooLongError", None), "invalid_key"),
    # -- throttling / transient: propagate, botocore retries
    ((None, "SlowDown", None), "transient"),
    ((None, "RequestTimeout", None), "transient"),
    ((None, "ServiceUnavailable", None), "transient"),
    ((None, "InternalError", None), "transient"),
    ((None, None, 503), "transient"),
)

#: Provider overrides live on the preset (ADR-0004 §6 — per-connection,
#: explicit, inspectable; NOT process-global). E.g. Supabase returns 400 where
#: AWS returns 404 on HeadBucket:
#: ``(('HeadBucket', None, 400), 'bucket_absent')``.


def _match(rule_key, operation, code, status) -> bool:
    op, c, s = rule_key
    return (
        (op is None or op == operation)
        and (c is None or c == code)
        and (s is None or s == status)
    )


def classify_client_error(
    operation: Optional[str],
    code: Optional[str],
    status: Optional[int],
    *,
    overrides: Iterable[Rule] = (),
) -> Classified:
    """Classify a backend error by ``(operation, code, status)``.

    ``overrides`` rows (typically from a preset) are consulted first, so a
    provider divergence is a table row, never a code branch.

    >>> classify_client_error('HeadBucket', None, 400,
    ...     overrides=((('HeadBucket', None, 400), 'bucket_absent'),)).kind
    'bucket_absent'
    """
    for rules in (tuple(overrides), DEFAULT_RULES):
        for key, kind in rules:
            if _match(key, operation, code, status):
                return Classified(kind=kind, exception_type=_KIND_TO_EXC.get(kind))
    return Classified(kind="unknown", exception_type=None)


def code_of(client_error: Any) -> Optional[str]:
    """The ``Error.Code`` of a botocore ``ClientError`` (or ``None``)."""
    try:
        return client_error.response["Error"]["Code"]
    except (AttributeError, KeyError, TypeError):
        return None


def status_of(client_error: Any) -> Optional[int]:
    """The HTTP status of a botocore ``ClientError`` (or ``None``)."""
    try:
        return int(client_error.response["ResponseMetadata"]["HTTPStatusCode"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def _describe(
    *,
    operation: str,
    code: Optional[str],
    bucket: Optional[str],
    key: Optional[str],
    endpoint: Optional[str],
    detail: str = "",
) -> str:
    """Build the standard error message: operation, bucket, key, endpoint,
    code — and nothing secret (ADR-0004 §7)."""
    parts = [f"{operation} failed"]
    if bucket is not None:
        parts.append(f"bucket={bucket!r}")
    if key is not None:
        parts.append(f"key={key!r}")
    if endpoint is not None:
        parts.append(f"endpoint={endpoint!r}")
    if code is not None:
        parts.append(f"code={code}")
    msg = ": ".join([parts[0], ", ".join(parts[1:])]) if parts[1:] else parts[0]
    if detail:
        msg = f"{msg}. {detail}"
    return redact(msg)


def translate_client_error(
    error: Any,
    *,
    operation: str,
    bucket: Optional[str] = None,
    key: Optional[str] = None,
    endpoint: Optional[str] = None,
    overrides: Iterable[Rule] = (),
    deny_means_absent: bool = False,
    head_ambiguity_resolver: Optional[Callable[[], bool]] = None,
) -> Optional[S3Error]:
    """Translate a botocore ``ClientError`` into the s3dol taxonomy.

    Returns the replacement exception, or ``None`` meaning *propagate the
    original* (transient/unknown — rule 2 of the module docstring).

    ``head_ambiguity_resolver`` resolves the body-less ``HeadObject`` 404
    (ADR-0004 §2): it returns ``True`` iff the bucket is known reachable, in
    which case the miss really is the object's. It is only called for the
    ambiguous case, so the common path costs nothing.

    ``deny_means_absent=True`` classifies an object-op 403 as
    ``ObjectNotFound`` — for deployments knowingly running the canonical
    least-privilege policy that omits ``s3:ListBucket`` (ADR-0004 §3).
    """
    code, status = code_of(error), status_of(error)
    classified = classify_client_error(operation, code, status, overrides=overrides)
    kind = classified.kind

    if kind == "ambiguous_head":
        if head_ambiguity_resolver is not None and head_ambiguity_resolver():
            kind = "object_absent"
        elif head_ambiguity_resolver is not None:
            kind = "bucket_absent"
        else:
            kind = "object_absent"  # documented default when no resolver bound

    if kind == "access_denied":
        if deny_means_absent and key is not None:
            kind = "object_absent"
        else:
            return AccessDenied(
                _describe(
                    operation=operation,
                    code=code,
                    bucket=bucket,
                    key=key,
                    endpoint=endpoint,
                    detail=(
                        "Permission denied. If this key may simply be absent, note "
                        "that the canonical least-privilege policy (no s3:ListBucket) "
                        "makes every miss a 403; opt into that reading with "
                        "S3Connection(deny_means_absent=True)."
                    ),
                )
            )

    if kind in ("transient", "unknown"):
        return None

    exc_type = _KIND_TO_EXC[kind]
    detail = {
        "wrong_region": "The bucket lives in a different region than the client.",
        "delete_marker": "The current version is a delete marker (versioned bucket).",
        "credentials": "Credentials are missing, expired, or invalid.",
    }.get(kind, "")
    message = _describe(
        operation=operation,
        code=code,
        bucket=bucket,
        key=key,
        endpoint=endpoint,
        detail=detail,
    )
    if exc_type is ObjectArchived:
        return ObjectArchived(message)
    return exc_type(message)


def translate_s3_errors(
    *,
    operation: str,
    key_arg: Optional[str] = None,
    **translate_kwargs,
):
    """The one seam: decorate a Layer B method so botocore's ``ClientError``
    becomes the s3dol taxonomy (ADR-0004 §1).

    ``key_arg`` names the decorated function's key *parameter* (resolved via
    the call's kwargs/positionals — never an attribute, so the key is not lost
    the way azuredol's ``key_arg='blob'`` loses it). The store instance is
    expected to provide ``_error_context()`` returning
    ``dict(bucket=..., endpoint=..., overrides=..., deny_means_absent=...,
    head_ambiguity_resolver=...)`` — Layer B owns that context.
    """

    def decorator(func):
        import inspect

        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            from botocore.exceptions import ClientError, NoCredentialsError

            try:
                return func(self, *args, **kwargs)
            except ClientError as error:
                key = None
                if key_arg is not None:
                    bound = sig.bind(self, *args, **kwargs)
                    key = bound.arguments.get(key_arg)
                context = dict(getattr(self, "_error_context", dict)())
                context.update(translate_kwargs)
                replacement = translate_client_error(
                    error, operation=operation, key=key, **context
                )
                if replacement is None:
                    raise
                raise replacement from error
            except NoCredentialsError as error:
                raise CredentialsError(
                    "No AWS credentials found. Configure credentials (env vars, "
                    "~/.aws/credentials, SSO, or S3Connection(credentials=...)), "
                    "or pass anon=True for public data."
                ) from error

        return wrapper

    return decorator
