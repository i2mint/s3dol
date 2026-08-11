"""Tier-1 tests for the error taxonomy and the classification seam (ADR-0004).

Pure unit tests over *synthesized* ``ClientError``s — including body-less HEAD
errors, which moto cannot produce (it returns an error body on HEAD where real
AWS returns none), so tier 2 could never cover them.
"""

import pytest

from s3dol.errors import (
    AccessDenied,
    BucketNotFound,
    Classified,
    ConfigurationError,
    CredentialsError,
    KeyNotValid,
    ObjectArchived,
    ObjectNotFound,
    S3DolWarning,
    S3Error,
    S3PartialFailure,
    classify_client_error,
    code_of,
    redact,
    status_of,
    translate_client_error,
    translate_s3_errors,
)


def synthesized_client_error(code=None, status=None, operation="GetObject"):
    """A ClientError like botocore raises, built with no network."""
    from botocore.exceptions import ClientError

    response = {"ResponseMetadata": {"HTTPStatusCode": status}}
    if code is not None:
        response["Error"] = {"Code": code, "Message": "synthesized"}
    return ClientError(response, operation)


# --------------------------------------------------------------------------- #
# taxonomy shape
# --------------------------------------------------------------------------- #


def test_mapping_contract_errors_are_keyerrors():
    for exc_type in (ObjectNotFound, BucketNotFound, KeyNotValid, ObjectArchived):
        assert issubclass(exc_type, KeyError)
        assert issubclass(exc_type, S3Error)


def test_key_not_valid_is_also_a_valueerror():
    # Deliberately both, so either `except` works (ADR-0004 §5).
    assert issubclass(KeyNotValid, ValueError)


def test_auth_errors_are_never_keyerrors():
    # A missing permission must not look like a missing key (ADR-0004 §1).
    for exc_type in (AccessDenied, CredentialsError):
        assert not issubclass(exc_type, KeyError)
        assert issubclass(exc_type, S3Error)


def test_configuration_error_is_a_valueerror():
    assert issubclass(ConfigurationError, ValueError)


def test_except_s3error_catches_the_package():
    with pytest.raises(S3Error):
        raise ObjectNotFound("k")
    with pytest.raises(S3Error):
        raise AccessDenied("nope")


def test_keyerror_str_is_not_repr_mangled():
    message = "line one\nline two: helpful detail"
    assert str(ObjectNotFound(message)) == message  # stdlib KeyError would repr()
    assert "\\n" not in str(BucketNotFound(message))


def test_partial_failure_carries_the_split():
    error = S3PartialFailure(
        "3 of 5 deleted", succeeded=["a", "b", "c"], failures={"d": AccessDenied("x")}
    )
    assert error.succeeded == ["a", "b", "c"]
    assert set(error.failures) == {"d"}


def test_object_archived_carries_restore_metadata():
    error = ObjectArchived("archived", storage_class="GLACIER", restore_status=None)
    assert error.storage_class == "GLACIER"
    with pytest.raises(S3Error):
        error.restore()  # nothing bound -> loud, not silent


def test_warning_tree_rooted_in_s3dolwarning():
    from s3dol.errors import (
        AmbiguousResolution,
        AnonymousFallback,
        S3DolResolutionChanged,
    )

    for warning_class in (S3DolResolutionChanged, AmbiguousResolution, AnonymousFallback):
        assert issubclass(warning_class, S3DolWarning)
        assert issubclass(warning_class, UserWarning)


# --------------------------------------------------------------------------- #
# classification
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "operation, code, status, kind",
    [
        ("GetObject", "NoSuchKey", 404, "object_absent"),
        ("GetObject", "NoSuchBucket", 404, "bucket_absent"),
        # HEAD has no body: the code is the synthesized status string.
        ("HeadObject", "404", 404, "ambiguous_head"),
        ("HeadBucket", "404", 404, "bucket_absent"),
        ("HeadObject", "404", None, "ambiguous_head"),
        # delete-marker HEAD on a versioned bucket
        ("HeadObject", "405", 405, "delete_marker"),
        # wrong region
        ("GetObject", "PermanentRedirect", 301, "wrong_region"),
        ("ListObjectsV2", None, 301, "wrong_region"),
        # archived
        ("GetObject", "InvalidObjectState", 403, "archived"),
        # credentials
        ("GetObject", "ExpiredToken", 400, "credentials"),
        ("GetObject", "InvalidAccessKeyId", 403, "credentials"),
        ("PutObject", "SignatureDoesNotMatch", 403, "credentials"),
        # permissions
        ("GetObject", "AccessDenied", 403, "access_denied"),
        ("HeadObject", "403", 403, "access_denied"),
        # transient
        ("GetObject", "SlowDown", 503, "transient"),
        ("PutObject", None, 503, "transient"),
        # unknown propagates
        ("GetObject", "TotallyNewCode", 418, "unknown"),
    ],
)
def test_classification_table(operation, code, status, kind):
    assert classify_client_error(operation, code, status).kind == kind


def test_provider_overrides_are_consulted_first():
    # Supabase returns 400 where AWS returns 404 on HeadBucket (ADR-0004 §2).
    overrides = ((("HeadBucket", None, 400), "bucket_absent"),)
    assert (
        classify_client_error("HeadBucket", "400", 400, overrides=overrides).kind
        == "bucket_absent"
    )
    # ...and only for the operation they name.
    assert (
        classify_client_error("GetObject", "400", 400, overrides=overrides).kind
        == "unknown"
    )


def test_classified_is_a_value():
    assert Classified(kind="object_absent", exception_type=ObjectNotFound).kind == (
        "object_absent"
    )


def test_code_and_status_extractors_are_total():
    error = synthesized_client_error("NoSuchKey", 404)
    assert code_of(error) == "NoSuchKey"
    assert status_of(error) == 404
    assert code_of(object()) is None
    assert status_of(None) is None


# --------------------------------------------------------------------------- #
# translation
# --------------------------------------------------------------------------- #


def test_translate_object_absent():
    error = synthesized_client_error("NoSuchKey", 404)
    replacement = translate_client_error(
        error, operation="GetObject", bucket="b", key="k", endpoint="http://e"
    )
    assert isinstance(replacement, ObjectNotFound)
    for fragment in ("GetObject", "'b'", "'k'", "http://e"):
        assert fragment in str(replacement)


def test_translate_ambiguous_head_with_resolver():
    error = synthesized_client_error("404", 404, operation="HeadObject")
    bucket_reachable = translate_client_error(
        error, operation="HeadObject", key="k", head_ambiguity_resolver=lambda: True
    )
    assert isinstance(bucket_reachable, ObjectNotFound)
    bucket_missing = translate_client_error(
        error, operation="HeadObject", key="k", head_ambiguity_resolver=lambda: False
    )
    assert isinstance(bucket_missing, BucketNotFound)


def test_translate_access_denied_names_the_policy_cause():
    error = synthesized_client_error("AccessDenied", 403)
    replacement = translate_client_error(error, operation="HeadObject", key="k")
    assert isinstance(replacement, AccessDenied)
    assert "s3:ListBucket" in str(replacement)
    assert "deny_means_absent" in str(replacement)


def test_deny_means_absent_opt_in():
    error = synthesized_client_error("AccessDenied", 403)
    replacement = translate_client_error(
        error, operation="HeadObject", key="k", deny_means_absent=True
    )
    assert isinstance(replacement, ObjectNotFound)


def test_transient_and_unknown_propagate():
    assert (
        translate_client_error(
            synthesized_client_error("SlowDown", 503), operation="GetObject"
        )
        is None
    )
    assert (
        translate_client_error(
            synthesized_client_error("BrandNew", 418), operation="GetObject"
        )
        is None
    )


# --------------------------------------------------------------------------- #
# the decorator seam
# --------------------------------------------------------------------------- #


class _FakeStore:
    def __init__(self, error):
        self._error = error

    def _error_context(self):
        return dict(bucket="b", endpoint="http://e")

    @translate_s3_errors(operation="GetObject", key_arg="k")
    def read(self, k):
        raise self._error

    @translate_s3_errors(operation="GetObject", key_arg="k")
    def ok(self, k):
        return k.upper()


def test_decorator_translates_and_carries_the_key():
    store = _FakeStore(synthesized_client_error("NoSuchKey", 404))
    with pytest.raises(ObjectNotFound) as info:
        store.read("my-key")
    assert "my-key" in str(info.value)
    assert "'b'" in str(info.value)
    # the botocore original is chained, not swallowed
    assert info.value.__cause__ is not None


def test_decorator_extracts_key_passed_by_keyword():
    store = _FakeStore(synthesized_client_error("NoSuchKey", 404))
    with pytest.raises(ObjectNotFound) as info:
        store.read(k="kw-key")
    assert "kw-key" in str(info.value)


def test_decorator_propagates_unknown_errors_untouched():
    original = synthesized_client_error("BrandNewCode", 418)
    store = _FakeStore(original)
    with pytest.raises(type(original)) as info:
        store.read("k")
    assert info.value is original


def test_decorator_translates_missing_credentials():
    from botocore.exceptions import NoCredentialsError

    store = _FakeStore(NoCredentialsError())
    with pytest.raises(CredentialsError) as info:
        store.read("k")
    assert "anon=True" in str(info.value)


def test_decorator_leaves_success_alone():
    store = _FakeStore(None)
    assert store.ok("abc") == "ABC"


# --------------------------------------------------------------------------- #
# redaction
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "secret_param",
    ["X-Amz-Signature", "X-Amz-Credential", "X-Amz-Security-Token", "AWSAccessKeyId"],
)
def test_redact_presigned_url_secrets(secret_param):
    url = f"https://b.s3.amazonaws.com/k?{secret_param}=SECRETVALUE&keep=1"
    redacted = redact(url)
    assert "SECRETVALUE" not in redacted
    assert "keep=1" in redacted


def test_redact_is_case_insensitive_and_leaves_plain_text_alone():
    assert redact("nothing secret here") == "nothing secret here"
    assert "hunter2" not in redact("?x-amz-signature=hunter2")
