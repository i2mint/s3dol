"""Tests for ``s3dol.diagnose()`` (ADR-0012 D6/D7). All tier 1."""

import io

from s3dol._diagnose import _spec_from_v0_args, _v0_mangle, diagnose


def run_diagnose(**kwargs):
    """diagnose() against a controlled environment, capturing the report."""
    kwargs.setdefault("environ", {})
    return diagnose(file=io.StringIO(), **kwargs)


# --------------------------------------------------------------------------- #
# never raises (D6)
# --------------------------------------------------------------------------- #


def test_reports_resolution_for_plain_v1_kwargs():
    report = run_diagnose(endpoint_url="http://localhost:9000")
    assert "resolution:" in report
    assert "http://localhost:9000" in report
    assert "endpoint_url=" in report
    assert "can_presign: True" in report


def test_a_failing_resolution_is_a_row_not_a_raise():
    report = run_diagnose(preset="minio")  # C1: MissingEndpoint
    assert "RESOLUTION FAILS" in report
    assert "MissingEndpoint" in report


def test_a_failing_construction_is_a_row_not_a_raise():
    report = run_diagnose(anon=True, profile="prod")
    assert "RESOLUTION FAILS" in report
    assert "ConfigurationError" in report


def test_utter_garbage_never_raises():
    report = run_diagnose(bogus_kwarg=42)
    assert isinstance(report, str) and report  # something readable came back


def test_secret_values_never_echo():
    report = run_diagnose(
        bucket_name="b",
        aws_access_key_id="AKIAEXAMPLE",
        aws_secret_access_key="sEkR3tVALUE",
    )
    assert "sEkR3tVALUE" not in report
    assert "<provided>" in report


def test_prints_to_the_given_file_and_returns_the_text():
    sink = io.StringIO()
    text = diagnose(endpoint_url="http://x:1", environ={}, file=sink)
    assert sink.getvalue() == text


# --------------------------------------------------------------------------- #
# the v0 mangler (D7): ~20 frozen lines mapping 1:1 onto v0
# --------------------------------------------------------------------------- #


def test_mangle_endpoint_dropped_under_env_credentials():
    branch, effective = _v0_mangle(
        {"endpoint_url": "http://r2:1", "aws_access_key_id": "EXPLICIT"},
        {"AWS_ACCESS_KEY_ID": "ENVKEY", "AWS_SECRET_ACCESS_KEY": "ENVSECRET"},
    )
    assert "endpoint dropped" in branch
    assert effective["endpoint_url"] is None  # the v0 data-misrouting bug
    assert effective["aws_access_key_id"] == "ENVKEY"  # env over explicit
    assert effective["aws_session_token"] is None  # nulled, silently
    assert effective["region_name"] is None  # nulled, silently


def test_mangle_explicit_branch_when_no_env_credentials():
    branch, effective = _v0_mangle(
        {"endpoint_url": "http://r2:1", "aws_access_key_id": "AK"}, {}
    )
    assert branch == "explicit"
    assert effective["endpoint_url"] == "http://r2:1"  # honoured on this path


def test_mangle_profile_with_explicit_creds_discards_profile():
    branch, _ = _v0_mangle(
        {"profile_name": "prod", "aws_access_key_id": "AK"}, {}
    )
    assert "discarded" in branch


def test_mangle_supabase_and_skip_branches_are_excluded():
    assert _v0_mangle({"endpoint_url": "https://x.supabase.co/s3"}, {})[1] is None
    assert _v0_mangle({"is_supabase_endpoint": True}, {})[1] is None
    assert _v0_mangle({"skip_bucket_check": True}, {})[1] is None


def test_spec_from_v0_args_maps_env_profile_to_the_chain():
    spec = _spec_from_v0_args({"profile_name": "environment variables"})
    assert spec.profile is None  # env creds are the top of botocore's own chain


# --------------------------------------------------------------------------- #
# the divergence table
# --------------------------------------------------------------------------- #


def test_divergence_flags_the_endpoint_drop():
    report = run_diagnose(
        bucket_name="b",
        endpoint_url="http://r2.example:1",
        aws_access_key_id="AK",
        aws_secret_access_key="SK",
        environ={"AWS_ACCESS_KEY_ID": "ENVK", "AWS_SECRET_ACCESS_KEY": "ENVS"},
    )
    assert "divergence" in report
    assert "CHANGES ON UPGRADE" in report
    assert "endpoint dropped" in report


def test_no_divergence_when_v0_and_v1_agree():
    report = run_diagnose(bucket_name="b", endpoint_url="http://minio:9000")
    assert "no divergence" in report


def test_excluded_branch_prints_the_reason():
    report = run_diagnose(bucket_name="b", endpoint_url="https://x.supabase.co/s3")
    assert "EXCLUDED" in report
    assert "process-global" in report


def test_credential_identity_divergence_is_a_predicate_not_a_value():
    report = run_diagnose(
        bucket_name="b",
        aws_access_key_id="AKIAEXPLICIT",
        aws_secret_access_key="sEkR3tVALUE",
        environ={
            "AWS_ACCESS_KEY_ID": "AKIAFROMENV",
            "AWS_SECRET_ACCESS_KEY": "ENVSEKRET99",
        },
    )
    assert "credentials:" in report
    assert "sEkR3tVALUE" not in report
    assert "ENVSEKRET99" not in report  # no env secret value either
