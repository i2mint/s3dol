"""Presigned POST uploads: the signed policy refuses what it must refuse.

No network and no moto needed: botocore signs locally, and a small evaluator
applies S3's documented POST-policy semantics (``starts-with``, ``eq``,
``content-length-range``, ``expiration``) to a submission, as S3 would.
"""

import base64
import json
from datetime import datetime, timedelta, timezone

import pytest

import s3dol
from s3dol.errors import ConfigurationError
from s3dol.testing import mock_s3
from s3dol.uploads import DailyByteBudget, UploadBudgetExceeded, presigned_upload


@pytest.fixture
def store(monkeypatch):
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    from s3dol.base import BucketStore
    from s3dol.connection import S3Connection

    return BucketStore(
        "up-bucket", connection=S3Connection(region_name="us-east-1"), prefix="app"
    )


def _policy(form):
    return json.loads(base64.b64decode(form.fields["policy"]))


def _accepts(form, *, key, size, content_type=None, now=None):
    """What S3 does with a submitted form, per the POST-policy spec."""
    policy = _policy(form)
    now = now or datetime.now(timezone.utc)
    expiry = datetime.strptime(policy["expiration"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    if now > expiry:
        return False
    # what the client SUBMITS overrides nothing the policy pins: fields first,
    # then the attacker-chosen parts
    submitted = {"bucket": "up-bucket", **form.fields, "key": key}
    if content_type is not None:
        submitted["Content-Type"] = content_type
    for cond in policy["conditions"]:
        if isinstance(cond, dict):
            ((name, value),) = cond.items()
            if submitted.get(name) != value:
                return False
        elif cond[0] == "starts-with":
            if not str(submitted.get(cond[1].lstrip("$"), "")).startswith(cond[2]):
                return False
        elif cond[0] == "content-length-range":
            if not cond[1] <= size <= cond[2]:
                return False
    return True


def test_key_is_server_generated_under_the_prefix(store):
    form = presigned_upload(
        store, key_prefix="tenant-7/", max_bytes=1000, content_type=None
    )
    assert form.key.startswith("app/tenant-7/") and form.fields["key"] == form.key
    assert "${filename}" not in form.fields["key"]
    assert _accepts(form, key=form.key, size=10)
    # the policy pins the EXACT key: nothing else is accepted
    for other in ("app/tenant-8/a", "app/tenant-7/other", form.key + "x", "app/tenant-7/../x"):
        assert not _accepts(form, key=other, size=10)
    assert presigned_upload(store, key_prefix="tenant-7/", max_bytes=9, content_type=None).key != form.key


@pytest.mark.parametrize(
    "prefix", ["", "tenant-7", "/t/", "t/../u/", "./", "t/$x/", "t/${filename}/"]
)
def test_unsafe_prefix_refused(store, prefix):
    with pytest.raises(ConfigurationError):
        presigned_upload(store, key_prefix=prefix, max_bytes=9, content_type=None)


def test_refuses_size_outside_range(store):
    form = presigned_upload(
        store, key_prefix="t/", min_bytes=5, max_bytes=100, content_type="image/png"
    )
    k = form.key
    assert _accepts(form, key=k, size=5) and _accepts(form, key=k, size=100)
    assert not _accepts(form, key=k, size=4)
    assert not _accepts(form, key=k, size=101)


def test_content_type_is_pinned(store):
    form = presigned_upload(
        store, key_prefix="t/", max_bytes=9, content_type="image/png"
    )
    assert _accepts(form, key=form.key, size=1, content_type="image/png")
    assert not _accepts(form, key=form.key, size=1, content_type="text/html")


def test_refuses_expired_form(store):
    form = presigned_upload(store, key_prefix="t/", max_bytes=9, content_type=None, expires_in=60)
    later = datetime.now(timezone.utc) + timedelta(seconds=120)
    assert _accepts(form, key=form.key, size=1)
    assert not _accepts(form, key=form.key, size=1, now=later)


def test_form_is_sigv4_and_posts_to_the_bucket(store):
    form = presigned_upload(store, key_prefix="t/", max_bytes=9, content_type=None)
    assert form.fields["x-amz-algorithm"] == "AWS4-HMAC-SHA256"
    assert "up-bucket" in form.url and form.fields["x-amz-signature"]


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(key_prefix="t", max_bytes=9),
        dict(key_prefix="t/", max_bytes=0),
        dict(key_prefix="t/", max_bytes=5, expires_in=True),
        dict(key_prefix="t/", max_bytes=5, min_bytes=6),
        dict(key_prefix="t/", max_bytes=5, expires_in=0),
    ],
)
def test_bad_arguments_are_refused(store, kwargs):
    with pytest.raises(ConfigurationError):
        presigned_upload(store, content_type=None, **kwargs)


def test_content_type_is_required(store):
    with pytest.raises(TypeError):
        presigned_upload(store, key_prefix="t/", max_bytes=9)


def test_budget_is_released_if_signing_fails(store, monkeypatch):
    budget = DailyByteBudget(100, today=lambda: "d")
    client = store.connection.presign_client

    def boom(*a, **k):
        raise RuntimeError("sign")

    monkeypatch.setattr(type(client), "generate_presigned_post", boom, raising=False)
    monkeypatch.setattr(client, "generate_presigned_post", boom, raising=False)
    with pytest.raises(RuntimeError):
        presigned_upload(store, key_prefix="t/", max_bytes=60, content_type=None, budget=budget)
    assert budget.ledger == {"d": 0}


def test_budget_refuses_before_signing(store):
    budget = DailyByteBudget(100, today=lambda: "d1")
    presigned_upload(store, key_prefix="t/", max_bytes=60, content_type=None, budget=budget)
    with pytest.raises(UploadBudgetExceeded):
        presigned_upload(store, key_prefix="t/", max_bytes=60, content_type=None, budget=budget)
    assert budget.ledger == {"d1": 60}  # the refused request reserved nothing
    budget.today = lambda: "d2"  # a new day, a fresh allowance
    presigned_upload(store, key_prefix="t/", max_bytes=60, content_type=None, budget=budget)


def test_expiry_is_capped_like_other_presigns(store):
    with pytest.warns(UserWarning, match="Clamping"):
        presigned_upload(store, key_prefix="t/", max_bytes=9, content_type=None, expires_in=10**9)


def test_anonymous_store_gets_none():
    store = mock_s3(data={})
    # the fake store has no real client; an anonymous real one is the contract
    from s3dol.base import BucketStore
    from s3dol.connection import S3Connection

    anon = BucketStore("b", connection=S3Connection(anon=True, region_name="us-east-1"))
    assert presigned_upload(anon, key_prefix="t/", max_bytes=9, content_type=None) is None
    del store


def test_exported_lazily():
    assert s3dol.presigned_upload is presigned_upload
