"""Pytest configuration for s3dol.

Lets the suite stay green **without a live S3 endpoint**. Most of s3dol's tests
are integration tests that talk to an S3-compatible server at
``http://localhost:4566`` (LocalStack in CI; nothing locally by default). When
no such endpoint is reachable, those items are *deselected* rather than left to
fail; when one is available (CI's LocalStack service, or a local dev endpoint)
they run normally.

The presigned-URL tests in ``test_url_for.py`` are hermetic — they mock the S3
API entirely with ``moto`` (``@mock_aws``) and need no endpoint — so they always
run and are never deselected.

Probe target defaults to ``localhost:4566`` and is overridable via the
``S3DOL_S3_PROBE_ENDPOINT`` env var (e.g. ``http://127.0.0.1:9000`` for a MinIO
dev server), letting you validate the deselect behaviour without touching the
tests' own hardcoded endpoint.
"""

import os
import pathlib
import socket
from urllib.parse import urlparse

#: Default S3 endpoint the integration tests connect to (LocalStack's port).
DFLT_S3_PROBE_ENDPOINT = "http://localhost:4566"

#: Test-module stems whose items do NOT need a live S3 endpoint (moto-mocked)
#: and are therefore always kept. Kept deliberately conservative: any test
#: module not listed here is deselected when no endpoint is reachable, so a new
#: endpoint-using test can never silently fail the local gate. Widen only after
#: verifying a module's tests are genuinely endpoint-free.
_S3_FREE_TEST_STEMS = frozenset(
    {
        "test_url_for",
        # Layer A (v1) suites: tier-1 pure + moto-mocked tier-2 — no endpoint.
        "test_errors",
        "test_presets",
        "test_connection",
        "test_diagnose",
    }
)


def _probe_host_port():
    """Parse ``S3DOL_S3_PROBE_ENDPOINT`` (or the default) into ``(host, port)``."""
    endpoint = os.environ.get("S3DOL_S3_PROBE_ENDPOINT", DFLT_S3_PROBE_ENDPOINT)
    parsed = urlparse(endpoint)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 4566)
    return host, port


def _s3_endpoint_available(*, timeout_s=0.5):
    """Return ``True`` iff an S3-compatible endpoint accepts a TCP connection."""
    host, port = _probe_host_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_s)
    try:
        return sock.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        sock.close()


def _requires_s3(item):
    """Whether a collected item needs a live S3 endpoint to run.

    Every test under ``s3dol/tests`` exercises a real (LocalStack/S3) client
    except the moto-mocked modules listed in :data:`_S3_FREE_TEST_STEMS`.
    """
    path = pathlib.Path(str(getattr(item, "path", None) or item.fspath))
    if path.stem in _S3_FREE_TEST_STEMS:
        return False
    return "tests" in path.parts


def pytest_collection_modifyitems(config, items):
    """Drop S3-endpoint-dependent items when no endpoint is reachable.

    Uses *deselection* (not a skip marker) so it stays robust across pytest
    versions and never generates a report for an item it drops.
    """
    if _s3_endpoint_available():
        return
    kept, deselected = [], []
    for item in items:
        (deselected if _requires_s3(item) else kept).append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept
