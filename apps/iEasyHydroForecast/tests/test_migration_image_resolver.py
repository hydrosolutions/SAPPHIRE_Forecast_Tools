"""Tests for the unpinned-tag warning in migration_py._common.resolve_image.

Stage E item #10. Note: the deployment-server detection (docker ps probe)
lives in the SHELL helper umh_resolve_image. The Python-side warning fires
on any unpinned tag; the shell helper decides whether to elevate it. The
shell-side probe is not unit-tested in P0 because it would require docker-
in-test-runner — it is shellcheck-verified and runbook-documented.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common  # noqa: E402

_LOGGER_NAME = "migration_py._common"


def test_resolve_image_warns_on_latest_tag(caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    image, source = _common.resolve_image(None, "latest")
    assert source == "configured"
    assert image.endswith(":latest")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected a WARNING for unpinned tag"
    text = caplog.text
    assert "latest" in text
    # Operators must see a clear "pin to release tag" cue.
    assert "pin" in text.lower()


def test_resolve_image_warns_on_local_tag(caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    image, _ = _common.resolve_image(None, "local")
    assert image.endswith(":local")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings
    assert "local" in caplog.text


def test_resolve_image_no_warning_on_dated_tag(caplog):
    # Dated YYYY-MM tags (e.g. 2026-06) are the canonical operator-pin form
    # after Finding 4 — v1.0.0 was never published to Docker Hub.
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    _common.resolve_image(None, "2026-06")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []


def test_resolve_image_warning_does_not_advise_v1_0_0(caplog):
    """Regression proof for Finding 4 (Tajik walkthrough, 2026-06-08).

    The operator-facing warning previously recommended pinning to
    ``mabesa/sapphire-prepgateway:v1.0.0``. That tag does not exist on
    Docker Hub; operators following the advice hit a pull failure. The
    abstract dated-tag guidance replaces it and must not regress.
    """
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    _common.resolve_image(None, "latest")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected at least one WARNING for unpinned :latest tag"
    combined = " ".join(r.getMessage() for r in warnings)
    assert "v1.0.0" not in combined, (
        f"warning text must not advise v1.0.0 (Finding 4 regression); got: {combined!r}"
    )


def test_resolve_image_warn_on_unpinned_can_be_disabled(caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    _common.resolve_image(None, "latest", warn_on_unpinned=False)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []


def test_resolve_image_warning_includes_resolved_image_string(caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    image, _ = _common.resolve_image(None, "latest")
    # Operators must be able to grep for the full image string.
    assert image in caplog.text
