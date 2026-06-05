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


def test_resolve_image_no_warning_on_release_tag(caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER_NAME)
    _common.resolve_image(None, "v1.0.0")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []


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
