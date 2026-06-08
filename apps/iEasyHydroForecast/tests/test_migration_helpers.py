"""Smoke + helper tests for the migration_py package public API.

Exercises the package's importability and the lighter helpers
(resolve_image core branches, log_redacted_station_count, acquire_temp_workspace).
The image-warning behaviour is covered in test_migration_image_resolver.py;
manifest validation in test_migration_manifest.py; mode selection in
test_migration_mode.py.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _audit, _common  # noqa: E402


def test_package_importable():
    """The migration_py namespace exposes _common and _audit submodules."""
    import migration_py  # noqa: F401  - sanity import

    assert _common is not None
    assert _audit is not None
    # The public API surface advertised in __all__ is callable.
    for name in (
        "resolve_image",
        "detect_mode",
        "validate_manifest",
        "acquire_temp_workspace",
        "log_redacted_station_count",
    ):
        assert callable(getattr(_common, name)), name


def test_resolve_image_cli_override_wins():
    image, source = _common.resolve_image("custom:tag", "release-x")
    assert image == "custom:tag"
    assert source == "cli"


def test_resolve_image_configured_tag_used():
    # Use a dated-tag form (YYYY-MM) — the canonical operator-pin shape after
    # Finding 4 (Tajik walkthrough): v1.0.0 was never published to Docker Hub,
    # so test fixtures no longer enshrine it.
    image, source = _common.resolve_image(None, "2026-06")
    assert image == "mabesa/sapphire-prepgateway:2026-06"
    assert source == "configured"


def test_resolve_image_fallback_to_latest():
    image, source = _common.resolve_image(None, None)
    assert image == _common.FALLBACK_IMAGE
    assert source == "fallback"


def test_resolve_image_empty_string_treated_as_none():
    image, source = _common.resolve_image("", "")
    assert image == _common.FALLBACK_IMAGE
    assert source == "fallback"


def test_log_redacted_station_count_only_logs_count(caplog):
    logger = logging.getLogger("migration_py.test.redacted_only_count")
    caplog.set_level(logging.INFO, logger=logger.name)
    # Mix of synthetic non-sentinel codes (19xxx but not the 19999 prefix) and
    # a real sentinel; this drives the "all redacted" branch. Using 19xxx
    # synthetics keeps the codes outside the real-station-code regex
    # (\b1[0-8][0-9]{3}\b|\b[2-9][0-9]{4}\b) used by the fixture guard.
    codes = ["19000", "19001", "19002", "19999"]  # not all sentinel
    _common.log_redacted_station_count(logger, codes)
    # None of the non-sentinel codes appear in the log text.
    text = caplog.text
    for code in ("19000", "19001", "19002"):
        assert code not in text, f"code {code!r} leaked into log: {text!r}"
    assert "count=4" in text
    assert "all redacted" in text


def test_log_redacted_station_count_handles_empty(caplog):
    logger = logging.getLogger("migration_py.test.redacted_empty")
    caplog.set_level(logging.INFO, logger=logger.name)
    _common.log_redacted_station_count(logger, [])
    assert "count=0" in caplog.text


def test_log_redacted_station_count_sentinel_recognition(caplog):
    logger = logging.getLogger("migration_py.test.redacted_sentinel")
    caplog.set_level(logging.INFO, logger=logger.name)
    # 19999, 19999-prefixed variant, and an HRU sentinel.
    codes = ["19999", "19999_dup", "00000"]
    _common.log_redacted_station_count(logger, codes)
    assert "sentinel-only" in caplog.text
    assert "19999-class" in caplog.text


def test_acquire_temp_workspace_creates_strict_dir(tmp_path):
    workspace = _common.acquire_temp_workspace(tmp_path, "foo", timestamp="20260101T000000Z")
    assert workspace.is_dir()
    assert workspace == tmp_path / "logs" / "foo_tmp" / "20260101T000000Z"
    # Mode 0o700.
    mode = workspace.stat().st_mode & 0o777
    assert mode == 0o700, f"expected 0o700, got {oct(mode)}"


def test_acquire_temp_workspace_collision_raises(tmp_path):
    ts = "20260101T000000Z"
    _common.acquire_temp_workspace(tmp_path, "foo", timestamp=ts)
    with pytest.raises(FileExistsError):
        _common.acquire_temp_workspace(tmp_path, "foo", timestamp=ts)
