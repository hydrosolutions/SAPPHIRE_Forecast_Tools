"""Tests for migration_py._common.detect_mode — Stage E Q2 layer 1."""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common  # noqa: E402


def test_detect_mode_empty_target_returns_full_import():
    assert _common.detect_mode(target_count=0, target_min_date=None) == (
        "full-import",
        None,
    )


def test_detect_mode_null_min_date_returns_full_import():
    # Documented edge case: corrupted / cleared table reports rows but null min.
    assert _common.detect_mode(target_count=42, target_min_date=None) == (
        "full-import",
        None,
    )


def test_detect_mode_populated_target_returns_pre_cutoff():
    assert _common.detect_mode(target_count=100, target_min_date="2025-01-15") == (
        "pre-cutoff",
        "2025-01-15",
    )


def test_detect_mode_cutoff_override_used_when_populated():
    assert _common.detect_mode(
        target_count=100,
        target_min_date="2025-01-15",
        cutoff_fallback="2024-06-01",
    ) == ("pre-cutoff", "2024-06-01")


def test_detect_mode_cutoff_override_ignored_when_empty():
    """Override must NOT promote an empty target into pre-cutoff mode."""
    assert _common.detect_mode(
        target_count=0,
        target_min_date=None,
        cutoff_fallback="2024-06-01",
    ) == ("full-import", None)
