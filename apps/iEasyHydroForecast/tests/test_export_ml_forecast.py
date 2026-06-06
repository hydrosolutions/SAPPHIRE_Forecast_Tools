"""Tests for the P4b ML forecast laptop-export wrapper.

Covers:
- ``bash bin/export_ml_forecast_history.sh`` CLI surface (``--help``,
  missing out_dir rejection).
- Validation of ``--model`` flag (accepts dir / API forms; rejects others).
- The ``WARNING`` log line documentation in ``--help`` for
  ``--include-legacy-horizons``.

This script's COPY against a live PostgreSQL is out of scope here (mirrors
architecture §Q7: live-DB exports belong to a separate integration sprint).
What we DO test is the script's CLI behavior, location guard, and
documentation of the two architectural quirks (enum case, legacy horizons).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_WRAPPER = _REPO_ROOT / "bin" / "export_ml_forecast_history.sh"


def test_wrapper_help_returns_zero_and_prints_usage():
    """``bash bin/export_ml_forecast_history.sh --help`` exits 0 and shows usage."""
    assert _WRAPPER.is_file(), f"wrapper missing: {_WRAPPER}"
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout
    # The binding P0 contract flag must appear in the help text.
    assert "--station-filter" in result.stdout


def test_wrapper_rejects_missing_out_dir():
    """Wrapper exits non-zero with descriptive error when out_dir is missing."""
    result = subprocess.run(
        ["bash", str(_WRAPPER)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "out_dir" in combined or "required" in combined


def test_wrapper_help_documents_include_legacy_horizons():
    """Help text mentions the --include-legacy-horizons flag + its semantics."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "-h"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--include-legacy-horizons" in result.stdout


def test_wrapper_help_documents_model_filter():
    """Help text lists the canonical API model spellings (TFT, TiDE, TSMixer)."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "-h"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    # The three canonical API enum values must appear in the help.
    assert "TFT" in result.stdout
    assert "TiDE" in result.stdout
    assert "TSMixer" in result.stdout


def test_wrapper_rejects_unknown_model_filter(tmp_path):
    """``--model SOMETHING_BAD`` is rejected by the validation step."""
    # We pass a non-existent out_dir to avoid actually creating one if the
    # validation accidentally proceeds; the validation happens before the
    # location-guard step.
    out_dir = tmp_path / "out"
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(out_dir), "--model", "GBT"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "not recognized" in combined or "expected one of" in combined


def test_wrapper_help_documents_location_guard():
    """Help text mentions the laptop-only location guard."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    text = result.stdout.lower()
    assert "location guard" in text or "laptop" in text


def test_wrapper_help_documents_manifest_sidecar():
    """Help / docstring describes the manifest sidecar that gets written."""
    text = _WRAPPER.read_text(encoding="utf-8").lower()
    assert "manifest" in text
    assert "export_type=ml_forecast" in text


def test_wrapper_uses_p0_manifest_required_keys():
    """The script must write the 5 required keys validated by
    ``migration_py._common.validate_manifest``."""
    text = _WRAPPER.read_text(encoding="utf-8")
    # The script's inline python prints these keys.
    for key in ("row_count", "station_count", "date_min", "date_max"):
        assert key in text, f"manifest key {key!r} not emitted by export script"
    # export_type is hard-coded.
    assert "export_type=ml_forecast" in text


def test_wrapper_documents_enum_case_mapping():
    """The script's docstring documents the on-disk-vs-API case mapping."""
    text = _WRAPPER.read_text(encoding="utf-8")
    # The three canonical API values must be present somewhere.
    assert "TFT" in text
    assert "TiDE" in text
    assert "TSMixer" in text
