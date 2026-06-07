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


def test_export_help_uses_i_am_on_laptop_not_allow_server_host():
    """Charter §Stage E #6: bypass flag is --i-am-on-laptop, not --allow-server-host.
    Reviewer flagged P4b shipping the wrong name; this regression guard locks it in."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--i-am-on-laptop" in result.stdout
    assert "--allow-server-host" not in result.stdout
    # And the same shape applies to the script source: no stale ALLOW_SERVER_HOST.
    src = _WRAPPER.read_text(encoding="utf-8")
    assert "ALLOW_SERVER_HOST" not in src
    assert "allow-server-host" not in src


def test_export_help_documents_dry_run():
    """Review feedback (round 2): the runbook §6 contract says every CSV-/DB-
    source wrapper supports --dry-run. P4b previously omitted it; the docs
    were brought back into line + the COUNT-only path was implemented.

    Regression guard: --help documents --dry-run AND the script source has
    the DRY_RUN handling that the contract requires."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--dry-run" in result.stdout
    src = _WRAPPER.read_text(encoding="utf-8")
    # The argparse case + the DRY_RUN branch must both be present.
    assert "DRY_RUN=true" in src
    assert 'DRY_RUN" == true' in src
    assert "SELECT COUNT(*)" in src


def test_export_rejects_zero_row_filter():
    """Reviewer finding: zero-row exports must fail up front with a clear error and
    no manifest written, instead of emitting blank date_min/date_max that the
    server-side validator rejects as non-ISO.

    This regression test uses the embedded zero-row guard as a string-level check.
    A live-DB test would require a running PostgreSQL with a known-empty filter,
    which is out of scope per architecture §Q7."""
    src = _WRAPPER.read_text(encoding="utf-8")
    # The guard is implemented in shell between summary computation and manifest write.
    assert "row_count=" in src and "cut -d=" in src
    # And the error message is specific enough that an operator can act on it.
    assert "no rows matched filter" in src
    # The manifest must NOT be written in the zero-row path; verify the early-exit
    # falls between summary computation and manifest emission.
    pre_manifest, _, post_manifest = src.partition("# Write the manifest.")
    assert "no rows matched filter" in pre_manifest
    assert "no rows matched filter" not in post_manifest


def test_export_rejects_zero_row_behaviorally(tmp_path):
    """Round-2 review feedback: prior zero-row test was source-text only.
    This stubs psql via PATH so the wrapper actually runs through its
    post-COPY control flow with a header-only CSV and we verify:
      (1) the wrapper exits non-zero,
      (2) the operator-facing error message is emitted, and
      (3) NO manifest sidecar is written.
    """
    import os
    import stat

    # Fake psql that emits just a CSV header line (no data rows).
    fake_bin = tmp_path / "fake_bin"
    fake_bin.mkdir()
    fake_psql = fake_bin / "psql"
    fake_psql.write_text(
        "#!/usr/bin/env bash\n"
        'echo "code,model_type,horizon_type,date,target,flag,Q5,Q25,Q50,Q75,Q95,forecasted_discharge"\n'
        "exit 0\n"
    )
    fake_psql.chmod(fake_psql.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    out_dir = tmp_path / "out"

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env.get('PATH', '')}"
    # The script's location guard probes docker for sapphire-postprocessing-db;
    # use --i-am-on-laptop to bypass without depending on docker state.
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(out_dir), "--i-am-on-laptop"],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    assert result.returncode != 0, (
        f"expected non-zero exit on zero-row export, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    combined = (result.stdout + result.stderr).lower()
    assert "no rows matched filter" in combined, (
        f"expected zero-row error message in output, got:\n{result.stdout}\n{result.stderr}"
    )
    # The manifest sidecar must NOT exist (the guard runs BEFORE manifest write).
    manifests = list(out_dir.glob("*.manifest")) if out_dir.exists() else []
    assert manifests == [], f"manifest should not exist after zero-row reject, got {manifests}"
