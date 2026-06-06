"""Tests for the P2a LAPTOP-SIDE export wrapper

``bin/export_runoff_period_history.sh``.

These tests exercise CLI surface, argument validation, and the location guard
(Stage E #6). The wrapper's actual psql COPY path is integration-only and is
NOT exercised here (architecture §Q7 — disposable integration is a separate
sprint; the export script's real-DB code path is operator-tested via the
runbook §6.1 canary).

Sentinel codes (19999-class) only. No real station codes anywhere in this file.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common  # noqa: E402

_EXPORT_WRAPPER = _REPO_ROOT / "bin" / "export_runoff_period_history.sh"
_FIXTURE_DIR = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "runoff_period"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_wrapper(
    args: list[str],
    env: dict[str, str] | None = None,
    *,
    bypass_location_guard: bool = True,
) -> subprocess.CompletedProcess:
    """Invoke the export wrapper with the given args + env.

    By default the location guard is bypassed (via the
    ``_P2A_EXPORT_SKIP_LOCATION_GUARD=1`` testing-only env hook) so the
    tests can exercise the wrapper's downstream validation paths even when
    the developer laptop has the SAPPHIRE stack running locally. The
    location guard itself is exercised in a dedicated positive test
    (``test_location_guard_fires_when_sapphire_containers_present``).
    """
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    if bypass_location_guard:
        full_env["_P2A_EXPORT_SKIP_LOCATION_GUARD"] = "1"
    return subprocess.run(
        ["bash", str(_EXPORT_WRAPPER), *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=full_env,
    )


# ---------------------------------------------------------------------------
# 1. Wrapper exists + --help returns 0
# ---------------------------------------------------------------------------


def test_export_wrapper_exists_on_disk():
    assert _EXPORT_WRAPPER.is_file(), f"export wrapper missing: {_EXPORT_WRAPPER}"


def test_export_wrapper_help_returns_zero_and_prints_usage():
    result = _run_wrapper(["--help"])
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout
    # All three required flags are documented.
    assert "--horizon" in result.stdout
    assert "--stations-file" in result.stdout
    assert "--out-dir" in result.stdout


def test_export_wrapper_short_help_works():
    result = _run_wrapper(["-h"])
    assert result.returncode == 0
    assert "Usage" in result.stdout


# ---------------------------------------------------------------------------
# 2. Argument validation
# ---------------------------------------------------------------------------


def test_export_wrapper_rejects_missing_horizon(tmp_path):
    stations = tmp_path / "stations.txt"
    stations.write_text("19999\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    result = _run_wrapper(["--stations-file", str(stations), "--out-dir", str(out_dir)])
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "--horizon" in combined or "horizon" in combined


def test_export_wrapper_rejects_invalid_horizon(tmp_path):
    stations = tmp_path / "stations.txt"
    stations.write_text("19999\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    result = _run_wrapper(
        [
            "--horizon",
            "month",  # not pentad/decade
            "--stations-file",
            str(stations),
            "--out-dir",
            str(out_dir),
        ]
    )
    assert result.returncode != 0


def test_export_wrapper_rejects_missing_stations_file(tmp_path):
    out_dir = tmp_path / "out"
    result = _run_wrapper(["--horizon", "pentad", "--out-dir", str(out_dir)])
    assert result.returncode != 0


def test_export_wrapper_rejects_nonexistent_stations_file(tmp_path):
    nonexistent = tmp_path / "no_such_stations.txt"
    out_dir = tmp_path / "out"
    result = _run_wrapper(
        [
            "--horizon",
            "pentad",
            "--stations-file",
            str(nonexistent),
            "--out-dir",
            str(out_dir),
        ],
        env={
            # Have to provide PG* env so we get past the env-var check;
            # the actual failure should be on the stations file.
            "PGHOST": "127.0.0.1",
            "PGUSER": "postgres",
            "PGDATABASE": "test",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "stations" in combined or "not found" in combined


def test_export_wrapper_rejects_missing_pg_env(tmp_path):
    """When PGHOST/PGUSER/PGDATABASE are unset, refuse before doing anything."""
    stations = tmp_path / "stations.txt"
    stations.write_text("19999\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    # Explicitly UNSET PG vars. Use a minimal env. Bypass location guard
    # so this developer-laptop test reaches the PG-env check.
    result = subprocess.run(
        [
            "bash",
            str(_EXPORT_WRAPPER),
            "--horizon",
            "pentad",
            "--stations-file",
            str(stations),
            "--out-dir",
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env={
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
            "_P2A_EXPORT_SKIP_LOCATION_GUARD": "1",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "pghost" in combined or "pguser" in combined or "pgdatabase" in combined


def test_export_wrapper_rejects_apostrophe_in_stations_file(tmp_path):
    """SQL-injection guard: a station code containing an apostrophe is rejected."""
    stations = tmp_path / "stations.txt"
    # Intentionally include an apostrophe; the wrapper should reject.
    stations.write_text("19999\nbad'code\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    result = _run_wrapper(
        [
            "--horizon",
            "pentad",
            "--stations-file",
            str(stations),
            "--out-dir",
            str(out_dir),
        ],
        env={
            "PGHOST": "127.0.0.1",
            "PGUSER": "postgres",
            "PGDATABASE": "test",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "apostrophe" in combined or "injection" in combined or "reject" in combined


# ---------------------------------------------------------------------------
# 3. Location guard (Stage E #6)
# ---------------------------------------------------------------------------


def test_export_wrapper_help_mentions_location_guard():
    """Help text documents the location-guard behavior so operators understand."""
    result = _run_wrapper(["--help"])
    assert result.returncode == 0
    low = result.stdout.lower()
    # Must mention "deployment server" or "laptop" or "jump host" so the
    # operator knows where to run this.
    assert "laptop" in low or "deployment" in low or "jump" in low


def _sapphire_containers_present() -> bool:
    """Return True iff at least one sapphire-* deployment container is running.

    Used to decide whether to exercise the location guard's positive path
    (it can only fire if the docker probe actually sees a container).
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=sapphire-preprocessing-db", "--quiet"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return bool(result.stdout.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(
    not _sapphire_containers_present(),
    reason=(
        "Location-guard positive test requires sapphire-* containers running "
        "locally (developer laptop with the SAPPHIRE stack up). On CI / "
        "clean machines this test is skipped because the guard physically "
        "cannot fire without the trigger condition."
    ),
)
def test_location_guard_fires_when_sapphire_containers_present(tmp_path):
    """Positive path: with sapphire containers running AND the bypass NOT set,
    the wrapper refuses with the documented deployment-server message.

    This test depends on the developer machine state (the SAPPHIRE stack
    must be up). It is automatically skipped where the guard cannot fire —
    NOT a hidden-bug skip per the Zero Skips Policy; the skip predicate is
    deterministic and the message is explicit.
    """
    stations = tmp_path / "stations.txt"
    stations.write_text("19999\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    # Explicitly DO NOT pass the bypass env var.
    result = _run_wrapper(
        [
            "--horizon",
            "pentad",
            "--stations-file",
            str(stations),
            "--out-dir",
            str(out_dir),
        ],
        env={
            "PGHOST": "127.0.0.1",
            "PGUSER": "postgres",
            "PGDATABASE": "test",
        },
        bypass_location_guard=False,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    # The documented error message includes "deployment-server containers".
    assert "deployment-server containers" in combined or "deployment server" in combined


# ---------------------------------------------------------------------------
# 4. Fixture sanity (sentinel-only)
# ---------------------------------------------------------------------------


def test_pentad_sample_fixture_sentinel_only():
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    assert csv.is_file(), f"fixture missing: {csv}"
    text = csv.read_text(encoding="utf-8")
    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


def test_decade_sample_fixture_sentinel_only():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    assert csv.is_file(), f"fixture missing: {csv}"
    text = csv.read_text(encoding="utf-8")
    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


def test_pentad_sample_manifest_validates():
    """The fixture manifest passes P0 manifest validation."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    result = _common.validate_manifest(csv, "runoff_period")
    assert result["export_type"] == "runoff_period"
    assert result["row_count"] == "3"
    assert result["station_count"] == "1"
    assert result["date_min"] == "2026-01-01"
    assert result["date_max"] == "2026-01-11"


def test_decade_sample_manifest_validates():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    result = _common.validate_manifest(csv, "runoff_period")
    assert result["export_type"] == "runoff_period"
    assert result["row_count"] == "3"
    assert result["station_count"] == "1"
    assert result["date_min"] == "2026-01-01"
    assert result["date_max"] == "2026-01-21"


def test_pentad_fixture_rejects_wrong_export_type():
    """Manifest validation should fail if the wrapper asks for a different type."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        _common.validate_manifest(csv, "hydrograph_period")


def test_pentad_csv_uses_discharge_not_discharge_avg():
    """Schema correction check: the CSV header uses ``discharge`` (DB column
    name), NOT ``discharge_avg`` (the CSV-source migrator name).

    This locks in the brief's load-bearing column-name correction at the
    fixture level so a future contributor cannot silently rename it back.
    """
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    header = csv.read_text(encoding="utf-8").splitlines()[0]
    cols = [c.strip() for c in header.split(",")]
    assert "discharge" in cols, f"expected 'discharge' in CSV header, got: {cols}"
    assert "discharge_avg" not in cols, (
        f"CSV header must not use legacy CSV-migrator column 'discharge_avg', got: {cols}"
    )


def test_decade_csv_uses_discharge_not_discharge_avg():
    """Same lock-in for the decade fixture (see pentad analog above)."""
    csv = _FIXTURE_DIR / "decade_sample.csv"
    header = csv.read_text(encoding="utf-8").splitlines()[0]
    cols = [c.strip() for c in header.split(",")]
    assert "discharge" in cols
    assert "discharge_avg" not in cols
