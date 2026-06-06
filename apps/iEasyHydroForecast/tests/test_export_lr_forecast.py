"""Tests for the P4a LAPTOP-SIDE export wrapper

``bin/export_lr_forecast_history.sh``.

These tests exercise CLI surface, argument validation, location guard
(Stage E #6), and fixture-manifest round trip. The wrapper's actual psql
COPY path is integration-only and is NOT exercised here (architecture
§Q7 — disposable integration is a separate sprint; the export script's
real-DB code path is operator-tested via the runbook §6.3 canary).

Sentinel codes (19999-class) only. No real station codes anywhere in this
file.
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

_EXPORT_WRAPPER = _REPO_ROOT / "bin" / "export_lr_forecast_history.sh"
_FIXTURE_DIR = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "lr_forecast"
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
    ``_P4A_EXPORT_SKIP_LOCATION_GUARD=1`` testing-only env hook) so the
    tests can exercise the wrapper's downstream validation paths even when
    the developer laptop has the SAPPHIRE stack running locally. The
    location guard itself is exercised in a dedicated positive test.
    """
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    if bypass_location_guard:
        full_env["_P4A_EXPORT_SKIP_LOCATION_GUARD"] = "1"
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
    # All required + key optional flags are documented.
    assert "--horizon" in result.stdout
    assert "--output-dir" in result.stdout
    assert "--station-list-file" in result.stdout


def test_export_wrapper_short_help_works():
    result = _run_wrapper(["-h"])
    assert result.returncode == 0
    assert "Usage" in result.stdout


def test_export_wrapper_help_documents_station_filter_contract():
    """The --station-list-file flag is the P0 binding contract (export-side
    equivalent of --station-filter); the help text MUST tell the operator
    what it does so they can wire it to their deployment's stations list."""
    result = _run_wrapper(["--help"])
    assert result.returncode == 0
    # Collapse whitespace so "one\n   per line" matches "one per line"
    # (the help block wraps long descriptions across multiple lines).
    out_lower = " ".join(result.stdout.lower().split())
    assert "--station-list-file" in result.stdout
    # The contract: filter cross-org codes from the COPY, and describe
    # the file format (one per line). Both expectations must be present.
    assert "one per line" in out_lower or "one code per line" in out_lower
    assert "cross-org" in out_lower or "filter" in out_lower or "interface" in out_lower


# ---------------------------------------------------------------------------
# 2. Argument validation
# ---------------------------------------------------------------------------


def test_export_wrapper_rejects_no_args():
    """First arg must be the env_file_path (positional, required)."""
    result = _run_wrapper([])
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "env_file_path" in combined or "required" in combined


def test_export_wrapper_rejects_missing_env_file(tmp_path):
    nonexistent = tmp_path / "no_such_env_file"
    result = _run_wrapper(
        [str(nonexistent), "--horizon", "pentad"],
        env={
            "PGHOST": "127.0.0.1",
            "PGUSER": "postgres",
            "PGDATABASE": "test",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "env file" in combined or "not found" in combined


def test_export_wrapper_rejects_missing_horizon(tmp_path):
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    result = _run_wrapper([str(env_file)])
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "--horizon" in combined or "horizon" in combined


def test_export_wrapper_rejects_invalid_horizon(tmp_path):
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    result = _run_wrapper([str(env_file), "--horizon", "month"])  # not pentad/decade
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "horizon" in combined


def test_export_wrapper_rejects_day_horizon(tmp_path):
    """LR forecasts do not exist at DAY horizon — explicitly rejected."""
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    result = _run_wrapper([str(env_file), "--horizon", "day"])
    assert result.returncode != 0


def test_export_wrapper_rejects_nonexistent_station_list_file(tmp_path):
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    nonexistent = tmp_path / "no_such_stations.txt"
    out_dir = tmp_path / "out"
    result = _run_wrapper(
        [
            str(env_file),
            "--horizon",
            "pentad",
            "--station-list-file",
            str(nonexistent),
            "--output-dir",
            str(out_dir),
        ],
        env={
            # Have to provide PG* env so we get past the env-var check.
            "PGHOST": "127.0.0.1",
            "PGUSER": "postgres",
            "PGDATABASE": "test",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "station" in combined or "not found" in combined


def test_export_wrapper_rejects_missing_pg_env(tmp_path):
    """When PGHOST/PGUSER/PGDATABASE are unset, refuse before doing anything."""
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    # Explicitly UNSET PG vars. Use a minimal env. Bypass location guard
    # so this developer-laptop test reaches the PG-env check.
    result = subprocess.run(
        [
            "bash",
            str(_EXPORT_WRAPPER),
            str(env_file),
            "--horizon",
            "pentad",
            "--output-dir",
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env={
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
            "_P4A_EXPORT_SKIP_LOCATION_GUARD": "1",
        },
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "pghost" in combined or "pguser" in combined or "pgdatabase" in combined


def test_export_wrapper_rejects_apostrophe_in_station_list(tmp_path):
    """SQL-injection guard: a station code containing an apostrophe is rejected."""
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    stations = tmp_path / "stations.txt"
    # Intentionally include an apostrophe; the wrapper should reject.
    stations.write_text("19999\nbad'code\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    result = _run_wrapper(
        [
            str(env_file),
            "--horizon",
            "pentad",
            "--station-list-file",
            str(stations),
            "--output-dir",
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
            [
                "docker",
                "ps",
                "--filter",
                "name=sapphire-postprocessing-db",
                "--quiet",
            ],
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
    env_file = tmp_path / "stub.env"
    env_file.write_text("# stub\n", encoding="utf-8")
    # Explicitly DO NOT pass the bypass env var.
    result = _run_wrapper(
        [
            str(env_file),
            "--horizon",
            "pentad",
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
    result = _common.validate_manifest(csv, "lr_forecast")
    assert result["export_type"] == "lr_forecast"
    assert result["row_count"] == "3"
    assert result["station_count"] == "1"
    assert result["date_min"] == "2026-01-01"
    assert result["date_max"] == "2026-01-11"


def test_decade_sample_manifest_validates():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    result = _common.validate_manifest(csv, "lr_forecast")
    assert result["export_type"] == "lr_forecast"
    assert result["row_count"] == "3"
    assert result["station_count"] == "1"
    assert result["date_min"] == "2026-01-01"
    assert result["date_max"] == "2026-01-21"


def test_pentad_fixture_rejects_wrong_export_type():
    """Manifest validation should fail if the wrapper asks for a different type."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        _common.validate_manifest(csv, "hydrograph_period")


# ---------------------------------------------------------------------------
# 5. Defensive checks specific to LR
# ---------------------------------------------------------------------------


def test_pentad_fixture_csv_has_no_model_type_column():
    """LR forecasts do not have a model_type column — defensive check."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    header = csv.read_text(encoding="utf-8").splitlines()[0]
    cols = [c.strip() for c in header.split(",")]
    assert "model_type" not in cols, (
        "lr_forecasts has NO model_type column (unique key is "
        "(horizon_type, code, date)); a model_type column in the fixture "
        "would indicate cross-pollution from a forecasts/skill_metrics CSV."
    )


def test_decade_fixture_csv_has_no_model_type_column():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    header = csv.read_text(encoding="utf-8").splitlines()[0]
    cols = [c.strip() for c in header.split(",")]
    assert "model_type" not in cols


def test_pentad_fixture_uses_lowercase_horizon_type():
    """Architecture §Q4 lock: lr_forecasts.horizon_type is the lowercase
    'pentad' / 'decade' enum. Uppercase would be rejected at the Pydantic
    boundary (HorizonType in postprocessing.app.models)."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    text = csv.read_text(encoding="utf-8")
    # Data rows (skip header).
    for line in text.splitlines()[1:]:
        if not line.strip():
            continue
        ht_cell = line.split(",", 1)[0].strip()
        assert ht_cell == "pentad", f"horizon_type cell {ht_cell!r} is not the lowercase 'pentad'"


def test_decade_fixture_uses_lowercase_horizon_type():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    text = csv.read_text(encoding="utf-8")
    for line in text.splitlines()[1:]:
        if not line.strip():
            continue
        ht_cell = line.split(",", 1)[0].strip()
        assert ht_cell == "decade", f"horizon_type cell {ht_cell!r} is not the lowercase 'decade'"


def test_export_rejects_zero_row_filter():
    """Review feedback: zero-row exports must abort with a clear error and no
    manifest written. String-level check on the script source confirms the
    early-exit lives between row_count computation and manifest emission.

    A live-DB test would require a running PostgreSQL with a known-empty
    filter, which is out of scope per architecture §Q7."""
    src = (_REPO_ROOT / "bin" / "export_lr_forecast_history.sh").read_text(encoding="utf-8")
    # The guard checks row_count -eq 0 and exits.
    assert 'row_count" -eq 0' in src or 'row_count" -eq "0"' in src or "row_count -eq 0" in src
    # The operator-facing error message is specific.
    assert "no rows matched filter" in src
    # The early-exit lives BEFORE the manifest is written. Anchor on the
    # heredoc that writes the manifest (the docstring above also mentions
    # export_type=lr_forecast, so we need a more specific marker).
    pre_manifest, _, post_manifest = src.partition('cat > "${manifest_path}"')
    assert "no rows matched filter" in pre_manifest
    assert "no rows matched filter" not in post_manifest
