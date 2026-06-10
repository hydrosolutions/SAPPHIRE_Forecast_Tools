"""Tests for the P2a SERVER-SIDE import wrapper +
``migration_py.runoff_period``.

Covers:
- Bash wrapper CLI surface (--help, missing env file rejection, --from-export
  + --horizon required-flag enforcement, --station-filter contract).
- ``_build_record`` payload shape per HORIZON: required fields, optional
  ``discharge`` / ``predictor`` (NULL handling).
- DB column name correction: ``discharge`` (NOT ``discharge_avg``).
- ``_read_filtered_records_with_manifest`` filters (cutoff, station_filter),
  manifest validation failures bubble up cleanly.
- ``main()`` dry-run output shape per the §4.4 inventory contract.
- Stdlib-only import audit covers runoff_period.

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §6.1 canary.

Sentinel codes (19999-class) only.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common, runoff_period  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_runoff_period_history.sh"
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
# CSV helper
# ---------------------------------------------------------------------------

_DEFAULT_HEADER = [
    "horizon_type",
    "code",
    "date",
    "discharge",
    "predictor",
    "horizon_value",
    "horizon_in_year",
]


def _write_csv_and_manifest(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    horizon: str = "pentad",
    header: list[str] | None = None,
    manifest_override: dict[str, str] | None = None,
    csv_name: str = "fixture.csv",
) -> tuple[Path, Path]:
    """Write a CSV + matching manifest. Returns (csv_path, manifest_path).

    If `manifest_override` is supplied, those keys override the auto-computed
    manifest fields (useful for testing manifest validation failures).
    """
    if header is None:
        header = list(_DEFAULT_HEADER)
    csv_path = tmp_path / csv_name
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(row.get(col, "") for col in header))
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Auto-compute manifest fields from the written rows.
    codes: set[str] = set()
    dates: list[str] = []
    for row in rows:
        c = (row.get("code") or "").strip()
        if c:
            codes.add(c)
        d = (row.get("date") or "").strip()
        if d:
            dates.append(d)
    manifest_fields = {
        "export_type": "runoff_period",
        "horizon": horizon,
        "row_count": str(len(rows)),
        "station_count": str(len(codes)),
        "date_min": min(dates) if dates else "1970-01-01",
        "date_max": max(dates) if dates else "1970-01-01",
    }
    if manifest_override:
        manifest_fields.update(manifest_override)

    manifest_path = tmp_path / (csv_name + ".manifest")
    manifest_path.write_text(
        "\n".join(f"{k}={v}" for k, v in manifest_fields.items()) + "\n",
        encoding="utf-8",
    )
    return csv_path, manifest_path


# ===========================================================================
# 1. Wrapper CLI surface
# ===========================================================================


def test_wrapper_exists_on_disk():
    assert _WRAPPER.is_file(), f"wrapper missing: {_WRAPPER}"


def test_wrapper_help_returns_zero_and_prints_usage():
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout
    # All P2a-specific flags are documented.
    assert "--from-export" in result.stdout
    assert "--horizon" in result.stdout
    assert "--station-filter" in result.stdout


def test_wrapper_rejects_missing_env_file(tmp_path):
    nonexistent = tmp_path / "no_such_env_file"
    csv = tmp_path / "x.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            str(_WRAPPER),
            str(nonexistent),
            "--from-export",
            str(csv),
            "--horizon",
            "pentad",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "env file" in combined or "not found" in combined


def test_wrapper_rejects_missing_from_export(tmp_path):
    env = tmp_path / "fake.env"
    env.write_text("# stub\n", encoding="utf-8")
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(env), "--horizon", "pentad"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "from-export" in combined or "required" in combined


def test_wrapper_rejects_missing_horizon(tmp_path):
    env = tmp_path / "fake.env"
    env.write_text("# stub\n", encoding="utf-8")
    csv = tmp_path / "x.csv"
    csv.write_text("\n", encoding="utf-8")
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(env), "--from-export", str(csv)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "horizon" in combined


def test_wrapper_rejects_invalid_horizon(tmp_path):
    env = tmp_path / "fake.env"
    env.write_text("# stub\n", encoding="utf-8")
    csv = tmp_path / "x.csv"
    csv.write_text("\n", encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            str(_WRAPPER),
            str(env),
            "--from-export",
            str(csv),
            "--horizon",
            "day",  # not pentad/decade — DAY is P1a's territory
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0


def test_wrapper_help_documents_station_filter_contract():
    """The --station-filter flag is the P0 binding contract; must be documented."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "-h"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--station-filter" in result.stdout
    assert (
        "contract" in result.stdout.lower()
        or "interface" in result.stdout.lower()
        or "binding" in result.stdout.lower()
    )


# ===========================================================================
# 2. Fixture sanity
# ===========================================================================


def test_pentad_sample_fixture_only_sentinel_codes():
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    assert csv.is_file()
    text = csv.read_text(encoding="utf-8")
    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


def test_decade_sample_fixture_only_sentinel_codes():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    assert csv.is_file()
    text = csv.read_text(encoding="utf-8")
    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


# ===========================================================================
# 3. _build_record — payload shape
# ===========================================================================


def test_build_record_pentad_includes_required_fields():
    """All RunoffBase required fields are emitted for a PENTAD record."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "discharge": "12.5",
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"
    assert rec["code"] == "19999"
    assert rec["date"] == "2026-01-01"
    assert rec["horizon_value"] == 1
    assert rec["horizon_in_year"] == 1
    assert rec["discharge"] == 12.5


def test_build_record_decade_horizon_type_value():
    """DECADE input produces ``horizon_type='decade'``."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
    }
    rec = runoff_period._build_record(row, "decade")
    assert rec is not None
    assert rec["horizon_type"] == "decade"


def test_build_record_rejects_unknown_horizon():
    """Anything other than 'pentad'/'decade' returns None."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
    }
    # 'month' is reserved for the LT regenerate hook; this module rejects it.
    assert runoff_period._build_record(row, "month") is None
    assert runoff_period._build_record(row, "day") is None


def test_build_record_returns_none_for_missing_required():
    """Row missing code / date / one of the horizon-required ints -> None."""
    base = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
    }
    # Missing code.
    bad = dict(base)
    bad["code"] = ""
    assert runoff_period._build_record(bad, "pentad") is None
    # Missing date.
    bad = dict(base)
    bad["date"] = ""
    assert runoff_period._build_record(bad, "pentad") is None
    # Unparseable date.
    bad = dict(base)
    bad["date"] = "not-a-date"
    assert runoff_period._build_record(bad, "pentad") is None
    # Missing horizon_value (int).
    bad = dict(base)
    bad["horizon_value"] = ""
    assert runoff_period._build_record(bad, "pentad") is None
    # Missing horizon_in_year.
    bad = dict(base)
    bad["horizon_in_year"] = ""
    assert runoff_period._build_record(bad, "pentad") is None


def test_build_record_pentad_includes_predictor_when_present():
    """When predictor is non-NULL in the CSV, it appears in the record."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "discharge": "12.5",
        "predictor": "11.8",
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["discharge"] == 12.5
    assert rec["predictor"] == 11.8


def test_build_record_excludes_null_discharge_and_predictor():
    """``discharge`` / ``predictor`` with NULL/empty/NaN source value are OMITTED."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "discharge": "",  # NULL
        "predictor": "nan",  # null-like
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    # NULL fields must be ABSENT, not present-with-null.
    assert "discharge" not in rec
    assert "predictor" not in rec
    # Required fields are still present.
    assert rec["horizon_type"] == "pentad"
    assert rec["code"] == "19999"


def test_build_record_uses_discharge_not_discharge_avg():
    """Schema correction: the payload key is ``discharge``, not ``discharge_avg``.

    This is the load-bearing schema check called out in the brief: an
    earlier version mistakenly used ``discharge_avg``. The actual DB column
    on ``runoffs`` is ``discharge`` (and the API field on RunoffBase is
    ``discharge``). The CSV header uses ``discharge`` as well — there is
    no rename anywhere in this module.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "discharge": "12.5",
        "discharge_avg": "99.9",  # legacy CSV-source migrator name — should NOT be used
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["discharge"] == 12.5
    # The legacy name must NEVER leak into the payload.
    assert "discharge_avg" not in rec


def test_build_record_rejects_non_finite_floats():
    """NaN / +/-Inf source values are rejected as None (cannot JSON-serialize)."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "discharge": "inf",
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert "discharge" not in rec


# ===========================================================================
# 4. _read_filtered_records_with_manifest — filtering + manifest validation
# ===========================================================================


def test_read_filtered_records_happy_path_pentad(tmp_path):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "discharge": "12.5",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "discharge": "13.1",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, dmin, dmax = runoff_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff=None, station_filter=None
    )
    assert counters["source_row_count"] == 2
    assert counters["filtered_row_count"] == 2
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-06"
    assert all(r["horizon_type"] == "pentad" for r in records)
    # Every record has ``discharge`` populated (since the source had it).
    assert all("discharge" in r for r in records)


def test_read_filtered_records_station_filter(tmp_path):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
            },
            {
                "code": "00000",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, _, _ = runoff_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff=None, station_filter="19999"
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 2
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    for rec in records:
        assert rec["code"] == "19999"


def test_read_filtered_records_cutoff_drops_on_or_after(tmp_path):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
            },
            {
                "code": "19999",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
            },
        ],
        horizon="pentad",
    )
    records, counters, _, _, _ = runoff_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff="2026-01-06", station_filter=None
    )
    # Only the row strictly < cutoff survives.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2026-01-01"


def test_read_filtered_records_rejects_csv_missing_required_columns(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("code,date\n19999,2026-01-01\n", encoding="utf-8")
    manifest = tmp_path / "bad.csv.manifest"
    manifest.write_text(
        "export_type=runoff_period\nrow_count=1\nstation_count=1\n"
        "date_min=2026-01-01\ndate_max=2026-01-01\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing required column"):
        runoff_period._read_filtered_records_with_manifest(
            bad, manifest, "pentad", cutoff=None, station_filter=None
        )


def test_read_filtered_records_manifest_missing_raises(tmp_path):
    """If the manifest file is absent, validation raises a ManifestMissingError."""
    csv = tmp_path / "lonely.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    with pytest.raises(_common.ManifestMissingError):
        runoff_period._read_filtered_records_with_manifest(
            csv,
            tmp_path / "lonely.csv.manifest",
            "pentad",
            cutoff=None,
            station_filter=None,
        )


def test_read_filtered_records_manifest_row_count_mismatch_raises(tmp_path):
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
        ],
        horizon="pentad",
        manifest_override={"row_count": "999"},
    )
    with pytest.raises(_common.ManifestRowCountMismatchError):
        runoff_period._read_filtered_records_with_manifest(
            csv,
            csv.with_name(csv.name + ".manifest"),
            "pentad",
            cutoff=None,
            station_filter=None,
        )


def test_read_filtered_records_manifest_wrong_export_type_raises(tmp_path):
    """A manifest claiming hydrograph_period on a runoff_period wrapper is rejected."""
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
        ],
        horizon="pentad",
        manifest_override={"export_type": "hydrograph_period"},
    )
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        runoff_period._read_filtered_records_with_manifest(
            csv,
            csv.with_name(csv.name + ".manifest"),
            "pentad",
            cutoff=None,
            station_filter=None,
        )


# ===========================================================================
# 5. main() dry-run output shape
# ===========================================================================


def test_main_dry_run_with_station_filter_reduces_filtered_count(tmp_path, capsys):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
            },
            {
                "code": "00000",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
            },
        ],
        horizon="pentad",
    )
    exit_code = runoff_period.main(
        [
            "--csv-path",
            str(csv),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/runoff/",
            "--station-filter",
            "19999",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SOURCE_ROW_COUNT=3" in out
    assert "FILTERED_ROW_COUNT=2" in out
    assert "MODE=full-import" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out
    assert "TARGET_TABLE=runoffs" in out
    assert "HORIZON_TYPE=pentad" in out  # lowercase per Q4 (was uppercase pre-review)


def test_main_dry_run_with_cutoff_emits_pre_cutoff(tmp_path, capsys):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
            },
        ],
        horizon="decade",
    )
    exit_code = runoff_period.main(
        [
            "--csv-path",
            str(csv),
            "--horizon",
            "decade",
            "--api-url",
            "http://localhost:8002/runoff/",
            "--cutoff",
            "2026-01-06",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=pre-cutoff (cutoff=2026-01-06)" in out
    assert "FILTERED_ROW_COUNT=1" in out
    assert "HORIZON_TYPE=decade" in out  # lowercase per Q4 (was uppercase pre-review)


def test_main_dry_run_default_inventory_lists_enrichment_only_policy(tmp_path, capsys):
    """The default inventory line is
    ``SAFE_WRITE_POLICY=enrichment-only (default)`` — same as P2b."""
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
            },
        ],
        horizon="pentad",
    )
    exit_code = runoff_period.main(
        [
            "--csv-path",
            str(csv),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/runoff/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SAFE_WRITE_POLICY=enrichment-only (default)" in out


def test_main_missing_csv_returns_nonzero(tmp_path):
    exit_code = runoff_period.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/runoff/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


def test_main_missing_manifest_returns_nonzero(tmp_path):
    csv = tmp_path / "lonely.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    exit_code = runoff_period.main(
        [
            "--csv-path",
            str(csv),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/runoff/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ===========================================================================
# 6. Stdlib-only audit covers runoff_period
# ===========================================================================


def test_runoff_period_module_imports_only_stdlib_and_intra_package():
    """The runoff_period module passes the P0 stdlib-only audit alongside
    _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


# ===========================================================================
# 7. Shipped fixtures round-trip through the parser
# ===========================================================================


def test_shipped_pentad_fixture_round_trips():
    """The shipped ``pentad_sample.csv`` + manifest parses through the helper."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    manifest = _FIXTURE_DIR / "pentad_sample.csv.manifest"
    records, counters, codes, dmin, dmax = runoff_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-11"
    for r in records:
        assert r["horizon_type"] == "pentad"
        # The shipped fixture populates both discharge and predictor.
        assert "discharge" in r
        assert "predictor" in r


def test_shipped_decade_fixture_round_trips():
    csv = _FIXTURE_DIR / "decade_sample.csv"
    manifest = _FIXTURE_DIR / "decade_sample.csv.manifest"
    records, counters, codes, dmin, dmax = runoff_period._read_filtered_records_with_manifest(
        csv, manifest, "decade", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-21"
    for r in records:
        assert r["horizon_type"] == "decade"


def test_build_record_rejects_row_with_mismatched_horizon_type():
    """Review feedback: when the CSV row carries its own ``horizon_type`` column
    and it disagrees with the CLI flag, drop the row instead of silently
    relabeling the payload from the CLI value."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "horizon_type": "decade",  # disagrees with the CLI value below
    }
    assert runoff_period._build_record(row, "pentad") is None


def test_build_record_accepts_row_with_matching_horizon_type():
    """When the CSV row's horizon_type agrees with the CLI flag, the record
    is built normally."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "horizon_type": "pentad",
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"


def test_build_record_normalizes_case_in_row_horizon_type():
    """Row horizon_type is case-normalized before comparison."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "horizon_type": "PENTAD",
    }
    rec = runoff_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"


def test_manifest_path_arg_removed():
    """Review feedback: --manifest-path CLI argument was decorative
    (existence-check-only; actual validation derives the sidecar path from
    --csv-path). It was removed."""
    parser = runoff_period._build_arg_parser()
    actions = {a.option_strings[0] for a in parser._actions if a.option_strings}
    assert "--csv-path" in actions
    assert "--manifest-path" not in actions
    # And the bash wrapper must stop passing it.
    wrapper_text = (_REPO_ROOT / "bin" / "initialize_runoff_period_history.sh").read_text(
        encoding="utf-8"
    )
    assert "--manifest-path" not in wrapper_text


def test_dry_run_output_uses_lowercase_horizon_type():
    """Review feedback: dry-run HORIZON_TYPE was uppercase; enum lock (Q4)
    requires lowercase to match the API/DB."""
    import contextlib
    import io

    counters = {
        "source_row_count": 0,
        "filtered_row_count": 0,
        "skipped_parse": 0,
        "skipped_cutoff": 0,
        "skipped_station": 0,
        "skipped_horizon_mismatch": 0,
    }
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        runoff_period._print_dry_run_inventory(
            csv_path=Path("/tmp/x.csv"),
            horizon="pentad",
            counters=counters,
            distinct_codes=set(),
            date_min=None,
            date_max=None,
            mode="full-import",
            cutoff=None,
        )
    out = buf.getvalue()
    assert "HORIZON_TYPE=pentad" in out
    assert "HORIZON_TYPE=PENTAD" not in out
    # And the new counter is surfaced.
    assert "SKIPPED_HORIZON_MISMATCH=0" in out
