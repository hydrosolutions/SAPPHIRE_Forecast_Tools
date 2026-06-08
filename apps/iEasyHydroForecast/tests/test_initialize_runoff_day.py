"""Tests for the P1a runoff DAY migration wrapper + Python POST module.

Covers:
- bash wrapper CLI surface (`--help`, missing env file rejection).
- migration_py.runoff_day record-builder unit behavior.
- migration_py.runoff_day filtering (cutoff, station-filter, NULL discharge).
- Dry-run inventory output for the empty-CSV / full-import branch.

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §5.1 canary.
"""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import runoff_day  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_runoff_day_history.sh"
_FIXTURE_CSV = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "runoff_day"
    / "sample.csv"
)


# ---------------------------------------------------------------------------
# Wrapper CLI surface
# ---------------------------------------------------------------------------


def test_wrapper_help_returns_zero_and_prints_usage():
    """`bash bin/initialize_runoff_day_history.sh --help` exits 0 and shows usage."""
    assert _WRAPPER.is_file(), f"wrapper missing: {_WRAPPER}"
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout, f"stdout missing 'Usage': {result.stdout!r}"
    # The binding contract flag must appear in the help text.
    assert "--station-filter" in result.stdout


def test_wrapper_rejects_missing_env_file(tmp_path):
    """Wrapper exits non-zero with descriptive error when env file is missing."""
    assert _WRAPPER.is_file()
    nonexistent = tmp_path / "no_such_env_file"
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(nonexistent)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0, "expected non-zero exit"
    combined = (result.stdout + result.stderr).lower()
    assert "env file" in combined or "not found" in combined, (
        f"expected error mentioning env file / not found; got "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


# ---------------------------------------------------------------------------
# Fixture sanity
# ---------------------------------------------------------------------------


def test_sample_fixture_present_and_has_sentinel_codes_only():
    """The shipped runoff_day sample fixture has only sentinel station codes."""
    assert _FIXTURE_CSV.is_file(), f"fixture missing: {_FIXTURE_CSV}"
    text = _FIXTURE_CSV.read_text(encoding="utf-8")
    # Every 5-digit number must be a sentinel — guarded separately by
    # test_migration_fixture_guard.py; quick sanity check here too.
    import re

    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


# ---------------------------------------------------------------------------
# Python module: _build_record (unit)
# ---------------------------------------------------------------------------


def test_build_record_payload_shape_and_keys():
    """_build_record returns the exact universal-safe-write payload for DAY."""
    rec = runoff_day._build_record("19999", "2026-01-15", 12.5)
    assert rec == {
        "horizon_type": "day",
        "code": "19999",
        "date": "2026-01-15",
        "discharge": 12.5,
        "horizon_value": 15,  # day of month
        "horizon_in_year": 15,  # day of year (Jan 15 = day 15)
    }
    # predictor MUST NOT be present (DAY rows have no predictor under safe-write).
    assert "predictor" not in rec


def test_build_record_horizon_in_year_for_march_first():
    """horizon_in_year is day-of-year; verify a non-Jan date."""
    rec = runoff_day._build_record("19999", "2026-03-01", 7.0)
    # 2026 is not a leap year; Jan(31) + Feb(28) + 1 = 60.
    assert rec["horizon_in_year"] == 60
    assert rec["horizon_value"] == 1  # day of month


# ---------------------------------------------------------------------------
# Python module: _parse_discharge (unit)
# ---------------------------------------------------------------------------


def test_parse_discharge_handles_blank_and_null_like():
    """Blank, 'nan', 'none', 'null' all parse to None (skipped at row level)."""
    for raw in ("", " ", "nan", "NaN", "None", "NULL", "null"):
        assert runoff_day._parse_discharge(raw) is None


def test_parse_discharge_parses_floats():
    assert runoff_day._parse_discharge("12.34") == 12.34
    assert runoff_day._parse_discharge("0") == 0.0
    assert runoff_day._parse_discharge(" 3.0  ") == 3.0


def test_parse_discharge_rejects_garbage():
    assert runoff_day._parse_discharge("not-a-number") is None


# ---------------------------------------------------------------------------
# Python module: _read_filtered_records (unit)
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, rows: list[tuple[str, str, str]]) -> Path:
    """Write a runoff_day-style CSV from (code, date, discharge) tuples."""
    csv_path = tmp_path / "fixture.csv"
    lines = ["code,date,discharge"]
    for code, date, discharge in rows:
        lines.append(f"{code},{date},{discharge}")
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path


def test_read_filtered_records_no_filter_loads_all_valid_rows(tmp_path):
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "12.34"),
            ("19999", "2026-01-02", "11.22"),
            ("00000", "2026-01-03", "5.0"),
        ],
    )
    records, counters, codes, dmin, dmax = runoff_day._read_filtered_records(
        csv, cutoff=None, station_filter=None
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 3
    assert counters["skipped_cutoff"] == 0
    assert counters["skipped_station"] == 0
    assert counters["skipped_null"] == 0
    assert codes == {"19999", "00000"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-03"
    assert len(records) == 3
    assert records[0]["horizon_type"] == "day"


def test_read_filtered_records_station_filter_keeps_only_match(tmp_path):
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "12.34"),
            ("19999", "2026-01-02", "11.22"),
            ("00000", "2026-01-03", "5.0"),
        ],
    )
    records, counters, codes, _, _ = runoff_day._read_filtered_records(
        csv, cutoff=None, station_filter="19999"
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 2
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    for rec in records:
        assert rec["code"] == "19999"


def test_read_filtered_records_cutoff_drops_on_or_after(tmp_path):
    """Cutoff is strict: rows with date >= cutoff are dropped (pre-cutoff)."""
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "1.0"),
            ("19999", "2026-01-02", "2.0"),
            ("19999", "2026-01-03", "3.0"),
        ],
    )
    records, counters, _, _, _ = runoff_day._read_filtered_records(
        csv, cutoff="2026-01-02", station_filter=None
    )
    # Only 2026-01-01 strictly < 2026-01-02 survives.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2026-01-01"


def test_read_filtered_records_skips_null_discharge_not_posted_as_null(tmp_path):
    """Universal safe-write rule: rows with NULL discharge are skipped, NOT
    sent with discharge=null."""
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "12.34"),
            ("19999", "2026-01-02", ""),  # null discharge
            ("19999", "2026-01-03", "nan"),  # null-like
            ("19999", "2026-01-04", "garbage"),  # parse failure
        ],
    )
    records, counters, _, _, _ = runoff_day._read_filtered_records(
        csv, cutoff=None, station_filter=None
    )
    assert counters["source_row_count"] == 4
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_null"] == 3
    # No record has discharge=None.
    for rec in records:
        assert rec["discharge"] is not None


def test_read_filtered_records_rejects_csv_missing_required_columns(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("code,date\n19999,2026-01-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required column"):
        runoff_day._read_filtered_records(bad, cutoff=None, station_filter=None)


# ---------------------------------------------------------------------------
# Python module: main() dry-run output
# ---------------------------------------------------------------------------


def test_main_dry_run_empty_csv_emits_full_import_mode(tmp_path, capsys):
    """Empty CSV (header only) -> dry-run prints MODE=full-import; no POST."""
    csv = tmp_path / "empty.csv"
    csv.write_text("code,date,discharge\n", encoding="utf-8")
    exit_code = runoff_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/runoff/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "MODE=full-import" in captured.out
    assert "DRY RUN: no POSTs attempted." in captured.out
    assert "SOURCE_ROW_COUNT=0" in captured.out
    assert "FILTERED_ROW_COUNT=0" in captured.out


def test_main_dry_run_with_station_filter_reduces_filtered_count(tmp_path, capsys):
    """Station filter reduces FILTERED_ROW_COUNT below SOURCE_ROW_COUNT."""
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "12.34"),
            ("19999", "2026-01-02", "11.22"),
            ("00000", "2026-01-03", "5.0"),
        ],
    )
    exit_code = runoff_day.main(
        [
            "--csv-path",
            str(csv),
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
    assert "MODE=full-import" in out  # no cutoff passed -> full-import
    # The inventory line confirms the count is redacted (case-insensitive).
    assert "REDACTED" in out or "redacted" in out.lower()
    # No raw non-sentinel station code in stdout (sentinel codes are
    # allowed by the redaction policy but not printed individually).
    # The codes 19999 / 00000 only appear in the source-file path or
    # similar; check that the count line is the only place a number
    # appears in association with stations.
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out


def test_main_dry_run_with_cutoff_emits_pre_cutoff_mode(tmp_path, capsys):
    """When a cutoff is provided, MODE=pre-cutoff is reported."""
    csv = _write_csv(
        tmp_path,
        [
            ("19999", "2026-01-01", "12.34"),
            ("19999", "2026-01-02", "11.22"),
            ("19999", "2026-01-03", "5.0"),
        ],
    )
    exit_code = runoff_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/runoff/",
            "--cutoff",
            "2026-01-02",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=pre-cutoff (cutoff=2026-01-02)" in out
    assert "FILTERED_ROW_COUNT=1" in out  # only 2026-01-01 < 2026-01-02


def test_main_missing_csv_returns_nonzero(tmp_path, capsys):
    exit_code = runoff_day.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--api-url",
            "http://localhost:8002/runoff/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ---------------------------------------------------------------------------
# Wrapper end-to-end: --help/--station-filter flag are documented
# ---------------------------------------------------------------------------


def test_wrapper_help_documents_station_filter_contract():
    """The --station-filter flag is documented in the help output (P0 contract)."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "-h"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "--station-filter" in result.stdout
    # Should also mention the binding contract / forward compatibility.
    assert (
        "contract" in result.stdout.lower()
        or "interface" in result.stdout.lower()
        or "binding" in result.stdout.lower()
    )


# ---------------------------------------------------------------------------
# Marker: stdlib-only audit covers runoff_day.py
# ---------------------------------------------------------------------------


def test_runoff_day_module_imports_only_stdlib_and_intra_package():
    """The runoff_day module passes the P0 stdlib-only audit alongside _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


# A small reminder of the captured-output pattern for future contributors.
def test_dry_run_inventory_uses_uniform_print(capsys):
    """Smoke: _print_dry_run_inventory writes to stdout (capturable by tests)."""
    runoff_day._print_dry_run_inventory(
        csv_path=Path("/tmp/x.csv"),  # noqa: S108
        counters={
            "source_row_count": 0,
            "filtered_row_count": 0,
            "skipped_null": 0,
            "skipped_parse": 0,
            "skipped_cutoff": 0,
            "skipped_station": 0,
        },
        distinct_codes=set(),
        date_min=None,
        date_max=None,
        mode="full-import",
        cutoff=None,
    )
    out = capsys.readouterr().out
    assert "MODE=full-import" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=0" in out


# Silence unused-import warning for io.StringIO if not actually used.
_ = io.StringIO
