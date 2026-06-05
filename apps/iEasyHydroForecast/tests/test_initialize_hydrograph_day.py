"""Tests for the P3 hydrograph DAY migration wrapper + Python POST module.

Covers:
- bash wrapper CLI surface (`--help`, missing env file rejection, station
  filter contract).
- Dynamic year-column discovery (2, 3, 0, 1 year-column cases).
- migration_py.hydrograph_day record-builder unit behavior (year-column
  mapping, quantile column normalization '5%' -> 'q05').
- Filtering (cutoff, station-filter, NULL stat handling — enrichment-only).
- Dry-run inventory output including the P3-specific
  HYDROGRAPH_YEAR_MAPPING={previous: <year>, current: <year>} line.

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §5.4 canary.
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

from migration_py import hydrograph_day  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_hydrograph_day_history.sh"
_FIXTURE_CSV = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "hydrograph_day"
    / "sample.csv"
)


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------


def _write_full_csv(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    year_cols: tuple[str, ...] = ("2025", "2026"),
    extra_columns: tuple[str, ...] = (),
) -> Path:
    """Write a hydrograph_day-style CSV from a list of row dicts.

    Header includes the standard stat / quantile / year columns plus a
    ``date`` and ``day_of_year``. Rows that omit a key default to empty
    string (i.e. NULL at the parser layer).
    """
    base_cols = [
        "code",
        "count",
        "mean",
        "std",
        "min",
        "max",
        "5%",
        "25%",
        "50%",
        "75%",
        "95%",
        "norm",
    ]
    header = list(base_cols) + list(year_cols) + ["date", "day_of_year"] + list(extra_columns)
    csv_path = tmp_path / "fixture.csv"
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(row.get(col, "") for col in header))
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path


# ---------------------------------------------------------------------------
# 1. Wrapper CLI surface
# ---------------------------------------------------------------------------


def test_wrapper_help_returns_zero_and_prints_usage():
    """`bash bin/initialize_hydrograph_day_history.sh --help` exits 0 and shows usage."""
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


# ---------------------------------------------------------------------------
# 2. Wrapper rejects missing env file
# ---------------------------------------------------------------------------


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
# 3. Fixture sanity
# ---------------------------------------------------------------------------


def test_sample_fixture_present_and_has_sentinel_codes_only():
    """The shipped hydrograph_day sample fixture has only sentinel station codes."""
    assert _FIXTURE_CSV.is_file(), f"fixture missing: {_FIXTURE_CSV}"
    text = _FIXTURE_CSV.read_text(encoding="utf-8")
    five_digit = re.findall(r"\b\d{5}\b", text)
    allowed = {"19999"} | {f"0000{i}" for i in range(10)}
    for code in five_digit:
        assert code in allowed, f"non-sentinel code {code!r} found in fixture"


# ---------------------------------------------------------------------------
# 4-7. _discover_year_columns
# ---------------------------------------------------------------------------


def test_discover_year_columns_picks_last_two():
    """Header with 3 year columns -> the two newest, sorted (prev, curr)."""
    header = ["code", "date", "2023", "2024", "2025", "mean"]
    prev, curr = hydrograph_day._discover_year_columns(header)
    assert (prev, curr) == (2024, 2025)


def test_discover_year_columns_handles_three_years_with_next_year():
    """Future-looking: ['...,'2024','2025','2026',...] -> (2025, 2026)."""
    header = ["code", "date", "2024", "2025", "2026", "mean", "day_of_year"]
    prev, curr = hydrograph_day._discover_year_columns(header)
    assert (prev, curr) == (2025, 2026)


def test_discover_year_columns_handles_unordered_header():
    """Year columns may appear anywhere in the header; sort by numeric value."""
    header = ["code", "2026", "date", "2025", "mean"]
    prev, curr = hydrograph_day._discover_year_columns(header)
    assert (prev, curr) == (2025, 2026)


def test_discover_year_columns_raises_on_zero_years():
    """Header without any year column -> ValueError."""
    header = ["code", "date", "mean", "min", "max"]
    with pytest.raises(ValueError, match="at least 2 year columns"):
        hydrograph_day._discover_year_columns(header)


def test_discover_year_columns_raises_on_one_year():
    """Only one year column -> ValueError (need both previous and current)."""
    header = ["code", "date", "2025", "mean"]
    with pytest.raises(ValueError, match="at least 2 year columns"):
        hydrograph_day._discover_year_columns(header)


def test_discover_year_columns_ignores_non_year_numerics():
    """Column names like '5%' / '95%' / '00000' / '20' must not be treated as years."""
    # '00000' would match \d{4}? No — it's 5 chars; '0000' would match.
    # But '00000' is 5 digits, so the strict ^\d{4}$ regex won't match it.
    # Include a 5-digit numeric, '5%' (quantile), and '20' to confirm.
    header = ["code", "date", "5%", "25%", "20", "00000", "2025", "2026", "mean"]
    prev, curr = hydrograph_day._discover_year_columns(header)
    assert (prev, curr) == (2025, 2026)


# ---------------------------------------------------------------------------
# 8. _build_record maps year columns to previous/current
# ---------------------------------------------------------------------------


def test_build_record_maps_year_columns_to_previous_current():
    """Given year_map {prev: 2024, curr: 2025} and row '2024'=1.5 '2025'=2.7,
    record has previous=1.5 and current=2.7."""
    row = {
        "code": "19999",
        "date": "2025-06-15",
        "day_of_year": "166",
        "2024": "1.5",
        "2025": "2.7",
        "mean": "2.0",
    }
    rec = hydrograph_day._build_record(row, {"previous": 2024, "current": 2025})
    assert rec is not None
    assert rec["previous"] == 1.5
    assert rec["current"] == 2.7
    assert rec["mean"] == 2.0
    assert rec["horizon_type"] == "day"
    assert rec["code"] == "19999"
    assert rec["date"] == "2025-06-15"
    # day_of_year from CSV, not derived from date.
    assert rec["day_of_year"] == 166


def test_build_record_omits_absent_year_columns_not_sent_as_null():
    """If the year value is missing from the source row, the payload OMITS
    'previous'/'current' (universal safe-write — never send null)."""
    row = {
        "code": "19999",
        "date": "2025-06-15",
        "day_of_year": "166",
        "2024": "",  # explicitly null
        "2025": "nan",  # null-like
        "mean": "2.0",
    }
    rec = hydrograph_day._build_record(row, {"previous": 2024, "current": 2025})
    assert rec is not None
    assert "previous" not in rec
    assert "current" not in rec
    # Other present fields still survive.
    assert rec["mean"] == 2.0


def test_build_record_derives_day_of_year_when_missing():
    """If the CSV row lacks a parseable day_of_year, derive from date."""
    row = {
        "code": "19999",
        "date": "2026-03-01",  # 2026 not leap; day-of-year = 60
        "day_of_year": "",
    }
    rec = hydrograph_day._build_record(row, {"previous": 2024, "current": 2025})
    assert rec is not None
    assert rec["day_of_year"] == 60
    assert rec["horizon_in_year"] == 60
    assert rec["horizon_value"] == 1  # day of month


def test_build_record_returns_none_for_missing_required():
    """Row missing code or date returns None (treated as parse skip)."""
    year_map = {"previous": 2024, "current": 2025}
    assert hydrograph_day._build_record({"code": "", "date": "2026-01-01"}, year_map) is None
    assert hydrograph_day._build_record({"code": "19999", "date": ""}, year_map) is None
    assert hydrograph_day._build_record({"code": "19999", "date": "not-a-date"}, year_map) is None


# ---------------------------------------------------------------------------
# 9. Quantile column normalization '5%' -> 'q05'
# ---------------------------------------------------------------------------


def test_read_filtered_records_quantile_column_normalization(tmp_path):
    """CSV with 5%/25%/50%/75%/95% columns produces payload with q05..q95."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "5%": "3.5",
                "25%": "4.5",
                "50%": "5.5",
                "75%": "6.5",
                "95%": "7.5",
                "2025": "4.2",
                "2026": "4.8",
            },
        ],
    )
    records, counters, _, _, _, year_map = hydrograph_day._read_filtered_records(
        csv, cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 1
    assert year_map == {"previous": 2025, "current": 2026}
    rec = records[0]
    assert rec["q05"] == 3.5
    assert rec["q25"] == 4.5
    assert rec["q50"] == 5.5
    assert rec["q75"] == 6.5
    assert rec["q95"] == 7.5
    # Raw CSV column names must NOT leak into the payload.
    for raw in ("5%", "25%", "50%", "75%", "95%"):
        assert raw not in rec


# ---------------------------------------------------------------------------
# 10. Station filter
# ---------------------------------------------------------------------------


def test_read_filtered_records_station_filter_keeps_only_match(tmp_path):
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "2025": "4.2",
                "2026": "4.8",
            },
            {
                "code": "19999",
                "date": "2026-01-02",
                "day_of_year": "2",
                "2025": "4.3",
                "2026": "4.9",
            },
            {
                "code": "00000",
                "date": "2026-01-03",
                "day_of_year": "3",
                "2025": "5.0",
                "2026": "5.5",
            },
        ],
    )
    records, counters, codes, _, _, _ = hydrograph_day._read_filtered_records(
        csv, cutoff=None, station_filter="19999"
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 2
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    for rec in records:
        assert rec["code"] == "19999"


# ---------------------------------------------------------------------------
# 11. Cutoff filter
# ---------------------------------------------------------------------------


def test_read_filtered_records_cutoff_drops_on_or_after(tmp_path):
    """Cutoff is strict: rows with date >= cutoff are dropped (pre-cutoff)."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "2025": "1.0",
                "2026": "2.0",
            },
            {
                "code": "19999",
                "date": "2026-01-02",
                "day_of_year": "2",
                "2025": "1.5",
                "2026": "2.5",
            },
            {
                "code": "19999",
                "date": "2026-01-03",
                "day_of_year": "3",
                "2025": "2.0",
                "2026": "3.0",
            },
        ],
    )
    records, counters, _, _, _, _ = hydrograph_day._read_filtered_records(
        csv, cutoff="2026-01-02", station_filter=None
    )
    # Only 2026-01-01 strictly < 2026-01-02 survives.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2026-01-01"


# ---------------------------------------------------------------------------
# 12. NULL stat values handled (enrichment-only)
# ---------------------------------------------------------------------------


def test_read_filtered_records_skips_null_stat_values_not_sent_as_null(tmp_path):
    """Universal safe-write rule: stat fields with NULL/empty/NaN source value
    are OMITTED from payload, never sent as null."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "mean": "5.5",
                "std": "",  # NULL
                "min": "nan",  # null-like
                "max": "garbage",  # parse failure
                "5%": "3.5",  # quantile present
                "25%": "",  # quantile null
                "norm": "5.4",
                "2025": "4.2",
                "2026": "4.8",
            },
        ],
    )
    records, counters, _, _, _, _ = hydrograph_day._read_filtered_records(
        csv, cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 1
    rec = records[0]
    assert rec["mean"] == 5.5
    assert rec["norm"] == 5.4
    assert rec["q05"] == 3.5
    assert rec["previous"] == 4.2
    assert rec["current"] == 4.8
    # NULL fields must be ABSENT, not present-with-null.
    for absent in ("std", "min", "max", "q25"):
        assert absent not in rec


# ---------------------------------------------------------------------------
# 13. Required-column rejection
# ---------------------------------------------------------------------------


def test_read_filtered_records_rejects_csv_missing_required_columns(tmp_path):
    bad = tmp_path / "bad.csv"
    # Missing 'code' but has year columns.
    bad.write_text("date,day_of_year,2025,2026\n2026-01-01,1,4.2,4.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required column"):
        hydrograph_day._read_filtered_records(bad, cutoff=None, station_filter=None)


def test_read_filtered_records_rejects_csv_with_one_year_column(tmp_path):
    """Even if required columns are present, fewer than 2 year cols -> raise."""
    bad = tmp_path / "one_year.csv"
    bad.write_text("code,date,day_of_year,2025\n19999,2026-01-01,1,4.2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="at least 2 year columns"):
        hydrograph_day._read_filtered_records(bad, cutoff=None, station_filter=None)


# ---------------------------------------------------------------------------
# 14. main() dry-run output — empty CSV / full-import
# ---------------------------------------------------------------------------


def test_main_dry_run_empty_csv_emits_full_import_mode(tmp_path, capsys):
    """Empty CSV (header only, but with valid year columns) -> MODE=full-import."""
    csv = tmp_path / "empty.csv"
    csv.write_text("code,date,day_of_year,2025,2026\n", encoding="utf-8")
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "MODE=full-import" in captured.out
    assert "DRY RUN: no POSTs attempted." in captured.out
    assert "SOURCE_ROW_COUNT=0" in captured.out
    assert "FILTERED_ROW_COUNT=0" in captured.out
    # Year mapping is reported even on empty CSV (discovered from header).
    assert "HYDROGRAPH_YEAR_MAPPING={previous: 2025, current: 2026}" in captured.out


# ---------------------------------------------------------------------------
# 15. main() dry-run emits year-column mapping
# ---------------------------------------------------------------------------


def test_main_dry_run_emits_year_column_mapping(tmp_path, capsys):
    """Dry-run output includes the HYDROGRAPH_YEAR_MAPPING line so operators
    can validate the dynamic discovery before any write."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "2025": "4.2",
                "2026": "4.8",
            },
        ],
        year_cols=("2025", "2026"),
    )
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "HYDROGRAPH_YEAR_MAPPING={previous: 2025, current: 2026}" in out


# ---------------------------------------------------------------------------
# 16. main() station-filter reduces filtered count
# ---------------------------------------------------------------------------


def test_main_dry_run_with_station_filter_reduces_filtered_count(tmp_path, capsys):
    """Station filter reduces FILTERED_ROW_COUNT below SOURCE_ROW_COUNT."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "2025": "4.2",
                "2026": "4.8",
            },
            {
                "code": "19999",
                "date": "2026-01-02",
                "day_of_year": "2",
                "2025": "4.3",
                "2026": "4.9",
            },
            {
                "code": "00000",
                "date": "2026-01-03",
                "day_of_year": "3",
                "2025": "5.0",
                "2026": "5.5",
            },
        ],
    )
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/hydrograph/",
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
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out


# ---------------------------------------------------------------------------
# 17. main() dry-run cutoff -> pre-cutoff MODE
# ---------------------------------------------------------------------------


def test_main_dry_run_with_cutoff_emits_pre_cutoff_mode(tmp_path, capsys):
    """When --cutoff is provided, MODE=pre-cutoff is reported."""
    csv = _write_full_csv(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "day_of_year": "1",
                "2025": "4.2",
                "2026": "4.8",
            },
            {
                "code": "19999",
                "date": "2026-01-02",
                "day_of_year": "2",
                "2025": "4.3",
                "2026": "4.9",
            },
        ],
    )
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--cutoff",
            "2026-01-02",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=pre-cutoff (cutoff=2026-01-02)" in out
    assert "FILTERED_ROW_COUNT=1" in out  # only 2026-01-01 < 2026-01-02


# ---------------------------------------------------------------------------
# 18. main() missing CSV exits non-zero
# ---------------------------------------------------------------------------


def test_main_missing_csv_returns_nonzero(tmp_path):
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ---------------------------------------------------------------------------
# 19. main() reports the year-discovery error gracefully
# ---------------------------------------------------------------------------


def test_main_reports_year_discovery_failure_gracefully(tmp_path, capsys):
    """A CSV with required cols but only one year column -> exit non-zero +
    descriptive error mentioning year columns."""
    csv = tmp_path / "one_year.csv"
    csv.write_text("code,date,day_of_year,2025\n19999,2026-01-01,1,4.2\n", encoding="utf-8")
    exit_code = hydrograph_day.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code != 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "year columns" in combined.lower()


# ---------------------------------------------------------------------------
# 20. Wrapper help documents the station-filter contract
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
# 21. stdlib-only audit covers hydrograph_day.py
# ---------------------------------------------------------------------------


def test_hydrograph_day_module_imports_only_stdlib_and_intra_package():
    """The hydrograph_day module passes the P0 stdlib-only audit alongside _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )
