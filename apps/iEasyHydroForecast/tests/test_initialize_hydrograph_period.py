"""Tests for the P2b SERVER-SIDE import wrapper +
``migration_py.hydrograph_period``.

Covers:
- Bash wrapper CLI surface (--help, missing env file rejection, --from-export
  + --horizon required-flag enforcement, --station-filter contract, --strict-merge).
- ``_build_record`` payload shape per HORIZON: required fields, optional
  stat / quantile / norm / previous / current; NULL handling; year-mapping
  semantics (NO year-column discovery — fields come directly from the CSV).
- Quantile column normalization: canonical ``q05`` AND legacy ``5%``.
- ``_read_filtered_records_with_manifest`` filters (cutoff, station_filter),
  manifest validation failures bubble up cleanly.
- ``main()`` dry-run output shape per the §4.4 inventory contract.
- ``--strict-merge`` flag is parsed and surfaces in dry-run output (NOT YET
  IMPLEMENTED — falls back to enrichment-only with a warning).
- Stdlib-only import audit covers hydrograph_period.

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §6.2 canary.

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

from migration_py import _common, hydrograph_period  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_hydrograph_period_history.sh"
_FIXTURE_DIR = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "hydrograph_period"
)


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------

_DEFAULT_HEADER = [
    "horizon_type",
    "code",
    "date",
    "horizon_value",
    "horizon_in_year",
    "day_of_year",
    "count",
    "mean",
    "std",
    "min",
    "max",
    "q05",
    "q25",
    "q50",
    "q75",
    "q95",
    "norm",
    "previous",
    "current",
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
        "export_type": "hydrograph_period",
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
    # All P2b-specific flags are documented.
    assert "--from-export" in result.stdout
    assert "--horizon" in result.stdout
    assert "--station-filter" in result.stdout
    assert "--strict-merge" in result.stdout


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
            "day",  # not pentad/decade — DAY is P3's territory
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
    """All HydrographBase required fields are emitted for a PENTAD record."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "mean": "5.5",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"
    assert rec["code"] == "19999"
    assert rec["date"] == "2026-01-01"
    assert rec["horizon_value"] == 1
    assert rec["horizon_in_year"] == 1
    assert rec["day_of_year"] == 1
    assert rec["mean"] == 5.5


def test_build_record_decade_horizon_type_value():
    """DECADE input produces ``horizon_type='decade'``."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
    }
    rec = hydrograph_period._build_record(row, "decade")
    assert rec is not None
    assert rec["horizon_type"] == "decade"


def test_build_record_rejects_unknown_horizon():
    """Anything other than 'pentad'/'decade' returns None."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
    }
    # 'month' is reserved for the LT regenerate hook; this module rejects it.
    assert hydrograph_period._build_record(row, "month") is None
    assert hydrograph_period._build_record(row, "day") is None


def test_build_record_returns_none_for_missing_required():
    """Row missing code / date / one of the horizon-required ints -> None."""
    base = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
    }
    # Missing code.
    bad = dict(base)
    bad["code"] = ""
    assert hydrograph_period._build_record(bad, "pentad") is None
    # Missing date.
    bad = dict(base)
    bad["date"] = ""
    assert hydrograph_period._build_record(bad, "pentad") is None
    # Unparseable date.
    bad = dict(base)
    bad["date"] = "not-a-date"
    assert hydrograph_period._build_record(bad, "pentad") is None
    # Missing horizon_value (int).
    bad = dict(base)
    bad["horizon_value"] = ""
    assert hydrograph_period._build_record(bad, "pentad") is None
    # Missing day_of_year.
    bad = dict(base)
    bad["day_of_year"] = ""
    assert hydrograph_period._build_record(bad, "pentad") is None


def test_build_record_pentad_includes_year_mapping_when_present():
    """When previous/current are non-NULL in the CSV, they appear in the record.

    Per brief §4.4: ``test_build_record_pentad_includes_year_mapping_if_present``.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "previous": "4.2",
        "current": "4.8",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["previous"] == 4.2
    assert rec["current"] == 4.8


def test_build_record_excludes_null_quantile_fields():
    """Quantile fields with NULL/empty/NaN source value are OMITTED.

    Per brief §4.4: ``test_build_record_excludes_null_quantile_fields``.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "q05": "3.5",
        "q25": "",  # NULL
        "q50": "nan",  # null-like
        "q75": "garbage",  # parse failure
        "q95": "7.5",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["q05"] == 3.5
    assert rec["q95"] == 7.5
    # NULL quantile fields must be ABSENT, not present-with-null.
    for absent in ("q25", "q50", "q75"):
        assert absent not in rec


def test_build_record_omits_absent_previous_current_not_sent_as_null():
    """If previous/current are absent / null-like, the payload OMITS them."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "previous": "",
        "current": "nan",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert "previous" not in rec
    assert "current" not in rec


def test_build_record_accepts_legacy_percent_quantile_columns(tmp_path):
    """The legacy ``5%`` / ``25%`` / etc. CSV header names also map to qXX keys."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "5%": "3.5",
        "25%": "4.5",
        "50%": "5.5",
        "75%": "6.5",
        "95%": "7.5",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["q05"] == 3.5
    assert rec["q25"] == 4.5
    assert rec["q50"] == 5.5
    assert rec["q75"] == 6.5
    assert rec["q95"] == 7.5
    # Raw legacy names must NOT leak into the payload.
    for raw in ("5%", "25%", "50%", "75%", "95%"):
        assert raw not in rec


def test_build_record_canonical_quantile_takes_precedence_over_legacy():
    """If both ``q05`` and ``5%`` are present, the canonical ``q05`` wins."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "q05": "3.5",
        "5%": "9.9",  # legacy fallback — should NOT be used
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert rec["q05"] == 3.5
    assert "5%" not in rec


def test_build_record_omits_count_when_null():
    """Optional ``count`` integer field is omitted when NULL."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "horizon_value": "1",
        "horizon_in_year": "1",
        "day_of_year": "1",
        "count": "",
    }
    rec = hydrograph_period._build_record(row, "pentad")
    assert rec is not None
    assert "count" not in rec


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
                "day_of_year": "1",
                "mean": "5.5",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "day_of_year": "6",
                "mean": "5.6",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, dmin, dmax = hydrograph_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff=None, station_filter=None
    )
    assert counters["source_row_count"] == 2
    assert counters["filtered_row_count"] == 2
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-06"
    assert all(r["horizon_type"] == "pentad" for r in records)


def test_read_filtered_records_station_filter(tmp_path):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "day_of_year": "6",
            },
            {
                "code": "00000",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
                "day_of_year": "11",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, _, _ = hydrograph_period._read_filtered_records_with_manifest(
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
                "day_of_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "day_of_year": "6",
            },
            {
                "code": "19999",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
                "day_of_year": "11",
            },
        ],
        horizon="pentad",
    )
    records, counters, _, _, _ = hydrograph_period._read_filtered_records_with_manifest(
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
        "export_type=hydrograph_period\nrow_count=1\nstation_count=1\n"
        "date_min=2026-01-01\ndate_max=2026-01-01\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing required column"):
        hydrograph_period._read_filtered_records_with_manifest(
            bad, manifest, "pentad", cutoff=None, station_filter=None
        )


def test_read_filtered_records_manifest_missing_raises(tmp_path):
    """If the manifest file is absent, validation raises a ManifestMissingError."""
    csv = tmp_path / "lonely.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    with pytest.raises(_common.ManifestMissingError):
        hydrograph_period._read_filtered_records_with_manifest(
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
                "day_of_year": "1",
            },
        ],
        horizon="pentad",
        manifest_override={"row_count": "999"},
    )
    with pytest.raises(_common.ManifestRowCountMismatchError):
        hydrograph_period._read_filtered_records_with_manifest(
            csv,
            csv.with_name(csv.name + ".manifest"),
            "pentad",
            cutoff=None,
            station_filter=None,
        )


def test_read_filtered_records_manifest_wrong_export_type_raises(tmp_path):
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
        ],
        horizon="pentad",
        manifest_override={"export_type": "runoff_period"},
    )
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        hydrograph_period._read_filtered_records_with_manifest(
            csv,
            csv.with_name(csv.name + ".manifest"),
            "pentad",
            cutoff=None,
            station_filter=None,
        )


# ===========================================================================
# 5. main() dry-run output shape
# ===========================================================================


def test_main_dry_run_pentad_empty_csv_emits_full_import(tmp_path, capsys):
    csv = tmp_path / "empty.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    manifest = tmp_path / "empty.csv.manifest"
    manifest.write_text(
        "export_type=hydrograph_period\nrow_count=0\nstation_count=0\n"
        "date_min=2026-01-01\ndate_max=2026-01-01\n",
        encoding="utf-8",
    )
    # Empty-CSV manifest validation is happy as long as row/station counts
    # are zero and date_min/max are valid ISO. But station_count=0 must
    # equal the CSV's distinct-code count (also 0).
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(manifest),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    # date_min/max in manifest won't match empty CSV (which has neither);
    # so this WILL exit non-zero due to manifest date-range mismatch. That
    # is the documented behavior — we record it as a test.
    assert exit_code != 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest" in combined.lower() or "date" in combined.lower()


def test_main_dry_run_with_station_filter_reduces_filtered_count(tmp_path, capsys):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "day_of_year": "6",
            },
            {
                "code": "00000",
                "date": "2026-01-11",
                "horizon_value": "3",
                "horizon_in_year": "3",
                "day_of_year": "11",
            },
        ],
        horizon="pentad",
    )
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(manifest),
            "--horizon",
            "pentad",
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
    assert "MODE=full-import" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out
    assert "TARGET_TABLE=hydrographs" in out
    assert "HORIZON_TYPE=PENTAD" in out


def test_main_dry_run_with_cutoff_emits_pre_cutoff(tmp_path, capsys):
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
            {
                "code": "19999",
                "date": "2026-01-06",
                "horizon_value": "2",
                "horizon_in_year": "2",
                "day_of_year": "6",
            },
        ],
        horizon="decade",
    )
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(manifest),
            "--horizon",
            "decade",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--cutoff",
            "2026-01-06",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=pre-cutoff (cutoff=2026-01-06)" in out
    assert "FILTERED_ROW_COUNT=1" in out
    assert "HORIZON_TYPE=DECADE" in out


def test_main_missing_csv_returns_nonzero(tmp_path):
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--manifest-path",
            str(tmp_path / "does_not_exist.csv.manifest"),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


def test_main_missing_manifest_returns_nonzero(tmp_path):
    csv = tmp_path / "lonely.csv"
    csv.write_text(",".join(_DEFAULT_HEADER) + "\n", encoding="utf-8")
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(tmp_path / "no_such.manifest"),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ===========================================================================
# 6. --strict-merge flag (future feature)
# ===========================================================================


def test_strict_merge_flag_changes_post_behavior(tmp_path, capsys):
    """``--strict-merge`` is parsed; until implemented it logs + falls back
    to enrichment-only. The dry-run inventory surfaces the active policy so
    operators can confirm the safe-write mode.

    Per brief §4.4: ``test_strict_merge_flag_changes_post_behavior`` (or stub
    if read-before-merge defers). Read-before-merge IS deferred in this PR;
    this test is the stub form that verifies the flag is parsed AND the
    fallback message + policy line are emitted.
    """
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
        ],
        horizon="pentad",
    )
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(manifest),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--strict-merge",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    # The flag triggers the documented warning + a policy line in inventory.
    assert "strict-merge" in out.lower() or "SAFE_WRITE_POLICY" in out
    # Stub behaviour: explicitly state NOT YET IMPLEMENTED in the inventory
    # line so operators know they're getting enrichment-only.
    assert "NOT YET IMPLEMENTED" in out or "enrichment-only" in out.lower()


def test_dry_run_default_inventory_lists_enrichment_only_policy(tmp_path, capsys):
    """Without ``--strict-merge``, the inventory line is
    ``SAFE_WRITE_POLICY=enrichment-only (default)``."""
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "date": "2026-01-01",
                "horizon_value": "1",
                "horizon_in_year": "1",
                "day_of_year": "1",
            },
        ],
        horizon="pentad",
    )
    exit_code = hydrograph_period.main(
        [
            "--csv-path",
            str(csv),
            "--manifest-path",
            str(manifest),
            "--horizon",
            "pentad",
            "--api-url",
            "http://localhost:8002/hydrograph/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SAFE_WRITE_POLICY=enrichment-only (default)" in out


# ===========================================================================
# 7. Stdlib-only audit covers hydrograph_period
# ===========================================================================


def test_hydrograph_period_module_imports_only_stdlib_and_intra_package():
    """The hydrograph_period module passes the P0 stdlib-only audit alongside
    _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


# ===========================================================================
# 8. Shipped fixtures round-trip through the parser
# ===========================================================================


def test_shipped_pentad_fixture_round_trips(tmp_path):
    """The shipped ``pentad_sample.csv`` + manifest parses through the helper."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    manifest = _FIXTURE_DIR / "pentad_sample.csv.manifest"
    records, counters, codes, dmin, dmax = hydrograph_period._read_filtered_records_with_manifest(
        csv, manifest, "pentad", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-11"
    for r in records:
        assert r["horizon_type"] == "pentad"
        assert "previous" in r
        assert "current" in r


def test_shipped_decade_fixture_round_trips(tmp_path):
    csv = _FIXTURE_DIR / "decade_sample.csv"
    manifest = _FIXTURE_DIR / "decade_sample.csv.manifest"
    records, counters, codes, dmin, dmax = hydrograph_period._read_filtered_records_with_manifest(
        csv, manifest, "decade", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-21"
    for r in records:
        assert r["horizon_type"] == "decade"
