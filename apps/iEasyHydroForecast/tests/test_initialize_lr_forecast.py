"""Tests for the P4a SERVER-SIDE import wrapper + ``migration_py.lr_forecast``.

Covers:
- Bash wrapper CLI surface (--help, missing env file rejection, --from-export
  + --horizon required-flag enforcement, --station-filter contract).
- ``_build_record`` payload shape per HORIZON: required fields, optional
  model-stat fields (slope/intercept/q_mean/q_std_sigma/delta/rsquared, plus
  discharge_avg/predictor/forecasted_discharge); NULL/empty handling; per-
  horizon column mapping (pentad_in_month/year vs decad_in_month/year).
- ``_read_filtered_records`` filters (cutoff, station_filter), required-
  column validation.
- ``main()`` dry-run output shape per the §4.4 inventory contract.
- ``horizon_type`` enum mapping (architecture §Q4 lock: lowercase 'pentad'
  / 'decade'); uppercase WOULD be rejected at the Pydantic boundary.
- Cross-check: lr_forecasts has NO ``model_type`` column.
- Stdlib-only import audit covers ``lr_forecast``.

Integration against a live docker stack is out of scope per architecture
§Q7 (disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §6.3 canary.

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

from migration_py import lr_forecast  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_lr_forecast_history.sh"
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
# CSV helpers
# ---------------------------------------------------------------------------


_PENTAD_HEADER = [
    "horizon_type",
    "code",
    "date",
    "pentad_in_month",
    "pentad_in_year",
    "discharge_avg",
    "predictor",
    "slope",
    "intercept",
    "forecasted_discharge",
    "q_mean",
    "q_std_sigma",
    "delta",
    "rsquared",
]

_DECADE_HEADER = [
    "horizon_type",
    "code",
    "date",
    "decad_in_month",
    "decad_in_year",
    "discharge_avg",
    "predictor",
    "slope",
    "intercept",
    "forecasted_discharge",
    "q_mean",
    "q_std_sigma",
    "delta",
    "rsquared",
]


def _write_csv(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    horizon: str = "pentad",
    csv_name: str = "fixture.csv",
) -> Path:
    """Write a CSV with the canonical header for the given horizon. Returns the path."""
    header = list(_PENTAD_HEADER if horizon == "pentad" else _DECADE_HEADER)
    csv_path = tmp_path / csv_name
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(row.get(col, "") for col in header))
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path


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
    # All P4a-specific flags are documented.
    assert "--from-export" in result.stdout
    assert "--horizon" in result.stdout
    assert "--station-filter" in result.stdout


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
    low = result.stdout.lower()
    assert "contract" in low or "interface" in low or "binding" in low


def test_wrapper_rejects_missing_env_file(tmp_path):
    nonexistent = tmp_path / "no_such_env_file"
    csv = tmp_path / "x.csv"
    csv.write_text(",".join(_PENTAD_HEADER) + "\n", encoding="utf-8")
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
            "day",  # not pentad/decade — LR doesn't exist at DAY
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0


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
    """All LRForecastBase required fields are emitted for a PENTAD record."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
        "forecasted_discharge": "14.30",
    }
    rec = lr_forecast._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"
    assert rec["code"] == "19999"
    assert rec["date"] == "2026-01-01"
    assert rec["horizon_value"] == 1
    assert rec["horizon_in_year"] == 1
    assert rec["forecasted_discharge"] == 14.30


def test_build_record_decade_uses_decad_columns():
    """DECADE input maps decad_in_month / decad_in_year to horizon_value /
    horizon_in_year (per LRForecastDataMigrator.prepare_decade_data)."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "decad_in_month": "2",
        "decad_in_year": "5",
    }
    rec = lr_forecast._build_record(row, "decade")
    assert rec is not None
    assert rec["horizon_type"] == "decade"
    assert rec["horizon_value"] == 2
    assert rec["horizon_in_year"] == 5


def test_build_record_horizon_type_value_is_lowercase():
    """Architecture §Q4 lock: API enum is lowercase 'pentad' / 'decade'.

    Uppercase 'PENTAD' / 'DECADE' WOULD be rejected at the Pydantic
    boundary (postprocessing.app.models.HorizonType uses lowercase
    values). This test pins the payload contract.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
    }
    rec = lr_forecast._build_record(row, "pentad")
    assert rec is not None
    assert rec["horizon_type"] == "pentad"  # NOT "PENTAD"
    # And the contrary case for decade.
    row_d = {
        "code": "19999",
        "date": "2026-01-01",
        "decad_in_month": "1",
        "decad_in_year": "1",
    }
    rec_d = lr_forecast._build_record(row_d, "decade")
    assert rec_d is not None
    assert rec_d["horizon_type"] == "decade"  # NOT "DECADE"


def test_build_record_rejects_unknown_horizon():
    """Anything other than 'pentad'/'decade' raises ValueError.

    LR does not exist at DAY/MONTH; the helper must refuse rather than
    silently swallowing the row.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
    }
    with pytest.raises(ValueError, match="horizon must be one of"):
        lr_forecast._build_record(row, "day")
    with pytest.raises(ValueError, match="horizon must be one of"):
        lr_forecast._build_record(row, "month")


def test_build_record_returns_none_for_missing_required():
    """Row missing code / date / one of the horizon-required ints -> None."""
    base = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
    }
    # Missing code.
    bad = dict(base)
    bad["code"] = ""
    assert lr_forecast._build_record(bad, "pentad") is None
    # Missing date.
    bad = dict(base)
    bad["date"] = ""
    assert lr_forecast._build_record(bad, "pentad") is None
    # Unparseable date.
    bad = dict(base)
    bad["date"] = "not-a-date"
    assert lr_forecast._build_record(bad, "pentad") is None
    # Missing horizon_value (int).
    bad = dict(base)
    bad["pentad_in_month"] = ""
    assert lr_forecast._build_record(bad, "pentad") is None
    # Missing horizon_in_year (int).
    bad = dict(base)
    bad["pentad_in_year"] = ""
    assert lr_forecast._build_record(bad, "pentad") is None


def test_build_record_excludes_null_model_stat_fields():
    """Nullable fields (slope/intercept/q_mean/q_std_sigma/delta/rsquared) with
    NULL/empty/NaN source values are OMITTED from the payload, never
    sent as ``null``. This is the universal safe-write rule (architecture
    §Q2 layer 2).
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
        "discharge_avg": "10.5",
        "predictor": "",  # NULL
        "slope": "nan",  # null-like
        "intercept": "1.5",
        "forecasted_discharge": "",  # NULL
        "q_mean": "garbage",  # parse failure
        "q_std_sigma": "0.85",
        "delta": "NULL",  # explicit null literal
        "rsquared": "0.78",
    }
    rec = lr_forecast._build_record(row, "pentad")
    assert rec is not None
    # Present fields.
    assert rec["discharge_avg"] == 10.5
    assert rec["intercept"] == 1.5
    assert rec["q_std_sigma"] == 0.85
    assert rec["rsquared"] == 0.78
    # NULL/empty/NaN/parse-fail fields must be ABSENT from the payload.
    for absent in ("predictor", "slope", "forecasted_discharge", "q_mean", "delta"):
        assert absent not in rec, f"{absent} should be omitted (was {rec.get(absent)!r})"


def test_build_record_omits_non_finite_floats():
    """Non-finite floats (+/-inf) are rejected to prevent invalid JSON.

    The Pydantic boundary rejects ``Infinity`` / ``NaN`` literals (RFC 7159).
    The helper's _parse_float guards by checking float != float (NaN trick)
    and equality with +/-inf.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
        "slope": "inf",
        "intercept": "-inf",
    }
    rec = lr_forecast._build_record(row, "pentad")
    assert rec is not None
    assert "slope" not in rec
    assert "intercept" not in rec


def test_build_record_omits_pentad_in_year_when_unparseable():
    """If pentad_in_year is unparseable, the whole record is None
    (required field)."""
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "garbage",
    }
    assert lr_forecast._build_record(row, "pentad") is None


# ===========================================================================
# 4. _read_filtered_records — filtering
# ===========================================================================


def test_read_filtered_records_happy_path_pentad(tmp_path):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-01",
                "pentad_in_month": "1",
                "pentad_in_year": "1",
                "forecasted_discharge": "14.3",
            },
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-06",
                "pentad_in_month": "2",
                "pentad_in_year": "2",
                "forecasted_discharge": "15.1",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, dmin, dmax = lr_forecast._read_filtered_records(
        csv, horizon="pentad", cutoff=None, station_filter=None
    )
    assert counters["source_row_count"] == 2
    assert counters["filtered_row_count"] == 2
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-06"
    assert all(r["horizon_type"] == "pentad" for r in records)


def test_read_filtered_records_station_filter(tmp_path):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-01",
                "pentad_in_month": "1",
                "pentad_in_year": "1",
            },
            {
                "horizon_type": "pentad",
                "code": "00000",
                "date": "2026-01-06",
                "pentad_in_month": "2",
                "pentad_in_year": "2",
            },
        ],
        horizon="pentad",
    )
    records, counters, codes, _, _ = lr_forecast._read_filtered_records(
        csv, horizon="pentad", cutoff=None, station_filter="19999"
    )
    assert counters["source_row_count"] == 2
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    assert records[0]["code"] == "19999"


def test_read_filtered_records_cutoff_drops_on_or_after(tmp_path):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-01",
                "pentad_in_month": "1",
                "pentad_in_year": "1",
            },
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-06",
                "pentad_in_month": "2",
                "pentad_in_year": "2",
            },
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-11",
                "pentad_in_month": "3",
                "pentad_in_year": "3",
            },
        ],
        horizon="pentad",
    )
    records, counters, _, _, _ = lr_forecast._read_filtered_records(
        csv, horizon="pentad", cutoff="2026-01-06", station_filter=None
    )
    # Only rows STRICTLY less than cutoff survive.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2026-01-01"


def test_read_filtered_records_rejects_csv_missing_required_columns(tmp_path):
    """If the CSV is missing horizon-required columns (e.g. pentad_in_month),
    _read_filtered_records raises ValueError before any iteration."""
    bad = tmp_path / "bad.csv"
    # Has horizon_type/code/date but missing pentad_in_month / pentad_in_year.
    bad.write_text("horizon_type,code,date\npentad,19999,2026-01-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required column"):
        lr_forecast._read_filtered_records(bad, horizon="pentad", cutoff=None, station_filter=None)


def test_read_filtered_records_rejects_unknown_horizon(tmp_path):
    csv = _write_csv(
        tmp_path,
        [{"code": "19999", "date": "2026-01-01"}],
        horizon="pentad",
    )
    with pytest.raises(ValueError, match="horizon must be one of"):
        lr_forecast._read_filtered_records(csv, horizon="day", cutoff=None, station_filter=None)


# ===========================================================================
# 5. main() dry-run output shape
# ===========================================================================


def test_main_dry_run_pentad_emits_inventory(tmp_path, capsys):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-01",
                "pentad_in_month": "1",
                "pentad_in_year": "1",
                "forecasted_discharge": "14.3",
            },
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-06",
                "pentad_in_month": "2",
                "pentad_in_year": "2",
            },
        ],
        horizon="pentad",
    )
    exit_code = lr_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/lr-forecast/",
            "--horizon",
            "pentad",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SOURCE_ROW_COUNT=2" in out
    assert "FILTERED_ROW_COUNT=2" in out
    assert "MODE=full-import" in out
    assert "TARGET_TABLE=lr_forecasts" in out
    assert "HORIZON=pentad" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out


def test_main_dry_run_with_station_filter_reduces_filtered_count(tmp_path, capsys):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-01",
                "pentad_in_month": "1",
                "pentad_in_year": "1",
            },
            {
                "horizon_type": "pentad",
                "code": "19999",
                "date": "2026-01-06",
                "pentad_in_month": "2",
                "pentad_in_year": "2",
            },
            {
                "horizon_type": "pentad",
                "code": "00000",
                "date": "2026-01-11",
                "pentad_in_month": "3",
                "pentad_in_year": "3",
            },
        ],
        horizon="pentad",
    )
    exit_code = lr_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/lr-forecast/",
            "--horizon",
            "pentad",
            "--station-filter",
            "19999",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SOURCE_ROW_COUNT=3" in out
    assert "FILTERED_ROW_COUNT=2" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out


def test_main_dry_run_with_cutoff_emits_pre_cutoff(tmp_path, capsys):
    csv = _write_csv(
        tmp_path,
        [
            {
                "horizon_type": "decade",
                "code": "19999",
                "date": "2026-01-01",
                "decad_in_month": "1",
                "decad_in_year": "1",
            },
            {
                "horizon_type": "decade",
                "code": "19999",
                "date": "2026-01-11",
                "decad_in_month": "2",
                "decad_in_year": "2",
            },
        ],
        horizon="decade",
    )
    exit_code = lr_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/lr-forecast/",
            "--horizon",
            "decade",
            "--cutoff",
            "2026-01-11",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=pre-cutoff (cutoff=2026-01-11)" in out
    assert "FILTERED_ROW_COUNT=1" in out
    assert "HORIZON=decade" in out


def test_main_missing_csv_returns_nonzero(tmp_path, capsys):
    exit_code = lr_forecast.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--api-url",
            "http://localhost:8003/lr-forecast/",
            "--horizon",
            "pentad",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ===========================================================================
# 6. Stdlib-only audit covers lr_forecast
# ===========================================================================


def test_lr_forecast_module_imports_only_stdlib_and_intra_package():
    """The lr_forecast module passes the P0 stdlib-only audit alongside _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


# ===========================================================================
# 7. Shipped fixtures round-trip through the parser
# ===========================================================================


def test_shipped_pentad_fixture_round_trips(tmp_path):
    """The shipped ``pentad_sample.csv`` parses through the helper."""
    csv = _FIXTURE_DIR / "pentad_sample.csv"
    records, counters, codes, dmin, dmax = lr_forecast._read_filtered_records(
        csv, horizon="pentad", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-11"
    for r in records:
        assert r["horizon_type"] == "pentad"
        # Per the fixture, all the optional model-stat fields ARE populated.
        for field in (
            "discharge_avg",
            "predictor",
            "slope",
            "intercept",
            "forecasted_discharge",
            "q_mean",
            "q_std_sigma",
            "delta",
            "rsquared",
        ):
            assert field in r, f"{field} should be present in fully-populated fixture row"


def test_shipped_decade_fixture_round_trips(tmp_path):
    csv = _FIXTURE_DIR / "decade_sample.csv"
    records, counters, codes, dmin, dmax = lr_forecast._read_filtered_records(
        csv, horizon="decade", cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 3
    assert codes == {"19999"}
    assert dmin == "2026-01-01"
    assert dmax == "2026-01-21"
    for r in records:
        assert r["horizon_type"] == "decade"


# ===========================================================================
# 8. Cross-check: lr_forecasts has NO model_type column
# ===========================================================================


def test_build_record_does_not_emit_model_type():
    """LR forecasts have NO ``model_type`` column in the DB; the unique key
    is (horizon_type, code, date). Even if a stray ``model_type`` cell
    appears in the CSV, _build_record must NOT include it in the payload —
    Pydantic boundary would silently drop it but defensive purity matters
    here because the same column name is meaningful in the sibling
    ``forecasts`` and ``skill_metrics`` tables.
    """
    row = {
        "code": "19999",
        "date": "2026-01-01",
        "pentad_in_month": "1",
        "pentad_in_year": "1",
        "model_type": "LR",  # noise that must not leak into payload
    }
    rec = lr_forecast._build_record(row, "pentad")
    assert rec is not None
    assert "model_type" not in rec
