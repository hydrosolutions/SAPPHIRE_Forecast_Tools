"""Tests for the P4b SERVER-SIDE import wrapper +
``migration_py.ml_forecast``.

Covers:
- bash wrapper CLI surface (``--help``, missing env file rejection,
  ``--from-export`` required-flag enforcement, ``--station-filter`` contract
  documentation, ``--preserve-legacy-ml-horizons`` opt-in WARNING docs).
- Manifest validation: happy path and 4 failure cases (missing manifest,
  row_count mismatch, wrong export_type, station_count mismatch) — exercises
  the P0 ``migration_py._common.validate_manifest`` contract.
- ``MODEL_DIR_TO_API`` enum case mapping (Stage A §E live-test result):
  uppercase dir spellings (``TFT``, ``TIDE``, ``TSMIXER``) and mixed-case
  API spellings (``TFT``, ``TiDE``, ``TSMixer``) both resolve to the
  canonical API form.
- ``_build_record`` payload shape: default ``horizon_type='day'`` per user-lock
  L6; ``--preserve-legacy-ml-horizons`` opt-in preserves PENTAD/DECADE; null
  quantile fields are OMITTED (never sent as JSON ``null``).
- ``_read_filtered_records`` station filter, cutoff filter, horizon-type
  guard (default rejects non-day source rows).
- ``main()`` dry-run inventory shape, per-model breakdown, model filter.
- Stdlib-only audit covers the ml_forecast module.

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §6.4 canary.

Fixture station codes are restricted by ``test_migration_fixture_guard``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common, ml_forecast  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_ml_forecast_history.sh"


# ---------------------------------------------------------------------------
# CSV + manifest helpers
# ---------------------------------------------------------------------------

# Source CSV header that matches the laptop-export output (see
# bin/export_ml_forecast_history.sh): code, model_type, horizon_type, date,
# target, flag, Q5, Q25, Q50, Q75, Q95, forecasted_discharge.
_DEFAULT_HEADER = [
    "code",
    "model_type",
    "horizon_type",
    "date",
    "target",
    "flag",
    "Q5",
    "Q25",
    "Q50",
    "Q75",
    "Q95",
    "forecasted_discharge",
]


def _write_csv_and_manifest(
    tmp_path: Path,
    rows: list[dict[str, str]],
    *,
    header: list[str] | None = None,
    manifest_override: dict[str, str] | None = None,
    csv_name: str = "ml_forecast.csv",
) -> tuple[Path, Path]:
    """Write a CSV + matching manifest. Returns (csv_path, manifest_path).

    Manifest auto-computes the 5 required keys from the CSV rows so the
    happy-path is always valid. ``manifest_override`` lets a test inject
    failure conditions (mismatched row_count, wrong export_type, etc.).
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
        "export_type": "ml_forecast",
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


def test_wrapper_help_returns_zero():
    """``bash bin/initialize_ml_forecast_history.sh --help`` exits 0."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout
    # All P4b-specific flags are documented.
    assert "--from-export" in result.stdout
    assert "--station-filter" in result.stdout
    assert "--preserve-legacy-ml-horizons" in result.stdout
    assert "--model" in result.stdout


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
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "env file" in combined or "not found" in combined


def test_wrapper_rejects_missing_export_path(tmp_path):
    env = tmp_path / "fake.env"
    env.write_text("# stub\n", encoding="utf-8")
    result = subprocess.run(
        ["bash", str(_WRAPPER), str(env)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    assert "from-export" in combined or "required" in combined


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
    # The docstring/help text marks this flag as the binding interface
    # contract (forward-compatible with all CSV-source wrappers).
    assert (
        "contract" in result.stdout.lower()
        or "interface" in result.stdout.lower()
        or "binding" in result.stdout.lower()
    )


def test_wrapper_help_warns_about_preserve_legacy_flag():
    """``--preserve-legacy-ml-horizons`` is an opt-in lock-override; help must
    surface the WARNING + clarify it's not the default modern behavior."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "-h"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    text = result.stdout
    assert "--preserve-legacy-ml-horizons" in text
    # Help text must clearly carry the WARNING wording so an operator skimming
    # ``--help`` cannot miss the lock-override semantics.
    assert "WARNING" in text or "warning" in text.lower()
    # And mention the default ``day`` storage so the contrast is explicit.
    assert "day" in text.lower()


# ===========================================================================
# 2. Manifest validation (P0 contract: validate_manifest)
# ===========================================================================


def test_manifest_validation_happy_path(tmp_path):
    """A manifest with the 5 required keys and matching row/station/date stats
    passes ``_common.validate_manifest`` for export_type=ml_forecast."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
                "flag": "0",
                "Q5": "3.5",
                "Q25": "4.5",
                "Q50": "5.5",
                "Q75": "6.5",
                "Q95": "7.5",
                "forecasted_discharge": "5.5",
            },
            {
                "code": "19999",
                "model_type": "TiDE",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
                "flag": "0",
                "Q5": "3.6",
                "Q25": "4.6",
                "Q50": "5.6",
                "Q75": "6.6",
                "Q95": "7.6",
                "forecasted_discharge": "5.6",
            },
        ],
    )
    parsed = _common.validate_manifest(csv, "ml_forecast")
    assert parsed["export_type"] == "ml_forecast"
    assert parsed["row_count"] == "2"
    assert parsed["station_count"] == "1"
    assert parsed["date_min"] == "2026-01-01"
    assert parsed["date_max"] == "2026-01-02"


def test_manifest_validation_rejects_missing_manifest(tmp_path):
    """If the manifest file is absent, ``ManifestMissingError`` is raised."""
    csv = tmp_path / "lonely.csv"
    csv.write_text(
        ",".join(_DEFAULT_HEADER) + "\n19999,TFT,day,2026-01-01,2026-01-02,0,,,,,,\n",
        encoding="utf-8",
    )
    with pytest.raises(_common.ManifestMissingError):
        _common.validate_manifest(csv, "ml_forecast")


def test_manifest_validation_rejects_row_count_mismatch(tmp_path):
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
        ],
        manifest_override={"row_count": "999"},
    )
    with pytest.raises(_common.ManifestRowCountMismatchError):
        _common.validate_manifest(csv, "ml_forecast")


def test_manifest_validation_rejects_wrong_export_type(tmp_path):
    """Manifest declares ``export_type=runoff_period`` but the wrapper
    expected ``ml_forecast`` -> ManifestExportTypeMismatchError."""
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
        ],
        manifest_override={"export_type": "runoff_period"},
    )
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        _common.validate_manifest(csv, "ml_forecast")


def test_manifest_validation_rejects_station_count_mismatch(tmp_path):
    """Manifest declares 5 stations but CSV has 1 distinct code ->
    ManifestStationCountMismatchError (catches cross-org / unfiltered
    exports per v2 R6)."""
    csv, _ = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
        ],
        manifest_override={"station_count": "5"},
    )
    with pytest.raises(_common.ManifestStationCountMismatchError):
        _common.validate_manifest(csv, "ml_forecast")


# ===========================================================================
# 3. MODEL_DIR_TO_API enum case mapping (Stage A §E)
# ===========================================================================


def test_model_enum_case_TFT_maps_to_TFT():
    """Both forms of ``TFT`` map to the canonical API value ``TFT``."""
    assert ml_forecast.MODEL_DIR_TO_API["TFT"] == "TFT"
    assert ml_forecast.resolve_model_type("TFT") == "TFT"


def test_model_enum_case_TIDE_maps_to_TiDE():
    """The legacy on-disk dir spelling ``TIDE`` (uppercase) resolves to the
    mixed-case API spelling ``TiDE`` — the case-distinction that the legacy
    in-service migrator silently corrupted."""
    assert ml_forecast.MODEL_DIR_TO_API["TIDE"] == "TiDE"
    assert ml_forecast.resolve_model_type("TIDE") == "TiDE"
    # The API form is accepted as idempotent.
    assert ml_forecast.resolve_model_type("TiDE") == "TiDE"


def test_model_enum_case_TSMIXER_maps_to_TSMixer():
    """The legacy on-disk dir spelling ``TSMIXER`` resolves to ``TSMixer``."""
    assert ml_forecast.MODEL_DIR_TO_API["TSMIXER"] == "TSMixer"
    assert ml_forecast.resolve_model_type("TSMIXER") == "TSMixer"
    assert ml_forecast.resolve_model_type("TSMixer") == "TSMixer"


def test_model_enum_case_unknown_raises():
    """Unknown spellings (typos, forward-compat ambiguities) raise
    ``UnknownMLModelTypeError`` — no silent fallback."""
    with pytest.raises(ml_forecast.UnknownMLModelTypeError):
        ml_forecast.resolve_model_type("GBT")
    with pytest.raises(ml_forecast.UnknownMLModelTypeError):
        ml_forecast.resolve_model_type("")
    with pytest.raises(ml_forecast.UnknownMLModelTypeError):
        ml_forecast.resolve_model_type(None)
    # Lowercase ``tft`` is NOT in the map either (case is meaningful).
    with pytest.raises(ml_forecast.UnknownMLModelTypeError):
        ml_forecast.resolve_model_type("tft")


# ===========================================================================
# 4. _build_record — payload shape (user-lock L6 default; legacy opt-in)
# ===========================================================================


def test_build_record_default_horizon_is_day():
    """Default behavior (user-lock L6): payload ``horizon_type=='day'``
    regardless of source row's ``horizon_type`` cell. Day-of-year is taken
    from the target date."""
    row = {
        "code": "19999",
        "model_type": "TFT",
        "horizon_type": "day",
        "date": "2026-01-01",
        "target": "2026-01-02",
        "Q50": "5.5",
    }
    rec = ml_forecast._build_record(row, preserve_legacy_horizons=False)
    assert rec is not None
    assert rec["horizon_type"] == "day"
    assert rec["code"] == "19999"
    assert rec["model_type"] == "TFT"
    assert rec["date"] == "2026-01-01"
    assert rec["target"] == "2026-01-02"
    # 2026-01-02 is day-of-year 2.
    assert rec["horizon_value"] == 2
    assert rec["horizon_in_year"] == 2
    # Q50 maps to the API's ``forecasted_discharge`` field per the legacy
    # ForecastDataMigrator convention (median quantile == central estimate);
    # the API schema has no separate ``q50`` field, so the payload omits it.
    assert "q50" not in rec
    assert rec["forecasted_discharge"] == 5.5


def test_build_record_preserves_legacy_pentad_horizon_with_flag():
    """With ``preserve_legacy_horizons=True`` AND source ``horizon_type=pentad``,
    the payload preserves ``pentad`` and zeroes horizon_value/horizon_in_year
    per the legacy ForecastDataMigrator convention."""
    row = {
        "code": "19999",
        "model_type": "TFT",
        "horizon_type": "pentad",
        "date": "2026-01-01",
        "target": "2026-01-06",
    }
    rec = ml_forecast._build_record(row, preserve_legacy_horizons=True)
    assert rec is not None
    assert rec["horizon_type"] == "pentad"
    # Legacy zero-fill — not a target day-of-year.
    assert rec["horizon_value"] == 0
    assert rec["horizon_in_year"] == 0


def test_build_record_preserves_legacy_decade_horizon_with_flag():
    """Same legacy-flag behavior for source ``horizon_type=decade``."""
    row = {
        "code": "19999",
        "model_type": "TiDE",
        "horizon_type": "decade",
        "date": "2026-02-01",
        "target": "2026-02-11",
    }
    rec = ml_forecast._build_record(row, preserve_legacy_horizons=True)
    assert rec is not None
    assert rec["horizon_type"] == "decade"
    assert rec["horizon_value"] == 0
    assert rec["horizon_in_year"] == 0


def test_preserve_legacy_flag_default_off_rejects_pentad_rows(tmp_path):
    """Default ``preserve_legacy_horizons=False``: source rows with
    ``horizon_type=pentad`` (or any non-day, non-empty) are skipped at the
    reader layer (``skipped_horizon``), NOT migrated as ``day``."""
    csv, manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "pentad",  # legacy row
                "date": "2026-01-01",
                "target": "2026-01-06",
            },
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",  # modern row
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
        ],
    )
    # Manifest validation happens in the wrapper, not in the reader directly;
    # however the reader's horizon guard fires before any record is built.
    records, counters, codes, dmin, dmax, per_model = ml_forecast._read_filtered_records(
        csv,
        cutoff=None,
        station_filter=None,
        model_filter=None,
        preserve_legacy_horizons=False,
    )
    assert counters["source_row_count"] == 2
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_horizon"] == 1
    # Only the modern day row survived.
    assert len(records) == 1
    assert records[0]["horizon_type"] == "day"
    # And per-model breakdown reflects only the kept row.
    assert per_model.get("TFT", 0) == 1
    # Manifest path is still referenceable (smoke check the helper wrote it).
    assert manifest.is_file()


def test_build_record_excludes_null_quantile_fields():
    """Quantile fields with NULL / empty / NaN source value are OMITTED from
    the payload (universal safe-write rule — never sent as JSON ``null``).
    """
    row = {
        "code": "19999",
        "model_type": "TFT",
        "horizon_type": "day",
        "date": "2026-01-01",
        "target": "2026-01-02",
        "Q5": "3.5",
        "Q25": "",  # NULL
        "Q50": "nan",  # null-like
        "Q75": "garbage",  # unparseable
        "Q95": "7.5",
    }
    rec = ml_forecast._build_record(row, preserve_legacy_horizons=False)
    assert rec is not None
    assert rec["q05"] == 3.5
    assert rec["q95"] == 7.5
    # NULL / unparseable quantile fields must be ABSENT, not key-with-null.
    for absent in ("q25", "q50", "q75"):
        assert absent not in rec
    # Q50 missing AND no explicit forecasted_discharge means no
    # forecasted_discharge in the record either.
    assert "forecasted_discharge" not in rec


def test_build_record_returns_none_for_missing_required():
    """Row missing code / date / target -> None (counted as skipped_parse)."""
    base = {
        "code": "19999",
        "model_type": "TFT",
        "horizon_type": "day",
        "date": "2026-01-01",
        "target": "2026-01-02",
    }
    # Missing code.
    bad = dict(base)
    bad["code"] = ""
    assert ml_forecast._build_record(bad, preserve_legacy_horizons=False) is None
    # Missing date.
    bad = dict(base)
    bad["date"] = ""
    assert ml_forecast._build_record(bad, preserve_legacy_horizons=False) is None
    # Missing target.
    bad = dict(base)
    bad["target"] = ""
    assert ml_forecast._build_record(bad, preserve_legacy_horizons=False) is None


# ===========================================================================
# 5. _read_filtered_records — filters
# ===========================================================================


def test_read_filtered_records_station_filter(tmp_path):
    """``station_filter`` drops rows whose ``code`` does not match."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
            {
                "code": "00000",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-03",
                "target": "2026-01-04",
            },
        ],
    )
    records, counters, codes, _, _, _ = ml_forecast._read_filtered_records(
        csv,
        cutoff=None,
        station_filter="19999",
        model_filter=None,
        preserve_legacy_horizons=False,
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 2
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    for rec in records:
        assert rec["code"] == "19999"


def test_read_filtered_records_cutoff(tmp_path):
    """``cutoff`` drops rows whose ``date >= cutoff`` (strict less-than survives)."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-03",
                "target": "2026-01-04",
            },
        ],
    )
    records, counters, _codes, _, _, _ = ml_forecast._read_filtered_records(
        csv,
        cutoff="2026-01-02",
        station_filter=None,
        model_filter=None,
        preserve_legacy_horizons=False,
    )
    # Strict less-than: 2026-01-01 only.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2026-01-01"


def test_read_filtered_records_model_filter_normalizes_case(tmp_path):
    """``model_filter='TIDE'`` (legacy dir form) restricts to API ``TiDE``."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "TiDE",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
            {
                "code": "19999",
                "model_type": "TSMixer",
                "horizon_type": "day",
                "date": "2026-01-03",
                "target": "2026-01-04",
            },
        ],
    )
    records, counters, _codes, _, _, per_model = ml_forecast._read_filtered_records(
        csv,
        cutoff=None,
        station_filter=None,
        model_filter="TIDE",  # uppercase legacy dir form
        preserve_legacy_horizons=False,
    )
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_model"] == 2
    assert records[0]["model_type"] == "TiDE"
    assert per_model == {"TiDE": 1}


def test_read_filtered_records_unknown_model_counted_not_raised(tmp_path):
    """Unknown source ``model_type`` does NOT raise — it's counted as
    ``skipped_unknown_model`` so a partial bad export can still migrate the
    valid rows (and the operator sees the count in the dry-run inventory)."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "GBT",  # unknown — should be skipped
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
        ],
    )
    records, counters, _codes, _, _, _ = ml_forecast._read_filtered_records(
        csv,
        cutoff=None,
        station_filter=None,
        model_filter=None,
        preserve_legacy_horizons=False,
    )
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_unknown_model"] == 1
    assert records[0]["model_type"] == "TFT"


# ===========================================================================
# 6. main() dry-run output shape + per-model breakdown
# ===========================================================================


def test_main_dry_run_emits_per_model_breakdown(tmp_path, capsys):
    """The dry-run inventory line ``ML_PER_MODEL_COUNTS={TFT: N, TiDE: N,
    TSMixer: N}`` is always emitted, with zeroes for missing models."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
            {
                "code": "19999",
                "model_type": "TiDE",
                "horizon_type": "day",
                "date": "2026-01-03",
                "target": "2026-01-04",
            },
        ],
    )
    exit_code = ml_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=full-import" in out
    assert "TARGET_TABLE=forecasts" in out
    assert "SOURCE_ROW_COUNT=3" in out
    assert "FILTERED_ROW_COUNT=3" in out
    assert "DISTINCT_STATION_COUNT_REDACTED=1" in out
    # Per-model breakdown always lists all three keys, zero for absent.
    assert "ML_PER_MODEL_COUNTS={TFT: 2, TiDE: 1, TSMixer: 0}" in out


def test_main_dry_run_model_filter_flag_restricts_to_one_model(tmp_path, capsys):
    """``--model TFT`` restricts the dry-run to that single model variant."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "day",
                "date": "2026-01-01",
                "target": "2026-01-02",
            },
            {
                "code": "19999",
                "model_type": "TiDE",
                "horizon_type": "day",
                "date": "2026-01-02",
                "target": "2026-01-03",
            },
        ],
    )
    exit_code = ml_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/forecast/",
            "--model",
            "TFT",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SOURCE_ROW_COUNT=2" in out
    assert "FILTERED_ROW_COUNT=1" in out
    assert "SKIPPED_MODEL=1" in out
    assert "ML_PER_MODEL_COUNTS={TFT: 1, TiDE: 0, TSMixer: 0}" in out


def test_main_dry_run_legacy_flag_emits_warning(tmp_path, capsys):
    """With ``--preserve-legacy-ml-horizons``, the dry-run inventory carries
    a prominent WARNING line so the operator sees the lock-override in
    stdout (and the wrapper tees stdout into the run log)."""
    csv, _manifest = _write_csv_and_manifest(
        tmp_path,
        [
            {
                "code": "19999",
                "model_type": "TFT",
                "horizon_type": "pentad",  # legacy row
                "date": "2026-01-01",
                "target": "2026-01-06",
            },
        ],
    )
    exit_code = ml_forecast.main(
        [
            "--csv-path",
            str(csv),
            "--api-url",
            "http://localhost:8003/forecast/",
            "--preserve-legacy-ml-horizons",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "preserve-legacy-ml-horizons" in out
    # The legacy row survived (vs the default-rejects-pentad case).
    assert "FILTERED_ROW_COUNT=1" in out


def test_main_missing_csv_returns_nonzero(tmp_path):
    exit_code = ml_forecast.main(
        [
            "--csv-path",
            str(tmp_path / "does_not_exist.csv"),
            "--api-url",
            "http://localhost:8003/forecast/",
            "--dry-run",
        ]
    )
    assert exit_code != 0


# ===========================================================================
# 7. Stdlib-only audit covers ml_forecast
# ===========================================================================


def test_ml_forecast_module_imports_only_stdlib_and_intra_package():
    """The ml_forecast module passes the P0 stdlib-only audit. The audit
    walks every ``*.py`` under bin/utils/migration_py/ — a clean result
    proves ml_forecast.py is part of the clean set."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


_FIXTURE_DIR = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "ml_forecast"
)

_RAW_FIXTURE_ROOT = _FIXTURE_DIR / "raw_predictions"


# ===========================================================================
# 8. Workstream A raw ML -> export reshape helper
# ===========================================================================


def _run_raw_to_export(args: list[str]) -> int:
    from migration_py import ml_raw_to_export

    return ml_raw_to_export.main(args)


def _read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    import csv

    with csv_path.open(newline="") as f:
        return list(csv.DictReader(f))


def test_raw_local_branch_file_cannot_be_fed_directly_to_ml_importer():
    """Local-branch raw ML files do not carry export-only ``model_type`` /
    ``target`` columns, so feeding them directly to ``migration_py.ml_forecast``
    must fail before any API write path is reachable."""
    raw_csv = _RAW_FIXTURE_ROOT / "predictions" / "TFT" / "pentad_TFT_forecast.csv"
    assert raw_csv.is_file()

    with pytest.raises(ValueError, match="model_type|target"):
        ml_forecast._read_filtered_records(
            raw_csv,
            cutoff=None,
            station_filter=None,
            model_filter=None,
            preserve_legacy_horizons=False,
        )


def test_ml_raw_to_export_reshapes_contract_and_manifest(tmp_path):
    """Raw local-branch ML rows are reshaped to the export schema consumed by
    ``migration_py.ml_forecast``.

    ``Q10`` / ``Q90`` are intentionally ignored for short-term ML because the
    target forecast schema and the operational writer store only Q5/Q25/Q75/Q95
    plus ``forecasted_discharge``.
    """
    out_csv = tmp_path / "ml_export.csv"
    exit_code = _run_raw_to_export(["--data-ref", str(_RAW_FIXTURE_ROOT), "--out", str(out_csv)])
    assert exit_code == 0

    rows = _read_csv_rows(out_csv)
    assert len(rows) == 4
    assert set(rows[0]) == {
        "code",
        "model_type",
        "horizon_type",
        "date",
        "target",
        "flag",
        "Q5",
        "Q25",
        "Q50",
        "Q75",
        "Q95",
        "forecasted_discharge",
    }
    assert "Q10" not in rows[0]
    assert "Q90" not in rows[0]

    by_model_target = {(r["model_type"], r["target"]): r for r in rows}
    tft = by_model_target[("TFT", "2026-01-10")]
    assert tft["code"] == "19999"
    assert tft["horizon_type"] == "day"
    assert tft["date"] == "2026-01-01"  # issue date <- raw forecast_date
    assert tft["target"] == "2026-01-10"  # target date <- raw date
    assert tft["Q50"] == "12.50"
    assert tft["forecasted_discharge"] == "12.50"
    assert tft["flag"] == "0"

    assert by_model_target[("TiDE", "2026-02-11")]["date"] == "2026-02-01"
    assert by_model_target[("TSMixer", "2026-03-06")]["date"] == "2026-03-01"

    manifest = _common.validate_manifest(out_csv, "ml_forecast")
    assert manifest["row_count"] == "4"
    assert manifest["station_count"] == "2"
    # Manifest range is over reshaped issue-date column, not raw target dates.
    assert manifest["date_min"] == "2026-01-01"
    assert manifest["date_max"] == "2026-03-01"

    records, counters, parsed_codes, date_min, date_max, per_model = (
        ml_forecast._read_filtered_records(
            out_csv,
            cutoff=None,
            station_filter=None,
            model_filter=None,
            preserve_legacy_horizons=False,
        )
    )
    assert counters["filtered_row_count"] == 4
    assert parsed_codes == {"19999", "00001"}
    assert date_min == "2026-01-01"
    assert date_max == "2026-03-01"
    assert per_model == {"TFT": 2, "TiDE": 1, "TSMixer": 1}
    assert {rec["horizon_type"] for rec in records} == {"day"}


def test_ml_raw_to_export_dry_run_inventory_redacts_station_codes(tmp_path, capsys):
    out_csv = tmp_path / "dry_run_export.csv"
    exit_code = _run_raw_to_export(
        [
            "--data-ref",
            str(_RAW_FIXTURE_ROOT),
            "--out",
            str(out_csv),
            "--dry-run",
        ]
    )
    assert exit_code == 0
    assert not out_csv.exists()
    assert not out_csv.with_name(out_csv.name + ".manifest").exists()

    out = capsys.readouterr().out
    assert "SOURCE_ROW_COUNT=4" in out
    assert "FILTERED_ROW_COUNT=4" in out
    assert "ISSUE_DATE_MIN=2026-01-01" in out
    assert "ISSUE_DATE_MAX=2026-03-01" in out
    assert "TARGET_DATE_MIN=2026-01-10" in out
    assert "TARGET_DATE_MAX=2026-03-06" in out
    assert "SKIPPED_BAD_DATE=0" in out
    assert "SKIPPED_MISSING_REQUIRED=0" in out
    assert "SKIPPED_UNKNOWN_MODEL=0" in out
    assert "RAW_ML_PER_MODEL_COUNTS={TFT: 2, TiDE: 1, TSMixer: 1}" in out
    assert "19999" not in out
    assert "00001" not in out


def test_ml_raw_to_export_station_filter(tmp_path):
    out_csv = tmp_path / "station_export.csv"
    exit_code = _run_raw_to_export(
        [
            "--data-ref",
            str(_RAW_FIXTURE_ROOT),
            "--out",
            str(out_csv),
            "--station-filter",
            "00001",
        ]
    )
    assert exit_code == 0

    rows = _read_csv_rows(out_csv)
    assert len(rows) == 1
    assert rows[0]["code"] == "00001"
    manifest = _common.validate_manifest(out_csv, "ml_forecast")
    assert manifest["row_count"] == "1"
    assert manifest["station_count"] == "1"


def test_ml_raw_to_export_model_filter_accepts_legacy_dir_spelling(tmp_path):
    out_csv = tmp_path / "model_export.csv"
    exit_code = _run_raw_to_export(
        [
            "--data-ref",
            str(_RAW_FIXTURE_ROOT),
            "--out",
            str(out_csv),
            "--model",
            "TIDE",
        ]
    )
    assert exit_code == 0

    rows = _read_csv_rows(out_csv)
    assert len(rows) == 1
    assert rows[0]["model_type"] == "TiDE"
    assert rows[0]["target"] == "2026-02-11"


def test_ml_raw_to_export_rejects_bad_dates(tmp_path):
    data_ref = tmp_path / "raw"
    model_dir = data_ref / "predictions" / "TFT"
    model_dir.mkdir(parents=True)
    (model_dir / "pentad_TFT_forecast.csv").write_text(
        (
            "Q5,Q25,Q50,Q75,Q95,date,code,forecast_date,flag\n"
            "10.05,11.25,12.50,13.75,14.95,not-a-date,19999,2026-01-01,0\n"
        ),
        encoding="utf-8",
    )
    out_csv = tmp_path / "bad_dates.csv"

    exit_code = _run_raw_to_export(["--data-ref", str(data_ref), "--out", str(out_csv)])
    assert exit_code != 0
    assert not out_csv.exists()


def test_ml_raw_to_export_rejects_missing_required_raw_columns(tmp_path):
    data_ref = tmp_path / "raw"
    model_dir = data_ref / "predictions" / "TFT"
    model_dir.mkdir(parents=True)
    (model_dir / "pentad_TFT_forecast.csv").write_text(
        ("Q5,Q25,Q50,Q75,Q95,date,code,flag\n10.05,11.25,12.50,13.75,14.95,2026-01-10,19999,0\n"),
        encoding="utf-8",
    )
    out_csv = tmp_path / "missing_cols.csv"

    exit_code = _run_raw_to_export(["--data-ref", str(data_ref), "--out", str(out_csv)])
    assert exit_code != 0
    assert not out_csv.exists()


def test_ml_raw_to_export_rejects_unknown_model_dirs(tmp_path):
    data_ref = tmp_path / "raw"
    model_dir = data_ref / "predictions" / "UNKNOWN"
    model_dir.mkdir(parents=True)
    (model_dir / "pentad_UNKNOWN_forecast.csv").write_text(
        (
            "Q5,Q25,Q50,Q75,Q95,date,code,forecast_date,flag\n"
            "10.05,11.25,12.50,13.75,14.95,2026-01-10,19999,2026-01-01,0\n"
        ),
        encoding="utf-8",
    )
    out_csv = tmp_path / "unknown_model.csv"

    exit_code = _run_raw_to_export(["--data-ref", str(data_ref), "--out", str(out_csv)])
    assert exit_code != 0
    assert not out_csv.exists()


def test_ml_raw_to_export_no_rows_after_filter_is_failure(tmp_path):
    out_csv = tmp_path / "empty_export.csv"
    exit_code = _run_raw_to_export(
        [
            "--data-ref",
            str(_RAW_FIXTURE_ROOT),
            "--out",
            str(out_csv),
            "--station-filter",
            "19997",
        ]
    )
    assert exit_code != 0
    assert not out_csv.exists()


# ===========================================================================
# 9. Shipped fixtures round-trip through the parser
# ===========================================================================


def test_shipped_TFT_fixture_round_trips(tmp_path):
    """The shipped ``TFT_sample.csv`` parses through the helper. (The fixture
    ships without a .manifest sidecar — that's intentional, per the project's
    gitignore rule for ``.manifest`` files. This test generates a matching
    manifest in tmp_path and copies the CSV so the round-trip exercises the
    same code path the wrapper does.)"""
    src = _FIXTURE_DIR / "TFT_sample.csv"
    assert src.is_file()
    csv = tmp_path / "TFT_sample.csv"
    csv.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    # Auto-compute manifest by parsing the CSV.
    rows: list[dict[str, str]] = []
    import csv as _csv

    with src.open(newline="") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)
    codes = {(r.get("code") or "").strip() for r in rows if r.get("code")}
    dates = sorted((r.get("date") or "").strip() for r in rows if r.get("date"))
    manifest = tmp_path / "TFT_sample.csv.manifest"
    manifest.write_text(
        (
            "export_type=ml_forecast\n"
            f"row_count={len(rows)}\n"
            f"station_count={len(codes)}\n"
            f"date_min={dates[0]}\n"
            f"date_max={dates[-1]}\n"
        ),
        encoding="utf-8",
    )

    # Validate manifest passes.
    _common.validate_manifest(csv, "ml_forecast")

    # Parse rows.
    records, counters, parsed_codes, _, _, per_model = ml_forecast._read_filtered_records(
        csv,
        cutoff=None,
        station_filter=None,
        model_filter=None,
        preserve_legacy_horizons=False,
    )
    assert counters["filtered_row_count"] == len(rows)
    assert parsed_codes == {"19999"}
    assert per_model == {"TFT": len(rows)}
    for rec in records:
        assert rec["horizon_type"] == "day"
        assert rec["model_type"] == "TFT"


# ===========================================================================
# 10. Finding 11 (Tajik live test): PG enum-label SQL regression guard
# ===========================================================================


def _capture_query_target_state_sql(tmp_path):
    """Behaviorally exercise the wrapper's ``query_target_state`` shell
    function with a fake ``docker`` shell function that records the
    ``-c`` argument (the SQL the wrapper would send to psql).

    Approach:
      1. Copy the wrapper to ``tmp_path`` with its trailing ``main "$@"``
         line removed — sourcing the file then defines all functions
         (including ``query_target_state``) without invoking ``main``.
      2. Spawn a bash subshell that pre-defines ``docker`` as a function
         capturing its ``-c`` argument, then sources the stripped wrapper
         and calls ``query_target_state`` directly.

    Returns the SQL string the wrapper would have sent (or ``""`` if the
    helper failed to capture).
    """
    capture_file = tmp_path / "captured.sql"
    stripped_wrapper = tmp_path / "init_nomain.sh"

    # Strip the final ``main "$@"`` line so sourcing only defines functions.
    src = _WRAPPER.read_text(encoding="utf-8")
    src_lines = src.rstrip("\n").splitlines()
    while src_lines and not src_lines[-1].strip().startswith("main "):
        src_lines.pop()
    if src_lines and src_lines[-1].strip().startswith("main "):
        src_lines.pop()
    stripped_wrapper.write_text("\n".join(src_lines) + "\n", encoding="utf-8")

    driver = f"""
docker() {{
    local prev=""
    local got_sql=""
    for arg in "$@"; do
        if [[ "$prev" == "-c" ]]; then
            got_sql="$arg"
        fi
        prev="$arg"
    done
    if [[ -n "$got_sql" ]]; then
        printf '%s' "$got_sql" > {capture_file!s}
    fi
    # Emit a fake "count<TAB>min_date" line so the caller's parsing succeeds.
    printf '0\\t\\n'
    return 0
}}
# Set $0 to the ORIGINAL wrapper path so the wrapper's
# ``source "$(dirname "$0")/utils/update_migration_helpers.sh"`` resolves
# to the real helpers library.
source "{stripped_wrapper!s}"
query_target_state
"""
    result = subprocess.run(
        ["bash", "-c", driver, str(_WRAPPER)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    captured = capture_file.read_text(encoding="utf-8") if capture_file.is_file() else ""
    return result, captured


def test_query_target_state_sql_uses_uppercase_pg_enum_labels(tmp_path):
    """Finding 11 regression: ``query_target_state`` (the MODE-detection
    helper in ``bin/initialize_ml_forecast_history.sh``) must send SQL
    that compares ``model_type`` against the PG enum LABELS in UPPERCASE
    (``TFT``/``TIDE``/``TSMIXER``) via ``::text`` — never the mixed-case
    API wire values.

    Reverting to the old form
    (``model_type IN ('TFT','TiDE','TSMixer')``) MUST make this test FAIL.

    Authority for the two-representation rule:
    ``sapphire/services/postprocessing/app/models.py:23-24``.
    """
    result, captured = _capture_query_target_state_sql(tmp_path)
    assert result.returncode == 0, (
        f"driver exited non-zero: {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert captured, (
        "fake docker did not capture any SQL from query_target_state\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # Required: uppercase PG enum labels.
    assert "'TFT'" in captured, f"missing 'TFT' literal in SQL: {captured!r}"
    assert "'TIDE'" in captured, f"missing 'TIDE' literal in SQL: {captured!r}"
    assert "'TSMIXER'" in captured, f"missing 'TSMIXER' literal in SQL: {captured!r}"
    # Required: ``::text`` cast.
    assert "model_type::text" in captured, f"missing ``model_type::text`` cast in SQL: {captured!r}"
    # Forbidden: mixed-case API spellings as SQL literals.
    assert "'TiDE'" not in captured, f"mixed-case 'TiDE' SQL literal slipped back in: {captured!r}"
    assert "'TSMixer'" not in captured, (
        f"mixed-case 'TSMixer' SQL literal slipped back in: {captured!r}"
    )


def test_verify_sql_tip_uses_uppercase_pg_enum_labels():
    """The post-completion verify SQL the wrapper PRINTS to the operator
    must also use PG enum LABELS (uppercase + ``::text``) so the operator
    can paste the snippet straight into psql against the live DB without
    hitting ``invalid input value for enum modeltype: "TiDE"``.

    This is a source-text check because the echo lines are unconditional
    output after the docker-run step — exercising the full path would
    require a stack of stubs (docker, helpers, env file, manifest)
    disproportionate to the contract under test."""
    src = _WRAPPER.read_text(encoding="utf-8")
    assert "model_type::text IN ('TFT','TIDE','TSMIXER')" in src, (
        "post-run verify SQL tip must use uppercase PG enum labels with "
        "``::text`` cast (Finding 11)"
    )
    # The mixed-case literals must NOT appear in the verify tip.
    assert "'TiDE'" not in src or src.count("'TiDE'") == 0, (
        "mixed-case 'TiDE' literal must not appear in wrapper SQL"
    )
    assert "'TSMixer'" not in src, "mixed-case 'TSMixer' literal must not appear in wrapper SQL"
