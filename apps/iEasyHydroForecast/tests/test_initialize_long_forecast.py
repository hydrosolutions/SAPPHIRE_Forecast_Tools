"""Tests for the P5 long-term forecast migration wrapper + Python POST module.

Covers:
- bash wrapper CLI surface (`--help`, missing env file rejection, station
  filter contract, mode/model filters, skip-mode flag).
- Mode discovery (skips ``monthly``, returns sorted list, handles missing dir).
- Mode config loading (extracts models + horizon_value + horizon_type).
- Hindcast CSV discovery (only models with existing files are returned).
- Per-model payload building:
    - LR family: q + standalone quantiles.
    - GBT family: above + ensemble fields (q_xgb / q_lgbm / q_catboost).
    - MC_ALD: above + q_loc.
- Universal safe-write rule: null/absent fields are OMITTED from payload.
- Filtering (cutoff, station-filter).
- UZB no-op acceptance (Stage E item #12): zero modes -> exit 0 with logged
  "no source data" message.
- Dry-run output: per-(mode,model) inventory lines, mode/model filters.
- Stdlib-only audit (module passes the P0 _audit walker).

Integration against a live docker stack is out of scope per architecture §Q7
(disposable integration belongs to a separate sprint). The wrapper's
docker-run path is exercised manually in the runbook §5.5 canary.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import long_forecast  # noqa: E402

_WRAPPER = _REPO_ROOT / "bin" / "initialize_long_forecast_history.sh"
_FIXTURE_DIR = (
    _REPO_ROOT
    / "apps"
    / "iEasyHydroForecast"
    / "tests"
    / "fixtures"
    / "migration_csv"
    / "long_forecast"
)


# ---------------------------------------------------------------------------
# Helpers — minimal LT directory layout in tmp_path
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path, mode: str, payload: dict) -> Path:
    """Write <tmp_path>/config/long_term_configs/<mode>.json."""
    cfg_dir = tmp_path / "config" / "long_term_configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / f"{mode}.json"
    cfg_path.write_text(json.dumps(payload), encoding="utf-8")
    return cfg_path


def _make_hindcast_csv(tmp_path: Path, mode: str, model: str, header: str, rows: list[str]) -> Path:
    """Write <tmp_path>/intermediate_data/long_term_predictions/<mode>/<model>/<model>_hindcast.csv."""
    data_dir = tmp_path / "intermediate_data" / "long_term_predictions" / mode / model
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_path = data_dir / f"{model}_hindcast.csv"
    csv_path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return csv_path


def _layout_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """Return (config_dir, data_dir) for the in-tmp_path LT layout."""
    return (
        tmp_path / "config" / "long_term_configs",
        tmp_path / "intermediate_data",
    )


# ---------------------------------------------------------------------------
# 1. Wrapper CLI surface
# ---------------------------------------------------------------------------


def test_wrapper_help_returns_zero_and_prints_usage():
    """`bash bin/initialize_long_forecast_history.sh --help` exits 0 and shows usage."""
    assert _WRAPPER.is_file(), f"wrapper missing: {_WRAPPER}"
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"expected exit 0, got {result.returncode}: {result.stderr}"
    assert "Usage" in result.stdout, f"stdout missing 'Usage': {result.stdout!r}"
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


def test_sample_fixtures_present_and_sentinel_only():
    """Shipped long_forecast fixtures: month_1.json + LR_Base + GBT hindcasts, sentinel codes only."""
    config_json = _FIXTURE_DIR / "month_1.json"
    lr_csv = _FIXTURE_DIR / "LR_Base_hindcast.csv"
    gbt_csv = _FIXTURE_DIR / "GBT_hindcast.csv"
    assert config_json.is_file(), f"fixture missing: {config_json}"
    assert lr_csv.is_file(), f"fixture missing: {lr_csv}"
    assert gbt_csv.is_file(), f"fixture missing: {gbt_csv}"
    # Sentinel-only enforcement at the file level (the fixture-guard test
    # walks the whole tree; this check makes the breach localised here too).
    for f in (lr_csv, gbt_csv):
        text = f.read_text(encoding="utf-8")
        five_digit = re.findall(r"\b\d{5}\b", text)
        allowed = {"19999"} | {f"0000{i}" for i in range(10)}
        for code in five_digit:
            assert code in allowed, f"non-sentinel code {code!r} in {f.name}"


# ---------------------------------------------------------------------------
# 4. _discover_modes finds JSON files
# ---------------------------------------------------------------------------


def test_discover_modes_finds_json_files(tmp_path):
    _make_config(tmp_path, "month_1", {"models_to_use": {}, "operational_month_lead_time": 1})
    _make_config(tmp_path, "month_2", {"models_to_use": {}, "operational_month_lead_time": 2})
    _make_config(tmp_path, "quarter", {"models_to_use": {}, "operational_month_lead_time": 3})
    config_dir, _ = _layout_dirs(tmp_path)
    modes = long_forecast._discover_modes(config_dir)
    assert modes == ["month_1", "month_2", "quarter"]


def test_discover_modes_returns_empty_on_missing_directory(tmp_path):
    """UZB no-op precondition: a missing config dir returns []."""
    config_dir = tmp_path / "config" / "long_term_configs"  # never created
    modes = long_forecast._discover_modes(config_dir)
    assert modes == []


# ---------------------------------------------------------------------------
# 5. _discover_modes skips `monthly` even when present
# ---------------------------------------------------------------------------


def test_discover_modes_skips_monthly(tmp_path):
    """The 'monthly' mode JSON must be HARD-SKIPPED even if it exists on disk
    (non-operational per apps/long_term_forecasting/lt_schedule_query.py:54-91)."""
    _make_config(tmp_path, "month_1", {"models_to_use": {}, "operational_month_lead_time": 1})
    _make_config(tmp_path, "monthly", {"models_to_use": {}, "operational_month_lead_time": 1})
    _make_config(tmp_path, "quarter", {"models_to_use": {}, "operational_month_lead_time": 3})
    config_dir, _ = _layout_dirs(tmp_path)
    modes = long_forecast._discover_modes(config_dir)
    assert "monthly" not in modes
    assert modes == ["month_1", "quarter"]


# ---------------------------------------------------------------------------
# 6. _load_mode_config extracts models + horizon_value + horizon_type
# ---------------------------------------------------------------------------


def test_load_mode_config_extracts_models_and_horizon(tmp_path):
    _make_config(
        tmp_path,
        "month_1",
        {
            "models_to_use": {
                "LR_family": ["LR_Base", "LR_SM"],
                "GBT_family": ["GBT"],
            },
            "operational_month_lead_time": 1,
            "horizon_type": "month",
        },
    )
    config_dir, _ = _layout_dirs(tmp_path)
    cfg = long_forecast._load_mode_config(config_dir, "month_1")
    assert set(cfg["models"]) == {"LR_Base", "LR_SM", "GBT"}
    assert cfg["horizon_value"] == 1
    assert cfg["horizon_type"] == "month"


def test_load_mode_config_defaults_horizon_type_to_month(tmp_path):
    """When horizon_type is absent from the JSON, defaults to 'month'."""
    _make_config(
        tmp_path,
        "month_2",
        {
            "models_to_use": {"LR_family": ["LR_Base"]},
            "operational_month_lead_time": 2,
        },
    )
    config_dir, _ = _layout_dirs(tmp_path)
    cfg = long_forecast._load_mode_config(config_dir, "month_2")
    assert cfg["horizon_type"] == "month"
    assert cfg["horizon_value"] == 2


# ---------------------------------------------------------------------------
# 7. _load_mode_config raises on missing JSON
# ---------------------------------------------------------------------------


def test_load_mode_config_raises_on_missing_json(tmp_path):
    config_dir, _ = _layout_dirs(tmp_path)
    config_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(FileNotFoundError, match="config not found"):
        long_forecast._load_mode_config(config_dir, "month_99")


def test_load_mode_config_raises_on_malformed_json(tmp_path):
    config_dir = tmp_path / "config" / "long_term_configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "month_1.json").write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid JSON"):
        long_forecast._load_mode_config(config_dir, "month_1")


def test_load_mode_config_raises_on_missing_models_to_use(tmp_path):
    _make_config(tmp_path, "month_1", {"operational_month_lead_time": 1})
    config_dir, _ = _layout_dirs(tmp_path)
    with pytest.raises(ValueError, match="models_to_use"):
        long_forecast._load_mode_config(config_dir, "month_1")


# ---------------------------------------------------------------------------
# 8. _discover_hindcast_csvs only returns existing files
# ---------------------------------------------------------------------------


def test_discover_hindcast_csvs_only_returns_existing(tmp_path):
    """Models without a hindcast CSV must NOT appear in the returned dict."""
    _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        ["19999,2024-01-22,2024-02-01,2024-02-29,12.3"],
    )
    # No CSV for GBT.
    _, data_dir = _layout_dirs(tmp_path)
    found = long_forecast._discover_hindcast_csvs(data_dir, "month_1", ["LR_Base", "GBT", "MC_ALD"])
    assert "LR_Base" in found
    assert "GBT" not in found
    assert "MC_ALD" not in found
    assert found["LR_Base"].is_file()


# ---------------------------------------------------------------------------
# 9. _build_record LR family minimal quantiles
# ---------------------------------------------------------------------------


def test_build_record_LR_model_minimal_quantiles():
    """LR family: row with Q_LR_Base + Q5/Q25/Q50/Q75/Q95 -> payload has
    q + q05/q25/q50/q75/q95. NO ensemble fields, NO q_loc."""
    row = {
        "code": "19999",
        "date": "2024-01-22",
        "valid_from": "2024-02-01",
        "valid_to": "2024-02-29",
        "flag": "0",
        "Q_LR_Base": "12.3",
        "Q5": "8.5",
        "Q25": "10.5",
        "Q50": "12.3",
        "Q75": "14.0",
        "Q95": "16.5",
    }
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    rec = long_forecast._build_record(row, "LR_Base", mode_config)
    assert rec is not None
    assert rec["horizon_type"] == "month"
    assert rec["horizon_value"] == 1
    assert rec["code"] == "19999"
    assert rec["model_type"] == "LR_Base"
    assert rec["date"] == "2024-01-22"
    assert rec["valid_from"] == "2024-02-01"
    assert rec["valid_to"] == "2024-02-29"
    assert rec["flag"] == 0
    assert rec["q"] == 12.3
    assert rec["q05"] == 8.5
    assert rec["q25"] == 10.5
    assert rec["q50"] == 12.3
    assert rec["q75"] == 14.0
    assert rec["q95"] == 16.5
    # LR has no ensemble / q_loc fields.
    for absent in ("q_xgb", "q_lgbm", "q_catboost", "q_loc"):
        assert absent not in rec


# ---------------------------------------------------------------------------
# 10. _build_record GBT family includes ensemble fields
# ---------------------------------------------------------------------------


def test_build_record_GBT_model_includes_ensemble_fields():
    """GBT family: row with Q_GBT + Q_GBT_xgb/_lgbm/_catboost -> payload
    includes q + q_xgb/q_lgbm/q_catboost."""
    row = {
        "code": "19999",
        "date": "2024-01-22",
        "valid_from": "2024-02-01",
        "valid_to": "2024-02-29",
        "Q_GBT": "13.1",
        "Q5": "9.0",
        "Q95": "17.0",
        "Q_GBT_xgb": "13.0",
        "Q_GBT_lgbm": "13.2",
        "Q_GBT_catboost": "13.1",
    }
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    rec = long_forecast._build_record(row, "GBT", mode_config)
    assert rec is not None
    assert rec["model_type"] == "GBT"
    assert rec["q"] == 13.1
    assert rec["q05"] == 9.0
    assert rec["q95"] == 17.0
    # Ensemble fields present.
    assert rec["q_xgb"] == 13.0
    assert rec["q_lgbm"] == 13.2
    assert rec["q_catboost"] == 13.1
    # No q_loc since column is absent.
    assert "q_loc" not in rec


# ---------------------------------------------------------------------------
# 11. _build_record MC_ALD includes q_loc
# ---------------------------------------------------------------------------


def test_build_record_MC_ALD_includes_q_loc():
    """MC_ALD: a row with Q_loc -> payload has q_loc."""
    row = {
        "code": "19999",
        "date": "2024-01-22",
        "valid_from": "2024-02-01",
        "valid_to": "2024-02-29",
        "Q_MC_ALD": "11.0",
        "Q5": "8.0",
        "Q95": "14.0",
        "Q_loc": "10.9",
    }
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    rec = long_forecast._build_record(row, "MC_ALD", mode_config)
    assert rec is not None
    assert rec["model_type"] == "MC_ALD"
    assert rec["q"] == 11.0
    assert rec["q_loc"] == 10.9


# ---------------------------------------------------------------------------
# 12. _build_record excludes null fields (universal safe-write)
# ---------------------------------------------------------------------------


def test_build_record_excludes_null_fields():
    """Universal safe-write rule: empty/NaN/garbage cells are OMITTED from
    the payload — never sent as null."""
    row = {
        "code": "19999",
        "date": "2024-01-22",
        "valid_from": "2024-02-01",
        "valid_to": "2024-02-29",
        "flag": "",  # null
        "Q_LR_Base": "12.3",
        "Q5": "nan",  # null-like
        "Q25": "",
        "Q50": "12.3",
        "Q75": "garbage",  # parse fail
        "Q95": "16.5",
    }
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    rec = long_forecast._build_record(row, "LR_Base", mode_config)
    assert rec is not None
    assert rec["q"] == 12.3
    assert rec["q50"] == 12.3
    assert rec["q95"] == 16.5
    # NULL fields must be ABSENT.
    for absent in ("flag", "q05", "q25", "q75"):
        assert absent not in rec


def test_build_record_returns_none_for_missing_required():
    """Row missing any of (code, date, valid_from, valid_to) -> None."""
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    base = {
        "code": "19999",
        "date": "2024-01-22",
        "valid_from": "2024-02-01",
        "valid_to": "2024-02-29",
    }
    # Confirm baseline is parseable.
    assert long_forecast._build_record(base, "LR_Base", mode_config) is not None
    # Drop each required field in turn.
    for key in ("code", "date", "valid_from", "valid_to"):
        row = dict(base)
        row[key] = ""
        assert long_forecast._build_record(row, "LR_Base", mode_config) is None, (
            f"expected None when {key!r} is empty"
        )


# ---------------------------------------------------------------------------
# 13. _read_filtered_records station filter
# ---------------------------------------------------------------------------


def test_read_filtered_records_station_filter(tmp_path):
    csv = _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        [
            "19999,2024-01-22,2024-02-01,2024-02-29,12.3",
            "19999,2024-02-22,2024-03-01,2024-03-31,15.8",
            "00000,2024-03-22,2024-04-01,2024-04-30,18.0",
        ],
    )
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    records, counters, codes, _, _ = long_forecast._read_filtered_records(
        csv, "LR_Base", mode_config, cutoff=None, station_filter="19999"
    )
    assert counters["source_row_count"] == 3
    assert counters["filtered_row_count"] == 2
    assert counters["skipped_station"] == 1
    assert codes == {"19999"}
    for rec in records:
        assert rec["code"] == "19999"


# ---------------------------------------------------------------------------
# 14. _read_filtered_records cutoff
# ---------------------------------------------------------------------------


def test_read_filtered_records_cutoff(tmp_path):
    """Cutoff is strict: rows with date >= cutoff are dropped (pre-cutoff)."""
    csv = _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        [
            "19999,2024-01-22,2024-02-01,2024-02-29,12.3",
            "19999,2024-02-22,2024-03-01,2024-03-31,15.8",
            "19999,2024-03-22,2024-04-01,2024-04-30,21.4",
        ],
    )
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    records, counters, _, _, _ = long_forecast._read_filtered_records(
        csv, "LR_Base", mode_config, cutoff="2024-02-22", station_filter=None
    )
    # 2024-01-22 strictly < 2024-02-22 survives. 2024-02-22 and 2024-03-22 dropped.
    assert counters["filtered_row_count"] == 1
    assert counters["skipped_cutoff"] == 2
    assert records[0]["date"] == "2024-01-22"


# ---------------------------------------------------------------------------
# 15. UZB no-op acceptance: zero modes -> exit 0 with no-source message
# ---------------------------------------------------------------------------


def test_main_dry_run_zero_modes_returns_no_source_message(tmp_path, capsys):
    """Stage E item #12: when the config dir exists but has zero non-skipped
    modes, the wrapper exits 0 with a logged 'no source data' message."""
    # Create empty config dir.
    config_dir = tmp_path / "config" / "long_term_configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir = tmp_path / "intermediate_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "no source data" in out.lower()


def test_main_dry_run_missing_config_dir_returns_no_source(tmp_path, capsys):
    """When config dir doesn't exist at all (e.g. UZB demo with no LT setup),
    the module also exits 0 with no-source message."""
    config_dir = tmp_path / "config" / "long_term_configs"  # not created
    data_dir = tmp_path / "intermediate_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "no source data" in out.lower()


def test_main_dry_run_only_monthly_returns_no_source(tmp_path, capsys):
    """If the only configured mode is 'monthly' (always-skipped), the wrapper
    still emits the no-source message and exits 0."""
    _make_config(
        tmp_path,
        "monthly",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 1},
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "no source data" in out.lower()


# ---------------------------------------------------------------------------
# 16. main() dry-run includes mode/model inventory lines
# ---------------------------------------------------------------------------


def test_main_dry_run_includes_mode_inventory(tmp_path, capsys):
    """Dry-run output has one MODE_INVENTORY line per (mode, model) pair."""
    _make_config(
        tmp_path,
        "month_1",
        {
            "models_to_use": {"LR_family": ["LR_Base"], "GBT_family": ["GBT"]},
            "operational_month_lead_time": 1,
            "horizon_type": "month",
        },
    )
    _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        ["19999,2024-01-22,2024-02-01,2024-02-29,12.3"],
    )
    _make_hindcast_csv(
        tmp_path,
        "month_1",
        "GBT",
        "code,date,valid_from,valid_to,Q_GBT,Q_GBT_xgb",
        ["19999,2024-01-22,2024-02-01,2024-02-29,13.1,13.0"],
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "MODE=full-import" in out
    assert "TARGET_TABLE=long_forecasts" in out
    assert "DISCOVERED_MODES=['month_1']" in out
    # One MODE_INVENTORY line per (mode, model).
    lr_line = [
        line
        for line in out.splitlines()
        if line.startswith("MODE_INVENTORY") and "model=LR_Base" in line
    ]
    gbt_line = [
        line
        for line in out.splitlines()
        if line.startswith("MODE_INVENTORY") and "model=GBT" in line
    ]
    assert len(lr_line) == 1, f"expected one LR_Base inventory line; got: {lr_line}"
    assert len(gbt_line) == 1, f"expected one GBT inventory line; got: {gbt_line}"
    assert "status=ok" in lr_line[0]
    assert "status=ok" in gbt_line[0]
    assert "source_rows=1" in lr_line[0]


# ---------------------------------------------------------------------------
# 17. main() --skip-mode flag excludes configured modes
# ---------------------------------------------------------------------------


def test_main_dry_run_skip_mode_flag_excludes_mode(tmp_path, capsys):
    """--skip-mode month_1 -> month_1 NOT in DISCOVERED_MODES."""
    _make_config(
        tmp_path,
        "month_1",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 1},
    )
    _make_config(
        tmp_path,
        "month_2",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 2},
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--skip-mode",
            "month_1",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "DISCOVERED_MODES=['month_2']" in out
    assert "MODE_INVENTORY mode=month_1" not in out


def test_main_dry_run_skip_mode_comma_separated(tmp_path, capsys):
    """--skip-mode month_1,quarter skips both."""
    _make_config(
        tmp_path,
        "month_1",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 1},
    )
    _make_config(
        tmp_path,
        "month_2",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 2},
    )
    _make_config(
        tmp_path,
        "quarter",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 3},
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--skip-mode",
            "month_1,quarter",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "DISCOVERED_MODES=['month_2']" in out


def test_main_dry_run_mode_filter_restricts_to_single_mode(tmp_path, capsys):
    """--mode month_2 restricts to that mode only."""
    _make_config(
        tmp_path,
        "month_1",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 1},
    )
    _make_config(
        tmp_path,
        "month_2",
        {"models_to_use": {"LR_family": ["LR_Base"]}, "operational_month_lead_time": 2},
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--mode",
            "month_2",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "DISCOVERED_MODES=['month_2']" in out


# ---------------------------------------------------------------------------
# 18. Missing-hindcast warning
# ---------------------------------------------------------------------------


def test_main_dry_run_reports_missing_hindcast(tmp_path, capsys):
    """When a model in the config has no hindcast CSV, the dry-run output
    includes a MODE_INVENTORY line with status=MISSING_HINDCAST AND a
    'no hindcast for ...' warning line."""
    _make_config(
        tmp_path,
        "month_1",
        {"models_to_use": {"LR_family": ["LR_Base", "LR_SM"]}, "operational_month_lead_time": 1},
    )
    # Provide LR_Base hindcast but NOT LR_SM.
    _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        ["19999,2024-01-22,2024-02-01,2024-02-29,12.3"],
    )
    config_dir, data_dir = _layout_dirs(tmp_path)
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(config_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "no hindcast for mode=month_1 model=LR_SM" in out
    # The inventory still records the (mode, model) pair as MISSING_HINDCAST.
    missing = [
        line
        for line in out.splitlines()
        if line.startswith("MODE_INVENTORY") and "model=LR_SM" in line
    ]
    assert len(missing) == 1
    assert "status=MISSING_HINDCAST" in missing[0]


# ---------------------------------------------------------------------------
# 19a. SQL enum case in MODE detection uses lowercase 'month'
# ---------------------------------------------------------------------------


def test_mode_detection_query_uses_lowercase_month_enum():
    """The query_target_state SQL must use horizon_type='month' (lowercase).

    The DB/API enum stores the value as lowercase 'month'. If the query
    used 'MONTH', it would return 0 rows on a populated table, causing
    the wrapper to enter full-import mode and re-POST existing records.
    """
    src = _WRAPPER.read_text(encoding="utf-8")
    assert "horizon_type='month'" in src, (
        "query_target_state must use horizon_type='month' (lowercase); "
        "found uppercase or missing reference in the script"
    )
    assert "horizon_type='MONTH'" not in src, (
        "uppercase 'MONTH' found in script; must be lowercase 'month'"
    )


# ---------------------------------------------------------------------------
# 19b. _load_mode_config lowercases horizon_type
# ---------------------------------------------------------------------------


def test_load_mode_config_lowercases_horizon_type(tmp_path):
    """A config JSON with 'horizon_type': 'MONTH' (uppercase) must be
    normalised to lowercase 'month' so payloads hit the DB enum correctly."""
    _make_config(
        tmp_path,
        "month_1",
        {
            "models_to_use": {"LR_family": ["LR_Base"]},
            "operational_month_lead_time": 1,
            "horizon_type": "MONTH",
        },
    )
    config_dir, _ = _layout_dirs(tmp_path)
    cfg = long_forecast._load_mode_config(config_dir, "month_1")
    assert cfg["horizon_type"] == "month", (
        f"expected 'month' after lowercasing; got {cfg['horizon_type']!r}"
    )


# ---------------------------------------------------------------------------
# 19c. _load_mode_config rejects unknown horizon_type values
# ---------------------------------------------------------------------------


def test_load_mode_config_rejects_unknown_horizon_type(tmp_path):
    """A config JSON with an unsupported 'horizon_type' (e.g. 'week' or
    'pentad') must raise ValueError with a descriptive message.

    Only 'month' is valid for long-term forecasts per architecture §Q4.
    """
    _make_config(
        tmp_path,
        "month_1",
        {
            "models_to_use": {"LR_family": ["LR_Base"]},
            "operational_month_lead_time": 1,
            "horizon_type": "pentad",
        },
    )
    config_dir, _ = _layout_dirs(tmp_path)
    with pytest.raises(ValueError, match="horizon_type"):
        long_forecast._load_mode_config(config_dir, "month_1")


# ---------------------------------------------------------------------------
# 19. Stdlib-only audit covers long_forecast.py
# ---------------------------------------------------------------------------


def test_long_forecast_module_imports_only_stdlib_and_intra_package():
    """The long_forecast module passes the P0 stdlib-only audit alongside _common."""
    from migration_py import _audit

    violations = _audit.audit_stdlib_only(_REPO_ROOT / "bin" / "utils" / "migration_py")
    assert violations == [], (
        f"stdlib-only audit reported violations under migration_py/: {violations}"
    )


# ---------------------------------------------------------------------------
# 20. Wrapper help documents station-filter contract
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
# 21. Wrapper help documents --skip-mode / --mode / --model filters
# ---------------------------------------------------------------------------


def test_wrapper_help_documents_skip_mode_and_mode_filter():
    """The P5-specific filters (--mode, --model, --skip-mode) must appear in --help."""
    result = subprocess.run(
        ["bash", str(_WRAPPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    for flag in ("--mode", "--model", "--skip-mode"):
        assert flag in result.stdout, f"help is missing {flag!r}"


# ---------------------------------------------------------------------------
# 22. CSV with full record + pandas-style timestamps
# ---------------------------------------------------------------------------


def test_read_filtered_records_tolerates_pandas_style_timestamps(tmp_path):
    """Source CSVs may contain 'YYYY-MM-DD HH:MM:SS' dates; the date columns
    are trimmed to ISO-only before downstream use."""
    csv = _make_hindcast_csv(
        tmp_path,
        "month_1",
        "LR_Base",
        "code,date,valid_from,valid_to,Q_LR_Base",
        ["19999,2024-01-22 00:00:00,2024-02-01,2024-02-29,12.3"],
    )
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    records, counters, _, _, _ = long_forecast._read_filtered_records(
        csv, "LR_Base", mode_config, cutoff=None, station_filter=None
    )
    assert counters["filtered_row_count"] == 1
    assert records[0]["date"] == "2024-01-22"


# ---------------------------------------------------------------------------
# 23. CSV missing required columns -> ValueError surfaced as exit non-zero
# ---------------------------------------------------------------------------


def test_read_filtered_records_rejects_csv_missing_required_columns(tmp_path):
    csv_dir = tmp_path / "intermediate_data" / "long_term_predictions" / "month_1" / "LR_Base"
    csv_dir.mkdir(parents=True, exist_ok=True)
    bad = csv_dir / "LR_Base_hindcast.csv"
    # Missing 'valid_from' and 'valid_to'.
    bad.write_text("code,date,Q_LR_Base\n19999,2024-01-22,12.3\n", encoding="utf-8")
    mode_config = {"horizon_value": 1, "horizon_type": "month"}
    with pytest.raises(ValueError, match="missing required column"):
        long_forecast._read_filtered_records(
            bad, "LR_Base", mode_config, cutoff=None, station_filter=None
        )


# ---------------------------------------------------------------------------
# 24. main() integration with shipped fixtures
# ---------------------------------------------------------------------------


def test_main_dry_run_with_shipped_fixtures(tmp_path, capsys):
    """End-to-end dry-run using the shipped sentinel fixtures (month_1.json,
    LR_Base + GBT hindcasts) — confirms the full discover/load/build pipeline
    works on real-shaped artifacts."""
    # Mirror the on-disk layout into tmp_path so the wrapper layout assumption
    # (config + data dir) is satisfied.
    cfg_dir = tmp_path / "config" / "long_term_configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "month_1.json").write_text(
        (_FIXTURE_DIR / "month_1.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    for model in ("LR_Base", "GBT"):
        target_dir = tmp_path / "intermediate_data" / "long_term_predictions" / "month_1" / model
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / f"{model}_hindcast.csv").write_text(
            (_FIXTURE_DIR / f"{model}_hindcast.csv").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    data_dir = tmp_path / "intermediate_data"
    exit_code = long_forecast.main(
        [
            "--config-dir",
            str(cfg_dir),
            "--data-dir",
            str(data_dir),
            "--api-url",
            "http://localhost:8003/long-forecast/",
            "--dry-run",
        ]
    )
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "DISCOVERED_MODES=['month_1']" in out
    # Both fixture models report 3 source rows each.
    lr_lines = [
        line
        for line in out.splitlines()
        if line.startswith("MODE_INVENTORY") and "model=LR_Base" in line
    ]
    gbt_lines = [
        line
        for line in out.splitlines()
        if line.startswith("MODE_INVENTORY") and "model=GBT" in line
    ]
    assert len(lr_lines) == 1 and "source_rows=3" in lr_lines[0]
    assert len(gbt_lines) == 1 and "source_rows=3" in gbt_lines[0]
