"""Wiring integration tests for postprocessing entry points.

These tests call the actual entry point functions but let the *internal*
modules (data_reader, ensemble_calculator, gap_detector) run with real
logic against CSV files written to tmp_path.  Only external boundaries
(setup_library, forecast_library save/load, tag_library) are mocked.

This catches bugs in how the entry-point scripts wire internal modules
together - a class of defect that all-mock workflow tests miss.
"""

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.dirname(__file__))

import src.horizon_config as _real_horizon_config
from conftest import DECAD, PENTAD

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _import_operational():
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_module",
        os.path.join(SCRIPT_DIR, "postprocessing_operational.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _import_maintenance():
    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_module",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def _make_skill_csv(
    tmp_path,
    horizon_type="pentad",
    stations=None,
    models=None,
):
    """Write a skill metrics CSV where specified models pass thresholds.

    Default: station 15001 has LR + TFT both passing.
    """
    if stations is None:
        stations = ["15001"]
    if models is None:
        models = ["LR", "TFT"]

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    rows = []
    for station in stations:
        for model_short in models:
            rows.append(
                {
                    period_col: 1,
                    "code": station,
                    "model_short": model_short,
                    "sdivsigma": 0.3,
                    "nse": 0.9,
                    "delta": 5.0,
                    "accuracy": 0.95,
                    "mae": 2.0,
                    "n_pairs": 10,
                }
            )
    df = pd.DataFrame(rows)

    if horizon_type == "pentad":
        filename = "skill_pentad.csv"
    else:
        filename = "skill_decad.csv"
    _write_csv(df, os.path.join(str(tmp_path), filename))
    return df


def _make_modelled_df(
    stations=None,
    models=None,
    horizon_type="pentad",
    dates=None,
    discharge_values=None,
):
    """Build a modelled forecasts DataFrame."""
    if stations is None:
        stations = ["15001"]
    if models is None:
        models = ["LR", "TFT"]
    if dates is None:
        dates = [pd.Timestamp("2024-01-05")]
    if discharge_values is None:
        discharge_values = {"LR": 100.0, "TFT": 110.0}

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"
    rows = []
    for date in dates:
        for station in stations:
            for model_short in models:
                rows.append(
                    {
                        "code": station,
                        "date": date,
                        period_col: 1,
                        period_in_month_col: "1",
                        "forecasted_discharge": discharge_values.get(model_short, 100.0),
                        "model_short": model_short,
                    }
                )
    return pd.DataFrame(rows)


def _make_observed_df(stations=None, dates=None):
    """Build an observed DataFrame."""
    if stations is None:
        stations = ["15001"]
    if dates is None:
        dates = [pd.Timestamp("2024-01-05")]
    rows = []
    for date in dates:
        for station in stations:
            rows.append(
                {
                    "code": station,
                    "date": date,
                    "discharge_avg": 95.0,
                    "delta": 5.0,
                }
            )
    return pd.DataFrame(rows)


def _make_combined_csv(
    tmp_path,
    horizon_type="pentad",
    rows_data=None,
):
    """Write a combined forecasts CSV for gap detection."""
    if rows_data is None:
        rows_data = []
    df = pd.DataFrame(rows_data)
    if horizon_type == "pentad":
        filename = "combined_pentad.csv"
    else:
        filename = "combined_decad.csv"
    _write_csv(df, os.path.join(str(tmp_path), filename))
    return df


def _setup_real_internal_mocks(
    tmp_path,
    prediction_mode,
    observed_pentad=None,
    modelled_pentad=None,
    observed_decad=None,
    modelled_decad=None,
):
    """Set up sys.modules with real src.* modules and mocked externals.

    Real: data_reader, ensemble_calculator, gap_detector, skill_metrics
    Mocked: setup_library, file_writer (save functions)
    """
    import tag_library as real_tl
    from src import data_reader as real_data_reader
    from src import ensemble_calculator as real_ensemble_calc
    from src import gap_detector as real_gap_detector
    from src import postprocessing_tools as real_pt
    from src import skill_metrics as real_skill_metrics

    mock_sl = MagicMock()
    mock_file_writer = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    # Build per-horizon return values for the unified reader
    _pentad_data = (
        observed_pentad if observed_pentad is not None else pd.DataFrame(),
        modelled_pentad if modelled_pentad is not None else pd.DataFrame(),
    )
    _decad_data = (
        observed_decad if observed_decad is not None else pd.DataFrame(),
        modelled_decad if modelled_decad is not None else pd.DataFrame(),
    )

    def _mock_read_observed_and_modelled(horizon_type, **kwargs):
        if horizon_type == "pentad":
            return _pentad_data
        return _decad_data

    real_data_reader.read_observed_and_modelled_data = MagicMock(
        side_effect=_mock_read_observed_and_modelled
    )

    # PP-021: maintenance now calls read_individual_model_forecasts_for_dates
    # instead of read_observed_and_modelled_data.  Return the modelled portion
    # of the test data (same as the old reader's second element).
    def _mock_read_individual_for_dates(horizon_type, dates, codes=None):
        if horizon_type == "pentad":
            modelled = _pentad_data[1] if _pentad_data[1] is not None else pd.DataFrame()
        else:
            modelled = _decad_data[1] if _decad_data[1] is not None else pd.DataFrame()
        if not modelled.empty and dates:
            dates_ts = pd.to_datetime(list(dates))
            modelled = modelled[modelled["date"].isin(dates_ts)].copy()
        return modelled, pd.DataFrame()

    real_data_reader.read_individual_model_forecasts_for_dates = MagicMock(
        side_effect=_mock_read_individual_for_dates
    )

    mock_file_writer.save_forecast_data.return_value = None

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = real_tl

    # Use real src submodules
    real_src = MagicMock()
    real_src.postprocessing_tools = real_pt
    real_src.data_reader = real_data_reader
    real_src.ensemble_calculator = real_ensemble_calc
    real_src.gap_detector = real_gap_detector
    real_src.skill_metrics = real_skill_metrics
    real_src.file_writer = mock_file_writer
    real_src.horizon_config = _real_horizon_config

    sys.modules["src"] = real_src
    sys.modules["src.postprocessing_tools"] = real_pt
    sys.modules["src.data_reader"] = real_data_reader
    sys.modules["src.ensemble_calculator"] = real_ensemble_calc
    sys.modules["src.gap_detector"] = real_gap_detector
    sys.modules["src.skill_metrics"] = real_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer
    sys.modules["src.horizon_config"] = _real_horizon_config

    return {
        "sl": mock_sl,
        "file_writer": mock_file_writer,
        "data_reader": real_data_reader,
    }


# ---------------------------------------------------------------------------
# Shared env fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def env_setup(tmp_path):
    """Set env vars pointing to tmp_path for CSV reads."""
    # Write station selection files so _read_station_codes() doesn't raise
    station_config = {"stationsID": ["15001"]}
    (tmp_path / "config_station_selection.json").write_text(json.dumps(station_config))
    (tmp_path / "config_station_selection_decad.json").write_text(json.dumps(station_config))

    overrides = {
        "ieasyforecast_intermediate_data_path": str(tmp_path),
        "ieasyforecast_pentadal_skill_metrics_file": "skill_pentad.csv",
        "ieasyforecast_decadal_skill_metrics_file": "skill_decad.csv",
        "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
        "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
        "ieasyhydroforecast_efficiency_threshold": "0.6",
        "ieasyhydroforecast_accuracy_threshold": "0.8",
        "ieasyhydroforecast_nse_threshold": "0.8",
        "SAPPHIRE_API_ENABLED": "false",
        "SAPPHIRE_CONSISTENCY_CHECK": "false",
        "SAPPHIRE_TEST_ENV": "True",
        "ieasyforecast_configuration_path": str(tmp_path),
        "ieasyforecast_config_file_station_selection": "config_station_selection.json",
        "ieasyforecast_config_file_station_selection_decad": "config_station_selection_decad.json",
    }
    with patch.dict(os.environ, overrides):
        yield tmp_path


# ===================================================================
# TestOperationalWiringIntegration
# ===================================================================
class TestOperationalWiringIntegration:
    """Entry point calls real data_reader + ensemble_calculator."""

    def test_pentad_real_ensemble_created(self, env_setup):
        """Operational PENTAD: real ensemble EM created, EM discharge = mean.

        Skill CSV has LR + TFT passing for station 15001.
        Modelled: LR=100, TFT=110 => EM = 105.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        observed = _make_observed_df()
        modelled = _make_modelled_df(discharge_values={"LR": 100.0, "TFT": 110.0})

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Verify save was called
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                # Real ensemble should have been created
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) == 1, f"Expected 1 EM row, got {len(em_rows)}"
                em_discharge = em_rows["forecasted_discharge"].iloc[0]
                assert em_discharge == pytest.approx(105.0), (
                    f"Expected EM discharge=105.0, got {em_discharge}"
                )

    def test_pentad_empty_skill_csv_skips_ensemble(self, env_setup):
        """Empty skill CSV => save called but no EM rows."""
        tmp_path = env_setup
        # Write an empty skill CSV (header only)
        empty_skill = pd.DataFrame(
            columns=[
                "pentad_in_year",
                "code",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )
        _write_csv(
            empty_skill,
            os.path.join(str(tmp_path), "skill_pentad.csv"),
        )

        observed = _make_observed_df()
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Save is called with the original modelled (no EM)
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) == 0

    def test_both_mode_creates_pentad_and_decad_ensembles(self, env_setup):
        """BOTH mode: skill CSVs for both horizons => both save with EM."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")
        _make_skill_csv(tmp_path, "decad")

        observed = _make_observed_df()
        modelled_pentad = _make_modelled_df(
            horizon_type="pentad",
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )
        modelled_decad = _make_modelled_df(
            horizon_type="decad",
            discharge_values={"LR": 200.0, "TFT": 220.0},
        )

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "BOTH",
                    observed_pentad=observed,
                    modelled_pentad=modelled_pentad,
                    observed_decad=observed,
                    modelled_decad=modelled_decad,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                # Override boundary checks so both horizons process
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Unified save_forecast_data called twice (pentad + decad)
                assert mocks["file_writer"].save_forecast_data.call_count == 2

                # Identify pentad vs decad call by config name
                calls = mocks["file_writer"].save_forecast_data.call_args_list
                pentad_df = None
                decad_df = None
                for call in calls:
                    cfg = call[0][0]
                    df = call[0][1]
                    if cfg.name == "pentad":
                        pentad_df = df
                    elif cfg.name == "decad":
                        decad_df = df

                assert pentad_df is not None, "save_forecast_data not called for pentad"
                assert decad_df is not None, "save_forecast_data not called for decad"

                # Pentad EM
                pentad_em = pentad_df[pentad_df["model_short"] == "EM"]
                assert len(pentad_em) == 1
                assert pentad_em["forecasted_discharge"].iloc[0] == (pytest.approx(105.0))

                # Decad EM
                decad_em = decad_df[decad_df["model_short"] == "EM"]
                assert len(decad_em) == 1
                assert decad_em["forecasted_discharge"].iloc[0] == (pytest.approx(210.0))

    def test_save_error_with_real_ensemble(self, env_setup):
        """Real ensemble created but save returns error => exit 1."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        observed = _make_observed_df()
        modelled = _make_modelled_df(discharge_values={"LR": 100.0, "TFT": 110.0})

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )
                mocks["file_writer"].save_forecast_data.return_value = "Error: disk full"

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 1


# ===================================================================
# TestMaintenanceWiringIntegration
# ===================================================================
class TestMaintenanceWiringIntegration:
    """Entry point calls real gap_detector + ensemble_calculator."""

    def test_pentad_gap_detected_and_filled(self, env_setup):
        """Gap at Jan 5 => real gap detection finds it, real ensemble fills.

        Combined CSV has LR + TFT at Jan 5 but NO EM.
        Skill CSV has LR + TFT passing.
        Modelled data for gap date: LR=100, TFT=110 => EM=105.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        # Combined has individual models but no EM
        _make_combined_csv(
            tmp_path,
            "pentad",
            rows_data=[
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "TFT",
                    "forecasted_discharge": 110.0,
                    "pentad_in_year": 1,
                },
            ],
        )

        observed = _make_observed_df()
        modelled = _make_modelled_df(discharge_values={"LR": 100.0, "TFT": 110.0})

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0

                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) == 1, f"Expected 1 EM gap-fill row, got {len(em_rows)}"
                assert em_rows["forecasted_discharge"].iloc[0] == (pytest.approx(105.0))

    def test_no_gaps_skips_data_reading(self, env_setup):
        """Combined has EM for all dates => no gap-fill needed."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        # Combined has EM already
        _make_combined_csv(
            tmp_path,
            "pentad",
            rows_data=[
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "EM",
                    "forecasted_discharge": 105.0,
                    "pentad_in_year": 1,
                },
            ],
        )

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # Modelled data is now read before gap detection,
                # but no ensemble creation needed when no gaps
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_gap_dates_no_matching_modelled_data(self, env_setup):
        """Gap detected but modelled data has no matching rows => no save."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        # Jan 5 has LR + NE + EM (complete), Jan 10 has LR only (EM gap)
        # NE included at Jan 5 so NE gaps don't pull Jan 5 into affected dates
        _make_combined_csv(
            tmp_path,
            "pentad",
            rows_data=[
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "NE",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "EM",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-10",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 2,
                },
            ],
        )

        # Modelled data only has Jan 5 — doesn't match Jan 10 gap
        observed = _make_observed_df(dates=[pd.Timestamp("2024-01-05")])
        modelled = _make_modelled_df(dates=[pd.Timestamp("2024-01-05")])

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                # PP-021: scoped read called but returns no rows for Jan 10 gap
                mocks["data_reader"].read_individual_model_forecasts_for_dates.assert_called_once()
                # Early return — no save
                mocks["file_writer"].save_forecast_data.assert_not_called()

    def test_both_mode_fills_pentad_and_decad_gaps(self, env_setup):
        """BOTH mode with gaps in both horizons => both filled."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")
        _make_skill_csv(tmp_path, "decad")

        # Pentad gap: LR + TFT at Jan 5, no EM
        _make_combined_csv(
            tmp_path,
            "pentad",
            rows_data=[
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                },
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": "TFT",
                    "forecasted_discharge": 110.0,
                    "pentad_in_year": 1,
                },
            ],
        )
        # Decad gap: LR + TFT at Jan 10, no EM
        _make_combined_csv(
            tmp_path,
            "decad",
            rows_data=[
                {
                    "date": "2024-01-10",
                    "code": "15001",
                    "model_short": "LR",
                    "forecasted_discharge": 200.0,
                    "decad_in_year": 1,
                },
                {
                    "date": "2024-01-10",
                    "code": "15001",
                    "model_short": "TFT",
                    "forecasted_discharge": 220.0,
                    "decad_in_year": 1,
                },
            ],
        )

        observed_pentad = _make_observed_df()
        modelled_pentad = _make_modelled_df(
            horizon_type="pentad",
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )
        observed_decad = _make_observed_df(dates=[pd.Timestamp("2024-01-10")])
        modelled_decad = _make_modelled_df(
            horizon_type="decad",
            dates=[pd.Timestamp("2024-01-10")],
            discharge_values={"LR": 200.0, "TFT": 220.0},
        )

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "BOTH",
                    observed_pentad=observed_pentad,
                    modelled_pentad=modelled_pentad,
                    observed_decad=observed_decad,
                    modelled_decad=modelled_decad,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0

                # Unified save_forecast_data called twice (pentad + decad)
                assert mocks["file_writer"].save_forecast_data.call_count == 2

                # Identify pentad vs decad call by config name
                calls = mocks["file_writer"].save_forecast_data.call_args_list
                pentad_df = None
                decad_df = None
                for call in calls:
                    cfg = call[0][0]
                    df = call[0][1]
                    if cfg.name == "pentad":
                        pentad_df = df
                    elif cfg.name == "decad":
                        decad_df = df

                assert pentad_df is not None, "save_forecast_data not called for pentad"
                assert decad_df is not None, "save_forecast_data not called for decad"

                # Pentad EM
                pentad_em = pentad_df[pentad_df["model_short"] == "EM"]
                assert len(pentad_em) == 1
                assert pentad_em["forecasted_discharge"].iloc[0] == (pytest.approx(105.0))

                # Decad EM
                decad_em = decad_df[decad_df["model_short"] == "EM"]
                assert len(decad_em) == 1
                assert decad_em["forecasted_discharge"].iloc[0] == (pytest.approx(210.0))


# ===================================================================
# TestExceptionPropagation (Gap 3)
# ===================================================================
class TestExceptionPropagation:
    """Verify internal exceptions are not silently swallowed."""

    def test_maintenance_gap_read_exception_propagates(self, env_setup):
        """RuntimeError in read_combined_forecasts propagates uncaught."""
        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                _setup_real_internal_mocks(
                    env_setup,
                    "PENTAD",
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                # Patch data_reader.read_combined_forecasts after exec
                target = "src.data_reader.read_combined_forecasts"
                with patch(
                    target,
                    side_effect=RuntimeError("corrupt CSV"),
                ):
                    with pytest.raises(RuntimeError, match="corrupt CSV"):
                        module.postprocessing_maintenance()

    def test_operational_data_reader_exception_propagates(self, env_setup):
        """IOError in data_reader.read_skill_metrics propagates uncaught."""
        tmp_path = env_setup

        observed = _make_observed_df()
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                target = "src.data_reader.read_skill_metrics"
                with patch(
                    target,
                    side_effect=OSError("permission denied"),
                ):
                    with pytest.raises(IOError, match="permission denied"):
                        module.postprocessing_operational()


# ===================================================================
# TestMismatchedInputShapes (Gap 5)
# ===================================================================
class TestMismatchedInputShapes:
    """Verify graceful handling of empty observed/modelled from setup_library."""

    def test_operational_empty_observed_nonempty_modelled(self, env_setup):
        """Empty observed + real modelled => no crash, save called.

        Ensemble calculator should still run; whether EM rows appear
        depends on whether skill metrics can match — but it must not crash.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        empty_observed = pd.DataFrame(columns=["code", "date", "discharge_avg", "delta"])
        modelled = _make_modelled_df()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=empty_observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()

    def test_operational_nonempty_observed_empty_modelled(self, env_setup):
        """Real observed + empty modelled => no crash, save called."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        observed = _make_observed_df()
        empty_modelled = pd.DataFrame(
            columns=[
                "code",
                "date",
                "pentad_in_year",
                "pentad_in_month",
                "forecasted_discharge",
                "model_short",
            ]
        )

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=empty_modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                # No modelled data => saved DF is empty (no rows to process)
                assert saved_df.empty, (
                    f"Empty modelled input should produce empty saved DF, got {len(saved_df)} rows"
                )


# ===================================================================
# TestRecalcWiringIntegration (Gap #1)
# ===================================================================
def _import_recalc():
    spec = importlib.util.spec_from_file_location(
        "recalculate_skill_metrics_module",
        os.path.join(SCRIPT_DIR, "recalculate_skill_metrics.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _setup_recalc_mocks(
    tmp_path,
    observed_pentad=None,
    modelled_pentad=None,
    observed_decad=None,
    modelled_decad=None,
):
    """Set up sys.modules for recalc with real calculate_skill_metrics.

    Real: src.skill_metrics (calculate_skill_metrics,
          calculate_all_skill_metrics), postprocessing_tools
    Mocked: setup_library (data reading), file_writer (save functions)
    """
    from src import ensemble_calculator as real_ensemble_calc
    from src import postprocessing_tools as real_pt
    from src import skill_metrics as real_skill_metrics

    mock_sl = MagicMock()
    mock_file_writer = MagicMock()
    mock_data_reader = MagicMock()

    mock_sl.load_environment.return_value = None
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    # Build per-horizon return values for the unified reader
    _pentad_data = (
        observed_pentad if observed_pentad is not None else pd.DataFrame(),
        modelled_pentad if modelled_pentad is not None else pd.DataFrame(),
    )
    _decad_data = (
        observed_decad if observed_decad is not None else pd.DataFrame(),
        modelled_decad if modelled_decad is not None else pd.DataFrame(),
    )

    def _mock_read_observed_and_modelled(horizon_type, **kwargs):
        if horizon_type == "pentad":
            return _pentad_data
        return _decad_data

    mock_data_reader.read_observed_and_modelled_data = MagicMock(
        side_effect=_mock_read_observed_and_modelled
    )

    mock_file_writer.save_forecast_data.return_value = None
    mock_file_writer.save_skill_metrics.return_value = None

    import tag_library as real_tl

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = real_tl

    real_src = MagicMock()
    real_src.postprocessing_tools = real_pt
    real_src.skill_metrics = real_skill_metrics
    real_src.data_reader = mock_data_reader
    real_src.ensemble_calculator = real_ensemble_calc
    real_src.file_writer = mock_file_writer
    real_src.horizon_config = _real_horizon_config
    sys.modules["src"] = real_src
    sys.modules["src.postprocessing_tools"] = real_pt
    sys.modules["src.skill_metrics"] = real_skill_metrics
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = real_ensemble_calc
    sys.modules["src.file_writer"] = mock_file_writer
    sys.modules["src.horizon_config"] = _real_horizon_config

    return {
        "sl": mock_sl,
        "file_writer": mock_file_writer,
        "data_reader": mock_data_reader,
    }


def _make_recalc_observed(stations, dates, base_values):
    """Build observed DF with required columns for calculate_skill_metrics_*."""
    rows = []
    for date in dates:
        for station in stations:
            rows.append(
                {
                    "code": station,
                    "date": date,
                    "discharge_avg": base_values[station],
                    "delta": 5.0,
                    "model_short": "obs",
                }
            )
    return pd.DataFrame(rows)


def _make_recalc_modelled(
    stations,
    dates,
    model_values,
    horizon_type="pentad",
):
    """Build modelled DF with required columns for calculate_skill_metrics_*.

    Args:
        model_values: dict mapping model_short -> {station: discharge}.
    """
    import tag_library as tl

    period_col = "pentad_in_year" if horizon_type == "pentad" else "decad_in_year"
    period_in_month_col = "pentad_in_month" if horizon_type == "pentad" else "decad_in_month"
    get_period_func = tl.get_pentad if horizon_type == "pentad" else tl.get_decad_in_month
    rows = []
    for date in dates:
        pim = str(get_period_func(date + pd.Timedelta(days=1)))
        for station in stations:
            for ms in model_values:
                rows.append(
                    {
                        "code": station,
                        "date": date,
                        period_col: 1,
                        period_in_month_col: pim,
                        "forecasted_discharge": model_values[ms][station],
                        "model_short": ms,
                    }
                )
    return pd.DataFrame(rows)


class TestRecalcWiringIntegration:
    """Entry point calls real calculate_skill_metrics.

    Unlike operational/maintenance wiring tests that exercise
    data_reader + ensemble_calculator, recalc wiring tests exercise
    the full calculate_skill_metrics path inside forecast_library.
    """

    def _build_test_data(self):
        """Build realistic test data: 1 station, 5 dates, 2 models.

        Observed values have large spread so that both models
        easily pass all skill thresholds (sdivsigma<0.6, accuracy>0.8,
        NSE>0.8). Both LR and TFT closely track observed.

        Hand-check for LR:
            obs  = [80, 90, 100, 110, 120]  (mean=100, SS_obs=1000)
            LR   = [81, 91, 101, 111, 121]  (bias=+1)
            diff = [1, 1, 1, 1, 1]          MAE=1, all<=5 → accuracy=1.0
            SS_res = 5, NSE = 1 - 5/1000 = 0.995, sdivsigma ≈ 0.071
        """
        stations = ["15001"]
        dates = pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
            ]
        )
        obs_values = [80.0, 90.0, 100.0, 110.0, 120.0]

        orows = []
        for date, obs_val in zip(dates, obs_values, strict=True):
            for station in stations:
                orows.append(
                    {
                        "code": station,
                        "date": date,
                        "discharge_avg": obs_val,
                        "delta": 5.0,
                        "model_short": "obs",
                    }
                )
        observed = pd.DataFrame(orows)

        # LR: obs + 1, TFT: obs - 1
        # EM will be mean(LR, TFT) = obs (perfectly)
        import tag_library as tl

        frows = []
        for date, obs_val in zip(dates, obs_values, strict=True):
            pim = str(tl.get_pentad(date + pd.Timedelta(days=1)))
            for station in stations:
                frows.append(
                    {
                        "code": station,
                        "date": date,
                        "pentad_in_year": 1,
                        "pentad_in_month": pim,
                        "forecasted_discharge": obs_val + 1.0,
                        "model_short": "LR",
                    }
                )
                frows.append(
                    {
                        "code": station,
                        "date": date,
                        "pentad_in_year": 1,
                        "pentad_in_month": pim,
                        "forecasted_discharge": obs_val - 1.0,
                        "model_short": "TFT",
                    }
                )
        modelled = pd.DataFrame(frows)
        return observed, modelled

    def test_pentad_real_skill_calculated_and_saved(self, env_setup):
        """Real calculate_skill_metrics runs, saves both outputs.

        Verifies the full chain: read → calculate → save forecasts + skills.
        """
        tmp_path = env_setup
        observed, modelled = self._build_test_data()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_recalc_mocks(
                    tmp_path,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                # Both save functions called
                mocks["file_writer"].save_forecast_data.assert_called_once()
                mocks["file_writer"].save_skill_metrics.assert_called_once()

                # Verify skill metrics have correct structure
                saved_skill = mocks["file_writer"].save_skill_metrics.call_args[0][1]
                assert not saved_skill.empty, "Skill metrics should not be empty"
                for col in [
                    "pentad_in_year",
                    "code",
                    "model_short",
                    "sdivsigma",
                    "nse",
                    "mae",
                    "n_pairs",
                ]:
                    assert col in saved_skill.columns, f"Skill metrics missing column: {col}"

                # LR and TFT should both have skill rows
                skill_models = set(saved_skill["model_short"].unique())
                assert "LR" in skill_models
                assert "TFT" in skill_models

                # n_pairs = 5 (5 dates in pentad 1)
                for _, row in saved_skill.iterrows():
                    if row["model_short"] in ("LR", "TFT"):
                        assert row["n_pairs"] == 5, f"Expected n_pairs=5, got {row['n_pairs']}"

    def test_pentad_em_excluded_in_recalc(self, env_setup):
        """PP-030: recalc skips EM derivation — no EM rows in saved forecasts.

        The recalculation path now passes exclude_models=["EM"] to avoid
        boundary-pentad date misalignment producing bad EM records.
        Individual model rows (LR, TFT) should still be present.
        """
        observed, modelled = self._build_test_data()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_recalc_mocks(
                    env_setup,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                saved_fc = mocks["file_writer"].save_forecast_data.call_args[0][1]
                em_rows = saved_fc[saved_fc["model_short"] == "EM"]
                assert em_rows.empty, "Recalculation should not produce EM rows (PP-030)"

                # Individual models should still be present
                models = set(saved_fc["model_short"].unique())
                assert "LR" in models, "LR should still be in output"
                assert "TFT" in models, "TFT should still be in output"

    def test_timing_stats_handoff(self, env_setup):
        """timing_stats returned by calculate_skill_metrics is used.

        recalculate_skill_metrics.py has a pattern where it checks
        returned_timing_stats is not None. This test verifies the
        handoff works without error.
        """
        tmp_path = env_setup
        observed, modelled = self._build_test_data()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                _setup_recalc_mocks(
                    tmp_path,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                # Should complete without error; timing_stats handoff
                # is exercised internally
                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

    def test_save_error_in_skill_metrics_causes_exit_1(self, env_setup):
        """Skill metrics save error → exit code 1."""
        tmp_path = env_setup
        observed, modelled = self._build_test_data()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_recalc_mocks(
                    tmp_path,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )
                mocks["file_writer"].save_skill_metrics.return_value = "Error: write failed"

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 1


# ===================================================================
# TestMaintenanceSurplusData (#30)
# ===================================================================
class TestMaintenanceSurplusData:
    """Gap detection with surplus data — only gap dates should be filled."""

    def test_fills_only_gap_dates_not_surplus(self, env_setup):
        """Combined CSV has 3 dates, 2 already have EM.

        Only the 1 gap date should get new rows; the 2 existing dates
        should NOT be duplicated.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = []
        # Date 1 (Jan 5): LR + TFT + NE + EM — complete
        for ms in ["LR", "TFT", "NE", "EM"]:
            combined_rows.append(
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": ms,
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                }
            )
        # Date 2 (Jan 10): LR + TFT + NE + EM — complete
        for ms in ["LR", "TFT", "NE", "EM"]:
            combined_rows.append(
                {
                    "date": "2024-01-10",
                    "code": "15001",
                    "model_short": ms,
                    "forecasted_discharge": 200.0,
                    "pentad_in_year": 2,
                }
            )
        # Date 3 (Jan 15): LR + TFT but NO NE/EM — gap
        for ms in ["LR", "TFT"]:
            combined_rows.append(
                {
                    "date": "2024-01-15",
                    "code": "15001",
                    "model_short": ms,
                    "forecasted_discharge": 300.0,
                    "pentad_in_year": 3,
                }
            )
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        observed = _make_observed_df(dates=[pd.Timestamp("2024-01-15")])
        modelled = _make_modelled_df(
            dates=[pd.Timestamp("2024-01-15")],
            discharge_values={"LR": 300.0, "TFT": 310.0},
        )

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0

                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                # Merged: 2 existing EM (Jan 5, Jan 10) + 1 new (Jan 15)
                assert len(em_rows) == 3, (
                    f"Expected 3 EM rows (2 existing + 1 gap-fill), got {len(em_rows)}"
                )
                em_dates = sorted(pd.to_datetime(em_rows["date"]).dt.strftime("%Y-%m-%d"))
                assert "2024-01-15" in em_dates, f"Gap-fill EM for Jan 15 missing from {em_dates}"
                assert "2024-01-05" in em_dates
                assert "2024-01-10" in em_dates
                # combined(10) + 1 new EM for Jan 15 = 11 total
                assert len(saved_df) == 11, f"Expected 11 merged rows, got {len(saved_df)}"


# ===================================================================
# TestMaintenanceStaleRefreshWiring (PP-022)
# ===================================================================
class TestMaintenanceStaleRefreshWiring:
    """Stale-record refresh with real gap_detector + real ensemble_calculator.

    These tests exercise the production merge logic (lambda key matching,
    concat + drop_duplicates) that the mock-based tests in
    test_maintenance_workflow.py cannot cover.
    """

    def test_stale_individual_rows_get_quantiles(self, env_setup):
        """Stale LR/TFT rows (q05=NULL) are replaced with fresh rows.

        Combined CSV has LR+TFT+NE+EM at Jan 5, where LR+TFT have
        forecasted_discharge but q05=NULL. Maintenance should detect
        these as stale, re-read modelled data, and merge fresh rows
        with quantiles into the output.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = [
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "pentad_in_year": 1,
                "q05": None,  # stale
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "TFT",
                "forecasted_discharge": 110.0,
                "pentad_in_year": 1,
                "q05": None,  # stale
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "NE",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 90.0,  # ok
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "EM",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 85.0,  # ok
            },
        ]
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        # Modelled data: fresh LR+TFT with quantiles from the reader
        modelled = _make_modelled_df(
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                # All 4 model types should be in the output
                assert set(saved_df["model_short"].unique()) >= {"LR", "TFT", "NE", "EM"}

                # The stale LR/TFT rows should be replaced (keep="last")
                # with fresh rows from modelled data. Verify row count
                # didn't grow (dedup should replace, not duplicate).
                lr_rows = saved_df[
                    (saved_df["model_short"] == "LR")
                    & (saved_df["date"].astype(str).str.startswith("2024-01-05"))
                ]
                assert len(lr_rows) == 1, f"Expected 1 LR row, got {len(lr_rows)}"

    def test_stale_em_rows_get_refreshed(self, env_setup):
        """Stale EM row (q05=NULL) is replaced with fresh EM from ensemble calc.

        Combined has LR+TFT (good) + EM (stale q05=NULL). Maintenance
        detects stale EM, re-reads data, creates fresh EM with quantiles.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = [
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "pentad_in_year": 1,
                "q05": 80.0,
                "q50": 100.0,
                "q95": 120.0,
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "TFT",
                "forecasted_discharge": 110.0,
                "pentad_in_year": 1,
                "q05": 90.0,
                "q50": 110.0,
                "q95": 130.0,
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "NE",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 85.0,
                "q50": 105.0,
                "q95": 125.0,
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "EM",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": None,  # stale EM
            },
        ]
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        modelled = _make_modelled_df(
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) == 1, f"Expected 1 EM row, got {len(em_rows)}"
                # EM discharge = mean(LR, TFT) = 105
                assert em_rows["forecasted_discharge"].iloc[0] == pytest.approx(105.0)
                # ensemble_calculator creates EM with composition, not quantiles
                assert "composition" in saved_df.columns

    def test_stale_refresh_without_skill_metrics(self, env_setup):
        """Stale individual rows refreshed even without skill CSV.

        No skill CSV → no EM creation, but stale LR/TFT rows should
        still be refreshed from the re-read modelled data.
        """
        tmp_path = env_setup
        # Deliberately do NOT create a skill CSV

        combined_rows = [
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "pentad_in_year": 1,
                "q05": None,  # stale
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "NE",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 85.0,
            },
        ]
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        modelled = _make_modelled_df(
            models=["LR"],
            discharge_values={"LR": 100.0},
        )

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                # LR row should be in the output (refreshed)
                lr_rows = saved_df[saved_df["model_short"] == "LR"]
                assert not lr_rows.empty, "Stale LR row should have been refreshed"
                # No EM created (no skill metrics)
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert em_rows.empty, "EM should not be created without skill metrics"

    def test_mixed_stale_and_gap(self, env_setup):
        """Stale individual rows + EM gap at same date handled together.

        Combined has stale LR (q05=NULL) + no EM row. Maintenance should
        refresh LR AND create EM in a single pass.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = [
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "pentad_in_year": 1,
                "q05": None,  # stale
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "TFT",
                "forecasted_discharge": 110.0,
                "pentad_in_year": 1,
                "q05": None,  # stale
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "NE",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 85.0,
            },
            # No EM row — gap
        ]
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        modelled = _make_modelled_df(
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    modelled_pentad=modelled,
                )

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                # All model types present: LR (refreshed), TFT (refreshed),
                # NE (preserved), EM (newly created)
                saved_models = set(saved_df["model_short"].unique())
                assert saved_models >= {"LR", "TFT", "NE", "EM"}, (
                    f"Expected all model types, got {saved_models}"
                )
                # EM was created with correct discharge
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) >= 1
                assert em_rows["forecasted_discharge"].iloc[0] == pytest.approx(105.0)


# ===================================================================
# TestMaintenanceNEGapFillWiring (PP-022)
# ===================================================================
class TestMaintenanceNEGapFillWiring:
    """NE gap-fill with neural_ensemble_func that actually creates NE rows.

    The default mock uses identity (lambda x: x) which never adds NE.
    These tests use a realistic mock that creates NE rows from
    individual-model data, validating the full NE gap-fill path.
    """

    def test_ne_gap_filled_by_neural_ensemble_func(self, env_setup):
        """Missing NE row detected and filled by neural_ensemble_func.

        Combined has LR+TFT+EM but no NE. Maintenance detects NE gap,
        reads modelled data, neural_ensemble_func creates NE, and the
        NE row is saved.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = [
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "pentad_in_year": 1,
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "TFT",
                "forecasted_discharge": 110.0,
                "pentad_in_year": 1,
            },
            {
                "date": "2024-01-05",
                "code": "15001",
                "model_short": "EM",
                "forecasted_discharge": 105.0,
                "pentad_in_year": 1,
                "q05": 85.0,
            },
            # No NE row — gap
        ]
        _make_combined_csv(tmp_path, "pentad", rows_data=combined_rows)

        modelled = _make_modelled_df(
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )

        # Realistic neural_ensemble_func: creates NE as mean of individual models
        def _create_ne(df):
            individual = df[~df["model_short"].isin(["NE", "EM"])]
            if individual.empty:
                return df
            ne_rows = []
            for (date, code), group in individual.groupby(["date", "code"]):
                ne_rows.append(
                    {
                        "date": date,
                        "code": code,
                        "model_short": "NE",
                        "forecasted_discharge": group["forecasted_discharge"].mean(),
                        "pentad_in_year": group["pentad_in_year"].iloc[0],
                    }
                )
            if ne_rows:
                ne_df = pd.DataFrame(ne_rows)
                return pd.concat([df, ne_df], ignore_index=True)
            return df

        with patch.dict(
            os.environ,
            {
                "SAPPHIRE_PREDICTION_MODE": "PENTAD",
                "POSTPROCESSING_GAPFILL_MAX_MONTHS": "13",
            },
        ):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    modelled_pentad=modelled,
                )
                # Replace identity mock with realistic NE creator
                mocks["sl"].calculate_neural_ensemble_forecast.side_effect = _create_ne

                module, spec = _import_maintenance()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance()

                assert exc_info.value.code == 0
                mocks["file_writer"].save_forecast_data.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]

                # NE row should now exist in the output
                ne_rows = saved_df[saved_df["model_short"] == "NE"]
                assert not ne_rows.empty, "NE gap should have been filled by neural_ensemble_func"
                # NE discharge = mean(LR=100, TFT=110) = 105
                assert ne_rows["forecasted_discharge"].iloc[0] == pytest.approx(105.0)


# ===================================================================
# TestNEExclusionIntegration (#32)
# ===================================================================
class TestNEExclusionIntegration:
    """NE passes thresholds but is excluded from EM composition."""

    def test_ne_passing_thresholds_excluded_from_em(self, env_setup):
        """NE with good skill metrics is NOT used in ensemble mean.

        Skill CSV: LR + TFT + NE all pass thresholds.
        Modelled: LR=100, TFT=110, NE=120.
        Expected EM = mean(LR, TFT) = 105 (not 110 = mean with NE).
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad", models=["LR", "TFT", "NE"])

        modelled = _make_modelled_df(
            models=["LR", "TFT", "NE"],
            discharge_values={
                "LR": 100.0,
                "TFT": 110.0,
                "NE": 120.0,
            },
        )
        observed = _make_observed_df()

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                saved_df = mocks["file_writer"].save_forecast_data.call_args[0][1]
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                assert len(em_rows) == 1, f"Expected 1 EM row, got {len(em_rows)}"
                em_discharge = em_rows["forecasted_discharge"].iloc[0]
                # EM = mean(LR=100, TFT=110) = 105, NOT mean(100,110,120)
                assert em_discharge == pytest.approx(105.0), (
                    f"EM should be mean(LR,TFT)=105, got {em_discharge}. "
                    f"NE was not excluded from ensemble."
                )
                # Verify composition does NOT contain NE
                comp = em_rows["composition"].iloc[0]
                assert "NE" not in comp, f"NE should be excluded from composition, got: {comp}"


# ===================================================================
# TestCrossWorkflowRoundtrip (#33)
# ===================================================================
class TestCrossWorkflowRoundtrip:
    """Save skill metrics from recalc → read back via data_reader."""

    def test_recalc_saves_skill_csv_data_reader_reads_it(self, env_setup):
        """Recalculate writes skill CSV, operational reads it back.

        This tests the seam between recalc and operational entry points:
        recalc writes skill_metrics CSV → operational reads it via
        data_reader.read_skill_metrics().
        """
        tmp_path = env_setup
        from src import data_reader

        # Step 1: Build test data with known skill outcomes
        dates = pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
            ]
        )
        obs_vals = [80, 90, 100, 110, 120]
        orows = []
        for date, obs_val in zip(dates, obs_vals, strict=True):
            orows.append(
                {
                    "code": "15001",
                    "date": date,
                    "discharge_avg": float(obs_val),
                    "delta": 5.0,
                    "model_short": "obs",
                }
            )
        observed = pd.DataFrame(orows)

        import tag_library as tl

        frows = []
        for date, obs_val in zip(dates, obs_vals, strict=True):
            pim = str(tl.get_pentad(date + pd.Timedelta(days=1)))
            frows.append(
                {
                    "code": "15001",
                    "date": date,
                    "pentad_in_year": 1,
                    "pentad_in_month": pim,
                    "forecasted_discharge": float(obs_val + 1),
                    "model_short": "LR",
                }
            )
            frows.append(
                {
                    "code": "15001",
                    "date": date,
                    "pentad_in_year": 1,
                    "pentad_in_month": pim,
                    "forecasted_discharge": float(obs_val - 1),
                    "model_short": "TFT",
                }
            )
        modelled = pd.DataFrame(frows)

        # Step 2: Run recalc — capture skill CSV write
        saved_skills = {}

        def capture_save_skill(config, df, year=None):
            """Intercept save_skill_metrics to write CSV."""
            csv_path = os.path.join(
                str(tmp_path),
                "skill_pentad.csv",
            )
            df.to_csv(csv_path, index=False)
            saved_skills["pentad"] = df
            return None

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_recalc_mocks(
                    tmp_path,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )
                mocks["file_writer"].save_skill_metrics.side_effect = capture_save_skill

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()
                assert exc_info.value.code == 0

        # Step 3: Read back via data_reader
        skill_df = data_reader.read_skill_metrics("pentad")
        assert not skill_df.empty, "data_reader should read back the skill CSV written by recalc"

        # Verify key columns match what recalc wrote
        assert set(skill_df["model_short"].unique()) >= {"LR", "TFT"}
        assert "15001" in skill_df["code"].values


# ===================================================================
# TestVaryingDelta (#34)
# ===================================================================
class TestVaryingDelta:
    """Accuracy with per-station varying delta values."""

    def test_accuracy_with_varying_delta(self, env_setup):
        """Recalc computes accuracy correctly when delta varies.

        Station 15001: delta=3.0
        Station 15002: delta=8.0

        LR forecasts are obs+2 for both stations.
        Accuracy = fraction where |forecast - observed| <= delta.

        Station 15001 (delta=3): |+2| <= 3 → all accurate → accuracy=1.0
        Station 15002 (delta=8): |+2| <= 8 → all accurate → accuracy=1.0

        Now make LR forecasts obs+5:
        Station 15001 (delta=3): |+5| > 3 → all inaccurate → accuracy=0.0
        Station 15002 (delta=8): |+5| <= 8 → all accurate → accuracy=1.0
        """
        tmp_path = env_setup

        stations = ["15001", "15002"]
        dates = pd.to_datetime(
            [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
            ]
        )
        obs_values = [80.0, 90.0, 100.0, 110.0, 120.0]
        deltas = {"15001": 3.0, "15002": 8.0}

        # Build observed
        orows = []
        for date, obs_val in zip(dates, obs_values, strict=True):
            for station in stations:
                orows.append(
                    {
                        "code": station,
                        "date": date,
                        "discharge_avg": obs_val,
                        "delta": deltas[station],
                        "model_short": "obs",
                    }
                )
        observed = pd.DataFrame(orows)

        # Build modelled: LR = obs + 5 (exceeds delta=3, within delta=8)
        import tag_library as tl

        frows = []
        for date, obs_val in zip(dates, obs_values, strict=True):
            pim = str(tl.get_pentad(date + pd.Timedelta(days=1)))
            for station in stations:
                frows.append(
                    {
                        "code": station,
                        "date": date,
                        "pentad_in_year": 1,
                        "pentad_in_month": pim,
                        "forecasted_discharge": obs_val + 5.0,
                        "model_short": "LR",
                    }
                )
        modelled = pd.DataFrame(frows)

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_recalc_mocks(
                    tmp_path,
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                saved_skill = mocks["file_writer"].save_skill_metrics.call_args[0][1]

                lr_15001 = saved_skill[
                    (saved_skill["code"] == "15001") & (saved_skill["model_short"] == "LR")
                ]
                lr_15002 = saved_skill[
                    (saved_skill["code"] == "15002") & (saved_skill["model_short"] == "LR")
                ]

                assert len(lr_15001) == 1, f"Expected 1 skill row for 15001/LR, got {len(lr_15001)}"
                assert len(lr_15002) == 1, f"Expected 1 skill row for 15002/LR, got {len(lr_15002)}"

                # 15001: |+5| > delta=3 → accuracy=0.0
                assert lr_15001["accuracy"].iloc[0] == pytest.approx(0.0, abs=0.01), (
                    f"Station 15001 (delta=3): forecast off by 5 should "
                    f"have accuracy=0, got {lr_15001['accuracy'].iloc[0]}"
                )
                # 15002: |+5| <= delta=8 → accuracy=1.0
                assert lr_15002["accuracy"].iloc[0] == pytest.approx(1.0, abs=0.01), (
                    f"Station 15002 (delta=8): forecast off by 5 should "
                    f"have accuracy=1, got {lr_15002['accuracy'].iloc[0]}"
                )


# ===================================================================
# TestLogMostRecentForecasts (#35)
# ===================================================================
class TestLogMostRecentForecasts:
    """log_most_recent_forecasts(config, ...) doesn't crash."""

    def test_pentad_log_no_crash_with_em(self, env_setup):
        """log_most_recent_forecasts on typical pentad data + EM rows."""
        from src import postprocessing_tools as pt

        modelled = _make_modelled_df(
            stations=["15001", "15002"],
            models=["LR", "TFT"],
            dates=[pd.Timestamp("2024-01-05")],
        )
        # Add EM rows
        em_rows = pd.DataFrame(
            [
                {
                    "code": "15001",
                    "date": pd.Timestamp("2024-01-05"),
                    "pentad_in_year": 1,
                    "pentad_in_month": "1",
                    "forecasted_discharge": 105.0,
                    "model_short": "EM",
                }
            ]
        )
        modelled = pd.concat([modelled, em_rows], ignore_index=True)

        result = pt.log_most_recent_forecasts(PENTAD, modelled)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_pentad_log_empty_df(self, env_setup):
        """log_most_recent_forecasts on empty DataFrame."""
        from src import postprocessing_tools as pt

        result = pt.log_most_recent_forecasts(PENTAD, pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_decade_log_no_crash(self, env_setup):
        """log_most_recent_forecasts on typical decad data."""
        from src import postprocessing_tools as pt

        modelled = _make_modelled_df(
            stations=["15001"],
            models=["LR", "TFT"],
            horizon_type="decad",
            dates=[pd.Timestamp("2024-01-10")],
        )

        result = pt.log_most_recent_forecasts(DECAD, modelled)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty


# ===================================================================
# TestCodesPassthrough — verify codes= is forwarded to data_reader
# ===================================================================
class TestCodesPassthrough:
    """Verify station codes are passed through to data_reader calls."""

    def test_operational_pentad_passes_codes_to_reader(self, env_setup):
        """Operational PENTAD: read_observed_and_modelled_data called with codes=['15001'].

        The env_setup fixture writes a station selection file containing
        station '15001'. This test asserts that codes= is extracted from
        that file and forwarded to the data reader call.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        observed = _make_observed_df()
        modelled = _make_modelled_df(discharge_values={"LR": 100.0, "TFT": 110.0})

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "PENTAD",
                    observed_pentad=observed,
                    modelled_pentad=modelled,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # Verify codes= was passed to data reader
                call_kwargs = mocks["data_reader"].read_observed_and_modelled_data.call_args
                assert call_kwargs is not None, "read_observed_and_modelled_data was not called"
                codes_arg = call_kwargs[1].get("codes") if call_kwargs[1] else None
                assert codes_arg == ["15001"], f"Expected codes=['15001'], got {codes_arg!r}"

    def test_operational_both_passes_separate_codes_per_horizon(self, env_setup):
        """BOTH mode: each horizon reads codes from its own station selection file.

        PENTAD uses config_station_selection.json (codes=['15001']).
        DECAD uses config_station_selection_decad.json (codes=['25001']).
        Both are passed as codes= to their respective data reader calls.
        """
        tmp_path = env_setup
        # Override decad station selection to use a different station
        decad_config = {"stationsID": ["25001"]}
        (tmp_path / "config_station_selection_decad.json").write_text(json.dumps(decad_config))
        # Write skill CSVs for both horizons (station 15001 for pentad, 25001 for decad)
        _make_skill_csv(tmp_path, "pentad", stations=["15001"])
        _make_skill_csv(tmp_path, "decad", stations=["25001"])

        observed_pentad = _make_observed_df(stations=["15001"])
        modelled_pentad = _make_modelled_df(
            stations=["15001"],
            horizon_type="pentad",
            discharge_values={"LR": 100.0, "TFT": 110.0},
        )
        observed_decad = _make_observed_df(
            stations=["25001"],
            dates=[pd.Timestamp("2024-01-10")],
        )
        modelled_decad = _make_modelled_df(
            stations=["25001"],
            horizon_type="decad",
            dates=[pd.Timestamp("2024-01-10")],
            discharge_values={"LR": 200.0, "TFT": 220.0},
        )

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "BOTH"}):
            with patch.dict(sys.modules, {}):
                mocks = _setup_real_internal_mocks(
                    tmp_path,
                    "BOTH",
                    observed_pentad=observed_pentad,
                    modelled_pentad=modelled_pentad,
                    observed_decad=observed_decad,
                    modelled_decad=modelled_decad,
                )

                module, spec = _import_operational()
                spec.loader.exec_module(module)
                module.is_pentad_boundary = lambda d: True
                module.is_decad_boundary = lambda d: True

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_operational()

                assert exc_info.value.code == 0

                # data reader should have been called twice (once per horizon)
                calls = mocks["data_reader"].read_observed_and_modelled_data.call_args_list
                assert len(calls) == 2, f"Expected 2 data reader calls, got {len(calls)}"

                codes_by_horizon = {}
                for call in calls:
                    horizon = call[0][0]  # first positional arg is horizon_type
                    codes = call[1].get("codes") if call[1] else None
                    codes_by_horizon[horizon] = codes

                assert codes_by_horizon.get("pentad") == ["15001"], (
                    f"PENTAD codes wrong: {codes_by_horizon.get('pentad')!r}"
                )
                assert codes_by_horizon.get("decad") == ["25001"], (
                    f"DECAD codes wrong: {codes_by_horizon.get('decad')!r}"
                )

    def test_recalc_passes_codes_to_reader(self, env_setup):
        """recalculate_skill_metrics passes station codes to read_observed_and_modelled_data.

        The env_setup fixture writes a station selection file with '15001'.
        The recalc script reads these codes and forwards them as codes= to
        the data reader. This test verifies that wiring is intact.

        Uses fully-mocked skill_metrics and file_writer so the test focuses
        only on the codes= passthrough contract, not skill calculation.
        """
        _ = env_setup  # triggers fixture side-effects (env vars, config files)
        mock_data = _make_modelled_df(discharge_values={"LR": 100.0, "TFT": 110.0})
        mock_skill = pd.DataFrame({"pentad_in_year": [1], "code": ["15001"], "sdivsigma": [0.3]})

        mock_sl = MagicMock()
        mock_skill_metrics = MagicMock()
        mock_file_writer = MagicMock()
        mock_data_reader = MagicMock()
        mock_pt_module = MagicMock()
        mock_pt_module.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
        mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
        mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

        mock_data_reader.read_observed_and_modelled_data.return_value = (mock_data, mock_data)
        mock_skill_metrics.calculate_skill_metrics.return_value = (mock_skill, mock_data, None)
        mock_file_writer.save_forecast_data.return_value = None
        mock_file_writer.save_skill_metrics.return_value = None
        mock_data_reader.read_quarterly_observations.return_value = pd.DataFrame()
        mock_data_reader.read_quarterly_forecasts.return_value = pd.DataFrame()
        mock_data_reader.read_seasonal_observations.return_value = pd.DataFrame()
        mock_data_reader.read_seasonal_forecasts.return_value = pd.DataFrame()
        mock_skill_metrics.calculate_quarterly_skill_metrics.return_value = (
            pd.DataFrame(),
            pd.DataFrame(),
            None,
        )
        mock_skill_metrics.calculate_seasonal_skill_metrics.return_value = (
            pd.DataFrame(),
            pd.DataFrame(),
            None,
        )
        mock_file_writer.save_quarterly_forecast_data.return_value = None
        mock_file_writer.save_quarterly_skill_metrics.return_value = True
        mock_file_writer.save_seasonal_forecast_data.return_value = None
        mock_file_writer.save_seasonal_skill_metrics.return_value = True

        import tag_library as real_tl

        mock_src = MagicMock()
        mock_src.skill_metrics = mock_skill_metrics
        mock_src.file_writer = mock_file_writer
        mock_src.data_reader = mock_data_reader
        mock_src.postprocessing_tools = mock_pt_module

        with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "PENTAD"}):
            with patch.dict(
                sys.modules,
                {
                    "setup_library": mock_sl,
                    "tag_library": real_tl,
                    "src": mock_src,
                    "src.skill_metrics": mock_skill_metrics,
                    "src.file_writer": mock_file_writer,
                    "src.data_reader": mock_data_reader,
                    "src.postprocessing_tools": mock_pt_module,
                    "src.horizon_config": _real_horizon_config,
                },
            ):
                module, spec = _import_recalc()
                spec.loader.exec_module(module)

                with pytest.raises(SystemExit) as exc_info:
                    module.recalculate_skill_metrics()

                assert exc_info.value.code == 0

                # Verify codes= was passed to data reader — the station selection
                # file (written by env_setup) contains ["15001"]
                call_kwargs = mock_data_reader.read_observed_and_modelled_data.call_args
                assert call_kwargs is not None, "read_observed_and_modelled_data was not called"
                codes_arg = call_kwargs[1].get("codes") if call_kwargs[1] else None
                assert codes_arg == ["15001"], f"Expected codes=['15001'], got {codes_arg!r}"
