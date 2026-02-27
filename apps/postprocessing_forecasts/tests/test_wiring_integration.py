"""Wiring integration tests for postprocessing entry points.

These tests call the actual entry point functions but let the *internal*
modules (data_reader, ensemble_calculator, gap_detector) run with real
logic against CSV files written to tmp_path.  Only external boundaries
(setup_library, forecast_library save/load, tag_library) are mocked.

This catches bugs in how the entry-point scripts wire internal modules
together - a class of defect that all-mock workflow tests miss.
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.dirname(__file__))

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
        (observed_pentad, modelled_pentad)
        if observed_pentad is not None
        else (pd.DataFrame(), pd.DataFrame())
    )
    _decad_data = (
        (observed_decad, modelled_decad)
        if observed_decad is not None
        else (pd.DataFrame(), pd.DataFrame())
    )

    def _mock_read_observed_and_modelled(horizon_type, **kwargs):
        if horizon_type == "pentad":
            return _pentad_data
        return _decad_data

    real_data_reader.read_observed_and_modelled_data = MagicMock(
        side_effect=_mock_read_observed_and_modelled
    )

    mock_file_writer.save_forecast_data_pentad.return_value = None
    mock_file_writer.save_forecast_data_decade.return_value = None

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

    sys.modules["src"] = real_src
    sys.modules["src.postprocessing_tools"] = real_pt
    sys.modules["src.data_reader"] = real_data_reader
    sys.modules["src.ensemble_calculator"] = real_ensemble_calc
    sys.modules["src.gap_detector"] = real_gap_detector
    sys.modules["src.skill_metrics"] = real_skill_metrics
    sys.modules["src.file_writer"] = mock_file_writer

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
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]

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
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
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

                # Both save functions called
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                mocks["file_writer"].save_forecast_data_decade.assert_called_once()

                # Pentad EM
                pentad_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
                pentad_em = pentad_df[pentad_df["model_short"] == "EM"]
                assert len(pentad_em) == 1
                assert pentad_em["forecasted_discharge"].iloc[0] == (pytest.approx(105.0))

                # Decad EM
                decad_df = mocks["file_writer"].save_forecast_data_decade.call_args[0][0]
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
                mocks["file_writer"].save_forecast_data_pentad.return_value = "Error: disk full"

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
                "POSTPROCESSING_GAPFILL_WINDOW_DAYS": "7",
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

                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
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
                # No gaps => data_reader.read_observed_and_modelled_data
                # should NOT have been called
                mocks["data_reader"].read_observed_and_modelled_data.assert_not_called()

    def test_gap_dates_no_matching_modelled_data(self, env_setup):
        """Gap detected but modelled data has no matching rows => no save."""
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        # Gap at Jan 10 (no EM)
        _make_combined_csv(
            tmp_path,
            "pentad",
            rows_data=[
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
                # Data was read but no rows match gap dates
                mocks["data_reader"].read_observed_and_modelled_data.assert_called_once()
                # Early return — no save
                mocks["file_writer"].save_forecast_data_pentad.assert_not_called()

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

                # Both saved
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                mocks["file_writer"].save_forecast_data_decade.assert_called_once()

                # Pentad EM
                pentad_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
                pentad_em = pentad_df[pentad_df["model_short"] == "EM"]
                assert len(pentad_em) == 1
                assert pentad_em["forecasted_discharge"].iloc[0] == (pytest.approx(105.0))

                # Decad EM
                decad_df = mocks["file_writer"].save_forecast_data_decade.call_args[0][0]
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
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()

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
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
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
    """Set up sys.modules for recalc with real calculate_skill_metrics_*.

    Real: src.skill_metrics (calculate_skill_metrics_pentad/decade,
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
        (observed_pentad, modelled_pentad)
        if observed_pentad is not None
        else (pd.DataFrame(), pd.DataFrame())
    )
    _decad_data = (
        (observed_decad, modelled_decad)
        if observed_decad is not None
        else (pd.DataFrame(), pd.DataFrame())
    )

    def _mock_read_observed_and_modelled(horizon_type, **kwargs):
        if horizon_type == "pentad":
            return _pentad_data
        return _decad_data

    mock_data_reader.read_observed_and_modelled_data = MagicMock(
        side_effect=_mock_read_observed_and_modelled
    )

    mock_file_writer.save_forecast_data_pentad.return_value = None
    mock_file_writer.save_forecast_data_decade.return_value = None
    mock_file_writer.save_pentadal_skill_metrics.return_value = None
    mock_file_writer.save_decadal_skill_metrics.return_value = None

    sys.modules["setup_library"] = mock_sl
    sys.modules["tag_library"] = MagicMock()

    real_src = MagicMock()
    real_src.postprocessing_tools = real_pt
    real_src.skill_metrics = real_skill_metrics
    real_src.data_reader = mock_data_reader
    real_src.ensemble_calculator = real_ensemble_calc
    real_src.file_writer = mock_file_writer
    sys.modules["src"] = real_src
    sys.modules["src.postprocessing_tools"] = real_pt
    sys.modules["src.skill_metrics"] = real_skill_metrics
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = real_ensemble_calc
    sys.modules["src.file_writer"] = mock_file_writer

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
    """Entry point calls real calculate_skill_metrics_pentad.

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
        """Real calculate_skill_metrics_pentad runs, saves both outputs.

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
                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                mocks["file_writer"].save_pentadal_skill_metrics.assert_called_once()

                # Verify skill metrics have correct structure
                saved_skill = mocks["file_writer"].save_pentadal_skill_metrics.call_args[0][0]
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

    def test_pentad_em_in_saved_forecasts(self, env_setup):
        """Real recalc produces EM rows in joint forecasts.

        Both LR and TFT are close to observed (bias ~2), so both should
        pass thresholds, and EM = mean(LR, TFT) should be created.
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

                saved_fc = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
                em_rows = saved_fc[saved_fc["model_short"] == "EM"]
                assert len(em_rows) == 5, f"Expected 5 EM rows (5 dates), got {len(em_rows)}"

                # EM = mean(LR=obs+1, TFT=obs-1) = obs
                obs_values = [80.0, 90.0, 100.0, 110.0, 120.0]
                em_sorted = em_rows.sort_values("date")
                for em_val, obs_val in zip(
                    em_sorted["forecasted_discharge"], obs_values, strict=True
                ):
                    assert abs(em_val - obs_val) < 0.01, (
                        f"EM discharge should be {obs_val}, got {em_val}"
                    )

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
                mocks[
                    "file_writer"
                ].save_pentadal_skill_metrics.return_value = "Error: write failed"

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

        Only the 1 gap date should get an EM row; the 2 existing EM rows
        should NOT be duplicated.
        """
        tmp_path = env_setup
        _make_skill_csv(tmp_path, "pentad")

        combined_rows = []
        # Date 1 (Jan 5): LR + TFT + EM — already complete
        for ms in ["LR", "TFT", "EM"]:
            combined_rows.append(
                {
                    "date": "2024-01-05",
                    "code": "15001",
                    "model_short": ms,
                    "forecasted_discharge": 100.0,
                    "pentad_in_year": 1,
                }
            )
        # Date 2 (Jan 10): LR + TFT + EM — already complete
        for ms in ["LR", "TFT", "EM"]:
            combined_rows.append(
                {
                    "date": "2024-01-10",
                    "code": "15001",
                    "model_short": ms,
                    "forecasted_discharge": 200.0,
                    "pentad_in_year": 2,
                }
            )
        # Date 3 (Jan 15): LR + TFT but NO EM — gap
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
                "POSTPROCESSING_GAPFILL_WINDOW_DAYS": "30",
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

                mocks["file_writer"].save_forecast_data_pentad.assert_called_once()
                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
                em_rows = saved_df[saved_df["model_short"] == "EM"]
                # Merged output: 2 existing EM (Jan 5, Jan 10) + 1 new (Jan 15)
                assert len(em_rows) == 3, (
                    f"Expected 3 EM rows (2 existing + 1 gap-fill), got {len(em_rows)}"
                )
                em_dates = sorted(pd.to_datetime(em_rows["date"]).dt.strftime("%Y-%m-%d"))
                assert "2024-01-15" in em_dates, f"Gap-fill EM for Jan 15 missing from {em_dates}"
                # Existing EM rows preserved
                assert "2024-01-05" in em_dates
                assert "2024-01-10" in em_dates
                # Total rows should be combined(8) + new EM(1) = 9
                assert len(saved_df) == 9, f"Expected 9 merged rows, got {len(saved_df)}"


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

                saved_df = mocks["file_writer"].save_forecast_data_pentad.call_args[0][0]
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

        def capture_save_skill(df, year=None):
            """Intercept save_pentadal_skill_metrics to write CSV."""
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
                mocks["file_writer"].save_pentadal_skill_metrics.side_effect = capture_save_skill

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

                saved_skill = mocks["file_writer"].save_pentadal_skill_metrics.call_args[0][0]

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
    """log_most_recent_forecasts_pentad/decade doesn't crash."""

    def test_pentad_log_no_crash_with_em(self, env_setup):
        """log_most_recent_forecasts_pentad on typical data + EM rows."""
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

        result = pt.log_most_recent_forecasts_pentad(modelled)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_pentad_log_empty_df(self, env_setup):
        """log_most_recent_forecasts_pentad on empty DataFrame."""
        from src import postprocessing_tools as pt

        result = pt.log_most_recent_forecasts_pentad(pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_decade_log_no_crash(self, env_setup):
        """log_most_recent_forecasts_decade on typical decad data."""
        from src import postprocessing_tools as pt

        modelled = _make_modelled_df(
            stations=["15001"],
            models=["LR", "TFT"],
            horizon_type="decad",
            dates=[pd.Timestamp("2024-01-10")],
        )

        result = pt.log_most_recent_forecasts_decade(modelled)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
