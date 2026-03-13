"""Tests for postprocessing_operational_long_term.py entry point.

Tests the monthly (long-term) ensemble forecast pipeline:
    load env -> read station codes -> read skill metrics ->
    read latest monthly forecasts -> create ensembles -> save -> log.
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _import_long_term_module():
    """Import postprocessing_operational_long_term via importlib.

    Uses importlib to avoid top-level side effects (logging file
    creation, sys.path manipulation) leaking across test sessions.
    """
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_long_term_module",
        os.path.join(SCRIPT_DIR, "postprocessing_operational_long_term.py"),
    )
    module = importlib.util.module_from_spec(spec)
    return module, spec


def _make_skill_stats():
    """Create a realistic monthly skill-metrics DataFrame."""
    return pd.DataFrame(
        {
            "month_in_year": [1, 1, 2, 2],
            "code": ["10001", "10002", "10001", "10002"],
            "model_short": ["LR", "LR", "LR", "LR"],
            "sdivsigma": [0.3, 0.4, 0.25, 0.35],
            "nse": [0.8, 0.7, 0.85, 0.75],
            "delta": [0.1, 0.15, 0.08, 0.12],
            "accuracy": [0.9, 0.85, 0.92, 0.88],
            "mae": [5.0, 6.0, 4.5, 5.5],
            "n_pairs": [10, 10, 10, 10],
        }
    )


def _make_forecasts():
    """Create a realistic monthly forecasts DataFrame."""
    return pd.DataFrame(
        {
            "code": ["10001", "10002"],
            "year": [2025, 2025],
            "month": [1, 1],
            "month_in_year": [1, 1],
            "model_short": ["LR", "LR"],
            "forecasted_discharge": [100.0, 200.0],
            "q05": [80.0, 160.0],
            "q25": [90.0, 180.0],
            "q50": [100.0, 200.0],
            "q75": [110.0, 220.0],
            "q95": [120.0, 240.0],
            "valid_from": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "valid_to": pd.to_datetime(["2025-01-31", "2025-01-31"]),
            "date": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "flag": [0, 0],
        }
    )


def _make_joint():
    """Create a joint DataFrame (forecasts + ensemble rows)."""
    forecasts = _make_forecasts()
    ensemble_rows = pd.DataFrame(
        {
            "code": ["10001", "10002"],
            "year": [2025, 2025],
            "month": [1, 1],
            "month_in_year": [1, 1],
            "model_short": ["EM", "EM"],
            "forecasted_discharge": [100.0, 200.0],
            "q05": [80.0, 160.0],
            "q25": [90.0, 180.0],
            "q50": [100.0, 200.0],
            "q75": [110.0, 220.0],
            "q95": [120.0, 240.0],
            "valid_from": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "valid_to": pd.to_datetime(["2025-01-31", "2025-01-31"]),
            "date": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "flag": [0, 0],
            "composition": ["LR", "LR"],
        }
    )
    return pd.concat([forecasts, ensemble_rows], ignore_index=True)


def _setup_mocks(
    skill_stats=None,
    forecasts=None,
    joint=None,
    save_return=None,
    existing_combined=None,
):
    """Wire up sys.modules mocks for the long-term entry point.

    Args:
        skill_stats: DataFrame for read_skill_metrics (default: valid).
        forecasts: DataFrame for read_latest_monthly_forecasts (default: valid).
        joint: DataFrame returned by create_monthly_ensemble_forecasts.
        save_return: Return value from save_monthly_forecast_data
            (None = success, string = error message).
        existing_combined: DataFrame for read_monthly_combined_forecasts
            (default: empty DataFrame).

    Returns:
        Dict of mock objects keyed by short name.
    """
    if skill_stats is None:
        skill_stats = _make_skill_stats()
    if forecasts is None:
        forecasts = _make_forecasts()
    if joint is None:
        joint = _make_joint()
    if existing_combined is None:
        existing_combined = pd.DataFrame()

    mock_sl = MagicMock()
    mock_sl.load_environment.return_value = None

    mock_data_reader = MagicMock()
    mock_data_reader.read_skill_metrics.return_value = skill_stats
    mock_data_reader.read_latest_monthly_forecasts.return_value = forecasts
    mock_data_reader.read_monthly_combined_forecasts.return_value = existing_combined

    mock_ensemble_calc = MagicMock()
    mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = joint

    mock_file_writer = MagicMock()
    mock_file_writer.save_monthly_forecast_data.return_value = save_return

    mock_pt = MagicMock()
    mock_pt.TimingStats.return_value.summary.return_value = ([], 0)
    mock_pt.log_most_recent_forecasts_monthly.return_value = None
    # timer must be a real context manager for the 'with' blocks
    mock_pt.timer = MagicMock(
        side_effect=lambda ts, name: MagicMock(
            __enter__=MagicMock(return_value=None),
            __exit__=MagicMock(return_value=False),
        )
    )

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.file_writer = mock_file_writer

    sys.modules["setup_library"] = mock_sl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.file_writer"] = mock_file_writer

    return {
        "sl": mock_sl,
        "pt": mock_pt,
        "data_reader": mock_data_reader,
        "ensemble_calc": mock_ensemble_calc,
        "file_writer": mock_file_writer,
    }


def _run_entry_point(mocks, station_codes=None):
    """Import, patch _read_station_codes, and execute the entry point.

    Args:
        mocks: Dict from _setup_mocks().
        station_codes: List of station code strings to return from
            _read_station_codes (default: ['10001', '10002']).

    Returns:
        (module, exit_info) where exit_info is the SystemExit exception.
    """
    if station_codes is None:
        station_codes = ["10001", "10002"]

    module, spec = _import_long_term_module()
    spec.loader.exec_module(module)

    # Replace _read_station_codes after module load so it doesn't
    # try to open real config files.
    module._read_station_codes = MagicMock(return_value=station_codes)

    with pytest.raises(SystemExit) as exc_info:
        module.postprocessing_operational_long_term()

    return module, exc_info


class TestOperationalLongTerm:
    """Tests for the monthly operational long-term entry point."""

    def test_happy_path(self):
        """Valid data flows through all stages; save is called, exit 0."""
        with _clean_sys_modules():
            mocks = _setup_mocks()
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            # Verify the full pipeline was executed
            mocks["sl"].load_environment.assert_called_once()
            mocks["data_reader"].read_skill_metrics.assert_any_call(
                "month", codes=["10001", "10002"]
            )
            call_args = mocks["data_reader"].read_latest_monthly_forecasts.call_args
            assert call_args[0][0] == ["10001", "10002"]
            assert "forecast_date" in call_args[1]
            mocks["ensemble_calc"].create_monthly_ensemble_forecasts.assert_called_once()
            mocks["file_writer"].save_monthly_forecast_data.assert_called_once()
            mocks["pt"].log_most_recent_forecasts_monthly.assert_called_once()

    def test_empty_skill_metrics_exits_zero(self):
        """Empty skill metrics warns and exits 0, skipping later stages."""
        with _clean_sys_modules():
            mocks = _setup_mocks(skill_stats=pd.DataFrame())
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            # Skill metrics read for month (and quarter/season)
            mocks["data_reader"].read_skill_metrics.assert_any_call(
                "month", codes=["10001", "10002"]
            )
            mocks["data_reader"].read_latest_monthly_forecasts.assert_not_called()
            mocks["ensemble_calc"].create_monthly_ensemble_forecasts.assert_not_called()
            mocks["file_writer"].save_monthly_forecast_data.assert_not_called()

    def test_empty_forecasts_exits_zero(self):
        """Empty forecasts (but valid skill) warns and exits 0."""
        with _clean_sys_modules():
            mocks = _setup_mocks(forecasts=pd.DataFrame())
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            # Skill read, forecasts read, but ensemble not created
            mocks["data_reader"].read_skill_metrics.assert_called_once()
            mocks["data_reader"].read_latest_monthly_forecasts.assert_called_once()
            mocks["ensemble_calc"].create_monthly_ensemble_forecasts.assert_not_called()
            mocks["file_writer"].save_monthly_forecast_data.assert_not_called()

    def test_ensemble_calculator_called_with_correct_args(self):
        """Verify create_monthly_ensemble_forecasts receives the right data."""
        skill_stats = _make_skill_stats()
        forecasts = _make_forecasts()
        joint = _make_joint()

        with _clean_sys_modules():
            mocks = _setup_mocks(
                skill_stats=skill_stats,
                forecasts=forecasts,
                joint=joint,
            )
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            call_args = mocks["ensemble_calc"].create_monthly_ensemble_forecasts.call_args
            passed_forecasts = call_args[0][0]
            passed_skill = call_args[0][1]

            pd.testing.assert_frame_equal(passed_forecasts, forecasts)
            pd.testing.assert_frame_equal(passed_skill, skill_stats)

    def test_save_error_causes_exit_one(self):
        """When save_monthly_forecast_data returns an error string, exit 1."""
        with _clean_sys_modules():
            mocks = _setup_mocks(save_return="Error: disk full")
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 1

            # Save was still called
            mocks["file_writer"].save_monthly_forecast_data.assert_called_once()
            # Logging of forecasts still happens (after save, before exit)
            mocks["pt"].log_most_recent_forecasts_monthly.assert_called_once()

    def test_station_codes_passed_to_forecast_reader(self):
        """Station codes and forecast_date flow to the reader."""
        custom_codes = ["50001", "50002", "50003"]

        with _clean_sys_modules():
            mocks = _setup_mocks()
            module, exc_info = _run_entry_point(mocks, station_codes=custom_codes)

            assert exc_info.value.code == 0
            call_args = mocks["data_reader"].read_latest_monthly_forecasts.call_args
            assert call_args[0][0] == custom_codes
            assert "forecast_date" in call_args[1], "forecast_date kwarg must be passed"

    def test_joint_passed_to_save_and_log(self):
        """The joint DataFrame from ensemble_calculator flows to save + log."""
        joint = _make_joint()

        with _clean_sys_modules():
            mocks = _setup_mocks(joint=joint)
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            # Verify save received the joint DataFrame
            save_args = mocks["file_writer"].save_monthly_forecast_data.call_args
            pd.testing.assert_frame_equal(save_args[0][0], joint)

            # Verify log received the joint DataFrame
            log_args = mocks["pt"].log_most_recent_forecasts_monthly.call_args
            pd.testing.assert_frame_equal(log_args[0][0], joint)

    def test_merges_with_existing_combined_csv(self):
        """New month's data is merged with existing historical data."""
        # Existing data for December 2024
        existing = pd.DataFrame(
            {
                "code": ["10001", "10002"],
                "year": [2024, 2024],
                "month": [12, 12],
                "month_in_year": [12, 12],
                "model_short": ["EM", "EM"],
                "forecasted_discharge": [50.0, 60.0],
            }
        )
        # New data for January 2025 (from _make_joint)
        joint = _make_joint()

        with _clean_sys_modules():
            mocks = _setup_mocks(
                joint=joint,
                existing_combined=existing,
            )
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            saved_df = mocks["file_writer"].save_monthly_forecast_data.call_args[0][0]
            # Should contain both December (existing) and January (new)
            months = set(saved_df["month"].unique())
            assert 12 in months, "Existing December data must be preserved"
            assert 1 in months, "New January data must be present"
            assert len(saved_df) == len(existing) + len(joint)

    def test_dedup_keeps_latest(self):
        """Overlapping keys: new data wins over stale existing data."""
        # Existing has an EM row for 10001/Jan with discharge=50
        existing = pd.DataFrame(
            {
                "code": ["10001"],
                "year": [2025],
                "month": [1],
                "month_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [50.0],
            }
        )
        # Joint from _make_joint has EM for 10001/Jan with discharge=100
        joint = _make_joint()

        with _clean_sys_modules():
            mocks = _setup_mocks(
                joint=joint,
                existing_combined=existing,
            )
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            saved_df = mocks["file_writer"].save_monthly_forecast_data.call_args[0][0]
            em_10001 = saved_df[(saved_df["model_short"] == "EM") & (saved_df["code"] == "10001")]
            assert len(em_10001) == 1, "Dedup should keep exactly one EM row per key"
            assert em_10001.iloc[0]["forecasted_discharge"] == 100.0, (
                "New value (100.0) should overwrite old (50.0)"
            )

    def test_empty_existing_saves_new_only(self):
        """Empty existing combined: save receives only new data."""
        joint = _make_joint()

        with _clean_sys_modules():
            mocks = _setup_mocks(
                joint=joint,
                existing_combined=pd.DataFrame(),
            )
            module, exc_info = _run_entry_point(mocks)

            assert exc_info.value.code == 0

            saved_df = mocks["file_writer"].save_monthly_forecast_data.call_args[0][0]
            pd.testing.assert_frame_equal(saved_df, joint)


class TestOperationalLongTermEdgeCases:
    """Edge case tests for the long-term operational entry point."""

    def test_load_environment_failure_propagates(self):
        """When load_environment() raises, exception propagates uncaught."""
        with _clean_sys_modules():
            mocks = _setup_mocks()
            mocks["sl"].load_environment.side_effect = FileNotFoundError("missing .env")

            module, spec = _import_long_term_module()
            spec.loader.exec_module(module)
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(FileNotFoundError, match="missing .env"):
                module.postprocessing_operational_long_term()

    def test_read_station_codes_failure_propagates(self):
        """When _read_station_codes raises, exception propagates."""
        with _clean_sys_modules():
            _setup_mocks()

            module, spec = _import_long_term_module()
            spec.loader.exec_module(module)
            module._read_station_codes = MagicMock(
                side_effect=ValueError("no station config found")
            )

            with pytest.raises(ValueError, match="no station config"):
                module.postprocessing_operational_long_term()


# -- helpers ----------------------------------------------------------

import contextlib


@contextlib.contextmanager
def _clean_sys_modules():
    """Temporarily clear sys.modules entries injected by _setup_mocks.

    Ensures each test starts with a clean module namespace and restores
    the original state on exit.
    """
    keys_to_clean = [
        "setup_library",
        "src",
        "src.postprocessing_tools",
        "src.data_reader",
        "src.ensemble_calculator",
        "src.file_writer",
        "postprocessing_operational_long_term_module",
    ]
    saved = {k: sys.modules.pop(k, None) for k in keys_to_clean}
    try:
        yield
    finally:
        # Remove any mocks injected during the test
        for k in keys_to_clean:
            sys.modules.pop(k, None)
        # Restore originals if they existed
        for k, v in saved.items():
            if v is not None:
                sys.modules[k] = v
