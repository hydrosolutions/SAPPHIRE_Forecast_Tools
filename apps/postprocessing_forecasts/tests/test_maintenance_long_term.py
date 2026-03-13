"""Tests for postprocessing_maintenance_long_term.py entry point.

Exercises the monthly gap-fill pipeline:
  read combined -> detect gaps -> read skill & forecasts ->
  create ensembles -> merge/dedup -> save
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)


# -- helpers ---------------------------------------------------------


def _make_combined(rows):
    """Build a combined-forecasts DataFrame from a list of dicts."""
    return pd.DataFrame(rows)


def _make_gaps(tuples):
    """Build a gaps DataFrame from (year, month, code, model_short) tuples.

    For backward compatibility, also accepts (year, month, code) tuples
    and defaults model_short to 'EM'.
    """
    if not tuples:
        return pd.DataFrame(columns=["year", "month", "code", "model_short"])
    if len(tuples[0]) == 3:
        tuples = [(y, m, c, "EM") for y, m, c in tuples]
    return pd.DataFrame(
        tuples,
        columns=["year", "month", "code", "model_short"],
    )


def _make_forecasts(rows):
    """Build a forecasts DataFrame with the columns the entry point expects."""
    df = pd.DataFrame(rows)
    if "month_in_year" not in df.columns and "month" in df.columns:
        df["month_in_year"] = df["month"]
    if "forecasted_discharge" not in df.columns and "q50" in df.columns:
        df["forecasted_discharge"] = df["q50"].astype(float)
    return df


def _make_skill():
    """Minimal monthly skill-metrics DataFrame."""
    return pd.DataFrame(
        {
            "month_in_year": [1, 1],
            "code": ["10001", "10002"],
            "model_short": ["LR", "LR"],
            "sdivsigma": [0.3, 0.4],
            "nse": [0.8, 0.7],
        }
    )


def _import_module(mocks_dict):
    """Set up sys.modules mocks and import the entry-point module.

    Uses importlib to re-execute the module source against mocked
    dependencies so each test gets a clean module namespace.
    """
    # Build mock objects for every import the module performs
    mock_sl = mocks_dict.get("sl", MagicMock())
    mock_pt = mocks_dict.get("pt", MagicMock())
    mock_data_reader = mocks_dict.get("data_reader", MagicMock())
    mock_ensemble_calc = mocks_dict.get("ensemble_calc", MagicMock())
    mock_gap_detector = mocks_dict.get("gap_detector", MagicMock())
    mock_file_writer = mocks_dict.get("file_writer", MagicMock())

    # TimingStats and timer must be usable at module level
    mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.gap_detector = mock_gap_detector
    mock_src.file_writer = mock_file_writer

    sys.modules["setup_library"] = mock_sl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.gap_detector"] = mock_gap_detector
    sys.modules["src.file_writer"] = mock_file_writer

    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_long_term_module",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance_long_term.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# -- fixtures --------------------------------------------------------


@pytest.fixture
def combined_with_models():
    """Combined forecasts with two base models but no EM."""
    return _make_combined(
        [
            {
                "year": 2025,
                "month": 1,
                "code": "10001",
                "model_short": "LR",
                "forecasted_discharge": 100.0,
                "month_in_year": 1,
            },
            {
                "year": 2025,
                "month": 1,
                "code": "10001",
                "model_short": "TFT",
                "forecasted_discharge": 110.0,
                "month_in_year": 1,
            },
        ]
    )


@pytest.fixture
def gap_tuples():
    """Gap tuples indicating year=2025, month=1, code=10001 needs EM."""
    return _make_gaps([(2025, 1, "10001")])


@pytest.fixture
def forecasts_for_gaps():
    """Forecast rows covering the gap period."""
    return _make_forecasts(
        [
            {
                "year": 2025,
                "month": 1,
                "code": "10001",
                "model_short": "LR",
                "q50": 100.0,
                "q05": 80.0,
                "q95": 120.0,
                "valid_from": "2025-01-01",
                "valid_to": "2025-01-31",
                "date": "2025-01-01",
                "flag": 0,
            },
            {
                "year": 2025,
                "month": 1,
                "code": "10001",
                "model_short": "TFT",
                "q50": 110.0,
                "q05": 85.0,
                "q95": 130.0,
                "valid_from": "2025-01-01",
                "valid_to": "2025-01-31",
                "date": "2025-01-01",
                "flag": 0,
            },
        ]
    )


@pytest.fixture
def ensemble_result(forecasts_for_gaps):
    """Ensemble calculator output: original rows + EM row."""
    em_row = pd.DataFrame(
        [
            {
                "year": 2025,
                "month": 1,
                "code": "10001",
                "model_short": "EM",
                "forecasted_discharge": 105.0,
                "month_in_year": 1,
                "q50": 105.0,
                "q05": 82.5,
                "q95": 125.0,
                "valid_from": "2025-01-01",
                "valid_to": "2025-01-31",
                "date": "2025-01-01",
                "flag": 0,
            }
        ]
    )
    return pd.concat(
        [forecasts_for_gaps, em_row],
        ignore_index=True,
    )


@pytest.fixture
def skill_stats():
    return _make_skill()


# -- test class ------------------------------------------------------


class TestMaintenanceLongTerm:
    """Tests for postprocessing_maintenance_long_term() entry point."""

    def test_no_combined_forecasts_exits_zero(self):
        """Empty combined forecasts -> logs info, sys.exit(0)."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                }
            )
            # Bypass _read_station_codes (needs real config file)
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_data_reader.read_monthly_combined_forecasts.assert_called_once()
            # Gap detection should never be reached
            mock_gap_detector.detect_missing_monthly_ensembles.assert_not_called()

    def test_no_gaps_found_exits_zero(self, combined_with_models):
        """Gap detector returns empty -> sys.exit(0), no ensemble work."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = pd.DataFrame(
            columns=["year", "month", "code", "model_short"]
        )

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            call_args = mock_gap_detector.detect_missing_monthly_ensembles.call_args
            pd.testing.assert_frame_equal(
                call_args[0][0],
                combined_with_models,
            )
            assert call_args[0][1] == 3  # default lookback
            assert call_args[1]["ensemble_models"] == {
                "EM",
                "Skilled Mean",
                "Naive Mean",
            }
            # No skill read, no ensemble creation
            mock_data_reader.read_skill_metrics.assert_not_called()
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_not_called()

    def test_gaps_found_creates_and_saves_ensembles(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
    ):
        """Full pipeline: gaps -> ensembles -> merge -> save -> exit 0."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_file_writer.save_monthly_forecast_data.return_value = None

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            # Verify the full call chain
            mock_data_reader.read_monthly_combined_forecasts.assert_called_once()
            mock_gap_detector.detect_missing_monthly_ensembles.assert_called_once()
            mock_data_reader.read_skill_metrics.assert_called_once_with(
                "month", codes=["10001", "10002"]
            )
            mock_data_reader.read_monthly_forecasts.assert_called_once_with(
                ["10001", "10002"],
                2025,
                2025,
            )
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_called_once()
            # save_monthly_forecast_data receives a merged DataFrame
            mock_file_writer.save_monthly_forecast_data.assert_called_once()
            saved_df = mock_file_writer.save_monthly_forecast_data.call_args[0][0]
            # Merged result should contain original rows + EM row
            assert len(saved_df) >= 3, (
                f"Expected at least 3 rows (2 base + 1 EM), got {len(saved_df)}"
            )
            em_rows = saved_df[saved_df["model_short"] == "EM"]
            assert len(em_rows) == 1
            assert em_rows.iloc[0]["forecasted_discharge"] == 105.0

    def test_deduplication_works(
        self,
        gap_tuples,
        forecasts_for_gaps,
        skill_stats,
    ):
        """Existing EM rows are replaced (not duplicated) during merge."""
        # Combined already has an old EM row with discharge=99.0
        combined = _make_combined(
            [
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "month_in_year": 1,
                },
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "EM",
                    "forecasted_discharge": 99.0,
                    "month_in_year": 1,
                },
            ]
        )

        # Ensemble calculator returns a new EM row with discharge=105.0
        new_em = pd.DataFrame(
            [
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "EM",
                    "forecasted_discharge": 105.0,
                    "month_in_year": 1,
                    "q50": 105.0,
                    "q05": 82.5,
                    "q95": 125.0,
                    "valid_from": "2025-01-01",
                    "valid_to": "2025-01-31",
                    "date": "2025-01-01",
                    "flag": 0,
                }
            ]
        )
        ensemble_output = pd.concat(
            [forecasts_for_gaps, new_em],
            ignore_index=True,
        )

        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_output
        mock_file_writer.save_monthly_forecast_data.return_value = None

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            saved_df = mock_file_writer.save_monthly_forecast_data.call_args[0][0]
            em_rows = saved_df[saved_df["model_short"] == "EM"]
            # Dedup keeps='last' so the new EM row (105.0) wins
            assert len(em_rows) == 1, f"Expected exactly 1 EM row after dedup, got {len(em_rows)}"
            assert em_rows.iloc[0]["forecasted_discharge"] == 105.0, (
                "New EM value should overwrite old one"
            )

    def test_audit_trail_logged(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
    ):
        """Gap-fill details are logged as AUDIT entries."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_file_writer.save_monthly_forecast_data.return_value = None

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            # Replace the module's logger with a MagicMock that
            # wraps the real logger so we can inspect .info() calls.
            mock_logger = MagicMock(wraps=module.logger)
            module.logger = mock_logger

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            # Collect all logged messages from logger.info calls
            info_calls = mock_logger.info.call_args_list
            messages = []
            for call in info_calls:
                # logger.info(format_str, *args) — resolve the format
                fmt = call[0][0] if call[0] else ""
                args = call[0][1:] if len(call[0]) > 1 else ()
                try:
                    messages.append(fmt % args if args else fmt)
                except TypeError:
                    messages.append(str(fmt))

            # Check AUDIT summary line
            audit_msgs = [m for m in messages if "AUDIT" in m]
            assert len(audit_msgs) >= 1, "Expected at least one AUDIT log entry"
            assert "monthly ensemble gaps" in audit_msgs[0]
            assert "lookback=3" in audit_msgs[0]

            # Check per-gap detail lines
            detail_msgs = [m for m in messages if "Filled: year=2025, month=1, code=10001" in m]
            assert len(detail_msgs) == 1, "Expected one detail line per gap tuple"

    def test_lookback_env_var_respected(self, combined_with_models):
        """POSTPROCESSING_GAPFILL_WINDOW_MONTHS controls lookback."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = pd.DataFrame(
            columns=["year", "month", "code", "model_short"]
        )

        with patch.dict(os.environ, {"POSTPROCESSING_GAPFILL_WINDOW_MONTHS": "6"}):
            with patch.dict(sys.modules, {}):
                module = _import_module(
                    {
                        "sl": mock_sl,
                        "data_reader": mock_data_reader,
                        "gap_detector": mock_gap_detector,
                    }
                )
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance_long_term()

                assert exc_info.value.code == 0
                call_args = mock_gap_detector.detect_missing_monthly_ensembles.call_args
                # Second positional arg is lookback
                assert call_args[0][1] == 6, f"Expected lookback=6, got {call_args[0][1]}"

    def test_empty_skill_metrics_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
    ):
        """No skill metrics available -> exits 0, no ensembles created."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_data_reader.read_skill_metrics.assert_called_once_with(
                "month", codes=["10001", "10002"]
            )
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_not_called()

    def test_empty_forecasts_for_gaps_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        skill_stats,
    ):
        """No forecast data for gap years -> exits 0."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_data_reader.read_monthly_forecasts.assert_called_once_with(
                ["10001", "10002"],
                2025,
                2025,
            )
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_not_called()

    def test_no_matching_forecast_rows_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        skill_stats,
    ):
        """Forecasts exist but none match gap tuples -> exits 0."""
        # Forecasts are for a different month (month=6) than the gap (month=1)
        non_matching_forecasts = _make_forecasts(
            [
                {
                    "year": 2025,
                    "month": 6,
                    "code": "10001",
                    "model_short": "LR",
                    "q50": 200.0,
                    "q05": 180.0,
                    "q95": 220.0,
                    "valid_from": "2025-06-01",
                    "valid_to": "2025-06-30",
                    "date": "2025-06-01",
                    "flag": 0,
                },
            ]
        )

        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = non_matching_forecasts

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_not_called()

    def test_ensemble_returns_no_em_rows_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        skill_stats,
    ):
        """Ensemble calculator returns rows but none are EM -> exits 0."""
        # Return only base model rows, no ensemble models
        base_only = forecasts_for_gaps.copy()

        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = base_only

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_file_writer.save_monthly_forecast_data.assert_not_called()

    def test_save_error_causes_exit_one(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
    ):
        """save_monthly_forecast_data returning error string -> exit 1."""
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gap_tuples
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_file_writer.save_monthly_forecast_data.return_value = "Error: disk full"

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 1

    def test_multi_station_gaps(self, skill_stats):
        """Multiple stations with gaps are all processed."""
        combined = _make_combined(
            [
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "LR",
                    "forecasted_discharge": 100.0,
                    "month_in_year": 1,
                },
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10002",
                    "model_short": "LR",
                    "forecasted_discharge": 200.0,
                    "month_in_year": 1,
                },
            ]
        )
        gaps = _make_gaps(
            [
                (2025, 1, "10001"),
                (2025, 1, "10002"),
            ]
        )
        forecasts = _make_forecasts(
            [
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "LR",
                    "q50": 100.0,
                    "q05": 80.0,
                    "q95": 120.0,
                    "valid_from": "2025-01-01",
                    "valid_to": "2025-01-31",
                    "date": "2025-01-01",
                    "flag": 0,
                },
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10002",
                    "model_short": "LR",
                    "q50": 200.0,
                    "q05": 180.0,
                    "q95": 220.0,
                    "valid_from": "2025-01-01",
                    "valid_to": "2025-01-31",
                    "date": "2025-01-01",
                    "flag": 0,
                },
            ]
        )
        em_rows = pd.DataFrame(
            [
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10001",
                    "model_short": "EM",
                    "forecasted_discharge": 100.0,
                    "month_in_year": 1,
                },
                {
                    "year": 2025,
                    "month": 1,
                    "code": "10002",
                    "model_short": "EM",
                    "forecasted_discharge": 200.0,
                    "month_in_year": 1,
                },
            ]
        )
        ensemble_output = pd.concat(
            [forecasts, em_rows],
            ignore_index=True,
        )

        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gaps
        mock_data_reader.read_skill_metrics.return_value = skill_stats
        mock_data_reader.read_monthly_forecasts.return_value = forecasts
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_output
        mock_file_writer.save_monthly_forecast_data.return_value = None

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "ensemble_calc": mock_ensemble_calc,
                    "file_writer": mock_file_writer,
                    "pt": mock_pt,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            saved_df = mock_file_writer.save_monthly_forecast_data.call_args[0][0]
            em_saved = saved_df[saved_df["model_short"] == "EM"]
            assert len(em_saved) == 2, f"Expected 2 EM rows (one per station), got {len(em_saved)}"
            codes_with_em = set(em_saved["code"].astype(str))
            assert codes_with_em == {"10001", "10002"}
