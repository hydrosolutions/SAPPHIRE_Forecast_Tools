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


def _load_real_model_names():
    """Load the REAL ``src/model_names.py`` module from disk.

    ``_import_module`` below replaces ``sys.modules["src"]`` with a
    MagicMock so the entry point's other ``src.*`` imports can be
    controlled per-test. ``postprocessing_maintenance_long_term.py``
    also does ``from src.model_names import (...)`` (PP-065 P1b Finding
    1/2 fix) -- a plain, dependency-free constants/helpers module with
    no mockable side effects worth faking, so tests use the real thing,
    loaded fresh from its file to stay independent of whatever else in
    the test session has already touched ``sys.modules["src"]``.
    """
    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_long_term_test_model_names",
        os.path.join(SCRIPT_DIR, "src", "model_names.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def _default_empty_quarterly_readers(mock_data_reader):
    """Default the quarterly gap-universe readers to empty DataFrames.

    postprocessing_maintenance_long_term always builds a quarterly
    gap-fill universe from BOTH ``read_quarterly_combined_forecasts``
    and ``read_quarterly_forecasts`` now (PP-065 P1b item 4), so an
    unconfigured MagicMock return value (which is truthy, and whose
    ``.empty`` is also a truthy MagicMock, unlike an empty DataFrame)
    would make ``pd.concat`` raise instead of yielding an empty
    universe. Only fills in a default when the test hasn't already set
    a real return value, so individual tests can still override either
    reader.
    """
    if isinstance(mock_data_reader.read_quarterly_combined_forecasts.return_value, MagicMock):
        mock_data_reader.read_quarterly_combined_forecasts.return_value = pd.DataFrame()
    if isinstance(mock_data_reader.read_quarterly_forecasts.return_value, MagicMock):
        mock_data_reader.read_quarterly_forecasts.return_value = pd.DataFrame()


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

    _default_empty_quarterly_readers(mock_data_reader)

    # TimingStats and timer must be usable at module level
    mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

    real_model_names = _load_real_model_names()

    mock_src = MagicMock()
    mock_src.postprocessing_tools = mock_pt
    mock_src.data_reader = mock_data_reader
    mock_src.ensemble_calculator = mock_ensemble_calc
    mock_src.gap_detector = mock_gap_detector
    mock_src.file_writer = mock_file_writer
    mock_src.model_names = real_model_names

    sys.modules["setup_library"] = mock_sl
    sys.modules["src"] = mock_src
    sys.modules["src.postprocessing_tools"] = mock_pt
    sys.modules["src.data_reader"] = mock_data_reader
    sys.modules["src.model_names"] = real_model_names
    sys.modules["src.ensemble_calculator"] = mock_ensemble_calc
    sys.modules["src.gap_detector"] = mock_gap_detector
    sys.modules["src.file_writer"] = mock_file_writer

    spec = importlib.util.spec_from_file_location(
        "postprocessing_maintenance_long_term_module",
        os.path.join(SCRIPT_DIR, "postprocessing_maintenance_long_term.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._supported_seasonal_issue_leads = MagicMock(return_value=[])
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
        """Empty combined forecasts -> monthly block skips gap detection.

        The monthly block does not complete, so quarterly gap-fill still
        runs (finds nothing, since both quarterly readers default to
        empty), and the script exits 0 right after -- before seasonal.
        """
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_file_writer = MagicMock()

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {}):
            module = _import_module(
                {
                    "sl": mock_sl,
                    "data_reader": mock_data_reader,
                    "gap_detector": mock_gap_detector,
                    "file_writer": mock_file_writer,
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
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

    def test_no_gaps_found_exits_zero(self, combined_with_models):
        """Gap detector returns empty -> monthly block does not complete.

        Quarterly gap-fill still runs (finds nothing), and the script
        exits 0 right after -- before seasonal.
        """
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()

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
                    "file_writer": mock_file_writer,
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
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

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

    def test_seasonal_gap_fill_processes_single_missing_issue_without_collapsing_leads(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
    ):
        """Seasonal gap-fill reads and fills only the missing issue lead."""
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
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_data_reader.read_quarterly_combined_forecasts.return_value = pd.DataFrame()
        mock_file_writer.save_monthly_forecast_data.return_value = None
        mock_file_writer.save_seasonal_forecast_data.return_value = None

        seasonal_combined_by_lead = {
            3: pd.DataFrame(
                {
                    "season_year": [2024],
                    "season_in_year": [3],
                    "code": ["10001"],
                    "model_short": ["EM"],
                    "forecasted_discharge": [103.0],
                }
            ),
            2: pd.DataFrame(
                {
                    "season_year": [2024],
                    "season_in_year": [2],
                    "code": ["10001"],
                    "model_short": ["LR"],
                    "forecasted_discharge": [102.0],
                }
            ),
            1: pd.DataFrame(
                {
                    "season_year": [2024],
                    "season_in_year": [1],
                    "code": ["10001"],
                    "model_short": ["EM"],
                    "forecasted_discharge": [101.0],
                }
            ),
            0: pd.DataFrame(
                {
                    "season_year": [2024],
                    "season_in_year": [0],
                    "code": ["10001"],
                    "model_short": ["EM"],
                    "forecasted_discharge": [100.0],
                }
            ),
        }

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "season":
                return pd.DataFrame(
                    {
                        "season_in_year": [2],
                        "code": ["10001"],
                        "model_short": ["LR"],
                        "sdivsigma": [0.3],
                        "nse": [0.8],
                    }
                )
            return skill_stats

        def read_seasonal_combined_forecasts(codes=None, horizon_value=None):
            return seasonal_combined_by_lead[horizon_value]

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_seasonal_combined_forecasts.side_effect = (
            read_seasonal_combined_forecasts
        )
        mock_gap_detector.detect_missing_seasonal_ensembles.return_value = pd.DataFrame(
            {
                "season_year": [2024],
                "season_in_year": [2],
                "code": ["10001"],
                "model_short": ["EM"],
            }
        )
        mock_data_reader.read_seasonal_forecasts.return_value = pd.DataFrame(
            {
                "season_year": [2024],
                "season_in_year": [2],
                "code": ["10001"],
                "model_short": ["LR"],
                "q50": [102.0],
            }
        )
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_ensemble_calc.create_seasonal_ensemble_forecasts.return_value = pd.DataFrame(
            {
                "season_year": [2024, 2024],
                "season_in_year": [2, 3],
                "code": ["10001", "10001"],
                "model_short": ["EM", "EM"],
                "forecasted_discharge": [102.0, 999.0],
            }
        )

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
            module._read_station_codes = MagicMock(return_value=["10001"])
            module._supported_seasonal_issue_leads = MagicMock(return_value=[3, 2, 1, 0])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            assert [
                call.kwargs["horizon_value"]
                for call in mock_data_reader.read_seasonal_combined_forecasts.call_args_list
            ] == [3, 2, 1, 0]
            mock_data_reader.read_seasonal_forecasts.assert_called_once()
            assert mock_data_reader.read_seasonal_forecasts.call_args.kwargs["horizon_value"] == 2

            saved = mock_file_writer.save_seasonal_forecast_data.call_args.args[0]
            assert set(saved["season_in_year"]) == {0, 1, 2, 3}
            filled = saved[(saved["season_in_year"] == 2) & (saved["model_short"] == "EM")]
            assert len(filled) == 1
            assert filled.iloc[0]["forecasted_discharge"] == 102.0
            assert not (
                (saved["season_in_year"] == 3) & (saved["forecasted_discharge"] == 999.0)
            ).any()

    @pytest.mark.parametrize("flag_on", [True, False])
    def test_quarterly_gap_fill_dedup_lead_aware(
        self,
        monkeypatch,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
        flag_on,
    ):
        """M1 P1b: the quarterly gap-fill dedup keeps two rows sharing

        (year, quarter_in_year, code, model_short) but differing in
        horizon_value when the flag is ON, vs. collapsing them (today's
        behavior) when OFF.
        """
        if flag_on:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        else:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

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
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_file_writer.save_monthly_forecast_data.return_value = None
        mock_file_writer.save_quarterly_forecast_data.return_value = None

        # Two existing quarterly rows for the SAME (year, quarter, code,
        # model_short) but DIFFERENT horizon_value (leads) -- the case
        # this dedup must not silently collapse under the flag.
        q_combined = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_Base"],
                "horizon_value": [0, 1],
                "forecasted_discharge": [100.0, 110.0],
            }
        )
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["EM"],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        # Two eligible raw models (LR_Base, LR_SM) at the SAME lead (0)
        # at the gap key so the two-raw-model gap-universe prefilter
        # admits it (PP-065 P1b item 4) -- a single-model key can never
        # form a Naive Mean and would otherwise be dropped before gap
        # detection even runs. horizon_value is set (matching what the
        # real reader always returns under flag ON) so the Finding-2
        # lead-aware prefilter key does not drop this admission via a
        # NaN-horizon_value groupby key.
        q_fc = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [0, 0],
                "forecasted_discharge": [100.0, 100.0],
            }
        )
        q_joint = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["EM"],
                "forecasted_discharge": [105.0],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return skill_stats

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_fc
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]
            lr_base_rows = saved[saved["model_short"] == "LR_Base"]
            if flag_on:
                assert len(lr_base_rows) == 2
                assert set(lr_base_rows["horizon_value"]) == {0, 1}
            else:
                assert len(lr_base_rows) == 1

    @pytest.mark.parametrize("flag_on", [True, False])
    def test_quarterly_gap_fill_only_writes_gap_leads(
        self,
        monkeypatch,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        ensemble_result,
        skill_stats,
        flag_on,
    ):
        """FIX 4: under the flag, freshly-generated ensemble rows are

        filtered to the ACTUAL missing gap keys before merge, so a NON-gap
        lead's existing ensemble row is not overwritten (keep="last") by a
        regenerated row for a lead that was never a gap. Flag OFF keeps
        today's (unfiltered) behavior.
        """
        if flag_on:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        else:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

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
        mock_data_reader.read_monthly_forecasts.return_value = forecasts_for_gaps
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = ensemble_result
        mock_file_writer.save_monthly_forecast_data.return_value = None
        mock_file_writer.save_quarterly_forecast_data.return_value = None

        # Existing lead-0 Naive Mean (NOT a gap) with a distinctive
        # discharge. Quarter no longer produces EM (PP-065 P1b item 3),
        # so gap detection keys on Naive Mean instead.
        q_combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
                "horizon_value": [0],
                "forecasted_discharge": [100.0],
            }
        )
        # ONLY the lead-1 Naive Mean is reported missing.
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
                "horizon_value": [1],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        # Two eligible raw models (LR_Base, LR_SM) so the two-raw-model
        # gap-universe prefilter admits the key (PP-065 P1b item 4).
        q_fc = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [100.0, 100.0],
            }
        )
        # Ensemble output regenerates Naive Mean for BOTH leads; the
        # lead-0 row has a DIFFERENT discharge (999) so an overwrite
        # would be observable.
        q_joint = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["Naive Mean", "Naive Mean"],
                "horizon_value": [0, 1],
                "forecasted_discharge": [999.0, 111.0],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return skill_stats

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_fc
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]
            em_rows = saved[saved["model_short"] == "Naive Mean"]

            if flag_on:
                # Both leads present, and the NON-gap lead-0 row keeps its
                # ORIGINAL discharge (100), NOT the regenerated 999.
                assert set(em_rows["horizon_value"]) == {0, 1}
                lead0 = em_rows[em_rows["horizon_value"] == 0]
                lead1 = em_rows[em_rows["horizon_value"] == 1]
                assert len(lead0) == 1
                assert float(lead0.iloc[0]["forecasted_discharge"]) == 100.0
                assert len(lead1) == 1
                assert float(lead1.iloc[0]["forecasted_discharge"]) == 111.0
            else:
                # Flag OFF: today's behavior -- leads collapse (no
                # horizon_value in dedup key), one row kept.
                assert len(em_rows) == 1

    @pytest.mark.parametrize("flag_on", [True, False])
    def test_monthly_gap_fill_only_writes_gap_leads(self, monkeypatch, flag_on):
        """M1 P5b: under the flag, the freshly-generated MONTHLY ensemble

        rows are filtered to the ACTUAL missing per-lead gap keys before
        merge, so a NON-gap lead's existing ensemble row is not overwritten
        (keep="last") by a regenerated row for a lead that was never a gap.
        The gap-filled EM is stamped at the lead it was detected missing
        (lead 1) and survives merge-back without collapsing the existing
        lead-0 EM. Flag OFF keeps today's (period-key) behavior, under which
        the non-gap lead-0 EM IS overwritten.
        """
        if flag_on:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        else:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        # Existing combined: lead-0 EM (NOT a gap) with a distinctive
        # discharge (100), plus LR_Base at leads 0 and 1.
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025],
                "month": [1, 1, 1],
                "code": ["19999", "19999", "19999"],
                "model_short": ["EM", "LR_Base", "LR_Base"],
                "horizon_value": [0, 0, 1],
                "forecasted_discharge": [100.0, 100.0, 90.0],
                "month_in_year": [1, 1, 1],
            }
        )
        # ONLY the lead-1 EM is reported missing.
        if flag_on:
            gaps = pd.DataFrame(
                {
                    "year": [2025],
                    "month": [1],
                    "code": ["19999"],
                    "model_short": ["EM"],
                    "horizon_value": [1],
                }
            )
        else:
            # Flag OFF: real detector emits period-only gaps (no lead col).
            gaps = pd.DataFrame(
                {
                    "year": [2025],
                    "month": [1],
                    "code": ["19999"],
                    "model_short": ["EM"],
                }
            )
        forecasts = pd.DataFrame(
            {
                "year": [2025, 2025],
                "month": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_Base"],
                "horizon_value": [0, 1],
                "q50": [100.0, 90.0],
                "valid_from": ["2025-01-01", "2025-01-01"],
                "valid_to": ["2025-01-31", "2025-01-31"],
                "date": ["2025-01-01", "2025-01-01"],
                "flag": [0, 0],
            }
        )
        # Ensemble output regenerates EM for BOTH leads; the lead-0 EM has a
        # DIFFERENT discharge (999) so an overwrite would be observable.
        joint = pd.concat(
            [
                forecasts.assign(month_in_year=1, forecasted_discharge=forecasts["q50"]),
                pd.DataFrame(
                    {
                        "year": [2025, 2025],
                        "month": [1, 1],
                        "code": ["19999", "19999"],
                        "model_short": ["EM", "EM"],
                        "horizon_value": [0, 1],
                        "forecasted_discharge": [999.0, 111.0],
                        "month_in_year": [1, 1],
                    }
                ),
            ],
            ignore_index=True,
        )

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = gaps
        mock_data_reader.read_monthly_forecasts.return_value = forecasts
        mock_data_reader.read_skill_metrics.return_value = _make_skill()
        mock_ensemble_calc.create_monthly_ensemble_forecasts.return_value = joint
        mock_file_writer.save_monthly_forecast_data.return_value = None
        # Quarterly + seasonal paths are no-ops for this monthly-focused test.
        mock_data_reader.read_quarterly_combined_forecasts.return_value = pd.DataFrame()

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_monthly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_monthly_forecast_data.call_args.args[0]
            em_rows = saved[saved["model_short"] == "EM"]

            if flag_on:
                # Both leads present; the gap-filled EM is stamped lead 1
                # (discharge 111), and the NON-gap lead-0 EM keeps its
                # ORIGINAL discharge (100), NOT the regenerated 999.
                assert set(em_rows["horizon_value"]) == {0, 1}
                lead0 = em_rows[em_rows["horizon_value"] == 0]
                lead1 = em_rows[em_rows["horizon_value"] == 1]
                assert len(lead0) == 1
                assert float(lead0.iloc[0]["forecasted_discharge"]) == 100.0
                assert len(lead1) == 1
                assert float(lead1.iloc[0]["forecasted_discharge"]) == 111.0
            else:
                # Flag OFF: today's behavior -- period-only gap filter keeps
                # BOTH regenerated EM rows, so the non-gap lead-0 EM IS
                # overwritten (keep="last") to 999.
                lead0 = em_rows[em_rows["horizon_value"] == 0]
                lead1 = em_rows[em_rows["horizon_value"] == 1]
                assert len(lead0) == 1
                assert float(lead0.iloc[0]["forecasted_discharge"]) == 999.0
                assert len(lead1) == 1
                assert float(lead1.iloc[0]["forecasted_discharge"]) == 111.0

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
        """POSTPROCESSING_GAPFILL_WINDOW_MONTHS controls lookback.

        The monthly block does not complete (no gaps), so quarterly
        gap-fill still runs (finds nothing), and the script exits 0
        right after -- before seasonal.
        """
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_file_writer = MagicMock()

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
                        "file_writer": mock_file_writer,
                    }
                )
                module._read_station_codes = MagicMock(return_value=["10001"])

                with pytest.raises(SystemExit) as exc_info:
                    module.postprocessing_maintenance_long_term()

                assert exc_info.value.code == 0
                call_args = mock_gap_detector.detect_missing_monthly_ensembles.call_args
                # Second positional arg is lookback
                assert call_args[0][1] == 6, f"Expected lookback=6, got {call_args[0][1]}"
                mock_file_writer.save_quarterly_forecast_data.assert_not_called()
                mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

    def test_empty_skill_metrics_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
    ):
        """No skill metrics -> monthly block does not complete.

        No ensembles are created. Quarterly gap-fill still runs (finds
        nothing), and the script exits 0 right after -- before seasonal.
        """
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()

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
                    "file_writer": mock_file_writer,
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
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

    def test_empty_forecasts_for_gaps_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        skill_stats,
    ):
        """No forecast data for gap years -> monthly block does not complete.

        Quarterly gap-fill still runs (finds nothing), and the script
        exits 0 right after -- before seasonal.
        """
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()

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
                    "file_writer": mock_file_writer,
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
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

    def test_no_matching_forecast_rows_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        skill_stats,
    ):
        """Forecasts exist but none match gap tuples.

        The monthly block does not complete. Quarterly gap-fill still
        runs (finds nothing), and the script exits 0 right after --
        before seasonal.
        """
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
        mock_file_writer = MagicMock()

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
                    "file_writer": mock_file_writer,
                }
            )
            module._read_station_codes = MagicMock(return_value=["10001", "10002"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            mock_ensemble_calc.create_monthly_ensemble_forecasts.assert_not_called()
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

    def test_ensemble_returns_no_em_rows_exits_zero(
        self,
        combined_with_models,
        gap_tuples,
        forecasts_for_gaps,
        skill_stats,
    ):
        """Ensemble calculator returns rows but none are EM.

        The monthly block does not complete. Quarterly gap-fill still
        runs (finds nothing), and the script exits 0 right after --
        before seasonal.
        """
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
            mock_file_writer.save_quarterly_forecast_data.assert_not_called()
            mock_data_reader.read_seasonal_combined_forecasts.assert_not_called()

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


class TestFilterQuarterlyGapUniverse:
    """PP-065 P1b Findings 1 & 2 (out-of-loop review): direct unit tests

    for ``_filter_quarterly_gap_universe``.
    """

    @staticmethod
    def _filter(universe):
        with patch.dict(sys.modules, {}):
            module = _import_module({})
            return module._filter_quarterly_gap_universe(universe)

    def test_admits_two_derived_models_with_no_lr_present(self):
        """FIX 1: a key with GBT + MC_ALD only (no LR rows at all) is a

        real two-raw-model Naive Mean candidate -- any two of the nine
        raw quarter models can form one -- and must be admitted, not
        dropped for lacking LR specifically.
        """
        universe = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["GBT", "MC_ALD"],
                "forecasted_discharge": [100.0, 200.0],
            }
        )

        result = self._filter(universe)

        assert len(result) == 2
        assert set(result["model_short"]) == {"GBT", "MC_ALD"}

    def test_single_derived_model_key_still_excluded(self):
        """A single-model key (even a derived model) can never form a

        Naive Mean and must remain excluded -- this fix only widens
        WHICH models count as raw, not the >= 2 threshold itself.
        """
        universe = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["GBT"],
                "forecasted_discharge": [100.0],
            }
        )

        result = self._filter(universe)

        assert result.empty

    def test_flag_on_excludes_single_model_per_lead_across_different_leads(self, monkeypatch):
        """FIX 2: under the flag, LR_Base at lead 0 and LR_SM at lead 1

        for the same (year, quarter, code) must NOT be counted together
        as "2 distinct models present" -- neither lead alone has 2
        models, so neither can form a Naive Mean, and the key must be
        excluded.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        universe = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [0, 1],
                "forecasted_discharge": [100.0, 110.0],
            }
        )

        result = self._filter(universe)

        assert result.empty

    def test_flag_on_admits_two_models_at_the_same_lead(self, monkeypatch):
        """Companion to the above: two single-model rows at the SAME

        lead ARE admitted under the flag.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        universe = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [100.0, 110.0],
            }
        )

        result = self._filter(universe)

        assert len(result) == 2

    def test_flag_off_ignores_horizon_value_for_admission(self, monkeypatch):
        """Flag OFF: unchanged (period-only key, no lead) -- two

        single-model rows at DIFFERENT leads still count together as
        "2 distinct models present" for the (year, quarter, code) key,
        exactly like today's behavior (no `horizon_value` in the key).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        universe = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [0, 1],
                "forecasted_discharge": [100.0, 110.0],
            }
        )

        result = self._filter(universe)

        assert len(result) == 2


class TestQuarterlySkilledMeanNotOverwritten:
    """PP-065 P1b Finding 4 (out-of-loop review, data-integrity risk):

    a quarter/lead whose ONLY gap is Naive Mean, with an existing
    correct Skilled Mean already persisted in ``q_combined``, must end
    up with the Naive Mean freshly added AND the Skilled Mean value
    UNCHANGED from what was already persisted -- even though
    ``ensemble_calculator`` would compute a numerically DIFFERENT
    Skilled Mean for that same key if run today (e.g. skill membership
    shifted). ``model_short`` is deliberately excluded from the q_new
    admission key so a fresh Naive Mean at a gapped key gets in; this
    must not also let a fresh, unrequested Skilled Mean silently win
    the keep="last" merge over an already-correct persisted value.
    """

    def test_existing_skilled_mean_survives_a_naive_mean_only_gap(self):
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        # Monthly block: no combined forecasts -> exits early, quarterly
        # still runs (this finding is independent of the monthly path).
        mock_data_reader.read_monthly_combined_forecasts.return_value = pd.DataFrame()

        # Existing, CORRECT, already-persisted Skilled Mean (100.0) --
        # never reported as a gap -- alongside a station/quarter that is
        # ONLY missing its Naive Mean.
        q_combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Skilled Mean"],
                "forecasted_discharge": [100.0],
            }
        )
        # Two eligible raw models so the gap-universe prefilter admits
        # the key (PP-065 P1b item 4 / Finding 1 fix).
        q_universe_raw = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "forecasted_discharge": [50.0, 60.0],
            }
        )
        # Gap detection reports ONLY Naive Mean missing at this key
        # (the gap detector's ensemble_models={"Naive Mean"} call).
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        # ensemble_calculator recomputes BOTH ensembles together. The
        # freshly computed Skilled Mean (999.0) is DELIBERATELY
        # different from the persisted value (100.0) -- e.g. skill
        # membership shifted since it was last computed -- to make an
        # accidental overwrite observable.
        q_joint = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["Naive Mean", "Skilled Mean"],
                "forecasted_discharge": [55.0, 999.0],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return pd.DataFrame()

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_universe_raw
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint
        mock_file_writer.save_quarterly_forecast_data.return_value = None

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]

            naive_rows = saved[saved["model_short"] == "Naive Mean"]
            assert len(naive_rows) == 1
            assert float(naive_rows.iloc[0]["forecasted_discharge"]) == 55.0

            skilled_rows = saved[saved["model_short"] == "Skilled Mean"]
            assert len(skilled_rows) == 1
            assert float(skilled_rows.iloc[0]["forecasted_discharge"]) == 100.0, (
                "The already-persisted Skilled Mean must survive unchanged -- "
                "it was never a gap -- not be silently overwritten by a "
                "freshly recomputed value."
            )


class TestQuarterlySkilledMeanGuardLeadAware:
    """PP-065 P1b Finding B (confirm-fixes review, fix round 2): the

    Finding-4 overwrite guard above must key on `horizon_value` under
    SAPPHIRE_SKILL_LEAD_AWARE even when `q_combined` (existing
    persisted data) lacks the column entirely -- e.g. it holds only a
    legacy Skilled Mean row written before this reader/writer contract
    existed, or before the flag was ever turned on for that org.
    Requiring the column on BOTH frames (fix round 1's condition) let a
    legacy no-lead row silently match ANY new lead, incorrectly
    suppressing a genuinely new, correctly-gapped Skilled Mean for a
    DIFFERENT lead than the legacy row's unknown one.
    """

    def test_new_lead_skilled_mean_written_despite_legacy_no_lead_existing_row(self, monkeypatch):
        """Regression for Finding B: q_combined has a legacy Skilled

        Mean with NO horizon_value column at all; q_new has a
        genuinely new, correctly-gapped Skilled Mean for a specific
        lead under flag ON -- the new lead's Skilled Mean must be
        WRITTEN, not suppressed.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = pd.DataFrame()

        # Legacy, pre-lead-aware Skilled Mean -- NO horizon_value column
        # at all, unlike a present-but-null column.
        q_combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Skilled Mean"],
                "forecasted_discharge": [100.0],
            }
        )
        assert "horizon_value" not in q_combined.columns

        # Two eligible raw models at the SAME lead so the flag-ON
        # gap-universe prefilter admits the key.
        q_universe_raw = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [50.0, 60.0],
            }
        )
        # Gap detection reports ONLY Naive Mean missing, at lead 1.
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
                "horizon_value": [1],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        # ensemble_calculator recomputes BOTH ensembles together, at
        # the gapped lead (1). The Skilled Mean value (77.0) is
        # genuinely NEW -- there is no known-lead existing value to
        # compare it against.
        q_joint = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["Naive Mean", "Skilled Mean"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [55.0, 77.0],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return pd.DataFrame()

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_universe_raw
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint
        mock_file_writer.save_quarterly_forecast_data.return_value = None

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]

            skilled_rows = saved[saved["model_short"] == "Skilled Mean"]
            # Both the legacy no-lead row and the new lead-1 row survive:
            # they are distinct dedup keys (differing horizon_value), so
            # this is not an overwrite -- the new lead's Skilled Mean is
            # simply ADDED, not suppressed.
            lead1_rows = skilled_rows[skilled_rows["horizon_value"] == 1]
            assert len(lead1_rows) == 1, (
                "The new lead-1 Skilled Mean must be written -- a legacy "
                "Skilled Mean with no known lead must never be treated as "
                "already covering it."
            )
            assert float(lead1_rows.iloc[0]["forecasted_discharge"]) == 77.0
            legacy_rows = skilled_rows[skilled_rows["horizon_value"].isna()]
            assert len(legacy_rows) == 1
            assert float(legacy_rows.iloc[0]["forecasted_discharge"]) == 100.0

    def test_existing_skilled_mean_at_same_known_lead_still_protected(self, monkeypatch):
        """Companion test: the ORIGINAL finding-4 protection (exact lead

        match) must still hold under the lead-aware flag when
        `q_combined` DOES carry the same, known lead as the new row --
        this fix must not reopen finding 4.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = pd.DataFrame()

        # Existing, CORRECT, already-persisted Skilled Mean at the SAME
        # known lead (1) as the freshly recomputed one below.
        q_combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Skilled Mean"],
                "horizon_value": [1],
                "forecasted_discharge": [100.0],
            }
        )
        q_universe_raw = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [50.0, 60.0],
            }
        )
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
                "horizon_value": [1],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        # Freshly computed Skilled Mean (999.0) DELIBERATELY differs
        # from the persisted value (100.0) at the SAME lead (1), to make
        # an accidental overwrite observable.
        q_joint = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["Naive Mean", "Skilled Mean"],
                "horizon_value": [1, 1],
                "forecasted_discharge": [55.0, 999.0],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return pd.DataFrame()

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_universe_raw
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint
        mock_file_writer.save_quarterly_forecast_data.return_value = None

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0

            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]

            naive_rows = saved[saved["model_short"] == "Naive Mean"]
            assert len(naive_rows) == 1
            assert float(naive_rows.iloc[0]["forecasted_discharge"]) == 55.0

            skilled_rows = saved[saved["model_short"] == "Skilled Mean"]
            assert len(skilled_rows) == 1
            assert float(skilled_rows.iloc[0]["forecasted_discharge"]) == 100.0, (
                "The already-persisted Skilled Mean at the SAME known lead "
                "must survive unchanged, exactly like the flag-OFF case in "
                "TestQuarterlySkilledMeanNotOverwritten -- this fix only "
                "changes behavior when q_combined lacks horizon_value "
                "entirely."
            )


class TestQuarterlyFallThroughAfterMonthlyEarlyExit:
    """PP-065 P1b Finding 5 (out-of-loop review, minor): the six

    monthly-early-exit tests in ``TestMaintenanceLongTerm`` default the
    quarterly readers to empty, so their "no quarterly save" assertions
    hold whether or not the quarterly block is actually REACHED after
    the monthly block exits early -- an immediate ``sys.exit(0)``
    before quarterly would look identical. This test proves the
    quarterly block is actually reached and does real work: the
    monthly block exits early (no monthly gaps), and the quarterly
    block still detects a gap, creates an ensemble, and calls
    ``save_quarterly_forecast_data``.
    """

    def test_monthly_early_exit_quarterly_block_still_saves(self, combined_with_models):
        mock_sl = MagicMock()
        mock_data_reader = MagicMock()
        mock_gap_detector = MagicMock()
        mock_ensemble_calc = MagicMock()
        mock_file_writer = MagicMock()
        mock_pt = MagicMock()
        mock_pt.TimingStats.return_value.summary.return_value = ([], 0)

        mock_sl.load_environment.return_value = None
        mock_data_reader.read_monthly_combined_forecasts.return_value = combined_with_models
        # Monthly: no gaps found -> _run_monthly_gap_fill returns
        # (False, gaps) and the monthly block exits early.
        mock_gap_detector.detect_missing_monthly_ensembles.return_value = pd.DataFrame(
            columns=["year", "month", "code", "model_short"]
        )

        # Quarterly: enough real data for a Naive Mean gap to be
        # detected AND filled -- two distinct raw models present so the
        # gap-universe prefilter admits the key (Finding 1 fix).
        q_combined = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "forecasted_discharge": [100.0],
            }
        )
        q_universe_raw = pd.DataFrame(
            {
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR_Base", "LR_SM"],
                "forecasted_discharge": [100.0, 105.0],
            }
        )
        q_gaps = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
            }
        )
        q_skill = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["LR_Base"],
                "sdivsigma": [0.3],
                "nse": [0.8],
            }
        )
        q_joint = pd.DataFrame(
            {
                "year": [2025],
                "quarter_in_year": [1],
                "code": ["19999"],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [102.5],
            }
        )

        def read_skill_metrics(horizon_type, codes=None):
            if horizon_type == "quarter":
                return q_skill
            return pd.DataFrame()

        mock_data_reader.read_skill_metrics.side_effect = read_skill_metrics
        mock_data_reader.read_quarterly_combined_forecasts.return_value = q_combined
        mock_data_reader.read_quarterly_forecasts.return_value = q_universe_raw
        mock_gap_detector.detect_missing_quarterly_ensembles.return_value = q_gaps
        mock_ensemble_calc.create_quarterly_ensemble_forecasts.return_value = q_joint
        mock_file_writer.save_quarterly_forecast_data.return_value = None

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
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.postprocessing_maintenance_long_term()

            assert exc_info.value.code == 0
            # The monthly block exited early (no gaps): prove the
            # quarterly block was actually REACHED and did real work,
            # not merely "nothing happened" -- which an early
            # sys.exit(0) before quarterly would also produce.
            mock_file_writer.save_quarterly_forecast_data.assert_called_once()
            saved = mock_file_writer.save_quarterly_forecast_data.call_args.args[0]
            assert "Naive Mean" in set(saved["model_short"])
