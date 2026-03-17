"""Tests for src/write_diagnostics.py — DEBUG-level write diagnostics."""

import logging
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src import write_diagnostics

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def forecast_df():
    """Small forecast DataFrame with known properties."""
    return pd.DataFrame(
        {
            "code": ["S1", "S1", "S2", "S2", "S1", "S2"],
            "model_short": ["LR", "TFT", "LR", "TFT", "EM", "EM"],
            "pentad_in_year": [5, 5, 5, 5, 5, 5],
            "date": pd.to_datetime(
                [
                    "2025-01-25",
                    "2025-01-25",
                    "2025-01-25",
                    "2025-01-25",
                    "2025-01-25",
                    "2025-01-25",
                ]
            ),
            "forecasted_discharge": [10.5, 0.0, -3.2, np.nan, 5.0, 7.0],
            "composition": [np.nan, np.nan, np.nan, np.nan, "LR, TFT", "LR, TFT"],
        }
    )


@pytest.fixture()
def skill_df():
    """Small skill metrics DataFrame with known properties."""
    return pd.DataFrame(
        {
            "code": ["S1", "S1", "S2", "S2"],
            "model_short": ["LR", "TFT", "LR", "TFT"],
            "pentad_in_year": [1, 1, 1, 1],
            "n_pairs": [2, 10, 5, 1],
            "nse": [-0.5, 0.8, 0.3, -1.2],
            "sdivsigma": [0.5, 1.0, 2.5, 3.0],
            "delta": [0.1, 0.2, 0.3, 0.4],
            "accuracy": [0.7, 0.8, 0.6, 0.5],
            "mae": [1.0, 2.0, 3.0, 4.0],
        }
    )


@pytest.fixture()
def fdc_df():
    """Small FDC metrics DataFrame."""
    return pd.DataFrame(
        {
            "code": ["S1", "S1"],
            "model_short": ["LR", "TFT"],
            "fhv": [-15.2, 42.3],
            "flv": [-28.1, 55.0],
        }
    )


@pytest.fixture()
def threshold_df():
    """Small threshold metrics DataFrame."""
    return pd.DataFrame(
        {
            "code": ["S1", "S1", "S1", "S1"],
            "model_short": ["LR", "LR", "TFT", "TFT"],
            "threshold_type": ["Q10", "Q90", "Q10", "Q90"],
            "n_years": [5, 14, 5, 14],
            "f1": [0.0, 0.89, 0.50, 0.78],
            "csi": [0.0, 0.78, 0.40, 0.65],
        }
    )


# ===================================================================
# diagnose_forecast_data
# ===================================================================


class TestDiagnoseForecastData:
    """Tests for write_diagnostics.diagnose_forecast_data."""

    def test_logs_header_at_debug(self, caplog, forecast_df):
        """Diagnostic block header and sections appear at DEBUG."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "pentad combined")
        assert "=== Write Diagnostics: pentad combined (6 rows) ===" in caplog.text

    def test_skipped_at_info(self, caplog, forecast_df):
        """No diagnostic output when logger is at INFO level."""
        with caplog.at_level(logging.INFO, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "pentad combined")
        assert "Write Diagnostics" not in caplog.text

    def test_discharge_counts(self, caplog, forecast_df):
        """NaN, zero, and negative discharge counts are correct."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        # 1 NaN, 1 zero, 1 negative
        assert "NaN=1" in caplog.text
        assert "zero=1" in caplog.text
        assert "negative=1" in caplog.text

    def test_per_model_row_counts(self, caplog, forecast_df):
        """Per-model row counts are accurate."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        assert "EM=2" in caplog.text
        assert "LR=2" in caplog.text
        assert "TFT=2" in caplog.text

    def test_ensemble_composition(self, caplog, forecast_df):
        """Ensemble composition groupings are detected."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        assert "Ensemble composition" in caplog.text
        assert "EM=" in caplog.text
        assert "LR, TFT" in caplog.text

    def test_completeness(self, caplog, forecast_df):
        """Completeness at latest period reports station x model combos."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        # 2 stations x 3 models = 6 expected, 6 actual
        assert "2 stations x 3 models = 6 expected, 6 actual" in caplog.text

    def test_station_count(self, caplog, forecast_df):
        """Station count is reported correctly."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        assert "Stations: 2 unique" in caplog.text

    def test_date_range(self, caplog, forecast_df):
        """Date range is reported."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(forecast_df, "pentad", "test")
        assert "2025-01-25" in caplog.text

    def test_empty_dataframe_no_crash(self, caplog):
        """Empty DataFrame produces minimal output, no crash."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(pd.DataFrame(), "pentad", "empty test")
        assert "empty test (empty)" in caplog.text

    def test_none_input_no_crash(self, caplog):
        """None input produces minimal output, no crash."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(None, "pentad", "none test")
        assert "none test (empty)" in caplog.text

    def test_decad_horizon_type(self, caplog):
        """decad horizon type uses decad_in_year for completeness."""
        df = pd.DataFrame(
            {
                "code": ["S1"],
                "model_short": ["LR"],
                "decad_in_year": [10],
                "date": pd.to_datetime(["2025-04-01"]),
                "forecasted_discharge": [42.0],
            }
        )
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_forecast_data(df, "decad", "test")
        assert "1 stations x 1 models = 1 expected, 1 actual" in caplog.text


# ===================================================================
# diagnose_skill_metrics
# ===================================================================


class TestDiagnoseSkillMetrics:
    """Tests for write_diagnostics.diagnose_skill_metrics."""

    def test_logs_header_at_debug(self, caplog, skill_df):
        """Diagnostic block header appears at DEBUG."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "pentad skill metrics")
        assert "=== Write Diagnostics: pentad skill metrics (4 rows) ===" in caplog.text

    def test_skipped_at_info(self, caplog, skill_df):
        """No output at INFO level."""
        with caplog.at_level(logging.INFO, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "pentad skill metrics")
        assert "Write Diagnostics" not in caplog.text

    def test_n_pairs_low_confidence(self, caplog, skill_df):
        """Counts n_pairs < 3 correctly (rows with n_pairs=2 and n_pairs=1)."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "test")
        assert "low-confidence (n<3): 2 rows" in caplog.text

    def test_nse_worse_than_climatology(self, caplog, skill_df):
        """Counts NSE < 0 correctly (two rows: -0.5 and -1.2)."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "test")
        assert "worse-than-climatology (NSE<0): 2 rows" in caplog.text

    def test_sdivsigma_high_bias(self, caplog, skill_df):
        """Counts sdivsigma > 2.0 correctly (two rows: 2.5 and 3.0)."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "test")
        assert "high-bias (>2.0): 2 rows" in caplog.text

    def test_nan_counts(self, caplog):
        """NaN counts per metric column are accurate."""
        df = pd.DataFrame(
            {
                "code": ["S1", "S2"],
                "model_short": ["LR", "LR"],
                "pentad_in_year": [1, 1],
                "n_pairs": [5, 10],
                "nse": [0.5, np.nan],
                "sdivsigma": [np.nan, 1.0],
                "crps": [np.nan, np.nan],
            }
        )
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(df, "pentad", "test")
        assert "sdivsigma=1" in caplog.text
        assert "nse=1" in caplog.text
        assert "crps=2" in caplog.text

    def test_period_column_pentad(self, caplog, skill_df):
        """Uses pentad_in_year for pentad horizon type."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "test")
        assert "pentad_in_year 1..1" in caplog.text

    def test_period_column_decad(self, caplog):
        """Uses decad_in_year for decad horizon type."""
        df = pd.DataFrame(
            {
                "code": ["S1"],
                "model_short": ["LR"],
                "decad_in_year": [5],
                "n_pairs": [10],
                "nse": [0.5],
            }
        )
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(df, "decad", "test")
        assert "decad_in_year 5..5" in caplog.text

    def test_period_column_month(self, caplog):
        """Uses month_in_year for month horizon type."""
        df = pd.DataFrame(
            {
                "code": ["S1"],
                "model_short": ["LR"],
                "month_in_year": [3],
                "n_pairs": [10],
                "nse": [0.5],
            }
        )
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(df, "month", "test")
        assert "month_in_year 3..3" in caplog.text

    def test_per_model_rows(self, caplog, skill_df):
        """Per-model row counts are accurate."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(skill_df, "pentad", "test")
        assert "LR=2" in caplog.text
        assert "TFT=2" in caplog.text

    def test_empty_dataframe_no_crash(self, caplog):
        """Empty DataFrame produces minimal output, no crash."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(pd.DataFrame(), "pentad", "empty test")
        assert "empty test (empty)" in caplog.text

    def test_none_input_no_crash(self, caplog):
        """None input produces minimal output, no crash."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_skill_metrics(None, "pentad", "none test")
        assert "none test (empty)" in caplog.text


# ===================================================================
# diagnose_daily_skill_metrics
# ===================================================================


class TestDiagnoseDailySkillMetrics:
    """Tests for write_diagnostics.diagnose_daily_skill_metrics."""

    def test_both_present(self, caplog, fdc_df, threshold_df):
        """Both FDC and Threshold sections appear when both provided."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(fdc_df, threshold_df)
        assert "FDC: 2 rows" in caplog.text
        assert "Threshold: 4 rows" in caplog.text
        assert "fhv:" in caplog.text
        assert "flv:" in caplog.text
        assert "Q10" in caplog.text
        assert "Q90" in caplog.text
        assert "f1:" in caplog.text
        assert "csi:" in caplog.text

    def test_fdc_only(self, caplog, fdc_df):
        """Only FDC section when threshold is None."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(fdc_df, None)
        assert "FDC: 2 rows" in caplog.text
        assert "Threshold" not in caplog.text

    def test_threshold_only(self, caplog, threshold_df):
        """Only Threshold section when fdc is None."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(None, threshold_df)
        assert "Threshold: 4 rows" in caplog.text
        assert "FDC" not in caplog.text

    def test_both_none(self, caplog):
        """Empty output when both are None."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(None, None)
        assert "(empty)" in caplog.text

    def test_both_empty_df(self, caplog):
        """Empty output when both are empty DataFrames."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(pd.DataFrame(), pd.DataFrame())
        assert "(empty)" in caplog.text

    def test_skipped_at_info(self, caplog, fdc_df, threshold_df):
        """No output at INFO level."""
        with caplog.at_level(logging.INFO, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(fdc_df, threshold_df)
        assert "Write Diagnostics" not in caplog.text

    def test_n_years_range(self, caplog, threshold_df):
        """n_years range is reported correctly."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(None, threshold_df)
        assert "n_years: 5..14" in caplog.text

    def test_fdc_station_count(self, caplog, fdc_df):
        """FDC station count is correct."""
        with caplog.at_level(logging.DEBUG, logger="src.write_diagnostics"):
            write_diagnostics.diagnose_daily_skill_metrics(fdc_df, None)
        assert "1 stations" in caplog.text
        assert "Models: LR, TFT" in caplog.text
