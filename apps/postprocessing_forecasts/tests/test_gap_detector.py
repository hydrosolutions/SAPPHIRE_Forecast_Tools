"""Tests for src/gap_detector.py — find missing ensemble forecasts."""

import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.gap_detector import (
    detect_missing_ensembles,
    detect_missing_monthly_ensembles,
    read_combined_forecasts,
)

# ---------------------------------------------------------------------------
# detect_missing_ensembles tests
# ---------------------------------------------------------------------------


class TestDetectMissingEnsembles:
    def test_no_gaps(self):
        """All (date, code) pairs have EM — no gaps detected."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 3),
                "code": ["10001"] * 3,
                "model_short": ["LR", "TFT", "EM"],
            }
        )
        result = detect_missing_ensembles(df, lookback_days=7)
        assert result.empty
        assert list(result.columns) == ["date", "code", "model_short"]

    def test_missing_em(self):
        """One (date, code) pair has LR+TFT but no EM."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10001"
        assert result.iloc[0]["date"] == pd.Timestamp("2024-01-05")
        assert result.iloc[0]["model_short"] == "EM"

    def test_lookback_window(self):
        """Only dates within lookback window are checked."""
        dates = pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-10", "2024-01-10"])
        df = pd.DataFrame(
            {
                "date": dates,
                "code": ["10001"] * 4,
                "model_short": ["LR", "TFT", "LR", "TFT"],
            }
        )
        # lookback=3 from max date 2024-01-10 => cutoff 2024-01-07
        # Only 2024-01-10 is in window
        result = detect_missing_ensembles(df, lookback_days=3)
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-01-10")

    def test_multi_code_gaps(self):
        """Gaps detected independently per code."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 5),
                "code": ["10001", "10001", "10001", "10002", "10002"],
                "model_short": ["LR", "TFT", "EM", "LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10002"

    def test_empty_input(self):
        """Empty input returns empty DataFrame."""
        df = pd.DataFrame(columns=["date", "code", "model_short"])
        result = detect_missing_ensembles(df, lookback_days=7)
        assert result.empty
        assert list(result.columns) == ["date", "code", "model_short"]

    def test_string_dates_converted(self):
        """String dates are properly converted to datetime."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-05", "2024-01-05"],
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        assert result.iloc[0]["code"] == "10001"

    def test_detects_missing_ne(self):
        """Data has EM but no NE; NE gap reported."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 3),
                "code": ["10001"] * 3,
                "model_short": ["LR", "TFT", "EM"],
            }
        )
        result = detect_missing_ensembles(
            df,
            lookback_days=7,
            ensemble_models={"EM", "NE"},
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "NE"

    def test_detects_both_em_and_ne_missing(self):
        """Neither EM nor NE present; both gaps reported."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(
            df,
            lookback_days=7,
            ensemble_models={"EM", "NE"},
        )
        assert len(result) == 2
        assert set(result["model_short"]) == {"EM", "NE"}

    def test_default_checks_em_only(self):
        """Without ensemble_models param, only EM gaps returned."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df, lookback_days=7)
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "EM"


# ---------------------------------------------------------------------------
# read_combined_forecasts tests
# ---------------------------------------------------------------------------


class TestReadCombinedForecasts:
    """Verify gap_detector.read_combined_forecasts delegates to data_reader."""

    def test_delegates_to_data_reader(self):
        """Calls data_reader.read_combined_forecasts with same args."""
        expected = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["LR"],
            }
        )
        with patch(
            "src.data_reader.read_combined_forecasts",
            return_value=expected,
        ) as mock_dr:
            result = read_combined_forecasts("pentad")
            mock_dr.assert_called_once_with("pentad")
            assert len(result) == 1
            assert result["code"].iloc[0] == "10001"

    def test_invalid_horizon_delegates_error(self):
        """ValueError from data_reader propagates through."""
        with pytest.raises(ValueError, match="'pentad' or 'decad'"):
            read_combined_forecasts("weekly")

    def test_delegates_decad(self):
        """Decad horizon type is passed through."""
        expected = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-10"]),
                "code": ["10002"],
                "model_short": ["TFT"],
            }
        )
        with patch(
            "src.data_reader.read_combined_forecasts",
            return_value=expected,
        ) as mock_dr:
            result = read_combined_forecasts("decad")
            mock_dr.assert_called_once_with("decad")
            assert len(result) == 1


# ---------------------------------------------------------------------------
# detect_missing_monthly_ensembles tests
# ---------------------------------------------------------------------------


class TestDetectMissingMonthlyEnsembles:
    def test_no_gaps(self):
        """All (year, month, code) tuples have EM — no gaps."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2024],
                "month": [6, 6, 6],
                "code": ["10001"] * 3,
                "model_short": ["LR", "TFT", "EM"],
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert result.empty
        assert list(result.columns) == [
            "year",
            "month",
            "code",
            "model_short",
        ]

    def test_missing_em(self):
        """One (year, month, code) has LR+TFT but no EM."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024],
                "month": [6, 6],
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10001"
        assert result.iloc[0]["year"] == 2024
        assert result.iloc[0]["month"] == 6
        assert result.iloc[0]["model_short"] == "EM"

    def test_lookback_window(self):
        """Only months within lookback window are checked."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2024, 2024],
                "month": [1, 1, 6, 6],
                "code": ["10001"] * 4,
                "model_short": ["LR", "TFT", "LR", "TFT"],
            }
        )
        # lookback=2 from max (2024, 6) => checks (2024, 6) and (2024, 5)
        # Only (2024, 6) has data and is missing EM
        result = detect_missing_monthly_ensembles(df, lookback_months=2)
        assert len(result) == 1
        assert result.iloc[0]["month"] == 6

    def test_multi_code_gaps(self):
        """Gaps detected independently per code."""
        df = pd.DataFrame(
            {
                "year": [2024] * 5,
                "month": [6] * 5,
                "code": ["10001", "10001", "10001", "10002", "10002"],
                "model_short": ["LR", "TFT", "EM", "LR", "TFT"],
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10002"

    def test_empty_input(self):
        """Empty input returns empty DataFrame."""
        df = pd.DataFrame(columns=["year", "month", "code", "model_short"])
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert result.empty
        assert list(result.columns) == [
            "year",
            "month",
            "code",
            "model_short",
        ]

    def test_year_boundary_lookback(self):
        """Lookback crosses year boundary: Dec 2023 → Oct 2023."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2023, 2023],
                "month": [1, 1, 12, 12],
                "code": ["10001"] * 4,
                "model_short": ["LR", "TFT", "LR", "TFT"],
            }
        )
        # lookback=3 from (2024, 1) => (2024, 1), (2023, 12), (2023, 11)
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        # Both (2024, 1) and (2023, 12) have data, both missing EM
        assert len(result) == 2

    def test_missing_required_columns_returns_empty(self):
        """DataFrame missing required columns returns empty result."""
        df = pd.DataFrame(
            {
                "year": [2024],
                "month": [6],
                # 'code' and 'model_short' are missing
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert result.empty
        assert list(result.columns) == [
            "year",
            "month",
            "code",
            "model_short",
        ]

    def test_non_numeric_year_month_handled(self):
        """String year/month values are coerced to numeric."""
        df = pd.DataFrame(
            {
                "year": ["2024", "2024"],
                "month": ["6", "6"],
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10001"

    def test_detects_missing_skilled_mean(self):
        """Data has EM but not Skilled Mean; gap reported."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2024],
                "month": [6, 6, 6],
                "code": ["10001"] * 3,
                "model_short": ["LR", "TFT", "EM"],
            }
        )
        result = detect_missing_monthly_ensembles(
            df,
            lookback_months=3,
            ensemble_models={"EM", "Skilled Mean", "Naive Mean"},
        )
        assert len(result) == 2
        assert set(result["model_short"]) == {
            "Naive Mean",
            "Skilled Mean",
        }

    def test_all_three_present_no_gaps(self):
        """EM, Skilled Mean, and Naive Mean all present — no gaps."""
        df = pd.DataFrame(
            {
                "year": [2024] * 5,
                "month": [6] * 5,
                "code": ["10001"] * 5,
                "model_short": [
                    "LR",
                    "TFT",
                    "EM",
                    "Skilled Mean",
                    "Naive Mean",
                ],
            }
        )
        result = detect_missing_monthly_ensembles(
            df,
            lookback_months=3,
            ensemble_models={"EM", "Skilled Mean", "Naive Mean"},
        )
        assert result.empty

    def test_monthly_default_checks_em_only(self):
        """Without ensemble_models param, only EM gaps returned."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024],
                "month": [6, 6],
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_monthly_ensembles(df, lookback_months=3)
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "EM"
