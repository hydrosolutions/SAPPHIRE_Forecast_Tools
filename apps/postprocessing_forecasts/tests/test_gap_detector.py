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
    detect_stale_quantiles,
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
        result = detect_missing_ensembles(df)
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
        result = detect_missing_ensembles(df)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10001"
        assert result.iloc[0]["date"] == pd.Timestamp("2024-01-05")
        assert result.iloc[0]["model_short"] == "EM"

    def test_lookback_window(self):
        """13-month default window caps how far back we scan."""
        # Data older than 13 months from max date is excluded
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-01", "2023-01-01", "2024-06-01", "2024-06-01"]),
                "code": ["10001"] * 4,
                "model_short": ["LR", "TFT", "LR", "TFT"],
            }
        )
        # Default 13 months from 2024-06-01 => cutoff ~2023-05-01
        # 2023-01-01 is outside => only 2024-06-01 gap detected
        result = detect_missing_ensembles(df)
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-06-01")

    def test_multi_code_gaps(self):
        """Gaps detected independently per code."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 5),
                "code": ["10001", "10001", "10001", "10002", "10002"],
                "model_short": ["LR", "TFT", "EM", "LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10002"

    def test_empty_input(self):
        """Empty input returns empty DataFrame."""
        df = pd.DataFrame(columns=["date", "code", "model_short"])
        result = detect_missing_ensembles(df)
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
        result = detect_missing_ensembles(df)
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
        result = detect_missing_ensembles(df)
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "EM"

    def test_13_month_cap(self):
        """Gaps older than 13 months from max date are excluded."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-01",
                        "2024-03-05",
                        "2024-03-05",
                    ]
                ),
                "code": ["10001"] * 4,
                "model_short": ["LR", "TFT", "LR", "TFT"],
            }
        )
        # Max = 2024-03-05, cutoff = 2023-02-05 => 2023-01-01 is outside
        result = detect_missing_ensembles(df, max_lookback_months=13)
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-03-05")

    def test_finds_gaps_across_months(self):
        """Multi-month span: gaps at different dates all detected."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2024-01-05",
                        "2024-01-05",
                        "2024-03-10",
                        "2024-03-10",
                        "2024-06-15",
                        "2024-06-15",
                        "2024-06-15",
                    ]
                ),
                "code": ["10001"] * 7,
                "model_short": ["LR", "TFT", "LR", "TFT", "LR", "TFT", "EM"],
            }
        )
        result = detect_missing_ensembles(df)
        # 2024-01-05 and 2024-03-10 are missing EM, 2024-06-15 has it
        assert len(result) == 2
        gap_dates = set(result["date"])
        assert pd.Timestamp("2024-01-05") in gap_dates
        assert pd.Timestamp("2024-03-10") in gap_dates

    def test_blind_spot_modelled_only(self):
        """Modelled data exists but combined doesn't — gap detected."""
        combined = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-06-15"] * 3),
                "code": ["10001"] * 3,
                "model_short": ["LR", "TFT", "EM"],
            }
        )
        modelled = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-06-10", "2024-06-10"]),
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(
            combined,
            modelled_forecasts=modelled,
        )
        # 2024-06-10 exists in modelled but not in combined => EM gap
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-06-10")
        assert result.iloc[0]["code"] == "10001"

    def test_year_boundary(self):
        """Dec→Jan gap detection across year boundary."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2023-12-25",
                        "2023-12-25",
                        "2024-01-05",
                        "2024-01-05",
                        "2024-01-05",
                    ]
                ),
                "code": ["10001"] * 5,
                "model_short": ["LR", "TFT", "LR", "TFT", "EM"],
            }
        )
        result = detect_missing_ensembles(df)
        # 2023-12-25 is within 13 months of 2024-01-05 => EM gap detected
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2023-12-25")

    def test_empty_combined_with_modelled(self):
        """Combined is empty, modelled has data — gaps detected."""
        combined = pd.DataFrame(columns=["date", "code", "model_short"])
        modelled = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-06-10", "2024-06-10"]),
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(
            combined,
            modelled_forecasts=modelled,
        )
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-06-10")

    def test_both_empty(self):
        """Both combined and modelled empty returns empty."""
        combined = pd.DataFrame(columns=["date", "code", "model_short"])
        modelled = pd.DataFrame(columns=["date", "code", "model_short"])
        result = detect_missing_ensembles(
            combined,
            modelled_forecasts=modelled,
        )
        assert result.empty

    def test_decad_horizon(self):
        """Works for decad horizon_type (parameter accepted)."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-10"] * 2),
                "code": ["10001"] * 2,
                "model_short": ["LR", "TFT"],
            }
        )
        result = detect_missing_ensembles(df, horizon_type="decad")
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


# ---------------------------------------------------------------------------
# detect_stale_quantiles tests (PP-021)
# ---------------------------------------------------------------------------


class TestDetectStaleQuantiles:
    """Tests for detect_stale_quantiles — find records with NULL q05."""

    def _make_df(self, rows):
        """Build a combined_forecasts-like DataFrame from row dicts."""
        return pd.DataFrame(rows)

    def test_empty_input_returns_empty(self):
        """Empty combined_forecasts returns empty DataFrame."""
        df = pd.DataFrame(columns=["date", "code", "model_short", "forecasted_discharge", "q05"])
        result = detect_stale_quantiles(df)
        assert result.empty
        assert list(result.columns) == ["date", "code", "model_short"]

    def test_no_q05_column_returns_empty(self):
        """DataFrame without a q05 column returns empty (nothing to check)."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["TFT"],
                "forecasted_discharge": [100.0],
            }
        )
        result = detect_stale_quantiles(df)
        assert result.empty

    def test_all_have_quantiles_returns_empty(self):
        """All rows have q05 filled — no stale records."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-05"]),
                "code": ["10001", "10001"],
                "model_short": ["TFT", "LR"],
                "forecasted_discharge": [100.0, 90.0],
                "q05": [80.0, 70.0],
            }
        )
        result = detect_stale_quantiles(df)
        assert result.empty

    def test_stale_row_within_lookback_returned(self):
        """Row with q05=NULL within lookback window is returned."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["TFT"],
                "forecasted_discharge": [100.0],
                "q05": [float("nan")],
            }
        )
        result = detect_stale_quantiles(df, max_lookback_months=13)
        assert len(result) == 1
        assert result.iloc[0]["code"] == "10001"
        assert result.iloc[0]["model_short"] == "TFT"

    def test_stale_row_outside_lookback_excluded(self):
        """Row with q05=NULL outside lookback window is excluded."""
        # Two dates: recent (within window) and old (outside)
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2022-01-05", "2024-06-05"]),
                "code": ["10001", "10001"],
                "model_short": ["TFT", "TFT"],
                "forecasted_discharge": [100.0, 110.0],
                "q05": [float("nan"), float("nan")],
            }
        )
        # lookback=3 months from max (2024-06-05) => cutoff ~2024-03-05
        # 2022-01-05 is outside => only 2024-06-05 returned
        result = detect_stale_quantiles(df, max_lookback_months=3)
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-06-05")

    def test_ensemble_mean_excluded(self):
        """EM rows with q05=NULL are NOT returned (handled separately)."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-05"]),
                "code": ["10001", "10001"],
                "model_short": ["EM", "TFT"],
                "forecasted_discharge": [100.0, 110.0],
                "q05": [float("nan"), float("nan")],
            }
        )
        result = detect_stale_quantiles(df)
        # Only TFT row returned, EM excluded
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "TFT"

    def test_mixed_stale_and_good_returns_only_stale(self):
        """Mix of stale and good records: only stale rows returned."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 3),
                "code": ["10001"] * 3,
                "model_short": ["TFT", "LR", "NE"],
                "forecasted_discharge": [100.0, 90.0, 95.0],
                "q05": [float("nan"), 70.0, float("nan")],  # TFT + NE stale
            }
        )
        result = detect_stale_quantiles(df)
        assert len(result) == 2
        assert set(result["model_short"]) == {"TFT", "NE"}

    def test_no_forecasted_discharge_excluded(self):
        """Row with NULL forecasted_discharge is not counted as stale."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"]),
                "code": ["10001"],
                "model_short": ["TFT"],
                "forecasted_discharge": [float("nan")],
                "q05": [float("nan")],
            }
        )
        result = detect_stale_quantiles(df)
        assert result.empty

    def test_string_dates_converted(self):
        """String dates are converted to datetime."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-05"],
                "code": ["10001"],
                "model_short": ["TFT"],
                "forecasted_discharge": [100.0],
                "q05": [float("nan")],
            }
        )
        result = detect_stale_quantiles(df)
        assert len(result) == 1
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_multi_code_multi_model_all_stale(self):
        """Multiple codes and models — all stale ones returned."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05"] * 4),
                "code": ["10001", "10001", "10002", "10002"],
                "model_short": ["TFT", "LR", "TFT", "LR"],
                "forecasted_discharge": [100.0, 90.0, 200.0, 180.0],
                "q05": [float("nan"), 70.0, float("nan"), 150.0],
            }
        )
        result = detect_stale_quantiles(df)
        assert len(result) == 2
        assert set(zip(result["code"], result["model_short"], strict=False)) == {
            ("10001", "TFT"),
            ("10002", "TFT"),
        }
