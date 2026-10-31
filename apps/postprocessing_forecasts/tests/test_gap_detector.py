"""Tests for src/gap_detector.py — find missing ensemble forecasts."""

import logging
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt

from src.gap_detector import (
    PeriodRefreshPlan,
    detect_missing_ensembles,
    detect_missing_monthly_ensembles,
    detect_stale_quantiles,
    plan_period_refresh,
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


class TestDetectMissingMonthlyEnsemblesLeadAware:
    """M1 P5b: under SAPPHIRE_SKILL_LEAD_AWARE the MONTHLY detector keys on

    horizon_value (mirroring the quarterly detector) so a missing ensemble
    at one lead is detected even when another lead is present for the same
    target month. Flag OFF, or horizon_value absent, is unchanged.
    """

    def test_flag_on_detects_per_lead_gap(self, monkeypatch):
        """EM present at lead 0 but MISSING at lead 1 -> lead-1 EM is a gap."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025, 2025],
                "month": [1, 1, 1, 1, 1],
                "code": ["19999"] * 5,
                "model_short": ["LR_Base", "LR_SM", "EM", "LR_Base", "LR_SM"],
                "horizon_value": [0, 0, 0, 1, 1],
                "forecasted_discharge": [100.0, 110.0, 105.0, 90.0, 95.0],
            }
        )
        gaps = detect_missing_monthly_ensembles(combined, lookback_months=3, ensemble_models={"EM"})
        assert len(gaps) == 1
        assert gaps.iloc[0]["model_short"] == "EM"
        assert int(gaps.iloc[0]["horizon_value"]) == 1
        assert int(gaps.iloc[0]["month"]) == 1
        assert str(gaps.iloc[0]["code"]) == "19999"

    def test_flag_off_conflates_leads(self, monkeypatch):
        """Companion: flag OFF keeps today's behavior -- the lead-0 EM

        satisfies the (year, month, code) grain for BOTH leads, so no gap
        is detected (leads conflated).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025, 2025],
                "month": [1, 1, 1, 1, 1],
                "code": ["19999"] * 5,
                "model_short": ["LR_Base", "LR_SM", "EM", "LR_Base", "LR_SM"],
                "horizon_value": [0, 0, 0, 1, 1],
                "forecasted_discharge": [100.0, 110.0, 105.0, 90.0, 95.0],
            }
        )
        gaps = detect_missing_monthly_ensembles(combined, lookback_months=3, ensemble_models={"EM"})
        assert gaps.empty
        # Flag OFF: legacy period-only grain, no horizon_value column emitted.
        assert list(gaps.columns) == ["year", "month", "code", "model_short"]

    def test_flag_off_byte_identity_representative_input(self, monkeypatch):
        """Flag OFF byte-identity: output equals the legacy period-only

        result on a representative multi-lead input (columns, values, order).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025],
                "month": [2, 2, 2, 2],
                "code": ["19999", "19999", "18888", "18888"],
                "model_short": ["LR_Base", "LR_SM", "LR_Base", "EM"],
                "horizon_value": [0, 1, 0, 0],
                "forecasted_discharge": [100.0, 90.0, 80.0, 82.0],
            }
        )
        gaps = detect_missing_monthly_ensembles(combined, lookback_months=3, ensemble_models={"EM"})
        expected = pd.DataFrame(
            {
                "year": [2025],
                "month": [2],
                "code": ["19999"],
                "model_short": ["EM"],
            }
        )
        pd.testing.assert_frame_equal(gaps.reset_index(drop=True), expected, check_dtype=False)

    def test_flag_on_null_lead_row_skipped_with_warning(self, monkeypatch, caplog):
        """Parity with the quarterly detector: a legacy NULL-lead row is

        EXCLUDED from the lead-aware gap universe (no phantom gap) and a
        WARNING naming the skipped count is emitted.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = pd.DataFrame(
            {
                "year": [2025, 2025, 2025],
                "month": [1, 1, 1],
                "code": ["19999", "19999", "19999"],
                "model_short": ["LR_Base", "LR_Base", "EM"],
                "horizon_value": [None, 0, 0],
                "forecasted_discharge": [100.0, 100.0, 100.0],
            }
        )
        with caplog.at_level(logging.WARNING):
            gaps = detect_missing_monthly_ensembles(
                combined, lookback_months=3, ensemble_models={"EM"}
            )
        # hv=0 has its EM; the NULL-lead row is excluded (no phantom gap).
        assert gaps.empty
        assert (gaps["horizon_value"] >= 0).all()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "expected a WARNING for the skipped NULL-lead row"
        assert any("1" in r.getMessage() for r in warnings)


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


# ---------------------------------------------------------------------------
# plan_period_refresh tests
# ---------------------------------------------------------------------------


def _make_payload(
    horizon_type="pentad",
    code="19999",
    model_type="TFT",
    date="2026-10-05",
    target="2026-10-06",
    forecasted_discharge=100.0,
    q05=80.0,
    q25=90.0,
    q75=110.0,
    q95=120.0,
    flag=None,
    composition=None,
    horizon_value=2,
    horizon_in_year=56,
) -> dict:
    # horizon_value=2: target Oct 6 falls in the 2nd pentad of October (days 6-10).
    # horizon_in_year=56: 9 months × 6 pentads + 2 = 56th pentad of the year.
    return {
        "horizon_type": horizon_type,
        "code": code,
        "model_type": model_type,
        "date": date,
        "target": target,
        "forecasted_discharge": forecasted_discharge,
        "q05": q05,
        "q25": q25,
        "q75": q75,
        "q95": q95,
        "flag": flag,
        "composition": composition,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
    }


# Shared reference date / bounds used across most tests
_FD = dt.date(2026, 10, 30)
_SD = dt.date(2026, 1, 1)
_ED = dt.date(2026, 10, 30)

# Canonical key matching _make_payload() defaults
_KEY = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
# Valid existing_read_status for tests that focus on non-status behaviour
_RS = "COMPLETE"


class TestPlanPeriodRefreshMissingAndIdentical:
    """Empty existing → all valid candidates land in missing."""

    def test_empty_existing_gives_missing(self):
        cand = _make_payload()
        _key = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand],
            {_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert isinstance(plan, PeriodRefreshPlan)
        assert len(plan.missing) == 1
        assert not plan.identical
        assert not plan.protected
        assert not plan.authorized_refresh
        assert not plan.excluded
        assert not plan.blocked

    def test_identical_existing_gives_identical(self):
        p = _make_payload()
        _key = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [p],
            [p],
            {_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert len(plan.identical) == 1
        assert not plan.missing

    def test_differing_existing_not_authorized_gives_protected(self):
        existing = _make_payload(forecasted_discharge=100.0)
        candidate = _make_payload(forecasted_discharge=105.0)
        plan = plan_period_refresh(
            [existing],
            [candidate],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert len(plan.protected) == 1
        assert not plan.authorized_refresh

    def test_authorized_key_refreshes_with_preserved_flag(self):
        existing = _make_payload(forecasted_discharge=100.0, flag=4)
        candidate = _make_payload(forecasted_discharge=105.0)
        key = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [existing],
            [candidate],
            {key},
            {key},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert len(plan.authorized_refresh) == 1
        _, merged = plan.authorized_refresh[0]
        # Updated discharge
        assert merged["forecasted_discharge"] == pytest.approx(105.0)
        # Flag preserved from existing
        assert merged["flag"] == 4

    def test_disjoint_codes_independent(self):
        ca = _make_payload(code="19998")
        cb = _make_payload(code="19997")
        ea = _make_payload(code="19998")  # 19998 exists, 19997 does not
        _key_a = ("pentad", "19998", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        _key_b = ("pentad", "19997", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [ea],
            [ca, cb],
            {_key_a, _key_b},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        # 19998 identical, 19997 missing
        assert len(plan.identical) == 1
        assert len(plan.missing) == 1

    def test_exact_duplicate_candidates_collapse(self):
        """Two identical candidate dicts → collapsed to one missing entry."""
        cand = _make_payload()
        _key = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand, dict(cand)],
            {_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1
        assert not plan.blocked

    def test_conflicting_duplicate_candidates_blocked(self):
        c1 = _make_payload(forecasted_discharge=100.0)
        c2 = _make_payload(forecasted_discharge=200.0)
        plan = plan_period_refresh(
            [],
            [c1, c2],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert not plan.missing
        assert len(plan.blocked) == 1


class TestPlanPeriodRefreshExclusions:
    """Candidates that fail validation land in excluded, not dropped silently."""

    def test_lr_model_excluded(self):
        cand = _make_payload(model_type="LR")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert not plan.missing
        assert len(plan.excluded) == 1

    def test_future_issue_date_excluded(self):
        # end_date == forecast_date: scope check passes; candidate issue_date
        # 2026-11-05 > forecast_date 2026-10-30 → per-candidate exclusion
        cand = _make_payload(date="2026-11-05", target="2026-11-06")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.excluded) == 1
        assert not plan.missing
        assert not plan.authorized_refresh

    def test_future_end_date_causes_plan_error(self):
        # end_date 2026-12-31 > forecast_date 2026-10-30 → scope-level plan_error
        cand = _make_payload()
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=dt.date(2026, 12, 31),
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing
        assert not plan.authorized_refresh

    def test_out_of_bounds_issue_date_excluded(self):
        # issue_date 2025-12-05 is before start_date 2026-01-01
        cand = _make_payload(date="2025-12-05", target="2025-12-06")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_non_boundary_issue_date_excluded(self):
        # 2026-10-06 is not a pentad boundary
        cand = _make_payload(date="2026-10-06", target="2026-10-07")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_wrong_target_excluded(self):
        # target should be issue+1 but is issue+2
        cand = _make_payload(date="2026-10-05", target="2026-10-07")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_incomplete_all5_excluded(self):
        cand = _make_payload(q05=None)  # q05 is None → incomplete
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_nan_value_excluded(self):
        import math

        cand = _make_payload(q25=math.nan)
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_inf_value_excluded(self):
        import math

        cand = _make_payload(forecasted_discharge=math.inf)
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1


class TestPlanPeriodRefreshBoundaryCalendar:
    """Calendar boundary and year-edge cases."""

    def test_oct_25_pentad_is_valid_boundary(self):
        # Oct 25 is a pentad boundary (5th pentad of October)
        # target = Oct 26 = issue+1
        # Oct 26 target → P6 of October (days 26-31); horizon_in_year=9*6+6=60
        cand = _make_payload(
            date="2026-10-25", target="2026-10-26", horizon_value=6, horizon_in_year=60
        )
        _key = ("pentad", "19999", "TFT", dt.date(2026, 10, 25), dt.date(2026, 10, 26))
        plan = plan_period_refresh(
            [],
            [cand],
            {_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1
        assert not plan.excluded

    def test_oct_29_not_pentad_boundary_in_2026(self):
        # Oct 31 is last day of month → valid boundary.  Oct 29 is not.
        cand = _make_payload(date="2026-10-29", target="2026-10-30")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.excluded) == 1

    def test_oct_31_pentad_boundary_in_2026(self):
        # Oct 31 is last day of month → valid pentad boundary
        # Nov 1 target → P1 of November (days 1-5); horizon_in_year=10*6+1=61
        cand = _make_payload(
            date="2026-10-31",
            target="2026-11-01",
            forecasted_discharge=50.0,
            q05=40.0,
            q25=45.0,
            q75=55.0,
            q95=60.0,
            horizon_value=1,
            horizon_in_year=61,
        )
        _key = ("pentad", "19999", "TFT", dt.date(2026, 10, 31), dt.date(2026, 11, 1))
        plan = plan_period_refresh(
            [],
            [cand],
            {_key},
            set(),
            start_date=_SD,
            end_date=dt.date(2026, 10, 31),
            forecast_date=dt.date(2026, 10, 31),
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1
        assert not plan.excluded

    def test_year_boundary_dec31_pentad(self):
        # Dec 31 is last-of-month → valid pentad boundary
        # Jan 1 target → P1 of January (days 1-5); horizon_in_year=0*6+1=1
        cand = _make_payload(
            date="2026-12-31",
            target="2027-01-01",
            forecasted_discharge=50.0,
            q05=40.0,
            q25=45.0,
            q75=55.0,
            q95=60.0,
            horizon_value=1,
            horizon_in_year=1,
        )
        _key = ("pentad", "19999", "TFT", dt.date(2026, 12, 31), dt.date(2027, 1, 1))
        plan = plan_period_refresh(
            [],
            [cand],
            {_key},
            set(),
            start_date=dt.date(2026, 12, 1),
            end_date=dt.date(2026, 12, 31),
            forecast_date=dt.date(2026, 12, 31),
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1

    def test_feb29_pentad_boundary_leap_year(self):
        # 2024 is a leap year: Feb 29 is last-of-month → valid pentad boundary
        # Mar 1 target → P1 of March (days 1-5); horizon_in_year=2*6+1=13
        cand = _make_payload(
            date="2024-02-29",
            target="2024-03-01",
            forecasted_discharge=50.0,
            q05=40.0,
            q25=45.0,
            q75=55.0,
            q95=60.0,
            horizon_value=1,
            horizon_in_year=13,
        )
        _key = ("pentad", "19999", "TFT", dt.date(2024, 2, 29), dt.date(2024, 3, 1))
        plan = plan_period_refresh(
            [],
            [cand],
            {_key},
            set(),
            start_date=dt.date(2024, 2, 1),
            end_date=dt.date(2024, 2, 29),
            forecast_date=dt.date(2024, 2, 29),
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1

    def test_decad_boundary_validation(self):
        # Oct 20 is a decad boundary; Oct 21 is not
        # Oct 21 target → D3 of October (days 21-31); horizon_in_year=9*3+3=30
        good = _make_payload(
            horizon_type="decad",
            date="2026-10-20",
            target="2026-10-21",
            horizon_value=3,
            horizon_in_year=30,
        )
        bad = _make_payload(
            horizon_type="decad", code="19998", date="2026-10-21", target="2026-10-22"
        )
        _key_good = ("decad", "19999", "TFT", dt.date(2026, 10, 20), dt.date(2026, 10, 21))
        plan = plan_period_refresh(
            [],
            [good, bad],
            {_key_good},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert len(plan.missing) == 1
        assert len(plan.excluded) == 1


class TestPlanPeriodRefreshProtection:
    """Existing healthy records remain protected unless authorized."""

    def test_finite_existing_no_candidate_protected(self):
        """Existing healthy key with no matching candidate goes to protected."""
        existing = _make_payload(code="19999")
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        # Existing key with no candidate → protected (planner never infers it can be skipped)
        assert not plan.missing
        assert len(plan.protected) == 1
        assert not plan.authorized_refresh

    def test_high_precision_preserved_in_authorized_refresh(self):
        """Float precision from existing is preserved for non-derived fields."""
        existing = _make_payload(forecasted_discharge=123.456789012345, q05=100.111222333, flag=2)
        candidate = _make_payload(
            forecasted_discharge=130.0, q05=105.0, q25=115.0, q75=145.0, q95=155.0
        )
        key = ("pentad", "19999", "TFT", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [existing],
            [candidate],
            {key},
            {key},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert len(plan.authorized_refresh) == 1
        _, merged = plan.authorized_refresh[0]
        # ALL5 updated from candidate
        assert merged["forecasted_discharge"] == pytest.approx(130.0)
        # flag preserved from existing
        assert merged["flag"] == 2


# ---------------------------------------------------------------------------
# plan_period_refresh status-guard tests
# ---------------------------------------------------------------------------


class TestPlanPeriodRefreshStatusGuard:
    """existing_read_status guard: only COMPLETE or CONFIRMED_EMPTY are accepted."""

    def test_empty_string_status_returns_plan_error(self):
        """Default empty string must not be accepted — forces explicit caller proof."""
        plan = plan_period_refresh(
            [],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
        )
        assert plan.plan_error is not None
        assert "existing_read_status" in plan.plan_error

    def test_arbitrary_string_status_returns_plan_error(self):
        plan = plan_period_refresh(
            [],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="OK",
        )
        assert plan.plan_error is not None

    def test_confirmed_empty_with_rows_returns_plan_error(self):
        """CONFIRMED_EMPTY contradicted by non-empty existing_payloads → plan_error."""
        existing = _make_payload()
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None

    def test_complete_with_empty_existing_no_error(self):
        """COMPLETE with zero rows is legitimate (scope queried, nothing stored yet)."""
        plan = plan_period_refresh(
            [],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None

    def test_malformed_existing_payload_returns_plan_error(self):
        """Unparseable existing record → fail closed; cannot infer absence safely."""
        bad = {"horizon_type": "pentad"}  # missing code/date/target/model_type
        plan = plan_period_refresh(
            [bad],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None


# ---------------------------------------------------------------------------
# Recovered regression classes (P1 planner corrections)
# ---------------------------------------------------------------------------


class TestPlanKeyStrictValidation:
    """_plan_key rejects inputs that require rescue beyond explicit aliases."""

    def test_code_with_leading_space_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["code"] = " 19999"
        assert _plan_key(p) is None

    def test_code_with_trailing_space_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["code"] = "19999 "
        assert _plan_key(p) is None

    def test_code_empty_string_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["code"] = ""
        assert _plan_key(p) is None

    def test_code_integer_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["code"] = 19999
        assert _plan_key(p) is None

    def test_code_none_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["code"] = None
        assert _plan_key(p) is None

    def test_horizon_type_lowercase_pentad_accepted(self):
        from src.gap_detector import _plan_key

        key = _plan_key(_make_payload(horizon_type="pentad"))
        assert key is not None
        assert key[0] == "pentad"

    def test_horizon_type_uppercase_pentad_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["horizon_type"] = "PENTAD"
        assert _plan_key(p) is None

    def test_horizon_type_decade_api_form_accepted(self):
        from src.gap_detector import _plan_key

        key = _plan_key(_make_payload(horizon_type="decade"))
        assert key is not None
        assert key[0] == "decad"

    def test_horizon_type_decad_internal_accepted(self):
        from src.gap_detector import _plan_key

        key = _plan_key(_make_payload(horizon_type="decad"))
        assert key is not None
        assert key[0] == "decad"

    def test_horizon_type_unknown_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["horizon_type"] = "daily"
        assert _plan_key(p) is None

    def test_horizon_type_none_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["horizon_type"] = None
        assert _plan_key(p) is None

    def test_model_lowercase_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["model_type"] = "tft"
        assert _plan_key(p) is None

    def test_model_with_whitespace_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["model_type"] = " TFT"
        assert _plan_key(p) is None

    def test_model_em_alias_accepted(self):
        from src.gap_detector import _plan_key

        key = _plan_key(_make_payload(model_type="EM"))
        assert key is not None
        assert key[2] == "ENSEMBLE_MEAN"

    def test_model_em_lowercase_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["model_type"] = "em"
        assert _plan_key(p) is None

    def test_date_longer_string_rejected(self):
        """String with time component beyond 10 chars → rejected; no truncation."""
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = "2026-10-05T00:00:00"
        assert _plan_key(p) is None

    def test_date_integer_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = 20261005
        assert _plan_key(p) is None

    def test_date_tz_aware_datetime_rejected(self):
        import datetime as dt_mod

        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = dt_mod.datetime(2026, 10, 5, tzinfo=dt_mod.UTC)
        assert _plan_key(p) is None

    def test_date_non_midnight_datetime_rejected(self):
        import datetime as dt_mod

        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = dt_mod.datetime(2026, 10, 5, 12, 0, 0)
        assert _plan_key(p) is None

    def test_date_midnight_datetime_accepted(self):
        import datetime as dt_mod

        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = dt_mod.datetime(2026, 10, 5, 0, 0, 0)
        key = _plan_key(p)
        assert key is not None
        assert key[3] == dt_mod.date(2026, 10, 5)

    def test_date_naive_midnight_timestamp_accepted(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = pd.Timestamp("2026-10-05")
        key = _plan_key(p)
        assert key is not None
        assert key[3] == dt.date(2026, 10, 5)

    def test_date_timestamp_with_time_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = pd.Timestamp("2026-10-05 12:00:00")
        assert _plan_key(p) is None

    def test_date_tz_aware_timestamp_rejected(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = pd.Timestamp("2026-10-05", tz="UTC")
        assert _plan_key(p) is None

    def test_date_pure_date_object_accepted(self):
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = dt.date(2026, 10, 5)
        key = _plan_key(p)
        assert key is not None
        assert key[3] == dt.date(2026, 10, 5)

    def test_date_iso_week_date_string_rejected(self):
        """ISO week-date '2026-W41-1' is 10 chars and accepted by fromisoformat
        in Python 3.11+, but its .isoformat() returns '2026-10-05' (not equal);
        the roundtrip guard must reject it."""
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = "2026-W41-1"
        assert _plan_key(p) is None

    def test_date_standard_calendar_string_accepted(self):
        """Standard YYYY-MM-DD string still accepted after roundtrip guard."""
        from src.gap_detector import _plan_key

        p = _make_payload()
        p["date"] = "2026-10-05"
        key = _plan_key(p)
        assert key is not None
        assert key[3] == dt.date(2026, 10, 5)


class TestPlanAll5FiniteStrictValidation:
    """_plan_all5_finite rejects booleans and non-numeric types."""

    def test_bool_true_rejected(self):
        from src.gap_detector import _plan_all5_finite

        assert not _plan_all5_finite(_make_payload(forecasted_discharge=True))

    def test_bool_false_rejected(self):
        from src.gap_detector import _plan_all5_finite

        assert not _plan_all5_finite(_make_payload(q05=False))

    def test_numeric_string_rejected(self):
        from src.gap_detector import _plan_all5_finite

        assert not _plan_all5_finite(_make_payload(q25="100.0"))

    def test_valid_int_accepted(self):
        from src.gap_detector import _plan_all5_finite

        p = _make_payload(forecasted_discharge=100, q05=80, q25=90, q75=110, q95=120)
        assert _plan_all5_finite(p)

    def test_valid_float_accepted(self):
        from src.gap_detector import _plan_all5_finite

        assert _plan_all5_finite(_make_payload())

    def test_nan_rejected(self):
        import math

        from src.gap_detector import _plan_all5_finite

        assert not _plan_all5_finite(_make_payload(q75=math.nan))

    def test_inf_rejected(self):
        import math

        from src.gap_detector import _plan_all5_finite

        assert not _plan_all5_finite(_make_payload(forecasted_discharge=math.inf))


class TestPlanPeriodRefreshReadStatus:
    """plan_period_refresh fails closed when existing_read_status is not valid."""

    def test_missing_status_default_fails(self):
        """Default empty-string status → plan_error; no safe work done."""
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
        )
        assert plan.plan_error is not None
        assert not plan.missing

    def test_unknown_status_fails(self):
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="PARTIAL",
        )
        assert plan.plan_error is not None

    def test_complete_status_passes(self):
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 1

    def test_confirmed_empty_with_no_rows_passes(self):
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.missing) == 1

    def test_confirmed_empty_with_rows_is_contradictory(self):
        plan = plan_period_refresh(
            [_make_payload()],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None


class TestPlanPeriodRefreshAuthorizedKeysAllowlist:
    """authorized_keys is the output allowlist; unauth candidates cannot be written."""

    def test_candidate_not_in_authorized_keys_protected_not_missing(self):
        """Unauth candidate with no existing → protected, not missing."""
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert not plan.missing
        assert len(plan.protected) == 1

    def test_candidate_not_in_authorized_with_diff_existing_is_protected(self):
        existing = _make_payload(forecasted_discharge=100.0)
        candidate = _make_payload(forecasted_discharge=200.0)
        plan = plan_period_refresh(
            [existing],
            [candidate],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert not plan.authorized_refresh
        assert len(plan.protected) >= 1

    def test_authorized_key_missing_from_db_goes_to_missing(self):
        plan = plan_period_refresh(
            [],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert len(plan.missing) == 1
        assert not plan.protected

    def test_explicit_refresh_keys_not_subset_raises_plan_error(self):
        plan = plan_period_refresh(
            [],
            [],
            set(),
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is not None

    def test_authorized_diff_existing_without_explicit_refresh_is_protected(self):
        """Auth candidate, existing differs, but NOT in explicit_refresh → protected."""
        existing = _make_payload(forecasted_discharge=100.0)
        candidate = _make_payload(forecasted_discharge=105.0)
        plan = plan_period_refresh(
            [existing],
            [candidate],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert not plan.authorized_refresh
        assert len(plan.protected) == 1


class TestPlanPeriodRefreshExistingDedupSemantics:
    """Exact existing duplicates collapse; conflicting duplicates block the key."""

    def test_exact_duplicate_existing_collapses(self):
        """Two identical existing records → not conflicted; candidate matches → identical."""
        p = _make_payload()
        plan = plan_period_refresh(
            [p, dict(p)],
            [p],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert len(plan.identical) == 1
        assert not plan.blocked

    def test_conflicting_existing_duplicate_blocks_key(self):
        e1 = _make_payload(forecasted_discharge=100.0)
        e2 = _make_payload(forecasted_discharge=200.0)
        plan = plan_period_refresh(
            [e1, e2],
            [_make_payload()],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert not plan.authorized_refresh
        assert len(plan.blocked) == 1

    def test_conflicting_existing_incomplete_variant_blocks(self):
        """Complete and incomplete variant of same key → conflicting → blocked."""
        e_complete = _make_payload(forecasted_discharge=100.0)
        e_incomplete = _make_payload(forecasted_discharge=100.0, q05=None)
        plan = plan_period_refresh(
            [e_complete, e_incomplete],
            [_make_payload()],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert len(plan.blocked) == 1
        assert not plan.authorized_refresh

    def test_conflicting_existing_without_candidate_goes_to_blocked(self):
        """Conflicting existing with no candidate → still accounted in blocked."""
        e1 = _make_payload(forecasted_discharge=100.0)
        e2 = _make_payload(forecasted_discharge=200.0)
        plan = plan_period_refresh(
            [e1, e2],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert len(plan.blocked) == 1

    def test_malformed_existing_triggers_plan_error(self):
        """Malformed existing record → plan_error; no missing inferred."""
        malformed = _make_payload()
        malformed["code"] = 99999  # int, not str → _plan_key returns None
        plan = plan_period_refresh(
            [_make_payload(), malformed],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is not None
        assert not plan.missing

    def test_malformed_existing_same_outcome_both_orders(self):
        """Malformed existing fails closed regardless of record order."""
        valid = _make_payload()
        malformed = _make_payload()
        malformed["code"] = 99999
        p1 = plan_period_refresh(
            [valid, malformed],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        p2 = plan_period_refresh(
            [malformed, valid],
            [_make_payload()],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert p1.plan_error is not None
        assert p2.plan_error is not None


class TestPlanPeriodRefreshExistingProtected:
    """Existing healthy keys with no surviving candidate go to protected."""

    def test_existing_healthy_no_candidate_goes_to_protected(self):
        plan = plan_period_refresh(
            [_make_payload()],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert len(plan.protected) == 1
        assert not plan.missing

    def test_existing_healthy_candidate_excluded_goes_to_protected(self):
        """Candidate excluded (LR model) → existing key still reported protected."""
        plan = plan_period_refresh(
            [_make_payload()],
            [_make_payload(model_type="LR")],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert len(plan.excluded) == 1
        assert len(plan.protected) == 1

    def test_authorized_refresh_noop_if_merged_equals_existing(self):
        """Authorized refresh; candidate ALL5 matches existing → identical (no write)."""
        p = _make_payload(forecasted_discharge=100.0, flag=7)
        cand = _make_payload(forecasted_discharge=100.0)  # same ALL5; no flag field
        plan = plan_period_refresh(
            [p],
            [cand],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status=_RS,
        )
        assert plan.plan_error is None
        assert len(plan.identical) == 1
        assert not plan.authorized_refresh


# ---------------------------------------------------------------------------
# Finding 5: explicit supported model alias map
# ---------------------------------------------------------------------------


class TestPlanPeriodRefreshFinding5ModelAliases:
    """Explicit model alias map: TiDE/TSMixer accepted; unknown/LT models → plan_error."""

    def test_tide_api_form_produces_missing(self):
        """TiDE (API enum surface form) maps to TIDE; authorized and absent → missing."""
        cand = _make_payload(model_type="TiDE")
        key = ("pentad", "19999", "TIDE", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand],
            {key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert key in plan.missing
        assert not plan.excluded
        assert not plan.protected

    def test_tsmixer_api_form_produces_missing(self):
        """TSMixer (API enum surface form) maps to TSMIXER; authorized and absent → missing."""
        cand = _make_payload(model_type="TSMixer")
        key = ("pentad", "19999", "TSMIXER", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand],
            {key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert key in plan.missing
        assert not plan.excluded
        assert not plan.protected

    def test_tide_internal_form_produces_missing(self):
        """TIDE (internal canonical key) authorized and absent → missing."""
        cand = _make_payload(model_type="TIDE")
        key = ("pentad", "19999", "TIDE", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand],
            {key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert key in plan.missing

    def test_tide_and_tsmixer_both_correct_keys(self):
        """TiDE and TSMixer candidates both authorized → two missing entries."""
        tide_key = ("pentad", "19999", "TIDE", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        tsmixer_key = ("pentad", "19999", "TSMIXER", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [_make_payload(model_type="TiDE"), _make_payload(model_type="TSMixer")],
            {tide_key, tsmixer_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert tide_key in plan.missing
        assert tsmixer_key in plan.missing
        assert len(plan.missing) == 2

    def test_unsupported_model_causes_plan_error(self):
        """Arbitrary uppercase model not in supported map → plan_error."""
        cand = _make_payload(model_type="UNKNOWN_MODEL")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing
        assert not plan.excluded

    def test_day_horizon_causes_plan_error(self):
        """DAY is not in _HT_EXPLICIT → _plan_key returns None → plan_error."""
        cand = _make_payload(horizon_type="DAY")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing

    def test_lt_model_name_causes_plan_error(self):
        """Long-term-only model name not in supported map → plan_error."""
        cand = _make_payload(model_type="LR_BASE")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None

    def test_linear_regression_excluded_not_plan_error(self):
        """LINEAR_REGRESSION maps to 'LR' canonical and is excluded as output."""
        cand = _make_payload(model_type="LINEAR_REGRESSION")
        plan = plan_period_refresh(
            [],
            [cand],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert len(plan.excluded) == 1
        assert not plan.missing

    def test_neural_ensemble_maps_to_ne_key(self):
        """NEURAL_ENSEMBLE maps to canonical NE key."""
        cand = _make_payload(model_type="NEURAL_ENSEMBLE")
        key = ("pentad", "19999", "NE", dt.date(2026, 10, 5), dt.date(2026, 10, 6))
        plan = plan_period_refresh(
            [],
            [cand],
            {key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert key in plan.missing


# ---------------------------------------------------------------------------
# Finding 6: malformed candidate → plan_error, not silent skip
# ---------------------------------------------------------------------------


class TestPlanPeriodRefreshFinding6MalformedCandidate:
    """_plan_key None on any candidate → plan_error; whole plan fails closed."""

    def test_only_malformed_candidate_plan_error(self):
        """Single candidate with int code (unparseable key) → plan_error, not clean no-op."""
        malformed = _make_payload()
        malformed["code"] = 19999  # int, not str → _plan_key returns None
        plan = plan_period_refresh(
            [],
            [malformed],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing
        assert not plan.excluded

    def test_valid_then_malformed_candidate_plan_error(self):
        """Valid candidate followed by malformed → plan_error, not partial success."""
        valid = _make_payload()
        malformed = _make_payload()
        malformed["horizon_type"] = None  # not a str → _plan_key returns None
        plan = plan_period_refresh(
            [],
            [valid, malformed],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing

    def test_malformed_then_valid_same_plan_error(self):
        """Malformed first, valid second → same plan_error (order does not matter)."""
        valid = _make_payload()
        malformed = _make_payload()
        malformed["date"] = 20261005  # int, not date/str → _plan_key returns None
        plan = plan_period_refresh(
            [],
            [malformed, valid],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is not None
        assert not plan.missing


# ---------------------------------------------------------------------------
# Finding 7: resolve all variants per key before classification; disjoint
# ---------------------------------------------------------------------------


class TestPlanPeriodRefreshFinding7DisjointCategories:
    """All variants of a key resolved before classification; output sets disjoint."""

    def test_valid_then_excluded_same_key_becomes_blocked(self):
        """Valid followed by excluded (same key, q05=None) → blocked, not missing+excluded."""
        valid = _make_payload(forecasted_discharge=100.0)
        excluded = _make_payload(q05=None)  # same key, fails _plan_all5_finite
        plan = plan_period_refresh(
            [],
            [valid, excluded],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert not plan.missing
        assert not plan.excluded
        assert _KEY in plan.blocked

    def test_excluded_then_valid_same_key_becomes_blocked(self):
        """Excluded followed by valid (same key) → blocked in both orderings."""
        excluded = _make_payload(q05=None)  # fails _plan_all5_finite
        valid = _make_payload(forecasted_discharge=100.0)
        plan = plan_period_refresh(
            [],
            [excluded, valid],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert not plan.missing
        assert not plan.excluded
        assert _KEY in plan.blocked

    def test_candidate_conflict_with_existing_not_also_protected(self):
        """Candidate conflict → blocked; existing healthy key NOT also in protected."""
        c1 = _make_payload(forecasted_discharge=100.0)
        c2 = _make_payload(forecasted_discharge=200.0)
        existing = _make_payload(forecasted_discharge=150.0)
        plan = plan_period_refresh(
            [existing],
            [c1, c2],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY in plan.blocked
        assert _KEY not in plan.protected  # disjoint: blocked and protected are exclusive

    def test_explicit_refresh_with_conflicting_candidates_not_actionable(self):
        """Key in explicit_refresh_keys but candidate conflict → blocked, no refresh."""
        c1 = _make_payload(forecasted_discharge=100.0)
        c2 = _make_payload(forecasted_discharge=200.0)
        plan = plan_period_refresh(
            [],
            [c1, c2],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert not plan.authorized_refresh
        assert _KEY in plan.blocked

    def test_all_categories_pairwise_disjoint(self):
        """A mixed-input plan has no key appearing in more than one output category."""
        from itertools import combinations

        valid_key = _KEY
        valid_cand = _make_payload()

        conflict_a = _make_payload(code="19998", forecasted_discharge=100.0)
        conflict_b = _make_payload(code="19998", forecasted_discharge=200.0)

        existing_only = _make_payload(code="19997")

        plan = plan_period_refresh(
            [existing_only],
            [valid_cand, conflict_a, conflict_b],
            {valid_key},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None

        cats: list[set] = [
            plan.missing,
            plan.identical,
            {k for k, _ in plan.authorized_refresh},
            plan.protected,
            set(plan.excluded),
            set(plan.blocked),
        ]
        for ca, cb in combinations(cats, 2):
            assert ca.isdisjoint(cb), f"categories not disjoint: {ca & cb}"


# ---------------------------------------------------------------------------
# Finding 8: type-aware comparison in _plan_records_identical
# ---------------------------------------------------------------------------


class TestPlanRecordsIdenticalFinding8TypeAware:
    """_plan_records_identical uses type-aware comparison; no blanket float coercion."""

    def test_composition_01_vs_1_different(self):
        """Composition '01' and '1' are distinct strings; must not compare equal."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(composition="01")
        b = _make_payload(composition="1")
        assert not _plan_records_identical(a, b)

    def test_composition_same_string_identical(self):
        """Identical composition strings → identical."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(composition="01")
        b = _make_payload(composition="01")
        assert _plan_records_identical(a, b)

    def test_flag_bool_true_not_equal_to_int_1(self):
        """flag=True (bool) must not compare equal to flag=1 (int)."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(flag=True)
        b = _make_payload(flag=1)
        assert not _plan_records_identical(a, b)

    def test_flag_equal_ints_identical(self):
        """flag=4, flag=4 (both int) → identical."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(flag=4)
        b = _make_payload(flag=4)
        assert _plan_records_identical(a, b)

    def test_large_integer_horizon_values_distinct(self):
        """horizon_value integers above 2**53 remain distinct without float coercion."""
        from src.gap_detector import _plan_records_identical

        big = 2**53 + 1
        a = _make_payload(horizon_value=big)
        b = _make_payload(horizon_value=big + 1)
        assert not _plan_records_identical(a, b)

    def test_metadata_only_difference_collapses(self):
        """Records differing only in non-writable fields (id, model_type_description) → identical."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload()
        b = dict(a)
        b["id"] = 9999
        b["model_type_description"] = "Temporal Fusion Transformer (TFT)"
        assert _plan_records_identical(a, b)

    def test_planner_composition_01_vs_1_not_collapsed(self):
        """Existing composition='01', candidate='1': planner sees them as different."""
        existing = _make_payload(composition="01", forecasted_discharge=100.0)
        candidate = _make_payload(composition="1", forecasted_discharge=100.0)
        plan = plan_period_refresh(
            [existing],
            [candidate],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        # "01" != "1" and composition is a derived field updated from candidate
        # → merged differs from existing → authorized_refresh
        assert len(plan.authorized_refresh) == 1

    def test_planner_bool_int_flag_not_collapsed(self):
        """Existing flag=True and candidate flag=1 with same ALL5 → not identical."""
        from src.gap_detector import _plan_records_identical

        existing = _make_payload(flag=True)
        candidate = _make_payload(flag=1)
        # With float coercion (old): float(True)==float(1)==1.0 → identical (BUG)
        # With type-aware (new): type(True) is bool != int → not identical (CORRECT)
        assert not _plan_records_identical(existing, candidate)

    def test_non_null_composition_preserved_on_authorized_refresh(self):
        """Non-null composition '01' in existing is preserved when candidate has None."""
        existing = _make_payload(composition="01", forecasted_discharge=100.0)
        candidate = _make_payload(forecasted_discharge=105.0)  # composition=None → not overwritten
        plan = plan_period_refresh(
            [existing],
            [candidate],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert len(plan.authorized_refresh) == 1
        _, merged = plan.authorized_refresh[0]
        assert merged["composition"] == "01"  # preserved from existing

    def test_all5_large_int_precision_not_collapsed(self):
        """ALL5 field with 2**53 vs 2**53+1: must NOT compare equal (no float coercion)."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(forecasted_discharge=2**53)
        b = _make_payload(forecasted_discharge=2**53 + 1)
        # float(2**53) == float(2**53+1) → True (precision loss); direct == → False (correct)
        assert not _plan_records_identical(a, b)

    def test_all5_large_int_precision_both_orderings(self):
        """Precision check holds regardless of which payload is 'a' vs 'b'."""
        from src.gap_detector import _plan_records_identical

        a = _make_payload(forecasted_discharge=2**53 + 1)
        b = _make_payload(forecasted_discharge=2**53)
        assert not _plan_records_identical(a, b)


# ---------------------------------------------------------------------------
# P1 fix: overlapping final dispositions and ALL5 precision
# ---------------------------------------------------------------------------


def _all_cats(plan) -> list[set]:
    return [
        plan.missing,
        plan.identical,
        {k for k, _ in plan.authorized_refresh},
        plan.protected,
        set(plan.excluded),
        set(plan.blocked),
    ]


def _assert_pairwise_disjoint(plan) -> None:
    from itertools import combinations

    cats = _all_cats(plan)
    for ca, cb in combinations(cats, 2):
        assert ca.isdisjoint(cb), f"categories overlap: {ca & cb}"


class TestP1FinalDispositionsDisjoint:
    """Incomplete candidate + existing record must never appear in both excluded and protected."""

    def test_incomplete_candidate_with_existing_disjoint(self):
        """Lone incomplete candidate (q05=None) + existing record: excluded only, not also protected."""
        existing = _make_payload(forecasted_discharge=100.0)
        incomplete = _make_payload(q05=None)
        plan = plan_period_refresh(
            [existing],
            [incomplete],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        _assert_pairwise_disjoint(plan)
        assert _KEY in plan.excluded
        assert _KEY not in plan.protected

    def test_conflicted_existing_with_incomplete_candidate_disjoint(self):
        """Conflicting existing pair + incomplete candidate: blocked only, not also excluded."""
        e1 = _make_payload(forecasted_discharge=100.0)
        e2 = _make_payload(forecasted_discharge=200.0)
        incomplete = _make_payload(q05=None)
        plan = plan_period_refresh(
            [e1, e2],
            [incomplete],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        _assert_pairwise_disjoint(plan)

    def test_no_actionable_key_in_incomplete_plus_existing(self):
        """Incomplete candidate + existing: key is excluded, no actionable disposition."""
        existing = _make_payload(forecasted_discharge=100.0)
        incomplete = _make_payload(q05=None)
        plan = plan_period_refresh(
            [existing],
            [incomplete],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY not in plan.missing
        assert _KEY not in plan.authorized_refresh
        _assert_pairwise_disjoint(plan)

    def test_no_actionable_key_conflicted_existing_incomplete_candidate(self):
        """Conflicted existing + incomplete candidate: key is not actionable."""
        e1 = _make_payload(forecasted_discharge=100.0)
        e2 = _make_payload(forecasted_discharge=200.0)
        incomplete = _make_payload(q05=None)
        plan = plan_period_refresh(
            [e1, e2],
            [incomplete],
            {_KEY},
            {_KEY},
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY not in plan.missing
        assert _KEY not in plan.authorized_refresh
        _assert_pairwise_disjoint(plan)


# ---------------------------------------------------------------------------
# NEWP1: full-record validation for existing and candidate writable fields
# ---------------------------------------------------------------------------


class TestPlanPeriodRefreshNewP1MetadataValidation:
    """NEWP1 additions: all _PLAN_WRITABLE_FIELDS validated for both existing and candidates."""

    def test_existing_null_horizon_value_aborts(self):
        """Existing record with horizon_value=None → plan_error (fail closed)."""
        existing = _make_payload()
        existing["horizon_value"] = None
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None
        assert not plan.missing
        assert not plan.protected

    def test_existing_bool_flag_aborts(self):
        """Existing record with flag=True (bool) → plan_error (bool is not int)."""
        existing = _make_payload(flag=True)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_wrong_horizon_value_aborts(self):
        """Existing record with wrong calendar horizon_value → plan_error."""
        # Oct 6 target → expected horizon_value=2; supply 5 → invalid
        existing = _make_payload(horizon_value=5, horizon_in_year=59)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_inf_all5_aborts(self):
        """Existing record with inf in ALL5 → plan_error."""
        import math

        existing = _make_payload(forecasted_discharge=math.inf)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_bool_all5_aborts(self):
        """Existing record with forecasted_discharge=True (bool) → plan_error."""
        existing = _make_payload(forecasted_discharge=True)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_null_all5_passes(self):
        """Existing record with q05=None (null ALL5) → valid; null is permitted for existing."""
        existing = _make_payload(q05=None)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY in plan.protected

    def test_candidate_wrong_horizon_ints_excluded(self):
        """Candidate with wrong horizon_value/horizon_in_year → excluded, not plan_error."""
        # Oct 6 target → expected horizon_value=2; supply 5 → excluded
        cand = _make_payload(horizon_value=5, horizon_in_year=59)
        plan = plan_period_refresh(
            [],
            [cand],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.excluded
        assert not plan.missing

    def test_candidate_null_horizon_value_excluded(self):
        """Candidate with horizon_value=None → excluded (not plan_error)."""
        cand = _make_payload()
        cand["horizon_value"] = None
        plan = plan_period_refresh(
            [],
            [cand],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.excluded
        assert not plan.missing

    def test_candidate_bool_flag_excluded(self):
        """Candidate with flag=True (bool) → excluded (bool is not int)."""
        cand = _make_payload(flag=True)
        plan = plan_period_refresh(
            [],
            [cand],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.excluded

    def test_candidate_valid_then_meta_invalid_blocked(self):
        """Valid candidate followed by meta-invalid (same key) → blocked, not missing."""
        valid = _make_payload()
        invalid_meta = _make_payload()
        invalid_meta["horizon_value"] = None
        plan = plan_period_refresh(
            [],
            [valid, invalid_meta],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.blocked
        assert not plan.missing

    def test_candidate_meta_invalid_then_valid_blocked(self):
        """Meta-invalid candidate followed by valid (same key) → blocked in both orderings."""
        invalid_meta = _make_payload()
        invalid_meta["horizon_value"] = None
        valid = _make_payload()
        plan = plan_period_refresh(
            [],
            [invalid_meta, valid],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.blocked
        assert not plan.missing

    def test_planner_large_int_all5_conflicting_candidates_blocked_both_orders(self):
        """Same key, 2**53 vs 2**53+1 in forecasted_discharge: conflicting → blocked in both orders."""
        big = 2**53
        c1 = _make_payload(forecasted_discharge=big)
        c2 = _make_payload(forecasted_discharge=big + 1)
        p_forward = plan_period_refresh(
            [],
            [c1, c2],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        p_reverse = plan_period_refresh(
            [],
            [c2, c1],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert p_forward.plan_error is None and _KEY in p_forward.blocked
        assert p_reverse.plan_error is None and _KEY in p_reverse.blocked

    def test_existing_conflicted_with_excluded_candidate_goes_to_blocked(self):
        """Conflicting existing + excluded candidate: key lands in blocked (not excluded)."""
        e1 = _make_payload(forecasted_discharge=100.0)
        e2 = _make_payload(forecasted_discharge=200.0)
        excluded_cand = _make_payload(q05=None)  # fails all5 → candidate_excluded
        plan = plan_period_refresh(
            [e1, e2],
            [excluded_cand],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY in plan.blocked
        assert _KEY not in plan.excluded
        _assert_pairwise_disjoint(plan)

    # --- NEWP1 presence checks ---

    def test_existing_missing_composition_aborts(self):
        """Existing record without 'composition' key → plan_error (key absent, not None)."""
        existing = _make_payload()
        del existing["composition"]
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None
        assert not plan.missing
        assert not plan.protected

    def test_existing_missing_flag_aborts(self):
        """Existing record without 'flag' key → plan_error."""
        existing = _make_payload()
        del existing["flag"]
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_missing_q05_aborts(self):
        """Existing record without 'q05' key → plan_error (ALL5 must be present as keys)."""
        existing = _make_payload()
        del existing["q05"]
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_candidate_missing_composition_excluded(self):
        """Candidate without 'composition' key → excluded (not plan_error)."""
        cand = _make_payload()
        del cand["composition"]
        plan = plan_period_refresh(
            [],
            [cand],
            {_KEY},
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="CONFIRMED_EMPTY",
        )
        assert plan.plan_error is None
        assert _KEY in plan.excluded
        assert not plan.missing

    def test_existing_explicit_null_composition_and_flag_accepted(self):
        """Existing record with composition=None and flag=None (explicit nulls) → valid."""
        existing = _make_payload(composition=None, flag=None)
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is None
        assert _KEY in plan.protected

    def test_existing_wrong_target_aborts(self):
        """Existing record with target != issue+1 day → plan_error."""
        existing = _make_payload(target="2026-10-07")  # issue=Oct5, should be Oct6
        # key will have target=Oct7 which doesn't equal issue+1=Oct6
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None

    def test_existing_non_boundary_issue_date_aborts(self):
        """Existing record with issue_date not a pentad boundary → plan_error."""
        # Oct 3 is not a pentad boundary (valid: 5,10,15,20,25,31)
        existing = _make_payload(
            date="2026-10-03", target="2026-10-04", horizon_value=1, horizon_in_year=55
        )
        plan = plan_period_refresh(
            [existing],
            [],
            set(),
            set(),
            start_date=_SD,
            end_date=_ED,
            forecast_date=_FD,
            existing_read_status="COMPLETE",
        )
        assert plan.plan_error is not None
