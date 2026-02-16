"""Edge case and boundary condition tests for monthly data reading.

Covers: empty/single-row data, NaN handling, date boundaries (leap year,
year transition), value boundaries (zero, large), duplicate handling,
multi-entity scenarios, and data preservation.

Reference: CLAUDE.md edge case test requirements.
"""

import os
import sys
from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

from src.data_reader import (
    read_monthly_observations,
    read_monthly_forecasts,
    _aggregate_daily_to_monthly,
    _normalize_monthly_forecasts,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_daily_runoff(code, start, end, discharge_value=100.0):
    """Create daily runoff DataFrame matching API response structure."""
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame({
        "code": code,
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "discharge_avg": discharge_value,
    })


def _make_daily_runoff_with_values(code, start, end, values):
    """Create daily runoff with specific per-day discharge values."""
    dates = pd.date_range(start, end, freq="D")
    assert len(dates) == len(values), (
        f"Expected {len(dates)} values, got {len(values)}"
    )
    return pd.DataFrame({
        "code": code,
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "discharge_avg": values,
    })


def _make_long_forecast_record(
    code, year, month, model_type="GBT", q50=120.0
):
    """Create a single long forecast record dict."""
    first_day = date(year, month, 1)
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - pd.Timedelta(days=1)
    return {
        "horizon_type": "month",
        "horizon_value": month,
        "code": code,
        "date": str(date(year, month, 1)),
        "model_type": model_type,
        "valid_from": str(first_day),
        "valid_to": str(last_day),
        "flag": 0,
        "composition": "",
        "q": q50,
        "q_obs": None,
        "q_xgb": None,
        "q_lgbm": None,
        "q_catboost": None,
        "q_loc": None,
        "q05": q50 * 0.7 if q50 is not None else None,
        "q10": q50 * 0.75 if q50 is not None else None,
        "q25": q50 * 0.85 if q50 is not None else None,
        "q50": q50,
        "q75": q50 * 1.15 if q50 is not None else None,
        "q90": q50 * 1.25 if q50 is not None else None,
        "q95": q50 * 1.3 if q50 is not None else None,
        "id": 1,
        "model_type_description": model_type,
    }


# ===================================================================
# Edge cases: _aggregate_daily_to_monthly
# ===================================================================


class TestAggregateEmptyData:
    """Empty and single-row inputs."""

    def test_empty_dataframe(self):
        """Empty input -> empty output with correct columns."""
        df = pd.DataFrame(columns=["code", "date", "discharge_avg"])
        result = _aggregate_daily_to_monthly(df)
        assert result.empty
        expected_cols = {
            "code", "year", "month", "month_in_year",
            "discharge_avg", "delta",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_single_day_below_50pct(self):
        """One day in a 31-day month (3.2%) -> excluded by coverage filter."""
        df = _make_daily_runoff("15013", "2023-01-15", "2023-01-15", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert result.empty

    def test_single_day_in_february_below_50pct(self):
        """One day in a 28-day month (3.6%) -> excluded."""
        df = _make_daily_runoff("15013", "2023-02-14", "2023-02-14", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert result.empty


class TestAggregateNanHandling:
    """NaN and missing value handling."""

    def test_all_nan_discharge_excluded(self):
        """All NaN discharge values -> count=0, below 50% threshold."""
        dates = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        df = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "discharge_avg": [np.nan] * 31,
        })
        result = _aggregate_daily_to_monthly(df)
        assert result.empty

    def test_mixed_nan_and_valid_above_threshold(self):
        """16 valid + 15 NaN in January (51.6%) -> included.

        Mean should be computed from the 16 valid values only.
        """
        dates = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        values = [100.0] * 16 + [np.nan] * 15
        df = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "discharge_avg": values,
        })
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(100.0)

    def test_mixed_nan_below_threshold_excluded(self):
        """14 valid + 17 NaN in January (45.2%) -> excluded."""
        dates = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        values = [100.0] * 14 + [np.nan] * 17
        df = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "discharge_avg": values,
        })
        result = _aggregate_daily_to_monthly(df)
        assert result.empty


class TestAggregateDateBoundaries:
    """Date boundary conditions."""

    def test_leap_year_feb29(self):
        """February 2024 (leap year) has 29 days. Full month -> included."""
        df = _make_daily_runoff("15013", "2024-02-01", "2024-02-29", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["month"] == 2
        assert result.iloc[0]["year"] == 2024

    def test_leap_year_50pct_threshold(self):
        """Feb 2024 has 29 days. 15 days = 51.7% -> included.
        14 days = 48.3% -> excluded.
        """
        # 15 days: included
        df_ok = _make_daily_runoff(
            "15013", "2024-02-01", "2024-02-15", 100.0
        )
        result_ok = _aggregate_daily_to_monthly(df_ok)
        assert len(result_ok) == 1

        # 14 days: excluded
        df_bad = _make_daily_runoff(
            "15013", "2024-02-01", "2024-02-14", 100.0
        )
        result_bad = _aggregate_daily_to_monthly(df_bad)
        assert result_bad.empty

    def test_dec_jan_year_boundary(self):
        """December 2022 and January 2023 are separate months."""
        df = pd.concat([
            _make_daily_runoff("15013", "2022-12-01", "2022-12-31", 50.0),
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 70.0),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 2
        dec = result[(result["year"] == 2022) & (result["month"] == 12)]
        jan = result[(result["year"] == 2023) & (result["month"] == 1)]
        assert dec.iloc[0]["discharge_avg"] == pytest.approx(50.0)
        assert jan.iloc[0]["discharge_avg"] == pytest.approx(70.0)

    def test_month_boundary_not_mixed(self):
        """Days at month boundary belong to correct month.

        March 31 -> month 3, April 1 -> month 4.
        """
        df = pd.concat([
            _make_daily_runoff("15013", "2023-03-01", "2023-03-31", 80.0),
            _make_daily_runoff("15013", "2023-04-01", "2023-04-30", 120.0),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        mar = result[result["month"] == 3]
        apr = result[result["month"] == 4]
        assert mar.iloc[0]["discharge_avg"] == pytest.approx(80.0)
        assert apr.iloc[0]["discharge_avg"] == pytest.approx(120.0)


class TestAggregateValueBoundaries:
    """Discharge value edge cases."""

    def test_zero_discharge(self):
        """Zero discharge is a valid value (dry river), not missing."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 0.0)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(0.0)

    def test_very_small_positive(self):
        """Very small positive discharge (0.001 m3/s) is preserved."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 0.001)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(0.001)

    def test_very_large_discharge(self):
        """Very large discharge (10000+ m3/s) is preserved."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 15000.0)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(15000.0)

    def test_varying_discharge_computes_mean(self):
        """Varying daily values produce correct monthly mean."""
        dates = pd.date_range("2023-06-01", "2023-06-30", freq="D")
        # 30 days with values 1..30; mean = 15.5
        values = list(range(1, 31))
        df = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "discharge_avg": [float(v) for v in values],
        })
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(15.5)


class TestAggregateDuplicates:
    """Duplicate date-station combinations."""

    def test_duplicate_dates_included_in_count_and_mean(self):
        """Duplicate dates for same station are treated as separate records.

        This matches pandas groupby behavior — duplicates inflate the count
        and contribute to the mean.
        """
        dates = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        df1 = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "discharge_avg": 100.0,
        })
        # Add one duplicate day with a different value
        dup = pd.DataFrame({
            "code": ["15013"],
            "date": ["2023-01-15"],
            "discharge_avg": [200.0],
        })
        df = pd.concat([df1, dup], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 1
        # 31 days of 100.0 + 1 day of 200.0 = 3300.0 / 32 = 103.125
        assert result.iloc[0]["discharge_avg"] == pytest.approx(
            3300.0 / 32.0
        )


class TestAggregateMultiEntity:
    """Multi-station scenarios."""

    def test_single_station_many_months(self):
        """One station with 12 months of data."""
        frames = []
        for m in range(1, 13):
            start = f"2023-{m:02d}-01"
            if m == 12:
                end = "2023-12-31"
            else:
                end_date = date(2023, m + 1, 1) - pd.Timedelta(days=1)
                end = str(end_date)
            frames.append(
                _make_daily_runoff("15013", start, end, float(m * 10))
            )
        df = pd.concat(frames, ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 12
        assert set(result["month"]) == set(range(1, 13))

    def test_many_stations_single_month(self):
        """Multiple stations in the same month -> one row per station."""
        codes = ["15013", "15020", "15030", "15040"]
        frames = []
        for i, code in enumerate(codes):
            frames.append(
                _make_daily_runoff(
                    code, "2023-06-01", "2023-06-30", float((i + 1) * 50)
                )
            )
        df = pd.concat(frames, ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 4
        assert set(result["code"]) == set(codes)
        # Check values are station-specific
        for i, code in enumerate(codes):
            row = result[result["code"] == code]
            assert row.iloc[0]["discharge_avg"] == pytest.approx(
                float((i + 1) * 50)
            )


class TestAggregateDelta:
    """Delta computation edge cases."""

    def test_delta_with_two_years(self):
        """Two years of data -> std is population-based, delta > 0."""
        df = pd.concat([
            _make_daily_runoff("15013", "2022-06-01", "2022-06-30", 100.0),
            _make_daily_runoff("15013", "2023-06-01", "2023-06-30", 200.0),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 2
        # std([100, 200]) with ddof=1 = 70.71...
        expected_delta = 0.674 * np.std([100.0, 200.0], ddof=1)
        for _, row in result.iterrows():
            assert row["delta"] == pytest.approx(expected_delta, rel=1e-3)

    def test_delta_identical_years_is_zero(self):
        """Multiple years with identical discharge -> std=0, delta=0."""
        df = pd.concat([
            _make_daily_runoff("15013", "2021-03-01", "2021-03-31", 100.0),
            _make_daily_runoff("15013", "2022-03-01", "2022-03-31", 100.0),
            _make_daily_runoff("15013", "2023-03-01", "2023-03-31", 100.0),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        assert len(result) == 3
        for _, row in result.iterrows():
            assert row["delta"] == pytest.approx(0.0)

    def test_delta_differs_by_month(self):
        """Different months for the same station get different deltas."""
        df = pd.concat([
            # Jan: 100, 200 -> std = 70.71
            _make_daily_runoff("15013", "2022-01-01", "2022-01-31", 100.0),
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 200.0),
            # Feb: 50, 50 -> std = 0
            _make_daily_runoff("15013", "2022-02-01", "2022-02-28", 50.0),
            _make_daily_runoff("15013", "2023-02-01", "2023-02-28", 50.0),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(df)
        jan_rows = result[result["month"] == 1]
        feb_rows = result[result["month"] == 2]
        assert jan_rows.iloc[0]["delta"] > 0
        assert feb_rows.iloc[0]["delta"] == pytest.approx(0.0)


class TestAggregateDataPreservation:
    """Schema and data preservation after aggregation."""

    def test_code_stays_string(self):
        """Code column remains string type after aggregation."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert result["code"].dtype == object

    def test_numeric_code_stays_string(self):
        """Numeric station codes are preserved as strings."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert result.iloc[0]["code"] == "15013"

    def test_output_has_no_intermediate_columns(self):
        """Intermediate columns (non_missing_days, days_in_month) are dropped."""
        df = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)
        result = _aggregate_daily_to_monthly(df)
        assert "non_missing_days" not in result.columns
        assert "days_in_month" not in result.columns


# ===================================================================
# Edge cases: _normalize_monthly_forecasts
# ===================================================================


class TestNormalizeForecastsEdgeCases:
    """Edge cases for monthly forecast normalization."""

    def test_all_nan_quantiles(self):
        """All quantile columns are NaN -> preserved as NaN, not dropped."""
        record = _make_long_forecast_record(
            "15013", 2023, 1, model_type="GBT", q50=None
        )
        # Override all quantile columns to NaN
        for col in ["q", "q_obs", "q05", "q10", "q25", "q50",
                     "q75", "q90", "q95"]:
            record[col] = None
        df = pd.DataFrame([record])
        result = _normalize_monthly_forecasts(df)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["q50"])

    def test_missing_model_type_column(self):
        """If model_type column is absent, model_short is not created."""
        record = _make_long_forecast_record("15013", 2023, 1)
        df = pd.DataFrame([record])
        df = df.drop(columns=["model_type"])
        result = _normalize_monthly_forecasts(df)
        assert "model_short" not in result.columns

    def test_numeric_code_cleaned(self):
        """Float station code (15013.0) is cleaned to '15013'."""
        record = _make_long_forecast_record("15013", 2023, 1)
        df = pd.DataFrame([record])
        df["code"] = 15013.0  # simulate float from JSON parsing
        result = _normalize_monthly_forecasts(df)
        assert result.iloc[0]["code"] == "15013"

    def test_single_record(self):
        """Single forecast record normalizes correctly."""
        record = _make_long_forecast_record(
            "15013", 2023, 7, model_type="MC_ALD", q50=85.0
        )
        df = pd.DataFrame([record])
        result = _normalize_monthly_forecasts(df)
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "MC_ALD"
        assert result.iloc[0]["month"] == 7
        assert result.iloc[0]["year"] == 2023
        assert result.iloc[0]["q50"] == pytest.approx(85.0)

    def test_all_lt_model_types_pass_through(self):
        """All long-term model types are preserved as model_short."""
        lt_models = [
            "LR_Base", "LR_SM", "LR_SM_DT", "LR_SM_ROF",
            "SM_GBT", "SM_GBT_LR", "SM_GBT_Norm",
            "MC_ALD", "GBT",
        ]
        records = [
            _make_long_forecast_record("15013", 2023, 1, m, 100.0)
            for m in lt_models
        ]
        df = pd.DataFrame(records)
        result = _normalize_monthly_forecasts(df)
        assert set(result["model_short"]) == set(lt_models)

    def test_december_month_extraction(self):
        """Month 12 (December) extracted correctly from valid_from."""
        record = _make_long_forecast_record("15013", 2023, 12)
        df = pd.DataFrame([record])
        result = _normalize_monthly_forecasts(df)
        assert result.iloc[0]["month"] == 12
        assert result.iloc[0]["year"] == 2023


# ===================================================================
# Edge cases: read_monthly_observations (end-to-end with mock API)
# ===================================================================


class TestReadMonthlyObservationsEdgeCases:
    """End-to-end edge cases for read_monthly_observations."""

    def test_all_months_below_threshold_returns_empty(self):
        """Every month has < 50% coverage -> empty result."""
        # One day per month for 12 months
        frames = [
            _make_daily_runoff("15013", f"2023-{m:02d}-15", f"2023-{m:02d}-15")
            for m in range(1, 13)
        ]
        daily = pd.concat(frames, ignore_index=True)
        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)
        assert result.empty

    def test_some_months_pass_some_fail_threshold(self):
        """Mix of months above and below 50% threshold."""
        daily = pd.concat([
            # Jan: 31 days (100%) -> pass
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0),
            # Feb: 5 days (17.9%) -> fail
            _make_daily_runoff("15013", "2023-02-01", "2023-02-05", 100.0),
            # Mar: 31 days (100%) -> pass
            _make_daily_runoff("15013", "2023-03-01", "2023-03-31", 100.0),
        ], ignore_index=True)
        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)
        assert len(result) == 2
        assert set(result["month"]) == {1, 3}

    def test_multi_year_span(self):
        """Data spanning 3 years -> rows for each year-month combination."""
        frames = []
        for year in [2021, 2022, 2023]:
            frames.append(
                _make_daily_runoff(
                    "15013", f"{year}-06-01", f"{year}-06-30", float(year)
                )
            )
        daily = pd.concat(frames, ignore_index=True)
        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2021, 2023)
        assert len(result) == 3
        assert set(result["year"]) == {2021, 2022, 2023}


class TestReadMonthlyForecastsEdgeCases:
    """End-to-end edge cases for read_monthly_forecasts."""

    def test_no_model_type_in_response(self):
        """API response missing model_type -> model_short absent."""
        record = _make_long_forecast_record("15013", 2023, 1)
        df = pd.DataFrame([record])
        df = df.drop(columns=["model_type"])
        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = df
            result = read_monthly_forecasts(["15013"], 2023, 2023)
        assert "model_short" not in result.columns

    def test_single_station_all_12_months(self):
        """One station with forecasts for all 12 months."""
        records = [
            _make_long_forecast_record("15013", 2023, m, "GBT", float(m * 10))
            for m in range(1, 13)
        ]
        df = pd.DataFrame(records)
        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = df
            result = read_monthly_forecasts(["15013"], 2023, 2023)
        assert len(result) == 12
        assert set(result["month"]) == set(range(1, 13))

    def test_multiple_stations_same_model(self):
        """Multiple stations with same model type."""
        records = [
            _make_long_forecast_record(code, 2023, 6, "GBT", 100.0)
            for code in ["15013", "15020", "15030"]
        ]
        df = pd.DataFrame(records)
        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = df
            result = read_monthly_forecasts(
                ["15013", "15020", "15030"], 2023, 2023
            )
        assert len(result) == 3
        assert set(result["code"]) == {"15013", "15020", "15030"}
