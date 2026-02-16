"""Tests for monthly observation and forecast reading in data_reader.py.

Step 3 of Phase 4a: Monthly skill metrics.
TDD — these tests are written before the implementation.
"""

import os
import sys
from datetime import date
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# Ensure the postprocessing_forecasts package is importable
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

from src.data_reader import (
    read_monthly_observations,
    read_monthly_forecasts,
)


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------

def _make_daily_runoff(code, start, end, discharge_value=100.0):
    """Create a DataFrame mimicking SapphirePreprocessingClient.read_runoff().

    Returns DataFrame with columns: code, date, discharge
    (matching the API response structure after selecting relevant columns).
    """
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame({
        "code": code,
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "discharge_avg": discharge_value,
    })


def _make_long_forecast_records(
    code, year, months, model_type="GBT", q50=120.0
):
    """Create a DataFrame mimicking read_long_term_forecasts() response."""
    records = []
    for m in months:
        first_day = date(year, m, 1)
        if m == 12:
            last_day = date(year, 12, 31)
        else:
            last_day = date(year, m + 1, 1).replace(day=1) - pd.Timedelta(
                days=1
            )
        records.append({
            "horizon_type": "month",
            "horizon_value": m,
            "code": code,
            "date": str(date(year, m, 1)),
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
            "q05": q50 * 0.7,
            "q10": q50 * 0.75,
            "q25": q50 * 0.85,
            "q50": q50,
            "q75": q50 * 1.15,
            "q90": q50 * 1.25,
            "q95": q50 * 1.3,
            "id": 1,
            "model_type_description": model_type,
        })
    return pd.DataFrame(records)


# ===================================================================
# read_monthly_observations
# ===================================================================


class TestReadMonthlyObservations:
    """Tests for read_monthly_observations()."""

    def test_happy_path_aggregates_daily_to_monthly(self):
        """Daily runoff for one station, one full month -> one monthly row."""
        daily = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert len(result) == 1
        assert result.iloc[0]["code"] == "15013"
        assert result.iloc[0]["year"] == 2023
        assert result.iloc[0]["month"] == 1
        assert result.iloc[0]["discharge_avg"] == pytest.approx(100.0)

    def test_multiple_months_multiple_stations(self):
        """Two stations, two months each -> 4 rows."""
        daily_a = pd.concat([
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0),
            _make_daily_runoff("15013", "2023-02-01", "2023-02-28", 150.0),
        ])
        daily_b = pd.concat([
            _make_daily_runoff("15020", "2023-01-01", "2023-01-31", 200.0),
            _make_daily_runoff("15020", "2023-02-01", "2023-02-28", 250.0),
        ])
        daily = pd.concat([daily_a, daily_b], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(
                ["15013", "15020"], 2023, 2023
            )

        assert len(result) == 4
        codes = result["code"].unique()
        assert set(codes) == {"15013", "15020"}

    def test_50pct_coverage_filter(self):
        """Month with < 50% non-missing days is excluded."""
        # January has 31 days. Create only 10 days (< 50% = 15.5).
        daily = _make_daily_runoff("15013", "2023-01-01", "2023-01-10", 100.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert len(result) == 0

    def test_exactly_50pct_coverage_included(self):
        """Month with exactly 50% non-missing days is included.

        February 2023 has 28 days. 14 days = 50%.
        """
        daily = _make_daily_runoff("15013", "2023-02-01", "2023-02-14", 100.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert len(result) == 1
        assert result.iloc[0]["month"] == 2

    def test_delta_column_computed(self):
        """Delta = 0.674 * std(discharge_avg) across years for same station+month."""
        # Three Januaries with discharge_avg = 100, 120, 140
        # std = 20.0, delta = 0.674 * 20.0 = 13.48
        daily = pd.concat([
            _make_daily_runoff("15013", "2021-01-01", "2021-01-31", 100.0),
            _make_daily_runoff("15013", "2022-01-01", "2022-01-31", 120.0),
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 140.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2021, 2023)

        assert len(result) == 3
        # All three rows should have the same delta for Jan
        expected_delta = 0.674 * 20.0
        for _, row in result.iterrows():
            assert row["delta"] == pytest.approx(expected_delta, rel=1e-3)

    def test_delta_single_year_is_zero(self):
        """With only one year of data, std=NaN -> delta=0."""
        daily = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert len(result) == 1
        assert result.iloc[0]["delta"] == 0.0

    def test_empty_api_response(self):
        """Empty API response -> empty DataFrame with correct columns."""
        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = pd.DataFrame()
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_output_columns(self):
        """Verify output has the expected column set."""
        daily = _make_daily_runoff("15013", "2023-06-01", "2023-06-30", 80.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        expected_cols = {
            "code", "year", "month", "month_in_year",
            "discharge_avg", "delta",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_month_in_year_equals_month(self):
        """month_in_year should equal month (1-12)."""
        daily = pd.concat([
            _make_daily_runoff("15013", "2023-03-01", "2023-03-31", 80.0),
            _make_daily_runoff("15013", "2023-07-01", "2023-07-31", 120.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        for _, row in result.iterrows():
            assert row["month_in_year"] == row["month"]

    def test_year_boundary(self):
        """Data spanning Dec -> Jan across year boundary is aggregated correctly."""
        daily = pd.concat([
            _make_daily_runoff("15013", "2022-12-01", "2022-12-31", 50.0),
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 70.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.return_value = daily
            result = read_monthly_observations(["15013"], 2022, 2023)

        assert len(result) == 2
        dec_row = result[
            (result["year"] == 2022) & (result["month"] == 12)
        ]
        jan_row = result[
            (result["year"] == 2023) & (result["month"] == 1)
        ]
        assert len(dec_row) == 1
        assert len(jan_row) == 1
        assert dec_row.iloc[0]["discharge_avg"] == pytest.approx(50.0)
        assert jan_row.iloc[0]["discharge_avg"] == pytest.approx(70.0)


class TestReadMonthlyObservationsApiFailure:
    """API failure tests for read_monthly_observations."""

    def test_api_unavailable_returns_empty(self):
        """When SAPPHIRE_API_AVAILABLE is False, returns empty DataFrame."""
        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", False):
            result = read_monthly_observations(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_api_disabled_returns_empty(self):
        """When SAPPHIRE_API_ENABLED=false, returns empty DataFrame."""
        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            result = read_monthly_observations(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_api_exception_returns_empty(self):
        """When API raises, returns empty DataFrame gracefully."""
        with patch("src.data_reader._read_daily_runoff_api") as mock_api:
            mock_api.side_effect = Exception("connection refused")
            result = read_monthly_observations(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ===================================================================
# read_monthly_forecasts
# ===================================================================


class TestReadMonthlyForecasts:
    """Tests for read_monthly_forecasts()."""

    def test_happy_path_reads_monthly_forecasts(self):
        """Reads long-term forecasts filtered to horizon_type=month."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [1, 2, 3], model_type="GBT", q50=120.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert len(result) == 3
        assert "model_short" in result.columns
        assert "q50" in result.columns
        assert result.iloc[0]["code"] == "15013"

    def test_model_type_becomes_model_short(self):
        """API model_type is mapped to model_short."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [1], model_type="SM_GBT", q50=100.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert result.iloc[0]["model_short"] == "SM_GBT"

    def test_month_extracted_from_valid_from(self):
        """Month column is extracted from valid_from date."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [6], model_type="GBT", q50=100.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert result.iloc[0]["month"] == 6

    def test_year_extracted(self):
        """Year column is extracted from valid_from date."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [3], model_type="GBT", q50=100.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert result.iloc[0]["year"] == 2023

    def test_multiple_models_preserved(self):
        """Multiple model types are preserved in output."""
        df1 = _make_long_forecast_records(
            "15013", 2023, [1], model_type="GBT", q50=100.0
        )
        df2 = _make_long_forecast_records(
            "15013", 2023, [1], model_type="LR_Base", q50=110.0
        )
        api_df = pd.concat([df1, df2], ignore_index=True)

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert len(result) == 2
        assert set(result["model_short"]) == {"GBT", "LR_Base"}

    def test_quantile_columns_preserved(self):
        """Quantile columns q05-q95 are present in output."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [1], model_type="GBT", q50=120.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        for qcol in ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]:
            assert qcol in result.columns
        assert result.iloc[0]["q50"] == pytest.approx(120.0)

    def test_output_columns(self):
        """Verify output has the expected column set."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [1], model_type="GBT", q50=100.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        expected_cols = {
            "code", "year", "month", "model_short",
            "q50", "q05", "q10", "q25", "q75", "q90", "q95",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_empty_api_returns_empty_df(self):
        """Empty API response -> empty DataFrame."""
        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = pd.DataFrame()
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_code_is_string(self):
        """Code column is always string type."""
        api_df = _make_long_forecast_records(
            "15013", 2023, [1], model_type="GBT", q50=100.0
        )

        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.return_value = api_df
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        assert result["code"].dtype == object


class TestReadMonthlyForecastsApiFailure:
    """API failure tests for read_monthly_forecasts."""

    def test_api_unavailable_returns_empty(self):
        """When SAPPHIRE_API_AVAILABLE is False, returns empty DataFrame."""
        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", False):
            result = read_monthly_forecasts(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_api_disabled_returns_empty(self):
        """When SAPPHIRE_API_ENABLED=false, returns empty DataFrame."""
        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            result = read_monthly_forecasts(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_api_exception_returns_empty(self):
        """When API raises, returns empty DataFrame gracefully."""
        with patch(
            "src.data_reader._read_long_forecasts_api"
        ) as mock_api:
            mock_api.side_effect = Exception("connection refused")
            result = read_monthly_forecasts(["15013"], 2023, 2023)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
