"""Integration tests for monthly observation and forecast reading.

Tests the full data flow: API read -> aggregation/normalization -> output,
mocking only the external API boundary (SapphirePreprocessingClient and
SapphirePostprocessingClient).

Reference: test_integration_postprocessing.py for integration test patterns.
"""

import os
import sys
from datetime import date
from unittest.mock import patch, MagicMock, call

import numpy as np
import pandas as pd
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

from src.data_reader import (
    read_monthly_observations,
    read_monthly_forecasts,
    _read_daily_runoff_api,
    _read_long_forecasts_api,
    SAPPHIRE_API_AVAILABLE,
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


def _make_long_forecast_records(
    code, year, months, model_type="GBT", q50=120.0
):
    """Create DataFrame mimicking read_long_term_forecasts() response."""
    records = []
    for m in months:
        first_day = date(year, m, 1)
        if m == 12:
            last_day = date(year, 12, 31)
        else:
            last_day = date(year, m + 1, 1) - pd.Timedelta(days=1)
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
# Integration: _read_daily_runoff_api with real client mock
# ===================================================================


class TestReadDailyRunoffApiIntegration:
    """Integration tests for _read_daily_runoff_api with mocked client."""

    def test_paginates_across_batches(self):
        """When API returns batch_size records, fetches next page.

        Pagination continues only when len(batch) == batch_size (1000).
        Create a 1000-row first page to trigger page 2 fetch.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        # Page 1: exactly 1000 rows -> triggers next page
        dates_p1 = pd.date_range("2020-01-01", periods=1000, freq="D")
        page1 = pd.DataFrame({
            "code": "15013",
            "date": [d.strftime("%Y-%m-%d") for d in dates_p1],
            "discharge_avg": 100.0,
        })
        # Page 2: partial (< 1000) -> loop exits
        page2 = _make_daily_runoff("15013", "2022-09-27", "2022-12-31")

        mock_client = MagicMock()
        mock_client.read_runoff.side_effect = [page1, page2]

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePreprocessingClient",
                 return_value=mock_client,
             ):
            result = _read_daily_runoff_api(["15013"], 2020, 2022)

        assert len(result) == 1000 + len(page2)
        assert mock_client.read_runoff.call_count == 2

    def test_multiple_codes_fetched_sequentially(self):
        """Each code is fetched separately and results are combined.

        With < 1000 rows per code, each code makes exactly one API call.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        daily_a = _make_daily_runoff("15013", "2023-06-01", "2023-06-30", 100)
        daily_b = _make_daily_runoff("15020", "2023-06-01", "2023-06-30", 200)

        mock_client = MagicMock()
        # Each code's first batch is < 1000 rows -> loop exits after 1 call
        mock_client.read_runoff.side_effect = [daily_a, daily_b]

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePreprocessingClient",
                 return_value=mock_client,
             ):
            result = _read_daily_runoff_api(
                ["15013", "15020"], 2023, 2023
            )

        assert len(result) == 60  # 30 + 30 days
        assert set(result["code"]) == {"15013", "15020"}

    def test_api_unavailable_returns_empty(self):
        """When API not installed, returns empty DataFrame."""
        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", False):
            result = _read_daily_runoff_api(["15013"], 2023, 2023)
        assert result.empty

    def test_date_range_passed_to_client(self):
        """Start and end dates are correctly formatted and passed."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        mock_client = MagicMock()
        mock_client.read_runoff.return_value = pd.DataFrame()

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePreprocessingClient",
                 return_value=mock_client,
             ):
            _read_daily_runoff_api(["15013"], 2021, 2023)

        # Verify the first call used correct date range
        first_call = mock_client.read_runoff.call_args_list[0]
        assert first_call.kwargs["start_date"] == "2021-01-01"
        assert first_call.kwargs["end_date"] == "2023-12-31"
        assert first_call.kwargs["horizon"] == "day"
        assert first_call.kwargs["code"] == "15013"


# ===================================================================
# Integration: _read_long_forecasts_api with real client mock
# ===================================================================


class TestReadLongForecastsApiIntegration:
    """Integration tests for _read_long_forecasts_api with mocked client."""

    def test_calls_read_long_term_forecasts(self):
        """Verify the client method is called with correct params."""
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        mock_client = MagicMock()
        mock_client.read_long_term_forecasts.return_value = pd.DataFrame()

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePostprocessingClient",
                 return_value=mock_client,
             ):
            _read_long_forecasts_api(["15013"], 2022, 2023)

        first_call = mock_client.read_long_term_forecasts.call_args_list[0]
        assert first_call.kwargs["horizon_type"] == "month"
        assert first_call.kwargs["code"] == "15013"
        assert first_call.kwargs["start_date"] == "2022-01-01"
        assert first_call.kwargs["end_date"] == "2023-12-31"

    def test_paginates_long_forecasts(self):
        """When first page has batch_size records, fetches next page.

        Pagination continues only when len(batch) == batch_size (1000).
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        # Build a 1000-row first page by repeating records
        base = _make_long_forecast_records("15013", 2023, [1])
        page1 = pd.concat([base] * 1000, ignore_index=True)
        page2 = _make_long_forecast_records("15013", 2023, [2, 3])

        mock_client = MagicMock()
        mock_client.read_long_term_forecasts.side_effect = [page1, page2]

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePostprocessingClient",
                 return_value=mock_client,
             ):
            result = _read_long_forecasts_api(["15013"], 2023, 2023)

        assert len(result) == 1002  # 1000 + 2
        assert mock_client.read_long_term_forecasts.call_count == 2

    def test_multiple_codes_combined(self):
        """Forecasts for multiple stations are combined.

        With < 1000 rows per code, each code makes one API call.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        fc_a = _make_long_forecast_records("15013", 2023, [1])
        fc_b = _make_long_forecast_records("15020", 2023, [1])

        mock_client = MagicMock()
        mock_client.read_long_term_forecasts.side_effect = [fc_a, fc_b]

        with patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}), \
             patch(
                 "src.data_reader.SapphirePostprocessingClient",
                 return_value=mock_client,
             ):
            result = _read_long_forecasts_api(
                ["15013", "15020"], 2023, 2023
            )

        assert len(result) == 2
        assert set(result["code"]) == {"15013", "15020"}


# ===================================================================
# Full pipeline integration
# ===================================================================


class TestObservationsPipelineIntegration:
    """Full pipeline: API mock -> read_monthly_observations -> output."""

    def test_full_pipeline_multi_station_multi_year(self):
        """Two stations, two years, full months -> correct aggregation."""
        daily = pd.concat([
            _make_daily_runoff("15013", "2022-06-01", "2022-06-30", 100.0),
            _make_daily_runoff("15013", "2023-06-01", "2023-06-30", 120.0),
            _make_daily_runoff("15020", "2022-06-01", "2022-06-30", 200.0),
            _make_daily_runoff("15020", "2023-06-01", "2023-06-30", 250.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(
                ["15013", "15020"], 2022, 2023
            )

        # 2 stations * 2 years * 1 month = 4 rows
        assert len(result) == 4
        assert set(result["code"]) == {"15013", "15020"}
        assert set(result["year"]) == {2022, 2023}

        # Verify station-specific values
        s13_2022 = result[
            (result["code"] == "15013") & (result["year"] == 2022)
        ]
        assert s13_2022.iloc[0]["discharge_avg"] == pytest.approx(100.0)

        s20_2023 = result[
            (result["code"] == "15020") & (result["year"] == 2023)
        ]
        assert s20_2023.iloc[0]["discharge_avg"] == pytest.approx(250.0)

    def test_pipeline_delta_computed_across_years(self):
        """Delta uses cross-year std for the same station+month."""
        # 3 years of June data for one station
        daily = pd.concat([
            _make_daily_runoff("15013", "2021-06-01", "2021-06-30", 80.0),
            _make_daily_runoff("15013", "2022-06-01", "2022-06-30", 100.0),
            _make_daily_runoff("15013", "2023-06-01", "2023-06-30", 120.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2021, 2023)

        assert len(result) == 3
        expected_delta = 0.674 * np.std([80.0, 100.0, 120.0], ddof=1)
        for _, row in result.iterrows():
            assert row["delta"] == pytest.approx(expected_delta, rel=1e-3)

    def test_pipeline_mixed_coverage_filters_correctly(self):
        """Months with mixed coverage: some pass, some filtered out."""
        daily = pd.concat([
            # Full January (31/31 = 100%) -> pass
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0),
            # 5 days of Feb (5/28 = 17.9%) -> fail
            _make_daily_runoff("15013", "2023-02-01", "2023-02-05", 50.0),
            # Full March (31/31 = 100%) -> pass
            _make_daily_runoff("15013", "2023-03-01", "2023-03-31", 200.0),
        ], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        assert len(result) == 2
        assert set(result["month"]) == {1, 3}

    def test_pipeline_preserves_all_output_columns(self):
        """All expected columns present in final output."""
        daily = _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0)

        with patch("src.data_reader._read_daily_runoff_api") as mock:
            mock.return_value = daily
            result = read_monthly_observations(["15013"], 2023, 2023)

        expected = {
            "code", "year", "month", "month_in_year",
            "discharge_avg", "delta",
        }
        assert expected == set(result.columns) & expected


class TestForecastsPipelineIntegration:
    """Full pipeline: API mock -> read_monthly_forecasts -> output."""

    def test_full_pipeline_multi_model_multi_station(self):
        """Multiple models and stations -> all preserved and normalized."""
        records = pd.concat([
            _make_long_forecast_records(
                "15013", 2023, [1, 2], model_type="GBT", q50=100.0
            ),
            _make_long_forecast_records(
                "15013", 2023, [1, 2], model_type="LR_Base", q50=90.0
            ),
            _make_long_forecast_records(
                "15020", 2023, [1, 2], model_type="GBT", q50=200.0
            ),
        ], ignore_index=True)

        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = records
            result = read_monthly_forecasts(
                ["15013", "15020"], 2023, 2023
            )

        # 2 models * 2 months + 1 model * 2 months = 6
        assert len(result) == 6
        assert set(result["model_short"]) == {"GBT", "LR_Base"}
        assert set(result["code"]) == {"15013", "15020"}

        # Verify specific values
        gbt_15013_jan = result[
            (result["code"] == "15013")
            & (result["model_short"] == "GBT")
            & (result["month"] == 1)
        ]
        assert len(gbt_15013_jan) == 1
        assert gbt_15013_jan.iloc[0]["q50"] == pytest.approx(100.0)

    def test_pipeline_quantiles_preserved_end_to_end(self):
        """Quantile columns survive the full pipeline unchanged."""
        q50 = 150.0
        records = _make_long_forecast_records(
            "15013", 2023, [6], model_type="SM_GBT", q50=q50
        )

        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = records
            result = read_monthly_forecasts(["15013"], 2023, 2023)

        row = result.iloc[0]
        assert row["q05"] == pytest.approx(q50 * 0.7)
        assert row["q10"] == pytest.approx(q50 * 0.75)
        assert row["q25"] == pytest.approx(q50 * 0.85)
        assert row["q50"] == pytest.approx(q50)
        assert row["q75"] == pytest.approx(q50 * 1.15)
        assert row["q90"] == pytest.approx(q50 * 1.25)
        assert row["q95"] == pytest.approx(q50 * 1.3)

    def test_pipeline_year_month_extraction_multi_year(self):
        """Year and month are correctly extracted across year boundary."""
        records = pd.concat([
            _make_long_forecast_records("15013", 2022, [11, 12]),
            _make_long_forecast_records("15013", 2023, [1, 2]),
        ], ignore_index=True)

        with patch("src.data_reader._read_long_forecasts_api") as mock:
            mock.return_value = records
            result = read_monthly_forecasts(["15013"], 2022, 2023)

        assert len(result) == 4
        assert set(result["year"]) == {2022, 2023}
        dec_row = result[
            (result["year"] == 2022) & (result["month"] == 12)
        ]
        assert len(dec_row) == 1
        jan_row = result[
            (result["year"] == 2023) & (result["month"] == 1)
        ]
        assert len(jan_row) == 1


class TestObservationsAndForecastsCombined:
    """Integration: both read functions produce compatible output."""

    def test_observations_and_forecasts_can_merge_on_key(self):
        """Observations and forecasts can be joined on (code, year, month)."""
        daily = pd.concat([
            _make_daily_runoff("15013", "2023-01-01", "2023-01-31", 100.0),
            _make_daily_runoff("15013", "2023-02-01", "2023-02-28", 120.0),
        ], ignore_index=True)

        forecasts_raw = _make_long_forecast_records(
            "15013", 2023, [1, 2], model_type="GBT", q50=110.0
        )

        with patch("src.data_reader._read_daily_runoff_api") as mock_obs, \
             patch("src.data_reader._read_long_forecasts_api") as mock_fc:
            mock_obs.return_value = daily
            mock_fc.return_value = forecasts_raw

            obs = read_monthly_observations(["15013"], 2023, 2023)
            fc = read_monthly_forecasts(["15013"], 2023, 2023)

        # Both have code, year, month columns
        merge_keys = ["code", "year", "month"]
        for key in merge_keys:
            assert key in obs.columns, f"Missing {key} in observations"
            assert key in fc.columns, f"Missing {key} in forecasts"

        # Merge should produce 2 rows (Jan + Feb)
        merged = obs.merge(fc, on=merge_keys, how="inner")
        assert len(merged) == 2

        # Verify merged data has both observation and forecast columns
        assert "discharge_avg" in merged.columns
        assert "q50" in merged.columns
        assert "model_short" in merged.columns
        assert "delta" in merged.columns

    def test_merge_multiple_models_per_month(self):
        """Multiple models per (code, month) -> one obs row per model row."""
        daily = _make_daily_runoff(
            "15013", "2023-06-01", "2023-06-30", 100.0
        )

        fc_gbt = _make_long_forecast_records(
            "15013", 2023, [6], model_type="GBT", q50=110.0
        )
        fc_lr = _make_long_forecast_records(
            "15013", 2023, [6], model_type="LR_Base", q50=95.0
        )
        forecasts_raw = pd.concat([fc_gbt, fc_lr], ignore_index=True)

        with patch("src.data_reader._read_daily_runoff_api") as mock_obs, \
             patch("src.data_reader._read_long_forecasts_api") as mock_fc:
            mock_obs.return_value = daily
            mock_fc.return_value = forecasts_raw

            obs = read_monthly_observations(["15013"], 2023, 2023)
            fc = read_monthly_forecasts(["15013"], 2023, 2023)

        merged = obs.merge(fc, on=["code", "year", "month"], how="inner")
        # 1 obs row * 2 model rows = 2 merged rows
        assert len(merged) == 2
        assert set(merged["model_short"]) == {"GBT", "LR_Base"}
        # Observation value replicated for both models
        for _, row in merged.iterrows():
            assert row["discharge_avg"] == pytest.approx(100.0)

    def test_no_overlap_produces_empty_merge(self):
        """Observations and forecasts for different months -> empty merge."""
        daily = _make_daily_runoff(
            "15013", "2023-01-01", "2023-01-31", 100.0
        )
        forecasts_raw = _make_long_forecast_records(
            "15013", 2023, [6], model_type="GBT", q50=110.0
        )

        with patch("src.data_reader._read_daily_runoff_api") as mock_obs, \
             patch("src.data_reader._read_long_forecasts_api") as mock_fc:
            mock_obs.return_value = daily
            mock_fc.return_value = forecasts_raw

            obs = read_monthly_observations(["15013"], 2023, 2023)
            fc = read_monthly_forecasts(["15013"], 2023, 2023)

        merged = obs.merge(fc, on=["code", "year", "month"], how="inner")
        assert merged.empty
