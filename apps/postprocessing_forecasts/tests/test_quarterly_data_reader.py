"""Tests for quarterly/seasonal data reader functions.

Phase 4b Step 2.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import data_reader


@pytest.fixture(autouse=True)
def long_term_horizon_config(monkeypatch, tmp_path):
    """Provide sentinel long-term resolver config for reader tests."""
    config_dir = tmp_path / "long_term"
    config_dir.mkdir()
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes",
        "quarter,seasonal_january,seasonal_february,seasonal_march,seasonal_april",
    )
    for name, lead in {
        "quarter": 1,
        "seasonal_january": 3,
        "seasonal_february": 2,
        "seasonal_march": 1,
        "seasonal_april": 0,
    }.items():
        (config_dir / f"{name}.json").write_text(json.dumps({"operational_month_lead_time": lead}))
    return config_dir


# ===================================================================
# read_skill_metrics() extended dispatch
# ===================================================================


class TestReadSkillMetricsDispatch:
    def test_quarter_accepted(self):
        """'quarter' is a valid horizon_type (no ValueError)."""
        with patch.object(data_reader, "read_quarterly_skill_metrics", return_value=pd.DataFrame()):
            result = data_reader.read_skill_metrics("quarter")
            assert isinstance(result, pd.DataFrame)

    def test_season_accepted(self):
        with patch.object(data_reader, "read_seasonal_skill_metrics", return_value=pd.DataFrame()):
            result = data_reader.read_skill_metrics("season")
            assert isinstance(result, pd.DataFrame)

    def test_invalid_horizon_type(self):
        with pytest.raises(ValueError, match="horizon_type must be one of"):
            data_reader.read_skill_metrics("weekly")


# ===================================================================
# Quarterly observations — delegation to monthly + aggregation
# ===================================================================


class TestReadQuarterlyObservations:
    def test_delegates_to_monthly(self):
        """read_quarterly_observations calls read_monthly_observations."""
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "year": [2024] * 6,
                "month": [1, 2, 3, 4, 5, 6],
                "discharge_avg": [100, 110, 120, 50, 60, 70],
            }
        )
        with patch.object(data_reader, "read_monthly_observations", return_value=monthly):
            result = data_reader.read_quarterly_observations(["S1"], 2024, 2024)
        assert not result.empty
        assert "quarter_in_year" in result.columns
        assert "discharge_avg" in result.columns
        assert "delta" in result.columns

    def test_empty_monthly_returns_empty(self):
        with patch.object(data_reader, "read_monthly_observations", return_value=pd.DataFrame()):
            result = data_reader.read_quarterly_observations(["S1"], 2024, 2024)
        assert result.empty
        assert "quarter_in_year" in result.columns


# ===================================================================
# Seasonal observations
# ===================================================================


class TestReadSeasonalObservations:
    def test_delegates_to_monthly(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "year": [2024] * 6,
                "month": [4, 5, 6, 7, 8, 9],
                "discharge_avg": [100, 110, 120, 130, 140, 150],
            }
        )
        with patch.object(data_reader, "read_monthly_observations", return_value=monthly):
            result = data_reader.read_seasonal_observations(["S1"], 2024, 2024)
        assert not result.empty
        assert "season_year" in result.columns
        assert "season_in_year" in result.columns


# ===================================================================
# Quarterly forecasts
# ===================================================================


class TestReadQuarterlyForecasts:
    def test_aggregated_from_monthly(self):
        """Quarterly forecasts still include aggregated monthly data."""
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 3,
                "year": [2024] * 3,
                "month": [1, 2, 3],
                "model_short": ["LR_Base"] * 3,
                "q05": [10, 20, 30],
                "q10": [15, 25, 35],
                "q25": [20, 30, 40],
                "q50": [30, 40, 50],
                "q75": [40, 50, 60],
                "q90": [50, 60, 70],
                "q95": [60, 70, 80],
                "forecasted_discharge": [30, 40, 50],
            }
        )
        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=monthly),
            patch.object(
                data_reader,
                "_read_long_forecasts_api",
                return_value=pd.DataFrame(),
            ) as read_api,
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)
        kwargs = read_api.call_args.kwargs
        assert kwargs["horizon_type"] == "quarter"
        assert kwargs["horizon_value"] == 1
        assert not result.empty
        assert "quarter_in_year" in result.columns
        assert "model_short" in result.columns

    def test_empty_both_sources_returns_empty(self):
        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=pd.DataFrame()),
            patch.object(data_reader, "_read_long_forecasts_api", return_value=pd.DataFrame()),
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)
        assert result.empty

    def test_quarter_read_uses_resolved_lead_zero(self, long_term_horizon_config):
        """Quarter direct API read follows a lead-0 deployment config."""
        (long_term_horizon_config / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 0})
        )
        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=pd.DataFrame()),
            patch.object(
                data_reader,
                "_read_long_forecasts_api",
                return_value=pd.DataFrame(),
            ) as read_api,
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)

        assert result.empty
        kwargs = read_api.call_args.kwargs
        assert kwargs["horizon_type"] == "quarter"
        assert kwargs["horizon_value"] == 0

    def test_direct_preferred_over_aggregated(self):
        """When same model in both sources, direct wins."""
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 3,
                "year": [2024] * 3,
                "month": [1, 2, 3],
                "model_short": ["LR_Base"] * 3,
                "q05": [10, 20, 30],
                "q10": [15, 25, 35],
                "q25": [20, 30, 40],
                "q50": [30, 40, 50],
                "q75": [40, 50, 60],
                "q90": [50, 60, 70],
                "q95": [60, 70, 80],
                "forecasted_discharge": [30, 40, 50],
            }
        )
        direct_api = pd.DataFrame(
            {
                "code": ["S1"],
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": ["2024-03-31"],
                "model_type": ["LR_Base"],
                "q05": [99],
                "q10": [99],
                "q25": [99],
                "q50": [99],
                "q75": [99],
                "q90": [99],
                "q95": [99],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return direct_api
            return pd.DataFrame()

        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=monthly),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api),
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)
        # Direct wins: q50 should be 99, not the aggregated mean
        lr_base_rows = result[result["model_short"] == "LR_Base"]
        assert len(lr_base_rows) == 1
        assert lr_base_rows.iloc[0]["q50"] == 99

    def test_filters_deprecated_models_after_combining_sources(self):
        """Quarterly reader keeps LR raw models and ensembles, dropping deprecated rows."""
        monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "year": [2024] * 6,
                "month": [1, 2, 3, 1, 2, 3],
                "model_short": ["LR_Base"] * 3 + ["GBT"] * 3,
                "q05": [10, 20, 30, 900, 900, 900],
                "q10": [15, 25, 35, 925, 925, 925],
                "q25": [20, 30, 40, 950, 950, 950],
                "q50": [30, 40, 50, 1000, 1000, 1000],
                "q75": [40, 50, 60, 1050, 1050, 1050],
                "q90": [50, 60, 70, 1075, 1075, 1075],
                "q95": [60, 70, 80, 1100, 1100, 1100],
                "forecasted_discharge": [30, 40, 50, 1000, 1000, 1000],
            }
        )
        direct_api = pd.DataFrame(
            {
                "code": ["S1", "S1", "S1"],
                "valid_from": pd.to_datetime(["2024-01-01"] * 3),
                "valid_to": ["2024-03-31"] * 3,
                "model_type": ["LR_SM", "SM_GBT_Norm", "EM"],
                "q05": [15, 900, 12],
                "q10": [20, 925, 17],
                "q25": [25, 950, 22],
                "q50": [35, 1000, 32],
                "q75": [45, 1050, 42],
                "q90": [55, 1075, 52],
                "q95": [65, 1100, 62],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return direct_api
            return pd.DataFrame()

        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=monthly),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api),
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)

        assert set(result["model_short"]) == {"LR_Base", "LR_SM", "EM"}
        assert not {"GBT", "SM_GBT_Norm"} & set(result["model_short"])

    def test_monthly_reader_keeps_deprecated_models(self):
        raw_api = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "valid_from": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "valid_to": ["2024-01-31", "2024-01-31"],
                "model_type": ["LR_Base", "GBT"],
                "q50": [30, 999],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_monthly_forecasts(["S1"], 2024, 2024)

        assert set(result["model_short"]) == {"LR_Base", "GBT"}


# ===================================================================
# Seasonal forecasts
# ===================================================================


class TestReadSeasonalForecasts:
    def test_reads_directly_from_api(self):
        """read_seasonal_forecasts reads season data directly from API."""
        raw_api = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "valid_from": pd.to_datetime(["2024-04-01", "2024-04-01"]),
                "valid_to": ["2024-09-30", "2024-09-30"],
                "model_type": ["LR_Base", "LR_SM"],
                "q05": [10, 15],
                "q10": [15, 20],
                "q25": [20, 25],
                "q50": [30, 35],
                "q75": [40, 45],
                "q90": [50, 55],
                "q95": [60, 65],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_seasonal_forecasts(["S1"], 2024, 2024)
        assert not result.empty
        assert "season_year" in result.columns
        assert "season_in_year" in result.columns
        assert "model_short" in result.columns
        assert set(result["model_short"]) == {"LR_Base", "LR_SM"}

    def test_empty_api_returns_empty(self):
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=pd.DataFrame()):
            result = data_reader.read_seasonal_forecasts(["S1"], 2024, 2024)
        assert result.empty

    def test_deprecated_models_filtered_and_ensemble_rows_kept(self):
        """Deprecated raw rows are excluded; stored ensemble rows are retained."""
        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 5,
                "valid_from": pd.to_datetime(["2024-04-01"] * 5),
                "valid_to": ["2024-09-30"] * 5,
                "model_type": ["LR_Base", "LR_SM", "GBT", "EM", "Skilled Mean"],
                "q50": [30, 35, 999, 40, 50],
                "q05": [10, 15, 900, 20, 30],
                "q10": [15, 20, 925, 25, 35],
                "q25": [20, 25, 950, 30, 40],
                "q75": [40, 45, 1050, 50, 60],
                "q90": [50, 55, 1075, 60, 70],
                "q95": [60, 65, 1100, 70, 80],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_seasonal_forecasts(["S1"], 2024, 2024)
        assert set(result["model_short"]) == {"LR_Base", "LR_SM", "EM", "Skilled Mean"}
        assert "GBT" not in set(result["model_short"])

    def test_preserves_four_issue_dates_and_leads(self):
        """Jan-Apr issue rows for one target season survive distinctly."""
        raw_api = pd.DataFrame(
            {
                "code": ["19999"] * 4,
                "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]),
                "horizon_value": [3, 2, 1, 0],
                "valid_from": pd.to_datetime(["2024-04-01"] * 4),
                "valid_to": ["2024-09-30"] * 4,
                "model_type": ["LR_Base"] * 4,
                "q50": [30, 31, 32, 33],
                "q05": [10, 11, 12, 13],
                "q10": [15, 16, 17, 18],
                "q25": [20, 21, 22, 23],
                "q75": [40, 41, 42, 43],
                "q90": [50, 51, 52, 53],
                "q95": [60, 61, 62, 63],
            }
        )

        with patch.object(
            data_reader,
            "_read_long_forecasts_api",
            return_value=raw_api,
        ) as read_api:
            result = data_reader.read_seasonal_forecasts(["19999"], 2024, 2024, horizon_value=3)

        assert len(result) == 4
        assert read_api.call_args.kwargs["horizon_value"] == 3
        assert set(result["season_year"]) == {2024}
        assert set(result["season_in_year"]) == {0, 1, 2, 3}
        assert set(result["horizon_value"]) == {0, 1, 2, 3}
        assert "date" in result.columns
        by_issue = {row["date"][:10]: int(row["season_in_year"]) for _, row in result.iterrows()}
        assert by_issue == {
            "2024-01-01": 3,
            "2024-02-01": 2,
            "2024-03-01": 1,
            "2024-04-01": 0,
        }


# ===================================================================
# Latest quarterly/seasonal forecasts
# ===================================================================


class TestReadLatestQuarterlyForecasts:
    def test_returns_most_recent_quarter(self):
        """Filters to the most recent quarter after combining sources."""
        raw_monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "valid_from": pd.to_datetime(
                    [
                        "2024-01-01",
                        "2024-02-01",
                        "2024-03-01",
                        "2024-04-01",
                        "2024-05-01",
                        "2024-06-01",
                    ]
                ),
                "valid_to": pd.to_datetime(
                    [
                        "2024-01-31",
                        "2024-02-29",
                        "2024-03-31",
                        "2024-04-30",
                        "2024-05-31",
                        "2024-06-30",
                    ]
                ),
                "model_type": ["LR_Base"] * 6,
                "q50": [100, 110, 120, 50, 60, 70],
                "q05": [80, 90, 100, 30, 40, 50],
                "q10": [85, 95, 105, 35, 45, 55],
                "q25": [90, 100, 110, 40, 50, 60],
                "q75": [110, 120, 130, 60, 70, 80],
                "q90": [120, 130, 140, 70, 80, 90],
                "q95": [130, 140, 150, 80, 90, 100],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "month":
                return raw_monthly
            return pd.DataFrame()  # No direct quarterly data

        with patch.object(
            data_reader,
            "_read_long_forecasts_api",
            side_effect=mock_read_api,
        ) as read_api:
            import datetime as dt

            result = data_reader.read_latest_quarterly_forecasts(
                ["S1"], forecast_date=dt.date(2024, 7, 1)
            )
        quarter_call = [
            call for call in read_api.call_args_list if call.kwargs.get("horizon_type") == "quarter"
        ][0]
        assert quarter_call.kwargs["horizon_value"] == 1
        assert not result.empty
        # Should be Q2 (latest quarter with data)
        assert all(result["quarter_in_year"] == 2)

    def test_empty_api_returns_empty(self):
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=None):
            result = data_reader.read_latest_quarterly_forecasts(["S1"])
        assert result.empty

    def test_latest_filters_deprecated_models_after_combining_sources(self):
        import datetime as dt

        raw_monthly = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "valid_from": pd.to_datetime(["2024-04-01", "2024-05-01", "2024-06-01"] * 2),
                "valid_to": pd.to_datetime(["2024-04-30", "2024-05-31", "2024-06-30"] * 2),
                "model_type": ["LR_Base"] * 3 + ["GBT"] * 3,
                "q50": [50, 60, 70, 1000, 1000, 1000],
                "q05": [30, 40, 50, 900, 900, 900],
                "q10": [35, 45, 55, 925, 925, 925],
                "q25": [40, 50, 60, 950, 950, 950],
                "q75": [60, 70, 80, 1050, 1050, 1050],
                "q90": [70, 80, 90, 1075, 1075, 1075],
                "q95": [80, 90, 100, 1100, 1100, 1100],
            }
        )
        raw_quarter = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "valid_from": pd.to_datetime(["2024-04-01", "2024-04-01"]),
                "valid_to": pd.to_datetime(["2024-06-30", "2024-06-30"]),
                "model_type": ["LR_SM", "LR_SM_DT"],
                "q50": [65, 999],
                "q05": [45, 900],
                "q10": [50, 925],
                "q25": [55, 950],
                "q75": [75, 1050],
                "q90": [85, 1075],
                "q95": [95, 1100],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return raw_quarter
            return raw_monthly

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api):
            result = data_reader.read_latest_quarterly_forecasts(
                ["S1"], forecast_date=dt.date(2024, 7, 1)
            )

        assert set(result["model_short"]) == {"LR_Base", "LR_SM"}
        assert not {"GBT", "LR_SM_DT"} & set(result["model_short"])


class TestReadLatestSeasonalForecasts:
    def test_returns_latest_season(self):
        """Returns seasonal forecasts for the most recent season_year."""
        import datetime as dt

        raw_api = pd.DataFrame(
            {
                "code": ["S1", "S1", "S1"],
                "valid_from": pd.to_datetime(["2023-04-01", "2024-04-01", "2024-04-01"]),
                "valid_to": ["2023-09-30", "2024-09-30", "2024-09-30"],
                "model_type": ["LR_Base", "LR_Base", "LR_SM"],
                "q05": [10, 15, 12],
                "q10": [15, 20, 17],
                "q25": [20, 25, 22],
                "q50": [30, 35, 32],
                "q75": [40, 45, 42],
                "q90": [50, 55, 52],
                "q95": [60, 65, 62],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "season":
                return raw_api
            return pd.DataFrame()

        with patch.object(
            data_reader,
            "_read_long_forecasts_api",
            side_effect=mock_read_api,
        ) as read_api:
            result = data_reader.read_latest_seasonal_forecasts(
                ["S1"], forecast_date=dt.date(2024, 10, 1), horizon_value=3
            )
        assert read_api.call_args.kwargs["horizon_value"] == 3
        assert not result.empty
        # Should only have 2024 season (latest)
        assert all(result["season_year"] == 2024)
        assert len(result) == 2  # LR_Base and LR_SM for 2024
        assert set(result["model_short"]) == {"LR_Base", "LR_SM"}
        assert "season_in_year" in result.columns
        assert "forecasted_discharge" in result.columns

    def test_deprecated_models_filtered_and_ensemble_rows_kept(self):
        """Deprecated raw rows are excluded; stored ensemble rows are retained."""
        import datetime as dt

        raw_api = pd.DataFrame(
            {
                "code": ["S1", "S1", "S1"],
                "valid_from": pd.to_datetime(["2024-04-01"] * 3),
                "valid_to": ["2024-09-30"] * 3,
                "model_type": ["LR_Base", "GBT", "EM"],
                "q05": [10, 900, 20],
                "q10": [15, 925, 25],
                "q25": [20, 950, 30],
                "q50": [30, 1000, 40],
                "q75": [40, 1050, 50],
                "q90": [50, 1075, 60],
                "q95": [60, 1100, 70],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "season":
                return raw_api
            return pd.DataFrame()

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api):
            result = data_reader.read_latest_seasonal_forecasts(
                ["S1"], forecast_date=dt.date(2024, 10, 1)
            )
        assert set(result["model_short"]) == {"LR_Base", "EM"}
        assert "GBT" not in set(result["model_short"])

    def test_empty_api_returns_empty(self):
        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame()

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api):
            result = data_reader.read_latest_seasonal_forecasts(["S1"])
        assert result.empty

    def test_latest_deduplicates_without_folding_four_issues(self):
        """Duplicate issue rows are removed, but all four leads remain."""
        import datetime as dt

        raw_api = pd.DataFrame(
            {
                "code": ["19999"] * 5,
                "date": pd.to_datetime(
                    [
                        "2024-01-01",
                        "2024-01-01",
                        "2024-02-01",
                        "2024-03-01",
                        "2024-04-01",
                    ]
                ),
                "horizon_value": [3, 3, 2, 1, 0],
                "valid_from": pd.to_datetime(["2024-04-01"] * 5),
                "valid_to": ["2024-09-30"] * 5,
                "model_type": ["LR_Base"] * 5,
                "q50": [30, 30, 31, 32, 33],
                "q05": [10, 10, 11, 12, 13],
                "q10": [15, 15, 16, 17, 18],
                "q25": [20, 20, 21, 22, 23],
                "q75": [40, 40, 41, 42, 43],
                "q90": [50, 50, 51, 52, 53],
                "q95": [60, 60, 61, 62, 63],
            }
        )

        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_latest_seasonal_forecasts(
                ["19999"], forecast_date=dt.date(2024, 10, 1)
            )

        assert len(result) == 4
        assert set(result["season_in_year"]) == {0, 1, 2, 3}
        identity_cols = ["code", "season_year", "season_in_year", "date", "model_short"]
        assert result[identity_cols].duplicated().sum() == 0


# ===================================================================
# Combined forecasts from API
# ===================================================================


class TestReadQuarterlyCombinedForecasts:
    def test_returns_empty_when_api_unavailable(self):
        with patch.object(data_reader, "_read_long_combined_forecasts_api", return_value=None):
            result = data_reader.read_quarterly_combined_forecasts()
        assert result.empty

    def test_returns_data_when_api_available(self):
        mock_df = pd.DataFrame(
            {
                "code": ["S1"],
                "year": [2024],
                "quarter_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with patch.object(
            data_reader,
            "_read_long_combined_forecasts_api",
            return_value=mock_df,
        ) as read_api:
            result = data_reader.read_quarterly_combined_forecasts()
        assert read_api.call_args.kwargs["horizon_value"] == 1
        assert len(result) == 1


class TestReadSeasonalCombinedForecasts:
    def test_returns_empty_when_api_unavailable(self):
        with patch.object(
            data_reader,
            "_read_long_combined_forecasts_api",
            return_value=None,
        ) as read_api:
            result = data_reader.read_seasonal_combined_forecasts(horizon_value=0)
        assert read_api.call_args.kwargs["horizon_value"] == 0
        assert result.empty


class TestLongTermApiHorizonValue:
    def test_read_long_forecasts_api_forwards_horizon_value_when_provided(self):
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.read_long_term_forecasts.return_value = pd.DataFrame()

        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(
                data_reader,
                "SapphirePostprocessingClient",
                return_value=mock_client,
            ),
        ):
            data_reader._read_long_forecasts_api(
                ["19999"],
                2024,
                2024,
                horizon_type="season",
                horizon_value=3,
            )

        kwargs = mock_client.read_long_term_forecasts.call_args.kwargs
        assert kwargs["horizon_type"] == "season"
        assert kwargs["code"] == "19999"
        assert kwargs["horizon_value"] == 3

    def test_read_long_forecasts_api_omits_horizon_value_by_default(self):
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.read_long_term_forecasts.return_value = pd.DataFrame()

        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(
                data_reader,
                "SapphirePostprocessingClient",
                return_value=mock_client,
            ),
        ):
            data_reader._read_long_forecasts_api(["19999"], 2024, 2024)

        assert "horizon_value" not in mock_client.read_long_term_forecasts.call_args.kwargs

    def test_read_long_combined_forecasts_api_forwards_horizon_value_when_provided(self):
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.read_long_term_forecasts.return_value = pd.DataFrame()

        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(
                data_reader,
                "SapphirePostprocessingClient",
                return_value=mock_client,
            ),
        ):
            data_reader._read_long_combined_forecasts_api(
                "season",
                codes=["19999"],
                horizon_value=2,
            )

        kwargs = mock_client.read_long_term_forecasts.call_args.kwargs
        assert kwargs["horizon_type"] == "season"
        assert kwargs["code"] == "19999"
        assert kwargs["horizon_value"] == 2

    def test_read_long_combined_forecasts_api_omits_horizon_value_by_default(self):
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.read_long_term_forecasts.return_value = pd.DataFrame()

        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(
                data_reader,
                "SapphirePostprocessingClient",
                return_value=mock_client,
            ),
        ):
            data_reader._read_long_combined_forecasts_api("season", codes=["19999"])

        assert "horizon_value" not in mock_client.read_long_term_forecasts.call_args.kwargs


class TestCombinedForecastNormalization:
    def test_quarter_normalization_still_drops_raw_horizon_value(self):
        raw_api = pd.DataFrame(
            {
                "code": ["19999"],
                "horizon_value": [1],
                "valid_from": pd.to_datetime(["2024-01-01"]),
                "valid_to": ["2024-03-31"],
                "model_type": ["LR_Base"],
                "q50": [30],
            }
        )

        result = data_reader._normalize_combined_forecasts(raw_api, "quarter")

        assert result.iloc[0]["year"] == 2024
        assert result.iloc[0]["quarter_in_year"] == 1
        assert result.iloc[0]["model_short"] == "LR_Base"
        assert "horizon_value" not in result.columns
