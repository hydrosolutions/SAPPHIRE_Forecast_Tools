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

DEPRECATED_MODEL_FORMS = [
    "GBT",
    "LR_SM_DT",
    "LR_SM_ROF",
    "MC_ALD",
    "SM_GBT",
    "SM_GBT_LR",
    "SM_GBT_NORM",
    "SM_GBT_Norm",
]


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

    def test_filter_accepts_db_form_lr_and_ensemble_names(self):
        direct_api = pd.DataFrame(
            {
                "code": ["S1"] * 8,
                "valid_from": pd.to_datetime(["2024-01-01"] * 8),
                "valid_to": ["2024-03-31"] * 8,
                "model_type": [
                    "LR_BASE",
                    "LR_SM",
                    "ENSEMBLE_MEAN",
                    "NAIVE_MEAN",
                    "SKILLED_MEAN",
                    "GBT",
                    "LR_SM_DT",
                    "SM_GBT_NORM",
                ],
                "q05": [10, 15, 12, 13, 14, 900, 901, 902],
                "q10": [15, 20, 17, 18, 19, 925, 926, 927],
                "q25": [20, 25, 22, 23, 24, 950, 951, 952],
                "q50": [30, 35, 32, 33, 34, 1000, 1001, 1002],
                "q75": [40, 45, 42, 43, 44, 1050, 1051, 1052],
                "q90": [50, 55, 52, 53, 54, 1075, 1076, 1077],
                "q95": [60, 65, 62, 63, 64, 1100, 1101, 1102],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return direct_api
            return pd.DataFrame()

        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=pd.DataFrame()),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api),
        ):
            result = data_reader.read_quarterly_forecasts(["S1"], 2024, 2024)

        assert set(result["model_short"]) == {
            "LR_BASE",
            "LR_SM",
            "ENSEMBLE_MEAN",
            "NAIVE_MEAN",
            "SKILLED_MEAN",
        }
        assert not set(DEPRECATED_MODEL_FORMS) & set(result["model_short"])

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
# Quarterly forecasts — M1 P1b lead-aware carry-through
# ===================================================================


class TestReadQuarterlyForecastsLeadAware:
    def test_direct_quarter_horizon_value_survives(self, monkeypatch, long_term_horizon_config):
        """The direct-quarter branch's selected horizon_value must survive

        into the final read_quarterly_forecasts() output (previously
        silently stripped by _QUARTERLY_FC_COLS / normalization).
        """
        (long_term_horizon_config / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        target_year = 2024
        operational_row = {
            "horizon_type": "quarter",
            "horizon_value": 99,
            "code": "19999",
            "date": "2023-12-25",
            "model_type": "LR_Base",
            "valid_from": f"{target_year}-01-01",
            "valid_to": f"{target_year}-03-31",
            "q50": 100.0,
            "q05": 70.0,
            "q10": 75.0,
            "q25": 85.0,
            "q75": 115.0,
            "q90": 125.0,
            "q95": 130.0,
            "id": 1,
            "model_type_description": "LR_Base",
        }

        def fake_api(codes, start_year, end_year, horizon_type=None, horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            if start_year <= 2023:
                return pd.DataFrame([operational_row])
            return pd.DataFrame()

        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=pd.DataFrame()),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake_api),
        ):
            result = data_reader.read_quarterly_forecasts(["19999"], target_year, target_year)

        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 1

    def test_monthly_aggregated_two_leads_survive(self, monkeypatch, long_term_horizon_config):
        """Depends on Site 1 (aggregation.py): a monthly-aggregated-quarter

        source with two distinct leads must survive read_quarterly_forecasts
        as two rows with distinct horizon_value.
        """
        (long_term_horizon_config / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monthly = pd.DataFrame(
            {
                "code": ["19999"] * 4,
                "year": [2024] * 4,
                "month": [1, 2, 1, 2],
                "model_short": ["LR_Base"] * 4,
                "horizon_value": [0, 0, 1, 1],
                "q05": [10, 20, 11, 21],
                "q10": [15, 25, 16, 26],
                "q25": [20, 30, 21, 31],
                "q50": [30, 40, 31, 41],
                "q75": [40, 50, 41, 51],
                "q90": [50, 60, 51, 61],
                "q95": [60, 70, 61, 71],
                "forecasted_discharge": [30, 40, 31, 41],
            }
        )
        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=monthly),
            patch.object(data_reader, "_read_long_forecasts_api", return_value=pd.DataFrame()),
        ):
            result = data_reader.read_quarterly_forecasts(["19999"], 2024, 2024)

        assert len(result) == 2
        assert set(result["horizon_value"]) == {0, 1}

    def test_dedup_keeps_distinct_leads_after_combining_sources(
        self, monkeypatch, long_term_horizon_config
    ):
        """Two rows sharing (code, year, quarter, model) but differing only

        in horizon_value (one from monthly-aggregation, one from the
        direct-quarter source) must both survive the combine+dedup step.
        """
        (long_term_horizon_config / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        monthly = pd.DataFrame(
            {
                "code": ["19999", "19999"],
                "year": [2024, 2024],
                "month": [1, 2],
                "model_short": ["LR_Base", "LR_Base"],
                "horizon_value": [0, 0],
                "q05": [10, 20],
                "q10": [15, 25],
                "q25": [20, 30],
                "q50": [30, 40],
                "q75": [40, 50],
                "q90": [50, 60],
                "q95": [60, 70],
                "forecasted_discharge": [30, 40],
            }
        )

        operational_row = {
            "horizon_type": "quarter",
            "horizon_value": 99,
            "code": "19999",
            "date": "2023-12-25",
            "model_type": "LR_Base",
            "valid_from": "2024-01-01",
            "valid_to": "2024-03-31",
            "q50": 100.0,
            "q05": 70.0,
            "q10": 75.0,
            "q25": 85.0,
            "q75": 115.0,
            "q90": 125.0,
            "q95": 130.0,
            "id": 1,
            "model_type_description": "LR_Base",
        }

        def fake_api(codes, start_year, end_year, horizon_type=None, horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            if start_year <= 2023:
                return pd.DataFrame([operational_row])
            return pd.DataFrame()

        with (
            patch.object(data_reader, "read_monthly_forecasts", return_value=monthly),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake_api),
        ):
            result = data_reader.read_quarterly_forecasts(["19999"], 2024, 2024)

        assert len(result) == 2
        assert set(result["horizon_value"]) == {0, 1}


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

    def test_db_form_deprecated_models_filtered_and_ensemble_rows_kept(self):
        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 8,
                "valid_from": pd.to_datetime(["2024-04-01"] * 8),
                "valid_to": ["2024-09-30"] * 8,
                "model_type": [
                    "LR_BASE",
                    "LR_SM",
                    "ENSEMBLE_MEAN",
                    "NAIVE_MEAN",
                    "SKILLED_MEAN",
                    "GBT",
                    "LR_SM_ROF",
                    "SM_GBT_NORM",
                ],
                "q50": [30, 35, 40, 41, 42, 999, 998, 997],
                "q05": [10, 15, 20, 21, 22, 900, 899, 898],
                "q10": [15, 20, 25, 26, 27, 925, 924, 923],
                "q25": [20, 25, 30, 31, 32, 950, 949, 948],
                "q75": [40, 45, 50, 51, 52, 1050, 1049, 1048],
                "q90": [50, 55, 60, 61, 62, 1075, 1074, 1073],
                "q95": [60, 65, 70, 71, 72, 1100, 1099, 1098],
            }
        )

        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_seasonal_forecasts(["S1"], 2024, 2024)

        assert set(result["model_short"]) == {
            "LR_BASE",
            "LR_SM",
            "ENSEMBLE_MEAN",
            "NAIVE_MEAN",
            "SKILLED_MEAN",
        }
        assert not set(DEPRECATED_MODEL_FORMS) & set(result["model_short"])

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

    def test_latest_accepts_db_form_lr_and_ensemble_names(self):
        import datetime as dt

        raw_quarter = pd.DataFrame(
            {
                "code": ["S1"] * 8,
                "valid_from": pd.to_datetime(["2024-04-01"] * 8),
                "valid_to": pd.to_datetime(["2024-06-30"] * 8),
                "model_type": [
                    "LR_BASE",
                    "LR_SM",
                    "ENSEMBLE_MEAN",
                    "NAIVE_MEAN",
                    "SKILLED_MEAN",
                    "MC_ALD",
                    "SM_GBT",
                    "SM_GBT_LR",
                ],
                "q50": [50, 60, 55, 56, 57, 1000, 1001, 1002],
                "q05": [30, 40, 35, 36, 37, 900, 901, 902],
                "q10": [35, 45, 40, 41, 42, 925, 926, 927],
                "q25": [40, 50, 45, 46, 47, 950, 951, 952],
                "q75": [60, 70, 65, 66, 67, 1050, 1051, 1052],
                "q90": [70, 80, 75, 76, 77, 1075, 1076, 1077],
                "q95": [80, 90, 85, 86, 87, 1100, 1101, 1102],
            }
        )

        def mock_read_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return raw_quarter
            return pd.DataFrame()

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=mock_read_api):
            result = data_reader.read_latest_quarterly_forecasts(
                ["S1"], forecast_date=dt.date(2024, 7, 1)
            )

        assert set(result["model_short"]) == {
            "LR_BASE",
            "LR_SM",
            "ENSEMBLE_MEAN",
            "NAIVE_MEAN",
            "SKILLED_MEAN",
        }
        assert not set(DEPRECATED_MODEL_FORMS) & set(result["model_short"])


class TestReadLatestQuarterlyForecastsLeadAware:
    """FIX 3: the DIRECT-quarter branch of read_latest_quarterly_forecasts

    must, under SAPPHIRE_SKILL_LEAD_AWARE, read WITHOUT the single
    horizon_value filter, expand the read window backward by the
    configured lead, and reduce raw rows to the operational issuance
    (matching BOTH derived lead AND issue day) -- mirroring
    read_quarterly_forecasts' direct branch. Before the fix this branch
    always read with horizon_value=quarter_horizon_value() and never
    selected, so it missed prior-calendar-year issuances and retained
    non-operational same-lead backfill rows.
    """

    def _config_quarter_lead1(self, long_term_horizon_config, monkeypatch):
        (long_term_horizon_config / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def _q1_row(self, issue_date, q50, horizon_value):
        return {
            "horizon_type": "quarter",
            "horizon_value": horizon_value,
            "code": "19999",
            "date": issue_date,
            "model_type": "LR_Base",
            "valid_from": "2024-01-01",
            "valid_to": "2024-03-31",
            "q50": q50,
            "q05": q50 - 30,
            "q10": q50 - 25,
            "q25": q50 - 15,
            "q75": q50 + 15,
            "q90": q50 + 25,
            "q95": q50 + 30,
            "id": 1,
            "model_type_description": "LR_Base",
        }

    def test_prior_year_operational_issuance_selected_and_backfill_dropped(
        self, monkeypatch, long_term_horizon_config
    ):
        import datetime as dt

        self._config_quarter_lead1(long_term_horizon_config, monkeypatch)

        # Operational Q1-2024 issuance made 2023-12-25 (lead 1, issue-day 25)
        # with a STALE stored horizon_value (99). Its derived lead is 1.
        operational = self._q1_row("2023-12-25", q50=100.0, horizon_value=99)
        # Non-operational backfill for the SAME target and SAME derived
        # lead (1) but a NON-matching issue day (10) -> must be dropped.
        backfill = self._q1_row("2023-12-10", q50=200.0, horizon_value=99)

        def fake_api(codes, start_year, end_year, horizon_type=None, horizon_value=None):
            # Direct rows only for the quarter source, and only when the
            # read window was actually expanded backward into 2023.
            if horizon_type != "quarter":
                return pd.DataFrame()
            if start_year <= 2023:
                return pd.DataFrame([operational, backfill])
            return pd.DataFrame()

        with patch.object(
            data_reader, "_read_long_forecasts_api", side_effect=fake_api
        ) as mock_api:
            result = data_reader.read_latest_quarterly_forecasts(
                ["19999"], forecast_date=dt.date(2024, 2, 15)
            )

        # The window must have been expanded backward for the quarter read.
        quarter_calls = [
            c for c in mock_api.call_args_list if c.kwargs.get("horizon_type") == "quarter"
        ]
        assert quarter_calls, "expected a direct quarter API call"
        assert quarter_calls[0].args[1] <= 2023
        # ...and the direct read must NOT pin a single horizon_value.
        assert quarter_calls[0].kwargs.get("horizon_value") is None

        # Exactly the operational issuance survives, carrying derived lead 1.
        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 1
        assert result.iloc[0]["quarter_in_year"] == 1
        assert result.iloc[0]["year"] == 2024
        # The backfill (q50=200) was dropped; the operational (q50=100) kept.
        assert float(result.iloc[0]["forecasted_discharge"]) == 100.0

    def test_flag_off_keeps_single_horizon_value_filter(
        self, monkeypatch, long_term_horizon_config
    ):
        import datetime as dt

        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        row = self._q1_row("2024-01-01", q50=100.0, horizon_value=1)

        def fake_api(codes, start_year, end_year, horizon_type=None, horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            return pd.DataFrame([row])

        with patch.object(
            data_reader, "_read_long_forecasts_api", side_effect=fake_api
        ) as mock_api:
            result = data_reader.read_latest_quarterly_forecasts(
                ["19999"], forecast_date=dt.date(2024, 2, 15)
            )

        quarter_calls = [
            c for c in mock_api.call_args_list if c.kwargs.get("horizon_type") == "quarter"
        ]
        assert quarter_calls, "expected a direct quarter API call"
        # Flag OFF: byte-identical single-lead filter, no window expansion.
        assert quarter_calls[0].kwargs.get("horizon_value") == 1
        assert quarter_calls[0].args[1] == 2023  # start_date.year, NOT expanded
        assert not result.empty


class TestReadLatestQuarterlyForecastsSource1LeadAware:
    """FINDING 1: read_latest_quarterly_forecasts Source 1 (monthly

    aggregation) must, under SAPPHIRE_SKILL_LEAD_AWARE, aggregate
    OPERATIONALLY-SELECTED monthly rows (routed through
    read_monthly_forecasts) rather than RAW monthly rows -- so a
    same-target backfill/reissue monthly row cannot leak into the latest
    quarterly output. Flag OFF keeps the raw path (backfill retained),
    mirroring read_quarterly_forecasts' Source 1.
    """

    def _config(self, config_dir, monkeypatch):
        # month_1 (lead 1, issue day 25) drives Source-1 operational
        # selection. The quarter mode must ALSO carry
        # operational_issue_day so Source 2's flag-ON resolution does not
        # fail loud (its API read is mocked empty to isolate Source 1).
        (config_dir / "month_1.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        (config_dir / "quarter.json").write_text(
            json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
        )
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "month_1,quarter")

    def _month_row(self, valid_from, issue_date, q50, horizon_value=99):
        return {
            "horizon_type": "month",
            "horizon_value": horizon_value,
            "code": "19999",
            "date": issue_date,
            "model_type": "LR_Base",
            "valid_from": valid_from,
            "valid_to": valid_from,
            "q50": q50,
            "q05": q50 - 30,
            "q10": q50 - 25,
            "q25": q50 - 15,
            "q75": q50 + 15,
            "q90": q50 + 25,
            "q95": q50 + 30,
            "id": 1,
            "model_type_description": "LR_Base",
        }

    def _rows(self):
        # Operational Q1-2024 monthly issuances (lead 1, issue-day 25):
        #   month 1 issued 2023-12-25, month 2 issued 2024-01-25, q50=100.
        op1 = self._month_row("2024-01-01", "2023-12-25", q50=100.0)
        op2 = self._month_row("2024-02-01", "2024-01-25", q50=100.0)
        # Same-target/same-lead BACKFILL at a NON-operational issue day
        # (day 10). These must be excluded under the flag.
        bf1 = self._month_row("2024-01-01", "2023-12-10", q50=500.0)
        bf2 = self._month_row("2024-02-01", "2024-01-10", q50=500.0)
        return [op1, op2, bf1, bf2]

    def test_flag_on_source1_aggregates_only_operational_monthly(
        self, monkeypatch, long_term_horizon_config
    ):
        import datetime as dt

        self._config(long_term_horizon_config, monkeypatch)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        rows = self._rows()

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return pd.DataFrame()  # isolate Source 1
            return pd.DataFrame(rows)

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake_api):
            result = data_reader.read_latest_quarterly_forecasts(
                ["19999"], forecast_date=dt.date(2024, 4, 15)
            )

        assert not result.empty
        assert set(result["quarter_in_year"]) == {1}
        assert set(result["year"]) == {2024}
        # ONLY the operational rows (q50=100) survive: the aggregated Q1
        # mean is 100, NOT the backfill-contaminated 300.
        assert float(result.iloc[0]["forecasted_discharge"]) == 100.0
        assert float(result.iloc[0]["q50"]) == 100.0
        # ...carrying the DERIVED lead (1), not the stale stored 99.
        assert int(result.iloc[0]["horizon_value"]) == 1

    def test_flag_off_source1_keeps_raw_path_backfill_retained(
        self, monkeypatch, long_term_horizon_config
    ):
        import datetime as dt

        self._config(long_term_horizon_config, monkeypatch)
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        rows = self._rows()

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "quarter":
                return pd.DataFrame()
            return pd.DataFrame(rows)

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake_api):
            result = data_reader.read_latest_quarterly_forecasts(
                ["19999"], forecast_date=dt.date(2024, 4, 15)
            )

        assert not result.empty
        assert set(result["quarter_in_year"]) == {1}
        # Flag OFF: raw path unchanged -- backfill (q50=500) is NOT
        # excluded, so the Q1 mean over all four rows is 300.
        assert float(result.iloc[0]["forecasted_discharge"]) == 300.0


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

    def test_latest_accepts_db_form_lr_and_ensemble_names(self):
        import datetime as dt

        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 8,
                "valid_from": pd.to_datetime(["2024-04-01"] * 8),
                "valid_to": ["2024-09-30"] * 8,
                "model_type": [
                    "LR_BASE",
                    "LR_SM",
                    "ENSEMBLE_MEAN",
                    "NAIVE_MEAN",
                    "SKILLED_MEAN",
                    "GBT",
                    "LR_SM_DT",
                    "SM_GBT_NORM",
                ],
                "q05": [10, 15, 20, 21, 22, 900, 901, 902],
                "q10": [15, 20, 25, 26, 27, 925, 926, 927],
                "q25": [20, 25, 30, 31, 32, 950, 951, 952],
                "q50": [30, 35, 40, 41, 42, 1000, 1001, 1002],
                "q75": [40, 45, 50, 51, 52, 1050, 1051, 1052],
                "q90": [50, 55, 60, 61, 62, 1075, 1076, 1077],
                "q95": [60, 65, 70, 71, 72, 1100, 1101, 1102],
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

        assert set(result["model_short"]) == {
            "LR_BASE",
            "LR_SM",
            "ENSEMBLE_MEAN",
            "NAIVE_MEAN",
            "SKILLED_MEAN",
        }
        assert not set(DEPRECATED_MODEL_FORMS) & set(result["model_short"])

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


class TestReadQuarterlyCombinedForecastsLeadAware:
    def test_flag_on_omits_horizon_value_filter(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        with patch.object(
            data_reader,
            "_read_long_combined_forecasts_api",
            return_value=pd.DataFrame(),
        ) as read_api:
            data_reader.read_quarterly_combined_forecasts()
        assert read_api.call_args.kwargs.get("horizon_value") is None

    def test_flag_off_still_filters_by_configured_lead(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        with patch.object(
            data_reader,
            "_read_long_combined_forecasts_api",
            return_value=pd.DataFrame(),
        ) as read_api:
            data_reader.read_quarterly_combined_forecasts()
        assert read_api.call_args.kwargs["horizon_value"] == 1


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

    def test_quarter_normalization_keeps_horizon_value_when_flag_on(self, monkeypatch):
        """Under SAPPHIRE_SKILL_LEAD_AWARE, horizon_value must survive

        normalization for quarter too (companion to the LOCKED flag-OFF
        test above, which must keep passing unmodified).
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
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

        assert result.iloc[0]["horizon_value"] == 1
