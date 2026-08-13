"""Tests for quarterly/seasonal extensions to api_writer.py.

Phase 4b Step 5.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api_writer import (
    HORIZON_TYPE_TO_API,
    WriteOutcome,
    _write_quarterly_ensemble_to_api,
    _write_seasonal_ensemble_to_api,
    _write_skill_metrics_to_api,
)


def _write_quarter_config(tmp_path, monkeypatch, lead):
    config_dir = tmp_path / "long_term"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "quarter.json").write_text(json.dumps({"operational_month_lead_time": lead}))
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_config_file_station_selection", "missing.json")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")


# ===================================================================
# Horizon mapping
# ===================================================================


class TestHorizonMapping:
    def test_quarter_in_mapping(self):
        assert "quarter" in HORIZON_TYPE_TO_API
        assert HORIZON_TYPE_TO_API["quarter"] == "quarter"

    def test_season_in_mapping(self):
        assert "season" in HORIZON_TYPE_TO_API
        assert HORIZON_TYPE_TO_API["season"] == "season"


# ===================================================================
# Skill metrics writer — quarter/season dispatch
# ===================================================================


class TestSkillMetricsQuarterSeason:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        """Mock API client for all tests in this class."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_skill_metrics.return_value = 1

    def test_quarter_horizon_accepted(self):
        """_write_skill_metrics_to_api accepts horizon_type='quarter'."""
        data = pd.DataFrame(
            {
                "quarter_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_skill_metrics_to_api(data, "quarter", 2025)
        assert result is WriteOutcome.WROTE
        # Verify the record has correct horizon_type
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["horizon_type"] == "quarter"

    def test_quarter_date_computation(self):
        """Quarter skill metric date should be first day of quarter."""
        data = pd.DataFrame(
            {
                "quarter_in_year": [2],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_skill_metrics_to_api(data, "quarter", 2025)
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        # Q2 → April 1
        assert records[0]["date"] == "2025-04-01"

    def test_season_horizon_accepted(self):
        """_write_skill_metrics_to_api accepts horizon_type='season'."""
        data = pd.DataFrame(
            {
                "season_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_skill_metrics_to_api(data, "season", 2025)
        assert result is WriteOutcome.WROTE
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["horizon_type"] == "season"

    def test_season_date_uses_season_start(self, monkeypatch):
        """Season date should be first day of season start month."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "4")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "9")
        data = pd.DataFrame(
            {
                "season_in_year": [1],
                "code": ["S1"],
                "model_short": ["LR"],
                "sdivsigma": [0.5],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.85],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_skill_metrics_to_api(data, "season", 2025)
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert records[0]["date"] == "2025-04-01"

    def test_season_skill_records_keep_each_lead(self, monkeypatch):
        """Seasonal skill uses season_in_year as API horizon_in_year."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "4")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "9")
        self.mock_client.write_skill_metrics.return_value = 4
        data = pd.DataFrame(
            {
                "season_in_year": [3, 2, 1, 0],
                "code": ["PP3_SENTINEL"] * 4,
                "model_short": ["LR"] * 4,
                "sdivsigma": [1.5, 1.0, 0.5, 0.0],
                "nse": [-2.3, -0.5, 0.6, 1.0],
                "delta": [5.0] * 4,
                "accuracy": [0.0, 0.0, 0.0, 1.0],
                "mae": [30.0, 20.0, 10.0, 0.0],
                "n_pairs": [3] * 4,
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_skill_metrics_to_api(data, "season", 2025)

        assert result is WriteOutcome.WROTE
        records = self.mock_client.write_skill_metrics.call_args[0][0]
        assert len(records) == 4
        assert {record["horizon_in_year"] for record in records} == {0, 1, 2, 3}
        assert {record["date"] for record in records} == {"2025-04-01"}
        upsert_keys = {
            (record["code"], record["model_type"], record["date"], record["horizon_in_year"])
            for record in records
        }
        assert len(upsert_keys) == 4


# ===================================================================
# Quarterly ensemble writer
# ===================================================================


class TestQuarterlyEnsembleWriter:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        _write_quarter_config(tmp_path, monkeypatch, lead=1)
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 2

    def test_writes_ensemble_rows(self):
        data = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "model_short": ["EM", "Naive Mean"],
                "forecasted_discharge": [100.0, 95.0],
                "q50": [100.0, 95.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert len(records) == 2
        assert records[0]["horizon_type"] == "quarter"

    def test_valid_from_valid_to(self):
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_quarterly_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-06-30"
        assert records[0]["horizon_value"] == 1

    @pytest.mark.parametrize("lead", [1, 0])
    def test_horizon_value_uses_resolver_config_lead(self, monkeypatch, tmp_path, lead):
        _write_quarter_config(tmp_path, monkeypatch, lead=lead)
        data = pd.DataFrame(
            {
                "code": ["PP4_Q_SENTINEL", "PP4_Q_SENTINEL"],
                "year": [2025, 2025],
                "quarter_in_year": [1, 2],
                "model_short": ["EM", "EM"],
                "forecasted_discharge": [100.0, 110.0],
            }
        )
        self.mock_client.write_long_forecasts.return_value = 2

        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert {record["horizon_value"] for record in records} == {lead}
        assert {record["valid_from"] for record in records} == {
            "2025-01-01",
            "2025-04-01",
        }

    def test_empty_data_returns_false(self):
        result = _write_quarterly_ensemble_to_api(pd.DataFrame())
        assert result is False

    def test_flag_on_row_own_horizon_value_and_date_used(self, monkeypatch):
        """Under the flag, a row carrying its own horizon_value/date is

        written using ITS OWN values, not quarter_horizon_value()/valid_from.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        data = pd.DataFrame(
            {
                "code": ["19999"],
                "year": [2025],
                "quarter_in_year": [1],
                "model_short": ["LR_Base"],
                "forecasted_discharge": [100.0],
                "horizon_value": [3],
                "date": ["2024-10-25"],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["horizon_value"] == 3
        assert records[0]["date"] == "2024-10-25"

    def test_flag_on_row_without_own_horizon_value_falls_back(self, monkeypatch):
        """Under the flag, a row lacking horizon_value/date still falls

        back to quarter_horizon_value()/valid_from (parity with today).
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        data = pd.DataFrame(
            {
                "code": ["19999"],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["horizon_value"] == 1  # quarter_horizon_value() resolver config
        assert records[0]["date"] == "2025-04-01"  # valid_from fallback

    def test_flag_on_aggregation_computed_date_round_trips_to_lead(self, monkeypatch):
        """FIX 6 round-trip: an aggregation-style row (own horizon_value +

        the aggregation-computed representative date = valid_from - hv
        months, with NO separately-supplied issue date) must write a
        record whose (date, valid_from) derives EXACTLY horizon_value.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        from src.aggregation import aggregate_monthly_fc_to_quarterly

        monthly = pd.DataFrame(
            {
                "code": ["19999", "19999"],
                "year": [2024, 2024],
                "month": [1, 2],
                "model_short": ["EM", "EM"],
                "horizon_value": [1, 1],
                "q05": [10.0, 20.0],
                "q10": [15.0, 25.0],
                "q25": [20.0, 30.0],
                "q50": [30.0, 40.0],
                "q75": [40.0, 50.0],
                "q90": [50.0, 60.0],
                "q95": [60.0, 70.0],
                "forecasted_discharge": [30.0, 40.0],
            }
        )
        aggregated = aggregate_monthly_fc_to_quarterly(monthly)
        assert "date" in aggregated.columns  # FIX 6 carried it through

        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_quarterly_ensemble_to_api(aggregated)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        rec = records[0]
        vf = pd.Timestamp(rec["valid_from"])
        d = pd.Timestamp(rec["date"])
        derived_lead = (vf.year - d.year) * 12 + (vf.month - d.month)
        assert derived_lead == rec["horizon_value"] == 1


# ===================================================================
# Seasonal ensemble writer
# ===================================================================


class TestSeasonalEnsembleWriter:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 1

    def test_writes_ensemble_rows(self):
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2025],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_seasonal_ensemble_to_api(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["horizon_type"] == "season"
        assert records[0]["horizon_value"] == 1

    def test_four_issue_rows_keep_lead_date_and_target_season(self):
        data = pd.DataFrame(
            {
                "code": ["PP4_S_SENTINEL"] * 4,
                "season_year": [2025] * 4,
                "season_in_year": [3, 2, 1, 0],
                "date": [
                    "2025-01-01",
                    "2025-02-01",
                    "2025-03-01",
                    "2025-04-01",
                ],
                "model_short": ["EM", "Naive Mean", "Skilled Mean", "EM"],
                "forecasted_discharge": [100.0, 110.0, 120.0, 130.0],
            }
        )
        self.mock_client.write_long_forecasts.return_value = 4

        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            result = _write_seasonal_ensemble_to_api(data)

        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        natural_keys = {
            (
                record["horizon_value"],
                record["date"],
                record["valid_from"],
                record["valid_to"],
            )
            for record in records
        }
        assert natural_keys == {
            (3, "2025-01-01", "2025-04-01", "2025-09-30"),
            (2, "2025-02-01", "2025-04-01", "2025-09-30"),
            (1, "2025-03-01", "2025-04-01", "2025-09-30"),
            (0, "2025-04-01", "2025-04-01", "2025-09-30"),
        }

    def test_valid_from_valid_to_default_season(self):
        """Default season Apr-Sep → valid_from=Apr 1, valid_to=Sep 30."""
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2025],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_seasonal_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-09-30"

    def test_cross_year_season(self, monkeypatch):
        """Oct-Mar season crosses year boundary."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")
        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2024],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            _write_seasonal_ensemble_to_api(data)
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2024-10-01"
        assert records[0]["valid_to"] == "2025-03-31"

    def test_empty_data_returns_false(self):
        result = _write_seasonal_ensemble_to_api(pd.DataFrame())
        assert result is False
