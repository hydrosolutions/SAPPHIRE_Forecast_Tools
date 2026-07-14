"""Tests for M1 P1 flag-gated wiring of `select_operational_issuances`

into the raw long-forecast readers (`read_monthly_forecasts`,
`read_quarterly_forecasts`, `read_seasonal_forecasts`), and the
read-window-expansion helpers.

Covers:
- flag OFF is byte-identical to trunk (no resolver calls, no window
  expansion, `select_operational_issuances` never invoked);
- flag ON expands the API issue-date read window backward by the max
  configured lead and trims the selected output back to the requested
  target-year range (the "Jan / Q1 boundary" case: a January (resp. Q1)
  target's operational issuance was made in the PRIOR calendar year).
"""

import json
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import data_reader

CODE = "19999"


def _write_mode_config(config_dir, mode, lead, issue_day):
    (config_dir / f"{mode}.json").write_text(
        json.dumps({"operational_month_lead_time": lead, "operational_issue_day": issue_day})
    )


def _set_long_term_env(monkeypatch, tmp_path, modes):
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes))


def _month_forecast_row(code, year, month, issue_date, model="LR_Base", horizon_value=99):
    valid_from = f"{year}-{month:02d}-01"
    return {
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_from,
        "q50": 100.0,
        "q05": 70.0,
        "q10": 75.0,
        "q25": 85.0,
        "q75": 115.0,
        "q90": 125.0,
        "q95": 130.0,
        "id": 1,
        "model_type_description": model,
    }


class TestReadWindowExpansionYears:
    @pytest.mark.parametrize(
        ("max_lead_months", "expected_years"),
        [(0, 0), (-1, 0), (1, 1), (3, 1), (12, 1), (13, 2), (24, 2), (25, 3)],
    )
    def test_ceil_division_by_twelve(self, max_lead_months, expected_years):
        assert data_reader._read_window_expansion_years(max_lead_months) == expected_years


class TestTrimToTargetYearRange:
    def test_trims_years_outside_range(self):
        df = pd.DataFrame({"year": [2022, 2023, 2024, 2025], "value": [1, 2, 3, 4]})
        result = data_reader._trim_to_target_year_range(df, "year", 2023, 2024)
        assert sorted(result["year"]) == [2023, 2024]

    def test_empty_input_returns_empty(self):
        df = pd.DataFrame()
        result = data_reader._trim_to_target_year_range(df, "year", 2023, 2024)
        assert result.empty

    def test_missing_year_col_returns_unchanged(self):
        df = pd.DataFrame({"other": [1, 2]})
        result = data_reader._trim_to_target_year_range(df, "year", 2023, 2024)
        pd.testing.assert_frame_equal(result, df)


class TestReadMonthlyForecastsFlagOff:
    def test_flag_off_never_resolves_schedules_or_expands_window(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        api_df = pd.DataFrame([_month_forecast_row(CODE, 2024, 6, "2024-06-01", horizon_value=6)])

        with (
            patch("src.data_reader._read_long_forecasts_api", return_value=api_df) as mock_api,
            patch.object(data_reader, "_operational_schedules_for_horizon_type") as mock_resolve,
        ):
            result = data_reader.read_monthly_forecasts([CODE], 2024, 2024)

        mock_resolve.assert_not_called()
        mock_api.assert_called_once_with([CODE], 2024, 2024)
        assert len(result) == 1
        # flag OFF: horizon_value is untouched (not overwritten by selection)
        assert result.iloc[0]["horizon_value"] == 6


class TestReadMonthlyForecastsJanuaryBoundary:
    """A January target forecast issued at lead=3 was issued in OCTOBER of

    the PRIOR calendar year. If the read window is not expanded backward,
    a request for start_year=end_year=Y misses it entirely (the API is
    queried for [Y-01-01, Y-12-31]).
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_3"])
        _write_mode_config(config_dir, "month_3", lead=3, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_expands_window_and_selects_prior_year_issuance(self):
        target_year = 2024
        operational_row = _month_forecast_row(
            CODE, target_year, 1, issue_date="2023-10-25", horizon_value=0
        )

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            # Only the EXPANDED window (start_year <= 2023) sees the
            # October-2023 issuance for the January-2024 target.
            if start_year <= 2023:
                return pd.DataFrame([operational_row])
            return pd.DataFrame()

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api:
            result = data_reader.read_monthly_forecasts([CODE], target_year, target_year)

        # Confirms the window was actually expanded backward.
        called_start_year = mock_api.call_args.args[1]
        assert called_start_year <= target_year - 1

        assert len(result) == 1
        assert result.iloc[0]["year"] == target_year
        assert result.iloc[0]["month"] == 1
        assert result.iloc[0]["horizon_value"] == 3

    def test_unexpanded_window_would_have_missed_the_row(self):
        """Sanity check on the fixture itself: WITHOUT expansion (i.e. a

        direct start_year=end_year=target_year call), the operational
        October-of-prior-year issuance is invisible.
        """
        target_year = 2024
        operational_row = _month_forecast_row(
            CODE, target_year, 1, issue_date="2023-10-25", horizon_value=0
        )

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if start_year <= 2023:
                return pd.DataFrame([operational_row])
            return pd.DataFrame()

        result = fake_api([CODE], target_year, target_year)
        assert result.empty


class TestFlagOnScheduleResolutionFailsLoud:
    """LOCKED regression (HIGH defect #3): under flag-ON, a config

    resolution failure (e.g. a supported month_N mode missing
    operational_issue_day) must PROPAGATE -- never be swallowed into a
    silent unfiltered read that retains backfill rows.
    """

    @pytest.fixture(autouse=True)
    def _broken_config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_1"])
        # month_1 config is missing operational_issue_day -> resolver raises.
        (config_dir / "month_1.json").write_text(json.dumps({"operational_month_lead_time": 1}))
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_read_monthly_forecasts_raises_and_does_not_read_api(self):
        from long_term_horizon_resolver import LongTermHorizonResolverError

        with patch("src.data_reader._read_long_forecasts_api") as mock_api:
            with pytest.raises(LongTermHorizonResolverError, match="operational_issue_day"):
                data_reader.read_monthly_forecasts([CODE], 2024, 2024)

        # Fail loud BEFORE any unfiltered read happens.
        mock_api.assert_not_called()


class TestReadQuarterlyForecastsQ1Boundary:
    """A Q1 (Jan-Mar) target forecast issued at lead=1 was issued in

    DECEMBER of the PRIOR calendar year.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_expands_window_and_selects_prior_year_issuance(self):
        target_year = 2024
        operational_row = {
            "horizon_type": "quarter",
            "horizon_value": 99,
            "code": CODE,
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
                return pd.DataFrame()  # no aggregated-from-monthly source in this test
            if start_year <= 2023:
                return pd.DataFrame([operational_row])
            return pd.DataFrame()

        with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api:
            result = data_reader.read_quarterly_forecasts([CODE], target_year, target_year)

        quarter_calls = [
            c for c in mock_api.call_args_list if c.kwargs.get("horizon_type") == "quarter"
        ]
        assert quarter_calls, "expected a direct quarter API call"
        assert quarter_calls[0].args[1] <= target_year - 1

        assert len(result) == 1
        assert result.iloc[0]["year"] == target_year
        assert result.iloc[0]["quarter_in_year"] == 1
        # P1b: horizon_value must survive the rest of the quarter path
        # (previously silently stripped by column selection / normalization).
        assert result.iloc[0]["horizon_value"] == 1
