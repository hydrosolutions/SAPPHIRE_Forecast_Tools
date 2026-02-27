"""Tests for validate_pipeline.py.

Covers early-exit checks, Tier 1 (presence), Tier 2 (correctness),
Tier 3 (consistency), module attribution, and the CLI entry point.
"""

import os
import sys
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import validate_pipeline as vp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_pre_client():
    """Mock SapphirePreprocessingClient that passes readiness check."""
    client = MagicMock()
    client.readiness_check.return_value = True
    return client


@pytest.fixture()
def mock_post_client():
    """Mock SapphirePostprocessingClient that passes readiness check."""
    client = MagicMock()
    client.readiness_check.return_value = True
    return client


@pytest.fixture()
def sample_runoff_df():
    """Sample runoff DataFrame with two stations."""
    return pd.DataFrame({
        "code": ["15001", "15001", "15002", "15002"],
        "date": ["2026-02-23"] * 4,
        "discharge": [10.5, 12.0, 8.3, 9.1],
        "horizon_type": ["day", "day", "day", "day"],
    })


@pytest.fixture()
def sample_forecast_df():
    """Sample forecast DataFrame with LR model, two stations."""
    return pd.DataFrame({
        "code": ["15001", "15002"],
        "date": ["2026-02-23", "2026-02-23"],
        "forecasted_discharge": [11.0, 8.5],
        "model_type": ["LR", "LR"],
        "horizon_type": ["pentad", "pentad"],
    })


@pytest.fixture()
def sample_ml_forecast_df():
    """ML forecast DataFrame with quantile columns."""
    return pd.DataFrame({
        "code": ["15001", "15002"],
        "date": ["2026-02-23", "2026-02-23"],
        "forecasted_discharge": [11.0, 8.5],
        "model_type": ["TFT", "TFT"],
        "q05": [8.0, 6.0],
        "q25": [9.5, 7.0],
        "q50": [11.0, 8.5],
        "q75": [12.5, 10.0],
        "q95": [14.0, 11.5],
    })


@pytest.fixture()
def sample_skill_df():
    """Sample skill metrics DataFrame."""
    return pd.DataFrame({
        "code": ["15001", "15002"],
        "model_type": ["LR", "LR"],
        "nse": [0.85, 0.72],
        "accuracy": [78.0, 65.0],
        "n_pairs": [50, 45],
    })


# ---------------------------------------------------------------------------
# Early-exit checks
# ---------------------------------------------------------------------------


class TestEarlyExitChecks:
    """Tests for conditions that cause the script to skip validation."""

    def test_api_unavailable_exits_zero(self):
        """When sapphire_api_client is not installed, exit 0."""
        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", False):
            rc = vp.main(["--target", "short-term"])
            assert rc == 0

    def test_api_disabled_exits_zero(self, monkeypatch):
        """When SAPPHIRE_API_ENABLED=false, exit 0."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "false")
        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", True):
            rc = vp.main(["--target", "short-term"])
            assert rc == 0

    def test_api_enabled_unset_does_not_skip(self, monkeypatch):
        """When SAPPHIRE_API_ENABLED is not set, validation proceeds."""
        monkeypatch.delenv("SAPPHIRE_API_ENABLED", raising=False)
        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(vp, "validate", return_value=0) as mock_val:
            rc = vp.main(["--target", "short-term"])
            assert rc == 0
            mock_val.assert_called_once()


# ---------------------------------------------------------------------------
# Readiness checks
# ---------------------------------------------------------------------------


class TestReadinessChecks:
    """Tests for API readiness check handling."""

    def test_preprocessing_not_ready_produces_fail(self):
        """When preprocessing readiness_check returns False, FAIL result."""
        mock_pre = MagicMock()
        mock_pre.readiness_check.return_value = False
        mock_post = MagicMock()
        mock_post.readiness_check.return_value = True
        # Mock empty tier 1 to avoid attribute errors
        mock_post.read_short_term_forecasts.return_value = pd.DataFrame()
        mock_post.read_lr_forecasts.return_value = pd.DataFrame()
        mock_post.read_skill_metrics.return_value = pd.DataFrame()

        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(vp, "SapphirePreprocessingClient",
                          return_value=mock_pre), \
             patch.object(vp, "SapphirePostprocessingClient",
                          return_value=mock_post):
            rc = vp.validate(
                "short-term", date(2026, 2, 23), ["pentad"],
            )
            assert rc == 1  # FAIL because preprocessing not ready

    def test_postprocessing_not_ready_produces_fail(self):
        """When postprocessing readiness_check returns False, FAIL result."""
        mock_pre = MagicMock()
        mock_pre.readiness_check.return_value = True
        mock_post = MagicMock()
        mock_post.readiness_check.return_value = False

        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(vp, "SapphirePreprocessingClient",
                          return_value=mock_pre), \
             patch.object(vp, "SapphirePostprocessingClient",
                          return_value=mock_post):
            rc = vp.validate(
                "short-term", date(2026, 2, 23), ["pentad"],
            )
            assert rc == 1

    def test_readiness_exception_treated_as_not_ready(self):
        """If readiness_check raises, treat as not ready (FAIL)."""
        mock_pre = MagicMock()
        mock_pre.readiness_check.side_effect = ConnectionError("refused")
        mock_post = MagicMock()
        mock_post.readiness_check.side_effect = ConnectionError("refused")

        with patch.object(vp, "SAPPHIRE_API_AVAILABLE", True), \
             patch.object(vp, "SapphirePreprocessingClient",
                          return_value=mock_pre), \
             patch.object(vp, "SapphirePostprocessingClient",
                          return_value=mock_post):
            rc = vp.validate(
                "short-term", date(2026, 2, 23), ["pentad"],
            )
            assert rc == 1


# ---------------------------------------------------------------------------
# Tier 1: Data Presence
# ---------------------------------------------------------------------------


class TestTier1Presence:
    """Tests for Tier 1 data presence checks."""

    def test_check_presence_pass_on_non_empty_df(self, mock_post_client):
        """Non-empty DataFrame from API -> PASS."""
        mock_post_client.read_short_term_forecasts.return_value = (
            pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        )
        result = vp.check_presence(
            mock_post_client, "read_short_term_forecasts",
            "Forecasts (LR, pentad)",
            module="linear_regression",
            horizon="pentad", model="LR",
            start_date="2026-02-23", end_date="2026-02-23",
        )
        assert result.status == "PASS"
        assert result.record_count == 1
        assert result.data is not None
        assert result.module == "linear_regression"

    def test_check_presence_fail_on_empty_df(self, mock_post_client):
        """Empty DataFrame from API -> FAIL."""
        mock_post_client.read_short_term_forecasts.return_value = (
            pd.DataFrame()
        )
        result = vp.check_presence(
            mock_post_client, "read_short_term_forecasts",
            "Forecasts (LR, pentad)",
            module="linear_regression",
            horizon="pentad", model="LR",
            start_date="2026-02-23", end_date="2026-02-23",
        )
        assert result.status == "FAIL"
        assert result.record_count == 0
        assert "no records" in result.detail
        assert result.module == "linear_regression"

    def test_check_presence_warn_when_configured(self, mock_pre_client):
        """Empty DataFrame with warn_if_empty=True -> WARN."""
        mock_pre_client.read_snow.return_value = pd.DataFrame()
        result = vp.check_presence(
            mock_pre_client, "read_snow", "Snow (SWE)",
            module="preprocessing_gateway",
            snow_type="SWE",
            start_date="2026-02-23", end_date="2026-02-23",
            warn_if_empty=True,
        )
        assert result.status == "WARN"
        assert "may not be configured" in result.detail

    def test_check_presence_fail_on_api_exception(self, mock_pre_client):
        """API exception -> FAIL with error detail."""
        mock_pre_client.read_runoff.side_effect = RuntimeError("timeout")
        result = vp.check_presence(
            mock_pre_client, "read_runoff", "Runoff (day)",
            module="preprocessing_runoff",
            horizon="day", start_date="2026-02-23", end_date="2026-02-23",
        )
        assert result.status == "FAIL"
        assert "API error" in result.detail
        assert result.module == "preprocessing_runoff"

    def test_tier1_short_term_returns_expected_check_count(
        self, mock_pre_client, mock_post_client,
    ):
        """Short-term Tier 1 produces the expected number of checks.

        Expected checks:
        - 1 runoff (day) + 1 hydrograph (day) + 2 meteo + 1 snow = 5
        - 6 forecast models + 1 LR details + 1 skill metrics = 8
        Total: 13
        """
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        mock_pre_client.read_runoff.return_value = single_row
        mock_pre_client.read_hydrograph.return_value = single_row
        mock_pre_client.read_meteo.return_value = single_row
        mock_pre_client.read_snow.return_value = single_row
        mock_post_client.read_short_term_forecasts.return_value = single_row
        mock_post_client.read_lr_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_short_term(
            mock_pre_client, mock_post_client,
            date(2026, 2, 23), "pentad",
        )
        # 1 runoff (day) + 1 hydrograph (day) + 2 meteo + 1 snow
        # + 6 forecast models + 1 LR details + 1 skill = 13
        assert len(results) == 13
        assert all(r.status == "PASS" for r in results)

    def test_tier1_no_pentad_runoff_or_hydrograph(
        self, mock_pre_client, mock_post_client,
    ):
        """Tier 1 should NOT check pentad/decade horizon for runoff
        or hydrograph — only day horizon exists in the API.
        """
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        mock_pre_client.read_runoff.return_value = single_row
        mock_pre_client.read_hydrograph.return_value = single_row
        mock_pre_client.read_meteo.return_value = single_row
        mock_pre_client.read_snow.return_value = single_row
        mock_post_client.read_short_term_forecasts.return_value = single_row
        mock_post_client.read_lr_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_short_term(
            mock_pre_client, mock_post_client,
            date(2026, 2, 23), "pentad",
        )
        check_names = [r.name for r in results]
        # No pentad/decade runoff or hydrograph checks
        assert "Runoff (pentad)" not in check_names
        assert "Runoff (decade)" not in check_names
        assert "Hydrograph (pentad)" not in check_names
        assert "Hydrograph (decade)" not in check_names
        # Day-horizon checks are present
        assert "Runoff (day)" in check_names
        assert "Hydrograph (day)" in check_names


# ---------------------------------------------------------------------------
# Module attribution
# ---------------------------------------------------------------------------


class TestModuleAttribution:
    """Tests that each check is linked to its source pipeline module."""

    def test_tier1_short_term_module_mapping(
        self, mock_pre_client, mock_post_client,
    ):
        """Each Tier 1 check has the correct module attribution."""
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        mock_pre_client.read_runoff.return_value = single_row
        mock_pre_client.read_hydrograph.return_value = single_row
        mock_pre_client.read_meteo.return_value = single_row
        mock_pre_client.read_snow.return_value = single_row
        mock_post_client.read_short_term_forecasts.return_value = single_row
        mock_post_client.read_lr_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_short_term(
            mock_pre_client, mock_post_client,
            date(2026, 2, 23), "pentad",
        )
        by_name = {r.name: r.module for r in results}

        # Preprocessing modules
        assert by_name["Runoff (day)"] == "preprocessing_runoff"
        assert by_name["Hydrograph (day)"] == "preprocessing_runoff"
        assert by_name["Meteo (T)"] == "preprocessing_gateway"
        assert by_name["Meteo (P)"] == "preprocessing_gateway"
        assert by_name["Snow (SWE)"] == "preprocessing_gateway"

        # Forecast models mapped to their source modules
        assert by_name["Forecasts (LR, pentad)"] == "linear_regression"
        assert by_name["Forecasts (TFT, pentad)"] == "machine_learning"
        assert by_name["Forecasts (TiDE, pentad)"] == "machine_learning"
        assert by_name["Forecasts (TSMixer, pentad)"] == "machine_learning"
        assert by_name["Forecasts (EM, pentad)"] == "postprocessing_forecasts"
        assert by_name["Forecasts (NE, pentad)"] == "postprocessing_forecasts"

        # LR details and skill metrics
        assert by_name["LR details (pentad)"] == "linear_regression"
        assert by_name["Skill metrics (pentad)"] == "postprocessing_forecasts"

    def test_tier1_long_term_module_mapping(self, mock_post_client):
        """Long-term checks have correct module attribution."""
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_long_term(
            mock_post_client, date(2026, 2, 23),
        )
        by_name = {r.name: r.module for r in results}
        assert by_name["Long-term forecasts (month)"] == (
            "long_term_forecasting"
        )
        assert by_name["Monthly skill metrics"] == "postprocessing_forecasts"

    def test_module_shown_in_output(self, capsys):
        """Module attribution appears in printed output."""
        results = [
            vp.CheckResult(
                "Runoff (day)", "PASS", detail="15 records",
                module="preprocessing_runoff",
            ),
        ]
        vp.print_results("Test Section", results)
        captured = capsys.readouterr()
        assert "[preprocessing_runoff]" in captured.out


# ---------------------------------------------------------------------------
# Tier 2: Data Correctness
# ---------------------------------------------------------------------------


class TestTier2Correctness:
    """Tests for Tier 2 data correctness checks."""

    def test_discharge_non_negative_pass(self, sample_runoff_df):
        """All positive discharge values -> PASS."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", data=sample_runoff_df)
        ]
        check = vp.check_discharge_non_negative(results)
        assert check.status == "PASS"

    def test_discharge_non_negative_fail(self):
        """Negative discharge values -> FAIL."""
        df = pd.DataFrame({
            "discharge": [10.0, -5.0, 8.0],
        })
        results = [vp.CheckResult("Runoff (day)", "PASS", data=df)]
        check = vp.check_discharge_non_negative(results)
        assert check.status == "FAIL"
        assert "1 records with negative" in check.detail

    def test_discharge_non_negative_skip_no_data(self):
        """No discharge data -> SKIP."""
        results = [vp.CheckResult("Empty", "FAIL", data=pd.DataFrame())]
        check = vp.check_discharge_non_negative(results)
        assert check.status == "SKIP"

    def test_no_nan_in_forecasts_pass(self, sample_forecast_df):
        """All forecasts present -> PASS."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "PASS", data=sample_forecast_df,
            )
        ]
        check = vp.check_no_nan_in_forecasts(results)
        assert check.status == "PASS"
        assert "2 values present" in check.detail

    def test_no_nan_in_forecasts_warn_on_nan(self):
        """NaN in forecasted_discharge -> WARN."""
        df = pd.DataFrame({
            "forecasted_discharge": [11.0, float("nan"), 8.5],
        })
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)
        ]
        check = vp.check_no_nan_in_forecasts(results)
        assert check.status == "WARN"
        assert "1/3" in check.detail

    def test_quantile_ordering_pass(self, sample_ml_forecast_df):
        """Properly ordered quantiles -> PASS."""
        results = [
            vp.CheckResult(
                "Forecasts (TFT, pentad)", "PASS",
                data=sample_ml_forecast_df,
            )
        ]
        check = vp.check_quantile_ordering(results)
        assert check.status == "PASS"

    def test_quantile_ordering_fail_on_disordered(self):
        """Disordered quantiles -> FAIL."""
        df = pd.DataFrame({
            "q05": [8.0],
            "q25": [12.0],  # q25 > q50
            "q50": [11.0],
            "q75": [10.0],  # q75 < q50
            "q95": [14.0],
        })
        results = [
            vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=df)
        ]
        check = vp.check_quantile_ordering(results)
        assert check.status == "FAIL"
        assert "disordered" in check.detail

    def test_quantile_ordering_skip_no_quantiles(self):
        """No quantile columns -> SKIP."""
        df = pd.DataFrame({"forecasted_discharge": [11.0]})
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)
        ]
        check = vp.check_quantile_ordering(results)
        assert check.status == "SKIP"

    def test_expected_models_pass(self):
        """All expected models found -> PASS."""
        results = []
        for model in vp.SHORT_TERM_MODELS:
            df = pd.DataFrame({
                "model_type": [model],
                "code": ["15001"],
                "date": ["2026-02-23"],
            })
            results.append(
                vp.CheckResult(f"Forecasts ({model}, pentad)", "PASS", data=df)
            )
        check = vp.check_expected_models(results, "pentad")
        assert check.status == "PASS"

    def test_expected_models_fail_missing(self):
        """Missing models -> FAIL with names."""
        # Only LR present
        df = pd.DataFrame({"model_type": ["LR"]})
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)
        ]
        check = vp.check_expected_models(results, "pentad")
        assert check.status == "FAIL"
        assert "missing" in check.detail
        assert "TFT" in check.detail

    def test_skill_metric_ranges_pass(self, sample_skill_df):
        """Valid skill metrics -> PASS."""
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS",
                           data=sample_skill_df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "PASS"

    def test_skill_metric_ranges_fail_nse_above_1(self):
        """NSE > 1.0 -> FAIL."""
        df = pd.DataFrame({
            "nse": [0.85, 1.5],
            "accuracy": [78.0, 65.0],
            "n_pairs": [50, 45],
        })
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "NSE > 1.0" in check.detail

    def test_skill_metric_ranges_fail_accuracy_out_of_range(self):
        """Accuracy outside [0, 100] -> FAIL."""
        df = pd.DataFrame({
            "nse": [0.85],
            "accuracy": [105.0],
            "n_pairs": [50],
        })
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "accuracy" in check.detail

    def test_skill_metric_ranges_warn_n_pairs_zero(self):
        """n_pairs <= 0 -> WARN (not FAIL) since new stations may lack data."""
        df = pd.DataFrame({
            "nse": [0.85],
            "accuracy": [78.0],
            "n_pairs": [0],
        })
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "WARN"
        assert "n_pairs" in check.detail
        assert "new stations" in check.detail

    def test_skill_metric_ranges_fail_nse_overrides_n_pairs_warn(self):
        """When both NSE > 1.0 and n_pairs <= 0, status is FAIL."""
        df = pd.DataFrame({
            "nse": [1.5],
            "accuracy": [78.0],
            "n_pairs": [0],
        })
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "NSE > 1.0" in check.detail
        assert "n_pairs" in check.detail

    def test_skill_metric_has_module(self):
        """Skill metric check result has postprocessing_forecasts module."""
        df = pd.DataFrame({
            "nse": [0.85],
            "accuracy": [78.0],
            "n_pairs": [50],
        })
        results = [
            vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)
        ]
        check = vp.check_skill_metric_ranges(results)
        assert check.module == "postprocessing_forecasts"


# ---------------------------------------------------------------------------
# Tier 3: Cross-module Consistency
# ---------------------------------------------------------------------------


class TestTier3Consistency:
    """Tests for Tier 3 cross-module consistency checks."""

    def test_station_codes_match_pass(self):
        """Forecast codes <= runoff codes -> PASS."""
        runoff = pd.DataFrame({
            "code": ["15001", "15002", "15003"],
            "date": ["2026-02-23"] * 3,
        })
        forecasts = pd.DataFrame({
            "code": ["15001", "15002"],
            "date": ["2026-02-23"] * 2,
        })
        results = [
            vp.CheckResult("Runoff (day)", "PASS", data=runoff),
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=forecasts),
        ]
        check = vp.check_station_codes_match(results)
        assert check.status == "PASS"

    def test_station_codes_match_warn_extra_codes(self):
        """Forecast has codes not in runoff -> WARN."""
        runoff = pd.DataFrame({
            "code": ["15001"],
            "date": ["2026-02-23"],
        })
        forecasts = pd.DataFrame({
            "code": ["15001", "15999"],
            "date": ["2026-02-23"] * 2,
        })
        results = [
            vp.CheckResult("Runoff (day)", "PASS", data=runoff),
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=forecasts),
        ]
        check = vp.check_station_codes_match(results)
        assert check.status == "WARN"
        assert "15999" in check.detail

    def test_station_codes_match_skip_no_data(self):
        """Insufficient data -> SKIP."""
        results = [
            vp.CheckResult("Empty", "FAIL", data=pd.DataFrame())
        ]
        check = vp.check_station_codes_match(results)
        assert check.status == "SKIP"

    def test_dates_consistent_pass(self):
        """All models have same (code, date) tuples -> PASS."""
        base = pd.DataFrame({
            "code": ["15001", "15002"],
            "date": ["2026-02-23", "2026-02-23"],
        })
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=base),
            vp.CheckResult("Forecasts (TFT, pentad)", "PASS",
                           data=base.copy()),
        ]
        check = vp.check_dates_consistent(results)
        assert check.status == "PASS"
        assert "2 (code, date) tuples" in check.detail

    def test_dates_consistent_warn_mismatch(self):
        """Models have different (code, date) coverage -> WARN."""
        lr_df = pd.DataFrame({
            "code": ["15001", "15002"],
            "date": ["2026-02-23", "2026-02-23"],
        })
        tft_df = pd.DataFrame({
            "code": ["15001"],
            "date": ["2026-02-23"],
        })
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=lr_df),
            vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=tft_df),
        ]
        check = vp.check_dates_consistent(results)
        assert check.status == "WARN"
        assert "missing" in check.detail


# ---------------------------------------------------------------------------
# Horizon resolution
# ---------------------------------------------------------------------------


class TestForecastDayHelpers:
    """Tests for is_pentad_forecast_day and is_decad_forecast_day."""

    @pytest.mark.parametrize("day,expected", [
        (5, True), (10, True), (15, True),
        (20, True), (25, True), (28, True),  # 28 = last day of Feb 2026
        (1, False), (6, False), (14, False), (19, False), (27, False),
    ])
    def test_pentad_forecast_days_feb(self, day, expected):
        """Feb 2026 pentad forecast days: 5,10,15,20,25,28."""
        assert vp.is_pentad_forecast_day(date(2026, 2, day)) is expected

    @pytest.mark.parametrize("day,expected", [
        (10, True), (20, True), (31, True),  # 31 = last day of Jan
        (5, False), (15, False), (25, False), (1, False),
    ])
    def test_decad_forecast_days_jan(self, day, expected):
        """Jan 2026 decad forecast days: 10,20,31."""
        assert vp.is_decad_forecast_day(date(2026, 1, day)) is expected

    def test_pentad_last_day_varies_by_month(self):
        """Last day of month is a pentad forecast day for all months."""
        # 30-day month
        assert vp.is_pentad_forecast_day(date(2026, 4, 30)) is True
        assert vp.is_pentad_forecast_day(date(2026, 4, 29)) is False
        # 31-day month
        assert vp.is_pentad_forecast_day(date(2026, 3, 31)) is True
        assert vp.is_pentad_forecast_day(date(2026, 3, 30)) is False
        # Leap year Feb 29
        assert vp.is_pentad_forecast_day(date(2024, 2, 29)) is True
        assert vp.is_pentad_forecast_day(date(2024, 2, 28)) is False

    def test_decad_last_day_varies_by_month(self):
        """Last day of month is a decad forecast day for all months."""
        assert vp.is_decad_forecast_day(date(2026, 4, 30)) is True
        assert vp.is_decad_forecast_day(date(2024, 2, 29)) is True


class TestNonForecastDaySkip:
    """Tests for _apply_non_forecast_day_skip."""

    def test_skip_on_non_pentad_day(self):
        """FAIL with 0 records from forecast module -> SKIP on non-pentad day."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "FAIL",
                detail="no records", record_count=0,
                module="linear_regression",
            ),
            vp.CheckResult(
                "Runoff (day)", "FAIL",
                detail="no records", record_count=0,
                module="preprocessing_runoff",
            ),
        ]
        # Feb 23 is NOT a pentad forecast day
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")

        assert results[0].status == "SKIP"
        assert "not a pentad forecast day" in results[0].detail
        # Preprocessing check is NOT changed
        assert results[1].status == "FAIL"

    def test_no_skip_on_pentad_day(self):
        """FAIL stays FAIL on an actual pentad forecast day."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "FAIL",
                detail="no records", record_count=0,
                module="linear_regression",
            ),
        ]
        # Feb 25 IS a pentad forecast day
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 25), "pentad")
        assert results[0].status == "FAIL"

    def test_no_skip_when_records_exist(self):
        """FAIL with record_count > 0 stays FAIL (real data issue)."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "FAIL",
                detail="API error: timeout", record_count=5,
                module="linear_regression",
            ),
        ]
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")
        assert results[0].status == "FAIL"

    def test_pass_not_changed(self):
        """PASS results are never touched."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "PASS",
                detail="50 records", record_count=50,
                module="linear_regression",
            ),
        ]
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")
        assert results[0].status == "PASS"

    def test_skip_on_non_decad_day(self):
        """FAIL -> SKIP on non-decad day for decade horizon."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, decade)", "FAIL",
                detail="no records", record_count=0,
                module="linear_regression",
            ),
        ]
        # Feb 25 is a pentad day but NOT a decad day
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 25), "decade")
        assert results[0].status == "SKIP"
        assert "not a decade forecast day" in results[0].detail

    def test_all_forecast_modules_affected(self):
        """All three forecast-day modules get SKIP on non-forecast day."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)", "FAIL",
                detail="no records", record_count=0,
                module="linear_regression",
            ),
            vp.CheckResult(
                "Forecasts (TFT, pentad)", "FAIL",
                detail="no records", record_count=0,
                module="machine_learning",
            ),
            vp.CheckResult(
                "Skill metrics (pentad)", "FAIL",
                detail="no records", record_count=0,
                module="postprocessing_forecasts",
            ),
        ]
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")
        assert all(r.status == "SKIP" for r in results)

    def test_long_term_never_skipped(self):
        """Long-term horizon defaults to True (always a forecast day).

        We can't predict long-term schedule, so we don't downgrade FAILs.
        """
        results = [
            vp.CheckResult(
                "Long-term forecasts (month)", "FAIL",
                detail="no records", record_count=0,
                module="long_term_forecasting",
            ),
        ]
        vp._apply_non_forecast_day_skip(
            results, date(2026, 2, 23), "long-term",
        )
        # long_term_forecasting is not in FORECAST_DAY_MODULES
        assert results[0].status == "FAIL"


class TestHorizonResolution:
    """Tests for resolve_horizons()."""

    def test_explicit_horizon_override(self):
        """Explicit --horizon argument takes precedence."""
        assert vp.resolve_horizons("pentad") == ["pentad"]
        assert vp.resolve_horizons("decade") == ["decade"]

    def test_mode_pentad(self, monkeypatch):
        """SAPPHIRE_PREDICTION_MODE=PENTAD -> ['pentad']."""
        monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "PENTAD")
        assert vp.resolve_horizons(None) == ["pentad"]

    def test_mode_decad(self, monkeypatch):
        """SAPPHIRE_PREDICTION_MODE=DECAD -> ['decade']."""
        monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "DECAD")
        assert vp.resolve_horizons(None) == ["decade"]

    def test_mode_both(self, monkeypatch):
        """SAPPHIRE_PREDICTION_MODE=BOTH -> ['pentad', 'decade']."""
        monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "BOTH")
        assert vp.resolve_horizons(None) == ["pentad", "decade"]

    def test_mode_unset_defaults_pentad(self, monkeypatch):
        """No mode set -> ['pentad']."""
        monkeypatch.delenv("SAPPHIRE_PREDICTION_MODE", raising=False)
        assert vp.resolve_horizons(None) == ["pentad"]


# ---------------------------------------------------------------------------
# Exit code logic
# ---------------------------------------------------------------------------


class TestExitCode:
    """Tests for exit code based on result status."""

    def test_all_pass_returns_zero(self):
        """All PASS results -> exit 0."""
        results = [
            vp.CheckResult("A", "PASS"),
            vp.CheckResult("B", "PASS"),
        ]
        assert vp.print_summary(results) == 0

    def test_warn_only_returns_zero(self):
        """WARN results without FAIL -> exit 0."""
        results = [
            vp.CheckResult("A", "PASS"),
            vp.CheckResult("B", "WARN"),
        ]
        assert vp.print_summary(results) == 0

    def test_any_fail_returns_one(self):
        """Any FAIL -> exit 1."""
        results = [
            vp.CheckResult("A", "PASS"),
            vp.CheckResult("B", "FAIL"),
            vp.CheckResult("C", "WARN"),
        ]
        assert vp.print_summary(results) == 1

    def test_skip_only_returns_zero(self):
        """SKIP results -> exit 0."""
        results = [
            vp.CheckResult("A", "SKIP"),
        ]
        assert vp.print_summary(results) == 0


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


class TestOutput:
    """Tests for status tag formatting."""

    def test_status_tags(self):
        """Each status has a distinct tag."""
        for status in ("PASS", "FAIL", "WARN", "SKIP"):
            tag = vp._status_tag(status)
            assert status[:2].upper() in tag.upper() or "[" in tag

    def test_module_in_output(self, capsys):
        """Module attribution appears in output for non-empty module."""
        results = [
            vp.CheckResult(
                "Test check", "PASS", detail="ok",
                module="my_module",
            ),
        ]
        vp.print_results("Section", results)
        captured = capsys.readouterr()
        assert "[my_module]" in captured.out

    def test_no_module_bracket_when_empty(self, capsys):
        """No brackets when module is empty string."""
        results = [
            vp.CheckResult("Test check", "PASS", detail="ok"),
        ]
        vp.print_results("Section", results)
        captured = capsys.readouterr()
        assert "[]" not in captured.out


# ---------------------------------------------------------------------------
# Phase 4 (INFRA-006): Boundary date helper functions
# ---------------------------------------------------------------------------


class TestMostRecentPentadBoundary:
    """Unit tests for most_recent_pentad_boundary().

    Pentad boundaries: 5, 10, 15, 20, 25, last day of month.
    """

    @pytest.mark.parametrize("d,expected", [
        # On a boundary day -> returns itself
        (date(2026, 2, 5), date(2026, 2, 5)),
        (date(2026, 2, 10), date(2026, 2, 10)),
        (date(2026, 2, 15), date(2026, 2, 15)),
        (date(2026, 2, 20), date(2026, 2, 20)),
        (date(2026, 2, 25), date(2026, 2, 25)),
        (date(2026, 2, 28), date(2026, 2, 28)),  # last day of Feb
        # Between boundaries -> returns most recent
        (date(2026, 2, 7), date(2026, 2, 5)),
        (date(2026, 2, 14), date(2026, 2, 10)),
        (date(2026, 2, 26), date(2026, 2, 25)),
        # Before first boundary -> wraps to previous month's last day
        (date(2026, 2, 1), date(2026, 1, 31)),
        (date(2026, 2, 4), date(2026, 1, 31)),
        (date(2026, 3, 1), date(2026, 2, 28)),
        # January 1 wraps to December 31
        (date(2026, 1, 4), date(2025, 12, 31)),
    ])
    def test_pentad_boundary(self, d, expected):
        assert vp.most_recent_pentad_boundary(d) == expected

    def test_leap_year_feb_29(self):
        """Feb 29 in a leap year is last day of month -> returns itself."""
        assert vp.most_recent_pentad_boundary(date(2024, 2, 29)) == date(
            2024, 2, 29,
        )

    def test_leap_year_before_first_boundary(self):
        """Mar 1 in a leap year wraps to Feb 29."""
        assert vp.most_recent_pentad_boundary(date(2024, 3, 1)) == date(
            2024, 2, 29,
        )

    def test_31_day_month_last_day(self):
        """Day 31 is a boundary in 31-day months."""
        assert vp.most_recent_pentad_boundary(date(2026, 3, 31)) == date(
            2026, 3, 31,
        )

    def test_30_day_month_last_day(self):
        """Day 30 is a boundary in 30-day months."""
        assert vp.most_recent_pentad_boundary(date(2026, 4, 30)) == date(
            2026, 4, 30,
        )


class TestMostRecentDecadBoundary:
    """Unit tests for most_recent_decad_boundary().

    Decad boundaries: 10, 20, last day of month.
    """

    @pytest.mark.parametrize("d,expected", [
        # On a boundary day -> returns itself
        (date(2026, 2, 10), date(2026, 2, 10)),
        (date(2026, 2, 20), date(2026, 2, 20)),
        (date(2026, 2, 28), date(2026, 2, 28)),  # last day of Feb
        # Between boundaries -> returns most recent
        (date(2026, 2, 5), date(2026, 1, 31)),   # before day 10, wraps
        (date(2026, 2, 9), date(2026, 1, 31)),
        (date(2026, 2, 15), date(2026, 2, 10)),
        (date(2026, 2, 25), date(2026, 2, 20)),
        # Before first boundary -> wraps to previous month
        (date(2026, 2, 1), date(2026, 1, 31)),
        (date(2026, 3, 1), date(2026, 2, 28)),
        (date(2026, 1, 9), date(2025, 12, 31)),
    ])
    def test_decad_boundary(self, d, expected):
        assert vp.most_recent_decad_boundary(d) == expected

    def test_leap_year_feb_29(self):
        """Feb 29 in a leap year is last day -> returns itself."""
        assert vp.most_recent_decad_boundary(date(2024, 2, 29)) == date(
            2024, 2, 29,
        )

    def test_leap_year_mar_3_wraps_to_feb_29(self):
        """Mar 3 in a leap year wraps to Feb 29."""
        assert vp.most_recent_decad_boundary(date(2024, 3, 3)) == date(
            2024, 2, 29,
        )


class TestCheckExpectedModelsSkipExclusion:
    """Phase 4 (INFRA-006): check_expected_models excludes SKIP'd models.

    When a model's Tier 1 check is SKIP (not a forecast day), it should
    be excluded from the expected model set.
    """

    def test_all_models_skipped_passes(self):
        """When all models are SKIP'd, check passes (nothing expected)."""
        results = []
        for model in vp.SHORT_TERM_MODELS:
            results.append(
                vp.CheckResult(
                    f"Forecasts ({model}, pentad)", "SKIP",
                    detail="not a pentad forecast day",
                    module="linear_regression",
                )
            )
        check = vp.check_expected_models(results, "pentad")
        assert check.status == "PASS"
        assert "skipped" in check.detail

    def test_partial_skip_reduces_expected(self):
        """When some models are SKIP'd, only non-skipped are expected."""
        results = []
        # LR is present with data
        df_lr = pd.DataFrame({"model_type": ["LR"], "code": ["15001"]})
        results.append(
            vp.CheckResult(
                "Forecasts (LR, pentad)", "PASS", data=df_lr,
            )
        )
        # TFT, TiDE, TSMixer, EM, NE are all SKIP'd
        for model in ["TFT", "TiDE", "TSMixer", "EM", "NE"]:
            results.append(
                vp.CheckResult(
                    f"Forecasts ({model}, pentad)", "SKIP",
                    detail="not a pentad forecast day",
                )
            )
        check = vp.check_expected_models(results, "pentad")
        # Only LR was expected (others skipped), and LR is present -> PASS
        assert check.status == "PASS"

    def test_skip_does_not_mask_real_failures(self):
        """SKIP'd models don't mask genuinely missing non-SKIP'd models."""
        results = []
        # LR present with data
        df_lr = pd.DataFrame({"model_type": ["LR"], "code": ["15001"]})
        results.append(
            vp.CheckResult(
                "Forecasts (LR, pentad)", "PASS", data=df_lr,
            )
        )
        # TFT is FAIL (not SKIP) — genuinely missing
        results.append(
            vp.CheckResult(
                "Forecasts (TFT, pentad)", "FAIL",
                detail="no records", record_count=0,
                module="machine_learning",
            )
        )
        # Others SKIP'd
        for model in ["TiDE", "TSMixer", "EM", "NE"]:
            results.append(
                vp.CheckResult(
                    f"Forecasts ({model}, pentad)", "SKIP",
                    detail="not a pentad forecast day",
                )
            )
        check = vp.check_expected_models(results, "pentad")
        # TFT was NOT skipped, so it's still expected and missing -> FAIL
        assert check.status == "FAIL"
        assert "TFT" in check.detail
