"""Tests for validate_pipeline.py.

Covers early-exit checks, Tier 1 (presence), Tier 2 (correctness),
Tier 3 (consistency), module attribution, and the CLI entry point.
"""

import builtins
import json
import os
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


@pytest.fixture(autouse=True)
def _long_term_resolver_env(monkeypatch, tmp_path):
    config_dir = tmp_path / "long_term_configs"
    config_dir.mkdir()
    leads = {
        "quarter": 1,
        "seasonal_january": 3,
        "seasonal_february": 2,
        "seasonal_march": 1,
        "seasonal_april": 0,
    }
    for name, lead in leads.items():
        (config_dir / f"{name}.json").write_text(json.dumps({"operational_month_lead_time": lead}))

    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_configuration",
        "long_term_configs",
    )
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes",
        ",".join(leads),
    )
    return config_dir


@pytest.fixture()
def sample_runoff_df():
    """Sample runoff DataFrame with two stations."""
    return pd.DataFrame(
        {
            "code": ["15001", "15001", "15002", "15002"],
            "date": ["2026-02-23"] * 4,
            "discharge": [10.5, 12.0, 8.3, 9.1],
            "horizon_type": ["day", "day", "day", "day"],
        }
    )


@pytest.fixture()
def sample_forecast_df():
    """Sample forecast DataFrame with LR model, two stations."""
    return pd.DataFrame(
        {
            "code": ["15001", "15002"],
            "date": ["2026-02-23", "2026-02-23"],
            "forecasted_discharge": [11.0, 8.5],
            "model_type": ["LR", "LR"],
            "horizon_type": ["pentad", "pentad"],
        }
    )


@pytest.fixture()
def sample_ml_forecast_df():
    """ML forecast DataFrame with quantile columns."""
    return pd.DataFrame(
        {
            "code": ["15001", "15002"],
            "date": ["2026-02-23", "2026-02-23"],
            "forecasted_discharge": [11.0, 8.5],
            "model_type": ["TFT", "TFT"],
            "q05": [8.0, 6.0],
            "q25": [9.5, 7.0],
            "q50": [11.0, 8.5],
            "q75": [12.5, 10.0],
            "q95": [14.0, 11.5],
        }
    )


@pytest.fixture()
def sample_skill_df():
    """Sample skill metrics DataFrame."""
    return pd.DataFrame(
        {
            "code": ["15001", "15002"],
            "model_type": ["LR", "LR"],
            "nse": [0.85, 0.72],
            "accuracy": [78.0, 65.0],
            "n_pairs": [50, 45],
        }
    )


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
        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "validate", return_value=0) as mock_val,
        ):
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

        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            rc = vp.validate(
                "short-term",
                date(2026, 2, 23),
                ["pentad"],
            )
            assert rc == 1  # FAIL because preprocessing not ready

    def test_postprocessing_not_ready_produces_fail(self):
        """When postprocessing readiness_check returns False, FAIL result."""
        mock_pre = MagicMock()
        mock_pre.readiness_check.return_value = True
        mock_post = MagicMock()
        mock_post.readiness_check.return_value = False

        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            rc = vp.validate(
                "short-term",
                date(2026, 2, 23),
                ["pentad"],
            )
            assert rc == 1

    def test_readiness_exception_treated_as_not_ready(self):
        """If readiness_check raises, treat as not ready (FAIL)."""
        mock_pre = MagicMock()
        mock_pre.readiness_check.side_effect = ConnectionError("refused")
        mock_post = MagicMock()
        mock_post.readiness_check.side_effect = ConnectionError("refused")

        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            rc = vp.validate(
                "short-term",
                date(2026, 2, 23),
                ["pentad"],
            )
            assert rc == 1


# ---------------------------------------------------------------------------
# Tier 1: Data Presence
# ---------------------------------------------------------------------------


class TestTier1Presence:
    """Tests for Tier 1 data presence checks."""

    def test_check_presence_pass_on_non_empty_df(self, mock_post_client):
        """Non-empty DataFrame from API -> PASS."""
        mock_post_client.read_short_term_forecasts.return_value = pd.DataFrame(
            {"code": ["15001"], "date": ["2026-02-23"]}
        )
        result = vp.check_presence(
            mock_post_client,
            "read_short_term_forecasts",
            "Forecasts (LR, pentad)",
            module="linear_regression",
            horizon="pentad",
            model="LR",
            start_date="2026-02-23",
            end_date="2026-02-23",
        )
        assert result.status == "PASS"
        assert result.record_count == 1
        assert result.data is not None
        assert result.module == "linear_regression"

    def test_check_presence_fail_on_empty_df(self, mock_post_client):
        """Empty DataFrame from API -> FAIL."""
        mock_post_client.read_short_term_forecasts.return_value = pd.DataFrame()
        result = vp.check_presence(
            mock_post_client,
            "read_short_term_forecasts",
            "Forecasts (LR, pentad)",
            module="linear_regression",
            horizon="pentad",
            model="LR",
            start_date="2026-02-23",
            end_date="2026-02-23",
        )
        assert result.status == "FAIL"
        assert result.record_count == 0
        assert "no records" in result.detail
        assert result.module == "linear_regression"

    def test_check_presence_warn_when_configured(self, mock_pre_client):
        """Empty DataFrame with warn_if_empty=True -> WARN."""
        mock_pre_client.read_snow.return_value = pd.DataFrame()
        result = vp.check_presence(
            mock_pre_client,
            "read_snow",
            "Snow (SWE)",
            module="preprocessing_gateway",
            snow_type="SWE",
            start_date="2026-02-23",
            end_date="2026-02-23",
            warn_if_empty=True,
        )
        assert result.status == "WARN"
        assert "may not be configured" in result.detail

    def test_check_presence_fail_on_api_exception(self, mock_pre_client):
        """API exception -> FAIL with error detail."""
        mock_pre_client.read_runoff.side_effect = RuntimeError("timeout")
        result = vp.check_presence(
            mock_pre_client,
            "read_runoff",
            "Runoff (day)",
            module="preprocessing_runoff",
            horizon="day",
            start_date="2026-02-23",
            end_date="2026-02-23",
        )
        assert result.status == "FAIL"
        assert "API error" in result.detail
        assert result.module == "preprocessing_runoff"

    def test_tier1_short_term_returns_expected_check_count(
        self,
        mock_pre_client,
        mock_post_client,
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
            mock_pre_client,
            mock_post_client,
            date(2026, 2, 23),
            "pentad",
        )
        # 1 runoff (day) + 1 hydrograph (day) + 2 meteo + 1 snow
        # + 6 forecast models + 1 LR details + 1 skill = 13
        assert len(results) == 13
        assert all(r.status == "PASS" for r in results)

    def test_tier1_no_pentad_runoff_or_hydrograph(
        self,
        mock_pre_client,
        mock_post_client,
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
            mock_pre_client,
            mock_post_client,
            date(2026, 2, 23),
            "pentad",
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
        self,
        mock_pre_client,
        mock_post_client,
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
            mock_pre_client,
            mock_post_client,
            date(2026, 2, 23),
            "pentad",
        )
        by_name = {r.name: r.module for r in results}

        # Preprocessing modules
        assert by_name["Runoff (day)"] == "preprocessing_runoff"
        assert by_name["Hydrograph (day)"] == "preprocessing_runoff"
        assert by_name["Meteo (T)"] == "preprocessing_gateway"
        assert by_name["Meteo (P)"] == "preprocessing_gateway"
        assert by_name["Snow (SWE)"] == "preprocessing_gateway"

        # Forecast models mapped to their source modules
        assert by_name["Forecasts (LR, pentad)"] == "postprocessing_forecasts"
        assert by_name["Forecasts (TFT, pentad)"] == "postprocessing_forecasts"
        assert by_name["Forecasts (TiDE, pentad)"] == "postprocessing_forecasts"
        assert by_name["Forecasts (TSMixer, pentad)"] == "postprocessing_forecasts"
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
            mock_post_client,
            date(2026, 2, 23),
        )
        by_name = {r.name: r.module for r in results}
        assert by_name["Long-term forecasts (month)"] == ("long_term_forecasting")
        assert by_name["Monthly skill metrics"] == "postprocessing_forecasts"
        assert by_name["Long-term forecasts (quarter hv1)"] == "postprocessing_forecasts"
        assert by_name["Quarterly skill metrics"] == "postprocessing_forecasts"
        assert by_name["Long-term forecasts (season issue 2 hv2)"] == "postprocessing_forecasts"
        assert by_name["Seasonal skill metrics"] == "postprocessing_forecasts"

    def test_tier1_long_term_fails_when_quarter_bucket_empty(self, mock_post_client):
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})

        def read_long_term_forecasts(**kwargs):
            if kwargs.get("horizon_type") == "quarter":
                return pd.DataFrame()
            return single_row

        mock_post_client.read_long_term_forecasts.side_effect = read_long_term_forecasts
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))
        by_name = {r.name: r for r in results}

        assert by_name["Long-term forecasts (quarter hv1)"].status == "FAIL"
        assert "no records" in by_name["Long-term forecasts (quarter hv1)"].detail

    def test_tier1_long_term_fails_when_season_bucket_empty(self, mock_post_client):
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})

        def read_long_term_forecasts(**kwargs):
            if kwargs.get("horizon_type") == "season" and kwargs.get("horizon_value") == 2:
                return pd.DataFrame()
            return single_row

        mock_post_client.read_long_term_forecasts.side_effect = read_long_term_forecasts
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))
        by_name = {r.name: r for r in results}

        assert by_name["Long-term forecasts (season issue 2 hv2)"].status == "FAIL"
        assert "no records" in by_name["Long-term forecasts (season issue 2 hv2)"].detail

    def test_tier1_long_term_passes_when_quarter_and_season_buckets_present(self, mock_post_client):
        single_row = pd.DataFrame({"code": ["15001"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))

        assert all(r.status == "PASS" for r in results)

    def test_module_shown_in_output(self, capsys):
        """Module attribution appears in printed output."""
        results = [
            vp.CheckResult(
                "Runoff (day)",
                "PASS",
                detail="15 records",
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
        results = [vp.CheckResult("Runoff (day)", "PASS", data=sample_runoff_df)]
        check = vp.check_discharge_non_negative(results)
        assert check.status == "PASS"

    def test_discharge_non_negative_fail(self):
        """Negative discharge values -> FAIL."""
        df = pd.DataFrame(
            {
                "discharge": [10.0, -5.0, 8.0],
            }
        )
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
                "Forecasts (LR, pentad)",
                "PASS",
                data=sample_forecast_df,
            )
        ]
        check = vp.check_no_nan_in_forecasts(results)
        assert check.status == "PASS"
        assert "2 values present" in check.detail

    def test_no_nan_in_forecasts_warn_on_nan(self):
        """NaN in forecasted_discharge -> WARN."""
        df = pd.DataFrame(
            {
                "forecasted_discharge": [11.0, float("nan"), 8.5],
            }
        )
        results = [vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)]
        check = vp.check_no_nan_in_forecasts(results)
        assert check.status == "WARN"
        assert "1/3" in check.detail

    def test_quantile_ordering_pass(self, sample_ml_forecast_df):
        """Properly ordered quantiles -> PASS."""
        results = [
            vp.CheckResult(
                "Forecasts (TFT, pentad)",
                "PASS",
                data=sample_ml_forecast_df,
            )
        ]
        check = vp.check_quantile_ordering(results)
        assert check.status == "PASS"

    def test_quantile_ordering_fail_on_disordered(self):
        """Disordered quantiles -> FAIL."""
        df = pd.DataFrame(
            {
                "q05": [8.0],
                "q25": [12.0],  # q25 > q50
                "q50": [11.0],
                "q75": [10.0],  # q75 < q50
                "q95": [14.0],
            }
        )
        results = [vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=df)]
        check = vp.check_quantile_ordering(results)
        assert check.status == "FAIL"
        assert "disordered" in check.detail

    def test_quantile_ordering_skip_no_quantiles(self):
        """No quantile columns -> SKIP."""
        df = pd.DataFrame({"forecasted_discharge": [11.0]})
        results = [vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)]
        check = vp.check_quantile_ordering(results)
        assert check.status == "SKIP"

    def test_expected_models_pass(self):
        """All expected models found -> PASS."""
        results = []
        for model in vp.SHORT_TERM_MODELS:
            df = pd.DataFrame(
                {
                    "model_type": [model],
                    "code": ["15001"],
                    "date": ["2026-02-23"],
                }
            )
            results.append(vp.CheckResult(f"Forecasts ({model}, pentad)", "PASS", data=df))
        check = vp.check_expected_models(results, "pentad")
        assert check.status == "PASS"

    def test_expected_models_fail_missing(self):
        """Missing models -> FAIL with names."""
        # Only LR present
        df = pd.DataFrame({"model_type": ["LR"]})
        results = [vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=df)]
        check = vp.check_expected_models(results, "pentad")
        assert check.status == "FAIL"
        assert "missing" in check.detail
        assert "TFT" in check.detail

    def test_skill_metric_ranges_pass(self, sample_skill_df):
        """Valid skill metrics -> PASS."""
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=sample_skill_df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "PASS"

    def test_skill_metric_ranges_fail_nse_above_1(self):
        """NSE > 1.0 -> FAIL."""
        df = pd.DataFrame(
            {
                "nse": [0.85, 1.5],
                "accuracy": [78.0, 65.0],
                "n_pairs": [50, 45],
            }
        )
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "NSE > 1.0" in check.detail

    def test_skill_metric_ranges_fail_accuracy_out_of_range(self):
        """Accuracy outside [0, 100] -> FAIL."""
        df = pd.DataFrame(
            {
                "nse": [0.85],
                "accuracy": [105.0],
                "n_pairs": [50],
            }
        )
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "accuracy" in check.detail

    def test_skill_metric_ranges_warn_n_pairs_zero(self):
        """n_pairs <= 0 -> WARN (not FAIL) since new stations may lack data."""
        df = pd.DataFrame(
            {
                "nse": [0.85],
                "accuracy": [78.0],
                "n_pairs": [0],
            }
        )
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "WARN"
        assert "n_pairs" in check.detail
        assert "new stations" in check.detail

    def test_skill_metric_ranges_fail_nse_overrides_n_pairs_warn(self):
        """When both NSE > 1.0 and n_pairs <= 0, status is FAIL."""
        df = pd.DataFrame(
            {
                "nse": [1.5],
                "accuracy": [78.0],
                "n_pairs": [0],
            }
        )
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.status == "FAIL"
        assert "NSE > 1.0" in check.detail
        assert "n_pairs" in check.detail

    def test_skill_metric_has_module(self):
        """Skill metric check result has postprocessing_forecasts module."""
        df = pd.DataFrame(
            {
                "nse": [0.85],
                "accuracy": [78.0],
                "n_pairs": [50],
            }
        )
        results = [vp.CheckResult("Skill metrics (pentad)", "PASS", data=df)]
        check = vp.check_skill_metric_ranges(results)
        assert check.module == "postprocessing_forecasts"


# ---------------------------------------------------------------------------
# Tier 3: Cross-module Consistency
# ---------------------------------------------------------------------------


class TestTier3Consistency:
    """Tests for Tier 3 cross-module consistency checks."""

    def test_station_codes_match_pass(self):
        """Forecast codes <= runoff codes -> PASS."""
        runoff = pd.DataFrame(
            {
                "code": ["15001", "15002", "15003"],
                "date": ["2026-02-23"] * 3,
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["15001", "15002"],
                "date": ["2026-02-23"] * 2,
            }
        )
        results = [
            vp.CheckResult("Runoff (day)", "PASS", data=runoff),
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=forecasts),
        ]
        check = vp.check_station_codes_match(results)
        assert check.status == "PASS"

    def test_station_codes_match_warn_extra_codes(self):
        """Forecast has codes not in runoff -> WARN."""
        runoff = pd.DataFrame(
            {
                "code": ["15001"],
                "date": ["2026-02-23"],
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["15001", "15999"],
                "date": ["2026-02-23"] * 2,
            }
        )
        results = [
            vp.CheckResult("Runoff (day)", "PASS", data=runoff),
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=forecasts),
        ]
        check = vp.check_station_codes_match(results)
        assert check.status == "WARN"
        assert "15999" in check.detail

    def test_station_codes_match_skip_no_data(self):
        """Insufficient data -> SKIP."""
        results = [vp.CheckResult("Empty", "FAIL", data=pd.DataFrame())]
        check = vp.check_station_codes_match(results)
        assert check.status == "SKIP"

    def test_dates_consistent_pass(self):
        """All models have same (code, date) tuples -> PASS."""
        base = pd.DataFrame(
            {
                "code": ["15001", "15002"],
                "date": ["2026-02-23", "2026-02-23"],
            }
        )
        results = [
            vp.CheckResult("Forecasts (LR, pentad)", "PASS", data=base),
            vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=base.copy()),
        ]
        check = vp.check_dates_consistent(results)
        assert check.status == "PASS"
        assert "2 (code, date) tuples" in check.detail

    def test_dates_consistent_warn_mismatch(self):
        """Models have different (code, date) coverage -> WARN."""
        lr_df = pd.DataFrame(
            {
                "code": ["15001", "15002"],
                "date": ["2026-02-23", "2026-02-23"],
            }
        )
        tft_df = pd.DataFrame(
            {
                "code": ["15001"],
                "date": ["2026-02-23"],
            }
        )
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

    @pytest.mark.parametrize(
        "day,expected",
        [
            (5, True),
            (10, True),
            (15, True),
            (20, True),
            (25, True),
            (28, True),  # 28 = last day of Feb 2026
            (1, False),
            (6, False),
            (14, False),
            (19, False),
            (27, False),
        ],
    )
    def test_pentad_forecast_days_feb(self, day, expected):
        """Feb 2026 pentad forecast days: 5,10,15,20,25,28."""
        assert vp.is_pentad_forecast_day(date(2026, 2, day)) is expected

    @pytest.mark.parametrize(
        "day,expected",
        [
            (10, True),
            (20, True),
            (31, True),  # 31 = last day of Jan
            (5, False),
            (15, False),
            (25, False),
            (1, False),
        ],
    )
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
                "Forecasts (LR, pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
                module="linear_regression",
            ),
            vp.CheckResult(
                "Runoff (day)",
                "FAIL",
                detail="no records",
                record_count=0,
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
                "Forecasts (LR, pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
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
                "Forecasts (LR, pentad)",
                "FAIL",
                detail="API error: timeout",
                record_count=5,
                module="linear_regression",
            ),
        ]
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")
        assert results[0].status == "FAIL"

    def test_pass_not_changed(self):
        """PASS results are never touched."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, pentad)",
                "PASS",
                detail="50 records",
                record_count=50,
                module="linear_regression",
            ),
        ]
        vp._apply_non_forecast_day_skip(results, date(2026, 2, 23), "pentad")
        assert results[0].status == "PASS"

    def test_skip_on_non_decad_day(self):
        """FAIL -> SKIP on non-decad day for decade horizon."""
        results = [
            vp.CheckResult(
                "Forecasts (LR, decade)",
                "FAIL",
                detail="no records",
                record_count=0,
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
                "Forecasts (LR, pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
                module="linear_regression",
            ),
            vp.CheckResult(
                "Forecasts (TFT, pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
                module="machine_learning",
            ),
            vp.CheckResult(
                "Skill metrics (pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
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
                "Long-term forecasts (month)",
                "FAIL",
                detail="no records",
                record_count=0,
                module="long_term_forecasting",
            ),
        ]
        vp._apply_non_forecast_day_skip(
            results,
            date(2026, 2, 23),
            "long-term",
        )
        # long_term_forecasting is not in FORECAST_DAY_MODULES
        assert results[0].status == "FAIL"


class TestHorizonResolution:
    """Tests for resolve_horizons()."""

    def test_explicit_horizon_override(self):
        """Explicit --horizon argument takes precedence."""
        assert vp.resolve_horizons("pentad") == ["pentad"]
        assert vp.resolve_horizons("decade") == ["decade"]
        assert vp.resolve_horizons("month") == ["month"]

    def test_explicit_horizon_overrides_target(self):
        """Explicit --horizon takes precedence even for long-term target."""
        assert vp.resolve_horizons("pentad", target="long-term") == ["pentad"]

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

    def test_long_term_target_defaults_to_month(self, monkeypatch):
        """Long-term target defaults to ['month'] regardless of env."""
        monkeypatch.delenv("SAPPHIRE_PREDICTION_MODE", raising=False)
        assert vp.resolve_horizons(None, target="long-term") == ["month"]

    def test_long_term_ignores_prediction_mode(self, monkeypatch):
        """Long-term target ignores SAPPHIRE_PREDICTION_MODE."""
        monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", "PENTAD")
        assert vp.resolve_horizons(None, target="long-term") == ["month"]


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
                "Test check",
                "PASS",
                detail="ok",
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

    @pytest.mark.parametrize(
        "d,expected",
        [
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
        ],
    )
    def test_pentad_boundary(self, d, expected):
        assert vp.most_recent_pentad_boundary(d) == expected

    def test_leap_year_feb_29(self):
        """Feb 29 in a leap year is last day of month -> returns itself."""
        assert vp.most_recent_pentad_boundary(date(2024, 2, 29)) == date(
            2024,
            2,
            29,
        )

    def test_leap_year_before_first_boundary(self):
        """Mar 1 in a leap year wraps to Feb 29."""
        assert vp.most_recent_pentad_boundary(date(2024, 3, 1)) == date(
            2024,
            2,
            29,
        )

    def test_31_day_month_last_day(self):
        """Day 31 is a boundary in 31-day months."""
        assert vp.most_recent_pentad_boundary(date(2026, 3, 31)) == date(
            2026,
            3,
            31,
        )

    def test_30_day_month_last_day(self):
        """Day 30 is a boundary in 30-day months."""
        assert vp.most_recent_pentad_boundary(date(2026, 4, 30)) == date(
            2026,
            4,
            30,
        )


class TestMostRecentDecadBoundary:
    """Unit tests for most_recent_decad_boundary().

    Decad boundaries: 10, 20, last day of month.
    """

    @pytest.mark.parametrize(
        "d,expected",
        [
            # On a boundary day -> returns itself
            (date(2026, 2, 10), date(2026, 2, 10)),
            (date(2026, 2, 20), date(2026, 2, 20)),
            (date(2026, 2, 28), date(2026, 2, 28)),  # last day of Feb
            # Between boundaries -> returns most recent
            (date(2026, 2, 5), date(2026, 1, 31)),  # before day 10, wraps
            (date(2026, 2, 9), date(2026, 1, 31)),
            (date(2026, 2, 15), date(2026, 2, 10)),
            (date(2026, 2, 25), date(2026, 2, 20)),
            # Before first boundary -> wraps to previous month
            (date(2026, 2, 1), date(2026, 1, 31)),
            (date(2026, 3, 1), date(2026, 2, 28)),
            (date(2026, 1, 9), date(2025, 12, 31)),
        ],
    )
    def test_decad_boundary(self, d, expected):
        assert vp.most_recent_decad_boundary(d) == expected

    def test_leap_year_feb_29(self):
        """Feb 29 in a leap year is last day -> returns itself."""
        assert vp.most_recent_decad_boundary(date(2024, 2, 29)) == date(
            2024,
            2,
            29,
        )

    def test_leap_year_mar_3_wraps_to_feb_29(self):
        """Mar 3 in a leap year wraps to Feb 29."""
        assert vp.most_recent_decad_boundary(date(2024, 3, 3)) == date(
            2024,
            2,
            29,
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
                    f"Forecasts ({model}, pentad)",
                    "SKIP",
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
                "Forecasts (LR, pentad)",
                "PASS",
                data=df_lr,
            )
        )
        # TFT, TiDE, TSMixer, EM, NE are all SKIP'd
        for model in ["TFT", "TiDE", "TSMixer", "EM", "NE"]:
            results.append(
                vp.CheckResult(
                    f"Forecasts ({model}, pentad)",
                    "SKIP",
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
                "Forecasts (LR, pentad)",
                "PASS",
                data=df_lr,
            )
        )
        # TFT is FAIL (not SKIP) — genuinely missing
        results.append(
            vp.CheckResult(
                "Forecasts (TFT, pentad)",
                "FAIL",
                detail="no records",
                record_count=0,
                module="machine_learning",
            )
        )
        # Others SKIP'd
        for model in ["TiDE", "TSMixer", "EM", "NE"]:
            results.append(
                vp.CheckResult(
                    f"Forecasts ({model}, pentad)",
                    "SKIP",
                    detail="not a pentad forecast day",
                )
            )
        check = vp.check_expected_models(results, "pentad")
        # TFT was NOT skipped, so it's still expected and missing -> FAIL
        assert check.status == "FAIL"
        assert "TFT" in check.detail


# ---------------------------------------------------------------------------
# Phase 1: JSON Output
# ---------------------------------------------------------------------------


class TestJsonOutput:
    """Tests for results_to_json() and JSON serialisation."""

    def test_json_is_valid_and_parseable(self):
        """results_to_json returns a dict that round-trips through JSON."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", detail="10 records", record_count=10),
        ]
        payload = vp.results_to_json(results)
        serialised = json.dumps(payload)
        parsed = json.loads(serialised)
        assert isinstance(parsed, dict)
        assert "Runoff (day)" in parsed

    def test_all_check_names_appear_as_keys(self):
        """Every check name is a top-level key in the JSON output."""
        results = [
            vp.CheckResult("Check A", "PASS"),
            vp.CheckResult("Check B", "FAIL"),
            vp.CheckResult("Check C", "WARN"),
        ]
        payload = vp.results_to_json(results)
        assert "Check A" in payload
        assert "Check B" in payload
        assert "Check C" in payload

    def test_status_field_is_valid(self):
        """status field in JSON is one of PASS, WARN, FAIL, SKIP."""
        valid_statuses = {"PASS", "WARN", "FAIL", "SKIP"}
        results = [
            vp.CheckResult("A", "PASS"),
            vp.CheckResult("B", "WARN"),
            vp.CheckResult("C", "FAIL"),
            vp.CheckResult("D", "SKIP"),
        ]
        payload = vp.results_to_json(results)
        for key in ("A", "B", "C", "D"):
            assert payload[key]["status"] in valid_statuses

    def test_max_date_and_counts_fields_present(self):
        """max_date and counts fields are present in each check entry."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", max_date="2026-02-23", counts={"a": 1}),
            vp.CheckResult("Snow (SWE)", "SKIP"),
        ]
        payload = vp.results_to_json(results)
        assert payload["Runoff (day)"]["max_date"] == "2026-02-23"
        assert payload["Runoff (day)"]["counts"] == {"a": 1}
        assert payload["Snow (SWE)"]["max_date"] is None
        assert payload["Snow (SWE)"]["counts"] == {}

    def test_writing_to_path_produces_correct_file(self, tmp_path):
        """Writing JSON to a path produces a parseable file with correct content."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", detail="5 records", record_count=5),
        ]
        output_path = tmp_path / "output.json"
        payload = vp.results_to_json(results)
        output_path.write_text(json.dumps(payload, indent=2))
        loaded = json.loads(output_path.read_text())
        assert loaded["Runoff (day)"]["status"] == "PASS"
        assert loaded["Runoff (day)"]["record_count"] == 5

    def test_metadata_stored_under_meta_key(self):
        """Optional metadata is stored under '_meta' key."""
        results = [vp.CheckResult("A", "PASS")]
        meta = {"forecast_date": "2026-02-23", "target": "short-term"}
        payload = vp.results_to_json(results, metadata=meta)
        assert "_meta" in payload
        assert payload["_meta"]["forecast_date"] == "2026-02-23"
        assert payload["_meta"]["target"] == "short-term"


# ---------------------------------------------------------------------------
# Phase 2: Baseline / Delta Mode
# ---------------------------------------------------------------------------


class TestPhaseMode:
    """Tests for write_baseline, load_and_validate_baseline, compute_deltas."""

    def test_pre_mode_writes_baseline_file(self, tmp_path):
        """write_baseline creates a file at the given path."""
        results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(results, date(2026, 2, 23), "short-term", path)
        assert (tmp_path / "baseline.json").exists()
        loaded = json.loads((tmp_path / "baseline.json").read_text())
        assert "Runoff (day)" in loaded
        assert loaded["_meta"]["forecast_date"] == "2026-02-23"

    def test_post_mode_loads_and_computes_deltas(self, tmp_path):
        """load_and_validate_baseline + compute_deltas produces delta lines."""
        results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(results, date(2026, 2, 23), "short-term", path)

        current_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=12)]
        baseline = vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")
        current_json = vp.results_to_json(current_results)
        lines = vp.compute_deltas(current_json, baseline)
        assert any("increased" in line for line in lines)

    def test_count_decrease_produces_warn_line(self, tmp_path):
        """A count decrease in delta mode produces a DELTA WARN line."""
        baseline_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(baseline_results, date(2026, 2, 23), "short-term", path)

        current_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=5)]
        baseline = vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")
        current_json = vp.results_to_json(current_results)
        lines = vp.compute_deltas(current_json, baseline)
        assert any("DELTA WARN" in line for line in lines)
        assert any("10 to 5" in line for line in lines)

    def test_count_increase_produces_info_not_warn(self, tmp_path):
        """A count increase produces INFO (not WARN) in delta output."""
        baseline_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=5)]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(baseline_results, date(2026, 2, 23), "short-term", path)

        current_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        baseline = vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")
        current_json = vp.results_to_json(current_results)
        lines = vp.compute_deltas(current_json, baseline)
        assert any("DELTA INFO" in line for line in lines)
        assert not any("DELTA WARN" in line for line in lines)

    def test_count_unchanged_no_delta_line(self, tmp_path):
        """Unchanged counts produce no delta lines."""
        baseline_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(baseline_results, date(2026, 2, 23), "short-term", path)

        current_results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        baseline = vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")
        current_json = vp.results_to_json(current_results)
        lines = vp.compute_deltas(current_json, baseline)
        assert lines == []

    def test_missing_baseline_raises_file_not_found(self, tmp_path):
        """Missing baseline file raises FileNotFoundError."""
        path = str(tmp_path / "nonexistent.json")
        with pytest.raises(FileNotFoundError, match="Baseline file not found"):
            vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")

    def test_no_phase_flag_existing_behaviour(self):
        """compute_deltas with identical dicts returns no delta lines."""
        results = [vp.CheckResult("Runoff (day)", "PASS", record_count=10)]
        payload = vp.results_to_json(results)
        lines = vp.compute_deltas(payload, payload)
        assert lines == []

    def test_pre_post_consistent_when_unchanged(self, tmp_path):
        """Pre then post with same data produces no delta lines."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", record_count=10),
            vp.CheckResult("Meteo (T)", "PASS", record_count=5),
        ]
        path = str(tmp_path / "baseline.json")
        vp.write_baseline(results, date(2026, 2, 23), "short-term", path)

        baseline = vp.load_and_validate_baseline(path, date(2026, 2, 23), "short-term")
        current_json = vp.results_to_json(results)
        lines = vp.compute_deltas(current_json, baseline)
        assert lines == []


# ---------------------------------------------------------------------------
# Phase 3: New Checks
# ---------------------------------------------------------------------------


class TestNewChecks:
    """Tests for check_ml_flag_distribution, check_snow_operational_values,
    check_em_ne_parity, check_data_freshness, and run_tier2_long_term.
    """

    # --- check_ml_flag_distribution ---

    def test_ml_flag_distribution_skip_no_data(self):
        """No ML forecast data -> SKIP."""
        results = [vp.CheckResult("Runoff (day)", "PASS", data=pd.DataFrame())]
        check = vp.check_ml_flag_distribution(results)
        assert check.status == "SKIP"

    def test_ml_flag_distribution_pass_mixed_flags(self):
        """Multiple distinct flag values -> PASS."""
        df = pd.DataFrame(
            {
                "flag": [0, 1, 0, 2, 1],
                "forecasted_discharge": [10.0, 11.0, 9.0, 8.5, 12.0],
            }
        )
        results = [vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=df)]
        check = vp.check_ml_flag_distribution(results)
        assert check.status == "PASS"
        assert len(check.counts) > 1

    def test_ml_flag_distribution_warn_stuck_flag(self):
        """All records with same flag -> WARN."""
        df = pd.DataFrame(
            {
                "flag": [1, 1, 1, 1],
                "forecasted_discharge": [10.0, 11.0, 9.0, 8.5],
            }
        )
        results = [vp.CheckResult("Forecasts (TFT, pentad)", "PASS", data=df)]
        check = vp.check_ml_flag_distribution(results)
        assert check.status == "WARN"
        assert "stuck" in check.detail

    # --- check_snow_operational_values ---

    def test_snow_operational_skip_no_data(self):
        """No snow data -> SKIP."""
        results = [vp.CheckResult("Runoff (day)", "PASS", data=pd.DataFrame())]
        check = vp.check_snow_operational_values(results)
        assert check.status == "SKIP"

    def test_snow_operational_pass_current_year(self):
        """Snow records with current-year dates -> PASS."""
        df = pd.DataFrame(
            {
                "date": ["2026-01-15", "2026-02-01"],
                "value": [50.0, 55.0],
            }
        )
        results = [vp.CheckResult("Snow (SWE)", "PASS", data=df)]
        check = vp.check_snow_operational_values(results)
        assert check.status == "PASS"
        assert "2026" in check.detail

    def test_snow_operational_warn_year_2000_only(self):
        """All snow records with year-2000 dates -> WARN."""
        df = pd.DataFrame(
            {
                "date": ["2000-01-15", "2000-02-01", "2000-03-10"],
                "value": [50.0, 55.0, 48.0],
            }
        )
        results = [vp.CheckResult("Snow (SWE)", "PASS", data=df)]
        check = vp.check_snow_operational_values(results)
        assert check.status == "WARN"
        assert "PREPG-003" in check.detail

    # --- check_em_ne_parity ---

    def test_em_ne_parity_skip_no_data(self):
        """No EM or NE data -> SKIP."""
        results = [vp.CheckResult("Runoff (day)", "PASS", data=pd.DataFrame())]
        check = vp.check_em_ne_parity(results, "pentad")
        assert check.status == "SKIP"

    def test_em_ne_parity_pass_equal_counts(self):
        """EM and NE have equal record counts -> PASS."""
        df = pd.DataFrame({"code": ["15001", "15002"], "date": ["2026-02-25"] * 2})
        results = [
            vp.CheckResult("Forecasts (EM, pentad)", "PASS", record_count=2, data=df),
            vp.CheckResult("Forecasts (NE, pentad)", "PASS", record_count=2, data=df),
        ]
        check = vp.check_em_ne_parity(results, "pentad")
        assert check.status == "PASS"
        assert check.counts["EM"] == check.counts["NE"]

    def test_em_ne_parity_warn_mismatch(self):
        """EM and NE have different record counts -> WARN."""
        em_df = pd.DataFrame({"code": ["15001", "15002"], "date": ["2026-02-25"] * 2})
        ne_df = pd.DataFrame({"code": ["15001"], "date": ["2026-02-25"]})
        results = [
            vp.CheckResult("Forecasts (EM, pentad)", "PASS", record_count=2, data=em_df),
            vp.CheckResult("Forecasts (NE, pentad)", "PASS", record_count=1, data=ne_df),
        ]
        check = vp.check_em_ne_parity(results, "pentad")
        assert check.status == "WARN"
        assert "EM=2" in check.detail
        assert "NE=1" in check.detail

    # --- check_data_freshness ---

    def test_data_freshness_skip_no_max_dates(self):
        """No max_date in any result -> SKIP."""
        results = [vp.CheckResult("Runoff (day)", "PASS")]
        check = vp.check_data_freshness(results, date(2026, 2, 23))
        assert check.status == "SKIP"

    def test_data_freshness_pass_recent_data(self):
        """max_date within threshold -> PASS."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", max_date="2026-02-23"),
            vp.CheckResult("Meteo (T)", "PASS", max_date="2026-02-22"),
        ]
        check = vp.check_data_freshness(results, date(2026, 2, 23))
        assert check.status == "PASS"

    def test_data_freshness_warn_stale_data(self):
        """max_date older than threshold -> WARN."""
        results = [
            vp.CheckResult("Runoff (day)", "PASS", max_date="2026-02-10"),
        ]
        # 13 days lag, default threshold is 3
        check = vp.check_data_freshness(results, date(2026, 2, 23))
        assert check.status == "WARN"
        assert "lag=13d" in check.detail

    def test_data_freshness_threshold_env_override(self, monkeypatch):
        """FRESHNESS_THRESHOLD_DAYS env var overrides default threshold."""
        monkeypatch.setenv("FRESHNESS_THRESHOLD_DAYS", "30")
        results = [
            vp.CheckResult("Runoff (day)", "PASS", max_date="2026-02-10"),
        ]
        # 13-day lag is within a 30-day threshold -> PASS
        check = vp.check_data_freshness(results, date(2026, 2, 23))
        assert check.status == "PASS"

    # --- run_tier2_long_term ---

    def test_lt_tier2_skip_no_data(self):
        """run_tier2_long_term with empty results -> all SKIP."""
        results = [vp.CheckResult("Long-term forecasts (month)", "FAIL", data=pd.DataFrame())]
        t2 = vp.run_tier2_long_term(results)
        assert all(r.status == "SKIP" for r in t2)

    def test_lt_tier2_pass_healthy_data(self):
        """run_tier2_long_term with valid LT data -> PASS checks."""
        df = pd.DataFrame(
            {
                "q05": [10.0],
                "q10": [12.0],
                "q25": [15.0],
                "q50": [20.0],
                "q75": [25.0],
                "q90": [28.0],
                "q95": [30.0],
                "nse": [0.75],
                "accuracy": [80.0],
                "n_pairs": [10],
            }
        )
        results = [vp.CheckResult("Long-term forecasts (month)", "PASS", data=df)]
        t2 = vp.run_tier2_long_term(results)
        # Quantile ordering and skill metrics should pass
        quantile_result = next((r for r in t2 if "Quantile" in r.name), None)
        assert quantile_result is not None
        assert quantile_result.status == "PASS"

    def test_lt_tier2_warn_disordered_quantiles(self):
        """run_tier2_long_term with disordered LT quantiles -> FAIL."""
        df = pd.DataFrame(
            {
                "q05": [30.0],
                "q10": [12.0],
                "q25": [15.0],  # q05 > q10
                "q50": [20.0],
                "q75": [25.0],
                "q90": [28.0],
                "q95": [29.0],
            }
        )
        results = [vp.CheckResult("Long-term forecasts (month)", "PASS", data=df)]
        t2 = vp.run_tier2_long_term(results)
        quantile_result = next((r for r in t2 if "Quantile" in r.name), None)
        assert quantile_result is not None
        assert quantile_result.status == "FAIL"


# ---------------------------------------------------------------------------
# Deployment .env loading (_load_deployment_env)
# ---------------------------------------------------------------------------

# The autouse `_long_term_resolver_env` fixture (defined near the top of this
# file) sets all three long-term resolver env vars for EVERY test — that is
# exactly why the missing-.env defect was invisible to this suite before this
# fix. Every test below that needs those vars absent explicitly deletes them
# with monkeypatch.delenv(..., raising=False) to neutralise the fixture for
# that test; monkeypatch restores the prior state automatically afterwards,
# so nothing leaks into other tests.

_RESOLVER_VARS = (
    "ieasyhydroforecast_ml_long_term_supported_modes",
    "ieasyforecast_configuration_path",
    "ieasyhydroforecast_ml_long_term_configuration",
)


class TestLoadDeploymentEnv:
    """Tests for vp._load_deployment_env()."""

    def test_env_file_loaded_when_pointer_set(self, monkeypatch, tmp_path):
        """A real .env file at the pointer path is loaded into os.environ."""
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)

        env_file = tmp_path / "deploy.env"
        env_file.write_text(
            "ieasyhydroforecast_ml_long_term_supported_modes=quarter\n"
            "ieasyforecast_configuration_path=/some/config/path\n"
            "ieasyhydroforecast_ml_long_term_configuration=long_term_configs\n"
        )
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))

        assert vp._load_deployment_env() is True
        assert os.environ["ieasyhydroforecast_ml_long_term_supported_modes"] == "quarter"
        assert os.environ["ieasyforecast_configuration_path"] == "/some/config/path"
        assert os.environ["ieasyhydroforecast_ml_long_term_configuration"] == "long_term_configs"
        # monkeypatch un-sets these again on teardown, so no leakage risk.
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)

    def test_nonexistent_pointer_path_fails_main(self, monkeypatch, tmp_path, capsys):
        """Pointer set to a non-existent path -> main() returns 1, [FAIL] printed."""
        missing_path = tmp_path / "does_not_exist.env"
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(missing_path))

        rc = vp.main(["--target", "short-term"])
        captured = capsys.readouterr()

        assert rc == 1
        assert "[FAIL]" in captured.out
        assert str(missing_path) in captured.out

    def test_pointer_absent_but_vars_already_exported(self, monkeypatch):
        """Container case: no pointer, resolver vars already in the environment.

        Must report success and change nothing — this is the working
        container / already-exported-env case and must not regress.
        """
        monkeypatch.delenv("ieasyhydroforecast_env_file_path", raising=False)
        # The autouse fixture has already exported the three resolver vars;
        # capture their values to prove _load_deployment_env changes nothing.
        before = {var: os.environ[var] for var in _RESOLVER_VARS}

        assert vp._load_deployment_env() is True

        for var in _RESOLVER_VARS:
            assert os.environ[var] == before[var]

    def test_override_false_does_not_clobber_existing_value(self, monkeypatch, tmp_path):
        """A var already in os.environ is NOT overwritten by the .env file."""
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "already-set-value")
        env_file = tmp_path / "deploy.env"
        env_file.write_text("ieasyhydroforecast_ml_long_term_supported_modes=from-file-value\n")
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))

        assert vp._load_deployment_env() is True
        assert os.environ["ieasyhydroforecast_ml_long_term_supported_modes"] == "already-set-value"

    def test_missing_dotenv_dependency_fails(self, monkeypatch, tmp_path, capsys):
        """A lazy ImportError for dotenv -> [FAIL] printed and False returned.

        F6: the message must name the venv actually running this process
        (sys.prefix), since under run_locally.sh that is NOT
        apps/validate_pipeline/.venv, and must mention that both that venv
        and apps/validate_pipeline need the dependency.
        """
        env_file = tmp_path / "deploy.env"
        env_file.write_text("ieasyforecast_configuration_path=/x\n")
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "dotenv":
                raise ImportError("no module named dotenv")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        result = vp._load_deployment_env()
        captured = capsys.readouterr()

        assert result is False
        assert "[FAIL]" in captured.out
        assert "python-dotenv is not installed" in captured.out
        # Names the interpreter actually running this process, not a
        # hardcoded (and, under run_locally.sh, wrong) venv guess.
        assert vp.sys.prefix in captured.out
        assert "apps/validate_pipeline" in captured.out

    def test_comment_only_env_file_fails_main(self, monkeypatch, tmp_path, capsys):
        """F2: a comment-only .env file loads nothing -> main() exits 1,
        [FAIL] names the path (load_dotenv's return value is not ignored).
        """
        env_file = tmp_path / "comment_only.env"
        env_file.write_text("# just a comment\n\n# another comment\n")
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))

        rc = vp.main(["--target", "short-term"])
        captured = capsys.readouterr()

        assert rc == 1
        assert "[FAIL]" in captured.out
        assert str(env_file) in captured.out

    def test_non_utf8_env_file_fails_without_traceback(self, monkeypatch, tmp_path, capsys):
        """F3: non-UTF-8 bytes in the env file -> [FAIL], no traceback.

        Verified against the installed python-dotenv: this raises
        UnicodeDecodeError, which must not escape as a traceback per the
        module's documented exit contract (0/1 only).
        """
        env_file = tmp_path / "bad_encoding.env"
        env_file.write_bytes(b"ieasyforecast_configuration_path=\xff\xfe\x00bad\n")
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))

        rc = vp.main(["--target", "short-term"])
        captured = capsys.readouterr()

        assert rc == 1
        assert "[FAIL]" in captured.out
        assert "UnicodeDecodeError" in captured.out

    def test_env_file_pointer_absent_by_default(self):
        """F4: the conftest autouse fixture keeps the pointer out of
        os.environ unless a test sets it itself."""
        assert "ieasyhydroforecast_env_file_path" not in os.environ

    def test_ambient_env_vars_absent_by_default(self):
        """R4: the conftest autouse fixture also clears the other exported
        variables that previously made the suite's result depend on the
        developer's shell state (PYTHON_DOTENV_DISABLED breaks
        test_env_file_loaded_when_pointer_set; SAPPHIRE_API_ENABLED=false
        breaks the main()-level long-term exit tests)."""
        for var in (
            "PYTHON_DOTENV_DISABLED",
            "SAPPHIRE_API_ENABLED",
            "SAPPHIRE_API_URL",
            "SAPPHIRE_PREDICTION_MODE",
            "FRESHNESS_THRESHOLD_DAYS",
        ):
            assert var not in os.environ


class TestLongTermResolverFailure:
    """Change 2: a resolver failure must surface as a FAIL CheckResult.

    Without the fix, run_tier1_long_term propagates an uncaught
    LongTermHorizonResolverError, violating the module's documented exit
    contract (0/1 only, no traceback).
    """

    def test_missing_resolver_env_produces_fail_checkresult(self, monkeypatch, mock_post_client):
        """With resolver vars absent, run_tier1_long_term returns a FAIL row
        instead of raising LongTermHorizonResolverError."""
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)

        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        # Must not raise vp.LongTermHorizonResolverError.
        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))

        fail_rows = [
            r for r in results if r.status == "FAIL" and r.module == "long_term_forecasting"
        ]
        assert fail_rows, f"expected a FAIL row for the unresolved horizon config, got: {results}"
        assert fail_rows[-1].detail
        # F1: this is a configuration failure, not a per-module data
        # finding — it must be marked critical so it survives --module
        # filtering and forces a non-zero exit even under --phase pre.
        assert fail_rows[-1].critical is True

    def test_missing_mode_config_file_produces_fail_checkresult(
        self, mock_post_client, _long_term_resolver_env
    ):
        """F7: FileNotFoundError (missing mode JSON) routes through the
        same loud FAIL path as LongTermHorizonResolverError, not a
        traceback.

        Reproduced against apps/iEasyHydroForecast/long_term_horizon_resolver.py:184,
        which raises FileNotFoundError when a supported mode's config file
        is absent.
        """
        config_dir = _long_term_resolver_env
        (config_dir / "quarter.json").unlink()

        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        # Must not raise FileNotFoundError.
        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))

        fail_rows = [
            r for r in results if r.status == "FAIL" and r.module == "long_term_forecasting"
        ]
        assert fail_rows, f"expected a FAIL row for the missing mode config, got: {results}"
        assert fail_rows[-1].critical is True
        assert "quarter" in fail_rows[-1].detail.lower()

    def test_no_seasonal_modes_supported_produces_fail_checkresult(
        self, monkeypatch, mock_post_client
    ):
        """F7: the ValueError raised by validate_pipeline's own
        _resolved_seasonal_presence_horizon_value (":610", "No seasonal
        long-term modes are supported by this deployment.") routes through
        the same loud FAIL path, not a traceback.
        """
        # Only "quarter" is supported — no seasonal_* mode is, so
        # _resolved_seasonal_presence_horizon_value must raise ValueError.
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")

        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        # Must not raise ValueError.
        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))

        fail_rows = [
            r for r in results if r.status == "FAIL" and r.module == "long_term_forecasting"
        ]
        assert fail_rows, f"expected a FAIL row for the unsupported seasonal modes, got: {results}"
        assert fail_rows[-1].critical is True
        assert "seasonal" in fail_rows[-1].detail.lower()

    def test_mode_config_path_is_directory_produces_fail_checkresult(
        self, mock_post_client, _long_term_resolver_env
    ):
        """R2: an OSError subclass other than FileNotFoundError must also
        route through the loud FAIL path, not a traceback.

        Reproduced against
        apps/iEasyHydroForecast/long_term_horizon_resolver.py:187
        (`with config_path.open() as config_file`): when the mode config
        path is a directory, `.exists()` is True (so the FileNotFoundError
        branch is not taken) but `.open()` raises IsADirectoryError, which
        is an OSError subclass but not a FileNotFoundError.
        """
        config_dir = _long_term_resolver_env
        (config_dir / "quarter.json").unlink()
        (config_dir / "quarter.json").mkdir()

        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})
        mock_post_client.read_long_term_forecasts.return_value = single_row
        mock_post_client.read_skill_metrics.return_value = single_row

        # Must not raise IsADirectoryError.
        results = vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))

        fail_rows = [
            r for r in results if r.status == "FAIL" and r.module == "long_term_forecasting"
        ]
        assert fail_rows, f"expected a FAIL row for the directory mode config path, got: {results}"
        assert fail_rows[-1].critical is True

    def test_check_presence_valueerror_not_mislabelled_though_still_propagates(
        self, mock_post_client, _long_term_resolver_env
    ):
        """Pins that a ValueError raised by check_presence itself (e.g.
        pandas' pd.to_datetime on a malformed response with duplicate
        'date' columns) is NOT caught by the horizon-resolution guard and
        mislabelled as "Long-term horizon configuration" — that guard
        wraps only the two resolver calls, not the check_presence() calls.

        This does NOT assert that the traceback itself is correct
        behaviour. It is a KNOWN PRE-EXISTING VIOLATION of this module's
        documented exit contract (main() must return 0 or 1, never raise)
        that this ValueError propagates uncaught at all, and it is tracked
        as a separate issue rather than fixed here. When that is fixed
        (e.g. by turning it into a reported FAIL CheckResult instead of
        letting it escape), THIS TEST MUST BE UPDATED to assert on the
        resulting CheckResult — it must not be treated as a blocker for
        that fix.
        """
        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})
        # A DataFrame with two columns named "date" makes df["date"] return
        # a DataFrame instead of a Series, so pd.to_datetime(...) raises
        # ValueError: "cannot assemble with duplicate keys".
        malformed = pd.DataFrame(
            [["19999", "2026-02-23", "2026-02-24"]],
            columns=["code", "date", "date"],
        )

        def fake_read_long_term_forecasts(**kwargs):
            if kwargs.get("horizon_type") == "quarter":
                return malformed
            return single_row

        mock_post_client.read_long_term_forecasts.side_effect = fake_read_long_term_forecasts
        mock_post_client.read_skill_metrics.return_value = single_row

        with pytest.raises(ValueError, match="duplicate keys"):
            vp.run_tier1_long_term(mock_post_client, date(2026, 2, 23))


class TestLongTermResolverGuardsAreIndependent:
    """S2: the quarter and seasonal horizon resolutions in
    run_tier1_long_term must be guarded INDEPENDENTLY — a failure
    resolving one must not suppress the presence checks for the other.

    Pins the regression from the previous round, which hoisted both
    resolver calls into one shared try/except that returned immediately on
    the first failure: with a valid quarter config and no supported
    seasonal mode, that version ran ZERO quarter checks. Reverting the
    independent try/except blocks in run_tier1_long_term back into one
    combined block, or reinstating the early `return results` inside the
    guard, makes these tests fail.
    """

    @staticmethod
    def _mocked_clients():
        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})

        mock_pre = MagicMock()
        mock_pre.readiness_check.return_value = True
        mock_pre.read_runoff.return_value = pd.DataFrame()
        mock_pre.read_hydrograph.return_value = pd.DataFrame()
        mock_pre.read_meteo.return_value = pd.DataFrame()
        mock_pre.read_snow.return_value = pd.DataFrame()

        mock_post = MagicMock()
        mock_post.readiness_check.return_value = True
        mock_post.read_long_term_forecasts.return_value = single_row
        mock_post.read_skill_metrics.return_value = single_row
        mock_post.read_short_term_forecasts.return_value = pd.DataFrame()
        mock_post.read_lr_forecasts.return_value = pd.DataFrame()
        return mock_pre, mock_post

    def _run(self, argv):
        mock_pre, mock_post = self._mocked_clients()
        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            return vp.main(argv)

    def test_quarter_valid_seasonal_fails_both_quarter_checks_still_run(self, monkeypatch, capsys):
        """Quarter config resolves; no seasonal mode is supported. Both
        quarter checks must still appear, plus a seasonal critical FAIL
        row, and the run must exit non-zero."""
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")

        rc = self._run(["--target", "long-term"])
        captured = capsys.readouterr()

        assert "Long-term forecasts (quarter hv" in captured.out
        assert "Quarterly skill metrics" in captured.out
        assert "Long-term horizon configuration (seasonal)" in captured.out
        assert rc == 1

    def test_seasonal_valid_quarter_fails_both_seasonal_checks_still_run(
        self, _long_term_resolver_env, capsys
    ):
        """The quarter mode config file is missing; seasonal modes remain
        supported and resolvable. Both seasonal checks must still appear,
        plus a quarter critical FAIL row, and the run must exit non-zero."""
        (_long_term_resolver_env / "quarter.json").unlink()

        rc = self._run(["--target", "long-term"])
        captured = capsys.readouterr()

        assert "Long-term forecasts (season issue" in captured.out
        assert "Seasonal skill metrics" in captured.out
        assert "Long-term horizon configuration (quarter)" in captured.out
        assert rc == 1

    def test_both_resolutions_fail_two_critical_rows_no_presence_checks(self, monkeypatch, capsys):
        """Neither resolution can succeed (all resolver env vars absent):
        two distinct critical FAIL rows are reported and neither pair of
        presence checks runs."""
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)

        rc = self._run(["--target", "long-term"])
        captured = capsys.readouterr()

        assert "Long-term horizon configuration (quarter)" in captured.out
        assert "Long-term horizon configuration (seasonal)" in captured.out
        assert "Long-term forecasts (quarter hv" not in captured.out
        assert "Long-term forecasts (season issue" not in captured.out
        assert "Quarterly skill metrics" not in captured.out
        assert "Seasonal skill metrics" not in captured.out
        assert rc == 1


class TestLongTermConfigFailureExitCode:
    """F1: a resolver failure must exit non-zero and print a visible
    failure message for EVERY --target/--module combination — it must
    never be silently dropped by --module filtering.

    test_module_postprocessing_forecasts is the one that pins the actual
    regression: before the fix, the new FAIL CheckResult was attributed
    module="long_term_forecasting", the --module postprocessing_forecasts
    filter dropped it, and main() exited 0 having performed no long-term
    validation at all. Reverting the `critical` bypass in validate()'s
    module-filtering of t1_lt makes this test fail.
    """

    @staticmethod
    def _mocked_clients():
        # read_long_term_forecasts / read_skill_metrics return a non-empty
        # row so the "Long-term forecasts (month)" and "Monthly skill
        # metrics" Tier 1 checks PASS — isolating the exit code / message
        # assertions below to the resolver failure itself, rather than
        # incidental FAILs from unrelated empty mocks.
        single_row = pd.DataFrame({"code": ["19999"], "date": ["2026-02-23"]})

        mock_pre = MagicMock()
        mock_pre.readiness_check.return_value = True
        mock_pre.read_runoff.return_value = pd.DataFrame()
        mock_pre.read_hydrograph.return_value = pd.DataFrame()
        mock_pre.read_meteo.return_value = pd.DataFrame()
        mock_pre.read_snow.return_value = pd.DataFrame()

        mock_post = MagicMock()
        mock_post.readiness_check.return_value = True
        mock_post.read_long_term_forecasts.return_value = single_row
        mock_post.read_skill_metrics.return_value = single_row
        mock_post.read_short_term_forecasts.return_value = pd.DataFrame()
        mock_post.read_lr_forecasts.return_value = pd.DataFrame()
        return mock_pre, mock_post

    def _run(self, monkeypatch, argv):
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)
        mock_pre, mock_post = self._mocked_clients()
        with (
            patch.object(vp, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            return vp.main(argv)

    def test_module_long_term_forecasting(self, monkeypatch, capsys):
        rc = self._run(
            monkeypatch,
            ["--target", "long-term", "--module", "long_term_forecasting"],
        )
        captured = capsys.readouterr()
        assert rc == 1
        assert "Long-term horizon configuration" in captured.out

    def test_module_postprocessing_forecasts(self, monkeypatch, capsys):
        """The regression case: module_filter="postprocessing_forecasts"
        does not match the FAIL row's module="long_term_forecasting"."""
        rc = self._run(
            monkeypatch,
            ["--target", "long-term", "--module", "postprocessing_forecasts"],
        )
        captured = capsys.readouterr()
        assert rc == 1
        assert "Long-term horizon configuration" in captured.out

    def test_target_long_term_no_module(self, monkeypatch, capsys):
        rc = self._run(monkeypatch, ["--target", "long-term"])
        captured = capsys.readouterr()
        assert rc == 1
        assert "Long-term horizon configuration" in captured.out

    def test_phase_pre_still_fails_on_critical_row(self, monkeypatch, tmp_path):
        """F1's invariant must hold even under --phase pre, which otherwise
        returns 0 unconditionally for ordinary FAIL rows (out of scope,
        left unchanged — see the comment in validate())."""
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)
        mock_pre, mock_post = self._mocked_clients()

        with (
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            rc = vp.validate(
                "long-term",
                date(2026, 2, 23),
                ["month"],
                phase="pre",
                baseline_path=str(tmp_path / "baseline.json"),
            )

        assert rc == 1

    def test_phase_pre_leaves_existing_baseline_untouched_on_critical_row(
        self, monkeypatch, tmp_path
    ):
        """R1: a critical row (the horizon config could not be resolved)
        must NOT overwrite an existing baseline file with an incomplete
        snapshot. Before the fix, write_baseline() ran unconditionally
        before the critical-row check, so a config failure corrupted a
        good baseline right before returning non-zero. Asserting only the
        exit code (as the previous round's test did) does not catch this —
        the file content must be checked too.
        """
        for var in _RESOLVER_VARS:
            monkeypatch.delenv(var, raising=False)
        mock_pre, mock_post = self._mocked_clients()

        baseline_path = tmp_path / "baseline.json"
        sentinel = b'{"sentinel": "do-not-touch", "forecast_date": "2020-01-01"}'
        baseline_path.write_bytes(sentinel)

        with (
            patch.object(vp, "SapphirePreprocessingClient", return_value=mock_pre),
            patch.object(vp, "SapphirePostprocessingClient", return_value=mock_post),
        ):
            rc = vp.validate(
                "long-term",
                date(2026, 2, 23),
                ["month"],
                phase="pre",
                baseline_path=str(baseline_path),
            )

        assert rc == 1
        assert baseline_path.read_bytes() == sentinel
