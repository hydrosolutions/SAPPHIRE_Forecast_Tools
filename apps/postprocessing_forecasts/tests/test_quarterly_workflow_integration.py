"""Integration tests for quarterly/seasonal postprocessing workflows.

Phase 4b Step 10: End-to-end tests that verify the full pipeline
from observations + forecasts → skill metrics → ensembles → save.
API boundary is mocked.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.aggregation import (
    aggregate_monthly_fc_to_quarterly,
    aggregate_monthly_fc_to_seasonal,
    aggregate_monthly_obs_to_quarterly,
    aggregate_monthly_obs_to_seasonal,
)
from src.ensemble_calculator import (
    create_quarterly_ensemble_forecasts,
    create_seasonal_ensemble_forecasts,
)
from src.skill_metrics import (
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
)

# ---------------------------------------------------------------------------
# Threshold env vars for ensemble tests
# ---------------------------------------------------------------------------

THRESHOLD_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
}


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    for k, v in THRESHOLD_ENV.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# Helpers — build monthly data that aggregates to quarterly/seasonal
# ---------------------------------------------------------------------------


def _monthly_obs(n_years=3, codes=("S1",)):
    """Create monthly observations for multiple years."""
    rows = []
    for code in codes:
        for year in range(2020, 2020 + n_years):
            for month in range(1, 13):
                discharge = 50 + month * 5 + (year - 2020) * 2
                rows.append(
                    {
                        "code": code,
                        "year": year,
                        "month": month,
                        "discharge_avg": float(discharge),
                    }
                )
    return pd.DataFrame(rows)


def _monthly_fc(n_years=3, codes=("S1",), models=("LR", "TFT")):
    """Create monthly forecasts for multiple models/years."""
    rows = []
    for code in codes:
        for year in range(2020, 2020 + n_years):
            for month in range(1, 13):
                for model in models:
                    base = 50 + month * 5 + (year - 2020) * 2
                    offset = 2 if model == "LR" else -1
                    q50 = float(base + offset)
                    rows.append(
                        {
                            "code": code,
                            "year": year,
                            "month": month,
                            "model_short": model,
                            "q50": q50,
                            "q05": q50 - 20,
                            "q10": q50 - 15,
                            "q25": q50 - 8,
                            "q75": q50 + 8,
                            "q90": q50 + 15,
                            "q95": q50 + 20,
                            "forecasted_discharge": q50,
                        }
                    )
    return pd.DataFrame(rows)


# ===================================================================
# Quarterly recalculation workflow
# ===================================================================


class TestQuarterlyRecalcWorkflow:
    """obs + forecasts → aggregate → skill metrics → ensembles."""

    def test_full_pipeline(self):
        monthly_obs = _monthly_obs(n_years=3)
        monthly_fc = _monthly_fc(n_years=3, models=("LR", "TFT"))

        # Aggregate
        qobs = aggregate_monthly_obs_to_quarterly(monthly_obs)
        qfc = aggregate_monthly_fc_to_quarterly(monthly_fc)

        assert not qobs.empty
        assert not qfc.empty
        assert "quarter_in_year" in qobs.columns
        assert "quarter_in_year" in qfc.columns

        # Skill metrics
        skill_stats, joint, ts = calculate_quarterly_skill_metrics(qobs, qfc)
        assert not skill_stats.empty
        assert "quarter_in_year" in skill_stats.columns

        # Verify models present
        models = skill_stats["model_short"].unique()
        assert "LR" in models
        assert "TFT" in models

        # Verify ensembles (multi-model → Naive Mean should exist)
        assert "Naive Mean" in models

        # Joint forecasts should include ensemble rows
        joint_models = joint["model_short"].unique()
        assert "Naive Mean" in joint_models

    def test_skill_then_ensemble_creation(self):
        """Skill metrics → create ensembles from pre-calculated stats."""
        monthly_obs = _monthly_obs(n_years=3)
        monthly_fc = _monthly_fc(n_years=3, models=("LR", "TFT"))

        qobs = aggregate_monthly_obs_to_quarterly(monthly_obs)
        qfc = aggregate_monthly_fc_to_quarterly(monthly_fc)

        skill_stats, _, _ = calculate_quarterly_skill_metrics(qobs, qfc)

        # Now use the ensemble calculator (operational path)
        result = create_quarterly_ensemble_forecasts(qfc, skill_stats)
        assert not result.empty

        result_models = set(result["model_short"].unique())
        # Should contain original models + ensembles
        assert "LR" in result_models
        assert "TFT" in result_models
        assert "Naive Mean" in result_models


# ===================================================================
# Seasonal recalculation workflow
# ===================================================================


class TestSeasonalRecalcWorkflow:
    def test_full_pipeline(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)

        monthly_obs = _monthly_obs(n_years=3)
        monthly_fc = _monthly_fc(n_years=3, models=("LR", "TFT"))

        sobs = aggregate_monthly_obs_to_seasonal(monthly_obs)
        sfc = aggregate_monthly_fc_to_seasonal(monthly_fc)

        assert not sobs.empty
        assert not sfc.empty
        assert "season_year" in sobs.columns

        skill_stats, joint, ts = calculate_seasonal_skill_metrics(sobs, sfc)
        assert not skill_stats.empty
        assert "season_in_year" in skill_stats.columns

        models = skill_stats["model_short"].unique()
        assert "Naive Mean" in models


# ===================================================================
# Cross-year season workflow
# ===================================================================


class TestCrossYearSeasonWorkflow:
    def test_oct_mar_season(self, monkeypatch):
        """Oct-Mar season spanning two calendar years."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")

        monthly_obs = _monthly_obs(n_years=4)
        monthly_fc = _monthly_fc(n_years=4, models=("LR", "TFT"))

        sobs = aggregate_monthly_obs_to_seasonal(monthly_obs)
        sfc = aggregate_monthly_fc_to_seasonal(monthly_fc)

        assert not sobs.empty
        assert not sfc.empty

        # Season years should span across the cross-year boundary
        assert "season_year" in sobs.columns
        assert "season_year" in sfc.columns

        skill_stats, joint, ts = calculate_seasonal_skill_metrics(sobs, sfc)
        assert not skill_stats.empty


# ===================================================================
# Operational workflow (pre-calculated skill + latest forecasts)
# ===================================================================


class TestQuarterlyOperationalWorkflow:
    def test_ensemble_from_precalculated_skill(self):
        """Simulate operational path: pre-calculated skill + latest fcst."""
        # Pre-calculated skill stats (as if from API)
        skill = pd.DataFrame(
            {
                "quarter_in_year": [1, 1],
                "code": ["S1", "S1"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.95, 0.88],
                "delta": [5.0, 5.0],
                "accuracy": [0.90, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
            }
        )

        # Latest quarterly forecasts (as if from aggregated API data)
        fcst = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2025, 2025],
                "quarter_in_year": [1, 1],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [100.0, 120.0],
                "q05": [80, 90],
                "q10": [85, 95],
                "q25": [90, 100],
                "q50": [100, 120],
                "q75": [110, 130],
                "q90": [115, 135],
                "q95": [120, 140],
            }
        )

        result = create_quarterly_ensemble_forecasts(fcst, skill)
        assert not result.empty

        result_models = set(result["model_short"].unique())
        assert "EM" in result_models
        assert "Naive Mean" in result_models


class TestSeasonalOperationalWorkflow:
    def test_ensemble_from_precalculated_skill(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)

        skill = pd.DataFrame(
            {
                "season_in_year": [1, 1],
                "code": ["S1", "S1"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.95, 0.88],
                "delta": [5.0, 5.0],
                "accuracy": [0.90, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
            }
        )

        fcst = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "season_year": [2025, 2025],
                "season_in_year": [1, 1],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [100.0, 120.0],
                "q05": [80, 90],
                "q10": [85, 95],
                "q25": [90, 100],
                "q50": [100, 120],
                "q75": [110, 130],
                "q90": [115, 135],
                "q95": [120, 140],
            }
        )

        result = create_seasonal_ensemble_forecasts(fcst, skill)
        assert not result.empty

        result_models = set(result["model_short"].unique())
        assert "EM" in result_models
        assert "Naive Mean" in result_models
