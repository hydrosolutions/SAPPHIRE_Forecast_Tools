"""Tests for quarterly/seasonal ensemble creation in ensemble_calculator.py.

Phase 4b Step 4.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ensemble_calculator import (
    create_monthly_ensemble_forecasts,
    create_quarterly_ensemble_forecasts,
    create_seasonal_ensemble_forecasts,
)

# ---------------------------------------------------------------------------
# Threshold env vars applied to all tests
# ---------------------------------------------------------------------------

THRESHOLD_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_quarterly_skill(rows):
    """(quarter_in_year, code, model_short, sdivsigma, nse, delta,
    accuracy, mae, n_pairs) → DataFrame."""
    return pd.DataFrame(
        rows,
        columns=[
            "quarter_in_year",
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "delta",
            "accuracy",
            "mae",
            "n_pairs",
        ],
    )


def _make_quarterly_fcst(rows):
    """(code, year, quarter_in_year, model_short,
    forecasted_discharge, q05..q95) → DataFrame."""
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "year",
            "quarter_in_year",
            "model_short",
            "forecasted_discharge",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ],
    )


def _make_seasonal_skill(rows):
    """(season_in_year, code, model_short, sdivsigma, nse, delta,
    accuracy, mae, n_pairs) → DataFrame."""
    return pd.DataFrame(
        rows,
        columns=[
            "season_in_year",
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "delta",
            "accuracy",
            "mae",
            "n_pairs",
        ],
    )


def _make_seasonal_fcst(rows):
    """(code, season_year, season_in_year, model_short,
    forecasted_discharge, q05..q95) → DataFrame."""
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "season_year",
            "season_in_year",
            "model_short",
            "forecasted_discharge",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ],
    )


def _two_model_quarterly_skill():
    """Two highly skilled models for station S1, quarter 1."""
    return _make_quarterly_skill(
        [
            (1, "S1", "LR_Base", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (1, "S1", "LR_SM", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
        ]
    )


def _two_model_quarterly_fcst():
    """Forecasts for LR_Base and LR_SM, station S1, Q1 2025."""
    return _make_quarterly_fcst(
        [
            ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ("S1", 2025, 1, "LR_SM", 120.0, 90, 95, 100, 120, 130, 135, 140),
        ]
    )


def _two_model_seasonal_skill():
    """Two highly skilled models for station S1, season 1."""
    return _make_seasonal_skill(
        [
            (1, "S1", "LR_Base", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (1, "S1", "LR_SM", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
        ]
    )


def _two_model_seasonal_fcst():
    """Forecasts for LR_Base and LR_SM, station S1, season_year 2025."""
    return _make_seasonal_fcst(
        [
            ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ("S1", 2025, 1, "LR_SM", 120.0, 90, 95, 100, 120, 130, 135, 140),
        ]
    )


# ===================================================================
# Quarterly ensemble creation
# ===================================================================


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    for k, v in THRESHOLD_ENV.items():
        monkeypatch.setenv(k, v)


class TestQuarterlyEnsembleEM:
    def test_em_created(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        assert not em.empty

    def test_em_discharge_is_mean(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        expected = (100.0 + 120.0) / 2
        assert np.isclose(em.iloc[0]["forecasted_discharge"], expected)

    def test_em_quantiles_averaged(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        # q50 = mean(100, 120) = 110
        assert np.isclose(em.iloc[0]["q50"], 110.0)

    def test_em_composition_string(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        comp = str(em.iloc[0]["composition"])
        assert "LR_Base" in comp
        assert "LR_SM" in comp

    def test_em_uses_lr_mean_when_lr_skills_fail_thresholds(self):
        skill = _make_quarterly_skill(
            [
                (1, "S1", "LR_Base", 0.9, -1.0, 5.0, 0.10, 20.0, 10),
                (1, "S1", "LR_SM", 0.8, -0.5, 5.0, 0.20, 30.0, 10),
                (1, "S1", "GBT", 0.3, 0.95, 5.0, 0.90, 1.0, 10),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
                ("S1", 2025, 1, "LR_SM", 120.0, 90, 95, 100, 120, 130, 135, 140),
                ("S1", 2025, 1, "GBT", 1000.0, 900, 925, 950, 1000, 1050, 1075, 1100),
            ]
        )

        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]

        assert len(em) == 1
        assert np.isclose(em.iloc[0]["forecasted_discharge"], 110.0)
        assert np.isclose(em.iloc[0]["q05"], 85.0)
        assert np.isclose(em.iloc[0]["q50"], 110.0)
        assert np.isclose(em.iloc[0]["q95"], 130.0)
        assert str(em.iloc[0]["composition"]) == "LR_Base, LR_SM"

    def test_em_not_created_single_model(self):
        """Single model should not produce EM."""
        skill = _make_quarterly_skill(
            [
                (1, "S1", "LR_Base", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ]
        )
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        assert em.empty


class TestQuarterlyEnsembleSkilledMean:
    def test_skilled_mean_created(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        assert not sm.empty

    def test_skilled_mean_weighted(self):
        """Skilled Mean should use 1/MAE weighting (LR_Base gets higher weight)."""
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        # LR_Base: MAE=2.0, LR_SM: MAE=3.0
        # LR_Base has lower MAE -> higher weight -> result closer to 100
        discharge = sm.iloc[0]["forecasted_discharge"]
        assert discharge < 110.0  # closer to 100 than simple mean 110


class TestQuarterlyEnsembleNaiveMean:
    def test_naive_mean_created(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        nm = result[result["model_short"] == "Naive Mean"]
        assert not nm.empty

    def test_naive_mean_is_unweighted_average(self):
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        nm = result[result["model_short"] == "Naive Mean"]
        expected = (100.0 + 120.0) / 2
        assert np.isclose(nm.iloc[0]["forecasted_discharge"], expected)


class TestQuarterlyEnsembleEdgeCases:
    def test_empty_forecasts(self):
        skill = _two_model_quarterly_skill()
        fcst = pd.DataFrame(
            columns=[
                "code",
                "year",
                "quarter_in_year",
                "model_short",
                "forecasted_discharge",
                "q05",
                "q10",
                "q25",
                "q50",
                "q75",
                "q90",
                "q95",
            ]
        )
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        assert result.empty

    def test_empty_skill_returns_forecasts_only(self):
        fcst = _two_model_quarterly_fcst()
        skill = pd.DataFrame(
            columns=[
                "quarter_in_year",
                "code",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        # Should return original forecasts without ensembles
        assert len(result) == len(fcst)
        models = set(result["model_short"].unique())
        assert "EM" not in models
        assert "Naive Mean" not in models


# ===================================================================
# Seasonal ensemble creation
# ===================================================================


class TestSeasonalEnsembleEM:
    def test_em_created(self):
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        assert not em.empty

    def test_em_discharge_is_mean(self):
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        expected = (100.0 + 120.0) / 2
        assert np.isclose(em.iloc[0]["forecasted_discharge"], expected)

    def test_four_issue_leads_produce_independent_ensembles(self):
        leads = [3, 2, 1, 0]
        issue_dates = {
            3: "2025-01-01",
            2: "2025-02-01",
            1: "2025-03-01",
            0: "2025-04-01",
        }
        forecast_rows = []
        skill_rows = []
        for lead in leads:
            for model_short, offset, mae in [("LR_Base", 0.0, 2.0), ("LR_SM", 20.0, 3.0)]:
                forecast_rows.append(
                    {
                        "code": "PP4_S_SENTINEL",
                        "season_year": 2025,
                        "season_in_year": lead,
                        "date": issue_dates[lead],
                        "valid_from": "2025-04-01",
                        "valid_to": "2025-09-30",
                        "model_short": model_short,
                        "forecasted_discharge": 100.0 + lead + offset,
                        "q05": 80.0 + lead + offset,
                        "q10": 85.0 + lead + offset,
                        "q25": 90.0 + lead + offset,
                        "q50": 100.0 + lead + offset,
                        "q75": 110.0 + lead + offset,
                        "q90": 115.0 + lead + offset,
                        "q95": 120.0 + lead + offset,
                    }
                )
                skill_rows.append(
                    (
                        lead,
                        "PP4_S_SENTINEL",
                        model_short,
                        0.3,
                        0.95,
                        5.0,
                        0.90,
                        mae,
                        10,
                    )
                )

        result = create_seasonal_ensemble_forecasts(
            pd.DataFrame(forecast_rows),
            _make_seasonal_skill(skill_rows),
        )

        for model_short in {"EM", "Naive Mean", "Skilled Mean"}:
            rows = result[result["model_short"] == model_short]
            assert len(rows) == 4
            assert set(rows["season_in_year"]) == {0, 1, 2, 3}
            assert dict(zip(rows["season_in_year"], rows["date"], strict=True)) == issue_dates
            assert set(rows["valid_from"]) == {"2025-04-01"}
            assert set(rows["valid_to"]) == {"2025-09-30"}

    def test_em_uses_lr_mean_when_lr_skills_fail_thresholds(self):
        skill = _make_seasonal_skill(
            [
                (1, "S1", "LR_Base", 0.9, -1.0, 5.0, 0.10, 20.0, 10),
                (1, "S1", "LR_SM", 0.8, -0.5, 5.0, 0.20, 30.0, 10),
                (1, "S1", "GBT", 0.3, 0.95, 5.0, 0.90, 1.0, 10),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
                ("S1", 2025, 1, "LR_SM", 120.0, 90, 95, 100, 120, 130, 135, 140),
                ("S1", 2025, 1, "GBT", 1000.0, 900, 925, 950, 1000, 1050, 1075, 1100),
            ]
        )

        result = create_seasonal_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]

        assert len(em) == 1
        assert np.isclose(em.iloc[0]["forecasted_discharge"], 110.0)
        assert np.isclose(em.iloc[0]["q05"], 85.0)
        assert np.isclose(em.iloc[0]["q50"], 110.0)
        assert np.isclose(em.iloc[0]["q95"], 130.0)
        assert str(em.iloc[0]["composition"]) == "LR_Base, LR_SM"


class TestSeasonalEnsembleNaiveMean:
    def test_naive_mean_created(self):
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        nm = result[result["model_short"] == "Naive Mean"]
        assert not nm.empty

    def test_naive_mean_is_unweighted(self):
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        nm = result[result["model_short"] == "Naive Mean"]
        expected = (100.0 + 120.0) / 2
        assert np.isclose(nm.iloc[0]["forecasted_discharge"], expected)


class TestSeasonalEnsembleEdgeCases:
    def test_empty_forecasts(self):
        skill = _two_model_seasonal_skill()
        fcst = pd.DataFrame(
            columns=[
                "code",
                "season_year",
                "season_in_year",
                "model_short",
                "forecasted_discharge",
                "q05",
                "q10",
                "q25",
                "q50",
                "q75",
                "q90",
                "q95",
            ]
        )
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        assert result.empty

    def test_empty_skill_returns_forecasts_only(self):
        fcst = _two_model_seasonal_fcst()
        skill = pd.DataFrame(
            columns=[
                "season_in_year",
                "code",
                "model_short",
                "sdivsigma",
                "nse",
                "delta",
                "accuracy",
                "mae",
                "n_pairs",
            ]
        )
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        assert len(result) == len(fcst)


# ===================================================================
# Monthly regression
# ===================================================================


class TestMonthlyEnsembleRegression:
    def test_monthly_em_remains_skill_gated(self):
        forecasts = pd.DataFrame(
            {
                "code": ["S1", "S1"],
                "year": [2025, 2025],
                "month": [1, 1],
                "month_in_year": [1, 1],
                "model_short": ["LR_Base", "LR_SM"],
                "forecasted_discharge": [100.0, 120.0],
                "q05": [80.0, 90.0],
                "q10": [85.0, 95.0],
                "q25": [90.0, 100.0],
                "q50": [100.0, 120.0],
                "q75": [110.0, 130.0],
                "q90": [115.0, 135.0],
                "q95": [120.0, 140.0],
            }
        )
        skill = pd.DataFrame(
            {
                "month_in_year": [1, 1],
                "code": ["S1", "S1"],
                "model_short": ["LR_Base", "LR_SM"],
                "sdivsigma": [0.3, 0.9],
                "nse": [0.95, -1.0],
                "delta": [5.0, 5.0],
                "accuracy": [0.90, 0.10],
                "mae": [2.0, 30.0],
                "n_pairs": [10, 10],
            }
        )

        result = create_monthly_ensemble_forecasts(forecasts, skill)

        assert result[result["model_short"] == "EM"].empty
