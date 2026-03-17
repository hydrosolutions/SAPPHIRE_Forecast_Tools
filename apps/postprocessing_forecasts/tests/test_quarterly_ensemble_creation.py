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
            (1, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (1, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
        ]
    )


def _two_model_quarterly_fcst():
    """Forecasts for LR and TFT, station S1, Q1 2025."""
    return _make_quarterly_fcst(
        [
            ("S1", 2025, 1, "LR", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ("S1", 2025, 1, "TFT", 120.0, 90, 95, 100, 120, 130, 135, 140),
        ]
    )


def _two_model_seasonal_skill():
    """Two highly skilled models for station S1, season 1."""
    return _make_seasonal_skill(
        [
            (1, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (1, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
        ]
    )


def _two_model_seasonal_fcst():
    """Forecasts for LR and TFT, station S1, season_year 2025."""
    return _make_seasonal_fcst(
        [
            ("S1", 2025, 1, "LR", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ("S1", 2025, 1, "TFT", 120.0, 90, 95, 100, 120, 130, 135, 140),
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
        assert "LR" in comp
        assert "TFT" in comp

    def test_em_not_created_single_model(self):
        """Single model should not produce EM."""
        skill = _make_quarterly_skill(
            [
                (1, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                ("S1", 2025, 1, "LR", 100.0, 80, 85, 90, 100, 110, 115, 120),
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
        """Skilled Mean should use 1/MAE weighting (LR gets higher weight)."""
        skill = _two_model_quarterly_skill()
        fcst = _two_model_quarterly_fcst()
        result = create_quarterly_ensemble_forecasts(fcst, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        # LR: MAE=2.0, TFT: MAE=3.0
        # LR has lower MAE → higher weight → result closer to LR (100)
        discharge = sm.iloc[0]["forecasted_discharge"]
        assert discharge < 110.0  # closer to LR's 100 than simple mean 110


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
