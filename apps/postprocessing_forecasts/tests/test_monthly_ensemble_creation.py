"""Tests for create_monthly_ensemble_forecasts() in src/ensemble_calculator.py.

Covers EM (threshold-filtered average), Skilled Mean (1/MAE weighted),
and Naive Mean (unweighted all-model average) creation for monthly
forecasts.  Tests verify discharge values, quantile propagation,
composition strings, baseline exclusion, and edge cases.
"""

import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ensemble_calculator import create_monthly_ensemble_forecasts

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


def _make_skill(rows):
    """Build skill_stats DataFrame.

    Each row: (month_in_year, code, model_short,
               sdivsigma, nse, delta, accuracy, mae, n_pairs)
    """
    return pd.DataFrame(
        rows,
        columns=[
            "month_in_year",
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


def _make_forecasts(rows):
    """Build forecasts DataFrame.

    Each row: (code, year, month, month_in_year, model_short,
               forecasted_discharge, q05, q10, q25, q50, q75, q90, q95,
               valid_from, valid_to, date, flag)
    """
    return pd.DataFrame(
        rows,
        columns=[
            "code",
            "year",
            "month",
            "month_in_year",
            "model_short",
            "forecasted_discharge",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
            "valid_from",
            "valid_to",
            "date",
            "flag",
        ],
    )


def _two_model_skill():
    """Two highly skilled models (LR, TFT) for station S1, month 3."""
    return _make_skill(
        [
            # Both pass all thresholds: sdivsigma<0.6, nse>0.8, accuracy>0.8
            (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
        ]
    )


def _two_model_forecasts():
    """Forecasts for LR and TFT, station S1, March 2025."""
    return _make_forecasts(
        [
            (
                "S1",
                2025,
                3,
                3,
                "LR",
                100.0,
                80.0,
                85.0,
                90.0,
                100.0,
                110.0,
                115.0,
                120.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "TFT",
                120.0,
                90.0,
                95.0,
                100.0,
                120.0,
                130.0,
                135.0,
                140.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
        ]
    )


# ---------------------------------------------------------------------------
# TestEMCreation
# ---------------------------------------------------------------------------


class TestEMCreation:
    """Ensemble Mean: threshold-filtered multi-model average."""

    def test_em_discharge_is_mean_of_qualified_models(self):
        """EM forecasted_discharge = mean(LR, TFT) when both pass."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1, "Expected exactly one EM row"
        expected = (100.0 + 120.0) / 2.0
        assert em_rows.iloc[0]["forecasted_discharge"] == pytest.approx(expected)

    def test_em_quantiles_are_averaged(self):
        """EM quantile columns are simple means of contributing models."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"].iloc[0]
        # LR q05=80, TFT q05=90 -> EM q05=85
        assert em["q05"] == pytest.approx(85.0)
        assert em["q50"] == pytest.approx(110.0)
        assert em["q95"] == pytest.approx(130.0)

    def test_em_composition_string(self):
        """EM composition lists contributing models, sorted."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"].iloc[0]
        assert em["composition"] == "LR, TFT"

    def test_em_single_model_discarded(self):
        """EM is not created when only one model passes thresholds."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.9, 0.50, 5.0, 0.50, 8.0, 10),  # fails
            ]
        )
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 0, "Single-model EM should be discarded"

    def test_em_excludes_baseline_models(self):
        """Baseline models (EM, Naive Mean, Skilled Mean) are not in pool."""
        # Add an existing EM row to forecasts -- it should not be included
        # in the new EM computation
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S1", "EM", 0.1, 0.99, 5.0, 0.99, 0.5, 10),
            ]
        )
        rows = [
            (
                "S1",
                2025,
                3,
                3,
                "LR",
                100.0,
                80.0,
                85.0,
                90.0,
                100.0,
                110.0,
                115.0,
                120.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "TFT",
                120.0,
                90.0,
                95.0,
                100.0,
                120.0,
                130.0,
                135.0,
                140.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "EM",
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
        ]
        forecasts = _make_forecasts(rows)

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em_new = result[(result["model_short"] == "EM") & (result["forecasted_discharge"] != 999.0)]
        assert len(em_new) == 1
        # EM should be mean of LR(100) and TFT(120) only, not 999
        assert em_new.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)

    def test_em_three_models_qualified(self):
        """EM averages three models when all pass thresholds."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S1", "TiDE", 0.2, 0.92, 5.0, 0.88, 2.5, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TiDE",
                    130.0,
                    100.0,
                    105.0,
                    110.0,
                    130.0,
                    140.0,
                    145.0,
                    150.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"]
        assert len(em) == 1
        expected = (100.0 + 120.0 + 130.0) / 3.0
        assert em.iloc[0]["forecasted_discharge"] == pytest.approx(expected)
        assert em.iloc[0]["composition"] == "LR, TFT, TiDE"

    def test_em_flag_is_zero(self):
        """Ensemble rows get flag=0."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"]
        assert len(em) == 1
        assert em.iloc[0]["flag"] == 0


# ---------------------------------------------------------------------------
# TestSkilledMeanCreation
# ---------------------------------------------------------------------------


class TestSkilledMeanCreation:
    """Skilled Mean: 1/MAE weighted average of threshold-filtered models."""

    def test_skilled_mean_is_mae_weighted(self):
        """Skilled Mean weights by 1/(MAE + eps)."""
        # LR MAE=2.0, TFT MAE=4.0 => LR has higher weight
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 4.0, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    200.0,
                    160.0,
                    170.0,
                    180.0,
                    200.0,
                    210.0,
                    215.0,
                    220.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm_rows = result[result["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 1

        # Compute expected weights: eps = mean(MAE) / 100 = 3.0 / 100 = 0.03
        eps = (2.0 + 4.0) / 2.0 / 100.0  # 0.03
        w_lr = 1.0 / (2.0 + eps)
        w_tft = 1.0 / (4.0 + eps)
        expected = (w_lr * 100.0 + w_tft * 200.0) / (w_lr + w_tft)
        assert sm_rows.iloc[0]["forecasted_discharge"] == pytest.approx(expected, rel=1e-4)

    def test_skilled_mean_quantiles_weighted(self):
        """Skilled Mean quantiles use 1/MAE weighting."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 4.0, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    200.0,
                    160.0,
                    170.0,
                    180.0,
                    200.0,
                    210.0,
                    215.0,
                    220.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm = result[result["model_short"] == "Skilled Mean"].iloc[0]
        eps = (2.0 + 4.0) / 2.0 / 100.0
        w_lr = 1.0 / (2.0 + eps)
        w_tft = 1.0 / (4.0 + eps)
        # q05: LR=80, TFT=160
        expected_q05 = (w_lr * 80.0 + w_tft * 160.0) / (w_lr + w_tft)
        assert sm["q05"] == pytest.approx(expected_q05, rel=1e-4)
        # q95: LR=120, TFT=220
        expected_q95 = (w_lr * 120.0 + w_tft * 220.0) / (w_lr + w_tft)
        assert sm["q95"] == pytest.approx(expected_q95, rel=1e-4)

    def test_skilled_mean_composition(self):
        """Skilled Mean composition lists contributing models."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 1
        assert sm.iloc[0]["composition"] == "LR, TFT"

    def test_skilled_mean_single_model_discarded(self):
        """Skilled Mean is not created with only one qualifying model.

        The monthly Skilled Mean pool is the long-term NSE>0 gate, so TFT must
        have NSE<=0 to be the single-model-discard case (only LR qualifies).
        """
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.9, -0.50, 5.0, 0.50, 8.0, 10),  # NSE<=0: excluded
            ]
        )
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm_rows = result[result["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 0

    def test_skilled_mean_excludes_baselines(self):
        """Baseline model_shorts are not included in the Skilled Mean pool."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S1", "Skilled Mean", 0.1, 0.99, 5.0, 0.99, 0.5, 10),
            ]
        )
        rows = [
            (
                "S1",
                2025,
                3,
                3,
                "LR",
                100.0,
                80.0,
                85.0,
                90.0,
                100.0,
                110.0,
                115.0,
                120.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "TFT",
                120.0,
                90.0,
                95.0,
                100.0,
                120.0,
                130.0,
                135.0,
                140.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "Skilled Mean",
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
        ]
        forecasts = _make_forecasts(rows)

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm_new = result[
            (result["model_short"] == "Skilled Mean") & (result["forecasted_discharge"] != 999.0)
        ]
        assert len(sm_new) == 1
        # Weighted mean of LR and TFT only, not 999
        assert sm_new.iloc[0]["forecasted_discharge"] < 200.0

    def test_skilled_mean_flag_is_zero(self):
        """Skilled Mean rows get flag=0."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        sm = result[result["model_short"] == "Skilled Mean"]
        assert len(sm) == 1
        assert sm.iloc[0]["flag"] == 0


# ---------------------------------------------------------------------------
# TestNaiveMeanCreation
# ---------------------------------------------------------------------------


class TestNaiveMeanCreation:
    """Naive Mean: simple average of ALL non-baseline models."""

    def test_naive_mean_is_simple_average(self):
        """Naive Mean = unweighted mean of all non-baseline models."""
        skill = _two_model_skill()
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TiDE",
                    150.0,
                    120.0,
                    125.0,
                    130.0,
                    150.0,
                    160.0,
                    165.0,
                    170.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        # Naive Mean uses ALL non-baseline models: LR, TFT, TiDE
        expected = (100.0 + 120.0 + 150.0) / 3.0
        assert nm.iloc[0]["forecasted_discharge"] == pytest.approx(expected)

    def test_naive_mean_includes_unskilled_models(self):
        """Naive Mean includes models that fail skill thresholds."""
        # TiDE fails thresholds but should still be in Naive Mean
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S1", "TiDE", 0.9, 0.50, 5.0, 0.50, 8.0, 10),  # fails
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TiDE",
                    150.0,
                    120.0,
                    125.0,
                    130.0,
                    150.0,
                    160.0,
                    165.0,
                    170.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        # All three models contribute (TiDE included despite failing)
        expected = (100.0 + 120.0 + 150.0) / 3.0
        assert nm.iloc[0]["forecasted_discharge"] == pytest.approx(expected)
        assert "TiDE" in nm.iloc[0]["composition"]

    def test_naive_mean_quantiles_averaged(self):
        """Naive Mean quantiles are simple means."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        # LR q05=80, TFT q05=90 -> 85
        assert nm.iloc[0]["q05"] == pytest.approx(85.0)
        # LR q95=120, TFT q95=140 -> 130
        assert nm.iloc[0]["q95"] == pytest.approx(130.0)

    def test_naive_mean_excludes_baselines(self):
        """Naive Mean does not include EM, Skilled Mean, or Naive Mean."""
        skill = _two_model_skill()
        rows = [
            (
                "S1",
                2025,
                3,
                3,
                "LR",
                100.0,
                80.0,
                85.0,
                90.0,
                100.0,
                110.0,
                115.0,
                120.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            (
                "S1",
                2025,
                3,
                3,
                "TFT",
                120.0,
                90.0,
                95.0,
                100.0,
                120.0,
                130.0,
                135.0,
                140.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
            # Pre-existing EM row should not affect Naive Mean
            (
                "S1",
                2025,
                3,
                3,
                "EM",
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                999.0,
                "2025-03-01",
                "2025-03-31",
                "2025-03-01",
                0,
            ),
        ]
        forecasts = _make_forecasts(rows)

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        # Mean of LR(100) and TFT(120) only, not the 999 EM
        assert nm.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)

    def test_naive_mean_composition_all_models(self):
        """Naive Mean composition lists all non-baseline models."""
        skill = _two_model_skill()
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TiDE",
                    130.0,
                    100.0,
                    105.0,
                    110.0,
                    130.0,
                    140.0,
                    145.0,
                    150.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        assert nm.iloc[0]["composition"] == "LR, TFT, TiDE"

    def test_naive_mean_single_model_discarded(self):
        """Naive Mean is not created when only one non-baseline model."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 0

    def test_naive_mean_flag_is_zero(self):
        """Naive Mean rows get flag=0."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        assert nm.iloc[0]["flag"] == 0


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: empty inputs, multi-station, quantile handling."""

    def test_empty_skill_stats_returns_forecasts_unchanged(self):
        """Empty skill_stats returns forecasts without ensemble rows."""
        forecasts = _two_model_forecasts()
        skill = pd.DataFrame(
            columns=[
                "month_in_year",
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

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        assert len(result) == len(forecasts)
        assert "EM" not in result["model_short"].values
        assert "Naive Mean" not in result["model_short"].values
        assert "Skilled Mean" not in result["model_short"].values

    def test_empty_forecasts_returns_empty_dataframe(self):
        """Empty forecasts returns empty DataFrame."""
        forecasts = pd.DataFrame(
            columns=[
                "code",
                "year",
                "month",
                "month_in_year",
                "model_short",
                "forecasted_discharge",
                "q05",
                "q10",
                "q25",
                "q50",
                "q75",
                "q90",
                "q95",
                "valid_from",
                "valid_to",
                "date",
                "flag",
            ]
        )
        skill = _two_model_skill()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        assert result.empty

    def test_all_quantile_columns_propagated(self):
        """All seven quantile columns are present in ensemble rows."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()
        qcols = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        for ensemble_type in ("EM", "Skilled Mean", "Naive Mean"):
            rows = result[result["model_short"] == ensemble_type]
            if rows.empty:
                continue
            for qc in qcols:
                assert qc in rows.columns, f"{qc} missing from {ensemble_type} rows"
                val = rows.iloc[0][qc]
                assert pd.notna(val), f"{qc} is NaN in {ensemble_type} row"

    def test_multi_station_independent_ensembles(self):
        """Each station gets its own ensemble rows."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S2", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S2", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S2",
                    2025,
                    3,
                    3,
                    "LR",
                    200.0,
                    180.0,
                    185.0,
                    190.0,
                    200.0,
                    210.0,
                    215.0,
                    220.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S2",
                    2025,
                    3,
                    3,
                    "TFT",
                    240.0,
                    210.0,
                    215.0,
                    220.0,
                    240.0,
                    250.0,
                    255.0,
                    260.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em_s1 = result[(result["model_short"] == "EM") & (result["code"] == "S1")]
        em_s2 = result[(result["model_short"] == "EM") & (result["code"] == "S2")]
        assert len(em_s1) == 1
        assert len(em_s2) == 1
        assert em_s1.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)
        assert em_s2.iloc[0]["forecasted_discharge"] == pytest.approx(220.0)

    def test_multi_month_independent_ensembles(self):
        """Each month gets independent ensemble rows."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (4, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (4, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    120.0,
                    90.0,
                    95.0,
                    100.0,
                    120.0,
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    4,
                    4,
                    "LR",
                    200.0,
                    180.0,
                    185.0,
                    190.0,
                    200.0,
                    210.0,
                    215.0,
                    220.0,
                    "2025-04-01",
                    "2025-04-30",
                    "2025-04-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    4,
                    4,
                    "TFT",
                    240.0,
                    210.0,
                    215.0,
                    220.0,
                    240.0,
                    250.0,
                    255.0,
                    260.0,
                    "2025-04-01",
                    "2025-04-30",
                    "2025-04-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em_m3 = result[(result["model_short"] == "EM") & (result["month"] == 3)]
        em_m4 = result[(result["model_short"] == "EM") & (result["month"] == 4)]
        assert len(em_m3) == 1
        assert len(em_m4) == 1
        assert em_m3.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)
        assert em_m4.iloc[0]["forecasted_discharge"] == pytest.approx(220.0)

    def test_original_forecasts_preserved(self):
        """Original forecast rows are present unchanged in the result."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        lr_rows = result[result["model_short"] == "LR"]
        tft_rows = result[result["model_short"] == "TFT"]
        assert len(lr_rows) == 1
        assert len(tft_rows) == 1
        assert lr_rows.iloc[0]["forecasted_discharge"] == 100.0
        assert tft_rows.iloc[0]["forecasted_discharge"] == 120.0

    def test_nan_forecasted_discharge_dropped(self):
        """Models with NaN forecasted_discharge are excluded from EM."""
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
                (3, "S1", "TFT", 0.4, 0.88, 5.0, 0.85, 3.0, 10),
                (3, "S1", "TiDE", 0.2, 0.92, 5.0, 0.88, 2.5, 10),
            ]
        )
        forecasts = _make_forecasts(
            [
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "LR",
                    100.0,
                    80.0,
                    85.0,
                    90.0,
                    100.0,
                    110.0,
                    115.0,
                    120.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TFT",
                    float("nan"),
                    90.0,
                    95.0,
                    100.0,
                    float("nan"),
                    130.0,
                    135.0,
                    140.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
                (
                    "S1",
                    2025,
                    3,
                    3,
                    "TiDE",
                    130.0,
                    100.0,
                    105.0,
                    110.0,
                    130.0,
                    140.0,
                    145.0,
                    150.0,
                    "2025-03-01",
                    "2025-03-31",
                    "2025-03-01",
                    0,
                ),
            ]
        )

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"]
        assert len(em) == 1
        # EM from LR(100) and TiDE(130) only; TFT(NaN) dropped
        expected = (100.0 + 130.0) / 2.0
        assert em.iloc[0]["forecasted_discharge"] == pytest.approx(expected)
        assert "TFT" not in em.iloc[0]["composition"]

    def test_no_models_pass_thresholds(self):
        """No ensembles created when all models fail thresholds.

        EM uses the default gate; the monthly Skilled Mean uses the long-term
        NSE>0 gate, so both models must have NSE<=0 for the Skilled-Mean pool to
        be empty as well.
        """
        skill = _make_skill(
            [
                (3, "S1", "LR", 0.9, -0.50, 5.0, 0.50, 8.0, 10),
                (3, "S1", "TFT", 0.9, -0.40, 5.0, 0.40, 9.0, 10),
            ]
        )
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        # EM and Skilled Mean should not be created (no qualifying models)
        assert "EM" not in result["model_short"].values
        assert "Skilled Mean" not in result["model_short"].values
        # Naive Mean should still be created (uses all non-baseline models)
        nm = result[result["model_short"] == "Naive Mean"]
        assert len(nm) == 1
        assert nm.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)

    def test_all_three_ensembles_created(self):
        """EM, Skilled Mean, and Naive Mean all appear when conditions met."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        ensemble_types = set(
            result[result["model_short"].isin({"EM", "Skilled Mean", "Naive Mean"})]["model_short"]
        )
        assert ensemble_types == {"EM", "Skilled Mean", "Naive Mean"}

    def test_result_row_count(self):
        """Result has original rows + ensemble rows (one per type)."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        # 2 original + 3 ensemble types = 5 rows
        assert len(result) == 5

    def test_month_in_year_derived_from_month_column(self):
        """month_in_year is derived from month if missing."""
        skill = _two_model_skill()
        forecasts = _two_model_forecasts()
        # Remove month_in_year to test derivation
        forecasts = forecasts.drop(columns=["month_in_year"])

        with patch.dict(os.environ, THRESHOLD_ENV):
            result = create_monthly_ensemble_forecasts(forecasts, skill)

        # Should still work -- month_in_year derived from month
        em = result[result["model_short"] == "EM"]
        assert len(em) == 1
        assert em.iloc[0]["forecasted_discharge"] == pytest.approx(110.0)


class TestMonthlyEnsembleHorizonValueGroupby:
    """PP-032: Ensembles must be computed per horizon_value."""

    def test_mixed_horizon_values_produce_separate_ensembles(self):
        """month_0 and month_1 for same target get separate EM rows."""
        forecasts = pd.DataFrame(
            {
                "code": ["15013"] * 4,
                "year": [2024] * 4,
                "month": [4] * 4,
                "month_in_year": [4] * 4,
                "model_short": ["GBT", "LR_Base", "GBT", "LR_Base"],
                "horizon_value": [0, 0, 1, 1],
                "date": [
                    "2024-04-10",
                    "2024-04-10",
                    "2024-03-25",
                    "2024-03-25",
                ],
                "forecasted_discharge": [100.0, 110.0, 90.0, 95.0],
                "valid_from": ["2024-04-01"] * 4,
                "valid_to": ["2024-04-30"] * 4,
                "flag": [0] * 4,
            }
        )
        # Skill stats that qualify both models
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame(
            {
                "month_in_year": [4, 4],
                "code": ["15013", "15013"],
                "model_short": ["GBT", "LR_Base"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.85, 0.9],
                "delta": [0.1, 0.2],
                "accuracy": [0.85, 0.9],
                "mae": [5.0, 6.0],
                "n_pairs": [10, 10],
            }
        )
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)

        # --- EM ---
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 2, f"Expected 2 EM rows (one per horizon_value), got {len(em_rows)}"
        assert set(em_rows["horizon_value"]) == {0, 1}
        # horizon_value=0 ensemble: mean(100, 110) = 105
        em_hv0 = em_rows[em_rows["horizon_value"] == 0]
        assert em_hv0["forecasted_discharge"].iloc[0] == pytest.approx(105.0)
        # horizon_value=1 ensemble: mean(90, 95) = 92.5
        em_hv1 = em_rows[em_rows["horizon_value"] == 1]
        assert em_hv1["forecasted_discharge"].iloc[0] == pytest.approx(92.5)

        # --- Skilled Mean ---
        sm_rows = result[result["model_short"] == "Skilled Mean"]
        assert len(sm_rows) == 2, "Skilled Mean must also separate by horizon_value"
        assert set(sm_rows["horizon_value"]) == {0, 1}

        # --- Naive Mean ---
        nm_rows = result[result["model_short"] == "Naive Mean"]
        assert len(nm_rows) == 2, "Naive Mean must also separate by horizon_value"
        assert set(nm_rows["horizon_value"]) == {0, 1}

    def test_single_horizon_value_works_unchanged(self):
        """When all records have the same horizon_value, behavior unchanged."""
        forecasts = pd.DataFrame(
            {
                "code": ["15013"] * 2,
                "year": [2024] * 2,
                "month": [4] * 2,
                "month_in_year": [4] * 2,
                "model_short": ["GBT", "LR_Base"],
                "horizon_value": [1, 1],
                "date": ["2024-03-25"] * 2,
                "forecasted_discharge": [100.0, 110.0],
                "valid_from": ["2024-04-01"] * 2,
                "valid_to": ["2024-04-30"] * 2,
                "flag": [0] * 2,
            }
        )
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame(
            {
                "month_in_year": [4, 4],
                "code": ["15013", "15013"],
                "model_short": ["GBT", "LR_Base"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.85, 0.9],
                "delta": [0.1, 0.2],
                "accuracy": [0.85, 0.9],
                "mae": [5.0, 6.0],
                "n_pairs": [10, 10],
            }
        )
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1
        assert em_rows["horizon_value"].iloc[0] == 1

    def test_no_horizon_value_column_backward_compat(self):
        """When horizon_value column absent, groupby uses (year, month, code)."""
        forecasts = pd.DataFrame(
            {
                "code": ["15013"] * 2,
                "year": [2024] * 2,
                "month": [4] * 2,
                "month_in_year": [4] * 2,
                "model_short": ["GBT", "LR_Base"],
                "date": ["2024-03-25"] * 2,
                "forecasted_discharge": [100.0, 110.0],
                "valid_from": ["2024-04-01"] * 2,
                "valid_to": ["2024-04-30"] * 2,
                "flag": [0] * 2,
            }
        )
        # Thresholds: sdivsigma < 0.6, nse > 0.8, accuracy > 0.8
        skill_stats = pd.DataFrame(
            {
                "month_in_year": [4, 4],
                "code": ["15013", "15013"],
                "model_short": ["GBT", "LR_Base"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.85, 0.9],
                "delta": [0.1, 0.2],
                "accuracy": [0.85, 0.9],
                "mae": [5.0, 6.0],
                "n_pairs": [10, 10],
            }
        )
        result = create_monthly_ensemble_forecasts(forecasts, skill_stats)
        em_rows = result[result["model_short"] == "EM"]
        assert len(em_rows) == 1
        # horizon_value should NOT be in the output since it wasn't in input
        assert "horizon_value" not in em_rows.columns or em_rows["horizon_value"].isna().all()
