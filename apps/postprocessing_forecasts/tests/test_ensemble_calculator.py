"""Tests for src/ensemble_calculator.py — ensemble creation from skill metrics."""

import logging
import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.dirname(__file__))

import tag_library as tl
from src.ensemble_calculator import (
    create_ensemble_forecasts,
    filter_for_highly_skilled_forecasts,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def skill_stats_pentad():
    """Skill stats with two highly-skilled models for code 10001."""
    return pd.DataFrame(
        {
            "pentad_in_year": [1, 1, 1],
            "code": ["10001", "10001", "10001"],
            "model_short": ["LR", "TFT", "TiDE"],
            "sdivsigma": [0.3, 0.4, 0.9],  # TiDE fails sdivsigma threshold
            "nse": [0.95, 0.9, 0.5],  # TiDE fails nse threshold
            "delta": [5.0, 5.0, 5.0],
            "accuracy": [0.95, 0.88, 0.6],  # TiDE fails accuracy threshold
            "mae": [2.0, 3.0, 8.0],
            "n_pairs": [10, 10, 10],
        }
    )


@pytest.fixture
def forecasts_pentad():
    """Forecast data for two models across multiple dates."""
    dates = pd.to_datetime(["2024-01-05", "2024-01-05", "2024-01-10", "2024-01-10"])
    discharges = [100.0, 110.0, 120.0, 130.0]
    return pd.DataFrame(
        {
            "code": ["10001"] * 4,
            "date": dates,
            "pentad_in_year": [1, 1, 2, 2],
            "pentad_in_month": ["1", "1", "2", "2"],  # string like tl.get_pentad
            "forecasted_discharge": discharges,
            "q05": [d * 0.9 for d in discharges],
            "q25": [d * 0.95 for d in discharges],
            "q75": [d * 1.05 for d in discharges],
            "q95": [d * 1.1 for d in discharges],
            "model_short": ["LR", "TFT", "LR", "TFT"],
        }
    )


# ---------------------------------------------------------------------------
# Filter tests
# ---------------------------------------------------------------------------


class TestFilterHighlySkilled:
    def test_default_thresholds(self, skill_stats_pentad):
        """With default thresholds, only LR and TFT pass."""
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            result = filter_for_highly_skilled_forecasts(skill_stats_pentad)
            assert len(result) == 2
            assert set(result["model_short"]) == {"LR", "TFT"}

    def test_custom_thresholds_all_pass(self, skill_stats_pentad):
        """With very loose thresholds, all models pass."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma=10.0,
            threshold_accuracy=0.0,
            threshold_nse=0.0,
        )
        assert len(result) == 3

    def test_disabled_threshold_via_false(self, skill_stats_pentad):
        """Threshold='False' disables that filter."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma="False",
            threshold_accuracy="False",
            threshold_nse="False",
        )
        assert len(result) == 3

    def test_strict_thresholds_none_pass(self, skill_stats_pentad):
        """With very strict thresholds, no models pass."""
        result = filter_for_highly_skilled_forecasts(
            skill_stats_pentad,
            threshold_sdivsigma=0.01,
            threshold_accuracy=0.99,
            threshold_nse=0.99,
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Ensemble creation tests
# ---------------------------------------------------------------------------


class TestCreateEnsembleForecasts:
    def _make_ensemble(self, forecasts, skill_stats, period="pentad"):
        """Helper to call create_ensemble_forecasts with pentad defaults."""
        if period == "pentad":
            return create_ensemble_forecasts(
                forecasts=forecasts,
                skill_stats=skill_stats,
                period_col="pentad_in_year",
                period_in_month_col="pentad_in_month",
                get_period_in_month_func=tl.get_pentad,
            )
        else:
            return create_ensemble_forecasts(
                forecasts=forecasts,
                skill_stats=skill_stats,
                period_col="decad_in_year",
                period_in_month_col="decad_in_month",
                get_period_in_month_func=tl.get_decad_in_month,
            )

    def test_ensemble_created_for_qualified_models(self, forecasts_pentad, skill_stats_pentad):
        """Ensemble is created from LR+TFT (TiDE excluded by threshold)."""
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, skill_out = self._make_ensemble(forecasts_pentad, skill_stats_pentad)
            # Should contain original models plus EM rows
            assert "EM" in joint["model_short"].values
            # Ensemble discharge should be mean of LR and TFT
            em_rows = joint[joint["model_short"] == "EM"]
            for _, row in em_rows.iterrows():
                date = row["date"]
                lr_val = forecasts_pentad[
                    (forecasts_pentad["date"] == date) & (forecasts_pentad["model_short"] == "LR")
                ]["forecasted_discharge"].iloc[0]
                tft_val = forecasts_pentad[
                    (forecasts_pentad["date"] == date) & (forecasts_pentad["model_short"] == "TFT")
                ]["forecasted_discharge"].iloc[0]
                expected_mean = (lr_val + tft_val) / 2
                assert abs(row["forecasted_discharge"] - expected_mean) < 0.01

    def test_skill_stats_passed_through_unchanged(self, forecasts_pentad, skill_stats_pentad):
        """Skill stats are passed through unchanged (no EM rows added)."""
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            _, skill_out = self._make_ensemble(forecasts_pentad, skill_stats_pentad)
            pd.testing.assert_frame_equal(
                skill_out.reset_index(drop=True),
                skill_stats_pentad.reset_index(drop=True),
            )

    def test_ne_excluded_from_ensemble(self, forecasts_pentad, skill_stats_pentad):
        """NE (neural ensemble) is excluded from ensemble candidates."""
        # Add NE model to forecasts and skill_stats
        ne_forecast = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_year": [1],
                "pentad_in_month": ["1"],
                "forecasted_discharge": [999.0],
                "model_short": ["NE"],
            }
        )
        ne_skill = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["NE"],
                "sdivsigma": [0.1],
                "nse": [0.99],
                "delta": [5.0],
                "accuracy": [0.99],
                "mae": [0.5],
                "n_pairs": [10],
            }
        )
        forecasts_with_ne = pd.concat([forecasts_pentad, ne_forecast], ignore_index=True)
        skills_with_ne = pd.concat([skill_stats_pentad, ne_skill], ignore_index=True)

        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts_with_ne, skills_with_ne)
            em_rows = joint[joint["model_short"] == "EM"]
            # Ensemble mean should NOT include 999.0 from NE
            for _, row in em_rows.iterrows():
                assert row["forecasted_discharge"] < 900.0

    def test_no_ensemble_when_no_qualified_models(self):
        """No ensemble created when NSE is negative (fails threshold_nse=0)."""
        # nse=-0.1 is below 0 so the model is excluded regardless of env thresholds
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["LR"],
                "sdivsigma": [0.9],
                "nse": [-0.1],
                "delta": [5.0],
                "accuracy": [0.6],
                "mae": [8.0],
                "n_pairs": [10],
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_year": [1],
                "pentad_in_month": ["1"],
                "forecasted_discharge": [100.0],
                "model_short": ["LR"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, skill_out = self._make_ensemble(forecasts, skill_stats)
            assert "EM" not in joint["model_short"].values

    def test_single_model_ensemble_discarded(self):
        """Ensemble with only one model (LR only) is discarded."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["LR"],
                "sdivsigma": [0.3],
                "nse": [0.95],
                "delta": [5.0],
                "accuracy": [0.95],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_year": [1],
                "pentad_in_month": ["1"],
                "forecasted_discharge": [100.0],
                "model_short": ["LR"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts, skill_stats)
            # Single-model "Ens. Mean with LR (EM)" should be discarded
            assert "EM" not in joint["model_short"].values

    def test_decad_ensemble(self):
        """Ensemble works for decadal data too."""
        skill_stats = pd.DataFrame(
            {
                "decad_in_year": [1, 1],
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.95, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.95, 0.88],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
            }
        )
        dates = pd.to_datetime(["2024-01-10", "2024-01-10"])
        forecasts = pd.DataFrame(
            {
                "code": ["10001", "10001"],
                "date": dates,
                "decad_in_year": [1, 1],
                "decad_in_month": ["1", "1"],  # string like tl.get_decad_in_month
                "forecasted_discharge": [100.0, 110.0],
                "q05": [90.0, 99.0],
                "q25": [95.0, 104.5],
                "q75": [105.0, 115.5],
                "q95": [110.0, 121.0],
                "model_short": ["LR", "TFT"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts, skill_stats, period="decad")
            assert "EM" in joint["model_short"].values

    def test_composition_string_format(self, forecasts_pentad, skill_stats_pentad):
        """Composition column contains 'LR, TFT' for ensemble rows."""
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts_pentad, skill_stats_pentad)
            em_rows = joint[joint["model_short"] == "EM"]
            assert not em_rows.empty, "EM rows should exist — LR and TFT both pass thresholds"
            composition = em_rows.iloc[0]["composition"]
            assert "LR" in composition
            assert "TFT" in composition

    def test_single_tft_ensemble_discarded(self):
        """Ensemble with only TFT (single model) is discarded."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["TFT"],
                "sdivsigma": [0.3],
                "nse": [0.95],
                "delta": [5.0],
                "accuracy": [0.95],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_year": [1],
                "pentad_in_month": ["1"],
                "forecasted_discharge": [110.0],
                "model_short": ["TFT"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts, skill_stats)
            assert "EM" not in joint["model_short"].values

    def test_single_tide_ensemble_discarded(self):
        """Ensemble with only TiDE (single model) is discarded."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["TiDE"],
                "sdivsigma": [0.3],
                "nse": [0.95],
                "delta": [5.0],
                "accuracy": [0.95],
                "mae": [2.0],
                "n_pairs": [10],
            }
        )
        forecasts = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_year": [1],
                "pentad_in_month": ["1"],
                "forecasted_discharge": [90.0],
                "model_short": ["TiDE"],
            }
        )
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, _ = self._make_ensemble(forecasts, skill_stats)
            assert "EM" not in joint["model_short"].values

    def test_observed_not_required(self, forecasts_pentad, skill_stats_pentad):
        """Ensemble creation works without observed data (not a param)."""
        with patch.dict(
            os.environ,
            {
                "ieasyhydroforecast_efficiency_threshold": "0.6",
                "ieasyhydroforecast_accuracy_threshold": "0.8",
                "ieasyhydroforecast_nse_threshold": "0.8",
            },
        ):
            joint, skill_out = self._make_ensemble(forecasts_pentad, skill_stats_pentad)
            assert "EM" in joint["model_short"].values
            # skill_stats passed through unchanged
            assert "EM" not in skill_out["model_short"].values


# ---------------------------------------------------------------------------
# Model name consistency tests
# ---------------------------------------------------------------------------


class TestModelNameConsistency:
    """Verify that MODEL_TYPE_MAP in api_writer covers all core types."""

    def test_model_type_map_covers_core_types(self):
        """MODEL_TYPE_MAP covers LR, TFT, TIDE, TSMIXER, EM, NE, RRAM."""
        from src.api_writer import MODEL_TYPE_MAP

        expected = {"LR", "TFT", "TIDE", "TSMIXER", "EM", "NE", "RRAM"}
        assert expected.issubset(set(MODEL_TYPE_MAP.keys()))

    def test_model_type_map_values_are_valid(self):
        """MODEL_TYPE_MAP values are valid API model type strings."""
        from src.api_writer import MODEL_TYPE_MAP

        for key, value in MODEL_TYPE_MAP.items():
            assert isinstance(value, str), f"MODEL_TYPE_MAP[{key!r}] = {value!r} is not a string"


# ---------------------------------------------------------------------------
# Quantile propagation tests (PP-019)
# ---------------------------------------------------------------------------

# Shared environment for ensemble threshold tests
_ENSEMBLE_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
    "ieasyhydroforecast_nse_threshold": "0.8",
}


@pytest.fixture
def skill_stats_three_models():
    """Skill stats where LR, TFT, and TiDE all pass thresholds."""
    return pd.DataFrame(
        {
            "pentad_in_year": [1, 1, 1],
            "code": ["A", "A", "A"],
            "model_short": ["LR", "TFT", "TiDE"],
            "sdivsigma": [0.3, 0.4, 0.5],
            "nse": [0.9, 0.85, 0.82],
            "delta": [5.0, 5.0, 5.0],
            "accuracy": [0.9, 0.85, 0.85],
            "mae": [5.0, 8.0, 10.0],
            "n_pairs": [20, 20, 20],
        }
    )


@pytest.fixture
def forecasts_with_quantiles():
    """Forecasts with LR (no quantiles) + TFT/TiDE (with quantiles)."""
    return pd.DataFrame(
        {
            "pentad_in_year": [1, 1, 1],
            "pentad_in_month": [1, 1, 1],
            "date": pd.Timestamp("2025-01-05"),
            "code": ["A", "A", "A"],
            "model_short": ["LR", "TFT", "TiDE"],
            "forecasted_discharge": [100.0, 110.0, 90.0],
            "q05": [np.nan, 80.0, 60.0],
            "q25": [np.nan, 95.0, 75.0],
            "q75": [np.nan, 125.0, 105.0],
            "q95": [np.nan, 140.0, 120.0],
        }
    )


class TestEnsembleQuantilePropagation:
    """PP-019: Verify quantiles propagate through short-term ensemble creation."""

    def _make_ensemble(self, forecasts, skill_stats):
        return create_ensemble_forecasts(
            forecasts=forecasts,
            skill_stats=skill_stats,
            period_col="pentad_in_year",
            period_in_month_col="pentad_in_month",
            get_period_in_month_func=tl.get_pentad,
        )

    def test_ensemble_propagates_quantiles(
        self, forecasts_with_quantiles, skill_stats_three_models
    ):
        """EM ensemble rows have averaged q05/q25/q75/q95 from ML models."""
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        em_rows = joint[joint["model_short"] == "EM"]
        assert not em_rows.empty

        for qcol in ("q05", "q25", "q75", "q95"):
            assert qcol in em_rows.columns
            assert em_rows[qcol].notna().all(), f"EM {qcol} should not be NaN"

    def test_ensemble_quantiles_numerical_verification(
        self, forecasts_with_quantiles, skill_stats_three_models
    ):
        """q05/q95 come from ML models only; q25/q75 also include LR's point -/+ delta.

        LR point=100, delta=5 -> LR q25=95, q75=105.
        """
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        em = joint[joint["model_short"] == "EM"].iloc[0]
        # TFT q05=80, TiDE q05=60 → mean=70
        assert em["q05"] == pytest.approx(70.0)
        # LR q25=95, TFT q25=95, TiDE q25=75 → mean=88.33
        assert em["q25"] == pytest.approx(265.0 / 3)
        # LR q75=105, TFT q75=125, TiDE q75=105 → mean=111.67
        assert em["q75"] == pytest.approx(335.0 / 3)
        # TFT q95=140, TiDE q95=120 → mean=130
        assert em["q95"] == pytest.approx(130.0)

    def test_ensemble_quantile_model_mix_asymmetry(
        self, forecasts_with_quantiles, skill_stats_three_models
    ):
        """LR contributes point and q25/q75 (point -/+ delta) but no q05/q95."""
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        em = joint[joint["model_short"] == "EM"].iloc[0]
        # Point forecast: mean(LR=100, TFT=110, TiDE=90) = 100.0
        assert em["forecasted_discharge"] == pytest.approx(100.0)
        # q25: mean(LR=100-5, TFT=95, TiDE=75)
        assert em["q25"] == pytest.approx(265.0 / 3)
        # q05 has no LR contribution: mean(TFT=80, TiDE=60)
        assert em["q05"] == pytest.approx(70.0)
        assert "LR" in em["composition"]

    def test_lr_contributes_point_and_delta_band_not_q05_q95(self):
        """LR contributes point and q25/q75 = point -/+ delta; q05/q95 from ML only."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1, 1],
                "code": ["A", "A", "A"],
                "model_short": ["LR", "TFT", "TiDE"],
                "sdivsigma": [0.3, 0.4, 0.5],
                "nse": [0.9, 0.85, 0.82],
                "delta": [5.0, 5.0, 5.0],
                "accuracy": [0.9, 0.85, 0.85],
                "mae": [5.0, 8.0, 10.0],
                "n_pairs": [20, 20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1, 1],
                "pentad_in_month": [1, 1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A", "A"],
                "model_short": ["LR", "TFT", "TiDE"],
                "forecasted_discharge": [100.0, 110.0, 90.0],
                "q05": [np.nan, 80.0, 60.0],
                "q25": [np.nan, 95.0, 75.0],
                "q75": [np.nan, 125.0, 105.0],
                "q95": [np.nan, 140.0, 120.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts, skill_stats)

        em = joint[joint["model_short"] == "EM"].iloc[0]
        # LR included in point mean: mean(100, 110, 90) = 100
        assert em["forecasted_discharge"] == pytest.approx(100.0)
        # LR delta=5: q25 = mean(95, 95, 75), q75 = mean(105, 125, 105)
        assert em["q25"] == pytest.approx((95.0 + 95.0 + 75.0) / 3)
        assert em["q75"] == pytest.approx((105.0 + 125.0 + 105.0) / 3)
        # q05/q95: ML models only
        assert em["q05"] == pytest.approx(70.0)
        assert em["q95"] == pytest.approx(130.0)
        assert "LR" in em["composition"]

    def test_ensemble_no_quantiles_backward_compatible(self):
        """No EM when input has no quantile columns (MR-H: no central-only exception)."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "pentad_in_month": [1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [100.0, 110.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts, skill_stats)

        em_rows = joint[joint["model_short"] == "EM"]
        # No q-cols present → no eligible contributors → no EM (MR-H)
        assert em_rows.empty, "EM must not be created when quantile columns are entirely absent"

    def test_joint_output_preserves_individual_quantiles(
        self, forecasts_with_quantiles, skill_stats_three_models
    ):
        """Individual ML model rows retain their original quantiles in joint output."""
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        tft = joint[joint["model_short"] == "TFT"].iloc[0]
        assert tft["q25"] == pytest.approx(95.0)
        assert tft["q75"] == pytest.approx(125.0)

        tide = joint[joint["model_short"] == "TiDE"].iloc[0]
        assert tide["q05"] == pytest.approx(60.0)
        assert tide["q95"] == pytest.approx(120.0)

    def test_lr_rows_have_nan_quantiles(self, forecasts_with_quantiles, skill_stats_three_models):
        """LR model rows retain NaN quantiles (unchanged)."""
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        lr = joint[joint["model_short"] == "LR"].iloc[0]
        for qcol in ("q05", "q25", "q75", "q95"):
            assert pd.isna(lr[qcol]), f"LR {qcol} should be NaN"

    def test_ensemble_missing_whole_quantile_column_produces_no_em(self):
        """If any of q05/q25/q75/q95 is missing as a column, no EM is produced (MR-H)."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [20, 20],
            }
        )
        # q75 is absent — only q05/q25/q95 present
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "pentad_in_month": [1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [100.0, 110.0],
                "q05": [80.0, 85.0],
                "q25": [90.0, 95.0],
                "q95": [120.0, 130.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts, skill_stats)

        em_rows = joint[joint["model_short"] == "EM"]
        assert em_rows.empty, "EM must not be created when q75 column is entirely absent"

    def test_ensemble_all_null_quantiles_produces_no_em(self):
        """All q-col values are NaN for every row → no EM (MR-H)."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "pentad_in_month": [1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [100.0, 110.0],
                "q05": [np.nan, np.nan],
                "q25": [np.nan, np.nan],
                "q75": [np.nan, np.nan],
                "q95": [np.nan, np.nan],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts, skill_stats)

        em_rows = joint[joint["model_short"] == "EM"]
        assert em_rows.empty, "EM must not be created when all q-col values are NaN"

    def test_ensemble_inf_central_excluded(self):
        """A row with inf forecasted_discharge is excluded from EM (MR-H)."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "pentad_in_month": [1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [np.inf, 110.0],
                "q05": [80.0, 85.0],
                "q25": [90.0, 95.0],
                "q75": [110.0, 120.0],
                "q95": [120.0, 130.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts, skill_stats)

        # LR excluded (inf central) → only TFT → single model → no EM
        em_rows = joint[joint["model_short"] == "EM"]
        assert em_rows.empty, "Single-model ensemble after inf exclusion must be discarded"


# ---------------------------------------------------------------------------
# PP-027: EM skip diagnostic logging tests
# ---------------------------------------------------------------------------


class TestEMSkipDiagnosticLogging:
    """PP-027: Verify log messages when EM ensemble is skipped."""

    def _make_ensemble(self, forecasts, skill_stats, period="pentad"):
        if period == "pentad":
            return create_ensemble_forecasts(
                forecasts=forecasts,
                skill_stats=skill_stats,
                period_col="pentad_in_year",
                period_in_month_col="pentad_in_month",
                get_period_in_month_func=tl.get_pentad,
            )
        return create_ensemble_forecasts(
            forecasts=forecasts,
            skill_stats=skill_stats,
            period_col="decad_in_year",
            period_in_month_col="decad_in_month",
            get_period_in_month_func=tl.get_decad_in_month,
        )

    def test_gate_a_logs_codes_with_no_skilled_forecasts(self, caplog):
        """Codes with no matching skill metrics are logged at Gate A."""
        # Station A has skill metrics; station B does not
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["A", "A"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.9],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1, 1],
                "pentad_in_month": [1, 1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A", "B"],  # B has no skill metrics
                "model_short": ["LR", "TFT", "TFT"],
                "forecasted_discharge": [100.0, 110.0, 90.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            with caplog.at_level(logging.INFO, logger="src.ensemble_calculator"):
                self._make_ensemble(forecasts, skill_stats)
        assert any("no skilled forecasts" in msg and "B" in msg for msg in caplog.messages)

    def test_gate_c_logs_single_model_codes(self, caplog):
        """Codes with only 1 qualifying model are logged at Gate C."""
        # Station A: 2 models pass → EM created
        # Station B: only 1 model passes → EM dropped, logged
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1, 1],
                "code": ["A", "A", "B"],
                "model_short": ["LR", "TFT", "LR"],
                "sdivsigma": [0.3, 0.4, 0.3],
                "nse": [0.9, 0.9, 0.9],
                "delta": [5.0, 5.0, 5.0],
                "accuracy": [0.9, 0.9, 0.9],
                "mae": [5.0, 5.0, 5.0],
                "n_pairs": [20, 20, 20],
            }
        )
        forecasts = pd.DataFrame(
            {
                "pentad_in_year": [1, 1, 1],
                "pentad_in_month": [1, 1, 1],
                "date": pd.Timestamp("2025-01-05"),
                "code": ["A", "A", "B"],
                "model_short": ["LR", "TFT", "LR"],
                "forecasted_discharge": [100.0, 110.0, 90.0],
                "q05": [90.0, 99.0, 81.0],
                "q25": [95.0, 104.5, 85.5],
                "q75": [105.0, 115.5, 94.5],
                "q95": [110.0, 121.0, 99.0],
            }
        )
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            with caplog.at_level(logging.INFO, logger="src.ensemble_calculator"):
                joint, _ = self._make_ensemble(forecasts, skill_stats)
        # A should have EM, B should not
        em_codes = set(joint[joint["model_short"] == "EM"]["code"])
        assert "A" in em_codes
        assert "B" not in em_codes
        # Gate C log should mention B
        assert any("1 qualifying model" in msg and "B" in msg for msg in caplog.messages)

    def test_no_diagnostic_logs_when_all_stations_produce_em(
        self, forecasts_pentad, skill_stats_pentad, caplog
    ):
        """No skip diagnostics when all stations produce EM successfully."""
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            with caplog.at_level(logging.INFO, logger="src.ensemble_calculator"):
                self._make_ensemble(forecasts_pentad, skill_stats_pentad)
        skip_messages = [
            msg
            for msg in caplog.messages
            if "no skilled forecasts" in msg
            or "only NE model" in msg
            or "1 qualifying model" in msg
        ]
        assert skip_messages == [], f"Unexpected skip logs: {skip_messages}"


# ---------------------------------------------------------------------------
# TestEMMinimalMaintenance — NSE>=0 gate boundary + LR q50/all-quantile masking
# ---------------------------------------------------------------------------


def _make_em_h(forecasts, skill_stats, horizon):
    """Test-only factory: call create_ensemble_forecasts for the given horizon."""
    if horizon == "pentad":
        return create_ensemble_forecasts(
            forecasts=forecasts,
            skill_stats=skill_stats,
            period_col="pentad_in_year",
            period_in_month_col="pentad_in_month",
            get_period_in_month_func=tl.get_pentad,
        )
    return create_ensemble_forecasts(
        forecasts=forecasts,
        skill_stats=skill_stats,
        period_col="decad_in_year",
        period_in_month_col="decad_in_month",
        get_period_in_month_func=tl.get_decad_in_month,
    )


def _base_skill(horizon, models, nse_vals, **overrides):
    """Test-only factory: minimal skill_stats frame."""
    pcol = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
    n = len(models)
    return pd.DataFrame(
        {
            pcol: [1] * n,
            "code": ["A"] * n,
            "model_short": models,
            "sdivsigma": overrides.get("sdivsigma", [999] * n),
            "nse": nse_vals,
            "delta": [5.0] * n,
            "accuracy": overrides.get("accuracy", [-99] * n),
            "mae": [5.0] * n,
            "n_pairs": overrides.get("n_pairs", [10] * n),
        }
    )


def _base_forecast(horizon, models, discharges, **qcols):
    """Test-only factory: minimal forecast frame."""
    pcol = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
    pmc = "pentad_in_month" if horizon == "pentad" else "decad_in_month"
    n = len(models)
    base = {
        pcol: [1] * n,
        pmc: [1] * n,
        "date": [pd.Timestamp("2025-01-05")] * n,
        "code": ["A"] * n,
        "model_short": models,
        "forecasted_discharge": discharges,
    }
    base.update(qcols)
    return pd.DataFrame(base)


class TestEMMinimalMaintenance:
    """NSE>=0 gate boundary (fix) and LR q-col masking (incl. q50) patch."""

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    @pytest.mark.parametrize(
        "tft_nse,expect_em",
        [
            (0.0, False),  # nse==0 excluded: gate is NSE > 0
            (0.4, True),  # nse>0 passes
            (-0.1, False),  # nse<0 excluded
            (np.nan, False),  # non-finite excluded
            (np.inf, False),  # non-finite excluded
            (-np.inf, False),  # non-finite excluded
        ],
    )
    def test_nse_gate_boundary(self, horizon, tft_nse, expect_em):
        """NSE>0 passes; <=0 or non-finite is excluded; sdivsigma/accuracy gates disabled."""
        skill = _base_skill(horizon, ["LR", "TFT"], [1.0, tft_nse])
        fc = _base_forecast(
            horizon,
            ["LR", "TFT"],
            [100.0, 20.0],
            q05=[np.nan, 10.0],
            q25=[np.nan, 15.0],
            q75=[np.nan, 25.0],
            q95=[np.nan, 30.0],
        )
        joint, skill_out = _make_em_h(fc, skill, horizon)
        em = joint[joint["model_short"] == "EM"]
        if expect_em:
            assert not em.empty, f"Expected EM for TFT nse={tft_nse}"
            assert em.iloc[0]["forecasted_discharge"] == pytest.approx(60.0)
        else:
            assert em.empty, f"Expected no EM for TFT nse={tft_nse}"
        pd.testing.assert_frame_equal(
            skill_out.reset_index(drop=True), skill.reset_index(drop=True)
        )

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    def test_lr_input_quantiles_replaced_by_delta_band(self, horizon):
        """LR's own (absurd) quantile values are ignored; LR gets q50=point, q25/q75=point-/+delta.

        Hand-computed: LR point 10 delta 5, TFT 20, TiDE 30.
        """
        skill = _base_skill(horizon, ["LR", "TFT", "TiDE"], [1.0, 0.9, 0.85])
        fc = _base_forecast(
            horizon,
            ["LR", "TFT", "TiDE"],
            [10.0, 20.0, 30.0],
            q05=[10000.0, 10.0, 15.0],
            q25=[10000.0, 15.0, 20.0],
            q50=[10000.0, 20.0, 30.0],
            q75=[10000.0, 25.0, 35.0],
            q95=[10000.0, 30.0, 40.0],
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        em = joint[joint["model_short"] == "EM"]
        assert not em.empty
        em0 = em.iloc[0]
        assert em0["forecasted_discharge"] == pytest.approx(20.0)  # (10+20+30)/3
        assert em0["q50"] == pytest.approx(20.0)  # (10+20+30)/3; LR q50 = point
        assert em0["q05"] == pytest.approx(12.5)  # (10+15)/2; no LR
        assert em0["q25"] == pytest.approx(40.0 / 3)  # (5+15+20)/3
        assert em0["q75"] == pytest.approx(25.0)  # (15+25+35)/3
        assert em0["q95"] == pytest.approx(35.0)  # (30+40)/2; no LR
        assert em0["q05"] <= em0["q25"] <= em0["q50"] <= em0["q75"] <= em0["q95"]
        # Input LR and TFT rows in joint keep their original q50
        assert joint[joint["model_short"] == "LR"].iloc[0]["q50"] == pytest.approx(10000.0)
        assert joint[joint["model_short"] == "TFT"].iloc[0]["q50"] == pytest.approx(20.0)

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    def test_lr_q50_ml_only_control(self, horizon):
        """Control: without LR, ML-only gives same quantiles; point = mean(TFT, TiDE)."""
        skill = _base_skill(horizon, ["TFT", "TiDE"], [0.9, 0.85])
        fc = _base_forecast(
            horizon,
            ["TFT", "TiDE"],
            [20.0, 30.0],
            q05=[10.0, 15.0],
            q25=[15.0, 20.0],
            q50=[20.0, 30.0],
            q75=[25.0, 35.0],
            q95=[30.0, 40.0],
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        em = joint[joint["model_short"] == "EM"]
        assert not em.empty
        em0 = em.iloc[0]
        assert em0["forecasted_discharge"] == pytest.approx(25.0)  # (20+30)/2
        assert em0["q50"] == pytest.approx(25.0)
        assert em0["q05"] == pytest.approx(12.5)
        assert em0["q25"] == pytest.approx(17.5)
        assert em0["q75"] == pytest.approx(30.0)
        assert em0["q95"] == pytest.approx(35.0)

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    @pytest.mark.parametrize("bad_val", [np.nan, np.inf])
    def test_bad_non_lr_quantile_excludes_whole_contributor(self, horizon, bad_val):
        """Non-LR with any non-finite q-band is dropped; LR+TFT still form EM."""
        skill = _base_skill(horizon, ["LR", "TFT", "TiDE"], [1.0, 0.9, 0.85])
        fc = _base_forecast(
            horizon,
            ["LR", "TFT", "TiDE"],
            [100.0, 110.0, 90.0],
            q05=[np.nan, 80.0, bad_val],
            q25=[np.nan, 95.0, 75.0],
            q75=[np.nan, 125.0, 105.0],
            q95=[np.nan, 140.0, 120.0],
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        em = joint[joint["model_short"] == "EM"]
        assert not em.empty, "LR+TFT should still form EM after TiDE excluded"
        em0 = em.iloc[0]
        assert em0["forecasted_discharge"] == pytest.approx(105.0)  # (100+110)/2
        assert em0["q05"] == pytest.approx(80.0)
        assert em0["q95"] == pytest.approx(140.0)
        assert "TiDE" not in em0["composition"]
        assert "LR" in em0["composition"]
        assert "TFT" in em0["composition"]

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    def test_lr_only_no_em(self, horizon):
        """All-LR input (single model) → no EM produced."""
        skill = _base_skill(horizon, ["LR"], [0.9])
        fc = _base_forecast(
            horizon,
            ["LR"],
            [100.0],
            q05=[np.nan],
            q25=[np.nan],
            q75=[np.nan],
            q95=[np.nan],
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        assert "EM" not in joint["model_short"].values

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    def test_tft_only_no_em(self, horizon):
        """Single TFT only (no LR) → no EM produced."""
        skill = _base_skill(horizon, ["TFT"], [0.9])
        fc = _base_forecast(
            horizon,
            ["TFT"],
            [110.0],
            q05=[80.0],
            q25=[95.0],
            q75=[125.0],
            q95=[140.0],
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        assert "EM" not in joint["model_short"].values

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    @pytest.mark.parametrize("dtype", ["object", "Float64"])
    @pytest.mark.parametrize("bad_band", ["q05", "q25", "q75", "q95"])
    def test_nullable_non_lr_band_excludes_contributor(self, horizon, bad_band, dtype):
        """pd.NA in one TiDE q-band (any dtype) excludes TiDE; LR+TFT still form EM.

        Models: LR (discharge=10), TFT (discharge=20), TiDE (discharge=30).
        LR (delta=5) contributes q25/q75 = point -/+ 5 but no q05/q95.
        TFT bands: q05=18, q25=19, q75=21, q95=22 (all finite).
        TiDE bands: q05=28, q25=29, q75=31, q95=32 except bad_band → pd.NA.
        Expected EM: point=(10+20)/2=15, bands from TFT only, composition "LR, TFT".
        """
        # TFT quantile values (finite, become sole quantile source for EM)
        q_tft = {"q05": 18.0, "q25": 19.0, "q75": 21.0, "q95": 22.0}
        q_tide = {"q05": 28.0, "q25": 29.0, "q75": 31.0, "q95": 32.0}

        fc = _base_forecast(
            horizon,
            ["LR", "TFT", "TiDE"],
            [10.0, 20.0, 30.0],
            q05=[np.nan, q_tft["q05"], q_tide["q05"]],
            q25=[np.nan, q_tft["q25"], q_tide["q25"]],
            q75=[np.nan, q_tft["q75"], q_tide["q75"]],
            q95=[np.nan, q_tft["q95"], q_tide["q95"]],
        )
        # Cast all four band columns to the chosen dtype, then inject pd.NA into TiDE's bad_band
        for col in ("q05", "q25", "q75", "q95"):
            fc[col] = fc[col].astype(dtype)
        tide_idx = fc[fc["model_short"] == "TiDE"].index[0]
        fc.at[tide_idx, bad_band] = pd.NA

        skill = _base_skill(horizon, ["LR", "TFT", "TiDE"], [1.0, 0.9, 0.85])

        # Deep-copy before passing to calculator so mutation of inputs can be detected
        fc_orig = fc.copy(deep=True)
        skill_orig = skill.copy(deep=True)

        joint, skill_out = _make_em_h(fc, skill, horizon)

        # Input frames must not be mutated by the calculator
        pd.testing.assert_frame_equal(
            fc.reset_index(drop=True), fc_orig.reset_index(drop=True), check_dtype=False
        )
        pd.testing.assert_frame_equal(
            skill.reset_index(drop=True), skill_orig.reset_index(drop=True)
        )

        # Exactly one EM row
        em_rows = joint[joint["model_short"] == "EM"]
        assert len(em_rows) == 1, (
            f"Expected exactly 1 EM row for horizon={horizon}, bad_band={bad_band}, "
            f"dtype={dtype}; got {len(em_rows)}"
        )
        em0 = em_rows.iloc[0]

        # Composition: LR and TFT included; TiDE excluded (bad band)
        assert "LR" in em0["composition"], "LR must appear in EM composition"
        assert "TFT" in em0["composition"], "TFT must appear in EM composition"
        assert "TiDE" not in em0["composition"], "TiDE must be excluded from EM composition"

        # Point mean = (LR=10 + TFT=20) / 2 = 15
        assert em0["forecasted_discharge"] == pytest.approx(15.0), (
            f"Expected point=15.0, got {em0['forecasted_discharge']}"
        )

        # q05/q95 from TFT only (TiDE excluded, LR has none); q25/q75 average LR delta band
        # Raw means: q05=18, q25=(5+19)/2=12, q75=(15+21)/2=18, q95=22; the
        # monotonicity guard (running max) lifts q25 to 18 -> 18, 18, 18, 22.
        # NOTE: q05/q95 come from non-LR members only while q25/q75 average in LR.
        # When LR's delta band lies below the non-LR q05, enforce_quantile_monotonicity
        # lifts q25 to q05 -- this is the guard acting, not a statistical expectation.
        assert em0["q05"] == pytest.approx(18.0)
        assert em0["q25"] == pytest.approx(18.0)
        assert em0["q75"] == pytest.approx(18.0)
        assert em0["q95"] == pytest.approx(q_tft["q95"]), "q95 must equal TFT's q95=22"

        # Skill output is passed through unchanged
        pd.testing.assert_frame_equal(
            skill_out.reset_index(drop=True), skill_orig.reset_index(drop=True)
        )

        # All original model rows in joint output retain their original values
        # (dtype promotion is allowed; compare only the original forecast columns)
        orig_cols = fc_orig.columns.tolist()
        for model in ["LR", "TFT", "TiDE"]:
            orig_row = fc_orig[fc_orig["model_short"] == model].reset_index(drop=True)
            out_row = joint[joint["model_short"] == model].reset_index(drop=True)
            pd.testing.assert_frame_equal(
                out_row[orig_cols],
                orig_row[orig_cols],
                check_dtype=False,
            )

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    @pytest.mark.parametrize("orphan_kind", ["direct_lr_only", "excluded_non_lr"])
    def test_nullable_lr_only_group_does_not_block_valid_group(self, horizon, orphan_kind):
        """pd.NA q-means in all-LR ORPHAN group must not crash or block GOOD's EM.

        GOOD: LR(10)+TFT(20), bands 18/19/21/22 → EM point=15, bands from TFT.
        ORPHAN direct_lr_only: LR only → q-means pd.NA → no EM, no crash.
        ORPHAN excluded_non_lr: LR+TiDE, TiDE q05=pd.NA → TiDE excluded → LR-only → no EM.
        """
        pcol = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
        pmc = "pentad_in_month" if horizon == "pentad" else "decad_in_month"
        q_tft = {"q05": 18.0, "q25": 19.0, "q75": 21.0, "q95": 22.0}

        good_df = pd.DataFrame(
            {
                pcol: [1, 1],
                pmc: [1, 1],
                "date": [pd.Timestamp("2025-01-05")] * 2,
                "code": ["GOOD", "GOOD"],
                "model_short": ["LR", "TFT"],
                "forecasted_discharge": [10.0, 20.0],
                "q05": [np.nan, q_tft["q05"]],
                "q25": [np.nan, q_tft["q25"]],
                "q75": [np.nan, q_tft["q75"]],
                "q95": [np.nan, q_tft["q95"]],
            }
        )

        if orphan_kind == "direct_lr_only":
            orphan_df = pd.DataFrame(
                {
                    pcol: [1],
                    pmc: [1],
                    "date": [pd.Timestamp("2025-01-05")],
                    "code": ["ORPHAN"],
                    "model_short": ["LR"],
                    "forecasted_discharge": [99.0],
                    "q05": [np.nan],
                    "q25": [np.nan],
                    "q75": [np.nan],
                    "q95": [np.nan],
                }
            )
            orphan_skill_codes, orphan_skill_models, orphan_nse = ["ORPHAN"], ["LR"], [1.0]
        else:  # LR + TiDE where TiDE's q05=pd.NA → TiDE excluded → LR-only
            orphan_df = pd.DataFrame(
                {
                    pcol: [1, 1],
                    pmc: [1, 1],
                    "date": [pd.Timestamp("2025-01-05")] * 2,
                    "code": ["ORPHAN", "ORPHAN"],
                    "model_short": ["LR", "TiDE"],
                    "forecasted_discharge": [99.0, 50.0],
                    "q05": [np.nan, np.nan],
                    "q25": [np.nan, 29.0],
                    "q75": [np.nan, 31.0],
                    "q95": [np.nan, 32.0],
                }
            )
            orphan_skill_codes = ["ORPHAN", "ORPHAN"]
            orphan_skill_models = ["LR", "TiDE"]
            orphan_nse = [1.0, 0.85]

        fc = pd.concat([good_df, orphan_df], ignore_index=True)
        for col in ("q05", "q25", "q75", "q95"):
            fc[col] = fc[col].astype("Float64")

        n_skill = 2 + len(orphan_skill_models)
        skill = pd.DataFrame(
            {
                pcol: [1] * n_skill,
                "code": ["GOOD", "GOOD"] + orphan_skill_codes,
                "model_short": ["LR", "TFT"] + orphan_skill_models,
                "sdivsigma": [999] * n_skill,
                "nse": [1.0, 0.9] + orphan_nse,
                "delta": [5.0] * n_skill,
                "accuracy": [-99] * n_skill,
                "mae": [5.0] * n_skill,
                "n_pairs": [10] * n_skill,
            }
        )

        fc_orig = fc.copy(deep=True)
        skill_orig = skill.copy(deep=True)

        joint, skill_out = _make_em_h(fc, skill, horizon)

        # Skill passthrough unchanged
        pd.testing.assert_frame_equal(
            skill_out.reset_index(drop=True), skill_orig.reset_index(drop=True)
        )

        # Exactly one EM row, for GOOD only
        em_rows = joint[joint["model_short"] == "EM"]
        assert len(em_rows) == 1, f"Expected 1 EM (GOOD only), got {len(em_rows)}"
        em0 = em_rows.iloc[0]
        assert em0["code"] == "GOOD"

        # Correct point, composition, and all 4 bands
        assert em0["forecasted_discharge"] == pytest.approx(15.0)
        assert "LR" in em0["composition"]
        assert "TFT" in em0["composition"]
        # Raw: q05=18 (TFT only), q25=(5+19)/2=12 (LR 10-5), q75=(15+21)/2=18,
        # q95=22; monotonicity guard (running max) lifts q25 to 18 -> 18, 18, 18, 22.
        # NOTE: q05/q95 come from non-LR members only while q25/q75 average in LR.
        # When LR's delta band lies below the non-LR q05, enforce_quantile_monotonicity
        # lifts q25 to q05 -- this is the guard acting, not a statistical expectation.
        assert em0["q05"] == pytest.approx(18.0)
        assert em0["q25"] == pytest.approx(18.0)
        assert em0["q75"] == pytest.approx(18.0)
        assert em0["q95"] == pytest.approx(q_tft["q95"])

        # Input not mutated; allow dtype promotion in output
        pd.testing.assert_frame_equal(
            fc.reset_index(drop=True), fc_orig.reset_index(drop=True), check_dtype=False
        )

        # All original rows (GOOD LR/TFT + ORPHAN) preserved in joint output
        orig_cols = fc_orig.columns.tolist()
        pd.testing.assert_frame_equal(
            joint[joint["model_short"] != "EM"][orig_cols].reset_index(drop=True),
            fc_orig[orig_cols].reset_index(drop=True),
            check_dtype=False,
        )

    @pytest.mark.parametrize("horizon", ["pentad", "decad"])
    def test_ne_and_em_aliases_excluded_from_composition(self, horizon):
        """NE, EM, ENSEMBLE_MEAN in input do not become EM constituents.

        Existing EM rows in input forecasts are preserved (legacy contract).
        Generated EM composition must not include NE/EM/ENSEMBLE_MEAN.
        """
        pcol = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
        pmc = "pentad_in_month" if horizon == "pentad" else "decad_in_month"
        all_models = ["LR", "TFT", "NE", "EM", "ENSEMBLE_MEAN"]
        skill = _base_skill(horizon, all_models, [1.0, 0.9, 0.95, 0.99, 0.99])
        fc = pd.DataFrame(
            {
                pcol: [1] * 5,
                pmc: [1] * 5,
                "date": [pd.Timestamp("2025-01-05")] * 5,
                "code": ["A"] * 5,
                "model_short": all_models,
                "forecasted_discharge": [100.0, 110.0, 9999.0, 9999.0, 9999.0],
                "q05": [np.nan, 80.0, 1000.0, 1000.0, 1000.0],
                "q25": [np.nan, 95.0, 1000.0, 1000.0, 1000.0],
                "q75": [np.nan, 125.0, 1000.0, 1000.0, 1000.0],
                "q95": [np.nan, 140.0, 1000.0, 1000.0, 1000.0],
            }
        )
        joint, _ = _make_em_h(fc, skill, horizon)
        # Find generated EM (not the input EM rows with discharge=9999)
        em_rows = joint[joint["model_short"] == "EM"]
        assert not em_rows.empty
        new_em = em_rows[em_rows["forecasted_discharge"] < 200.0]
        assert not new_em.empty, "Generated EM with LR+TFT point should be ~105"
        em0 = new_em.iloc[0]
        assert em0["forecasted_discharge"] == pytest.approx(105.0)  # (100+110)/2
        assert "NE" not in em0["composition"]
        assert "EM" not in em0["composition"]
        assert "ENSEMBLE_MEAN" not in em0["composition"]
        assert "LR" in em0["composition"]
        assert "TFT" in em0["composition"]
