"""Tests for src/ensemble_calculator.py — ensemble creation from skill metrics."""

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
    return pd.DataFrame(
        {
            "code": ["10001"] * 4,
            "date": dates,
            "pentad_in_year": [1, 1, 2, 2],
            "pentad_in_month": ["1", "1", "2", "2"],  # string like tl.get_pentad
            "forecasted_discharge": [100.0, 110.0, 120.0, 130.0],
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
        """No ensemble created when no models pass thresholds."""
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["LR"],
                "sdivsigma": [0.9],
                "nse": [0.5],
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
        """EM quantiles are the mean of qualifying ML models' quantiles.

        LR has NaN quantiles, so only TFT and TiDE contribute.
        """
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        em = joint[joint["model_short"] == "EM"].iloc[0]
        # TFT q05=80, TiDE q05=60 → mean=70
        assert em["q05"] == pytest.approx(70.0)
        # TFT q25=95, TiDE q25=75 → mean=85
        assert em["q25"] == pytest.approx(85.0)
        # TFT q75=125, TiDE q75=105 → mean=115
        assert em["q75"] == pytest.approx(115.0)
        # TFT q95=140, TiDE q95=120 → mean=130
        assert em["q95"] == pytest.approx(130.0)

    def test_ensemble_quantile_model_mix_asymmetry(
        self, forecasts_with_quantiles, skill_stats_three_models
    ):
        """EM point forecast uses all 3 models; EM quantiles use only 2 ML models.

        This asymmetry is expected: LR contributes to the point forecast
        but not to quantiles (it has NaN quantiles).
        """
        with patch.dict(os.environ, _ENSEMBLE_ENV):
            joint, _ = self._make_ensemble(forecasts_with_quantiles, skill_stats_three_models)

        em = joint[joint["model_short"] == "EM"].iloc[0]
        # Point forecast: mean(100, 110, 90) = 100.0 (all 3 models)
        assert em["forecasted_discharge"] == pytest.approx(100.0)
        # q25: mean(95, 75) = 85.0 (only TFT + TiDE)
        assert em["q25"] == pytest.approx(85.0)

    def test_ensemble_no_quantiles_backward_compatible(self):
        """Ensemble creation works when input has no quantile columns."""
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
        assert not em_rows.empty
        assert em_rows["forecasted_discharge"].iloc[0] == pytest.approx(105.0)

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
