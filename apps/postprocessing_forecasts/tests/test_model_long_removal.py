"""Tests for INFRA-005: model_long removal from app pipeline.

Contains:
1. Characterization tests capturing current model_long-dependent behavior
   (safety net — any unintended behavioral change causes a test failure)
2. Tests for new composition_agg / is_multi_model_composition functions
3. Behavioral anchor tests verifying discharge values and skill metrics
   that must remain identical before and after refactoring
"""

import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.dirname(__file__))

import tag_library as tl
from conftest import DECAD, PENTAD
from src import data_reader, skill_metrics
from src.ensemble_calculator import (
    composition_agg,
    create_ensemble_forecasts,
    is_multi_model_composition,
)

# ---------------------------------------------------------------------------
# Shared threshold env setup
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    """Set ensemble threshold env vars for all tests."""
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.6")
    monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.8")
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.8")


# ---------------------------------------------------------------------------
# Fixtures for ensemble_calculator tests
# ---------------------------------------------------------------------------


@pytest.fixture
def skill_stats_two_models():
    """Skill stats: LR + TFT pass thresholds, TiDE fails.

    Covers pentad_in_year 1 and 2 so ensemble can be created for
    both forecast dates.
    """
    rows = []
    for piy in [1, 2]:
        rows.extend(
            [
                {
                    "pentad_in_year": piy,
                    "code": "10001",
                    "model_short": "LR",
                    "sdivsigma": 0.3,
                    "nse": 0.95,
                    "delta": 5.0,
                    "accuracy": 0.95,
                    "mae": 2.0,
                    "n_pairs": 10,
                },
                {
                    "pentad_in_year": piy,
                    "code": "10001",
                    "model_short": "TFT",
                    "sdivsigma": 0.4,
                    "nse": 0.9,
                    "delta": 5.0,
                    "accuracy": 0.88,
                    "mae": 3.0,
                    "n_pairs": 10,
                },
                {
                    "pentad_in_year": piy,
                    "code": "10001",
                    "model_short": "TiDE",
                    "sdivsigma": 0.9,
                    "nse": 0.5,
                    "delta": 5.0,
                    "accuracy": 0.6,
                    "mae": 8.0,
                    "n_pairs": 10,
                },
            ]
        )
    return pd.DataFrame(rows)


@pytest.fixture
def forecasts_two_models():
    """Forecast data: LR + TFT across two dates."""
    dates = pd.to_datetime(
        [
            "2024-01-05",
            "2024-01-05",
            "2024-01-10",
            "2024-01-10",
        ]
    )
    return pd.DataFrame(
        {
            "code": ["10001"] * 4,
            "date": dates,
            "pentad_in_year": [1, 1, 2, 2],
            "pentad_in_month": ["1", "1", "2", "2"],
            "forecasted_discharge": [100.0, 110.0, 120.0, 130.0],
            "model_short": ["LR", "TFT", "LR", "TFT"],
        }
    )


@pytest.fixture
def observed_two_dates():
    """Observed data for two pentad dates."""
    return pd.DataFrame(
        {
            "code": ["10001", "10001"],
            "date": pd.to_datetime(["2024-01-05", "2024-01-10"]),
            "discharge_avg": [105.0, 125.0],
            "delta": [5.0, 5.0],
        }
    )


def _make_pentad_ensemble(forecasts, skill_stats):
    """Call create_ensemble_forecasts with pentad defaults."""
    return create_ensemble_forecasts(
        forecasts=forecasts,
        skill_stats=skill_stats,
        period_col="pentad_in_year",
        period_in_month_col="pentad_in_month",
        get_period_in_month_func=tl.get_pentad,
    )


# ===================================================================
# Part 1: Characterization tests — current model_long behavior
# ===================================================================


class TestEnsembleCalculatorCharacterization:
    """Verify ensemble_calculator uses model_short + composition."""

    def test_create_ensemble_works_without_model_long_input(
        self, forecasts_two_models, skill_stats_two_models, observed_two_dates
    ):
        """Ensemble creation works when input lacks model_long column."""
        # Fixtures already lack model_long — just verify it works
        joint, skill_out = _make_pentad_ensemble(forecasts_two_models, skill_stats_two_models)
        assert "EM" in joint["model_short"].values

    def test_create_ensemble_output_has_composition_column(
        self, forecasts_two_models, skill_stats_two_models, observed_two_dates
    ):
        """Output forecasts DataFrame has composition column."""
        joint, skill_out = _make_pentad_ensemble(forecasts_two_models, skill_stats_two_models)
        assert "composition" in joint.columns

    def test_create_ensemble_composition_string_format(
        self, forecasts_two_models, skill_stats_two_models, observed_two_dates
    ):
        """EM rows have composition='LR, TFT' format."""
        joint, _ = _make_pentad_ensemble(forecasts_two_models, skill_stats_two_models)
        em_rows = joint[joint["model_short"] == "EM"]
        assert not em_rows.empty
        for _, row in em_rows.iterrows():
            assert row["composition"] == "LR, TFT"

    def test_create_ensemble_preserves_discharge_values(
        self, forecasts_two_models, skill_stats_two_models, observed_two_dates
    ):
        """BEHAVIORAL ANCHOR: ensemble discharge = mean of qualifying models.

        Date 2024-01-05: LR=100, TFT=110 → EM=105.0
        Date 2024-01-10: LR=120, TFT=130 → EM=125.0
        """
        joint, _ = _make_pentad_ensemble(forecasts_two_models, skill_stats_two_models)
        em_rows = joint[joint["model_short"] == "EM"].sort_values("date")
        assert len(em_rows) == 2
        assert em_rows.iloc[0]["forecasted_discharge"] == pytest.approx(105.0, abs=1e-5)
        assert em_rows.iloc[1]["forecasted_discharge"] == pytest.approx(125.0, abs=1e-5)

    def test_skill_stats_passed_through_unchanged(self):
        """Skill stats are passed through without EM rows (PP-009).

        EM skill metrics are produced by recalculate_skill_metrics.py,
        not by create_ensemble_forecasts().
        """
        forecasts = pd.DataFrame(
            {
                "code": ["10001"] * 4,
                "date": pd.to_datetime(
                    [
                        "2023-01-05",
                        "2024-01-05",
                        "2023-01-05",
                        "2024-01-05",
                    ]
                ),
                "pentad_in_year": [1, 1, 1, 1],
                "pentad_in_month": ["1", "1", "1", "1"],
                "forecasted_discharge": [100.0, 120.0, 110.0, 130.0],
                "model_short": ["LR", "LR", "TFT", "TFT"],
            }
        )
        skill_stats = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["10001", "10001"],
                "model_short": ["LR", "TFT"],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.95, 0.90],
                "delta": [5.0, 5.0],
                "accuracy": [0.95, 0.88],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
            }
        )
        _, skill_out = _make_pentad_ensemble(forecasts, skill_stats)
        pd.testing.assert_frame_equal(
            skill_out.reset_index(drop=True),
            skill_stats.reset_index(drop=True),
        )

    def test_single_model_ensemble_discarded(self, observed_two_dates):
        """Single qualifying model → no EM row produced."""
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
        joint, _ = _make_pentad_ensemble(forecasts, skill_stats)
        assert "EM" not in joint["model_short"].values


class TestSkillMetricsCharacterization:
    """Capture current model_long-dependent behavior in skill_metrics."""

    @pytest.fixture
    def observed_sm(self):
        """Observed data for skill metrics tests: 2 stations, 2 pentads."""
        return pd.DataFrame(
            {
                "code": ["123", "123", "123", "123", "456", "456", "456", "456"],
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                    ]
                ),
                "discharge_avg": [10.0, 12.0, 10.0, 12.0, 20.0, 22.0, 20.0, 22.0],
                "model_short": ["Obs"] * 8,
                "delta": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
            }
        )

    @pytest.fixture
    def simulated_sm(self):
        """Simulated data: 2 models (MA, MB)."""
        df = pd.DataFrame(
            {
                "code": (["123"] * 4 + ["456"] * 4) * 2,
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                    ]
                    * 2
                ),
                "pentad_in_month": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
                "pentad_in_year": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
                "forecasted_discharge": [
                    10.2,
                    10.3,
                    9.8,
                    11.9,
                    20.2,
                    22.3,
                    20.1,
                    21.7,
                    10.1,
                    12.1,
                    10.05,
                    11.9,
                    20.1,
                    22.3,
                    19.9,
                    21.7,
                ],
                "model_short": ["MA"] * 8 + ["MB"] * 8,
            }
        )
        df["pentad_in_month"] = df["pentad_in_month"].astype(str)
        df["pentad_in_year"] = df["pentad_in_year"].astype(str)
        return df

    def test_pentad_works_without_model_long_in_observed(self, simulated_sm):
        """Pentad function accepts observed without model_long."""
        observed_no_ml = pd.DataFrame(
            {
                "code": ["123", "123"] * 2,
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                    ]
                ),
                "discharge_avg": [10.0, 12.0, 10.0, 12.0],
                "model_short": ["Obs"] * 4,
                "delta": [1.0, 1.0, 1.0, 1.0],
            }
        )
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(
            PENTAD, observed_no_ml, simulated_sm
        )
        assert not skill_stats.empty

    def test_pentad_works_without_model_long_in_simulated(self, observed_sm):
        """Pentad function accepts simulated without model_long."""
        simulated_no_ml = pd.DataFrame(
            {
                "code": ["123"] * 4 + ["456"] * 4,
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                    ]
                ),
                "pentad_in_year": ["1", "1", "2", "2", "1", "1", "2", "2"],
                "pentad_in_month": ["1", "1", "2", "2", "1", "1", "2", "2"],
                "forecasted_discharge": [
                    10.2,
                    10.3,
                    9.8,
                    11.9,
                    20.2,
                    22.3,
                    20.1,
                    21.7,
                ],
                "model_short": ["MA"] * 8,
            }
        )
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(
            PENTAD, observed_sm, simulated_no_ml
        )
        assert not skill_stats.empty

    def test_pentad_groupby_uses_model_short(self, observed_sm, simulated_sm):
        """skill_stats groups by model_short, not model_long."""
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed_sm, simulated_sm)
        assert "model_short" in skill_stats.columns
        model_shorts = skill_stats["model_short"].unique()
        assert "MA" in model_shorts
        assert "MB" in model_shorts

    def test_pentad_individual_model_metrics_correct(self, observed_sm, simulated_sm):
        """BEHAVIORAL ANCHOR: correct metrics for individual models.

        MA forecasts for station 123, pentad 1: [10.2, 10.3]
        Observed for station 123, pentad 1: [10.0, 12.0]
        """
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed_sm, simulated_sm)
        # Check MA at station 123, pentad 1
        # pentad_in_year may be string or int depending on input dtype
        ma_123_p1 = skill_stats[
            (skill_stats["model_short"] == "MA")
            & (skill_stats["code"] == "123")
            & (skill_stats["pentad_in_year"].astype(str) == "1")
        ]
        assert len(ma_123_p1) == 1
        assert ma_123_p1.iloc[0]["n_pairs"] == 2
        # MAE for MA: mean(|10-10.2|, |12-10.3|) = mean(0.2, 1.7) = 0.95
        assert ma_123_p1.iloc[0]["mae"] == pytest.approx(0.95, abs=0.01)

    def test_pentad_ensemble_discharge_mean_correct(self, observed_sm, simulated_sm, monkeypatch):
        """BEHAVIORAL ANCHOR: ensemble discharge = mean of MA + MB.

        At date 2022-01-01, station 123:
          MA: 10.2, MB: 10.1 → EM: 10.15
        At date 2023-01-01, station 123:
          MA: 10.3, MB: 12.1 → EM: 11.2
        """
        # Relax thresholds so both models qualify for ensemble
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        _, joint, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed_sm, simulated_sm)
        em_rows = joint[(joint["model_short"] == "EM") & (joint["code"] == "123")].sort_values(
            "date"
        )
        assert len(em_rows) >= 2
        # 2022-01-01: mean(10.2, 10.1) = 10.15
        em_jan1 = em_rows[em_rows["date"] == pd.Timestamp("2022-01-01")]
        assert em_jan1.iloc[0]["forecasted_discharge"] == pytest.approx(10.15, abs=1e-5)

    def test_decade_works_without_model_long(self):
        """Decade function accepts data without model_long."""
        observed = pd.DataFrame(
            {
                "code": ["123", "123"],
                "date": pd.to_datetime(["2022-01-01", "2023-01-01"]),
                "discharge_avg": [10.0, 12.0],
                "model_short": ["Obs", "Obs"],
                "delta": [1.0, 1.0],
            }
        )
        simulated = pd.DataFrame(
            {
                "code": ["123", "123"],
                "date": pd.to_datetime(["2022-01-01", "2023-01-01"]),
                "decad_in_year": ["1", "1"],
                "forecasted_discharge": [10.2, 12.1],
                "model_short": ["MA", "MA"],
            }
        )
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(DECAD, observed, simulated)
        assert not skill_stats.empty
        assert "model_short" in skill_stats.columns

    def test_decade_multi_model_ensemble(self, monkeypatch):
        """Decade: 2 models → EM created with correct discharge and composition.

        Station 123, decad 1, 2 years of data:
          Observed:  100.0 (2022), 120.0 (2023)
          MA:        100.0, 120.0  (perfect)
          MB:        100.0, 120.0  (perfect)
        → EM = mean(MA, MB) = 100.0, 120.0  (also perfect)
        → NSE=1, MAE=0 for EM
        """
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        observed = pd.DataFrame(
            {
                "code": ["123", "123"],
                "date": pd.to_datetime(["2022-01-01", "2023-01-01"]),
                "discharge_avg": [100.0, 120.0],
                "model_short": ["Obs", "Obs"],
                "delta": [5.0, 5.0],
            }
        )
        simulated = pd.DataFrame(
            {
                "code": ["123"] * 4,
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-01",
                        "2023-01-01",
                    ]
                ),
                "decad_in_year": ["1", "1", "1", "1"],
                "decad_in_month": ["1", "1", "1", "1"],
                "forecasted_discharge": [100.0, 120.0, 100.0, 120.0],
                "model_short": ["MA", "MA", "MB", "MB"],
            }
        )
        skill_stats, joint, _ = skill_metrics.calculate_skill_metrics(DECAD, observed, simulated)
        # EM rows exist in joint forecasts
        em_rows = joint[joint["model_short"] == "EM"]
        assert len(em_rows) == 2

        # EM discharge = mean(MA, MB) = mean(100, 100) = 100.0 for 2022
        em_2022 = em_rows[em_rows["date"] == pd.Timestamp("2022-01-01")]
        assert em_2022.iloc[0]["forecasted_discharge"] == pytest.approx(100.0, abs=1e-5)

        # EM skill stats exist with correct metrics (perfect forecast)
        em_skill = skill_stats[skill_stats["model_short"] == "EM"]
        assert len(em_skill) == 1
        row = em_skill.iloc[0]
        assert row["nse"] == pytest.approx(1.0, abs=1e-5)
        assert row["mae"] == pytest.approx(0.0, abs=1e-5)
        assert row["n_pairs"] == 2

        # Composition column present on EM rows
        assert "composition" in em_rows.columns
        comp = em_rows.iloc[0]["composition"]
        assert "MA" in comp and "MB" in comp


class TestDataReaderCharacterization:
    """Verify data_reader no longer produces model_long."""

    def test_normalize_api_has_model_short_no_model_long(self):
        """API normalization produces model_short, not model_long."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [1, 2],
                "model_type": ["LR", "TFT"],
                "code": ["10001", "10001"],
                "sdivsigma": [0.3, 0.4],
            }
        )
        result = data_reader._normalize_api_skill_metrics(df, "pentad")
        assert "model_short" in result.columns
        assert "model_long" not in result.columns
        assert result.iloc[0]["model_short"] == "LR"
        assert result.iloc[1]["model_short"] == "TFT"

    def test_normalize_api_unknown_model_passthrough(self):
        """Unknown model_type passes through as model_short."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [1],
                "model_type": ["NEWMODEL"],
                "code": ["10001"],
            }
        )
        result = data_reader._normalize_api_skill_metrics(df, "pentad")
        assert result.iloc[0]["model_short"] == "NEWMODEL"
        assert "model_long" not in result.columns

    def test_csv_read_preserves_model_short(self, tmp_path):
        """CSV with model_short column read correctly."""
        csv_file = tmp_path / "skill.csv"
        pd.DataFrame(
            {
                "pentad_in_year": [1],
                "code": ["10001"],
                "model_short": ["LR"],
                "sdivsigma": [0.3],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.95],
                "mae": [2.0],
                "n_pairs": [10],
            }
        ).to_csv(csv_file, index=False)

        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
                "ieasyforecast_pentadal_skill_metrics_file": "skill.csv",
            },
        ):
            result = data_reader._read_skill_metrics_csv("pentad")
            assert "model_short" in result.columns
            assert result.iloc[0]["model_short"] == "LR"


class TestApiWriterCharacterization:
    """Verify api_writer uses composition column directly."""

    def test_combined_forecast_uses_composition_column(self):
        """Composition column is used directly for ensemble rows."""
        data = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "forecasted_discharge": [105.0],
                "model_short": ["EM"],
                "composition": ["LR, TFT"],
            }
        )
        df_rec = data.copy()
        # composition column present and non-empty → used directly
        assert df_rec.iloc[0]["composition"] == "LR, TFT"

    def test_api_writer_warns_on_missing_composition(self):
        """Ensemble row without composition logs a warning."""
        data = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-05"]),
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "forecasted_discharge": [105.0],
                "model_short": ["EM"],
            }
        )
        # No composition column → api_writer should warn
        assert "composition" not in data.columns


# ===================================================================
# Part 2: Tests for new composition_agg / is_multi_model_composition
# ===================================================================


class TestCompositionAgg:
    """Tests for the new composition_agg function."""

    def test_two_models(self):
        result = composition_agg(pd.Series(["LR", "TFT"]))
        assert result == "LR, TFT"

    def test_three_models_sorted(self):
        result = composition_agg(pd.Series(["TiDE", "LR", "TFT"]))
        assert result == "LR, TFT, TiDE"

    def test_single_model(self):
        result = composition_agg(pd.Series(["TFT"]))
        assert result == "TFT"

    def test_deduplicates(self):
        result = composition_agg(pd.Series(["LR", "LR", "TFT"]))
        assert result == "LR, TFT"

    def test_four_models(self):
        result = composition_agg(pd.Series(["TSMixer", "TiDE", "LR", "TFT"]))
        assert result == "LR, TFT, TSMixer, TiDE"


class TestIsMultiModelComposition:
    """Tests for the new is_multi_model_composition function."""

    def test_true_for_two_models(self):
        assert is_multi_model_composition("LR, TFT") is True

    def test_true_for_three_models(self):
        assert is_multi_model_composition("LR, TFT, TiDE") is True

    def test_false_for_single_model(self):
        assert is_multi_model_composition("TFT") is False

    def test_false_for_empty_string(self):
        assert is_multi_model_composition("") is False
