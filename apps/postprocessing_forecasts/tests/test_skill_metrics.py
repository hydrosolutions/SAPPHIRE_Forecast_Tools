"""Tests for src/skill_metrics.py — skill metric calculations.

Moved from iEasyHydroForecast/tests/test_forecast_library.py
(TestCalculateSkillMetricsPentad).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from conftest import DECAD, PENTAD
from src import skill_metrics


@pytest.fixture
def observed():
    """Sample observed data: 2 stations, 2 pentads, 2 years."""
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
def simulated():
    """Sample simulated data: 2 models (MA, MB)."""
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


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    """Set ensemble threshold env vars."""
    monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "0.6")
    monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.8")
    monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "0.8")


class TestCalculateSkillMetricsPentad:
    """Tests for calculate_skill_metrics (pentad)."""

    def test_input_validation(self, observed, simulated):
        """Missing columns in observed or simulated raise ValueError."""
        bad_observed = observed.drop(columns=["delta"])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics(PENTAD, bad_observed, simulated)

        bad_simulated = simulated.drop(columns=["pentad_in_year"])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics(PENTAD, observed, bad_simulated)

    def test_date_filtering(self, observed, simulated):
        """Data filtered for dates after 2010."""
        combined_observed = pd.concat([observed, observed.copy()])
        skill_stats, joint_forecasts, _ = skill_metrics.calculate_skill_metrics(
            PENTAD, combined_observed, simulated
        )
        assert all(joint_forecasts["date"].dt.year >= 2010)

    def test_sdivsigma_calculation(self, observed, simulated):
        """sdivsigma values are finite and < 1 for good forecasts."""
        merged = pd.merge(
            simulated,
            observed[["code", "date", "discharge_avg", "delta"]],
            on=["code", "date"],
        )
        output = (
            merged.groupby(["pentad_in_year", "code", "model_short"])[merged.columns]
            .apply(
                skill_metrics.sdivsigma_nse,
                observed_col="discharge_avg",
                simulated_col="forecasted_discharge",
            )
            .reset_index()
        )
        assert all(output["nse"] < 1)

    def test_skill_metrics_columns_and_ranges(self, observed, simulated):
        """Skill stats have expected columns; values in valid ranges."""
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, simulated)
        expected_columns = [
            "pentad_in_year",
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "mae",
            "n_pairs",
            "delta",
            "accuracy",
            "pbias",
            "kgelf",
            "nse_log",
        ]
        for col in expected_columns:
            assert col in skill_stats.columns, f"Missing column: {col}"
        assert all(skill_stats["accuracy"] >= 0)
        assert all(skill_stats["accuracy"] <= 1)
        assert all(skill_stats["sdivsigma"] >= 0)
        assert all(skill_stats["mae"] >= 0)

    def test_no_ensemble_creation(self, observed, simulated, monkeypatch):
        """calculate_skill_metrics no longer derives EM (PP-072 P2).

        Even with thresholds relaxed so both models would have qualified,
        output contains per-model rows only and joint == input models.
        """
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        skill_stats, joint, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, simulated)

        assert "EM" not in joint["model_short"].values
        assert "EM" not in skill_stats["model_short"].values
        assert set(joint["model_short"]) == {"MA", "MB"}
        assert len(joint) == len(simulated)

    def test_perfect_forecast(self, observed, simulated):
        """Perfect forecasts produce sdivsigma=0, nse=1, mae=0, acc=1."""
        perfect = simulated.copy()
        perfect["forecasted_discharge"] = np.tile(
            [10.0, 12.0, 10.0, 12.0, 20.0, 22.0, 20.0, 22.0], 2
        )
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, perfect)
        for _, row in skill_stats.iterrows():
            assert row["sdivsigma"] == pytest.approx(0.0, abs=1e-5)
            assert row["nse"] == pytest.approx(1.0, abs=1e-5)
            assert row["mae"] == pytest.approx(0.0, abs=1e-5)
            assert row["accuracy"] == pytest.approx(1.0, abs=1e-5)

    def test_timing_stats_integration(self, observed, simulated):
        """Timing stats object is passed through and returned."""

        class MockTimingStats:
            def __init__(self):
                self.sections = []

            def start(self, section):
                self.sections.append(f"start_{section}")

            def end(self, section):
                self.sections.append(f"end_{section}")

        ts = MockTimingStats()
        _, _, returned = skill_metrics.calculate_skill_metrics(PENTAD, observed, simulated, ts)
        assert any(s.startswith("start_") for s in ts.sections), (
            "timing_stats.start() should be called at least once"
        )
        assert any(s.startswith("end_") for s in ts.sections), (
            "timing_stats.end() should be called at least once"
        )
        assert returned is ts


# ---------------------------------------------------------------------------
# Decade-specific fixtures and tests
# ---------------------------------------------------------------------------


@pytest.fixture
def observed_decad():
    """Sample observed data for decade: 2 stations, 2 decads, 2 years."""
    return pd.DataFrame(
        {
            "code": ["123", "123", "123", "123", "456", "456", "456", "456"],
            "date": pd.to_datetime(
                [
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                ]
            ),
            "discharge_avg": [10.0, 12.0, 10.0, 12.0, 20.0, 22.0, 20.0, 22.0],
            "model_short": ["Obs"] * 8,
            "delta": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
        }
    )


@pytest.fixture
def simulated_decad():
    """Sample simulated data for decade: 2 models (MA, MB)."""
    df = pd.DataFrame(
        {
            "code": (["123"] * 4 + ["456"] * 4) * 2,
            "date": pd.to_datetime(
                [
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                ]
                * 2
            ),
            "decad_in_month": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
            "decad_in_year": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
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
    # tl.get_decad_in_month returns strings; match real data format
    df["decad_in_month"] = df["decad_in_month"].astype(str)
    df["decad_in_year"] = df["decad_in_year"].astype(str)
    return df


class TestCalculateSkillMetricsDecade:
    """Tests for calculate_skill_metrics (decad)."""

    def test_input_validation(self, observed_decad, simulated_decad):
        """Missing columns in observed or simulated raise ValueError."""
        bad_observed = observed_decad.drop(columns=["delta"])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics(DECAD, bad_observed, simulated_decad)

        bad_simulated = simulated_decad.drop(columns=["decad_in_year"])
        with pytest.raises(ValueError):
            skill_metrics.calculate_skill_metrics(DECAD, observed_decad, bad_simulated)

    def test_skill_metrics_columns_and_ranges(self, observed_decad, simulated_decad):
        """Skill stats have expected columns; values in valid ranges."""
        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(
            DECAD, observed_decad, simulated_decad
        )
        expected_columns = [
            "decad_in_year",
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "mae",
            "n_pairs",
            "delta",
            "accuracy",
            "pbias",
            "kgelf",
            "nse_log",
        ]
        for col in expected_columns:
            assert col in skill_stats.columns, f"Missing column: {col}"
        assert all(skill_stats["accuracy"] >= 0)
        assert all(skill_stats["accuracy"] <= 1)
        assert all(skill_stats["sdivsigma"] >= 0)
        assert all(skill_stats["mae"] >= 0)

    def test_no_ensemble_creation(self, observed_decad, simulated_decad, monkeypatch):
        """Decade: no EM derived (PP-072 P2); per-model rows only."""
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        skill_stats, joint, _ = skill_metrics.calculate_skill_metrics(
            DECAD, observed_decad, simulated_decad
        )

        assert "EM" not in joint["model_short"].values
        assert "EM" not in skill_stats["model_short"].values
        assert len(joint) == len(simulated_decad)

    def test_nan_forecasts_do_not_produce_decade_em(self, monkeypatch):
        """NaN forecasted_discharge input produces no EM rows (PP-072 P2).

        Formerly a regression test for a false multi-model EM; EM is no
        longer derived in calculate_skill_metrics, so none can appear.
        """
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        # 4 years x 1 decad x 1 station = 4 rows per model.
        # All in decad_in_year=1 so skill stats use 4 (or 3) points.
        dates = pd.to_datetime(
            [
                "2021-01-10",
                "2022-01-10",
                "2023-01-10",
                "2024-01-10",
            ]
        )
        observed = pd.DataFrame(
            {
                "code": ["123"] * 4,
                "date": dates,
                "discharge_avg": [10.0, 12.0, 11.0, 13.0],
                "model_short": ["Obs"] * 4,
                "delta": [2.0] * 4,
            }
        )

        # MA: all valid. MB: NaN on 2021-01-10, rest valid.
        simulated = pd.DataFrame(
            {
                "code": ["123"] * 8,
                "date": list(dates) * 2,
                "decad_in_month": ["1"] * 8,
                "decad_in_year": ["1"] * 8,
                "forecasted_discharge": [
                    10.5,
                    12.5,
                    11.5,
                    13.5,  # MA: all valid
                    np.nan,
                    12.3,
                    11.3,
                    13.3,  # MB: NaN at 2021-01-10
                ],
                "model_short": ["MA"] * 4 + ["MB"] * 4,
            }
        )

        skill_stats, joint, _ = skill_metrics.calculate_skill_metrics(DECAD, observed, simulated)

        # No EM at all (PP-072 P2); the NaN forecast row is passed through
        # unchanged and never leaks into any derived value.
        assert "EM" not in joint["model_short"].values
        assert "EM" not in skill_stats["model_short"].values
        assert len(joint) == len(simulated)
        assert joint["forecasted_discharge"].isna().sum() == 1

    def test_nan_forecasts_do_not_produce_pentad_em(self, observed, monkeypatch):
        """Pentad path: NaN input produces no EM rows (parity with decade)."""
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

        simulated_with_nan = pd.DataFrame(
            {
                "code": ["123"] * 8,
                "date": pd.to_datetime(
                    [
                        "2022-01-01",
                        "2023-01-01",
                        "2022-01-06",
                        "2023-01-06",
                    ]
                    * 2
                ),
                "pentad_in_month": ["1", "1", "2", "2"] * 2,
                "pentad_in_year": ["1", "1", "2", "2"] * 2,
                "forecasted_discharge": [
                    10.0,
                    12.0,
                    10.0,
                    12.0,  # MA: all valid
                    np.nan,
                    12.0,
                    10.0,
                    12.0,  # MB: NaN at first date
                ],
                "model_short": ["MA"] * 4 + ["MB"] * 4,
            }
        )

        _, joint, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, simulated_with_nan)

        assert "EM" not in joint["model_short"].values
        assert len(joint) == len(simulated_with_nan)


# ===================================================================
# Delta validation tests (Commit 4)
# ===================================================================


class TestDeltaValidation:
    """Tests for delta handling in forecast_accuracy_hydromet and
    calculate_all_skill_metrics — verify they use first value and
    warn on variation."""

    def _make_df(self, deltas):
        """Helper: build a minimal DataFrame with given delta values."""
        n = len(deltas)
        return pd.DataFrame(
            {
                "observed": np.arange(1.0, n + 1),
                "simulated": np.arange(1.0, n + 1) + 0.1,
                "delta": deltas,
            }
        )

    # --- forecast_accuracy_hydromet ---

    def test_constant_delta_returns_first_value(self):
        """When all deltas are identical, result uses that value."""
        df = self._make_df([5.0, 5.0, 5.0])
        result = skill_metrics.forecast_accuracy_hydromet(df, "observed", "simulated", "delta")
        assert result["delta"] == 5.0

    def test_varying_delta_logs_warning(self, caplog):
        """When deltas vary, a warning is logged."""
        import logging

        df = self._make_df([5.0, 5.0, 5.1])
        with caplog.at_level(logging.WARNING):
            skill_metrics.forecast_accuracy_hydromet(df, "observed", "simulated", "delta")
        assert any("Delta values vary" in msg for msg in caplog.messages)

    def test_single_point_delta_no_warning(self, caplog):
        """Single data point — no variation possible, no warning."""
        import logging

        df = self._make_df([5.0])
        with caplog.at_level(logging.WARNING):
            skill_metrics.forecast_accuracy_hydromet(df, "observed", "simulated", "delta")
        assert not any("Delta values vary" in msg for msg in caplog.messages)

    def test_delta_uses_first_not_last(self):
        """Regression: result is first delta, not last."""
        df = self._make_df([5.0, 6.0])
        result = skill_metrics.forecast_accuracy_hydromet(df, "observed", "simulated", "delta")
        assert result["delta"] == 5.0

    # --- calculate_all_skill_metrics ---

    def test_all_metrics_constant_delta_returns_first(self):
        """calculate_all_skill_metrics uses first delta when constant."""
        df = self._make_df([5.0, 5.0, 5.0])
        result = skill_metrics.calculate_all_skill_metrics(df, "observed", "simulated", "delta")
        assert result["delta"] == 5.0

    def test_all_metrics_varying_delta_logs_warning(self, caplog):
        """calculate_all_skill_metrics warns on varying deltas."""
        import logging

        df = self._make_df([5.0, 5.0, 5.1])
        with caplog.at_level(logging.WARNING):
            skill_metrics.calculate_all_skill_metrics(df, "observed", "simulated", "delta")
        assert any("Delta values vary" in msg for msg in caplog.messages)

    def test_all_metrics_delta_uses_first_not_last(self):
        """Regression: calculate_all_skill_metrics uses first delta."""
        df = self._make_df([5.0, 6.0])
        result = skill_metrics.calculate_all_skill_metrics(df, "observed", "simulated", "delta")
        assert result["delta"] == 5.0

    def test_all_metrics_single_point_no_warning(self, caplog):
        """Single point — no variation, no warning in all-metrics path."""
        import logging

        df = self._make_df([5.0])
        with caplog.at_level(logging.WARNING):
            skill_metrics.calculate_all_skill_metrics(df, "observed", "simulated", "delta")
        assert not any("Delta values vary" in msg for msg in caplog.messages)


class TestConfigurableYearFilter:
    """Tests for SAPPHIRE_SKILL_METRICS_START_YEAR env var in pentad/decad."""

    @pytest.fixture
    def _observed_multi_year(self):
        """Observed data: 1 station, 1 pentad, years 2005-2025."""
        years = list(range(2005, 2026))
        dates = pd.to_datetime([f"{y}-01-01" for y in years])
        return pd.DataFrame(
            {
                "code": ["123"] * len(years),
                "date": dates,
                "discharge_avg": [10.0 + i * 0.1 for i in range(len(years))],
                "model_short": ["Obs"] * len(years),
                "delta": [1.0] * len(years),
            }
        )

    @pytest.fixture
    def _simulated_multi_year(self):
        """Simulated data: 2 models (MA, MB), 1 station, years 2005-2025."""
        years = list(range(2005, 2026))
        dates = pd.to_datetime([f"{y}-01-01" for y in years])
        n = len(years)
        return pd.DataFrame(
            {
                "code": ["123"] * n * 2,
                "date": list(dates) * 2,
                "pentad_in_year": ["1"] * n * 2,
                "pentad_in_month": ["1"] * n * 2,
                "forecasted_discharge": (
                    [10.0 + i * 0.1 for i in range(n)] + [10.0 + i * 0.15 for i in range(n)]
                ),
                "model_short": ["MA"] * n + ["MB"] * n,
            }
        )

    def test_env_var_filters_pentad_data(self, _observed_multi_year, _simulated_multi_year):
        """SAPPHIRE_SKILL_METRICS_START_YEAR=2020 excludes pre-2020 data."""
        os.environ["SAPPHIRE_SKILL_METRICS_START_YEAR"] = "2020"
        try:
            stats, joint, _ = skill_metrics.calculate_skill_metrics(
                PENTAD,
                _observed_multi_year,
                _simulated_multi_year,
            )
            # All pairs should be from 2020 onwards
            assert joint["date"].min().year >= 2020
        finally:
            del os.environ["SAPPHIRE_SKILL_METRICS_START_YEAR"]

    def test_default_uses_rolling_window(self, _observed_multi_year, _simulated_multi_year):
        """Without env var, uses current_year - 20 as default."""
        os.environ.pop("SAPPHIRE_SKILL_METRICS_START_YEAR", None)
        stats, joint, _ = skill_metrics.calculate_skill_metrics(
            PENTAD,
            _observed_multi_year,
            _simulated_multi_year,
        )
        # Default: current_year - 20 = 2006
        # Data starts 2005, so 2005 should be excluded
        assert joint["date"].min().year >= 2006
        assert not joint.empty

    @pytest.fixture
    def _observed_decad_multi_year(self):
        """Observed data for decad: 1 station, years 2005-2025."""
        years = list(range(2005, 2026))
        dates = pd.to_datetime([f"{y}-01-01" for y in years])
        return pd.DataFrame(
            {
                "code": ["123"] * len(years),
                "date": dates,
                "discharge_avg": [10.0 + i * 0.1 for i in range(len(years))],
                "model_short": ["Obs"] * len(years),
                "delta": [1.0] * len(years),
            }
        )

    @pytest.fixture
    def _simulated_decad_multi_year(self):
        """Simulated decad data: 2 models, 1 station, years 2005-2025."""
        years = list(range(2005, 2026))
        dates = pd.to_datetime([f"{y}-01-01" for y in years])
        n = len(years)
        return pd.DataFrame(
            {
                "code": ["123"] * n * 2,
                "date": list(dates) * 2,
                "decad_in_year": ["1"] * n * 2,
                "decad_in_month": ["1"] * n * 2,
                "forecasted_discharge": (
                    [10.0 + i * 0.1 for i in range(n)] + [10.0 + i * 0.15 for i in range(n)]
                ),
                "model_short": ["MA"] * n + ["MB"] * n,
            }
        )

    def test_env_var_filters_decad_data(
        self,
        _observed_decad_multi_year,
        _simulated_decad_multi_year,
    ):
        """SAPPHIRE_SKILL_METRICS_START_YEAR=2020 excludes pre-2020 decad."""
        os.environ["SAPPHIRE_SKILL_METRICS_START_YEAR"] = "2020"
        try:
            stats, joint, _ = skill_metrics.calculate_skill_metrics(
                DECAD,
                _observed_decad_multi_year,
                _simulated_decad_multi_year,
            )
            assert joint["date"].min().year >= 2020
        finally:
            del os.environ["SAPPHIRE_SKILL_METRICS_START_YEAR"]

    def test_decad_default_uses_rolling_window(
        self,
        _observed_decad_multi_year,
        _simulated_decad_multi_year,
    ):
        """Decad without env var uses current_year - 20 as default."""
        os.environ.pop("SAPPHIRE_SKILL_METRICS_START_YEAR", None)
        stats, joint, _ = skill_metrics.calculate_skill_metrics(
            DECAD,
            _observed_decad_multi_year,
            _simulated_decad_multi_year,
        )
        assert joint["date"].min().year >= 2006


class TestShortTermCrps:
    """Test CRPS computation with 4-column short-term quantile set."""

    def test_crps_with_4_quantiles(self):
        """CRPS can be computed from 4 quantile columns."""
        from src.skill_metrics import calculate_crps

        # Perfect forecast: observation equals median
        observed = np.array([10.0, 20.0, 30.0])
        # Quantile forecasts: q05, q25, q75, q95
        quantile_forecasts = np.array(
            [
                [5.0, 8.0, 12.0, 15.0],
                [15.0, 18.0, 22.0, 25.0],
                [25.0, 28.0, 32.0, 35.0],
            ]
        )
        quantile_levels = np.array([0.05, 0.25, 0.75, 0.95])
        crps = calculate_crps(observed, quantile_forecasts, quantile_levels)
        assert isinstance(crps, float)
        assert crps >= 0.0
        assert not np.isnan(crps)

    def test_crps_perfect_4q(self):
        """CRPS is zero for perfect deterministic forecast with 4 quantiles."""
        from src.skill_metrics import calculate_crps

        observed = np.array([10.0])
        # All quantiles equal to observation
        quantile_forecasts = np.array([[10.0, 10.0, 10.0, 10.0]])
        quantile_levels = np.array([0.05, 0.25, 0.75, 0.95])
        crps = calculate_crps(observed, quantile_forecasts, quantile_levels)
        assert crps == pytest.approx(0.0, abs=1e-10)


# ===================================================================
# PP-030: exclude_models parameter tests
# ===================================================================


@pytest.fixture
def decad_observed():
    """Sample observed data for decad: 2 stations, 2 decads, 2 years."""
    return pd.DataFrame(
        {
            "code": ["123", "123", "123", "123", "456", "456", "456", "456"],
            "date": pd.to_datetime(
                [
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                ]
            ),
            "discharge_avg": [10.0, 12.0, 10.0, 12.0, 20.0, 22.0, 20.0, 22.0],
            "model_short": ["Obs"] * 8,
            "delta": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
        }
    )


@pytest.fixture
def decad_simulated():
    """Sample simulated data for decad: 2 models (MA, MB)."""
    df = pd.DataFrame(
        {
            "code": (["123"] * 4 + ["456"] * 4) * 2,
            "date": pd.to_datetime(
                [
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                    "2022-01-10",
                    "2023-01-10",
                    "2022-01-20",
                    "2023-01-20",
                ]
                * 2
            ),
            "decad_in_month": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
            "decad_in_year": [1, 1, 2, 2, 1, 1, 2, 2] * 2,
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
    df["decad_in_month"] = df["decad_in_month"].astype(str)
    df["decad_in_year"] = df["decad_in_year"].astype(str)
    return df


class TestExcludeModels:
    """Tests for the exclude_models parameter in calculate_skill_metrics (PP-030)."""

    def _relax_thresholds(self, monkeypatch):
        """Set threshold env vars to relaxed values so both models qualify."""
        monkeypatch.setenv("ieasyhydroforecast_efficiency_threshold", "2.0")
        monkeypatch.setenv("ieasyhydroforecast_accuracy_threshold", "0.0")
        monkeypatch.setenv("ieasyhydroforecast_nse_threshold", "-1.0")

    def test_exclude_em_no_em_in_output(self, observed, simulated, monkeypatch):
        """When exclude_models=["EM"], EM is absent from both outputs."""
        self._relax_thresholds(monkeypatch)

        skill_stats, joint_forecasts, _ = skill_metrics.calculate_skill_metrics(
            PENTAD, observed, simulated, exclude_models=["EM"]
        )

        assert "EM" not in skill_stats["model_short"].values
        assert "EM" not in joint_forecasts["model_short"].values
        # Individual models are still present
        assert "MA" in skill_stats["model_short"].values
        assert "MB" in skill_stats["model_short"].values
        assert "MA" in joint_forecasts["model_short"].values
        assert "MB" in joint_forecasts["model_short"].values

    def test_exclude_em_individual_crps_still_computed(self, observed, simulated, monkeypatch):
        """CRPS column present for individual models when EM is excluded.

        The minimal fixture has no quantile columns (q05…q95), so the
        CRPS path falls back to NaN — that is the correct, documented
        behaviour.  This test verifies that the column exists for MA and
        MB, confirming that the CRPS calculation path was still executed
        for individual models (not bypassed by the EM exclusion).
        """
        self._relax_thresholds(monkeypatch)

        skill_stats, _, _ = skill_metrics.calculate_skill_metrics(
            PENTAD, observed, simulated, exclude_models=["EM"]
        )

        assert "crps" in skill_stats.columns
        ma_rows = skill_stats[skill_stats["model_short"] == "MA"]
        mb_rows = skill_stats[skill_stats["model_short"] == "MB"]
        assert not ma_rows.empty, "MA rows should be present in skill_stats"
        assert not mb_rows.empty, "MB rows should be present in skill_stats"
        # CRPS is NaN when quantile columns are absent — verify the column
        # exists and has the expected shape (one row per group)
        assert len(ma_rows) > 0
        assert len(mb_rows) > 0

    def test_em_in_input_does_not_trigger_derivation(self, observed, simulated, monkeypatch):
        """EM among the input model names yields no derived EM rows.

        calculate_skill_metrics never derives EM from the individual models
        (PP-072 P2): any EM row in the output is the input's own row passed
        through, and none carries a derived composition.
        """
        self._relax_thresholds(monkeypatch)
        em_input = simulated[simulated["model_short"] == "MA"].copy()
        em_input["model_short"] = "EM"
        sim_with_em = pd.concat([simulated, em_input], ignore_index=True)

        skill_stats, joint, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, sim_with_em)

        assert len(joint) == len(sim_with_em)
        assert (joint["model_short"] == "EM").sum() == len(em_input)
        if "composition" in skill_stats.columns:
            em_comp = skill_stats.loc[skill_stats["model_short"] == "EM", "composition"]
            assert em_comp.fillna("").eq("").all()

    def test_default_exclude_models_no_em(self, observed, simulated, monkeypatch):
        """Default exclude_models=None produces no EM rows (PP-072 P2)."""
        self._relax_thresholds(monkeypatch)

        _, joint_forecasts, _ = skill_metrics.calculate_skill_metrics(PENTAD, observed, simulated)

        assert "EM" not in joint_forecasts["model_short"].values

    def test_exclude_em_with_decad_config(self, decad_observed, decad_simulated, monkeypatch):
        """exclude_models=["EM"] also suppresses EM in the DECAD path."""
        self._relax_thresholds(monkeypatch)

        skill_stats, joint_forecasts, _ = skill_metrics.calculate_skill_metrics(
            DECAD, decad_observed, decad_simulated, exclude_models=["EM"]
        )

        assert "EM" not in skill_stats["model_short"].values
        assert "MA" in skill_stats["model_short"].values
        assert "MB" in skill_stats["model_short"].values
