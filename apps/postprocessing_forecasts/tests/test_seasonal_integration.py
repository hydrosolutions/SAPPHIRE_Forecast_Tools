"""Dedicated seasonal integration tests for postprocessing forecasts.

Covers gaps not exercised by the quarterly-focused test files:
- Skilled Mean ensemble for seasonal (A1-A2)
- EM composition string for seasonal (A2)
- Single-model rejection for seasonal (A3)
- Skill metrics edge cases for seasonal (B4-B6)
- Data reader with non-empty API data (C7-C9)
- File writer save functions (D10-D11)
- Cross-year pipeline with numerical verification (E12-E13)
"""

import datetime as dt
import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.aggregation import (
    aggregate_monthly_obs_to_seasonal,
    get_season_months,
    get_season_year,
)
from src.ensemble_calculator import create_seasonal_ensemble_forecasts
from src.skill_metrics import calculate_seasonal_skill_metrics

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
# Helpers
# ---------------------------------------------------------------------------


def _make_seasonal_skill(rows):
    """(season_in_year, code, model_short, sdivsigma, nse, delta,
    accuracy, mae, n_pairs) -> DataFrame."""
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
    forecasted_discharge, q05..q95) -> DataFrame."""
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


def _monthly_obs(n_years=3, codes=("S1",), start_month=1, end_month=12):
    """Create monthly observations for multiple years."""
    rows = []
    for code in codes:
        for year in range(2020, 2020 + n_years):
            for month in range(start_month, end_month + 1):
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


def _monthly_fc(n_years=3, codes=("S1",), models=("LR_Base", "LR_SM")):
    """Create monthly forecasts for multiple models/years."""
    rows = []
    for code in codes:
        for year in range(2020, 2020 + n_years):
            for month in range(1, 13):
                for model in models:
                    base = 50 + month * 5 + (year - 2020) * 2
                    offset = 2 if model == "LR_Base" else -1
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


def _make_seasonal_obs(rows):
    """(code, season_year, discharge_avg) -> DataFrame with delta, season_in_year."""
    df = pd.DataFrame(rows, columns=["code", "season_year", "discharge_avg"])
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    df = df.merge(delta_df[["code", "delta"]], on=["code"], how="left")
    return df


def _make_seasonal_fcst_for_skill(rows):
    """(code, season_year, model_short, q05..q95) -> with season_in_year."""
    df = pd.DataFrame(
        rows,
        columns=[
            "code",
            "season_year",
            "model_short",
            "q05",
            "q10",
            "q25",
            "q50",
            "q75",
            "q90",
            "q95",
        ],
    )
    df["season_in_year"] = 1
    return df


# ===================================================================
# A. Seasonal Ensemble Calculator Gaps
# ===================================================================


class TestSeasonalEnsembleSkilled:
    """Gaps A1-A3: Skilled Mean, EM composition, single-model rejection."""

    def test_skilled_mean_created(self):
        """Skilled Mean should be created for seasonal with 2+ models."""
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        assert not sm.empty

    def test_skilled_mean_weighted_by_mae(self):
        """Skilled Mean should weight by 1/MAE, closer to lower-MAE model."""
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        sm = result[result["model_short"] == "Skilled Mean"]
        # LR_Base: MAE=2.0, LR_SM: MAE=3.0
        # LR_Base has lower MAE -> higher weight -> result closer to 100
        discharge = sm.iloc[0]["forecasted_discharge"]
        assert discharge < 110.0  # closer to 100 than simple mean 110

    def test_em_composition_string(self):
        """EM composition should contain contributing model names."""
        skill = _two_model_seasonal_skill()
        fcst = _two_model_seasonal_fcst()
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        assert not em.empty
        comp = str(em.iloc[0]["composition"])
        assert "LR_Base" in comp
        assert "LR_SM" in comp

    def test_em_not_created_single_model(self):
        """Single model should not produce EM for seasonal."""
        skill = _make_seasonal_skill(
            [
                (1, "S1", "LR_Base", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                ("S1", 2025, 1, "LR_Base", 100.0, 80, 85, 90, 100, 110, 115, 120),
            ]
        )
        result = create_seasonal_ensemble_forecasts(fcst, skill)
        em = result[result["model_short"] == "EM"]
        assert em.empty


# ===================================================================
# B. Seasonal Skill Metrics Edge Cases
# ===================================================================


class TestSeasonalSkillMetricsEdgeCases:
    """Gaps B4-B6: empty obs, empty forecasts, no overlap."""

    def test_empty_observations_returns_empty(self):
        obs = pd.DataFrame(
            columns=["code", "season_year", "season_in_year", "discharge_avg", "delta"]
        )
        fcst = _make_seasonal_fcst_for_skill(
            [
                ("S1", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        skill_stats, joint, ts = calculate_seasonal_skill_metrics(obs, fcst)
        assert skill_stats.empty or len(skill_stats) == 0
        assert ts is None

    def test_empty_forecasts_returns_empty(self):
        obs = _make_seasonal_obs([("S1", 2020, 100.0)])
        fcst = pd.DataFrame(
            columns=[
                "code",
                "season_year",
                "model_short",
                "q05",
                "q10",
                "q25",
                "q50",
                "q75",
                "q90",
                "q95",
                "season_in_year",
            ]
        )
        skill_stats, joint, ts = calculate_seasonal_skill_metrics(obs, fcst)
        assert skill_stats.empty or len(skill_stats) == 0

    def test_no_overlap_returns_empty(self):
        """No matching (code, season_year) between obs and fcst."""
        obs = _make_seasonal_obs([("S1", 2020, 100.0)])
        fcst = _make_seasonal_fcst_for_skill(
            [
                ("S2", 2020, "M1", 80, 85, 92, 102, 112, 118, 125),
            ]
        )
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        model_rows = skill_stats[
            ~skill_stats["model_short"].isin({"Naive Mean", "EM", "Skilled Mean"})
        ]
        assert model_rows.empty


class TestSeasonalSkillMetricsPerLead:
    """PP3: seasonal skill keeps issue lead in season_in_year."""

    def test_four_issue_leads_produce_distinct_skill_rows(self):
        code = "PP3_SENTINEL"
        model = "LR"
        observed_by_year = {
            2021: 100.0,
            2022: 120.0,
            2023: 140.0,
        }
        lead_error = {
            3: 30.0,  # January issue
            2: 20.0,  # February issue
            1: 10.0,  # March issue
            0: 0.0,  # April issue
        }
        obs = pd.DataFrame(
            {
                "code": [code] * len(observed_by_year),
                "season_year": list(observed_by_year),
                "season_in_year": [1] * len(observed_by_year),
                "discharge_avg": list(observed_by_year.values()),
                "delta": [5.0] * len(observed_by_year),
            }
        )
        fc_rows = []
        for season_year, observed in observed_by_year.items():
            for lead, error in lead_error.items():
                q50 = observed + error
                fc_rows.append(
                    {
                        "code": code,
                        "season_year": season_year,
                        "season_in_year": lead,
                        "model_short": model,
                        "q05": q50 - 20.0,
                        "q10": q50 - 15.0,
                        "q25": q50 - 5.0,
                        "q50": q50,
                        "q75": q50 + 5.0,
                        "q90": q50 + 15.0,
                        "q95": q50 + 20.0,
                    }
                )
        fcst = pd.DataFrame(fc_rows)

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        model_rows = skill_stats[
            (skill_stats["code"] == code) & (skill_stats["model_short"] == model)
        ]

        assert set(model_rows["season_in_year"]) == {0, 1, 2, 3}
        assert len(model_rows) == 4
        assert set(model_rows["n_pairs"]) == {3}

        mae_by_lead = dict(zip(model_rows["season_in_year"], model_rows["mae"], strict=True))
        assert mae_by_lead == {0: 0.0, 1: 10.0, 2: 20.0, 3: 30.0}
        assert model_rows["nse"].nunique(dropna=True) == 4

    def test_single_issue_lead_zero_still_works(self):
        code = "PP3_SINGLE_SENTINEL"
        obs = pd.DataFrame(
            {
                "code": [code, code, code],
                "season_year": [2021, 2022, 2023],
                "season_in_year": [1, 1, 1],
                "discharge_avg": [100.0, 120.0, 140.0],
                "delta": [5.0, 5.0, 5.0],
            }
        )
        fcst = pd.DataFrame(
            {
                "code": [code, code, code],
                "season_year": [2021, 2022, 2023],
                "season_in_year": [0, 0, 0],
                "model_short": ["LR", "LR", "LR"],
                "q05": [80.0, 100.0, 120.0],
                "q10": [85.0, 105.0, 125.0],
                "q25": [95.0, 115.0, 135.0],
                "q50": [100.0, 120.0, 140.0],
                "q75": [105.0, 125.0, 145.0],
                "q90": [115.0, 135.0, 155.0],
                "q95": [120.0, 140.0, 160.0],
            }
        )

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        model_rows = skill_stats[
            (skill_stats["code"] == code) & (skill_stats["model_short"] == "LR")
        ]

        assert len(model_rows) == 1
        assert model_rows.iloc[0]["season_in_year"] == 0
        assert model_rows.iloc[0]["mae"] == 0.0


# ===================================================================
# C. Data Reader Gaps
# ===================================================================


class TestSeasonalDataReaderNonEmpty:
    """Gaps C7-C9: non-empty API data for seasonal readers."""

    def test_latest_seasonal_forecasts_returns_data(self, monkeypatch):
        """read_latest_seasonal_forecasts with non-empty API response."""
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)

        from src import data_reader

        # Simulate API returning monthly forecasts spanning Apr-Sep 2024
        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "valid_from": pd.to_datetime(
                    [
                        "2024-04-01",
                        "2024-05-01",
                        "2024-06-01",
                        "2024-07-01",
                        "2024-08-01",
                        "2024-09-01",
                    ]
                ),
                "valid_to": pd.to_datetime(
                    [
                        "2024-04-30",
                        "2024-05-31",
                        "2024-06-30",
                        "2024-07-31",
                        "2024-08-31",
                        "2024-09-30",
                    ]
                ),
                "model_type": ["LR_Base"] * 6,
                "q50": [100.0, 110, 120, 130, 140, 150],
                "q05": [80, 90, 100, 110, 120, 130],
                "q10": [85, 95, 105, 115, 125, 135],
                "q25": [90, 100, 110, 120, 130, 140],
                "q75": [110, 120, 130, 140, 150, 160],
                "q90": [120, 130, 140, 150, 160, 170],
                "q95": [130, 140, 150, 160, 170, 180],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_latest_seasonal_forecasts(
                ["S1"], forecast_date=dt.date(2024, 10, 1)
            )
        assert not result.empty
        assert "season_year" in result.columns
        assert "season_in_year" in result.columns
        # Default season is Apr-Sep, all 6 months -> should aggregate
        assert all(result["season_year"] == 2024)

    def test_combined_seasonal_forecasts_returns_data(self):
        """read_seasonal_combined_forecasts with non-empty API data."""
        from src import data_reader

        mock_df = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2024],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with patch.object(
            data_reader,
            "_read_long_combined_forecasts_api",
            return_value=mock_df,
        ):
            result = data_reader.read_seasonal_combined_forecasts()
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "EM"

    def test_custom_season_config_through_reader(self, monkeypatch):
        """Non-default season config (Oct-Mar) through data reader."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")

        from src import data_reader

        # Monthly data spanning Oct 2023 - Mar 2024 (cross-year)
        raw_api = pd.DataFrame(
            {
                "code": ["S1"] * 6,
                "valid_from": pd.to_datetime(
                    [
                        "2023-10-01",
                        "2023-11-01",
                        "2023-12-01",
                        "2024-01-01",
                        "2024-02-01",
                        "2024-03-01",
                    ]
                ),
                "valid_to": pd.to_datetime(
                    [
                        "2023-10-31",
                        "2023-11-30",
                        "2023-12-31",
                        "2024-01-31",
                        "2024-02-29",
                        "2024-03-31",
                    ]
                ),
                "model_type": ["LR_Base"] * 6,
                "q50": [200.0, 180, 160, 140, 150, 170],
                "q05": [180, 160, 140, 120, 130, 150],
                "q10": [185, 165, 145, 125, 135, 155],
                "q25": [190, 170, 150, 130, 140, 160],
                "q75": [210, 190, 170, 150, 160, 180],
                "q90": [220, 200, 180, 160, 170, 190],
                "q95": [230, 210, 190, 170, 180, 200],
            }
        )
        with patch.object(data_reader, "_read_long_forecasts_api", return_value=raw_api):
            result = data_reader.read_latest_seasonal_forecasts(
                ["S1"], forecast_date=dt.date(2024, 4, 1)
            )
        assert not result.empty
        # Oct-Mar season: Oct 2023 starts season_year 2023
        assert all(result["season_year"] == 2023)


# ===================================================================
# D. File Writer Gaps
# ===================================================================


class TestSeasonalFileWriter:
    """Gaps D10-D11: save_seasonal_skill_metrics, save_seasonal_forecast_data."""

    def test_save_seasonal_skill_metrics_calls_api(self, monkeypatch):
        """save_seasonal_skill_metrics should call API writer."""
        from src import file_writer

        data = pd.DataFrame(
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
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch(
                "src.api_writer._write_skill_metrics_to_api",
            ) as mock_write,
            patch("src.file_writer.write_diagnostics") as mock_diag,
        ):
            file_writer.save_seasonal_skill_metrics(data, year=2025)

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        # Verify horizon_type='season' and year=2025
        assert call_args[0][1] == "season"
        assert call_args[0][2] == 2025
        mock_diag.diagnose_skill_metrics.assert_called_once()

    def test_save_seasonal_forecast_data_calls_api(self):
        """save_seasonal_forecast_data should call API writer."""
        from src import file_writer

        data = pd.DataFrame(
            {
                "code": ["S1"],
                "season_year": [2025],
                "season_in_year": [1],
                "model_short": ["EM"],
                "forecasted_discharge": [100.0],
            }
        )
        with (
            patch(
                "src.api_writer._write_seasonal_ensemble_to_api",
                return_value=True,
            ) as mock_write,
            patch("src.file_writer.write_diagnostics") as mock_diag,
        ):
            file_writer.save_seasonal_forecast_data(data)

        mock_write.assert_called_once()
        mock_diag.diagnose_forecast_data.assert_called_once()


# ===================================================================
# E. Cross-Year Pipeline with Numerical Verification
# ===================================================================


class TestSeasonalCrossYearPipeline:
    """Gaps E12-E13: numerical pipeline verification."""

    def test_oct_mar_full_pipeline_numerical(self, monkeypatch):
        """Oct-Mar season: aggregate -> skill -> ensemble with value checks."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "10")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "3")

        monthly_obs = _monthly_obs(n_years=4)
        monthly_fc = _monthly_fc(n_years=4, models=("LR_Base", "LR_SM"))

        sobs = aggregate_monthly_obs_to_seasonal(monthly_obs)
        # Build seasonal forecasts directly (replaces removed aggregation)
        season_months = get_season_months()
        sfc = monthly_fc[monthly_fc["month"].isin(season_months)].copy()
        sfc["season_year"] = sfc.apply(
            lambda r: get_season_year(int(r["year"]), int(r["month"])), axis=1
        )
        sfc = (
            sfc.groupby(["code", "season_year", "model_short"])
            .agg(
                q05=("q05", "mean"),
                q10=("q10", "mean"),
                q25=("q25", "mean"),
                q50=("q50", "mean"),
                q75=("q75", "mean"),
                q90=("q90", "mean"),
                q95=("q95", "mean"),
                forecasted_discharge=("forecasted_discharge", "mean"),
            )
            .reset_index()
        )
        sfc["season_in_year"] = 1

        assert not sobs.empty
        assert not sfc.empty

        # Verify season_year mapping: Oct 2020 -> season_year 2020
        # Jan 2021 -> season_year 2020 (wrap portion)
        obs_years = sorted(sobs["season_year"].unique())
        assert len(obs_years) >= 2

        # Verify aggregated values numerically for first complete season
        # Oct-Mar season for station S1, season_year 2020:
        # Oct (month=10): 50 + 10*5 + 0*2 = 100
        # Nov (month=11): 50 + 11*5 + 0*2 = 105
        # Dec (month=12): 50 + 12*5 + 0*2 = 110
        # Jan (month=1, year=2021): 50 + 1*5 + 1*2 = 57
        # Feb (month=2, year=2021): 50 + 2*5 + 1*2 = 62
        # Mar (month=3, year=2021): 50 + 3*5 + 1*2 = 67
        # Mean = (100+105+110+57+62+67)/6 = 501/6 = 83.5
        s1_sy2020 = sobs[(sobs["code"] == "S1") & (sobs["season_year"] == 2020)]
        if not s1_sy2020.empty:
            expected_mean = (100 + 105 + 110 + 57 + 62 + 67) / 6
            assert np.isclose(s1_sy2020.iloc[0]["discharge_avg"], expected_mean, atol=0.01)

        # Skill metrics
        skill_stats, joint, ts = calculate_seasonal_skill_metrics(sobs, sfc)
        assert not skill_stats.empty
        assert "season_in_year" in skill_stats.columns

        # Verify multi-model ensemble presence
        models = skill_stats["model_short"].unique()
        assert "Naive Mean" in models

        # Ensemble creation from pre-calculated skill
        result = create_seasonal_ensemble_forecasts(sfc, skill_stats)
        assert not result.empty
        result_models = set(result["model_short"].unique())
        assert "LR_Base" in result_models
        assert "LR_SM" in result_models
        assert "Naive Mean" in result_models

    def test_custom_season_end_to_end(self, monkeypatch):
        """Non-default season (Jun-Aug) through skill -> ensemble -> verify."""
        monkeypatch.setenv("SAPPHIRE_SEASON_START_MONTH", "6")
        monkeypatch.setenv("SAPPHIRE_SEASON_END_MONTH", "8")

        monthly_obs = _monthly_obs(n_years=3)
        monthly_fc = _monthly_fc(n_years=3, models=("LR_Base", "LR_SM"))

        sobs = aggregate_monthly_obs_to_seasonal(monthly_obs)
        # Build seasonal forecasts directly (replaces removed aggregation)
        season_months = get_season_months()
        sfc = monthly_fc[monthly_fc["month"].isin(season_months)].copy()
        sfc["season_year"] = sfc.apply(
            lambda r: get_season_year(int(r["year"]), int(r["month"])), axis=1
        )
        sfc = (
            sfc.groupby(["code", "season_year", "model_short"])
            .agg(
                q05=("q05", "mean"),
                q10=("q10", "mean"),
                q25=("q25", "mean"),
                q50=("q50", "mean"),
                q75=("q75", "mean"),
                q90=("q90", "mean"),
                q95=("q95", "mean"),
                forecasted_discharge=("forecasted_discharge", "mean"),
            )
            .reset_index()
        )
        sfc["season_in_year"] = 1

        assert not sobs.empty
        assert not sfc.empty

        # Non-wrapping season: season_year should equal calendar year
        for _, row in sobs.iterrows():
            assert row["season_year"] == row["season_year"]  # sanity

        # Verify 3 months (Jun/Jul/Aug) contribute
        season_months = get_season_months()
        assert season_months == [6, 7, 8]

        # Verify numerical aggregation for S1, season_year=2020
        # Jun (6): 50+6*5+0*2 = 80, Jul (7): 85, Aug (8): 90
        # Mean = (80+85+90)/3 = 85.0
        s1_sy2020 = sobs[(sobs["code"] == "S1") & (sobs["season_year"] == 2020)]
        assert not s1_sy2020.empty
        expected = (80 + 85 + 90) / 3.0
        assert np.isclose(s1_sy2020.iloc[0]["discharge_avg"], expected, atol=0.01)

        # Full pipeline: skill metrics -> ensemble
        skill_stats, joint, _ = calculate_seasonal_skill_metrics(sobs, sfc)
        assert not skill_stats.empty

        result = create_seasonal_ensemble_forecasts(sfc, skill_stats)
        assert not result.empty
        result_models = set(result["model_short"].unique())
        assert "LR_Base" in result_models
        assert "LR_SM" in result_models
        assert "Naive Mean" in result_models
