"""M1 P2: per-lead aggregated (quarter/season) skill + ensembles.

Locks the SAPPHIRE_SKILL_LEAD_AWARE flag-ON behavior of
``_calculate_aggregated_skill_metrics`` (skill_metrics.py) and
``_create_aggregated_ensemble_forecasts`` (ensemble_calculator.py): point
metrics, CRPS, EM, Naive Mean, and Skilled Mean must stratify by the
operational lead (``horizon_value``) instead of pooling multiple leads
that share the same target quarter/season into one row.

Before P2, the aggregated path grouped only by (period, code, model),
so two leads forecasting the same target quarter/season were scored as
one inflated n_pairs group and produced one blended EM/Naive/Skilled Mean
row — defeating the #411 min-n floor (a lead with < K pairs could "borrow"
pairs from another lead to cross the floor). See
doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md phase P2.
"""

import logging
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ensemble_calculator import (
    create_quarterly_ensemble_forecasts,
    create_seasonal_ensemble_forecasts,
)
from src.skill_metrics import (
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
)

THRESHOLD_ENV = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",
    "ieasyhydroforecast_nse_threshold": "0.8",
    "ieasyhydroforecast_accuracy_threshold": "0.8",
}

_QCOLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]

_YEARS_5 = [2020, 2021, 2022, 2023, 2024]
_YEARS_3 = [2020, 2021, 2022]

# Target discharge shared by all leads forecasting the same quarter/season —
# the actual outcome doesn't depend on which lead predicted it.
_OBS_5 = [100.0, 110.0, 120.0, 130.0, 140.0]
_OBS_3 = [100.0, 110.0, 120.0]


@pytest.fixture(autouse=True)
def _set_thresholds(monkeypatch):
    for k, v in THRESHOLD_ENV.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def lead_aware_on(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "1")


def _quantile_row(q50, spread=20.0):
    return {
        "q05": q50 - spread,
        "q10": q50 - spread * 0.75,
        "q25": q50 - spread * 0.35,
        "q50": q50,
        "q75": q50 + spread * 0.35,
        "q90": q50 + spread * 0.75,
        "q95": q50 + spread,
    }


# ===================================================================
# Quarterly fixtures: quarter_in_year=1 is the SAME target quarter for
# both leads; horizon_value=1 vs horizon_value=3 distinguishes them.
# LR_BASE is the better (lower-MAE) model at lead 1; LR_SM is the better
# model at lead 3 — this makes "Skilled Mean weights differ by lead"
# observable, not just "EM count differs by lead".
# ===================================================================


def _quarterly_obs(years=_YEARS_5, obs=_OBS_5, code="19999"):
    rows = [(code, y, 1, o) for y, o in zip(years, obs, strict=True)]
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def _quarterly_fcst_rows(years, obs, horizon_value, model_short, offset, code="19999"):
    rows = []
    for y, o in zip(years, obs, strict=True):
        row = {
            "code": code,
            "year": y,
            "quarter_in_year": 1,
            "horizon_value": horizon_value,
            "model_short": model_short,
        }
        row.update(_quantile_row(o + offset))
        rows.append(row)
    return rows


_GOOD_OFFSET = -2.0
_BAD_OFFSET = -8.0  # still NSE>0 at this obs scale, so BOTH models qualify
# for the (relaxed) Skilled Mean pool — only the WEIGHT differs, matching
# the Skilled Mean weighting contract, rather than one model dropping out.


def _two_lead_quarterly_fcst(years_lead1=_YEARS_5, years_lead3=_YEARS_5):
    # Lead 1: LR_BASE close to obs (low MAE), LR_SM further off (high MAE,
    # but still NSE>0).
    rows = _quarterly_fcst_rows(years_lead1, _OBS_5[: len(years_lead1)], 1, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(years_lead1, _OBS_5[: len(years_lead1)], 1, "LR_SM", _BAD_OFFSET)
    # Lead 3: the ranking flips — LR_SM is now the tighter fit — so Skilled
    # Mean weighting is a real per-lead signal, not just per-model.
    rows += _quarterly_fcst_rows(years_lead3, _OBS_5[: len(years_lead3)], 3, "LR_BASE", _BAD_OFFSET)
    rows += _quarterly_fcst_rows(years_lead3, _OBS_5[: len(years_lead3)], 3, "LR_SM", _GOOD_OFFSET)
    return pd.DataFrame(rows)


class TestQuarterlyLeadAwarePointMetricsAndCRPS:
    def test_point_metrics_separate_rows_per_lead(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert "horizon_value" in skill_stats.columns
        assert set(base_rows["horizon_value"]) == {1, 3}
        # Each lead pools its own 5 years — NOT the pooled 10 a
        # lead-blind (period, code, model) group would produce.
        assert set(base_rows["n_pairs"]) == {5}

    def test_point_metrics_values_differ_by_lead(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"].set_index("horizon_value")
        # Lead 1 LR_BASE offset -2 (tight fit); lead 3 LR_BASE offset -8
        # (looser fit) — MAE must reflect that, not an averaged-together
        # value from pooling both leads' pairs into one group.
        assert base_rows.loc[1, "mae"] == pytest.approx(abs(_GOOD_OFFSET), abs=1e-6)
        assert base_rows.loc[3, "mae"] == pytest.approx(abs(_BAD_OFFSET), abs=1e-6)

    def test_crps_present_and_differs_by_lead(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"].set_index("horizon_value")
        assert base_rows.loc[1, "crps"] == pytest.approx(base_rows.loc[1, "crps"])  # not NaN
        assert pd.notna(base_rows.loc[1, "crps"])
        assert pd.notna(base_rows.loc[3, "crps"])
        assert base_rows.loc[1, "crps"] != pytest.approx(base_rows.loc[3, "crps"])


class TestQuarterlyLeadAwareEnsembles:
    def test_em_two_leads_two_rows_in_skill_side(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        em_skill = skill_stats[skill_stats["model_short"] == "EM"]
        em_joint = joint[joint["model_short"] == "EM"]
        assert set(em_skill["horizon_value"]) == {1, 3}
        assert set(em_joint["horizon_value"]) == {1, 3}
        # 5 years x 2 leads = 10 EM forecast rows, not blended into fewer.
        assert len(em_joint) == 10

    def test_naive_mean_two_leads(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        naive_skill = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert set(naive_skill["horizon_value"]) == {1, 3}
        assert set(naive_skill["n_pairs"]) == {5}

    def test_skilled_mean_two_leads_weights_differ(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        sm_joint = joint[joint["model_short"] == "Skilled Mean"].copy()
        assert set(sm_joint["horizon_value"]) == {1, 3}

        # Raw model rows in `joint` don't carry forecasted_discharge (only
        # ensemble rows do); compare against q50, their point forecast.
        base_joint = joint[joint["model_short"] == "LR_BASE"].set_index(["horizon_value", "year"])
        sm_joint_raw = joint[joint["model_short"] == "LR_SM"].set_index(["horizon_value", "year"])

        # Lead 1: LR_BASE has the lower MAE (_GOOD_OFFSET) -> the 1/MAE
        # weighted Skilled Mean must sit closer to LR_BASE's forecast than
        # to LR_SM's, per (lead, year) pair — not just "somewhere between".
        lead1 = sm_joint[sm_joint["horizon_value"] == 1].sort_values("year")
        for _, row in lead1.iterrows():
            key = (1, row["year"])
            dist_to_base = abs(row["forecasted_discharge"] - base_joint.loc[key, "q50"])
            dist_to_sm = abs(row["forecasted_discharge"] - sm_joint_raw.loc[key, "q50"])
            assert dist_to_base < dist_to_sm

        # Lead 3: the ranking flips — LR_SM now has the lower MAE -> Skilled
        # Mean must sit closer to LR_SM's forecast than to LR_BASE's.
        lead3 = sm_joint[sm_joint["horizon_value"] == 3].sort_values("year")
        for _, row in lead3.iterrows():
            key = (3, row["year"])
            dist_to_base = abs(row["forecasted_discharge"] - base_joint.loc[key, "q50"])
            dist_to_sm = abs(row["forecasted_discharge"] - sm_joint_raw.loc[key, "q50"])
            assert dist_to_sm < dist_to_base

    def test_create_quarterly_ensemble_forecasts_per_lead(self, lead_aware_on):
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        # Re-run ensemble creation directly against raw forecasts + the
        # (already per-lead) skill_stats, mirroring the maintenance recalc
        # call path rather than the skill function's internal EM/SM/Naive.
        result = create_quarterly_ensemble_forecasts(fcst, skill_stats)
        for model_short in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == model_short]
            assert set(rows["horizon_value"]) == {1, 3}, model_short


class TestQuarterlyLeadAwareMinNFloorPerLead:
    def test_floor_drops_only_the_thin_lead(self, lead_aware_on):
        """Reproduces the #411-defeat bug: lead 3 alone has 3 pairs (< K=5),
        lead 1 has 5. Pre-P2 pooling would combine them into one 8-pair
        group and incorrectly pass the floor. Post-P2, lead 3 must be
        dropped and lead 1 must survive.
        """
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst(years_lead1=_YEARS_5, years_lead3=_YEARS_3)

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert set(base_rows["horizon_value"]) == {1}
        assert 3 not in set(base_rows["horizon_value"])


class TestQuarterlyLeadAwareFlagOffUnchanged:
    def test_flag_off_still_pools_across_leads(self):
        """Documents the pre-P2 behavior under flag-OFF: this is the bug
        P2 fixes under the flag, but flag-OFF must remain byte-identical
        to trunk (no horizon_value column, single pooled row).
        """
        obs = _quarterly_obs()
        fcst = _two_lead_quarterly_fcst()

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        assert "horizon_value" not in skill_stats.columns
        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        # Pooled: one row, 10 pairs (5 years x 2 leads), not split by lead.
        assert len(base_rows) == 1
        assert int(base_rows.iloc[0]["n_pairs"]) == 10


# ===================================================================
# Seasonal fixtures: season_in_year already carries the lead
# pre-lead-aware (0-3); under the flag, horizon_value must ALSO be
# populated (equal to season_in_year) on every emitted row so the
# schema/merge keys are consistent with quarter/month, per P2.
# ===================================================================


def _seasonal_obs(years=_YEARS_5, obs=_OBS_5, code="19999"):
    rows = [(code, y, o) for y, o in zip(years, obs, strict=True)]
    df = pd.DataFrame(rows, columns=["code", "season_year", "discharge_avg"])
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "delta"]], on=["code"])


def _seasonal_fcst_rows(years, obs, season_in_year, model_short, offset, code="19999"):
    rows = []
    for y, o in zip(years, obs, strict=True):
        row = {
            "code": code,
            "season_year": y,
            "season_in_year": season_in_year,
            # Under SAPPHIRE_SKILL_LEAD_AWARE, the reader (data_reader.py)
            # writes the derived lead into BOTH horizon_value and
            # season_in_year (they carry the same value); simulate that
            # here since this fixture bypasses the reader.
            "horizon_value": season_in_year,
            "date": f"{y}-01-01",
            "model_short": model_short,
        }
        row.update(_quantile_row(o + offset))
        rows.append(row)
    return rows


def _two_lead_seasonal_fcst(years_lead2=_YEARS_5, years_lead3=_YEARS_5):
    # Same _GOOD_OFFSET/_BAD_OFFSET pattern as quarter: both models stay
    # NSE>0 (qualify for the Skilled Mean pool), but the better-fit model
    # flips between lead 2 and lead 3.
    rows = _seasonal_fcst_rows(years_lead2, _OBS_5[: len(years_lead2)], 2, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows(years_lead2, _OBS_5[: len(years_lead2)], 2, "LR_SM", _BAD_OFFSET)
    rows += _seasonal_fcst_rows(years_lead3, _OBS_5[: len(years_lead3)], 3, "LR_BASE", _BAD_OFFSET)
    rows += _seasonal_fcst_rows(years_lead3, _OBS_5[: len(years_lead3)], 3, "LR_SM", _GOOD_OFFSET)
    return pd.DataFrame(rows)


class TestSeasonalLeadAwarePointMetricsAndCRPS:
    def test_point_metrics_carry_horizon_value_matching_season_in_year(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert "horizon_value" in skill_stats.columns
        assert set(base_rows["horizon_value"]) == {2, 3}
        assert (base_rows["horizon_value"] == base_rows["season_in_year"]).all()
        assert set(base_rows["n_pairs"]) == {5}

    def test_crps_present_and_differs_by_lead(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"].set_index("horizon_value")
        assert pd.notna(base_rows.loc[2, "crps"])
        assert pd.notna(base_rows.loc[3, "crps"])
        assert base_rows.loc[2, "crps"] != pytest.approx(base_rows.loc[3, "crps"])


class TestSeasonalLeadAwareEnsembles:
    def test_em_two_leads_two_rows(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)

        em_skill = skill_stats[skill_stats["model_short"] == "EM"]
        em_joint = joint[joint["model_short"] == "EM"]
        assert set(em_skill["horizon_value"]) == {2, 3}
        assert set(em_joint["horizon_value"]) == {2, 3}
        assert len(em_joint) == 10

    def test_naive_mean_two_leads(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        naive_skill = skill_stats[skill_stats["model_short"] == "Naive Mean"]
        assert set(naive_skill["horizon_value"]) == {2, 3}
        assert set(naive_skill["n_pairs"]) == {5}

    def test_skilled_mean_two_leads_weights_differ(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)

        sm_joint = joint[joint["model_short"] == "Skilled Mean"].copy()
        assert set(sm_joint["horizon_value"]) == {2, 3}

        # Raw model rows in `joint` don't carry forecasted_discharge (only
        # ensemble rows do); compare against q50, their point forecast.
        base_joint = joint[joint["model_short"] == "LR_BASE"].set_index(
            ["horizon_value", "season_year"]
        )
        sm_joint_raw = joint[joint["model_short"] == "LR_SM"].set_index(
            ["horizon_value", "season_year"]
        )

        # Lead 2: LR_BASE has the lower MAE -> Skilled Mean must sit closer
        # to LR_BASE's forecast than to LR_SM's, per (lead, year) pair.
        lead2 = sm_joint[sm_joint["horizon_value"] == 2].sort_values("season_year")
        for _, row in lead2.iterrows():
            key = (2, row["season_year"])
            dist_to_base = abs(row["forecasted_discharge"] - base_joint.loc[key, "q50"])
            dist_to_sm = abs(row["forecasted_discharge"] - sm_joint_raw.loc[key, "q50"])
            assert dist_to_base < dist_to_sm

        # Lead 3: the ranking flips — LR_SM now has the lower MAE.
        lead3 = sm_joint[sm_joint["horizon_value"] == 3].sort_values("season_year")
        for _, row in lead3.iterrows():
            key = (3, row["season_year"])
            dist_to_base = abs(row["forecasted_discharge"] - base_joint.loc[key, "q50"])
            dist_to_sm = abs(row["forecasted_discharge"] - sm_joint_raw.loc[key, "q50"])
            assert dist_to_sm < dist_to_base

    def test_create_seasonal_ensemble_forecasts_per_lead(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        result = create_seasonal_ensemble_forecasts(fcst, skill_stats)
        for model_short in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == model_short]
            assert set(rows["horizon_value"]) == {2, 3}, model_short


class TestSeasonalLeadAwareMinNFloorPerLead:
    def test_floor_drops_only_the_thin_lead(self, lead_aware_on):
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst(years_lead2=_YEARS_5, years_lead3=_YEARS_3)

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert set(base_rows["horizon_value"]) == {2}
        assert 3 not in set(base_rows["horizon_value"])


class TestSeasonalLeadAwareFlagOffUnchanged:
    def test_flag_off_still_uses_season_in_year_as_the_lead(self):
        """Flag-OFF: season_in_year already IS the lead pre-lead-aware, so
        rows stay split by season_in_year without a horizon_value column
        (byte-identical to trunk).
        """
        obs = _seasonal_obs()
        fcst = _two_lead_seasonal_fcst()

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        assert "horizon_value" not in skill_stats.columns
        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert set(base_rows["season_in_year"]) == {2, 3}
        assert set(base_rows["n_pairs"]) == {5}


# ===================================================================
# NULL / non-numeric horizon_value handling under flag-ON
# (adversarial-review defects: pandas groupby(dropna=True) would
# silently drop NULL-lead rows -> KeyError on all-NULL CRPS merge, or
# silent skill/ensemble loss on mixed frames). Fix: exclude + WARN
# before the lead-aware groupby, proper empty schema on all-NULL.
# ===================================================================

_NULL = float("nan")

_WARN_FRAGMENT = "legacy NULL-lead"


def _quarterly_fcst_mixed():
    """Lead-1 rows (valid) + rows with a NULL horizon_value (all else valid)."""
    rows = _quarterly_fcst_rows(_YEARS_5, _OBS_5, 1, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, 1, "LR_SM", _BAD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _quarterly_fcst_all_null():
    rows = _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _seasonal_fcst_rows_hv(
    years, obs, season_in_year, horizon_value, model_short, offset, code="19999"
):
    rows = []
    for y, o in zip(years, obs, strict=True):
        row = {
            "code": code,
            "season_year": y,
            "season_in_year": season_in_year,
            "horizon_value": horizon_value,
            "date": f"{y}-01-01",
            "model_short": model_short,
        }
        row.update(_quantile_row(o + offset))
        rows.append(row)
    return rows


def _seasonal_fcst_mixed():
    """Lead-2 rows (valid) + rows with a NULL horizon_value."""
    rows = _seasonal_fcst_rows(_YEARS_5, _OBS_5, 2, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows(_YEARS_5, _OBS_5, 2, "LR_SM", _BAD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _seasonal_fcst_all_null():
    rows = _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


_AGG_SCHEMA_COLS = {"horizon_value", "code", "model_short", "n_pairs", "crps"}


class TestQuarterlyNullLeadSkill:
    def test_mixed_produces_valid_lead_and_warns_skipping_nulls(self, lead_aware_on, caplog):
        obs = _quarterly_obs()
        fcst = _quarterly_fcst_mixed()
        with caplog.at_level(logging.WARNING):
            skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        # Real-lead skill + ensembles produced.
        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert set(base_rows["horizon_value"]) == {1}
        assert set(base_rows["n_pairs"]) == {5}  # NOT 10 (NULL rows not pooled in)
        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert set(skill_stats[skill_stats["model_short"] == ms]["horizon_value"]) == {1}
        # NULL-lead rows warn-skipped, not silently dropped.
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_null_returns_empty_schema_no_crash_and_warns(self, lead_aware_on, caplog):
        obs = _quarterly_obs()
        fcst = _quarterly_fcst_all_null()
        with caplog.at_level(logging.WARNING):
            skill_stats, joint, _ = calculate_quarterly_skill_metrics(obs, fcst)

        # No KeyError; proper empty skill frame carrying the lead-aware schema.
        assert skill_stats.empty
        assert _AGG_SCHEMA_COLS.issubset(set(skill_stats.columns))
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)


class TestSeasonalNullLeadSkill:
    def test_mixed_produces_valid_lead_and_warns_skipping_nulls(self, lead_aware_on, caplog):
        obs = _seasonal_obs()
        fcst = _seasonal_fcst_mixed()
        with caplog.at_level(logging.WARNING):
            skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)

        base_rows = skill_stats[skill_stats["model_short"] == "LR_BASE"]
        assert set(base_rows["horizon_value"]) == {2}
        assert set(base_rows["n_pairs"]) == {5}
        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert set(skill_stats[skill_stats["model_short"] == ms]["horizon_value"]) == {2}
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_null_returns_empty_schema_no_crash_and_warns(self, lead_aware_on, caplog):
        obs = _seasonal_obs()
        fcst = _seasonal_fcst_all_null()
        with caplog.at_level(logging.WARNING):
            skill_stats, joint, _ = calculate_seasonal_skill_metrics(obs, fcst)

        assert skill_stats.empty
        assert _AGG_SCHEMA_COLS.issubset(set(skill_stats.columns))
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)


def _quarterly_skill_two_lead(skill_stats):
    return skill_stats


class TestQuarterlyNullLeadEnsembleCreator:
    def test_mixed_generates_only_valid_lead_ensembles_and_warns(self, lead_aware_on, caplog):
        obs = _quarterly_obs()
        # Build valid per-lead skill from the clean two-lead fixture.
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, _two_lead_quarterly_fcst())
        mixed_fcst = _quarterly_fcst_mixed()  # lead 1 valid + NULL-lead rows

        with caplog.at_level(logging.WARNING):
            result = create_quarterly_ensemble_forecasts(mixed_fcst, skill_stats)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == ms]
            assert set(rows["horizon_value"]) == {1}, ms
        # NULL-lead raw rows preserved as passthrough (not silently dropped).
        raw = result[result["model_short"].isin({"LR_BASE", "LR_SM"})]
        assert raw["horizon_value"].isna().any()
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_null_no_ensembles_no_crash_and_warns(self, lead_aware_on, caplog):
        obs = _quarterly_obs()
        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, _two_lead_quarterly_fcst())
        all_null_fcst = _quarterly_fcst_all_null()

        with caplog.at_level(logging.WARNING):
            result = create_quarterly_ensemble_forecasts(all_null_fcst, skill_stats)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert result[result["model_short"] == ms].empty, ms
        # Raw NULL-lead rows still present (passthrough), no crash.
        assert not result.empty
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)


class TestSeasonalNullLeadEnsembleCreator:
    def test_mixed_generates_only_valid_lead_ensembles_and_warns(self, lead_aware_on, caplog):
        obs = _seasonal_obs()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, _two_lead_seasonal_fcst())
        mixed_fcst = _seasonal_fcst_mixed()

        with caplog.at_level(logging.WARNING):
            result = create_seasonal_ensemble_forecasts(mixed_fcst, skill_stats)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == ms]
            assert set(rows["horizon_value"]) == {2}, ms
        raw = result[result["model_short"].isin({"LR_BASE", "LR_SM"})]
        assert raw["horizon_value"].isna().any()
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_null_no_ensembles_no_crash_and_warns(self, lead_aware_on, caplog):
        obs = _seasonal_obs()
        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, _two_lead_seasonal_fcst())
        all_null_fcst = _seasonal_fcst_all_null()

        with caplog.at_level(logging.WARNING):
            result = create_seasonal_ensemble_forecasts(all_null_fcst, skill_stats)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert result[result["model_short"] == ms].empty, ms
        assert not result.empty
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)


# ===================================================================
# One-sided legacy skill_stats: forecasts carry horizon_value (incl.
# NULL/'bad') but skill_stats is LEGACY 3-key (no horizon_value).
# The grouping still keys on horizon_value (via time_group_cols), so
# null-lead exclusion MUST run regardless of skill_stats' shape, and
# a non-numeric ('bad') lead must NOT generate an ensemble row.
# (adversarial-review MED defect: exclusion was gated on skill_stats
# also carrying the lead.)
# ===================================================================

_BAD = "oops"  # non-numeric horizon_value -> coerces to NaN


def _legacy_quarterly_skill():
    """3-key (quarter_in_year, code, model_short) skill — NO horizon_value."""
    return pd.DataFrame(
        [
            (1, "19999", "LR_BASE", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (1, "19999", "LR_SM", 0.4, 0.90, 5.0, 0.88, 3.0, 10),
        ],
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


def _legacy_seasonal_skill():
    """3-key (season_in_year, code, model_short) skill — NO horizon_value."""
    return pd.DataFrame(
        [
            (2, "19999", "LR_BASE", 0.3, 0.95, 5.0, 0.90, 2.0, 10),
            (2, "19999", "LR_SM", 0.4, 0.90, 5.0, 0.88, 3.0, 10),
        ],
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


def _quarterly_fcst_with_bad_and_null_leads():
    """Valid lead-1 rows + NULL-lead rows + non-numeric ('bad') lead rows."""
    rows = _quarterly_fcst_rows(_YEARS_5, _OBS_5, 1, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, 1, "LR_SM", _BAD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _BAD, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _quarterly_fcst_all_bad_null_leads():
    rows = _quarterly_fcst_rows(_YEARS_5, _OBS_5, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _quarterly_fcst_rows(_YEARS_5, _OBS_5, _BAD, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _seasonal_fcst_with_bad_and_null_leads():
    rows = _seasonal_fcst_rows(_YEARS_5, _OBS_5, 2, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows(_YEARS_5, _OBS_5, 2, "LR_SM", _BAD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _BAD, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _seasonal_fcst_all_bad_null_leads():
    rows = _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _NULL, "LR_BASE", _GOOD_OFFSET)
    rows += _seasonal_fcst_rows_hv(_YEARS_5, _OBS_5, 2, _BAD, "LR_SM", _BAD_OFFSET)
    return pd.DataFrame(rows)


def _ensemble_leads(result):
    """horizon_value set across all three generated ensemble model types."""
    leads = set()
    for ms in ("EM", "Naive Mean", "Skilled Mean"):
        leads |= set(result[result["model_short"] == ms]["horizon_value"].dropna())
    return leads


class TestQuarterlyOneSidedLegacySkill:
    def test_mixed_excludes_null_and_bad_leads_and_warns(self, lead_aware_on, caplog):
        fcst = _quarterly_fcst_with_bad_and_null_leads()
        skill = _legacy_quarterly_skill()  # legacy 3-key, no horizon_value

        with caplog.at_level(logging.WARNING):
            result = create_quarterly_ensemble_forecasts(fcst, skill)

        # Real lead 1 produced across all ensemble types; NO 'bad'/NULL lead.
        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == ms]
            assert set(rows["horizon_value"].dropna()) == {1}, ms
        # A non-numeric lead must never become an ensemble group.
        assert _BAD not in set(result["horizon_value"].astype("object"))
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_bad_null_no_ensembles_no_crash_and_warns(self, lead_aware_on, caplog):
        fcst = _quarterly_fcst_all_bad_null_leads()
        skill = _legacy_quarterly_skill()

        with caplog.at_level(logging.WARNING):
            result = create_quarterly_ensemble_forecasts(fcst, skill)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert result[result["model_short"] == ms].empty, ms
        assert not result.empty  # raw NULL/bad rows still pass through
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)


class TestSeasonalOneSidedLegacySkill:
    def test_mixed_excludes_null_and_bad_leads_and_warns(self, lead_aware_on, caplog):
        fcst = _seasonal_fcst_with_bad_and_null_leads()
        skill = _legacy_seasonal_skill()

        with caplog.at_level(logging.WARNING):
            result = create_seasonal_ensemble_forecasts(fcst, skill)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            rows = result[result["model_short"] == ms]
            assert set(rows["horizon_value"].dropna()) == {2}, ms
        assert _BAD not in set(result["horizon_value"].astype("object"))
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)

    def test_all_bad_null_no_ensembles_no_crash_and_warns(self, lead_aware_on, caplog):
        fcst = _seasonal_fcst_all_bad_null_leads()
        skill = _legacy_seasonal_skill()

        with caplog.at_level(logging.WARNING):
            result = create_seasonal_ensemble_forecasts(fcst, skill)

        for ms in ("EM", "Naive Mean", "Skilled Mean"):
            assert result[result["model_short"] == ms].empty, ms
        assert not result.empty
        assert any(_WARN_FRAGMENT in r.message for r in caplog.records)
