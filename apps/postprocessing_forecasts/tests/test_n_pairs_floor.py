"""Tests for the n_pairs < 2 floor filter in skill metric functions.

All skill rows with n_pairs < 2 must be silently dropped before the frame
is returned to the writer path. Rationale: variance-based metrics
(sdivsigma, NSE via std(ddof=1)) are already NaN at n<2, and per-lead
splitting can produce n_pairs=1 rows whose mae/accuracy are meaningless.

Applies to:
- calculate_monthly_skill_metrics (monthly path)
- calculate_quarterly_skill_metrics (aggregated path, period_col=quarter_in_year)
- calculate_seasonal_skill_metrics (aggregated path, period_col=season_in_year)

The filter must cover ALL rows returned — raw model rows AND aggregated
baselines (EM, Skilled Mean, Naive Mean).

Station code: "19999" throughout (synthetic, no real operational codes).
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    calculate_monthly_skill_metrics,
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
)

# ---------------------------------------------------------------------------
# Helpers shared across all horizons
# ---------------------------------------------------------------------------

STATION = "19999"
MODEL = "LR_Base"
QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]


def _q_row(*q_values):
    """Build a list of 7 quantile values. Accepts a single q50 scalar
    (expanded symmetrically) or an explicit 7-tuple."""
    if len(q_values) == 1:
        q = float(q_values[0])
        return [q * 0.7, q * 0.75, q * 0.85, q, q * 1.15, q * 1.25, q * 1.30]
    assert len(q_values) == 7
    return list(q_values)


# ---------------------------------------------------------------------------
# Monthly helpers
# ---------------------------------------------------------------------------


def _make_monthly_obs(rows):
    """Rows: (code, year, month, discharge_avg). Adds month_in_year and delta."""
    df = pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])
    df["month_in_year"] = df["month"]
    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "month_in_year", "delta"]], on=["code", "month_in_year"])


def _make_monthly_fcst(rows):
    """Rows: (code, year, month, model_short, q50)."""
    records = []
    for code, year, month, model, q50 in rows:
        records.append([code, year, month, model] + _q_row(q50))
    return pd.DataFrame(records, columns=["code", "year", "month", "model_short"] + QUANTILE_COLS)


# ---------------------------------------------------------------------------
# Quarterly helpers
# ---------------------------------------------------------------------------


def _make_quarterly_obs(rows):
    """Rows: (code, year, quarter_in_year, discharge_avg). Adds delta."""
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def _make_quarterly_fcst(rows):
    """Rows: (code, year, quarter_in_year, model_short, q50)."""
    records = []
    for code, year, qiy, model, q50 in rows:
        records.append([code, year, qiy, model] + _q_row(q50))
    return pd.DataFrame(
        records,
        columns=["code", "year", "quarter_in_year", "model_short"] + QUANTILE_COLS,
    )


# ---------------------------------------------------------------------------
# Seasonal helpers
# ---------------------------------------------------------------------------


def _make_seasonal_obs(rows):
    """Rows: (code, season_year, discharge_avg). Adds season_in_year=1, delta."""
    df = pd.DataFrame(rows, columns=["code", "season_year", "discharge_avg"])
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "delta"]], on=["code"])


def _make_seasonal_fcst(rows):
    """Rows: (code, season_year, season_in_year, model_short, q50)."""
    records = []
    for code, sy, siy, model, q50 in rows:
        records.append([code, sy, siy, model] + _q_row(q50))
    return pd.DataFrame(
        records,
        columns=["code", "season_year", "season_in_year", "model_short"] + QUANTILE_COLS,
    )


# ===========================================================================
# Monthly path
# ===========================================================================


class TestMonthlyNPairsFloor:
    """calculate_monthly_skill_metrics drops rows with n_pairs < 2."""

    def test_single_pair_group_is_dropped(self):
        """Group with exactly 1 obs-forecast pair produces no skill row.

        n_pairs=1: sdivsigma/NSE are NaN at n<2, mae/accuracy are
        technically computable but meaningless. Entire row must be dropped.
        """
        obs = _make_monthly_obs([(STATION, 2020, 1, 100.0)])
        fcst = _make_monthly_fcst([(STATION, 2020, 1, MODEL, 102.0)])

        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats.get("code") == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert model_rows.empty, (
            f"Expected no rows for n_pairs=1 group, got {len(model_rows)} row(s): "
            f"{model_rows[['month_in_year', 'code', 'model_short', 'n_pairs']].to_dict('records')}"
        )

    def test_two_pair_group_is_retained(self):
        """Group with n_pairs=2 is kept with correct metric values.

        Two years of monthly obs-forecast pairs: n_pairs must be 2,
        the row must appear in the output.
        """
        obs = _make_monthly_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_monthly_fcst(
            [
                (STATION, 2020, 1, MODEL, 102.0),
                (STATION, 2021, 1, MODEL, 108.0),
            ]
        )

        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats["code"] == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert len(model_rows) == 1, f"Expected 1 row for n_pairs=2 group, got {len(model_rows)}"
        assert model_rows.iloc[0]["n_pairs"] == 2

    def test_mixed_months_drops_single_pair_keeps_double(self):
        """Month 1 has n_pairs=1 (dropped), month 2 has n_pairs=2 (kept).

        Per-group filtering: only the starved group is removed.
        """
        obs = _make_monthly_obs(
            [
                (STATION, 2020, 1, 100.0),  # only 1 year for month 1
                (STATION, 2020, 2, 80.0),
                (STATION, 2021, 2, 85.0),  # 2 years for month 2
            ]
        )
        fcst = _make_monthly_fcst(
            [
                (STATION, 2020, 1, MODEL, 102.0),
                (STATION, 2020, 2, MODEL, 82.0),
                (STATION, 2021, 2, MODEL, 84.0),
            ]
        )

        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        month1_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["month_in_year"] == 1)
        ]
        month2_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["month_in_year"] == 2)
        ]

        assert month1_rows.empty, "Month 1 group has n_pairs=1 — must be dropped"
        assert len(month2_rows) == 1, "Month 2 group has n_pairs=2 — must be retained"
        assert month2_rows.iloc[0]["n_pairs"] == 2

    def test_all_single_pair_returns_empty_skill_stats(self):
        """When every group has n_pairs=1 the output frame is empty.

        Two models, one year each → all raw and baseline rows n_pairs=1.
        No row survives the filter.
        """
        obs = _make_monthly_obs([(STATION, 2020, 1, 100.0)])
        fcst = _make_monthly_fcst(
            [
                (STATION, 2020, 1, "LR_Base", 102.0),
                (STATION, 2020, 1, "LR_SM", 98.0),
            ]
        )

        skill_stats, _, _ = calculate_monthly_skill_metrics(obs, fcst)

        # n_pairs filter applies to baselines too (Naive Mean, EM, Skilled Mean).
        # With only 1 year all groups have n_pairs=1 → entire frame empty.
        assert skill_stats.empty or (
            (skill_stats["n_pairs"] < 2).all() is False  # no n_pairs<2 rows should remain
        )
        # Explicit check: no rows with n_pairs < 2 may survive
        if not skill_stats.empty:
            surviving_bad = skill_stats[skill_stats["n_pairs"].fillna(0) < 2]
            assert surviving_bad.empty, (
                f"Rows with n_pairs<2 survived the floor filter: "
                f"{surviving_bad[['model_short', 'n_pairs']].to_dict('records')}"
            )


# ===========================================================================
# Aggregated (quarterly) path
# ===========================================================================


class TestQuarterlyNPairsFloor:
    """calculate_quarterly_skill_metrics drops rows with n_pairs < 2."""

    def test_single_pair_quarterly_group_is_dropped(self):
        """Quarter group with 1 obs-forecast pair produces no skill row."""
        obs = _make_quarterly_obs([(STATION, 2020, 1, 100.0)])
        fcst = _make_quarterly_fcst([(STATION, 2020, 1, MODEL, 102.0)])

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats["code"] == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert model_rows.empty, (
            f"Expected no rows for n_pairs=1 quarterly group, got {len(model_rows)} row(s)"
        )

    def test_two_pair_quarterly_group_is_retained(self):
        """Quarter group with n_pairs=2 is kept."""
        obs = _make_quarterly_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                (STATION, 2020, 1, MODEL, 102.0),
                (STATION, 2021, 1, MODEL, 108.0),
            ]
        )

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats["code"] == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert len(model_rows) == 1
        assert model_rows.iloc[0]["n_pairs"] == 2

    def test_mixed_quarters_drops_single_pair_keeps_double(self):
        """Q1 with n_pairs=1 dropped, Q2 with n_pairs=2 retained."""
        obs = _make_quarterly_obs(
            [
                (STATION, 2020, 1, 100.0),  # Q1: 1 year → n_pairs=1
                (STATION, 2020, 2, 80.0),
                (STATION, 2021, 2, 85.0),  # Q2: 2 years → n_pairs=2
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                (STATION, 2020, 1, MODEL, 102.0),
                (STATION, 2020, 2, MODEL, 82.0),
                (STATION, 2021, 2, MODEL, 84.0),
            ]
        )

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        q1_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["quarter_in_year"] == 1)
        ]
        q2_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["quarter_in_year"] == 2)
        ]

        assert q1_rows.empty, "Q1 group has n_pairs=1 — must be dropped"
        assert len(q2_rows) == 1, "Q2 group has n_pairs=2 — must be retained"

    def test_no_n_pairs_lt2_rows_survive_quarterly(self):
        """After filter: no row in the output has n_pairs < 2."""
        obs = _make_quarterly_obs(
            [
                (STATION, 2020, 1, 100.0),
                (STATION, 2021, 1, 110.0),
                (STATION, 2020, 2, 80.0),  # Q2: single year
            ]
        )
        fcst = _make_quarterly_fcst(
            [
                (STATION, 2020, 1, "LR_Base", 102.0),
                (STATION, 2021, 1, "LR_Base", 108.0),
                (STATION, 2020, 1, "LR_SM", 104.0),
                (STATION, 2021, 1, "LR_SM", 107.0),
                (STATION, 2020, 2, "LR_Base", 82.0),  # n_pairs=1 for Q2
            ]
        )

        skill_stats, _, _ = calculate_quarterly_skill_metrics(obs, fcst)

        bad_rows = skill_stats[skill_stats["n_pairs"].fillna(0) < 2]
        assert bad_rows.empty, (
            f"Rows with n_pairs<2 survived the floor filter: "
            f"{bad_rows[['quarter_in_year', 'model_short', 'n_pairs']].to_dict('records')}"
        )


# ===========================================================================
# Aggregated (seasonal) path
# ===========================================================================


class TestSeasonalNPairsFloor:
    """calculate_seasonal_skill_metrics drops rows with n_pairs < 2."""

    def test_single_pair_seasonal_group_is_dropped(self):
        """Seasonal group with 1 obs-forecast pair produces no skill row."""
        obs = _make_seasonal_obs([(STATION, 2020, 100.0)])
        fcst = _make_seasonal_fcst([(STATION, 2020, 1, MODEL, 102.0)])

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats["code"] == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert model_rows.empty, (
            f"Expected no rows for n_pairs=1 seasonal group, got {len(model_rows)} row(s)"
        )

    def test_two_pair_seasonal_group_is_retained(self):
        """Seasonal group with n_pairs=2 is kept with metrics unchanged."""
        obs = _make_seasonal_obs(
            [
                (STATION, 2020, 100.0),
                (STATION, 2021, 110.0),
            ]
        )
        fcst = _make_seasonal_fcst(
            [
                (STATION, 2020, 1, MODEL, 102.0),
                (STATION, 2021, 1, MODEL, 108.0),
            ]
        )

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        model_rows = skill_stats[
            (skill_stats["code"] == STATION) & (skill_stats["model_short"] == MODEL)
        ]
        assert len(model_rows) == 1, f"Expected 1 row for n_pairs=2 group, got {len(model_rows)}"
        assert model_rows.iloc[0]["n_pairs"] == 2

    def test_per_lead_seasonal_drops_starved_lead_keeps_full_lead(self):
        """Per-lead (season_in_year) filter: lead with n_pairs=1 dropped, >=2 kept.

        season_in_year acts as the lead dimension:
        - season_in_year=0 (lead 0): 2 years → n_pairs=2, retained
        - season_in_year=1 (lead 1): 1 year  → n_pairs=1, dropped

        This is the primary regression guard for the already-shipped
        per-lead seasonal skill splitting.
        """
        obs = _make_seasonal_obs(
            [
                (STATION, 2020, 100.0),
                (STATION, 2021, 110.0),
            ]
        )
        # Lead 0: both years present → 2 pairs
        # Lead 1: only 2020 present → 1 pair
        fcst = _make_seasonal_fcst(
            [
                (STATION, 2020, 0, MODEL, 102.0),
                (STATION, 2021, 0, MODEL, 108.0),
                (STATION, 2020, 1, MODEL, 99.0),
            ]
        )

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        lead0_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["season_in_year"] == 0)
        ]
        lead1_rows = skill_stats[
            (skill_stats["code"] == STATION)
            & (skill_stats["model_short"] == MODEL)
            & (skill_stats["season_in_year"] == 1)
        ]

        assert len(lead0_rows) == 1, "Lead 0 has n_pairs=2 — must be retained"
        assert lead1_rows.empty, "Lead 1 has n_pairs=1 — must be dropped"
        assert lead0_rows.iloc[0]["n_pairs"] == 2

    def test_no_n_pairs_lt2_rows_survive_seasonal(self):
        """After filter: no skill row in seasonal output has n_pairs < 2."""
        obs = _make_seasonal_obs(
            [
                (STATION, 2020, 100.0),
                (STATION, 2021, 110.0),
                (STATION, 2022, 105.0),
            ]
        )
        # Mix: some season_in_year groups with 3 years, one with 1 year
        fcst = _make_seasonal_fcst(
            [
                (STATION, 2020, 1, "LR_Base", 102.0),
                (STATION, 2021, 1, "LR_Base", 108.0),
                (STATION, 2022, 1, "LR_Base", 104.0),
                (STATION, 2020, 1, "LR_SM", 101.0),
                (STATION, 2021, 1, "LR_SM", 109.0),
                (STATION, 2022, 1, "LR_SM", 103.0),
                (STATION, 2020, 2, "LR_Base", 99.0),  # lead 2: 1 year only → n_pairs=1
            ]
        )

        skill_stats, _, _ = calculate_seasonal_skill_metrics(obs, fcst)

        bad_rows = skill_stats[skill_stats["n_pairs"].fillna(0) < 2]
        assert bad_rows.empty, (
            f"Rows with n_pairs<2 survived the floor filter: "
            f"{bad_rows[['season_in_year', 'model_short', 'n_pairs']].to_dict('records')}"
        )
