"""Tests for PP-065 P1a: quarter-only model constants and the same-issue
monthly-triplet derivation helper in ``src/aggregation.py``.

Station code 19999 only (project convention -- never a real station code).
"""

import logging
import os
import sys
import warnings
from collections import Counter

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.aggregation import (
    _FC_QUANTILE_COLS,
    _QUARTER_START_MONTHS,
    MONTH_TO_QUARTER,
    QUARTER_MONTHS,
    _add_months,
    _quarter_end_date,
    clamp_issue_day,
    derive_quarterly_from_monthly_same_issue,
    filter_calendar_quarter_windows,
    local_calendar_date,
)
from src.model_names import (
    AGGREGATED_EM_RAW_MODELS,
    AGGREGATED_ENSEMBLE_MODELS,
    AGGREGATED_SUPPORTED_MODELS,
    QUARTER_NATIVE_RAW_MODELS,
    QUARTER_SUPPORTED_MODELS,
    QUARTERLY_DERIVED_MODELS,
    canonical_model_short,
    canonical_model_short_series,
)

CODE = "19999"

FULL_COLUMNS = [
    "code",
    "model_short",
    "date",
    "horizon_value",
    "q",
    "q50",
    "valid_from",
    "valid_to",
]


def _frame(rows, columns=FULL_COLUMNS):
    """Build a raw monthly frame with an EXPLICIT column list.

    Passing a shorter ``columns`` list is how a test simulates a column
    being entirely ABSENT from the input (not merely null).
    """
    return pd.DataFrame(rows, columns=list(columns))


_reference_logger = logging.getLogger("src.aggregation")


def _reference_derive(
    monthly_raw: pd.DataFrame,
    lead: int,
    issue_day: int,
    models: frozenset,
) -> tuple[pd.DataFrame, dict]:
    """Row-wise reference implementation (PP-065 F3 safety net).

    A FROZEN copy of ``derive_quarterly_from_monthly_same_issue`` as it
    stood right after the F1/F2/F4/F5 fixes (commit-local, before the F3
    vectorization rewrite), kept here ONLY as ground truth for
    ``test_vectorized_matches_reference_differential`` below. Do NOT
    "fix" this to match the production function when they diverge for a
    real bug -- fix production and this copy will keep it honest. Any
    intentional behaviour change belongs in the production docstring and
    in a new frozen copy, not a silent edit here.
    """
    has_q = "q" in monthly_raw.columns
    counts: Counter = Counter()

    _INT_COLS = ("year", "quarter_in_year", "horizon_value")

    def output_columns() -> list:
        cols = [
            "code",
            "model_short",
            "year",
            "quarter_in_year",
            "date",
            "horizon_value",
            "valid_from",
            "valid_to",
            "forecasted_discharge",
        ]
        if has_q:
            cols.append("q")
        cols += list(_FC_QUANTILE_COLS)
        return cols

    def _float_cols() -> list:
        cols = ["forecasted_discharge"]
        if has_q:
            cols.append("q")
        cols += list(_FC_QUANTILE_COLS)
        return cols

    def empty_result() -> pd.DataFrame:
        cols = output_columns()
        float_cols = set(_float_cols())
        data = {}
        for c in cols:
            if c in _INT_COLS:
                data[c] = pd.Series([], dtype="int64")
            elif c in float_cols:
                data[c] = pd.Series([], dtype="float64")
            else:
                data[c] = pd.Series([], dtype="object")
        return pd.DataFrame(data, columns=cols)

    def typed(result: pd.DataFrame) -> pd.DataFrame:
        for c in _INT_COLS:
            result[c] = result[c].astype("int64")
        for c in _float_cols():
            if c in result.columns:
                result[c] = result[c].astype("float64")
        return result

    def log_counts() -> None:
        for key, n in counts.items():
            level = (
                logging.WARNING
                if key in ("invalid_config", "ambiguous_duplicate")
                else logging.INFO
            )
            _reference_logger.log(
                level,
                "_reference_derive: %s=%d (lead=%s, issue_day=%s)",
                key,
                n,
                lead,
                issue_day,
            )

    if issue_day < 1 or lead < 0:
        counts["invalid_config"] += 1
        log_counts()
        return empty_result(), counts

    missing_required = [c for c in ("code", "model_short", "date") if c not in monthly_raw.columns]
    if missing_required:
        for c in missing_required:
            counts[f"missing_column:{c}"] += 1
        log_counts()
        return empty_result(), counts

    if "horizon_value" not in monthly_raw.columns:
        counts["missing_column:horizon_value"] += 1
        log_counts()
        return empty_result(), counts

    df = monthly_raw.copy()
    canon_model = canonical_model_short_series(df["model_short"])
    in_scope = canon_model.isin(models)
    df = df.loc[in_scope].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts
    df["_canon_model"] = canon_model.loc[in_scope]

    hv_numeric = pd.to_numeric(df["horizon_value"], errors="coerce")
    valid_hv = hv_numeric.notna() & np.isfinite(hv_numeric) & hv_numeric.eq(np.round(hv_numeric))
    n_bad_hv = int((~valid_hv).sum())
    if n_bad_hv:
        counts["bad_horizon_value"] = n_bad_hv
    df = df.loc[valid_hv].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts
    df["_hv"] = hv_numeric.loc[valid_hv].round().astype(int)

    leads_needed = (lead, lead + 1, lead + 2)
    df = df.loc[df["_hv"].isin(leads_needed)].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    df["_d"] = local_calendar_date(df["date"])
    bad_date = df["_d"].isna()
    n_bad_date = int(bad_date.sum())
    if n_bad_date:
        counts["bad_date"] = n_bad_date
    df = df.loc[~bad_date].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    target_years, target_months = [], []
    for ts in df["_d"]:
        ty, tm = _add_months(ts.year, ts.month, lead)
        target_years.append(ty)
        target_months.append(tm)
    df["_target_year"] = target_years
    df["_target_month"] = target_months
    df = df.loc[df["_target_month"].isin(_QUARTER_START_MONTHS)].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    df["_expected_day"] = [clamp_issue_day(ts.year, ts.month, issue_day) for ts in df["_d"]]
    wrong_day = df["_d"].dt.day != df["_expected_day"]
    n_wrong_day = int(wrong_day.sum())
    if n_wrong_day:
        counts["wrong_issue_day"] = n_wrong_day
    df = df.loc[~wrong_day].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

    q_val = (
        pd.to_numeric(df["q"], errors="coerce")
        if "q" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    q50_val = (
        pd.to_numeric(df["q50"], errors="coerce")
        if "q50" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    df["_point_value"] = q_val.where(np.isfinite(q_val), q50_val)

    key_cols = ["code", "_canon_model", "_d", "_hv"]
    key_cols += [c for c in ("valid_from", "valid_to") if c in df.columns]
    value_cols = []
    if "q" in df.columns:
        df["_dedup_q"] = q_val
        value_cols.append("_dedup_q")
    if "q50" in df.columns:
        df["_dedup_q50"] = q50_val
        value_cols.append("_dedup_q50")

    if "id" in df.columns:
        id_notna = df["id"].notna()
        with_id = df.loc[id_notna].drop_duplicates(subset=["id"]).copy()
        without_id = df.loc[~id_notna].drop_duplicates(subset=key_cols + value_cols).copy()
        df = pd.concat([with_id, without_id])
    else:
        df = df.drop_duplicates(subset=key_cols + value_cols).copy()
    df = df.drop(columns=[c for c in ("_dedup_q", "_dedup_q50") if c in df.columns])

    row_target_years, row_target_months = [], []
    for ts, hv in zip(df["_d"], df["_hv"], strict=True):
        ry, rm = _add_months(ts.year, ts.month, int(hv))
        row_target_years.append(ry)
        row_target_months.append(rm)
    df["_row_target_year"] = row_target_years
    df["_row_target_month"] = row_target_months

    has_valid_from_col = "valid_from" in df.columns
    if has_valid_from_col:
        vf = local_calendar_date(df["valid_from"])
        df["_vf_year"] = vf.dt.year
        df["_vf_month"] = vf.dt.month

    winners = []
    ambiguous_triplets = set()
    for key, group in df.groupby(["code", "_canon_model", "_d", "_hv"], sort=False):
        triplet_key = key[:3]
        if len(group) == 1:
            winners.append(group.iloc[0])
            continue
        if not has_valid_from_col:
            ambiguous_triplets.add(triplet_key)
            continue
        target_y = group["_row_target_year"].iloc[0]
        target_m = group["_row_target_month"].iloc[0]
        match_mask = (
            group["_vf_year"].notna()
            & group["_vf_year"].eq(target_y)
            & group["_vf_month"].eq(target_m)
        )
        matches = group.loc[match_mask]
        if len(matches) == 1:
            winners.append(matches.iloc[0])
        else:
            ambiguous_triplets.add(triplet_key)

    if ambiguous_triplets:
        counts["ambiguous_duplicate"] = len(ambiguous_triplets)

    if not winners:
        log_counts()
        return empty_result(), counts

    winners_df = pd.DataFrame(winners)
    winners_df["_triplet_key"] = list(
        zip(winners_df["code"], winners_df["_canon_model"], winners_df["_d"], strict=True)
    )
    winners_df = winners_df.loc[~winners_df["_triplet_key"].isin(ambiguous_triplets)].copy()

    rows_out = []
    for _triplet_key, group in winners_df.groupby("_triplet_key", sort=False):
        hv_present = set(group["_hv"])
        if hv_present != set(leads_needed):
            counts["missing_lead"] += 1
            continue

        point_values = {
            hv: group.loc[group["_hv"] == hv, "_point_value"].iloc[0] for hv in leads_needed
        }
        if not all(np.isfinite(v) for v in point_values.values()):
            counts["non_finite_value"] += 1
            continue

        lead_row = group.loc[group["_hv"] == lead].iloc[0]
        year = int(lead_row["_target_year"])
        quarter_in_year = MONTH_TO_QUARTER[int(lead_row["_target_month"])]
        forecasted_discharge = float(np.mean(list(point_values.values())))

        row = {
            "code": lead_row["code"],
            "model_short": lead_row["model_short"],
            "year": year,
            "quarter_in_year": quarter_in_year,
            "date": lead_row["_d"].strftime("%Y-%m-%d"),
            "horizon_value": lead,
            "valid_from": f"{year}-{QUARTER_MONTHS[quarter_in_year][0]:02d}-01",
            "valid_to": _quarter_end_date(year, quarter_in_year),
            "forecasted_discharge": forecasted_discharge,
        }
        if has_q:
            row["q"] = forecasted_discharge
        for qcol in _FC_QUANTILE_COLS:
            row[qcol] = np.nan
        rows_out.append(row)

    log_counts()
    if not rows_out:
        return empty_result(), counts
    return typed(pd.DataFrame(rows_out, columns=output_columns())), counts


# ===================================================================
# 1. Quarter-only model constants
# ===================================================================


class TestQuarterModelConstants:
    def test_native_and_derived_sets_are_fixed_points(self):
        for name in QUARTER_NATIVE_RAW_MODELS | QUARTERLY_DERIVED_MODELS | QUARTER_SUPPORTED_MODELS:
            assert canonical_model_short(name) == name

    def test_native_and_derived_sets_are_disjoint(self):
        assert QUARTER_NATIVE_RAW_MODELS.isdisjoint(QUARTERLY_DERIVED_MODELS)

    def test_quarter_supported_models_is_the_union(self):
        assert QUARTER_SUPPORTED_MODELS == (
            QUARTER_NATIVE_RAW_MODELS | QUARTERLY_DERIVED_MODELS | AGGREGATED_ENSEMBLE_MODELS
        )

    def test_aggregated_em_raw_models_unchanged(self):
        assert frozenset({"LR_BASE", "LR_SM"}) == AGGREGATED_EM_RAW_MODELS

    def test_aggregated_supported_models_unchanged(self):
        assert AGGREGATED_SUPPORTED_MODELS == AGGREGATED_EM_RAW_MODELS | AGGREGATED_ENSEMBLE_MODELS


# ===================================================================
# 14. Clamp helper
# ===================================================================


class TestClampIssueDay:
    def test_31_in_30_day_june(self):
        assert clamp_issue_day(2026, 6, 31) == 30

    def test_31_in_leap_february(self):
        assert clamp_issue_day(2028, 2, 31) == 29

    def test_within_range_unchanged(self):
        assert clamp_issue_day(2026, 1, 25) == 25


# ===================================================================
# Derivation helper
# ===================================================================


class TestDeriveQuarterlyFromMonthlySameIssue:
    # ---- 1. kghm ----------------------------------------------------

    @pytest.mark.parametrize("flag", [None, "true", "false"])
    def test_kghm_derives_q1_2027(self, monkeypatch, flag):
        if flag is None:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        else:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)

        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert row["code"] == CODE
        assert row["model_short"] == "GBT"
        assert row["year"] == 2027
        assert row["quarter_in_year"] == 1
        assert row["date"] == "2026-12-25"
        assert row["horizon_value"] == 1
        assert row["valid_from"] == "2027-01-01"
        assert row["valid_to"] == "2027-03-31"
        assert abs(row["forecasted_discharge"] - 110.0) < 1e-9
        for qcol in ("q05", "q10", "q25", "q50", "q75", "q90", "q95"):
            assert pd.isna(row[qcol])
        assert not counts

    def test_flag_on_and_off_produce_identical_output(self, monkeypatch):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        on_result, on_counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        off_result, off_counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        pd.testing.assert_frame_equal(
            on_result.reset_index(drop=True), off_result.reset_index(drop=True)
        )
        assert on_counts == off_counts

    # ---- 2. tjhm ----------------------------------------------------

    @pytest.mark.parametrize("flag", [None, "true"])
    def test_tjhm_derives_q1_2027_hv0(self, monkeypatch, flag):
        if flag is None:
            monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        else:
            monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)

        raw = _frame(
            [
                (CODE, "GBT", "2027-01-01", 0, np.nan, 50.0, None, None),
                (CODE, "GBT", "2027-01-01", 1, np.nan, 60.0, None, None),
                (CODE, "GBT", "2027-01-01", 2, np.nan, 70.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=0, issue_day=1, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert row["year"] == 2027
        assert row["quarter_in_year"] == 1
        assert row["horizon_value"] == 0
        assert not counts

    # ---- 3. still derived --------------------------------------------

    def test_offset_windows_still_derive_as_singletons(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-02", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "2027-02-01", None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, "2027-03-03", None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert "ambiguous_duplicate" not in counts

    def test_gbt_row_labelled_with_issue_year_still_derives(self):
        raw = _frame(
            [
                # LTF-016: valid_from carries the ISSUE year, not the target year.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2026-01-01", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "2027-02-01", None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "ambiguous_duplicate" not in counts

    @pytest.mark.parametrize("models", [QUARTERLY_DERIVED_MODELS, QUARTER_NATIVE_RAW_MODELS])
    def test_issue_day_31_in_30_day_month_still_derives(self, models):
        model = "GBT" if models is QUARTERLY_DERIVED_MODELS else "LR_BASE"
        raw = _frame(
            [
                (CODE, model, "2026-06-30", 1, np.nan, 100.0, None, None),
                (CODE, model, "2026-06-30", 2, np.nan, 110.0, None, None),
                (CODE, model, "2026-06-30", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=31, models=models
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert row["year"] == 2026
        assert row["quarter_in_year"] == 3
        assert row["model_short"] == model
        assert "wrong_issue_day" not in counts

    # ---- 4. negatives, each with a control ----------------------------

    def test_wrong_issue_day_excluded_control_derives(self):
        raw = _frame(
            [
                # Negative: d.day=24 != clamp(25) -> wrong_issue_day.
                (CODE, "MC_ALD", "2026-12-24", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-24", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-24", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["wrong_issue_day"] == 3

    def test_issue_month_not_quarter_start_ignored_not_counted(self):
        raw = _frame(
            [
                # Negative: d.month=11, +lead(1) = Dec -> not a quarter start.
                (CODE, "MC_ALD", "2026-11-25", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-11-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-11-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert not counts

    def test_missing_lead_excluded_control_derives(self):
        raw = _frame(
            [
                # Negative: only hv 1 and 2 present, hv 3 absent entirely.
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["missing_lead"] == 1

    def test_non_finite_point_value_excluded_control_derives(self):
        raw = _frame(
            [
                # Negative: hv=2 month has both q and q50 NaN.
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, np.nan, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["non_finite_value"] == 1

    def test_positive_q_nan_q50_finite_derives_from_q50(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert not counts

    def test_null_stored_hv_excluded_control_derives(self):
        raw = _frame(
            [
                # Negative: hv is null on one row.
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", np.nan, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["bad_horizon_value"] == 1

    def test_hv_1_5_in_float_column_excluded_control_derives(self):
        raw = _frame(
            [
                # Negative: hv=1.5 in a float64 column -- non-integer.
                (CODE, "MC_ALD", "2026-12-25", 1.5, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 2.0, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3.0, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["bad_horizon_value"] == 1

    def test_ambiguous_duplicate_neither_matches_control_derives(self):
        raw = _frame(
            [
                # Negative: duplicate pair at hv=1, neither valid_from matches
                # the target (2027-01); one is Feb, the other is naT (missing).
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "2027-02-01", None),
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 105.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    # ---- 5. q preferred over q50 --------------------------------------

    def test_q_preferred_over_q50_when_both_finite(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, 100.0, 999.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, 110.0, 999.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, 120.0, 999.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert abs(result.iloc[0]["q"] - 110.0) < 1e-9

    # ---- 6. missing columns, each in its own frame --------------------

    def test_no_horizon_value_column_nothing_derived_counted(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25"),
                (CODE, "GBT", "2026-12-25"),
                (CODE, "GBT", "2026-12-25"),
            ],
            columns=["code", "model_short", "date"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["missing_column:horizon_value"] == 1

    def test_no_q_column_q50_used(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, 100.0, None),
                (CODE, "GBT", "2026-12-25", 2, 110.0, None),
                (CODE, "GBT", "2026-12-25", 3, 120.0, None),
            ],
            columns=["code", "model_short", "date", "horizon_value", "q50", "valid_from"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "q" not in result.columns

    def test_neither_q_nor_q50_column_nothing_derived(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1),
                (CODE, "GBT", "2026-12-25", 2),
                (CODE, "GBT", "2026-12-25", 3),
            ],
            columns=["code", "model_short", "date", "horizon_value"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty

    def test_no_date_column_empty_schema_counted(self):
        raw = _frame(
            [
                (CODE, "GBT", 1),
                (CODE, "GBT", 2),
                (CODE, "GBT", 3),
            ],
            columns=["code", "model_short", "horizon_value"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["missing_column:date"] == 1

    # ---- 7. duplicate pair matching rules -----------------------------

    def test_duplicate_pair_only_one_matches_target_wins(self):
        raw = _frame(
            [
                # hv=1 duplicate pair: only the second matches (2027, 1).
                (CODE, "GBT", "2026-12-25", 1, np.nan, 999.0, "2027-02-15", None),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        # winning value at hv=1 is 100.0 (the matching row), not 999.0.
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "ambiguous_duplicate" not in counts

    def test_duplicate_pair_month_matches_year_mismatch_is_ambiguous(self):
        raw = _frame(
            [
                # hv=1 duplicate pair: row 1 is January but the WRONG year
                # (month matches, year does not -> no match); row 2 is a
                # different month entirely (no match either way). This
                # isolates a year+month check from a month-only check: a
                # month-only check would wrongly accept row 1 as the sole
                # "match" and pick it as the winner instead of flagging
                # the triplet ambiguous.
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "2026-01-05", None),
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 105.0, "2027-03-01", None),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    # ---- 8. two matches, both row orders ------------------------------

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_two_matching_rows_same_target_month_ambiguous_both_orders(self, order):
        row_a = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31")
        row_b = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 200.0, "2027-01-02", "2027-02-01")
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    # ---- 9. model-name contract ----------------------------------------

    def test_input_spelling_sm_gbt_norm_preserved_in_output(self):
        raw = _frame(
            [
                (CODE, "SM_GBT_Norm", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "SM_GBT_Norm", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "SM_GBT_Norm", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "SM_GBT_Norm"

    def test_spelling_variants_group_together_output_uses_lead_row_spelling(self):
        raw = _frame(
            [
                (CODE, "SM_GBT_Norm", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "SM_GBT_NORM", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "SM_GBT_NORM", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        # hv == lead (1) row's own spelling.
        assert result.iloc[0]["model_short"] == "SM_GBT_Norm"

    def test_issue_date_not_quarter_start_derives_nothing(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-11-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-11-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-11-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert not counts

    # ---- 10. mixed tz ---------------------------------------------------

    def test_mixed_tz_date_and_valid_from_no_exception(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25T00:00:00+06:00", 1, np.nan, 100.0, "2027-01-01", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "2027-02-01T00:00:00+06:00", None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9

    # ---- 11. exact duplicates -------------------------------------------

    def test_exact_duplicate_pair_deduped_not_ambiguous(self):
        raw = _frame(
            [
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),  # exact repeat
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 2
        assert set(result["model_short"]) == {"MC_ALD", "GBT"}
        assert "ambiguous_duplicate" not in counts

    # ---- 12. invalid config ---------------------------------------------

    def test_invalid_config_both_invalid_empty_schema_one_warning_no_exception(self, caplog):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
            ]
        )
        with caplog.at_level("WARNING", logger="src.aggregation"):
            result, counts = derive_quarterly_from_monthly_same_issue(
                raw, lead=-1, issue_day=0, models=QUARTERLY_DERIVED_MODELS
            )
        assert result.empty
        assert counts["invalid_config"] == 1
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1

    def test_invalid_config_lead_negative_alone(self, caplog):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
            ]
        )
        with caplog.at_level("WARNING", logger="src.aggregation"):
            result, counts = derive_quarterly_from_monthly_same_issue(
                raw, lead=-1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
            )
        assert result.empty
        assert counts["invalid_config"] == 1
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1

    def test_invalid_config_issue_day_zero_alone(self, caplog):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
            ]
        )
        with caplog.at_level("WARNING", logger="src.aggregation"):
            result, counts = derive_quarterly_from_monthly_same_issue(
                raw, lead=1, issue_day=0, models=QUARTERLY_DERIVED_MODELS
            )
        assert result.empty
        assert counts["invalid_config"] == 1
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1

    # ---- 13. output passes filter_calendar_quarter_windows --------------

    def test_output_passes_filter_calendar_quarter_windows(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        kept, dropped = filter_calendar_quarter_windows(result)
        assert dropped == 0
        assert len(kept) == len(result)

    # ---- Empty output schema shape --------------------------------------

    def test_empty_output_has_q_column_only_when_input_has_q(self):
        raw_with_q = _frame([(CODE, "GBT", "2026-11-25", 1, np.nan, 100.0, None, None)])
        result, _ = derive_quarterly_from_monthly_same_issue(
            raw_with_q, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert "q" in result.columns

        raw_without_q = _frame(
            [(CODE, "GBT", "2026-11-25", 1, 100.0)],
            columns=["code", "model_short", "date", "horizon_value", "q50"],
        )
        result2, _ = derive_quarterly_from_monthly_same_issue(
            raw_without_q, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert "q" not in result2.columns

    def test_no_exception_on_out_of_scope_model(self):
        # A COMPLETE triplet (all 3 leads, otherwise fully eligible) for a
        # model that is simply not in `models`: it must derive NOTHING,
        # not merely "derive nothing because the triplet is incomplete" --
        # a test with only 1 row would pass for the wrong reason (missing
        # leads) even if the model-scope filter were broken.
        raw = _frame(
            [
                (CODE, "SOME_OTHER_MODEL", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "SOME_OTHER_MODEL", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "SOME_OTHER_MODEL", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert not counts

    # ---- F1: exact-duplicate pre-step is value-aware ---------------------

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_null_valid_from_pair_different_values_is_ambiguous(self, order):
        row_a = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None)
        row_b = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 200.0, None, None)
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_no_valid_from_column_pair_different_values_is_ambiguous(self, order):
        columns = ["code", "model_short", "date", "horizon_value", "q", "q50"]
        row_a = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0)
        row_b = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 200.0)
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0),
            ],
            columns=columns,
        )
        assert "valid_from" not in raw.columns
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_identical_window_pair_different_values_is_ambiguous(self, order):
        # Same (code, model, d, hv) AND same valid_from/valid_to window,
        # but a different value: the window match can no longer break the
        # tie (both rows either match or don't, together), so this must be
        # ambiguous, not an exact-duplicate collapse to whichever row is
        # first.
        row_a = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31")
        row_b = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 200.0, "2027-01-01", "2027-01-31")
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert counts["ambiguous_duplicate"] == 1

    # ---- F2: `id`-based dedup only among non-null ids ---------------------

    def test_all_null_ids_derive_via_key_value_rule(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None, np.nan),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, np.nan),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, np.nan),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "missing_lead" not in counts

    def test_partly_null_id_column_two_models_both_derive(self):
        raw = _frame(
            [
                # Model A: valid, distinct ids.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None, "id-a1"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, "id-a2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, "id-a3"),
                # Model B: all-null ids.
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 200.0, None, None, np.nan),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 210.0, None, None, np.nan),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 220.0, None, None, np.nan),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 2
        assert set(result["model_short"]) == {"GBT", "MC_ALD"}
        assert "missing_lead" not in counts
        assert "ambiguous_duplicate" not in counts

    def test_id_dedup_ignores_value_divergence(self):
        # Same id twice with a DIFFERENT value, and no valid_from column at
        # all: the id rule must collapse this to ONE row regardless of the
        # value mismatch (a repeated read, by construction, PP-065 F2).
        # Without the id branch, the key+value rule would NOT collapse
        # these (values differ), and -- with no valid_from column present
        # -- the pair would become ambiguous instead of deriving.
        columns = ["code", "model_short", "date", "horizon_value", "q", "q50", "id"]
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "same-id-1"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 999.0, "same-id-1"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "id-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, "id-3"),
            ],
            columns=columns,
        )
        assert "valid_from" not in raw.columns
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "ambiguous_duplicate" not in counts

    # ---- F5: out-of-scope rows are dropped before any counted check ------

    def test_out_of_range_hv_row_ignored_not_counted(self):
        # A different monthly mode's row (e.g. kghm's day-10 month_0: hv=0
        # when this call's lead is 1), whose issue month + lead HAPPENS to
        # land on a quarter start and whose day does NOT match this call's
        # issue_day. Before F5, hv-range was checked after wrong_issue_day,
        # so this row was wrongly counted as wrong_issue_day; it must now
        # be silently out of scope (hv=0 not in {1,2,3}) and never reach
        # the issue-day check at all.
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-10", 0, np.nan, 100.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts == {}

    # ---- F4: typed empty schema -------------------------------------------

    def test_empty_and_nonempty_result_dtypes_match(self):
        raw_empty = _frame(
            [(CODE, "GBT", "2026-11-25", 1, np.nan, 100.0, None, None)]
        )  # not a quarter start -> empty
        empty_result, _ = derive_quarterly_from_monthly_same_issue(
            raw_empty, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        raw_full = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        full_result, _ = derive_quarterly_from_monthly_same_issue(
            raw_full, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert empty_result.empty
        assert not full_result.empty
        assert list(empty_result.columns) == list(full_result.columns)
        for col in empty_result.columns:
            assert empty_result[col].dtype == full_result[col].dtype, col
        for col in ("year", "quarter_in_year", "horizon_value"):
            assert full_result[col].dtype == np.dtype("int64")
        for col in ("forecasted_discharge", "q", "q05", "q10", "q25", "q50", "q75", "q90", "q95"):
            assert full_result[col].dtype == np.dtype("float64")

    def test_concat_empty_derived_with_typed_direct_frame_keeps_int64_no_warning(self):
        raw_empty = _frame(
            [(CODE, "GBT", "2026-11-25", 1, np.nan, 100.0, None, None)]
        )  # not a quarter start -> empty
        empty_result, _ = derive_quarterly_from_monthly_same_issue(
            raw_empty, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        direct = pd.DataFrame(
            {
                "code": pd.Series(["19999"], dtype="object"),
                "model_short": pd.Series(["LR_BASE"], dtype="object"),
                "year": pd.Series([2027], dtype="int64"),
                "quarter_in_year": pd.Series([1], dtype="int64"),
                "date": pd.Series(["2026-12-25"], dtype="object"),
                "horizon_value": pd.Series([1], dtype="int64"),
                "valid_from": pd.Series(["2027-01-01"], dtype="object"),
                "valid_to": pd.Series(["2027-03-31"], dtype="object"),
                "forecasted_discharge": pd.Series([110.0], dtype="float64"),
                "q": pd.Series([110.0], dtype="float64"),
                "q05": pd.Series([np.nan], dtype="float64"),
                "q10": pd.Series([np.nan], dtype="float64"),
                "q25": pd.Series([np.nan], dtype="float64"),
                "q50": pd.Series([np.nan], dtype="float64"),
                "q75": pd.Series([np.nan], dtype="float64"),
                "q90": pd.Series([np.nan], dtype="float64"),
                "q95": pd.Series([np.nan], dtype="float64"),
            }
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            combined = pd.concat([direct, empty_result], ignore_index=True)
        assert combined["year"].dtype == np.dtype("int64")
        assert len(combined) == 1

    # ---- F6(b): ambiguous_duplicate WARNING, no station code -------------

    def test_ambiguous_duplicate_warning_has_no_station_code(self, caplog):
        raw = _frame(
            [
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "2027-02-01", None),
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 105.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        agg_logger = logging.getLogger("src.aggregation")
        assert agg_logger.propagate is True
        with caplog.at_level("INFO", logger="src.aggregation"):
            result, counts = derive_quarterly_from_monthly_same_issue(
                raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
            )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1
        ambiguous_records = [r for r in caplog.records if "ambiguous_duplicate" in r.getMessage()]
        assert len(ambiguous_records) == 1
        assert ambiguous_records[0].levelname == "WARNING"
        assert CODE not in ambiguous_records[0].getMessage()


# =======================================================================
# F3: randomized differential test -- production (vectorized) vs the
# frozen row-wise `_reference_derive` above. Station code 19999 only
# (this generator never invents a second code, per the module convention;
# model/date/duplicate/column-presence variety carries the coverage
# burden instead).
# =======================================================================

_POOL_DERIVED = sorted(QUARTERLY_DERIVED_MODELS)
_POOL_NATIVE = sorted(QUARTER_NATIVE_RAW_MODELS)


def _spelled(rng, canonical: str) -> str:
    """Return a random case/spacing variant of a canonical model name."""
    r = rng.random()
    if r < 0.25:
        return canonical.lower()
    if r < 0.5:
        return canonical.title().replace("_", "_")
    return canonical


def _tz_date_str(rng, ts: pd.Timestamp) -> str:
    if rng.random() < 0.3:
        return ts.strftime("%Y-%m-%dT00:00:00+06:00")
    return ts.strftime("%Y-%m-%d")


def _quarter_issue_date(rng, lead: int) -> pd.Timestamp:
    """A random issue date `d` such that d.month + lead lands on a quarter start."""
    year = int(rng.integers(2020, 2031))
    target_month = int(rng.choice([1, 4, 7, 10]))
    dm_total = target_month - 1 - lead
    d_month = dm_total % 12 + 1
    d_year = year - 1 if dm_total < 0 else year
    day = 15  # scenario builders below adjust the day where the scenario needs to
    return pd.Timestamp(year=d_year, month=d_month, day=day)


def _random_frame_and_params(rng, idx: int):
    lead = int(rng.integers(0, 3))
    issue_day = int(rng.choice([1, 10, 25, 28]))
    models = QUARTERLY_DERIVED_MODELS if rng.random() < 0.5 else QUARTER_NATIVE_RAW_MODELS
    pool = _POOL_DERIVED if models is QUARTERLY_DERIVED_MODELS else _POOL_NATIVE

    has_q = rng.random() < 0.7
    has_q50 = (rng.random() < 0.85) or not has_q
    has_valid_from = rng.random() < 0.8
    has_valid_to = has_valid_from and rng.random() < 0.9
    has_id = rng.random() < 0.4

    columns = ["code", "model_short", "date", "horizon_value"]
    if has_q:
        columns.append("q")
    if has_q50:
        columns.append("q50")
    if has_valid_from:
        columns.append("valid_from")
    if has_valid_to:
        columns.append("valid_to")
    if has_id:
        columns.append("id")

    def make_row(model, d_str, hv, value=100.0, valid_from=None, valid_to=None, rid=None):
        rec = {"code": CODE, "model_short": model, "date": d_str, "horizon_value": hv}
        if has_q:
            rec["q"] = value if rng.random() < 0.6 else np.nan
        if has_q50:
            rec["q50"] = value
        if has_valid_from:
            rec["valid_from"] = valid_from
        if has_valid_to:
            rec["valid_to"] = valid_to
        if has_id:
            rec["id"] = rid
        return rec

    leads_needed = (lead, lead + 1, lead + 2)
    rows = []
    counter = 0

    def next_id():
        nonlocal counter
        counter += 1
        return f"c{idx}-{counter}"

    n_scenarios = int(rng.integers(4, 9))
    scenario_names = [
        "clean",
        "ambiguous_offset",
        "exact_dup",
        "wrong_day",
        "missing_lead",
        "non_finite",
        "out_of_range_hv",
        "out_of_scope_model",
        "bad_hv",
        "id_dup_diff_value",
    ]
    for _ in range(n_scenarios):
        scenario = rng.choice(scenario_names)
        model = str(rng.choice(pool))
        model_spelled = _spelled(rng, model)
        d = _quarter_issue_date(rng, lead)
        d = d.replace(day=min(issue_day, 28))
        d_str = _tz_date_str(rng, d)

        if scenario == "clean":
            for hv in leads_needed:
                vf = vt = None
                if has_valid_from:
                    ty, tm = _add_months(d.year, d.month, hv)
                    day = 1 if rng.random() < 0.7 else int(rng.integers(2, 5))
                    vf = f"{ty}-{tm:02d}-{day:02d}"
                    vt = vf if has_valid_to else None
                rows.append(
                    make_row(
                        model_spelled,
                        d_str,
                        hv,
                        value=float(rng.integers(10, 200)),
                        valid_from=vf,
                        valid_to=vt,
                        rid=next_id(),
                    )
                )

        elif scenario == "ambiguous_offset":
            hv = int(rng.choice(leads_needed))
            for _k in range(2):
                by, bm = _add_months(d.year, d.month, hv + int(rng.choice([1, -1, 2])))
                vf = f"{by}-{bm:02d}-01" if has_valid_from else None
                rows.append(
                    make_row(
                        model_spelled,
                        d_str,
                        hv,
                        value=float(rng.integers(10, 200)),
                        valid_from=vf,
                        valid_to=vf if has_valid_to else None,
                        rid=next_id(),
                    )
                )
            for hv2 in leads_needed:
                if hv2 == hv:
                    continue
                ty, tm = _add_months(d.year, d.month, hv2)
                vf = f"{ty}-{tm:02d}-01" if has_valid_from else None
                rows.append(
                    make_row(
                        model_spelled,
                        d_str,
                        hv2,
                        value=float(rng.integers(10, 200)),
                        valid_from=vf,
                        valid_to=vf if has_valid_to else None,
                        rid=next_id(),
                    )
                )

        elif scenario == "exact_dup":
            for hv in leads_needed:
                ty, tm = _add_months(d.year, d.month, hv)
                vf = f"{ty}-{tm:02d}-01" if has_valid_from else None
                value = float(rng.integers(10, 200))
                rid = f"c{idx}-dup-{hv}"
                rows.append(
                    make_row(
                        model_spelled,
                        d_str,
                        hv,
                        value=value,
                        valid_from=vf,
                        valid_to=vf if has_valid_to else None,
                        rid=rid,
                    )
                )
                if hv == leads_needed[0]:
                    rows.append(
                        make_row(
                            model_spelled,
                            d_str,
                            hv,
                            value=value,
                            valid_from=vf,
                            valid_to=vf if has_valid_to else None,
                            rid=rid,
                        )
                    )

        elif scenario == "wrong_day":
            wrong_d = d + pd.Timedelta(days=3)
            wrong_d_str = _tz_date_str(rng, wrong_d)
            for hv in leads_needed:
                rows.append(
                    make_row(model_spelled, wrong_d_str, hv, value=float(rng.integers(10, 200)))
                )

        elif scenario == "missing_lead":
            chosen = list(leads_needed)
            rng.shuffle(chosen)
            for hv in chosen[:-1]:
                rows.append(make_row(model_spelled, d_str, hv, value=float(rng.integers(10, 200))))

        elif scenario == "non_finite":
            broken_hv = leads_needed[1]
            for hv in leads_needed:
                if hv == broken_hv:
                    rec = make_row(model_spelled, d_str, hv, value=np.nan)
                    if has_q:
                        rec["q"] = np.nan
                    if has_q50:
                        rec["q50"] = np.nan
                    rows.append(rec)
                else:
                    rows.append(
                        make_row(model_spelled, d_str, hv, value=float(rng.integers(10, 200)))
                    )

        elif scenario == "out_of_range_hv":
            out_of_range = [h for h in range(0, 6) if h not in leads_needed]
            bad_hv_val = int(rng.choice(out_of_range)) if out_of_range else lead + 5
            rows.append(
                make_row(model_spelled, d_str, bad_hv_val, value=float(rng.integers(10, 200)))
            )

        elif scenario == "out_of_scope_model":
            other_pool = sorted(
                (QUARTERLY_DERIVED_MODELS | QUARTER_NATIVE_RAW_MODELS) - set(pool)
            ) or ["ZZZ_NOT_A_MODEL"]
            other_model = str(rng.choice(other_pool))
            for hv in leads_needed:
                rows.append(make_row(other_model, d_str, hv, value=float(rng.integers(10, 200))))

        elif scenario == "bad_hv":
            hv_val = rng.choice([np.nan, float(lead) + 0.5])
            rows.append(make_row(model_spelled, d_str, hv_val, value=float(rng.integers(10, 200))))
            for hv in leads_needed:
                if rng.random() < 0.5:
                    rows.append(
                        make_row(model_spelled, d_str, hv, value=float(rng.integers(10, 200)))
                    )

        elif scenario == "id_dup_diff_value" and has_id:
            hv = int(rng.choice(leads_needed))
            rid = f"c{idx}-sameid"
            rows.append(make_row(model_spelled, d_str, hv, value=100.0, rid=rid))
            rows.append(make_row(model_spelled, d_str, hv, value=999.0, rid=rid))
            for hv2 in leads_needed:
                if hv2 != hv:
                    rows.append(
                        make_row(
                            model_spelled,
                            d_str,
                            hv2,
                            value=float(rng.integers(10, 200)),
                            rid=f"c{idx}-{hv2}",
                        )
                    )

    if not rows:
        rows.append(make_row(pool[0], "2026-12-25", lead, value=100.0))

    frame = pd.DataFrame(rows, columns=columns)
    return frame, lead, issue_day, models


def _sorted_for_compare(result: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [
        c for c in ("code", "model_short", "year", "quarter_in_year") if c in result.columns
    ]
    if not sort_cols or result.empty:
        return result.reset_index(drop=True)
    return result.sort_values(sort_cols).reset_index(drop=True)


class TestVectorizedMatchesReferenceDifferential:
    """PP-065 F3: the production (vectorized) implementation must match
    the frozen row-wise `_reference_derive` exactly, sorted output and
    dtypes included, across a wide random sample of inputs."""

    @pytest.mark.parametrize("seed_offset", range(300))
    def test_matches_reference(self, seed_offset):
        rng = np.random.default_rng(20260927 + seed_offset)
        frame, lead, issue_day, models = _random_frame_and_params(rng, seed_offset)

        prod_result, prod_counts = derive_quarterly_from_monthly_same_issue(
            frame, lead=lead, issue_day=issue_day, models=models
        )
        ref_result, ref_counts = _reference_derive(
            frame, lead=lead, issue_day=issue_day, models=models
        )

        prod_sorted = _sorted_for_compare(prod_result)
        ref_sorted = _sorted_for_compare(ref_result)
        pd.testing.assert_frame_equal(prod_sorted, ref_sorted, check_dtype=True)
        assert prod_counts == ref_counts, (seed_offset, prod_counts, ref_counts)
