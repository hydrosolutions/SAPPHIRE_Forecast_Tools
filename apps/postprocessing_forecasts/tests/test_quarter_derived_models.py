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
    _window_dedup_key,
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
    vectorization rewrite) -- and since round-2/round-3 review, ALSO
    carrying the G1 (value-aware id dedup), G4 (bad_key), G5 (typed object
    columns), H3 (windows compared as parsed local dates), H4
    (deterministic model_short spelling tiebreak), J1 (unparseable
    windows fall back to a raw-value key, not both-NaT) and K1 (that
    fallback key is hashable and type-qualified, not the raw value
    itself) correctness fixes, since those are behavioural guarantees,
    and the differential test is only meaningful if both sides uphold
    them. Kept here ONLY as ground truth for
    ``TestVectorizedMatchesReferenceDifferential`` below. Do NOT "fix"
    this to match the production function when they diverge for a real
    bug -- fix production and this copy will keep it honest. Any
    intentional behaviour change belongs in the production docstring and
    in this frozen copy, applied identically, not a silent edit here.
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
        for c in ("code", "date", "valid_from", "valid_to"):
            if c in result.columns:
                result[c] = result[c].astype("object")
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

    bad_key = df["code"].isna() | df["model_short"].isna()
    n_bad_key = int(bad_key.sum())
    if n_bad_key:
        counts["bad_key"] = n_bad_key
    df = df.loc[~bad_key].copy()
    if df.empty:
        log_counts()
        return empty_result(), counts

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

    # Windows compared as PARSED local calendar dates where they parse
    # (PP-065 H3), not raw strings -- consistent with the amendment's
    # parsing rule everywhere else. Where a value does NOT parse, fall
    # back to a hashable, type-qualified string built from the RAW value,
    # with an actual null kept null (PP-065 J1/K1, see `_window_dedup_key`
    # in src/aggregation.py): two DIFFERENT unparseable strings must not
    # both become NaT and therefore compare equal, and an UNHASHABLE raw
    # value (e.g. a list) must not reach `drop_duplicates` as-is.
    has_valid_from_col = "valid_from" in df.columns
    has_valid_to_col = "valid_to" in df.columns
    if has_valid_from_col:
        df["_vf_parsed"] = local_calendar_date(df["valid_from"])
        df["_vf_dedup_key"] = _window_dedup_key(df["valid_from"], df["_vf_parsed"])
    if has_valid_to_col:
        vt_parsed = local_calendar_date(df["valid_to"])
        df["_vt_dedup_key"] = _window_dedup_key(df["valid_to"], vt_parsed)

    key_cols = ["code", "_canon_model", "_d", "_hv"]
    if has_valid_from_col:
        key_cols.append("_vf_dedup_key")
    if has_valid_to_col:
        key_cols.append("_vt_dedup_key")
    value_cols = []
    if "q" in df.columns:
        df["_dedup_q"] = q_val
        value_cols.append("_dedup_q")
    if "q50" in df.columns:
        df["_dedup_q50"] = q50_val
        value_cols.append("_dedup_q50")

    # Deterministic spelling tiebreak (PP-065 H4): sort by the stored
    # model_short spelling (stable sort) before the exact-duplicate drop,
    # so the lexicographically smallest spelling wins regardless of input
    # row order.
    df = df.sort_values("model_short", kind="stable")

    if "id" in df.columns:
        id_notna = df["id"].notna()
        with_id = df.loc[id_notna].drop_duplicates(subset=[*key_cols, "id", *value_cols]).copy()
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

    if has_valid_from_col:
        df["_vf_year"] = df["_vf_parsed"].dt.year
        df["_vf_month"] = df["_vf_parsed"].dt.month

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

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_same_id_different_values_is_ambiguous_both_orders(self, order):
        # Same id twice with a DIFFERENT value, and no valid_from column at
        # all (PP-065 G1): a same-id pair with a DIFFERENT value is a
        # genuine conflict, not a repeated read -- both rows are kept and
        # reach the uniqueness rule as a group of >= 2, which (with no
        # valid_from column) is unresolvable: ambiguous, counted, in
        # EITHER row order. Deduping on id alone (the pre-G1 behaviour)
        # kept whichever row sorted first, making the output depend on
        # row order -- that is exactly the bug this test guards against.
        columns = ["code", "model_short", "date", "horizon_value", "q", "q50", "id"]
        row_a = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, "same-id-1")
        row_b = (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 999.0, "same-id-1")
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, "id-2"),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, "id-3"),
                # Control.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "gbt-1"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "gbt-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, "gbt-3"),
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

    def test_same_id_same_value_is_exact_duplicate_derives(self):
        # Same id, same value: a genuine repeated read -- collapses to a
        # singleton (PP-065 G1's positive case) and derives normally.
        columns = ["code", "model_short", "date", "horizon_value", "q", "q50", "id"]
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "same-id-1"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "same-id-1"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, "id-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, "id-3"),
            ],
            columns=columns,
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9
        assert "ambiguous_duplicate" not in counts

    # ---- H1: the value check also applies in the NULL-id partition -------

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_null_id_partition_value_check_ambiguous_both_orders(self, order):
        # An `id` column that is ALL None: every row falls into the
        # "without_id" dedup partition. A same-window hv=1 pair with
        # DIFFERENT values (q50 100 vs 999) must NOT collapse there
        # either -- the value check applies in every partition, not just
        # when `id` is absent entirely. Absent a matching valid_from
        # column, the surviving group of 2 is ambiguous.
        columns = ["code", "model_short", "date", "horizon_value", "q", "q50", "id"]
        row_a = (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None)
        row_b = (CODE, "GBT", "2026-12-25", 1, np.nan, 999.0, None)
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None),
            ],
            columns=columns,
        )
        assert raw["id"].isna().all()
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    # ---- H2: the window is part of the exact-duplicate identity ----------

    def test_window_is_part_of_exact_duplicate_identity(self):
        # No id column. Two hv=1 rows with the SAME value (q50=100) but
        # DIFFERENT windows (01-01..01-31 vs 01-02..02-01), both of which
        # parse to January 2027 -- i.e. both MATCH the target month. If
        # the window were not part of the exact-duplicate key, this pair
        # would wrongly collapse to a singleton (same value); since it
        # IS part of the key, they survive as a group of 2, both match,
        # and 2 matches is ambiguous (not exactly 1).
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-02", "2027-02-01"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    def test_valid_to_alone_is_part_of_exact_duplicate_identity(self):
        # No id column. Two hv=1 rows with the SAME value and the SAME
        # valid_from, but DIFFERENT valid_to (01-31 vs 02-01). If
        # valid_to were not part of the key, matching value + valid_from
        # alone would wrongly collapse this to a singleton; since it IS
        # part of the key, they survive as a group of 2 -- both still
        # match the target month via valid_from (valid_to plays no part
        # in the uniqueness rule's OWN matching), so 2 matches is
        # ambiguous (PP-065 K2).
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-02-01"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    # ---- J2: the window is part of the identity in the id-PRESENT --------
    # ---- partitions too (with-id and null-id-with-id-column) -------------

    def test_window_is_part_of_exact_duplicate_identity_with_id(self):
        # Both hv=1 rows share the SAME id and the SAME value, but
        # DIFFERENT windows that both match the target month. The
        # `with_id` dedup subset must still include the window: if it
        # did not, matching id + value alone would wrongly collapse this
        # to a singleton instead of leaving a group of 2 for the
        # uniqueness rule (which then correctly calls it ambiguous, since
        # both windows match the same target month).
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31", "x"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-02", "2027-02-01", "x"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, "id-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, "id-3"),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    def test_valid_to_alone_is_part_of_exact_duplicate_identity_with_id(self):
        # Same id + same value + same valid_from, DIFFERENT valid_to
        # (PP-065 K2): the `with_id` subset must include valid_to too.
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31", "x"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-02-01", "x"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, "id-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, "id-3"),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    def test_window_is_part_of_exact_duplicate_identity_null_id(self):
        # Same as above, but an `id` COLUMN is present and both rows'
        # `id` is None -- routed through the "without_id" (null-id)
        # partition specifically, a distinct code path from having no
        # `id` column at all (H2) even though it uses the same formula.
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31", None),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-02", "2027-02-01", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, None),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        assert raw["id"].isna().all()
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    def test_valid_to_alone_is_part_of_exact_duplicate_identity_null_id(self):
        # Same as the with-id valid_to variant, but both rows' `id` is
        # None -- the "without_id" (null-id) partition (PP-065 K2).
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31", None),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-02-01", None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, None),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        assert raw["id"].isna().all()
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    # ---- H3: windows compared as parsed local dates -----------------------

    def test_exact_duplicate_window_compared_as_parsed_local_date(self):
        # Same value, same (code, model, d, hv); windows differ only in
        # STRING form (naive vs tz-aware for the same local calendar
        # date) -- must be treated as the same window and collapse to a
        # singleton, deriving normally.
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 1.0, "2027-01-01", "2027-01-01"),
                (
                    CODE,
                    "GBT",
                    "2026-12-25",
                    1,
                    np.nan,
                    1.0,
                    "2027-01-01T00:00:00+06:00",
                    "2027-01-01T00:00:00+06:00",
                ),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 2.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 3.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert abs(result.iloc[0]["forecasted_discharge"] - 2.0) < 1e-9
        assert "ambiguous_duplicate" not in counts

    # ---- J1: unparseable windows fall back to the RAW value, not NaT -----

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_unparseable_windows_are_not_treated_as_equal(self, order):
        # Two hv=1 rows with the SAME value but DIFFERENT unparseable
        # valid_from strings ("garbage" vs "xx"): under H3's parsing,
        # both become NaT, but the fix must NOT then treat them as the
        # same window -- the fallback is the RAW value, so "garbage" !=
        # "xx" and the pair is ambiguous (the pre-H3, adf59f77 outcome),
        # not a silent singleton collapse.
        row_a = (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "garbage", None)
        row_b = (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "xx", None)
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    def test_unparseable_window_versus_null_window_not_treated_as_equal(self):
        # "garbage" (unparseable, non-null) vs None (genuinely null) must
        # also not compare equal -- the raw-value fallback only applies
        # to the unparseable side; the null side stays null.
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "garbage", None),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_unparseable_valid_to_not_treated_as_equal(self, order):
        # Same as test_unparseable_windows_are_not_treated_as_equal, but
        # on valid_to instead of valid_from (PP-065 K2): two hv=1 rows
        # with the SAME value but DIFFERENT unparseable valid_to strings
        # must not both become NaT and compare equal.
        row_a = (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, "garbage")
        row_b = (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, "xx")
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert counts["ambiguous_duplicate"] == 1

    # ---- K1: an UNHASHABLE unparseable window must not crash --------------

    def test_unhashable_window_value_no_exception_control_derives(self):
        # A list-valued valid_from (malformed upstream data) is a
        # singleton at its own hv, so it derives too -- the point of this
        # test is that it does NOT raise `TypeError: unhashable type`
        # inside drop_duplicates, next to an independent control triplet.
        raw = _frame(
            [
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, ["garbage"], None),
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
        assert set(result["model_short"]) == {"MC_ALD", "GBT"}
        assert not counts

    # ---- H4: deterministic model_short spelling tiebreak -------------------

    @pytest.mark.parametrize("order", ["a_then_b", "b_then_a"])
    def test_exact_duplicate_spelling_tiebreak_is_deterministic(self, order):
        # Two exact-duplicate rows (same code/canonical-model/d/hv/window/
        # value) differing ONLY in the raw model_short spelling: the
        # surviving spelling must be the lexicographically smallest one,
        # regardless of which row was first in the input.
        row_a = (CODE, "SM_GBT_NORM", "2026-12-25", 1, np.nan, 100.0, None, None)
        row_b = (CODE, "sm_gbt_norm", "2026-12-25", 1, np.nan, 100.0, None, None)
        pair = [row_a, row_b] if order == "a_then_b" else [row_b, row_a]
        raw = _frame(
            pair
            + [
                (CODE, "SM_GBT_NORM", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "SM_GBT_NORM", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "SM_GBT_NORM"
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

    # ---- G2: `code` is part of the grouping key ---------------------------

    def test_two_stations_derive_independently(self):
        # Two fake stations (19999, 19998 -- both project-convention
        # placeholders), same model, same issue date, each with its own
        # complete triplet: `code` MUST be part of the (code, model, d, hv)
        # grouping key, or the two stations' hv=1 rows (etc.) would land
        # in the same group and, with no valid_from column to disambiguate,
        # become ambiguous instead of deriving independently.
        raw = _frame(
            [
                ("19999", "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                ("19999", "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                ("19999", "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
                ("19998", "GBT", "2026-12-25", 1, np.nan, 200.0, None, None),
                ("19998", "GBT", "2026-12-25", 2, np.nan, 210.0, None, None),
                ("19998", "GBT", "2026-12-25", 3, np.nan, 220.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 2
        assert set(result["code"]) == {"19999", "19998"}
        assert not counts

    # ---- G4: null code / model_short (`bad_key`) --------------------------

    def test_null_code_excluded_counted_no_exception_control_derives(self):
        raw = _frame(
            [
                (None, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None),
                (None, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None),
                (None, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None),
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
        assert counts["bad_key"] == 3

    def test_null_model_short_excluded_counted_no_exception(self):
        raw = _frame(
            [
                (CODE, None, "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert counts["bad_key"] == 1

    # ---- G5: `code` output dtype is always object -------------------------

    def test_code_string_dtype_input_output_is_object(self):
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        raw["code"] = raw["code"].astype("string")
        result, _ = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert len(result) == 1
        assert result["code"].dtype == object

        raw_empty_trigger = raw.copy()
        raw_empty_trigger["date"] = "2026-11-25"  # not a quarter start -> empty
        empty_result, _ = derive_quarterly_from_monthly_same_issue(
            raw_empty_trigger, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert empty_result["code"].dtype == object
        assert empty_result["code"].dtype == result["code"].dtype


class TestHandComputedSpecDerivedResults:
    """PP-065 G3: expected results computed by hand against the SPEC (this
    plan's rules), not by calling either implementation -- an oracle
    independent of both production and `_reference_derive`. Values, target
    months/years, and calendar day counts below are asserted as literal
    facts (e.g. "March has 31 days"), never looked up via a helper."""

    @pytest.mark.parametrize(
        "lead,d_str",
        [
            (0, "2027-01-25"),  # d itself is Jan -- no rollover, included for contrast.
            (1, "2026-12-25"),  # the canonical kghm-shaped Dec -> Jan rollover.
            (2, "2026-11-25"),  # Nov -> Jan, two months forward.
            (11, "2026-02-25"),  # Feb -> Jan of the FOLLOWING year, 11 months forward.
        ],
    )
    def test_dec_jan_rollover_across_leads(self, lead, d_str):
        raw = _frame(
            [
                (CODE, "GBT", d_str, lead, np.nan, 100.0, None, None),
                (CODE, "GBT", d_str, lead + 1, np.nan, 110.0, None, None),
                (CODE, "GBT", d_str, lead + 2, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=lead, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert not counts
        assert len(result) == 1
        row = result.iloc[0]
        assert row["code"] == CODE
        assert row["model_short"] == "GBT"
        assert row["year"] == 2027
        assert row["quarter_in_year"] == 1
        assert row["date"] == d_str
        assert row["horizon_value"] == lead
        assert row["valid_from"] == "2027-01-01"
        assert row["valid_to"] == "2027-03-31"  # March has 31 days.
        assert abs(row["forecasted_discharge"] - 110.0) < 1e-9

    def test_feb_29_leap_year_clamp(self):
        # issue_day=31 in Feb of a LEAP year (2028): clamps to 29.
        raw = _frame(
            [
                (CODE, "GBT", "2028-02-29", 2, np.nan, 100.0, None, None),
                (CODE, "GBT", "2028-02-29", 3, np.nan, 110.0, None, None),
                (CODE, "GBT", "2028-02-29", 4, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=2, issue_day=31, models=QUARTERLY_DERIVED_MODELS
        )
        assert not counts
        assert len(result) == 1
        row = result.iloc[0]
        assert row["year"] == 2028
        assert row["quarter_in_year"] == 2
        assert row["date"] == "2028-02-29"
        assert row["valid_from"] == "2028-04-01"
        assert row["valid_to"] == "2028-06-30"  # June has 30 days.
        assert abs(row["forecasted_discharge"] - 110.0) < 1e-9

    def test_june_30_day_month_clamp(self):
        # issue_day=31 in June (30 days): clamps to 30.
        raw = _frame(
            [
                (CODE, "GBT", "2026-06-30", 1, np.nan, 100.0, None, None),
                (CODE, "GBT", "2026-06-30", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-06-30", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=31, models=QUARTERLY_DERIVED_MODELS
        )
        assert not counts
        assert len(result) == 1
        row = result.iloc[0]
        assert row["year"] == 2026
        assert row["quarter_in_year"] == 3
        assert row["date"] == "2026-06-30"
        assert row["valid_from"] == "2026-07-01"
        assert row["valid_to"] == "2026-09-30"  # September has 30 days.
        assert abs(row["forecasted_discharge"] - 110.0) < 1e-9

    def test_same_id_conflict_ambiguous(self):
        raw = _frame(
            [
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 100.0, None, None, "same-id"),
                (CODE, "MC_ALD", "2026-12-25", 1, np.nan, 999.0, None, None, "same-id"),
                (CODE, "MC_ALD", "2026-12-25", 2, np.nan, 110.0, None, None, "id-2"),
                (CODE, "MC_ALD", "2026-12-25", 3, np.nan, 120.0, None, None, "id-3"),
                # Control: a fully independent, unambiguous triplet.
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, None, None, "gbt-1"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None, "gbt-2"),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None, "gbt-3"),
            ],
            columns=FULL_COLUMNS + ["id"],
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert counts["ambiguous_duplicate"] == 1
        assert len(result) == 1
        assert result.iloc[0]["model_short"] == "GBT"
        assert abs(result.iloc[0]["forecasted_discharge"] - 110.0) < 1e-9

    def test_single_match_in_group_of_two_wins(self):
        # hv=1 has two rows: one whose valid_from (year, month) matches
        # its own target (2027, 1) exactly, one that does not -- the
        # matching row's value (100.0) must win, giving mean(100,110,120).
        raw = _frame(
            [
                (CODE, "GBT", "2026-12-25", 1, np.nan, 100.0, "2027-01-01", "2027-01-31"),
                (CODE, "GBT", "2026-12-25", 1, np.nan, 999.0, "2027-02-15", "2027-02-28"),
                (CODE, "GBT", "2026-12-25", 2, np.nan, 110.0, None, None),
                (CODE, "GBT", "2026-12-25", 3, np.nan, 120.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert "ambiguous_duplicate" not in counts
        assert len(result) == 1
        row = result.iloc[0]
        assert row["year"] == 2027
        assert row["quarter_in_year"] == 1
        assert row["valid_from"] == "2027-01-01"
        assert row["valid_to"] == "2027-03-31"
        assert abs(row["forecasted_discharge"] - 110.0) < 1e-9


# =======================================================================
# F3: randomized differential test -- production (vectorized) vs the
# frozen row-wise `_reference_derive` above. The generator draws 1-3 fake
# station codes (19999/19998/19997, PP-065 G2/G3), multiple models, leads
# 0-11, issue days including the ones that only differ from the day-of-
# month via the clamp (29/30/31), and a battery of duplicate/scope/
# missing-column scenarios. Every frame is also run with its rows
# shuffled and a duplicated non-default index, asserting the production
# output is identical (order/index invariance, PP-065 G3) -- this is
# exactly the axis the G1 same-id bug lived on.
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


_FAKE_CODES = ("19999", "19998", "19997")

# Issue days including the exact days that only exist via the clamp
# (PP-065 G3): 29/30/31 all require clamp_issue_day to land on a real day
# in Feb/short months, at least some of the time.
_ISSUE_DAYS = (1, 10, 25, 28, 29, 30, 31)


def _quarter_issue_date(rng, lead: int, issue_day: int) -> pd.Timestamp:
    """A random issue date `d` such that d.month + lead lands on a quarter start.

    `d.day` is `clamp_issue_day(d.year, d.month, issue_day)` -- the SAME
    clamp production applies -- so a large `issue_day` (29/30/31) lands
    on the true schedule date even when `d.month` is Feb or a 30-day
    month, instead of the pre-G3 `min(issue_day, 28)` approximation that
    never actually exercised the clamp end to end.
    """
    year = int(rng.integers(2020, 2031))
    target_month = int(rng.choice([1, 4, 7, 10]))
    # Invert _add_months_vectorized's "months since epoch" formula: find
    # (d_year, d_month) such that d + lead months == (year, target_month),
    # for ANY lead (correctly handles multi-year wraps for lead up to 11+).
    total_target = year * 12 + (target_month - 1)
    total_d = total_target - lead
    d_year = total_d // 12
    d_month = total_d % 12 + 1
    day = clamp_issue_day(d_year, d_month, issue_day)
    return pd.Timestamp(year=d_year, month=d_month, day=day)


def _random_frame_and_params(rng, idx: int):
    lead = int(rng.integers(0, 12))
    issue_day = int(rng.choice(_ISSUE_DAYS))
    models = QUARTERLY_DERIVED_MODELS if rng.random() < 0.5 else QUARTER_NATIVE_RAW_MODELS
    pool = _POOL_DERIVED if models is QUARTERLY_DERIVED_MODELS else _POOL_NATIVE
    n_codes = int(rng.integers(1, 4))
    codes_pool = list(rng.choice(_FAKE_CODES, size=n_codes, replace=False))

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

    def make_row(code, model, d_str, hv, value=100.0, valid_from=None, valid_to=None, rid=None):
        rec = {"code": code, "model_short": model, "date": d_str, "horizon_value": hv}
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
        "single_match_group",
        "bad_key",
    ]
    for _ in range(n_scenarios):
        scenario = rng.choice(scenario_names)
        code = str(rng.choice(codes_pool))
        model = str(rng.choice(pool))
        model_spelled = _spelled(rng, model)
        d = _quarter_issue_date(rng, lead, issue_day)
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
                        code,
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
                        code,
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
                        code,
                        model_spelled,
                        d_str,
                        hv2,
                        value=float(rng.integers(10, 200)),
                        valid_from=vf,
                        valid_to=vf if has_valid_to else None,
                        rid=next_id(),
                    )
                )

        elif scenario == "single_match_group":
            # Deliberately a group of >= 2 with EXACTLY one match (PP-065
            # G3): one row's valid_from matches this row's own target
            # (year, month), the other's does not -- the matching row
            # must win, unambiguously.
            hv = int(rng.choice(leads_needed))
            ty, tm = _add_months(d.year, d.month, hv)
            matching_vf = f"{ty}-{tm:02d}-01" if has_valid_from else None
            oy, om = _add_months(d.year, d.month, hv + 1)
            other_vf = f"{oy}-{om:02d}-01" if has_valid_from else None
            rows.append(
                make_row(
                    code,
                    model_spelled,
                    d_str,
                    hv,
                    value=111.0,
                    valid_from=matching_vf,
                    valid_to=matching_vf if has_valid_to else None,
                    rid=next_id(),
                )
            )
            rows.append(
                make_row(
                    code,
                    model_spelled,
                    d_str,
                    hv,
                    value=222.0,
                    valid_from=other_vf,
                    valid_to=other_vf if has_valid_to else None,
                    rid=next_id(),
                )
            )
            for hv2 in leads_needed:
                if hv2 == hv:
                    continue
                ty2, tm2 = _add_months(d.year, d.month, hv2)
                vf2 = f"{ty2}-{tm2:02d}-01" if has_valid_from else None
                rows.append(
                    make_row(
                        code,
                        model_spelled,
                        d_str,
                        hv2,
                        value=float(rng.integers(10, 200)),
                        valid_from=vf2,
                        valid_to=vf2 if has_valid_to else None,
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
                        code,
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
                            code,
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
                    make_row(
                        code, model_spelled, wrong_d_str, hv, value=float(rng.integers(10, 200))
                    )
                )

        elif scenario == "missing_lead":
            chosen = list(leads_needed)
            rng.shuffle(chosen)
            for hv in chosen[:-1]:
                rows.append(
                    make_row(code, model_spelled, d_str, hv, value=float(rng.integers(10, 200)))
                )

        elif scenario == "non_finite":
            broken_hv = leads_needed[1]
            for hv in leads_needed:
                if hv == broken_hv:
                    rec = make_row(code, model_spelled, d_str, hv, value=np.nan)
                    if has_q:
                        rec["q"] = np.nan
                    if has_q50:
                        rec["q50"] = np.nan
                    rows.append(rec)
                else:
                    rows.append(
                        make_row(code, model_spelled, d_str, hv, value=float(rng.integers(10, 200)))
                    )

        elif scenario == "out_of_range_hv":
            out_of_range = [h for h in range(0, 15) if h not in leads_needed]
            bad_hv_val = int(rng.choice(out_of_range)) if out_of_range else lead + 20
            rows.append(
                make_row(code, model_spelled, d_str, bad_hv_val, value=float(rng.integers(10, 200)))
            )

        elif scenario == "out_of_scope_model":
            other_pool = sorted(
                (QUARTERLY_DERIVED_MODELS | QUARTER_NATIVE_RAW_MODELS) - set(pool)
            ) or ["ZZZ_NOT_A_MODEL"]
            other_model = str(rng.choice(other_pool))
            for hv in leads_needed:
                rows.append(
                    make_row(code, other_model, d_str, hv, value=float(rng.integers(10, 200)))
                )

        elif scenario == "bad_hv":
            hv_val = rng.choice([np.nan, float(lead) + 0.5])
            rows.append(
                make_row(code, model_spelled, d_str, hv_val, value=float(rng.integers(10, 200)))
            )
            for hv in leads_needed:
                if rng.random() < 0.5:
                    rows.append(
                        make_row(code, model_spelled, d_str, hv, value=float(rng.integers(10, 200)))
                    )

        elif scenario == "bad_key":
            # A null code or model_short (PP-065 G4): must be excluded and
            # counted, never reach the groupby that would otherwise crash.
            if rng.random() < 0.5:
                rows.append(
                    make_row(None, model_spelled, d_str, lead, value=float(rng.integers(10, 200)))
                )
            else:
                rows.append(make_row(code, None, d_str, lead, value=float(rng.integers(10, 200))))

        elif scenario == "id_dup_diff_value" and has_id:
            hv = int(rng.choice(leads_needed))
            rid = f"c{idx}-sameid"
            rows.append(make_row(code, model_spelled, d_str, hv, value=100.0, rid=rid))
            rows.append(make_row(code, model_spelled, d_str, hv, value=999.0, rid=rid))
            for hv2 in leads_needed:
                if hv2 != hv:
                    rows.append(
                        make_row(
                            code,
                            model_spelled,
                            d_str,
                            hv2,
                            value=float(rng.integers(10, 200)),
                            rid=f"c{idx}-{hv2}",
                        )
                    )

    if not rows:
        rows.append(make_row(codes_pool[0], pool[0], "2026-12-25", lead, value=100.0))

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

        # Order/index invariance (PP-065 G3): shuffle the row order and
        # assign a non-default, DUPLICATED index, then re-run production
        # on that copy. The output must be byte-identical (sorted, dtypes
        # included) to the unshuffled run. This is exactly the axis G1's
        # bug lived on: `drop_duplicates(keep="first")` on a same-id pair
        # with different values depended on which row happened to be
        # first, so a shuffle could change the result even though nothing
        # about the DATA changed.
        if not frame.empty:
            shuffle_seed = int(rng.integers(0, 1_000_000))
            shuffled = frame.sample(frac=1, random_state=shuffle_seed).copy()
            shuffled.index = np.zeros(len(shuffled), dtype=int)
            shuffled_result, shuffled_counts = derive_quarterly_from_monthly_same_issue(
                shuffled, lead=lead, issue_day=issue_day, models=models
            )
            shuffled_sorted = _sorted_for_compare(shuffled_result)
            pd.testing.assert_frame_equal(prod_sorted, shuffled_sorted, check_dtype=True)
            assert prod_counts == shuffled_counts, (seed_offset, prod_counts, shuffled_counts)
