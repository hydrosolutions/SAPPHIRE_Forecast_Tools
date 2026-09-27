"""Tests for PP-065 P1a: quarter-only model constants and the same-issue
monthly-triplet derivation helper in ``src/aggregation.py``.

Station code 19999 only (project convention -- never a real station code).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.aggregation import (
    clamp_issue_day,
    derive_quarterly_from_monthly_same_issue,
    filter_calendar_quarter_windows,
)
from src.model_names import (
    AGGREGATED_EM_RAW_MODELS,
    AGGREGATED_ENSEMBLE_MODELS,
    AGGREGATED_SUPPORTED_MODELS,
    QUARTER_NATIVE_RAW_MODELS,
    QUARTER_SUPPORTED_MODELS,
    QUARTERLY_DERIVED_MODELS,
    canonical_model_short,
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

    def test_invalid_config_empty_schema_one_warning_no_exception(self, caplog):
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
        raw = _frame(
            [
                (CODE, "SOME_OTHER_MODEL", "2026-12-25", 1, np.nan, 100.0, None, None),
            ]
        )
        result, counts = derive_quarterly_from_monthly_same_issue(
            raw, lead=1, issue_day=25, models=QUARTERLY_DERIVED_MODELS
        )
        assert result.empty
        assert not counts
