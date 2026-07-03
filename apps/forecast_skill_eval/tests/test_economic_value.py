"""Unit tests for the Relative Economic Value (REV) module (Phase-4, Part B).

All fixtures use synthetic station codes (19999/29999) and invented counts —
no real station codes or discharge values appear anywhere in this file.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from forecast_skill_eval.economic_value import (
    ECONOMIC_VALUE_COLUMNS,
    ECONOMIC_VALUE_SUMMARY_COLUMNS,
    REV_ALPHA_GRID,
    compute_economic_value,
    compute_economic_value_summary,
    rev_curve,
)
from forecast_skill_eval.metrics import add_metrics

# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #

_GROUP_DEFAULTS = {
    "horizon": "pentad",
    "model": "LR",
    "regime": "all",
    "season": "all",
    "code": "19999",
    "basin": "all",
    "norm_provenance": "all",
    "lead": None,
}


def _contingency_row(tp: int, fp: int, fn: int, tn: int, **overrides) -> dict:
    """Build one raw contingency count row (pre-metrics)."""
    row = dict(_GROUP_DEFAULTS)
    row.update(overrides)
    row.update({"TP": tp, "FP": fp, "FN": fn, "TN": tn})
    return row


def _contingency_frame(rows: list[dict], event: str = "below_norm") -> pd.DataFrame:
    """Attach real metric columns (base_rate/pod/pofd/*_undefined) + event."""
    counts = pd.DataFrame(rows)
    counts["n_pairs"] = counts[["TP", "FP", "FN", "TN"]].sum(axis=1)
    frame = add_metrics(counts)
    frame["event"] = event
    return frame


# --------------------------------------------------------------------------- #
# rev_curve primitive
# --------------------------------------------------------------------------- #


def test_rev_curve_hand_computed_golden():
    """Hand-computed V(alpha) for s=0.5, H=0.8, F=0.2."""
    s, H, F = 0.5, 0.8, 0.2
    alphas = np.array([0.3, 0.5, 0.7])
    values, v_max, alpha_star = rev_curve(s, H, F, alphas)

    # V(0.3) = 0.05 / 0.15 = 1/3
    # V(0.5) = 0.15 / 0.25 = 0.6  (= H - F)
    # V(0.7) = 0.05 / 0.15 = 1/3
    np.testing.assert_allclose(values, [1.0 / 3.0, 0.6, 1.0 / 3.0], atol=1e-12)
    assert v_max == pytest.approx(0.6, abs=1e-12)
    assert alpha_star == pytest.approx(0.5, abs=1e-12)


def test_rev_curve_v_max_is_analytic_H_minus_F():
    values, v_max, alpha_star = rev_curve(0.3, 0.9, 0.1, REV_ALPHA_GRID)
    assert v_max == pytest.approx(0.9 - 0.1, abs=1e-12)
    assert alpha_star == pytest.approx(0.3, abs=1e-12)
    # Analytic peak must be >= any discrete grid sample.
    assert v_max + 1e-12 >= np.nanmax(values)


def test_rev_curve_perfect_forecast_is_one_everywhere():
    values, v_max, _ = rev_curve(0.4, 1.0, 0.0, REV_ALPHA_GRID)
    np.testing.assert_allclose(values, 1.0, atol=1e-12)
    assert v_max == pytest.approx(1.0, abs=1e-12)


def test_rev_curve_value_at_alpha_star_equals_v_max():
    s, H, F = 0.37, 0.72, 0.18
    alphas = np.array([s])
    values, v_max, _ = rev_curve(s, H, F, alphas)
    assert values[0] == pytest.approx(v_max, abs=1e-12)


def test_rev_curve_not_clamped_allows_negative():
    # Skill-negative: H < F -> v_max negative, V(s) negative.
    values, v_max, _ = rev_curve(0.5, 0.2, 0.8, np.array([0.5]))
    assert v_max == pytest.approx(-0.6, abs=1e-12)
    assert values[0] < 0.0


def test_rev_curve_nan_inputs_yield_nan_values():
    for s, H, F in ((np.nan, 0.5, 0.2), (0.5, np.nan, 0.2), (0.5, 0.5, np.nan)):
        values, v_max, _ = rev_curve(s, H, F, REV_ALPHA_GRID)
        assert np.isnan(values).all()
        assert np.isnan(v_max)


def test_rev_alpha_grid_is_interior_99_points():
    assert REV_ALPHA_GRID.shape == (99,)
    assert REV_ALPHA_GRID.min() == pytest.approx(0.01)
    assert REV_ALPHA_GRID.max() == pytest.approx(0.99)


# --------------------------------------------------------------------------- #
# compute_economic_value reducer
# --------------------------------------------------------------------------- #


def test_reducer_golden_group_v_max_and_alpha_star():
    # s=0.5, H=0.8, F=0.2: TP=8, FN=2, FP=2, TN=8, N=20 (above min_pairs).
    frame = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    long_df, summary = compute_economic_value(frame)

    assert list(long_df.columns) == list(ECONOMIC_VALUE_COLUMNS)
    assert list(summary.columns) == list(ECONOMIC_VALUE_SUMMARY_COLUMNS)
    assert len(summary) == 1

    row = summary.iloc[0]
    assert row["base_rate_s"] == pytest.approx(0.5)
    assert row["hit_rate_H"] == pytest.approx(0.8)
    assert row["pofd_F"] == pytest.approx(0.2)
    assert row["v_max"] == pytest.approx(0.6, abs=1e-9)
    assert row["alpha_star"] == pytest.approx(0.5, abs=1e-9)
    assert row["n_pairs"] == 20
    assert row["event"] == "below_norm"


def test_reducer_value_at_alpha_star_row_equals_v_max():
    frame = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    long_df, summary = compute_economic_value(frame)
    v_max = summary.iloc[0]["v_max"]

    # alpha_star = s = 0.5 is appended to the grid, so it must be present.
    at_star = long_df[np.isclose(long_df["alpha"], 0.5)]
    assert not at_star.empty
    assert at_star.iloc[0]["value"] == pytest.approx(v_max, abs=1e-9)


def test_reducer_pofd_F_is_pofd_not_far():
    # TP=8, FP=2, FN=2, TN=18: far = 2/10 = 0.2, pofd = 2/20 = 0.1.
    frame = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=18)])
    _, summary = compute_economic_value(frame)
    row = summary.iloc[0]
    assert row["pofd_F"] == pytest.approx(0.1, abs=1e-12)  # = pofd
    assert row["pofd_F"] != pytest.approx(0.2, abs=1e-3)  # != far


def test_reducer_negative_value_preserved():
    # Skill-negative table: TP=2, FN=8, FP=8, TN=2 -> H=0.2, F=0.8.
    frame = _contingency_frame([_contingency_row(tp=2, fp=8, fn=8, tn=2)])
    long_df, summary = compute_economic_value(frame)
    assert summary.iloc[0]["v_max"] == pytest.approx(-0.6, abs=1e-9)
    assert (long_df["value"] < 0.0).any()


def test_reducer_base_rate_undefined_all_normal_obs():
    # No events observed: TP=0, FN=0 -> pod undefined, base_rate = 0 (defined).
    frame = _contingency_frame([_contingency_row(tp=0, fp=5, fn=0, tn=25)])
    long_df, summary = compute_economic_value(frame)
    row = summary.iloc[0]
    assert row["base_rate_s"] == pytest.approx(0.0)
    assert np.isnan(row["v_max"])
    assert long_df["value"].isna().all()
    assert row["n_pairs"] == 30


# --------------------------------------------------------------------------- #
# P1b: below_norm_100 event selection is independent of below_norm
# --------------------------------------------------------------------------- #


def test_below_norm_100_event_selection_is_independent():
    """A contingency frame carrying BOTH events yields the 1.0 rows when asked,
    and the below_norm selection is unaffected by the extra event rows."""
    bn = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)], event="below_norm")
    bn100 = _contingency_frame([_contingency_row(tp=9, fp=1, fn=3, tn=7)], event="below_norm_100")
    combined = pd.concat([bn, bn100], ignore_index=True)

    long_bn, summary_bn = compute_economic_value(combined, event="below_norm")
    long_100, summary_100 = compute_economic_value(combined, event="below_norm_100")

    assert set(long_bn["event"].unique()) == {"below_norm"}
    assert set(long_100["event"].unique()) == {"below_norm_100"}

    # below_norm selection from the combined frame equals a below_norm-only frame.
    long_bn_alone, summary_bn_alone = compute_economic_value(bn, event="below_norm")
    pd.testing.assert_frame_equal(long_bn, long_bn_alone)
    pd.testing.assert_frame_equal(summary_bn, summary_bn_alone)

    # The 1.0 base rate differs (9+3)/20 = 0.6 vs 0.5 for below_norm.
    assert summary_100.iloc[0]["base_rate_s"] == pytest.approx(0.6)


def test_reducer_pofd_undefined_all_below_obs():
    # No non-events: FP=0, TN=0 -> pofd undefined.
    frame = _contingency_frame([_contingency_row(tp=25, fp=0, fn=5, tn=0)])
    long_df, summary = compute_economic_value(frame)
    assert np.isnan(summary.iloc[0]["v_max"])
    assert long_df["value"].isna().all()


def test_reducer_zero_n_row_emitted_with_nan():
    frame = _contingency_frame([_contingency_row(tp=0, fp=0, fn=0, tn=0)])
    long_df, summary = compute_economic_value(frame)
    assert len(summary) == 1
    assert summary.iloc[0]["n_pairs"] == 0
    assert np.isnan(summary.iloc[0]["v_max"])
    assert long_df["value"].isna().all()


def test_reducer_min_pairs_gate_suppresses_thin_group():
    # N=8 < MIN_PAIRS (10): counts recorded, value NaN, not dropped.
    frame = _contingency_frame([_contingency_row(tp=3, fp=1, fn=1, tn=3)])
    long_df, summary = compute_economic_value(frame)
    row = summary.iloc[0]
    assert row["n_pairs"] == 8
    assert row["base_rate_s"] == pytest.approx(0.5)  # counts still recorded
    assert np.isnan(row["v_max"])
    assert long_df["value"].isna().all()


def test_reducer_min_pairs_override():
    frame = _contingency_frame([_contingency_row(tp=3, fp=1, fn=1, tn=3)])
    _, summary = compute_economic_value(frame, min_pairs=4)
    assert not np.isnan(summary.iloc[0]["v_max"])


def test_reducer_n_invariance():
    single = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    doubled = _contingency_frame([_contingency_row(tp=16, fp=4, fn=4, tn=16)])
    long_single, _ = compute_economic_value(single)
    long_doubled, _ = compute_economic_value(doubled)
    np.testing.assert_allclose(
        long_single["value"].to_numpy(dtype=float),
        long_doubled["value"].to_numpy(dtype=float),
        atol=1e-12,
    )


def test_reducer_key_alignment_one_row_per_group():
    frame = _contingency_frame(
        [
            _contingency_row(tp=8, fp=2, fn=2, tn=8, code="19999"),
            _contingency_row(tp=6, fp=3, fn=4, tn=7, code="29999"),
        ]
    )
    _, summary = compute_economic_value(frame)
    # One summary row per below_norm contingency group.
    assert len(summary) == 2
    assert set(summary["code"]) == {"19999", "29999"}


def test_reducer_filters_to_event():
    below = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    other = below.copy()
    other["event"] = "distribution"
    combined = pd.concat([below, other], ignore_index=True)
    _, summary = compute_economic_value(combined)
    assert len(summary) == 1
    assert set(summary["event"]) == {"below_norm"}


def test_reducer_empty_input_returns_empty_frames():
    for frame in (pd.DataFrame(), _contingency_frame([_contingency_row(8, 2, 2, 8)], event="x")):
        long_df, summary = compute_economic_value(frame)
        assert long_df.empty
        assert summary.empty
        assert list(long_df.columns) == list(ECONOMIC_VALUE_COLUMNS)
        assert list(summary.columns) == list(ECONOMIC_VALUE_SUMMARY_COLUMNS)


def test_reducer_no_runtime_warning_on_undefined():
    frame = _contingency_frame([_contingency_row(tp=0, fp=5, fn=0, tn=25)])
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        compute_economic_value(frame)


def test_summary_wrapper_matches_full_reducer():
    frame = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    _, summary = compute_economic_value(frame)
    only_summary = compute_economic_value_summary(frame)
    pd.testing.assert_frame_equal(summary, only_summary)


def test_long_frame_grid_includes_appended_alpha_star():
    frame = _contingency_frame([_contingency_row(tp=8, fp=2, fn=2, tn=8)])
    long_df, _ = compute_economic_value(frame)
    # s=0.5 is already a grid point, so length stays 99.  Craft an off-grid base
    # rate to prove alpha_star is appended: TP+FN=13, N=40 -> s=0.325.
    frame3 = _contingency_frame([_contingency_row(tp=9, fp=4, fn=4, tn=23)])  # s=13/40=0.325
    long3, _ = compute_economic_value(frame3)
    assert np.isclose(long3["alpha"], 0.325).any()
    assert len(long3) == 100  # 99 grid + appended off-grid alpha_star
    assert len(long_df) == 99  # s=0.5 already on grid, no extra row
