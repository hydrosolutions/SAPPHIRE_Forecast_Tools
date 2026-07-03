"""Tests for fill_forcing_gaps() and _interpolate_with_gap_limits() in
utils_ml_forecast.py.

Covers:
1. 1-day gap inside recent window IS filled (gap_limit_recent=1).
2. 2-day consecutive gap inside recent window stays NaN (exceeds gap_limit_recent=1).
3. 3-day consecutive gap in past region IS filled (gap_limit_past=3).
4. 4-day consecutive gap in past region stays NaN (exceeds gap_limit_past=3).
5. Missing DATES (dropped rows) are reindexed and short gap is filled.
6. Leading and trailing NaNs are NOT extrapolated.
7. Two different codes are handled independently.
8. Both P and T columns are interpolated.
9. Lenient-everywhere: when gap_limit_recent == gap_limit_past, gaps filled uniformly.
"""

import os
import sys

# Mock heavy dependencies before importing from scr
from unittest.mock import MagicMock

sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.utils.likelihood_models"] = MagicMock()
sys.modules["darts.utils.likelihood_models.base"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["torch.optim"] = MagicMock()
sys.modules["torch.optim.lr_scheduler"] = MagicMock()
sys.modules["torch.nn"] = MagicMock()
sys.modules["torch.nn.modules"] = MagicMock()
sys.modules["torch.nn.modules.loss"] = MagicMock()
sys.modules["torch.serialization"] = MagicMock()
sys.modules["torchmetrics"] = MagicMock()
sys.modules["torchmetrics.collections"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()

# Add module root and scr to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

import numpy as np
import pandas as pd

from scr.utils_ml_forecast import fill_forcing_gaps  # noqa: E402

# Fixed reference date used by all tests — never use real "today"
REF_DATE = pd.Timestamp("2024-06-15")
RECENT_DAY_THRESHOLD = 7  # days back that count as "recent"
GAP_LIMIT_RECENT = 1  # at most 1 consecutive day may be filled in recent window
GAP_LIMIT_PAST = 3  # at most 3 consecutive days may be filled in older data


def _make_df(code, dates, P_values, T_values):
    """Helper: build a minimal long-format forcing DataFrame."""
    return pd.DataFrame(
        {
            "code": code,
            "date": pd.to_datetime(dates),
            "P": P_values,
            "T": T_values,
        }
    )


# ---------------------------------------------------------------------------
# Test functions
# ---------------------------------------------------------------------------


class TestFillForcingGaps:
    """Tests for fill_forcing_gaps()."""

    def test_one_day_gap_in_recent_window_is_filled(self):
        """A single missing day inside the recent window is interpolated.

        recent window: dates >= REF_DATE - 7d = 2024-06-08
        Gap on 2024-06-11 (recent). Valid neighbours: 2024-06-10 (P=10) and
        2024-06-12 (P=14). Expected interpolated value: 12.0.
        """
        dates = ["2024-06-10", "2024-06-12"]  # 2024-06-11 is missing (NaN)
        df = _make_df("A", dates, [10.0, 14.0], [20.0, 24.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        gap_row = result[result["date"] == pd.Timestamp("2024-06-11")]
        assert len(gap_row) == 1
        assert gap_row["P"].values[0] == pytest_approx(12.0)
        assert gap_row["T"].values[0] == pytest_approx(22.0)

    def test_two_day_consecutive_gap_in_recent_window_stays_nan(self):
        """Two consecutive missing days in the recent window exceed gap_limit_recent=1
        and must remain NaN.

        Valid neighbours: 2024-06-10 and 2024-06-13 (both recent).
        Gap: 2024-06-11 and 2024-06-12 (run length=2 > gap_limit_recent=1).
        """
        dates = ["2024-06-10", "2024-06-13"]
        df = _make_df("A", dates, [10.0, 16.0], [20.0, 26.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        gap_rows = result[result["date"].isin(
            [pd.Timestamp("2024-06-11"), pd.Timestamp("2024-06-12")]
        )]
        assert gap_rows["P"].isna().all(), "Both gap days should remain NaN in recent window"
        assert gap_rows["T"].isna().all()

    def test_three_day_consecutive_gap_in_past_is_filled(self):
        """Three consecutive missing days in the past region are filled
        (gap_limit_past=3 allows runs of up to 3).

        Past region: dates < REF_DATE - 7d = 2024-06-08.
        Use dates well in the past: 2024-05-01 and 2024-05-05,
        missing 2024-05-02, 2024-05-03, 2024-05-04 (run=3 == gap_limit_past=3).
        Valid neighbours: P=10.0 and P=18.0 → midpoint at 2024-05-03 = 14.0.
        """
        dates = ["2024-05-01", "2024-05-05"]
        df = _make_df("A", dates, [10.0, 18.0], [20.0, 28.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        gap_rows = result[result["date"].isin(
            [pd.Timestamp("2024-05-02"), pd.Timestamp("2024-05-03"), pd.Timestamp("2024-05-04")]
        )]
        assert not gap_rows["P"].isna().any(), "3-day past gap should be fully filled"
        assert not gap_rows["T"].isna().any()
        mid_row = result[result["date"] == pd.Timestamp("2024-05-03")]
        assert mid_row["P"].values[0] == pytest_approx(14.0)

    def test_four_day_consecutive_gap_in_past_stays_nan(self):
        """Four consecutive missing days in the past region exceed gap_limit_past=3
        and must remain NaN.

        Past: 2024-05-01 (P=10) and 2024-05-06 (P=20).
        Missing: 2024-05-02..2024-05-05 (run=4 > gap_limit_past=3).
        """
        dates = ["2024-05-01", "2024-05-06"]
        df = _make_df("A", dates, [10.0, 20.0], [0.0, 10.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        gap_rows = result[result["date"].isin(
            [
                pd.Timestamp("2024-05-02"),
                pd.Timestamp("2024-05-03"),
                pd.Timestamp("2024-05-04"),
                pd.Timestamp("2024-05-05"),
            ]
        )]
        assert gap_rows["P"].isna().all(), "4-day past gap must remain NaN"
        assert gap_rows["T"].isna().all()

    def test_missing_dates_reindexed_and_short_gap_filled(self):
        """Missing rows (not NaN cells, just absent dates) are reindexed to a
        continuous daily range and a short gap is filled.

        Dates provided: 2024-05-01 and 2024-05-03 (2024-05-02 row entirely absent).
        After reindex the DataFrame should have 3 rows (2024-05-01 to 2024-05-03)
        and the single-day gap at 2024-05-02 is within gap_limit_past=3 so it
        should be filled.
        """
        dates = ["2024-05-01", "2024-05-03"]  # 2024-05-02 row is simply absent
        df = _make_df("A", dates, [10.0, 14.0], [20.0, 24.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        code_rows = result[result["code"] == "A"].sort_values("date")
        # Must have a continuous range: 3 rows
        assert len(code_rows) == 3, "Reindex should produce 3 contiguous rows"
        gap_row = code_rows[code_rows["date"] == pd.Timestamp("2024-05-02")]
        assert len(gap_row) == 1
        assert gap_row["P"].values[0] == pytest_approx(12.0)

    def test_leading_and_trailing_nans_not_extrapolated(self):
        """NaNs at the start and end of a series are never filled (limit_area='inside').

        Series: NaN, 10.0, NaN, 14.0, NaN on five consecutive past dates.
        The leading NaN (2024-05-01) and trailing NaN (2024-05-05) must remain NaN.
        The interior NaN (2024-05-03) is a 1-day gap and should be filled = 12.0.
        """
        dates = ["2024-05-01", "2024-05-02", "2024-05-03", "2024-05-04", "2024-05-05"]
        P_vals = [np.nan, 10.0, np.nan, 14.0, np.nan]
        T_vals = [np.nan, 20.0, np.nan, 24.0, np.nan]
        df = _make_df("A", dates, P_vals, T_vals)

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        result = result.sort_values("date").reset_index(drop=True)
        assert pd.isna(result.loc[0, "P"]), "Leading NaN must not be extrapolated"
        assert result.loc[2, "P"] == pytest_approx(12.0), "Interior NaN should be filled"
        assert pd.isna(result.loc[4, "P"]), "Trailing NaN must not be extrapolated"

    def test_two_codes_handled_independently(self):
        """A fillable gap in code A must not influence results for code B.

        Code A: 1-day gap in past → filled (P midpoint = 12.0).
        Code B: 4-day gap in past → remains NaN.
        Results per code must reflect only that code's data.
        """
        dates_a = ["2024-05-01", "2024-05-03"]
        dates_b = ["2024-05-01", "2024-05-06"]

        df_a = _make_df("A", dates_a, [10.0, 14.0], [20.0, 24.0])
        df_b = _make_df("B", dates_b, [10.0, 20.0], [0.0, 10.0])
        df = pd.concat([df_a, df_b], ignore_index=True)

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        # Code A gap should be filled
        a_gap = result[(result["code"] == "A") & (result["date"] == pd.Timestamp("2024-05-02"))]
        assert len(a_gap) == 1
        assert a_gap["P"].values[0] == pytest_approx(12.0)

        # Code B gaps should remain NaN (4-day gap > gap_limit_past=3)
        b_gap_dates = [pd.Timestamp(f"2024-05-0{d}") for d in [2, 3, 4, 5]]
        b_gaps = result[(result["code"] == "B") & (result["date"].isin(b_gap_dates))]
        assert b_gaps["P"].isna().all(), "Code B gaps must stay NaN"

    def test_both_p_and_t_columns_interpolated(self):
        """Both P and T value columns are independently interpolated for a short gap."""
        dates = ["2024-05-01", "2024-05-03"]
        df = _make_df("A", dates, [10.0, 14.0], [100.0, 200.0])

        result = fill_forcing_gaps(
            df,
            reference_date=REF_DATE,
            recent_day_threshold=RECENT_DAY_THRESHOLD,
            gap_limit_recent=GAP_LIMIT_RECENT,
            gap_limit_past=GAP_LIMIT_PAST,
        )

        gap_row = result[result["date"] == pd.Timestamp("2024-05-02")]
        assert gap_row["P"].values[0] == pytest_approx(12.0), "P should be interpolated"
        assert gap_row["T"].values[0] == pytest_approx(150.0), "T should be interpolated"

    def test_lenient_everywhere_fills_uniformly_regardless_of_reference_date(self):
        """When gap_limit_recent == gap_limit_past, a gap is filled regardless of
        where reference_date sits — the recent/past split is irrelevant.

        Use a reference_date far in the future so all test dates are in the
        'recent' window, but with equal limits both recent and past get the same
        treatment.
        """
        # All dates are well before 2030-01-01, so all will be in "recent" window
        # with recent_day_threshold=9999
        dates = ["2024-05-01", "2024-05-04"]  # 2-day gap at 2024-05-02..03
        df = _make_df("A", dates, [10.0, 16.0], [20.0, 26.0])

        result = fill_forcing_gaps(
            df,
            reference_date=pd.Timestamp("2030-01-01"),
            recent_day_threshold=9999,
            gap_limit_recent=3,   # same as gap_limit_past
            gap_limit_past=3,
        )

        gap_rows = result[result["date"].isin(
            [pd.Timestamp("2024-05-02"), pd.Timestamp("2024-05-03")]
        )]
        assert not gap_rows["P"].isna().any(), (
            "2-day gap should be filled when gap_limit_recent == gap_limit_past == 3"
        )
        # Linear interpolation: 10, 12, 14, 16
        assert gap_rows.sort_values("date")["P"].values[0] == pytest_approx(12.0)
        assert gap_rows.sort_values("date")["P"].values[1] == pytest_approx(14.0)


# ---------------------------------------------------------------------------
# Alias so pytest.approx works as a module-level name inside the class methods
# ---------------------------------------------------------------------------
import pytest

pytest_approx = pytest.approx
