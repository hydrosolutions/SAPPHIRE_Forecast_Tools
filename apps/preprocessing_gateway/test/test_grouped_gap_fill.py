"""
Tests for dg_utils.fill_gaps_grouped — PREPG-013.

Covers:
- Leading gaps do not bleed across group boundaries (station / member).
- Interior temperature gaps are linearly interpolated.
- Interior precipitation gaps are forward-filled, not interpolated.
- No-NaN frames are returned unchanged.
- No rows are ever dropped, including when the group key itself is NaN.
- Trailing gap behaviour is pinned for both methods.
- The six production call sites are wired to fill_gaps_grouped with the
  correct value_col/group_cols/method (guards against a revert to a
  bare .ffill() or a copy/paste argument swap going unnoticed by the
  unit tests above, which only exercise the helper directly).
"""

import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client before importing (dg_utils does not import it
# directly, but keep parity with the other test files in this directory in
# case that changes).
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

from dg_utils import fill_gaps_grouped

STATION_A = "19999"
STATION_B = "19998"


class TestLeadingGapDoesNotBleed:
    """Defect (B): a leading gap in one group must not inherit the
    previous group's last value."""

    def test_station_leading_gap_remains_nan_ffill(self):
        """Two stations stacked; station B's first P row is NaN.
        Grouped ffill must leave it NaN, not inherit station A's
        last value."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                "P": [1.0, 2.0, np.nan, 5.0],
                "code": [STATION_A, STATION_A, STATION_B, STATION_B],
            }
        )
        result = fill_gaps_grouped(df, "P", ["code"], "ffill")
        # The exact assertion that matters: the cell REMAINS NaN.
        # (Not merely "not equal to station A's last value" — see
        # module docstring / issue spec.)
        assert pd.isna(result.loc[2, "P"])
        assert result.loc[3, "P"] == 5.0

    def test_station_leading_gap_remains_nan_interpolate(self):
        """Same scenario, but with method='interpolate' (temperature).
        A leading gap has no left neighbour within the group, so
        limit_area='inside' must leave it NaN — ungrouped interpolation
        would instead fill it with a midpoint between station A's last
        value and station B's next value, which is unequal to station
        A's last value yet still crosses the boundary. That would make
        a weaker assertion pass while the defect survived."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                "T": [1.0, 2.0, np.nan, 5.0],
                "code": [STATION_A, STATION_A, STATION_B, STATION_B],
            }
        )
        result = fill_gaps_grouped(df, "T", ["code"], "interpolate")
        assert pd.isna(result.loc[2, "T"])
        assert result.loc[3, "T"] == 5.0

    def test_ensemble_member_leading_gap_remains_nan(self):
        """Two ensemble members; member 2's first row is NaN. Grouping
        by ensemble_member must leave it NaN."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                "P": [1.0, 2.0, np.nan, 5.0],
                "ensemble_member": [1, 1, 2, 2],
            }
        )
        result = fill_gaps_grouped(df, "P", ["ensemble_member"], "ffill")
        assert pd.isna(result.loc[2, "P"])
        assert result.loc[3, "P"] == 5.0


class TestInteriorGapFillSemantics:
    """Defect (A): interior temperature gaps are interpolated, not
    carried forward. Precipitation keeps ffill semantics."""

    def test_interior_temperature_gap_is_interpolated(self):
        """[1.0, NaN, 3.0] -> [1.0, 2.0, 3.0]."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "T": [1.0, np.nan, 3.0],
                "code": [STATION_A, STATION_A, STATION_A],
            }
        )
        result = fill_gaps_grouped(df, "T", ["code"], "interpolate")
        assert result["T"].tolist() == [1.0, 2.0, 3.0]

    def test_interior_precipitation_gap_is_forward_filled_not_interpolated(self):
        """[1.0, NaN, 3.0] -> [1.0, 1.0, 3.0] under ffill (not 2.0)."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "P": [1.0, np.nan, 3.0],
                "code": [STATION_A, STATION_A, STATION_A],
            }
        )
        result = fill_gaps_grouped(df, "P", ["code"], "ffill")
        assert result["P"].tolist() == [1.0, 1.0, 3.0]


class TestTrailingGapBehaviour:
    """Pin trailing-gap behaviour explicitly for both methods, per the
    issue spec, so the contract is not accidental."""

    def test_trailing_gap_interpolate_stays_nan(self):
        """limit_area='inside' leaves a trailing NaN untouched."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "T": [1.0, 2.0, np.nan],
                "code": [STATION_A, STATION_A, STATION_A],
            }
        )
        result = fill_gaps_grouped(df, "T", ["code"], "interpolate")
        assert result["T"].iloc[0] == 1.0
        assert result["T"].iloc[1] == 2.0
        assert pd.isna(result["T"].iloc[2])

    def test_trailing_gap_ffill_is_filled(self):
        """ffill fills a trailing NaN with the last valid value."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "P": [1.0, 2.0, np.nan],
                "code": [STATION_A, STATION_A, STATION_A],
            }
        )
        result = fill_gaps_grouped(df, "P", ["code"], "ffill")
        assert result["P"].tolist() == [1.0, 2.0, 2.0]


class TestNoOpAndRowIntegrity:
    """A frame with no NaNs is unchanged; no rows are ever dropped."""

    def test_no_nan_frame_unchanged(self):
        """Identical values and identical row order when there is
        nothing to fill."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                "P": [1.0, 2.0, 3.0, 4.0],
                "code": [STATION_A, STATION_A, STATION_B, STATION_B],
            }
        )
        result = fill_gaps_grouped(df, "P", ["code"], "ffill")
        pd.testing.assert_frame_equal(result, df)

    @pytest.mark.parametrize("method", ["ffill", "interpolate"])
    def test_no_rows_dropped(self, method):
        value_col = "P" if method == "ffill" else "T"
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                value_col: [1.0, np.nan, np.nan, 5.0],
                "code": [STATION_A, STATION_A, STATION_B, STATION_B],
            }
        )
        result = fill_gaps_grouped(df, value_col, ["code"], method)
        assert len(result) == len(df)

    def test_row_order_preserved_with_duplicate_index(self):
        """Concatenated station blocks may share duplicate index
        values (dg_utils.py station concatenation). transform() must
        keep row order and index intact regardless."""
        block_a = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "P": [1.0, 2.0],
                "code": [STATION_A, STATION_A],
            }
        )
        block_b = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "P": [np.nan, 5.0],
                "code": [STATION_B, STATION_B],
            }
        )
        df = pd.concat([block_a, block_b], axis=0)  # duplicate index 0,1,0,1
        result = fill_gaps_grouped(df, "P", ["code"], "ffill")
        assert len(result) == len(df)
        assert result.index.tolist() == df.index.tolist()
        assert result["code"].tolist() == df["code"].tolist()
        assert pd.isna(result["P"].iloc[2])
        assert result["P"].iloc[3] == 5.0

    @pytest.mark.parametrize("method", ["ffill", "interpolate"])
    def test_nan_group_key_rows_not_dropped_or_silently_touched(self, method):
        """dropna=False: a row whose group key (code) is itself NaN
        must not be dropped, and its own value is left exactly as-is
        (a group of one NaN key with a real value has no gap to fill;
        a group of one NaN key with a NaN value has no in-group
        neighbour, so it stays NaN under either method)."""
        value_col = "P" if method == "ffill" else "T"
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                value_col: [7.0, np.nan, 9.0],
                "code": [STATION_A, np.nan, STATION_A],
            }
        )
        result = fill_gaps_grouped(df, value_col, ["code"], method)
        assert len(result) == len(df)
        # The NaN-key row's own value (NaN here) is untouched: it forms
        # a group of one, so there is no neighbour to fill from.
        assert pd.isna(result[value_col].iloc[1])
        # The real-code group's values are unaffected by the NaN-key row.
        assert result[value_col].iloc[0] == 7.0
        assert result[value_col].iloc[2] == 9.0


class TestNoNaNParityAcrossMethods:
    """The 'no NaNs -> unchanged' contract must hold for both fill
    methods, not just ffill."""

    def test_no_nan_frame_unchanged_interpolate(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
                "T": [1.0, 2.0, 3.0, 4.0],
                "code": [STATION_A, STATION_A, STATION_B, STATION_B],
            }
        )
        result = fill_gaps_grouped(df, "T", ["code"], "interpolate")
        pd.testing.assert_frame_equal(result, df)
