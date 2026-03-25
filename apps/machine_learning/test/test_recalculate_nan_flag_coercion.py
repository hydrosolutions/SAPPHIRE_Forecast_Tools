"""Tests for ML-012: flag coercion in recalculate_nan_forecasts.

These tests verify that pd.to_numeric(errors='coerce') correctly handles
NaN, float, and mixed flag values — preventing the ValueError crash that
occurred with the old astype(int) approach.

The coercion logic lives in recalculate_nan_forecasts.py at:
- Line 253: forecast['flag'] = pd.to_numeric(forecast['flag'], errors='coerce')
- Line 334: hindcast['flag'] = pd.to_numeric(hindcast['flag'], errors='coerce')
- Lines 335-342: hindcast.loc[hindcast['flag'].isna(), 'flag'] = 3
"""

import numpy as np
import pandas as pd


class TestHindcastNaNFlagCoercion:
    """Scenario 1: Hindcast DataFrame with flag=NaN rows should not crash.

    Before the fix, line 334 was hindcast['flag'].astype(int) which raised
    ValueError on NaN. After the fix, pd.to_numeric(errors='coerce')
    preserves NaN, then the sentinel assignment sets them to flag=3.
    """

    def test_nan_flag_rows_get_sentinel_value_3(self):
        """NaN flag values become flag=3 after coercion + sentinel."""
        # Arrange: hindcast with some NaN flags (as returned by API when
        # flag was stored as NULL)
        hindcast = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-06-01", "2025-06-02", "2025-06-03"]),
                "forecast_date": pd.to_datetime(["2025-06-01"] * 3),
                "code": ["15013"] * 3,
                "Q50": [1.5, np.nan, 2.0],
                "flag": [4.0, np.nan, np.nan],  # 2 NaN flags
            }
        )

        # Act: apply the same coercion as recalculate_nan_forecasts.py:334-342
        hindcast["flag"] = pd.to_numeric(hindcast["flag"], errors="coerce")
        hindcast.loc[hindcast["flag"].isna(), "flag"] = 3

        # Assert
        assert not hindcast["flag"].isna().any(), "No NaN flags should remain"
        assert hindcast.loc[0, "flag"] == 4.0  # original value preserved
        assert hindcast.loc[1, "flag"] == 3.0  # NaN → sentinel
        assert hindcast.loc[2, "flag"] == 3.0  # NaN → sentinel

    def test_string_flag_values_coerced_to_nan_then_sentinel(self):
        """Non-numeric flag strings become NaN via coerce, then flag=3."""
        hindcast = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-06-01", "2025-06-02"]),
                "forecast_date": pd.to_datetime(["2025-06-01"] * 2),
                "code": ["15013"] * 2,
                "Q50": [1.5, 2.0],
                "flag": ["4", "invalid"],  # string that can't be numeric
            }
        )

        hindcast["flag"] = pd.to_numeric(hindcast["flag"], errors="coerce")
        hindcast.loc[hindcast["flag"].isna(), "flag"] = 3

        assert hindcast.loc[0, "flag"] == 4.0  # "4" → 4.0
        assert hindcast.loc[1, "flag"] == 3.0  # "invalid" → NaN → 3

    def test_all_nan_flags_all_become_sentinel(self):
        """Edge case: every flag is NaN."""
        hindcast = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-06-01", "2025-06-02"]),
                "forecast_date": pd.to_datetime(["2025-06-01"] * 2),
                "code": ["15013"] * 2,
                "Q50": [np.nan, np.nan],
                "flag": [np.nan, np.nan],
            }
        )

        hindcast["flag"] = pd.to_numeric(hindcast["flag"], errors="coerce")
        hindcast.loc[hindcast["flag"].isna(), "flag"] = 3

        assert (hindcast["flag"] == 3).all()


class TestFloatFlagRecognition:
    """Scenario 2: flag=3.0 (float64) correctly matches .isin([3]).

    After pd.to_numeric coercion, flags become float64. The .isin([1, 2])
    and .isin([3]) checks must still work with float equality.
    """

    def test_float_flag_3_matches_isin_int_3(self):
        """float64 3.0 is recognized by .isin([3])."""
        flags = pd.Series([3.0, 3.0, 4.0, 3.0], dtype="float64")
        mask = flags.isin([3])
        assert mask.sum() == 3
        assert not mask.iloc[2]  # flag=4.0 should not match

    def test_float_flags_1_2_match_isin(self):
        """float64 1.0 and 2.0 match .isin([1, 2])."""
        flags = pd.Series([0.0, 1.0, 2.0, 3.0, 4.0], dtype="float64")
        mask = flags.isin([1, 2])
        expected = [False, True, True, False, False]
        assert list(mask) == expected

    def test_nan_does_not_match_any_isin(self):
        """NaN values should NOT match .isin([1, 2]) or .isin([3])."""
        flags = pd.Series([1.0, np.nan, 3.0])
        assert not flags.isin([1, 2]).iloc[1]  # NaN ≠ 1 or 2
        assert not flags.isin([3]).iloc[1]  # NaN ≠ 3


class TestMixedForecastFlagFiltering:
    """Scenario 3: Forecast with mixed int/NaN flags filters correctly.

    The forecast flag coercion at line 253 uses pd.to_numeric(errors='coerce').
    After coercion, .isin([1, 2]) must select only the actual flag=1/2 rows
    and NOT select NaN or flag=0 rows.
    """

    def test_mixed_flags_filter_correctly(self):
        """Mixed int/NaN/float flags: only flag=1 and flag=2 selected."""
        forecast = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-06-01"] * 5),
                "forecast_date": pd.to_datetime(["2025-06-01"] * 5),
                "code": ["15013"] * 5,
                "Q50": [1.0, np.nan, np.nan, 3.0, np.nan],
                "flag": [0, 1, 2, 4, np.nan],  # mixed: int-like + NaN
            }
        )

        # Act: same coercion as recalculate_nan_forecasts.py:253
        forecast["flag"] = pd.to_numeric(forecast["flag"], errors="coerce")
        nan_mask = forecast["flag"].isin([1, 2])

        # Assert
        assert nan_mask.sum() == 2  # only flag=1 and flag=2
        assert not nan_mask.iloc[0]  # flag=0 → not selected
        assert nan_mask.iloc[1]  # flag=1 → selected
        assert nan_mask.iloc[2]  # flag=2 → selected
        assert not nan_mask.iloc[3]  # flag=4 → not selected
        assert not nan_mask.iloc[4]  # flag=NaN → not selected

    def test_all_valid_flags_no_nan(self):
        """When no NaN flags exist, coercion is a no-op."""
        forecast = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-06-01"] * 3),
                "forecast_date": pd.to_datetime(["2025-06-01"] * 3),
                "code": ["15013"] * 3,
                "Q50": [1.0, np.nan, 2.0],
                "flag": [0, 1, 4],  # all valid integers
            }
        )

        forecast["flag"] = pd.to_numeric(forecast["flag"], errors="coerce")
        assert not forecast["flag"].isna().any()
        assert forecast["flag"].isin([1, 2]).sum() == 1  # only flag=1

    def test_original_astype_int_would_crash_on_nan(self):
        """Confirm the OLD code path (astype(int)) raises on NaN.

        This is a regression guard: if someone reverts to astype(int),
        this test documents why it was changed.
        """
        import pytest

        flags = pd.Series([4.0, np.nan, 3.0])
        with pytest.raises((ValueError, TypeError, pd.errors.IntCastingNaNError)):
            flags.astype(int)
