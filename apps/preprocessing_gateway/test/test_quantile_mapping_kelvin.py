"""
Tests for Kelvin conversion in quantile mapping, input mutation
behavior of quantile_mapping_ptf, and ffill NaN patterns.

Covers gaps in:
- do_quantile_mapping (Kelvin conversion with non-trivial params)
- quantile_mapping_ptf (in-place mutation of input array)
- ffill NaN behavior used in Quantile_Mapping_OP.py:747-755
"""

import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client before importing
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import dg_utils

# =====================================================================
# Kelvin Conversion in do_quantile_mapping
# =====================================================================


class TestKelvinConversion:
    """Tests that do_quantile_mapping correctly converts T to Kelvin
    before applying the power transform, then back to Celsius.

    Formula: T_out = a * (T_celsius + 273.15)^b - 273.15
    """

    def _make_data(self, t_values, code="12345"):
        """Build a minimal DataFrame for do_quantile_mapping."""
        n = len(t_values)
        dates = pd.date_range("2024-01-01", periods=n, freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "P": [5.0] * n,
                "T": t_values,
                "code": [code] * n,
            }
        )

    def _make_params(self, a_p, b_p, wet_day, a_t, b_t, code="12345"):
        """Build P and T parameter DataFrames."""
        P_param = pd.DataFrame(
            {
                "code": [code],
                "a": [a_p],
                "b": [b_p],
                "wet_day": [wet_day],
            }
        )
        T_param = pd.DataFrame(
            {
                "code": [code],
                "a": [a_t],
                "b": [b_t],
            }
        )
        return P_param, T_param

    def test_nontrivial_params_exact_values(self):
        """a=1.001, b=0.999, T=10.0 C -> result approx 9.72 C."""
        data = self._make_data([10.0])
        P_param, T_param = self._make_params(1.0, 1.0, 0.0, 1.001, 0.999)
        _, T_data = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        expected = 1.001 * (283.15**0.999) - 273.15
        assert abs(T_data["T"].iloc[0] - expected) < 0.01

    def test_negative_temperature_exact(self):
        """T=-20.0 C, a=1.001, b=0.999 -> valid result (not NaN).
        Without Kelvin conversion, (-20)^0.999 produces NaN."""
        data = self._make_data([-20.0])
        P_param, T_param = self._make_params(1.0, 1.0, 0.0, 1.001, 0.999)
        _, T_data = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        expected = 1.001 * (253.15**0.999) - 273.15
        assert not pd.isna(T_data["T"].iloc[0])
        assert abs(T_data["T"].iloc[0] - expected) < 0.01

    def test_zero_celsius_exact(self):
        """T=0.0 C -> 1.001 * 273.15^0.999 - 273.15 approx -0.27."""
        data = self._make_data([0.0])
        P_param, T_param = self._make_params(1.0, 1.0, 0.0, 1.001, 0.999)
        _, T_data = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        expected = 1.001 * (273.15**0.999) - 273.15
        assert abs(T_data["T"].iloc[0] - expected) < 0.01

    def test_precipitation_not_kelvin_converted(self):
        """P uses ptf directly: P=5.0, a=1.1, b=0.95 -> 1.1*5^0.95."""
        data = self._make_data([10.0])
        # Override P values
        data["P"] = [5.0]
        P_param, T_param = self._make_params(1.1, 0.95, 0.0, 1.0, 1.0)
        P_data, _ = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        expected = round(1.1 * (5.0**0.95), 2)
        assert P_data["P"].iloc[0] == expected

    def test_identity_params_roundtrip(self):
        """a=1, b=1 returns original T values (Kelvin offset
        cancels)."""
        data = self._make_data([10.0, -5.0, 0.0, 35.0])
        P_param, T_param = self._make_params(1.0, 1.0, 0.0, 1.0, 1.0)
        _, T_data = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        # With a=1, b=1: a*(T+273.15)^1 - 273.15 = T
        for i, expected_t in enumerate([10.0, -5.0, 0.0, 35.0]):
            assert abs(T_data["T"].iloc[i] - expected_t) < 0.01

    def test_wrong_without_kelvin(self):
        """Documents why Kelvin matters: the naive result
        1.001 * 10.0^0.999 differs from the correct Kelvin-based
        result by ~0.27."""
        data = self._make_data([10.0])
        P_param, T_param = self._make_params(1.0, 1.0, 0.0, 1.001, 0.999)
        _, T_data = dg_utils.do_quantile_mapping(data, P_param, T_param, ensemble=False)
        correct = 1.001 * (283.15**0.999) - 273.15
        naive = round(1.001 * (10.0**0.999), 2)
        # The function returns the Kelvin-correct value
        assert abs(T_data["T"].iloc[0] - correct) < 0.01
        # The naive value is different
        assert abs(correct - naive) > 0.2


# =====================================================================
# Input Mutation in quantile_mapping_ptf
# =====================================================================


class TestQuantileMappingInputMutation:
    """Documents the in-place mutation behavior of
    quantile_mapping_ptf."""

    def test_wet_days_zeroes_input_array_in_place(self):
        """Input [0.0, 3.0, 0.5, 0.0] with wet_day_threshold=0.5:
        dry positions zeroed in place."""
        original = np.array([0.0, 3.0, 0.5, 0.0])
        result = dg_utils.quantile_mapping_ptf(
            original, a=1.1, b=0.95, wet_days=True, wet_day_threshold=0.5
        )
        # Dry positions (values <= 0.5) zeroed in place
        assert original[0] == 0.0
        assert original[2] == 0.0
        assert original[3] == 0.0
        # Wet position: output = round(1.1 * 3.0^0.95, 2)
        expected_wet = round(1.1 * (3.0**0.95), 2)
        assert result[1] == expected_wet

    def test_wet_days_false_does_not_zero_input(self):
        """Input [10.0, 20.0], a=1.0, b=1.0, wet_days=False:
        output == input (identity), input not zeroed."""
        original = np.array([10.0, 20.0])
        input_copy = original.copy()
        result = dg_utils.quantile_mapping_ptf(
            original, a=1.0, b=1.0, wet_days=False, wet_day_threshold=0
        )
        np.testing.assert_array_equal(result, [10.0, 20.0])
        # Input array is not modified
        np.testing.assert_array_equal(original, input_copy)


# =====================================================================
# ffill NaN Behavior (Quantile_Mapping_OP.py:747-755)
# =====================================================================


class TestFfillNanBehavior:
    """Tests the ffill pattern used for NaN handling in the main
    pipeline. This exercises pandas ffill behavior on DataFrames
    matching the pipeline structure."""

    def test_middle_nan_filled_forward(self):
        """[1, NaN, 3] -> ffill -> [1, 1, 3]."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3),
                "P": [1.0, np.nan, 3.0],
                "code": ["A", "A", "A"],
            }
        )
        result = df.ffill()
        assert result["P"].iloc[1] == 1.0

    def test_leading_nan_stays_nan(self):
        """[NaN, 2, 3] -> ffill -> [NaN, 2, 3]."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3),
                "P": [np.nan, 2.0, 3.0],
                "code": ["A", "A", "A"],
            }
        )
        result = df.ffill()
        assert pd.isna(result["P"].iloc[0])
        assert result["P"].iloc[1] == 2.0

    def test_all_nan_stays_all_nan(self):
        """[NaN, NaN] -> ffill -> [NaN, NaN]."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=2),
                "P": [np.nan, np.nan],
                "code": ["A", "A"],
            }
        )
        result = df.ffill()
        assert pd.isna(result["P"].iloc[0])
        assert pd.isna(result["P"].iloc[1])

    def test_ffill_crosses_codes_no_groupby(self):
        """Documents that ffill propagates across code boundaries
        (current behavior — no groupby)."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=4),
                "P": [1.0, 2.0, np.nan, 4.0],
                "code": ["A", "A", "B", "B"],
            }
        )
        result = df.ffill()
        # NaN at index 2 (code B) is filled with value from index 1
        # (code A), because ffill has no groupby
        assert result["P"].iloc[2] == 2.0
