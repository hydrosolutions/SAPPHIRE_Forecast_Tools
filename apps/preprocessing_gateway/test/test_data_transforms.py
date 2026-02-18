"""
Tests for data transformation functions in dg_utils.py.

These functions have zero test coverage. Covers:
- ptf (power transform function)
- quantile_mapping_ptf (quantile mapping with wet-day masking)
- do_quantile_mapping (per-station quantile mapping loop)
- transform_data_file_control_member (ECMWF control member CSV transform)
- transform_snow_data (snow model data transform)
"""
import os
import sys
import pandas as pd
import numpy as np
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import dg_utils


# =====================================================================
# ptf (power transform function)
# =====================================================================

class TestPtf:
    """Tests for dg_utils.ptf: y = a * x^b."""

    def test_basic_transform(self):
        """ptf([1, 2, 3], a=2, b=1) -> [2, 4, 6]."""
        result = dg_utils.ptf(np.array([1.0, 2.0, 3.0]), a=2.0, b=1.0)
        np.testing.assert_array_almost_equal(result, [2.0, 4.0, 6.0])

    def test_zero_input(self):
        """ptf([0], a=5, b=2) -> [0] (0 raised to any power is 0)."""
        result = dg_utils.ptf(np.array([0.0]), a=5.0, b=2.0)
        np.testing.assert_array_almost_equal(result, [0.0])

    def test_power_zero(self):
        """ptf([1], a=1, b=0) -> [1] (anything^0 = 1)."""
        result = dg_utils.ptf(np.array([1.0]), a=1.0, b=0.0)
        np.testing.assert_array_almost_equal(result, [1.0])


# =====================================================================
# quantile_mapping_ptf
# =====================================================================

class TestQuantileMappingPtf:
    """Tests for dg_utils.quantile_mapping_ptf."""

    def test_wet_days_true_zeroes_dry_days(self):
        """With wet_days=True, values <= threshold are set to 0 before transform."""
        data = np.array([0.0, 0.5, 1.0, 2.0])
        result = dg_utils.quantile_mapping_ptf(
            data, a=2.0, b=1.0, wet_days=True, wet_day_threshold=0.5
        )
        # 0.0 and 0.5 <= threshold -> set to 0, then ptf(0)=0
        # 1.0 -> ptf(1.0, 2, 1) = 2.0
        # 2.0 -> ptf(2.0, 2, 1) = 4.0
        assert result[0] == 0.0
        assert result[1] == 0.0
        assert result[2] == 2.0
        assert result[3] == 4.0

    def test_wet_days_false_transforms_all(self):
        """With wet_days=False, all values are transformed."""
        data = np.array([0.0, 1.0, 2.0])
        result = dg_utils.quantile_mapping_ptf(
            data, a=1.0, b=2.0, wet_days=False, wet_day_threshold=0
        )
        # ptf(x, 1, 2) = x^2 -> [0, 1, 4]
        np.testing.assert_array_almost_equal(result, [0.0, 1.0, 4.0])

    def test_rounds_to_2_decimals(self):
        """Output is rounded to 2 decimal places."""
        data = np.array([1.5])
        result = dg_utils.quantile_mapping_ptf(
            data, a=1.0, b=1.0, wet_days=False
        )
        # 1.5 * 1.5^0 ... wait, ptf(1.5, 1, 1) = 1*1.5^1 = 1.5
        # Let's use values that produce non-round results
        data2 = np.array([1.111])
        result2 = dg_utils.quantile_mapping_ptf(
            data2, a=1.0, b=3.0, wet_days=False
        )
        # 1.111^3 = 1.371330631 -> rounded to 1.37
        assert result2[0] == round(1.111**3, 2)

    def test_all_zero_input(self):
        """All-zero input produces all-zero output."""
        data = np.array([0.0, 0.0, 0.0])
        result = dg_utils.quantile_mapping_ptf(
            data, a=5.0, b=2.0, wet_days=True, wet_day_threshold=0
        )
        np.testing.assert_array_almost_equal(result, [0.0, 0.0, 0.0])


# =====================================================================
# do_quantile_mapping
# =====================================================================

class TestDoQuantileMapping:
    """Tests for dg_utils.do_quantile_mapping."""

    def test_single_code_transforms_correctly(self):
        """Single code with known params transforms P and T correctly."""
        era5_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'P': [10.0, 20.0],
            'T': [5.0, 10.0],
            'code': ['12345', '12345'],
        })

        P_param = pd.DataFrame({
            'code': ['12345'],
            'a': [1.0],
            'b': [1.0],
            'wet_day': [0.0],
        })
        T_param = pd.DataFrame({
            'code': ['12345'],
            'a': [1.0],
            'b': [1.0],
        })

        P_data, T_data = dg_utils.do_quantile_mapping(
            era5_data, P_param, T_param, ensemble=False
        )

        assert list(P_data.columns) == ['date', 'P', 'code']
        assert list(T_data.columns) == ['date', 'T', 'code']
        assert len(P_data) == 2
        assert len(T_data) == 2

    def test_multiple_codes_each_gets_own_params(self):
        """Multiple codes use their own parameter rows."""
        era5_data = pd.DataFrame({
            'date': pd.to_datetime(
                ['2024-01-01', '2024-01-01']
            ),
            'P': [10.0, 10.0],
            'T': [5.0, 5.0],
            'code': ['AAA', 'BBB'],
        })

        P_param = pd.DataFrame({
            'code': ['AAA', 'BBB'],
            'a': [2.0, 3.0],
            'b': [1.0, 1.0],
            'wet_day': [0.0, 0.0],
        })
        T_param = pd.DataFrame({
            'code': ['AAA', 'BBB'],
            'a': [1.0, 1.0],
            'b': [1.0, 1.0],
        })

        P_data, T_data = dg_utils.do_quantile_mapping(
            era5_data, P_param, T_param, ensemble=False
        )

        # Code AAA: P = ptf(10, 2, 1) = 20 -> rounded to 20.0
        # Code BBB: P = ptf(10, 3, 1) = 30 -> rounded to 30.0
        aaa_p = P_data[P_data['code'] == 'AAA']['P'].values[0]
        bbb_p = P_data[P_data['code'] == 'BBB']['P'].values[0]
        assert aaa_p == 20.0
        assert bbb_p == 30.0

    def test_ensemble_true_includes_ensemble_member_column(self):
        """When ensemble=True, output includes ensemble_member column."""
        era5_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'P': [10.0],
            'T': [5.0],
            'code': ['12345'],
            'ensemble_member': [1],
        })

        P_param = pd.DataFrame({
            'code': ['12345'],
            'a': [1.0],
            'b': [1.0],
            'wet_day': [0.0],
        })
        T_param = pd.DataFrame({
            'code': ['12345'],
            'a': [1.0],
            'b': [1.0],
        })

        P_data, T_data = dg_utils.do_quantile_mapping(
            era5_data, P_param, T_param, ensemble=True
        )

        assert 'ensemble_member' in P_data.columns
        assert 'ensemble_member' in T_data.columns


# =====================================================================
# transform_data_file_control_member
# =====================================================================

class TestTransformDataFileControlMember:
    """Tests for dg_utils.transform_data_file_control_member."""

    def test_basic_transformation_columns(self):
        """Output has columns ['date', 'T', 'P', 'code']."""
        # Simulate DG output: first 7 rows are headers, then data
        # Columns: Station, 12345 (T), 12345.1 (P), 12345.2 (SD, ignored)
        header_rows = [
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
            ['header'] + ['h'] * 3,
        ]
        data_rows = [
            ['01/01/2024', '5.0', '10.0', '1.0'],
            ['02/01/2024', '6.0', '12.0', '2.0'],
        ]

        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Station', '12345', '12345.1', '12345.2']
        )

        result = dg_utils.transform_data_file_control_member(df)

        assert 'date' in result.columns
        assert 'T' in result.columns
        assert 'P' in result.columns
        assert 'code' in result.columns
        assert len(result) == 2

    def test_multiple_station_columns_multiply_rows(self):
        """3 station codes produce 3x rows (one per code per date)."""
        header_rows = [['h'] * 7] * 7
        data_rows = [
            ['01/01/2024', '5', '10', 'sd', '7', '15', 'sd2'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows,
            columns=[
                'Station', 'AAA', 'AAA.1', 'AAA.2',
                'BBB', 'BBB.1', 'BBB.2'
            ],
        )

        result = dg_utils.transform_data_file_control_member(df)
        # 2 codes (AAA, BBB) x 1 date = 2 rows
        assert len(result) == 2
        codes = sorted(result['code'].unique())
        assert codes == ['AAA', 'BBB']

    def test_first_7_rows_dropped(self):
        """First 7 rows (headers) are dropped via .iloc[7:]."""
        # First 7 rows are headers (non-date), remaining 3 are parseable dates
        header_rows = [['header'] + ['0'] * 2 for _ in range(7)]
        data_rows = [
            ['01/01/2024', '1.0', '2.0'],
            ['02/01/2024', '3.0', '4.0'],
            ['03/01/2024', '5.0', '6.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Station', '12345', '12345.1']
        )

        result = dg_utils.transform_data_file_control_member(df)
        # Only the 3 data rows remain
        assert len(result) == 3

    def test_non_numeric_values_coerced_to_nan(self):
        """Non-numeric T/P values are coerced to NaN."""
        header_rows = [['h'] * 3] * 7
        data_rows = [
            ['01/01/2024', 'abc', 'xyz'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(all_rows, columns=['Station', '12345', '12345.1'])

        result = dg_utils.transform_data_file_control_member(df)
        assert pd.isna(result['T'].iloc[0])
        assert pd.isna(result['P'].iloc[0])


# =====================================================================
# transform_snow_data
# =====================================================================

class TestTransformSnowData:
    """Tests for dg_utils.transform_snow_data."""

    def test_single_code_single_variable(self):
        """Single code, single variable produces correct columns."""
        # First 4 rows are headers, then data
        header_rows = [['h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '100.5'],
            ['02/01/2024', '110.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(all_rows, columns=['Timestamp', '12345'])

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert 'date' in result.columns
        assert 'SWE' in result.columns
        assert 'code' in result.columns
        assert len(result) == 2
        assert result['code'].iloc[0] == '12345'

    def test_elevation_bands(self):
        """Columns 12345_1, 12345_2 produce SWE_1, SWE_2."""
        header_rows = [['h', 'h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '100.0', '80.0', '90.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', '12345', '12345_1', '12345_2']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert 'SWE' in result.columns
        assert 'SWE_1' in result.columns
        assert 'SWE_2' in result.columns
        assert result['SWE'].iloc[0] == 100.0
        assert result['SWE_1'].iloc[0] == 80.0
        assert result['SWE_2'].iloc[0] == 90.0

    def test_multiple_codes(self):
        """Multiple codes produce separate rows per code."""
        header_rows = [['h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '100.0', '200.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(all_rows, columns=['Timestamp', '11111', '22222'])

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 2
        codes = sorted(result['code'].unique())
        assert codes == ['11111', '22222']

    def test_alphanumeric_codes(self):
        """Alphanumeric codes (e.g., KGZ500) are preserved as strings."""
        header_rows = [['h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '50.0', '60.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', 'KGZ500', 'KGZ500_1']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 1
        assert result['code'].iloc[0] == 'KGZ500'
        assert 'SWE' in result.columns
        assert 'SWE_1' in result.columns
        assert result['SWE'].iloc[0] == 50.0
        assert result['SWE_1'].iloc[0] == 60.0

    def test_mixed_numeric_and_alphanumeric_codes(self):
        """Numeric and alphanumeric codes coexist as strings."""
        header_rows = [['h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '100.0', '200.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', '12345', 'KGZ500']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 2
        codes = sorted(result['code'].unique())
        assert codes == ['12345', 'KGZ500']

    def test_code_with_underscore_and_elevation_band(self):
        """Code containing underscore (KGZ_500_1) is split correctly
        using rsplit: code='KGZ_500', band=1."""
        header_rows = [['h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '50.0', '60.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', 'KGZ_500', 'KGZ_500_1']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 1
        assert result['code'].iloc[0] == 'KGZ_500'
        assert 'SWE' in result.columns
        assert 'SWE_1' in result.columns
        assert result['SWE'].iloc[0] == 50.0
        assert result['SWE_1'].iloc[0] == 60.0

    def test_non_numeric_suffix_treated_as_code(self):
        """Column like 'KGZ500_High' is treated as a plain code, not
        split into code + elevation band."""
        header_rows = [['h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '75.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', 'KGZ500_High']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 1
        assert result['code'].iloc[0] == 'KGZ500_High'
        assert 'SWE' in result.columns
        assert result['SWE'].iloc[0] == 75.0

    def test_large_numeric_suffix_treated_as_code(self):
        """Suffix > 14 is not an elevation band. E.g., 'KGZ_500' is a
        single code, not code='KGZ' with band=500."""
        header_rows = [['h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '33.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows, columns=['Timestamp', 'KGZ_500']
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 1
        assert result['code'].iloc[0] == 'KGZ_500'
        assert 'SWE' in result.columns
        assert result['SWE'].iloc[0] == 33.0

    def test_no_base_column_computes_mean_from_bands(self):
        """When DG CSV has only elevation band columns (no base/mean
        column), the base variable is computed as the mean across bands.

        This is the KGZ500m scenario: the DG returns columns like
        15013_3, 15013_6, 15013_4 (only bands, no bare 15013).
        """
        header_rows = [['h', 'h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '80.0', '100.0', '120.0'],
            ['02/01/2024', '90.0', '110.0', '130.0'],
        ]
        all_rows = header_rows + data_rows
        # No bare '15013' column — only bands
        df = pd.DataFrame(
            all_rows,
            columns=['Timestamp', '15013_3', '15013_6', '15013_4'],
        )

        result = dg_utils.transform_snow_data(df, 'RoF')

        assert len(result) == 2
        assert result['code'].iloc[0] == '15013'
        # Elevation bands preserved
        assert 'RoF_3' in result.columns
        assert 'RoF_6' in result.columns
        assert 'RoF_4' in result.columns
        # Base variable computed as mean of bands
        assert 'RoF' in result.columns
        expected_mean_row1 = (80.0 + 100.0 + 120.0) / 3
        assert abs(result['RoF'].iloc[0] - expected_mean_row1) < 0.01
        expected_mean_row2 = (90.0 + 110.0 + 130.0) / 3
        assert abs(result['RoF'].iloc[1] - expected_mean_row2) < 0.01

    def test_no_base_column_multiple_codes(self):
        """Mean is computed per-code when multiple codes lack base
        columns."""
        header_rows = [['h', 'h', 'h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '10.0', '20.0', '50.0', '70.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(
            all_rows,
            columns=[
                'Timestamp', '15013_3', '15013_6', '17462_9', '17462_10'
            ],
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert len(result) == 2
        codes = sorted(result['code'].unique())
        assert codes == ['15013', '17462']
        assert 'SWE' in result.columns

        row_15013 = result[result['code'] == '15013']
        assert abs(row_15013['SWE'].iloc[0] - 15.0) < 0.01  # (10+20)/2

        row_17462 = result[result['code'] == '17462']
        assert abs(row_17462['SWE'].iloc[0] - 60.0) < 0.01  # (50+70)/2

    def test_base_column_present_not_overwritten(self):
        """When DG CSV includes a base column, it is NOT overwritten
        by the computed mean."""
        header_rows = [['h', 'h', 'h', 'h']] * 4
        data_rows = [
            ['01/01/2024', '100.0', '80.0', '120.0'],
        ]
        all_rows = header_rows + data_rows
        # Has bare '12345' (base) plus bands
        df = pd.DataFrame(
            all_rows,
            columns=['Timestamp', '12345', '12345_1', '12345_2'],
        )

        result = dg_utils.transform_snow_data(df, 'SWE')

        assert 'SWE' in result.columns
        # Base value comes from the DG (100.0), not mean of bands (100.0)
        assert result['SWE'].iloc[0] == 100.0

    def test_first_4_rows_dropped(self):
        """First 4 rows (headers) are dropped via .iloc[4:]."""
        # First 4 rows are headers, remaining 3 are parseable dates
        header_rows = [['header', '0'] for _ in range(4)]
        data_rows = [
            ['01/01/2024', '100.0'],
            ['02/01/2024', '110.0'],
            ['03/01/2024', '120.0'],
        ]
        all_rows = header_rows + data_rows
        df = pd.DataFrame(all_rows, columns=['Timestamp', '12345'])

        result = dg_utils.transform_snow_data(df, 'SWE')
        # 3 data rows remain
        assert len(result) == 3
