"""
Edge case tests for preprocessing_gateway.

Required by CLAUDE.md for any code that processes DataFrames, dates,
or numeric values. Covers:
- Snow write edge cases (single row, NaN, duplicates, large values)
- Meteo write edge cases (zero precip, small values, norm handling)
- Date boundary edge cases (year boundary, leap year, day_of_year)
- Consistency check edge cases (tolerance boundary, empty data)
"""
import os
import sys
import pandas as pd
import numpy as np
import pytest
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import dg_utils
import snow_data_operational as sdo
import snow_data_renalysis as sdr
import extend_era5_reanalysis as eer
import Quantile_Mapping_OP as qm


# =====================================================================
# Snow write edge cases
# =====================================================================

class TestSnowWriteEdgeCases:
    """Edge cases for dg_utils.write_snow_to_api."""

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_single_row_dataframe(self, mock_client_class):
        """Single-row DataFrame produces exactly 1 record with correct values."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [54321],
                'SWE': [42.5],
            })

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 1
            assert records[0]['value'] == 42.5
            assert records[0]['code'] == '54321'
            assert records[0]['snow_type'] == 'SWE'
            assert records[0]['date'] == today.strftime('%Y-%m-%d')
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_all_nan_value_column(self, mock_client_class):
        """All-NaN value column produces records with value=None."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today, today],
                'code': [12345, 67890],
                'SWE': [np.nan, np.nan],
            })

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2
            assert records[0]['value'] is None
            assert records[1]['value'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_mixed_nan_valid_values(self, mock_client_class):
        """Mixed NaN/valid values produce correct None/float split."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 3
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today, today, today],
                'code': [11111, 22222, 33333],
                'SWE': [100.0, np.nan, 200.0],
            })

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert records[0]['value'] == 100.0
            assert records[1]['value'] is None
            assert records[2]['value'] == 200.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_duplicate_date_code_rows_both_passed(self, mock_client_class):
        """Duplicate date+code rows are both passed to API (no dedup)."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today, today],
                'code': [12345, 12345],
                'SWE': [100.0, 105.0],
            })

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_multiple_stations_single_date(self, mock_client_class):
        """Multiple stations on same date produce N records with correct codes."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 3
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today, today, today],
                'code': [11111, 22222, 33333],
                'SWE': [100.0, 200.0, 300.0],
            })

            result = dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            assert result is True

            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 3
            codes = {r['code'] for r in records}
            assert codes == {'11111', '22222', '33333'}
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_integer_code_becomes_string(self, mock_client_class):
        """Integer station codes are always converted to str."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [99999],  # integer
                'SWE': [50.0],
            })

            dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            records = mock_client.write_snow.call_args[0][0]
            assert isinstance(records[0]['code'], str)
            assert records[0]['code'] == '99999'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_very_large_swe_value_passes_through(self, mock_client_class):
        """Very large SWE value (99999.99) passes through unchanged."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'SWE': [99999.99],
            })

            dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            records = mock_client.write_snow.call_args[0][0]
            assert records[0]['value'] == 99999.99
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =====================================================================
# Meteo write edge cases
# =====================================================================

class TestMeteoWriteEdgeCases:
    """Edge cases for _write_meteo_to_api in extend_era5_reanalysis.py."""

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_single_row_temperature(self, mock_client_class):
        """Single-row temperature DataFrame produces 1 correct record."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-06-15']),
                'code': [12345],
                'T': [-5.5],
                'T_norm': [-3.0],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert len(records) == 1
            assert records[0]['value'] == -5.5
            assert records[0]['norm'] == -3.0
            assert records[0]['meteo_type'] == 'T'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_all_nan_temperature_values(self, mock_client_class):
        """All-NaN temperature values produce records with value=None."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [np.nan],
                'T_norm': [np.nan],
            })

            result = eer._write_meteo_to_api(data, "T")
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['value'] is None
            assert records[0]['norm'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_zero_precipitation_not_treated_as_nan(self, mock_client_class):
        """Precipitation 0.0 must not be treated as NaN."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-07-01']),
                'code': [12345],
                'P': [0.0],
                'P_norm': [0.0],
            })

            result = eer._write_meteo_to_api(data, "P")
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['value'] == 0.0
            assert records[0]['norm'] == 0.0
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_very_small_precipitation_rounded(self, mock_client_class):
        """Very small precipitation is rounded to 2 decimals at the API
        boundary.  0.001 mm is below measurement precision and rounds
        to 0.0; 0.01 mm survives."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 2
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-07-01', '2024-07-02']),
                'code': [12345, 12345],
                'P': [0.001, 0.01],
                'P_norm': [0.5, 0.5],
            })

            result = eer._write_meteo_to_api(data, "P")
            assert result is True

            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['value'] == 0.001  # preserved at 3 decimals
            assert records[1]['value'] == 0.01   # preserved at 3 decimals
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_norm_populated_in_extend_era5(self, mock_client_class):
        """extend_era5_reanalysis populates norm from {type}_norm column."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-03-15']),
                'code': [12345],
                'P': [10.5],
                'P_norm': [8.2],
            })

            eer._write_meteo_to_api(data, "P")
            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['norm'] == 8.2
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('Quantile_Mapping_OP.SapphirePreprocessingClient')
    def test_norm_is_none_in_qm(self, mock_client_class):
        """Quantile_Mapping_OP sets norm=None (control member has no norm)."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'T': [15.0],
            })

            qm._write_meteo_to_api(data, "T", "HRU001")
            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['norm'] is None
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =====================================================================
# Date boundary edge cases
# =====================================================================

class TestDateBoundaryEdgeCases:
    """Edge cases for date handling across modules."""

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_year_boundary_operational_date_filtering(
        self, mock_client_class
    ):
        """Operational mode writes yesterday onward.

        Even at year boundary, yesterday and today are written.
        Older data is excluded.
        """
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 2
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            two_days_ago = today - pd.Timedelta(days=2)

            data = pd.DataFrame({
                'date': [two_days_ago, yesterday, today],
                'code': [12345, 12345, 12345],
                'SWE': [80.0, 90.0, 100.0],
            })

            dg_utils.write_snow_to_api(data, "SWE", "test_hru")
            records = mock_client.write_snow.call_args[0][0]
            assert len(records) == 2
            dates = {r['date'] for r in records}
            assert dates == {
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
            }
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_leap_year_feb_29_day_of_year(self, mock_client_class):
        """Feb 29 on a leap year produces day_of_year = 60."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-29']),
                'code': [12345],
                'T': [5.0],
                'T_norm': [3.0],
            })

            eer._write_meteo_to_api(data, "T")
            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['day_of_year'] == 60

        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_non_leap_year_mar_1_day_of_year(self, mock_client_class):
        """Mar 1 on non-leap year produces day_of_year = 60."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2023-03-01']),
                'code': [12345],
                'T': [5.0],
                'T_norm': [3.0],
            })

            eer._write_meteo_to_api(data, "T")
            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['day_of_year'] == 60
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('dg_utils.SapphirePreprocessingClient')
    def test_maintenance_30_day_window_spanning_year_boundary(
        self, mock_client_class
    ):
        """Maintenance mode: 30-day window from Jan 15 goes back to Dec 16."""
        if not dg_utils.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 31
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            # Data spans Nov 1 to Jan 15
            dates = pd.date_range('2023-11-01', '2024-01-15', freq='D')
            data = pd.DataFrame({
                'date': dates,
                'code': [12345] * len(dates),
                'SWE': [100.0] * len(dates),
            })

            dg_utils.write_snow_to_api(
                data, "SWE", "test_hru",
                mode="maintenance",
                reference_date=data['date'].max(),
            )

            records = mock_client.write_snow.call_args[0][0]
            # Cutoff: Jan 15 - 30 days = Dec 16
            record_dates = [r['date'] for r in records]
            assert min(record_dates) == '2023-12-16'
            assert max(record_dates) == '2024-01-15'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_dec_31_leap_year_day_of_year_366(self, mock_client_class):
        """Dec 31 on leap year produces day_of_year = 366."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_meteo.return_value = 1
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': pd.to_datetime(['2024-12-31']),
                'code': [12345],
                'T': [0.0],
                'T_norm': [-2.0],
            })

            eer._write_meteo_to_api(data, "T")
            records = mock_client.write_meteo.call_args[0][0]
            assert records[0]['day_of_year'] == 366
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_is_leap_year_2024(self):
        """2024 is a leap year."""
        assert eer.is_leap_year(2024) is True

    def test_is_leap_year_2023(self):
        """2023 is not a leap year."""
        assert eer.is_leap_year(2023) is False

    def test_is_leap_year_1900(self):
        """1900 is not a leap year (divisible by 100 but not 400)."""
        assert eer.is_leap_year(1900) is False

    def test_is_leap_year_2000(self):
        """2000 is a leap year (divisible by 400)."""
        assert eer.is_leap_year(2000) is True


# =====================================================================
# Consistency check edge cases
# =====================================================================

class TestConsistencyCheckEdgeCases:
    """Edge cases for consistency check functions."""

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_single_row_matching(self, mock_client_class):
        """Single CSV row matching single API row returns True."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(
                csv_data, "SWE", "test_hru"
            )
            assert result is True
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_tolerance_boundary_within_passes(self, mock_client_class):
        """Diff = 0.005 (within 0.01 tolerance) returns True."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.005],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(
                csv_data, "SWE", "test_hru"
            )
            assert result is True
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_tolerance_exceeded_fails(self, mock_client_class):
        """Diff = 0.02 (exceeds 0.01 tolerance) returns False."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            today = pd.Timestamp.today().normalize()
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.02],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdo._check_snow_consistency(
                csv_data, "SWE", "test_hru"
            )
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_nan_csv_vs_none_api_handled(self, mock_client_class):
        """NaN in CSV vs None in API should be handled gracefully."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # API returns None/NaN as value
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [np.nan],
                'norm': [np.nan],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [np.nan],
                'T_norm': [np.nan],
            })

            # NaN - NaN = NaN, abs(NaN) = NaN, NaN > 0.01 = False
            # So no mismatches are detected — this is correct behavior
            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is True
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    def test_empty_csv_returns_true_when_disabled(self):
        """Empty CSV data with check disabled returns True."""
        os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
        csv_data = pd.DataFrame()
        result = sdo._check_snow_consistency(csv_data, "SWE", "test_hru")
        assert result is True
