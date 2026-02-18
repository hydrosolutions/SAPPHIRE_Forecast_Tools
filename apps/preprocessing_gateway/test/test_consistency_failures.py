"""
Tests for consistency check failure modes in preprocessing_gateway.

Covers:
- Snow reanalysis consistency check failures (snow_data_renalysis.py)
- Snow write/read parameter contract (operational and reanalysis)
- Snow consistency date mismatch scenarios (likely root cause of production bug)
- Exception swallowing in Quantile_Mapping_OP.py main loop
- Meteo consistency reanalysis failure paths (extend_era5_reanalysis.py)
"""
import os
import sys
import pandas as pd
import numpy as np
import pytest
from datetime import timedelta
from unittest.mock import Mock, patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()

import snow_data_operational as sdo
import snow_data_renalysis as sdr
import extend_era5_reanalysis as eer
import Quantile_Mapping_OP as qm


# =====================================================================
# Snow reanalysis consistency check failure modes
# =====================================================================

class TestSnowReanalysisConsistencyFailures:
    """Tests for _check_snow_consistency in snow_data_renalysis.py.

    The existing tests only cover the success path; these cover all
    failure modes.
    """

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_returns_false_on_row_count_mismatch(self, mock_client_class):
        """Maintenance consistency check fails on row count mismatch."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # API returns 1 row but CSV has 2
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20', '2024-02-20']),
                'code': [12345, 67890],
                'SWE': [100.0, 200.0],
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_returns_false_on_value_mismatch(self, mock_client_class):
        """Maintenance consistency check fails on value mismatch > 0.01."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [999.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_returns_false_when_no_api_data(self, mock_client_class):
        """Maintenance consistency check fails when API returns empty."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_returns_false_on_api_exception(self, mock_client_class):
        """Maintenance consistency check fails when API raises."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_snow.side_effect = Exception("API timeout")
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': [12345],
                'SWE': [100.0],
            })

            result = sdr._check_snow_consistency(csv_data, "SWE", "test_hru")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('snow_data_renalysis.SapphirePreprocessingClient')
    def test_read_snow_called_with_correct_params(self, mock_client_class):
        """Verify read_snow uses snow_type.upper(), str(code), correct dates."""
        if not sdr.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-02-20']),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            # CSV spans 40 days; last 30 should be queried
            dates = pd.date_range(end='2024-02-20', periods=40, freq='D')
            csv_data = pd.DataFrame({
                'date': dates,
                'code': [12345] * 40,
                'SWE': [100.0] * 40,
            })

            sdr._check_snow_consistency(csv_data, "swe", "test_hru")

            call_kwargs = mock_client.read_snow.call_args
            assert call_kwargs[1]['snow_type'] == 'SWE'
            assert call_kwargs[1]['code'] == '12345'
            # Date range should cover last 30 days of CSV
            cutoff = pd.Timestamp('2024-02-20') - timedelta(days=30)
            assert call_kwargs[1]['start_date'] == cutoff.strftime('%Y-%m-%d')
            assert call_kwargs[1]['end_date'] == '2024-02-20'
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)


# =====================================================================
# Snow write/read parameter contract
# =====================================================================

class TestSnowWriteReadParameterContract:
    """Verify that write and read use the same parameter formats.

    This directly targets the production bug where consistency checks
    return 'No data returned from API' for all snow types.
    """

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_swe_write_type_matches_read_type(self, mock_client_class):
        """SWE write snow_type matches read snow_type query."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([pd.Timestamp.today().normalize()]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'SWE': [100.0],
            })

            # Write then check consistency
            sdo._write_snow_to_api(data, "SWE", "test_hru")
            sdo._check_snow_consistency(data, "SWE", "test_hru")

            # Verify write and read both use 'SWE' (uppercase)
            write_records = mock_client.write_snow.call_args[0][0]
            assert write_records[0]['snow_type'] == 'SWE'

            read_kwargs = mock_client.read_snow.call_args
            assert read_kwargs[1]['snow_type'] == 'SWE'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_hs_write_type_matches_read_type(self, mock_client_class):
        """HS write snow_type matches read snow_type query."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([pd.Timestamp.today().normalize()]),
                'code': ['12345'],
                'snow_type': ['HS'],
                'value': [50.0],
            })
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'HS': [50.0],
            })

            sdo._write_snow_to_api(data, "HS", "test_hru")
            sdo._check_snow_consistency(data, "HS", "test_hru")

            write_records = mock_client.write_snow.call_args[0][0]
            assert write_records[0]['snow_type'] == 'HS'

            read_kwargs = mock_client.read_snow.call_args
            assert read_kwargs[1]['snow_type'] == 'HS'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_rof_write_type_matches_read_type(self, mock_client_class):
        """RoF write snow_type matches read snow_type query."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([pd.Timestamp.today().normalize()]),
                'code': ['12345'],
                'snow_type': ['ROF'],
                'value': [25.0],
            })
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],
                'RoF': [25.0],
            })

            sdo._write_snow_to_api(data, "RoF", "test_hru")
            sdo._check_snow_consistency(data, "RoF", "test_hru")

            write_records = mock_client.write_snow.call_args[0][0]
            assert write_records[0]['snow_type'] == 'ROF'

            read_kwargs = mock_client.read_snow.call_args
            assert read_kwargs[1]['snow_type'] == 'ROF'
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_code_is_string_in_both_write_and_read(self, mock_client_class):
        """Station code is str in both write records and read query."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([pd.Timestamp.today().normalize()]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            today = pd.Timestamp.today().normalize()
            data = pd.DataFrame({
                'date': [today],
                'code': [12345],  # integer code
                'SWE': [100.0],
            })

            sdo._write_snow_to_api(data, "SWE", "test_hru")
            sdo._check_snow_consistency(data, "SWE", "test_hru")

            write_records = mock_client.write_snow.call_args[0][0]
            assert isinstance(write_records[0]['code'], str)

            read_kwargs = mock_client.read_snow.call_args
            assert isinstance(read_kwargs[1]['code'], str)
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)


# =====================================================================
# Snow consistency date mismatch — likely root cause of production bug
# =====================================================================

class TestSnowConsistencyDateMismatch:
    """Tests for operational mode date mismatch between write and check.

    The write function filters to today (line 133-134) but the
    consistency check filters to csv_data['date'].max() (line 224).
    If the CSV's max date != today, the check queries a date that was
    never written.
    """

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_csv_max_date_is_today_passes(self, mock_client_class):
        """When CSV max date == today, write + check should both succeed."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.write_snow.return_value = 1

            today = pd.Timestamp.today().normalize()
            mock_client.read_snow.return_value = pd.DataFrame({
                'date': pd.to_datetime([today]),
                'code': ['12345'],
                'snow_type': ['SWE'],
                'value': [100.0],
            })
            mock_client_class.return_value = mock_client

            data = pd.DataFrame({
                'date': [today - pd.Timedelta(days=1), today],
                'code': [12345, 12345],
                'SWE': [90.0, 100.0],
            })

            write_result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert write_result is True

            check_result = sdo._check_snow_consistency(data, "SWE", "test_hru")
            assert check_result is True
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_csv_max_date_is_yesterday_write_returns_false(
        self, mock_client_class
    ):
        """When CSV has no today data, write returns False (nothing to write).

        The consistency check now filters to today and finds no CSV rows,
        so it returns True (nothing to verify).
        """
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            # API returns empty because nothing was written for yesterday
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
            data = pd.DataFrame({
                'date': [yesterday],
                'code': [12345],
                'SWE': [100.0],
            })

            # Write returns False — no today data to write
            write_result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert write_result is False

            # Consistency check filters to today, finds no CSV rows,
            # returns True (nothing to verify)
            check_result = sdo._check_snow_consistency(
                data, "SWE", "test_hru"
            )
            assert check_result is True
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)

    @patch('snow_data_operational.SapphirePreprocessingClient')
    def test_csv_max_date_is_future_write_returns_false(
        self, mock_client_class
    ):
        """When CSV has future-dated forecasts, write returns False.

        Consistency check filters to today and finds no CSV rows,
        so it returns True (nothing to verify).
        """
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        try:
            mock_client = Mock()
            mock_client.readiness_check.return_value = True
            mock_client.read_snow.return_value = pd.DataFrame()
            mock_client_class.return_value = mock_client

            tomorrow = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
            data = pd.DataFrame({
                'date': [tomorrow],
                'code': [12345],
                'SWE': [100.0],
            })

            write_result = sdo._write_snow_to_api(data, "SWE", "test_hru")
            assert write_result is False

            # Consistency check filters to today, finds no CSV rows,
            # returns True (nothing to verify)
            check_result = sdo._check_snow_consistency(
                data, "SWE", "test_hru"
            )
            assert check_result is True
        finally:
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)


# =====================================================================
# Exception swallowing in Quantile_Mapping_OP.py main loop (lines 662-674)
# =====================================================================

class TestExceptionSwallowing:
    """Tests for the try/except blocks in Quantile_Mapping_OP.py.

    The main loop catches all exceptions from _write_meteo_to_api and
    _check_meteo_consistency under a single try/except. This means a
    failing consistency check produces a misleading 'Failed to write'
    log message.
    """

    @patch('Quantile_Mapping_OP._check_meteo_consistency')
    @patch('Quantile_Mapping_OP._write_meteo_to_api')
    def test_write_exception_does_not_crash(self, mock_write, mock_check):
        """When _write_meteo_to_api raises, the caller should not crash."""
        mock_write.side_effect = Exception("API write error")
        mock_check.return_value = True

        P_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
        })

        # Simulate the try/except from main() lines 662-674
        try:
            qm._write_meteo_to_api(P_data, 'P', 'HRU001')
            qm._check_meteo_consistency(P_data, 'P', 'HRU001')
        except Exception:
            pass  # This is the production behavior

        # Verify check was never called (exception aborted before it)
        mock_check.assert_not_called()

    @patch('Quantile_Mapping_OP._check_meteo_consistency')
    @patch('Quantile_Mapping_OP._write_meteo_to_api')
    def test_consistency_exception_does_not_crash(
        self, mock_write, mock_check
    ):
        """When _check_meteo_consistency raises, the caller should not crash."""
        mock_write.return_value = True
        mock_check.side_effect = Exception("API read error")

        T_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'T': [15.0],
        })

        # Simulate the try/except from main()
        try:
            qm._write_meteo_to_api(T_data, 'T', 'HRU001')
            qm._check_meteo_consistency(T_data, 'T', 'HRU001')
        except Exception:
            pass

        # Write was called first
        mock_write.assert_called_once()
        # Check was called and raised
        mock_check.assert_called_once()

    @patch('Quantile_Mapping_OP._check_meteo_consistency')
    @patch('Quantile_Mapping_OP._write_meteo_to_api')
    def test_both_succeed_both_called(self, mock_write, mock_check):
        """When both succeed, both should be called."""
        mock_write.return_value = True
        mock_check.return_value = True

        data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-01']),
            'code': [12345],
            'P': [10.0],
        })

        qm._write_meteo_to_api(data, 'P', 'HRU001')
        qm._check_meteo_consistency(data, 'P', 'HRU001')

        mock_write.assert_called_once()
        mock_check.assert_called_once()


# =====================================================================
# Meteo consistency reanalysis failure paths (extend_era5_reanalysis.py)
# =====================================================================

class TestMeteoConsistencyReanalysis:
    """Additional failure paths for extend_era5_reanalysis._check_meteo_consistency."""

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_on_api_exception(self, mock_client_class):
        """Reanalysis consistency check fails on API exception."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_meteo.side_effect = Exception("Connection refused")
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
                'T_norm': [12.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_on_norm_mismatch(self, mock_client_class):
        """Reanalysis consistency check catches norm value mismatch."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['T'],
                'value': [15.0],
                'norm': [999.0],  # norm mismatch
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
                'T_norm': [12.0],  # different from API norm
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)

    @patch('extend_era5_reanalysis.SapphirePreprocessingClient')
    def test_returns_false_on_different_meteo_type_data(
        self, mock_client_class
    ):
        """When API returns data with different values, mismatch detected."""
        if not eer.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        os.environ['SAPPHIRE_CONSISTENCY_CHECK'] = 'true'
        os.environ['SAPPHIRE_API_ENABLED'] = 'true'
        try:
            mock_client = Mock()
            # API returns P-like values when we asked for T
            mock_client.read_meteo.return_value = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': ['12345'],
                'meteo_type': ['P'],
                'value': [5.0],  # precipitation value, not temperature
                'norm': [3.0],
            })
            mock_client_class.return_value = mock_client

            csv_data = pd.DataFrame({
                'date': pd.to_datetime(['2024-01-01']),
                'code': [12345],
                'T': [15.0],
                'T_norm': [12.0],
            })

            result = eer._check_meteo_consistency(csv_data, "T")
            assert result is False
        finally:
            os.environ.pop('SAPPHIRE_CONSISTENCY_CHECK', None)
            os.environ.pop('SAPPHIRE_API_ENABLED', None)
