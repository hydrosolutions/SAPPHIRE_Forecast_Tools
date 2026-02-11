"""
Tests for spot-check validation functionality.

Tests for get_spot_check_sites(), spot_check_sites(), spot_check_sites_dual(),
and run_spot_check_validation() functions.
Part of PR-004 test coverage.
"""

import os
import sys
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from unittest.mock import patch, MagicMock

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


# Helper to get a recent date string for testing (yesterday)
def get_recent_date_str():
    """Return yesterday's date as string YYYY-MM-DD."""
    yesterday = dt.date.today() - dt.timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def get_recent_datetime_str():
    """Return yesterday's datetime as ISO string with T separator."""
    yesterday = dt.date.today() - dt.timedelta(days=1)
    return yesterday.strftime('%Y-%m-%dT08:00:00')


class TestGetSpotCheckSites:
    """Tests for get_spot_check_sites() environment variable parsing."""

    def test_returns_empty_list_when_env_not_set(self):
        """Returns empty list when IEASYHYDRO_SPOTCHECK_SITES is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the variable if it exists
            os.environ.pop('IEASYHYDRO_SPOTCHECK_SITES', None)
            result = src.get_spot_check_sites()
            assert result == []

    def test_returns_empty_list_when_env_is_empty(self):
        """Returns empty list when IEASYHYDRO_SPOTCHECK_SITES is empty string."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': ''}):
            result = src.get_spot_check_sites()
            assert result == []

    def test_parses_single_site_code(self):
        """Parses single site code correctly."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': '99901'}):
            result = src.get_spot_check_sites()
            assert result == ['99901']

    def test_parses_multiple_site_codes(self):
        """Parses multiple comma-separated site codes."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': '99901,99902,99903'}):
            result = src.get_spot_check_sites()
            assert result == ['99901', '99902', '99903']

    def test_strips_whitespace_from_codes(self):
        """Strips whitespace from site codes."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': ' 99901 , 99902 , 99903 '}):
            result = src.get_spot_check_sites()
            assert result == ['99901', '99902', '99903']

    def test_ignores_empty_entries(self):
        """Ignores empty entries from extra commas."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': '99901,,99902,,,99903'}):
            result = src.get_spot_check_sites()
            assert result == ['99901', '99902', '99903']

    def test_handles_whitespace_only_entries(self):
        """Handles entries that are only whitespace."""
        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': '99901,   ,99902'}):
            result = src.get_spot_check_sites()
            assert result == ['99901', '99902']


class TestSpotCheckSitesComparison:
    """Tests for spot_check_sites() comparison logic."""

    def create_api_response(self, value, timestamp):
        """Create properly formatted API response structure."""
        return {
            'count': 1,
            'results': [{
                'station_type': 'hydro',
                'data': [{
                    'values': [{
                        'value': value,
                        'timestamp_local': timestamp
                    }]
                }]
            }]
        }

    def create_mock_sdk(self, api_response):
        """Create a mock SDK that returns the specified response."""
        mock_sdk = MagicMock()
        mock_sdk.get_data_values_for_site.return_value = api_response
        return mock_sdk

    def create_output_df(self, data):
        """Create output DataFrame for testing."""
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df

    def test_match_when_values_equal(self):
        """Reports match when output and API values are equal."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [123.45]
        })

        api_response = self.create_api_response(123.45, recent_datetime)
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == True
        assert result['summary']['matched'] == 1
        assert result['summary']['mismatched'] == 0

    def test_match_within_tolerance(self):
        """Reports match when values differ by less than tolerance (0.01)."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [123.456]
        })

        # Difference of 0.004 - within tolerance
        api_response = self.create_api_response(123.46, recent_datetime)
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == True

    def test_mismatch_when_values_differ(self):
        """Reports mismatch when values differ beyond tolerance."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [123.45]
        })

        # Significant difference
        api_response = self.create_api_response(150.00, recent_datetime)
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == False
        assert result['summary']['mismatched'] == 1

    def test_error_when_no_api_data(self):
        """Reports error when API returns no data."""
        recent_date = get_recent_date_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [123.45]
        })

        api_response = {
            'count': 0,
            'results': []
        }
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['error'] is not None
        assert result['summary']['errors'] == 1

    def test_error_when_site_not_in_output(self):
        """Reports error when site code not found in output DataFrame."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99902'],  # Different site
            'discharge': [123.45]
        })

        api_response = self.create_api_response(123.45, recent_datetime)
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],  # Not in output_df
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['error'] is not None

    def test_multiple_sites(self):
        """Handles multiple sites correctly."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date, recent_date],
            'code': ['99901', '99902'],
            'discharge': [100.0, 200.0]
        })

        def create_response(value):
            return {
                'count': 1,
                'results': [{
                    'station_type': 'hydro',
                    'data': [{
                        'values': [{
                            'value': value,
                            'timestamp_local': recent_datetime
                        }]
                    }]
                }]
            }

        # Mock SDK to return different responses per call
        mock_sdk = MagicMock()
        mock_sdk.get_data_values_for_site.side_effect = [
            create_response(100.0),  # Match
            create_response(250.0),  # Mismatch
        ]

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901', '99902'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == True
        assert result['results']['99902']['match'] == False
        assert result['summary']['matched'] == 1
        assert result['summary']['mismatched'] == 1


class TestSpotCheckSitesDual:
    """Tests for spot_check_sites_dual() combined logic."""

    def create_api_response(self, value, timestamp):
        """Create properly formatted API response structure."""
        return {
            'count': 1,
            'results': [{
                'station_type': 'hydro',
                'data': [{
                    'values': [{
                        'value': value,
                        'timestamp_local': timestamp
                    }]
                }]
            }]
        }

    def create_mock_sdk(self, wdda_value, wdd_value, timestamp):
        """Create mock SDK returning different responses for WDDA vs WDD."""
        mock_sdk = MagicMock()

        def side_effect(filters):
            if 'WDDA' in filters.get('variable_names', []):
                return self.create_api_response(wdda_value, timestamp)
            elif 'WDD' in filters.get('variable_names', []):
                return self.create_api_response(wdd_value, timestamp)
            return {'count': 0, 'results': []}

        mock_sdk.get_data_values_for_site.side_effect = side_effect
        return mock_sdk

    def create_output_df(self, data):
        """Create output DataFrame for testing."""
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df

    def test_passes_when_wdda_matches(self):
        """Site passes when WDDA matches even if WDD doesn't."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [100.0]
        })

        # WDDA matches (100.0), WDD differs (150.0)
        mock_sdk = self.create_mock_sdk(100.0, 150.0, recent_datetime)

        result = src.spot_check_sites_dual(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df
        )

        assert result['combined_summary']['matched'] == 1
        assert result['combined_summary']['mismatched'] == 0

    def test_passes_when_wdd_matches(self):
        """Site passes when WDD matches even if WDDA doesn't."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [100.0]
        })

        # WDDA differs (150.0), WDD matches (100.0)
        mock_sdk = self.create_mock_sdk(150.0, 100.0, recent_datetime)

        result = src.spot_check_sites_dual(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df
        )

        assert result['combined_summary']['matched'] == 1

    def test_mismatch_when_both_differ(self):
        """Site mismatches only when both WDDA and WDD differ."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        output_df = self.create_output_df({
            'date': [recent_date],
            'code': ['99901'],
            'discharge': [100.0]
        })

        # Both differ: WDDA=150.0, WDD=200.0, output=100.0
        mock_sdk = self.create_mock_sdk(150.0, 200.0, recent_datetime)

        result = src.spot_check_sites_dual(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df
        )

        assert result['combined_summary']['mismatched'] == 1


class TestRunSpotCheckValidation:
    """Tests for run_spot_check_validation() workflow."""

    def create_api_response(self, value, timestamp):
        """Create properly formatted API response structure."""
        return {
            'count': 1,
            'results': [{
                'station_type': 'hydro',
                'data': [{
                    'values': [{
                        'value': value,
                        'timestamp_local': timestamp
                    }]
                }]
            }]
        }

    def test_returns_empty_when_no_sites_configured(self):
        """Returns empty result when no spot-check sites configured."""
        recent_date = get_recent_date_str()

        mock_sdk = MagicMock()
        output_df = pd.DataFrame({
            'date': pd.to_datetime([recent_date]),
            'code': ['99901'],
            'discharge': [100.0]
        })

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop('IEASYHYDRO_SPOTCHECK_SITES', None)
            result = src.run_spot_check_validation(
                sdk=mock_sdk,
                output_df=output_df
            )

        # When no sites configured, returns structure with zero counts
        assert result['combined_summary']['checked'] == 0

    def test_calls_spot_check_when_sites_configured(self):
        """Calls spot-check validation when sites are configured."""
        recent_date = get_recent_date_str()
        recent_datetime = get_recent_datetime_str()

        mock_sdk = MagicMock()
        mock_sdk.get_data_values_for_site.return_value = self.create_api_response(100.0, recent_datetime)

        output_df = pd.DataFrame({
            'date': pd.to_datetime([recent_date]),
            'code': ['99901'],
            'discharge': [100.0]
        })

        with patch.dict(os.environ, {'IEASYHYDRO_SPOTCHECK_SITES': '99901'}):
            result = src.run_spot_check_validation(
                sdk=mock_sdk,
                output_df=output_df
            )

        assert 'combined_summary' in result
        assert result['combined_summary']['checked'] == 1


class TestSpotCheckDateParsing:
    """Tests for date parsing in spot-check validation."""

    def create_api_response(self, value, timestamp):
        """Create properly formatted API response structure."""
        return {
            'count': 1,
            'results': [{
                'station_type': 'hydro',
                'data': [{
                    'values': [{
                        'value': value,
                        'timestamp_local': timestamp
                    }]
                }]
            }]
        }

    def create_mock_sdk(self, api_response):
        """Create a mock SDK that returns the specified response."""
        mock_sdk = MagicMock()
        mock_sdk.get_data_values_for_site.return_value = api_response
        return mock_sdk

    def test_parses_iso_format_with_t_separator(self):
        """Parses ISO format date with T separator."""
        recent_date = get_recent_date_str()

        output_df = pd.DataFrame({
            'date': pd.to_datetime([recent_date]),
            'code': ['99901'],
            'discharge': [100.0]
        })

        # ISO format with T separator
        api_response = self.create_api_response(100.0, f'{recent_date}T08:30:00')
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == True
        assert result['results']['99901']['api_date'] == recent_date

    def test_parses_date_only_format(self):
        """Parses date-only format (YYYY-MM-DD)."""
        recent_date = get_recent_date_str()

        output_df = pd.DataFrame({
            'date': pd.to_datetime([recent_date]),
            'code': ['99901'],
            'discharge': [100.0]
        })

        # Date only format
        api_response = self.create_api_response(100.0, recent_date)
        mock_sdk = self.create_mock_sdk(api_response)

        result = src.spot_check_sites(
            sdk=mock_sdk,
            site_codes=['99901'],
            output_df=output_df,
            variable_name='WDDA'
        )

        assert result['results']['99901']['match'] == True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
