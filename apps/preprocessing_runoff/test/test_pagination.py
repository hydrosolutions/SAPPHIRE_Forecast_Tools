"""
Pagination Edge Case Tests (PR-004)

Regression tests for the pagination bug where sites with both hydro and meteo
data appearing on different pages were misclassified as "meteo-only".

The fix: aggregate ALL pages before classification (not per-page).
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


def create_api_result_item(site_code, station_type, values_data, station_id=None, station_name=None):
    """
    Create a single result item matching the iEasyHydro HF API structure.

    Args:
        site_code: Site code string
        station_type: 'hydro' or 'meteo'
        values_data: List of dicts with 'value' and 'timestamp_local' keys
        station_id: Optional station ID (defaults to site_code as int)
        station_name: Optional station name (defaults to "Station {site_code}")
    """
    # Expand values_data to include all required fields
    full_values = []
    for v in values_data:
        full_values.append({
            'value': v.get('value'),
            'timestamp_local': v.get('timestamp_local'),
            'timestamp_utc': v.get('timestamp_local', '').replace('+06:00', '+00:00'),  # Mock UTC
            'value_code': 'WDDA',
            'value_type': 'daily_average'
        })

    return {
        'station_code': site_code,
        'station_id': station_id or int(site_code),
        'station_name': station_name or f'Station {site_code}',
        'station_type': station_type,
        'data': [{
            'variable_code': 'WDDA',
            'unit': 'm3/s',
            'values': full_values
        }]
    }


def create_paginated_response(results, total_count):
    """Create a paginated API response structure."""
    return {
        'count': total_count,
        'results': results
    }


class TestPaginationBugRegression:
    """
    Regression tests for PR-004: sites appearing as meteo on one page
    and hydro on another being misclassified.
    """

    def test_site_meteo_page1_hydro_page2_keeps_hydro_data(self):
        """
        Core bug scenario: same site appears as meteo on page 1, hydro on page 2.
        Before fix: classified as meteo-only, hydro data lost.
        After fix: hydro data is kept.
        """
        # Page 1: Site 99901 appears as meteo (temperature data)
        page1_results = [
            create_api_result_item('99901', 'meteo', [
                {'value': 15.5, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
            # Other sites on page 1
            create_api_result_item('99902', 'hydro', [
                {'value': 100.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        # Page 2: Site 99901 appears as hydro (discharge data)
        page2_results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 150.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        # Combine all results (simulating aggregation before processing)
        all_results = page1_results + page2_results
        combined_response = {'results': all_results}

        # Process with the fixed function
        result_df = src.process_hydro_HF_data(combined_response)

        # Site 99901 should have hydro data (not be excluded as meteo-only)
        # Note: DataFrame uses 'station_code' and 'value' columns
        site_99901_data = result_df[result_df['station_code'] == '99901']
        assert len(site_99901_data) > 0, "Site 99901 should have hydro data"
        assert site_99901_data['value'].iloc[0] == 150.0

    def test_site_hydro_page1_meteo_page2_keeps_hydro_data(self):
        """
        Reverse scenario: hydro on page 1, meteo on page 2.
        Should still keep hydro data.
        """
        # Page 1: Site 99901 appears as hydro
        page1_results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 200.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        # Page 2: Site 99901 appears as meteo
        page2_results = [
            create_api_result_item('99901', 'meteo', [
                {'value': 20.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        all_results = page1_results + page2_results
        combined_response = {'results': all_results}

        result_df = src.process_hydro_HF_data(combined_response)

        # Site 99901 should have hydro data
        site_99901_data = result_df[result_df['station_code'] == '99901']
        assert len(site_99901_data) > 0, "Site 99901 should have hydro data"
        assert site_99901_data['value'].iloc[0] == 200.0


class TestDualTypeSiteClassification:
    """Tests for sites that have both hydro and meteo data."""

    def test_dual_type_site_keeps_all_hydro_records(self):
        """
        A site with both hydro and meteo records should keep all hydro records.
        """
        # Site appears with both types in same response
        results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 100.0, 'timestamp_local': '2024-01-14T08:00:00'},
                {'value': 110.0, 'timestamp_local': '2024-01-15T08:00:00'},
            ]),
            create_api_result_item('99901', 'meteo', [
                {'value': 15.0, 'timestamp_local': '2024-01-14T08:00:00'},
                {'value': 16.0, 'timestamp_local': '2024-01-15T08:00:00'},
            ]),
        ]

        combined_response = {'results': results}
        result_df = src.process_hydro_HF_data(combined_response)

        # Should have both hydro records
        site_data = result_df[result_df['station_code'] == '99901']
        assert len(site_data) == 2, "Should have 2 hydro records"
        assert set(site_data['value'].tolist()) == {100.0, 110.0}

    def test_meteo_only_site_excluded(self):
        """
        A site with ONLY meteo data (no hydro anywhere) should be excluded.
        """
        results = [
            # Site 99901 has only meteo data
            create_api_result_item('99901', 'meteo', [
                {'value': 15.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
            # Site 99902 has hydro data
            create_api_result_item('99902', 'hydro', [
                {'value': 200.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        combined_response = {'results': results}
        result_df = src.process_hydro_HF_data(combined_response)

        # Site 99901 should NOT be in results (meteo-only)
        assert '99901' not in result_df['station_code'].values, "Meteo-only site should be excluded"
        # Site 99902 should be present
        assert '99902' in result_df['station_code'].values, "Hydro site should be present"


class TestMultiPageAggregation:
    """Tests for aggregation across multiple pages."""

    def test_records_from_multiple_pages_aggregated(self):
        """
        Records from 3+ pages should all be present in final DataFrame.
        """
        # Simulate 3 pages of results for different sites
        page1_results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 100.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
            create_api_result_item('99902', 'hydro', [
                {'value': 200.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        page2_results = [
            create_api_result_item('99903', 'hydro', [
                {'value': 300.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
            create_api_result_item('99904', 'hydro', [
                {'value': 400.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        page3_results = [
            create_api_result_item('99905', 'hydro', [
                {'value': 500.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        # Aggregate all pages
        all_results = page1_results + page2_results + page3_results
        combined_response = {'results': all_results}

        result_df = src.process_hydro_HF_data(combined_response)

        # All 5 sites should be present
        assert len(result_df) == 5
        expected_sites = {'99901', '99902', '99903', '99904', '99905'}
        assert set(result_df['station_code'].values) == expected_sites

    def test_same_site_multiple_dates_across_pages(self):
        """
        Same site with data on different dates across pages should be aggregated.
        """
        page1_results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 100.0, 'timestamp_local': '2024-01-13T08:00:00'},
                {'value': 110.0, 'timestamp_local': '2024-01-14T08:00:00'},
            ]),
        ]

        page2_results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 120.0, 'timestamp_local': '2024-01-15T08:00:00'},
                {'value': 130.0, 'timestamp_local': '2024-01-16T08:00:00'},
            ]),
        ]

        all_results = page1_results + page2_results
        combined_response = {'results': all_results}

        result_df = src.process_hydro_HF_data(combined_response)

        # Should have all 4 records for site 99901
        site_data = result_df[result_df['station_code'] == '99901']
        assert len(site_data) == 4, "Should have 4 records across all pages"
        assert set(site_data['value'].tolist()) == {100.0, 110.0, 120.0, 130.0}


class TestEmptyAndEdgeCases:
    """Tests for empty results and edge cases."""

    def test_empty_results_returns_empty_dataframe(self):
        """Empty API results should return empty DataFrame."""
        combined_response = {'results': []}

        result_df = src.process_hydro_HF_data(combined_response)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 0

    def test_all_meteo_returns_empty_dataframe(self):
        """If all results are meteo-only, should return empty DataFrame."""
        results = [
            create_api_result_item('99901', 'meteo', [
                {'value': 15.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
            create_api_result_item('99902', 'meteo', [
                {'value': 16.0, 'timestamp_local': '2024-01-15T08:00:00'}
            ]),
        ]

        combined_response = {'results': results}
        result_df = src.process_hydro_HF_data(combined_response)

        assert len(result_df) == 0, "All meteo results should return empty DataFrame"

    def test_null_values_skipped(self):
        """Records with null/None values should be skipped."""
        results = [
            create_api_result_item('99901', 'hydro', [
                {'value': 100.0, 'timestamp_local': '2024-01-14T08:00:00'},
                {'value': None, 'timestamp_local': '2024-01-15T08:00:00'},  # Null value
                {'value': 120.0, 'timestamp_local': '2024-01-16T08:00:00'},
            ]),
        ]

        combined_response = {'results': results}
        result_df = src.process_hydro_HF_data(combined_response)

        # Should have only 2 records (null skipped)
        assert len(result_df) == 2
        assert set(result_df['value'].tolist()) == {100.0, 120.0}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
