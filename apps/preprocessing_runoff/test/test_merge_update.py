"""
Unit tests for _merge_with_update() function (PREPQ-005 Phase 1).

These tests verify the core merge logic used in maintenance mode.
The function merges new API data into cached data, with the following behavior:
- New data overwrites cached data where keys (code, date) match
- New rows are added when they don't exist in cached data
- Cached rows are preserved when no corresponding new data exists

Test data is synthetic and generated in the tests (not copied from production).
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


class TestMergeWithUpdate:
    """Unit tests for _merge_with_update() function."""

    def test_merge_update_newer_timestamp(self):
        """
        Test that newer API data overwrites cached data.

        When new_data contains a row with the same (code, date) as existing data,
        the discharge value should be updated to the new value.
        """
        # Cached data with old discharge values
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'B'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01']),
            'discharge': [100.0, 110.0, 200.0]
        })

        # New API data with updated values for some dates
        new_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'discharge': [105.5, 115.5]  # Updated values
        })

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Verify the updated values
        result_a_jan1 = result[
            (result['code'] == 'A') & (result['date'] == pd.Timestamp('2024-01-01'))
        ]
        result_a_jan2 = result[
            (result['code'] == 'A') & (result['date'] == pd.Timestamp('2024-01-02'))
        ]

        assert len(result_a_jan1) == 1, "Should have exactly one row for A, 2024-01-01"
        assert len(result_a_jan2) == 1, "Should have exactly one row for A, 2024-01-02"

        # New values should overwrite old values
        assert result_a_jan1['discharge'].iloc[0] == 105.5, (
            f"Expected discharge 105.5 (from new_data), got {result_a_jan1['discharge'].iloc[0]}"
        )
        assert result_a_jan2['discharge'].iloc[0] == 115.5, (
            f"Expected discharge 115.5 (from new_data), got {result_a_jan2['discharge'].iloc[0]}"
        )

        # Station B should be unchanged (no new data for it)
        result_b = result[result['code'] == 'B']
        assert len(result_b) == 1, "Station B should have 1 row"
        assert result_b['discharge'].iloc[0] == 200.0, "Station B discharge should be unchanged"

    def test_merge_keep_older_if_no_new(self):
        """
        Test that cached data is preserved when no new data exists for that date.

        When new_data doesn't contain a row for a (code, date) combination that
        exists in cached data, the cached value should be preserved.
        """
        # Cached data with 5 days of data
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
            ]),
            'discharge': [100.0, 110.0, 120.0, 130.0, 140.0]
        })

        # New data only for days 4 and 5 (days 1-3 should be preserved)
        new_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-01-04', '2024-01-05']),
            'discharge': [135.0, 145.0]  # Updated values for days 4-5
        })

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Should have all 5 rows
        assert len(result) == 5, f"Expected 5 rows, got {len(result)}"

        # Sort by date for easier verification
        result = result.sort_values('date').reset_index(drop=True)

        # Days 1-3 should preserve original values
        assert result.iloc[0]['discharge'] == 100.0, "Day 1 should be preserved (100.0)"
        assert result.iloc[1]['discharge'] == 110.0, "Day 2 should be preserved (110.0)"
        assert result.iloc[2]['discharge'] == 120.0, "Day 3 should be preserved (120.0)"

        # Days 4-5 should have updated values
        assert result.iloc[3]['discharge'] == 135.0, "Day 4 should be updated (135.0)"
        assert result.iloc[4]['discharge'] == 145.0, "Day 5 should be updated (145.0)"

    def test_merge_handles_overlapping_ranges(self):
        """
        Test that partial overlap between cached and new data is handled correctly.

        When cached data and new data have partially overlapping date ranges:
        - Overlapping dates should use new data values
        - Non-overlapping dates in cached data should be preserved
        - Non-overlapping dates in new data should be added
        """
        # Cached data: Jan 1-5
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
            ]),
            'discharge': [100.0, 110.0, 120.0, 130.0, 140.0]
        })

        # New data: Jan 3-7 (overlaps on Jan 3-5, extends to Jan 6-7)
        new_data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-06', '2024-01-07'
            ]),
            'discharge': [125.0, 135.0, 145.0, 155.0, 165.0]  # Different values
        })

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Should have 7 rows (Jan 1-7)
        assert len(result) == 7, f"Expected 7 rows, got {len(result)}"

        # Sort by date for verification
        result = result.sort_values('date').reset_index(drop=True)

        # Verify each row
        expected = [
            ('2024-01-01', 100.0),  # Preserved from cached
            ('2024-01-02', 110.0),  # Preserved from cached
            ('2024-01-03', 125.0),  # Updated from new
            ('2024-01-04', 135.0),  # Updated from new
            ('2024-01-05', 145.0),  # Updated from new
            ('2024-01-06', 155.0),  # Added from new
            ('2024-01-07', 165.0),  # Added from new
        ]

        for i, (expected_date, expected_discharge) in enumerate(expected):
            actual_date = result.iloc[i]['date']
            actual_discharge = result.iloc[i]['discharge']

            assert actual_date == pd.Timestamp(expected_date), (
                f"Row {i}: expected date {expected_date}, got {actual_date}"
            )
            assert actual_discharge == expected_discharge, (
                f"Row {i} ({expected_date}): expected discharge {expected_discharge}, "
                f"got {actual_discharge}"
            )

    def test_merge_empty_cached_data(self):
        """
        Test first run with no cache (empty cached data).

        When existing_data is empty, the function should return new_data as-is.
        """
        # Empty cached data
        cached_data = pd.DataFrame(columns=['code', 'date', 'discharge'])
        cached_data['date'] = pd.to_datetime(cached_data['date'])
        cached_data['discharge'] = cached_data['discharge'].astype(float)

        # New API data
        new_data = pd.DataFrame({
            'code': ['A', 'A', 'B'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01']),
            'discharge': [100.0, 110.0, 200.0]
        })

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Result should be identical to new_data
        assert len(result) == len(new_data), (
            f"Expected {len(new_data)} rows, got {len(result)}"
        )

        # Verify all new data is present
        for _, row in new_data.iterrows():
            matching = result[
                (result['code'] == row['code']) &
                (result['date'] == row['date'])
            ]
            assert len(matching) == 1, (
                f"Missing row: code={row['code']}, date={row['date']}"
            )
            assert matching['discharge'].iloc[0] == row['discharge'], (
                f"Discharge mismatch for code={row['code']}, date={row['date']}"
            )

    def test_merge_empty_new_data(self):
        """
        Test when API returns nothing (empty new data).

        When new_data is empty, the function should return existing_data as-is.
        """
        # Cached data with values
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'B'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01']),
            'discharge': [100.0, 110.0, 200.0]
        })

        # Empty API response
        new_data = pd.DataFrame(columns=['code', 'date', 'discharge'])
        new_data['date'] = pd.to_datetime(new_data['date'])
        new_data['discharge'] = new_data['discharge'].astype(float)

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Result should be identical to cached_data
        assert len(result) == len(cached_data), (
            f"Expected {len(cached_data)} rows, got {len(result)}"
        )

        # Verify all cached data is preserved
        for _, row in cached_data.iterrows():
            matching = result[
                (result['code'] == row['code']) &
                (result['date'] == row['date'])
            ]
            assert len(matching) == 1, (
                f"Missing row: code={row['code']}, date={row['date']}"
            )
            assert matching['discharge'].iloc[0] == row['discharge'], (
                f"Discharge should be preserved: code={row['code']}, date={row['date']}"
            )

    def test_merge_both_empty(self):
        """
        Test fresh install with no data (both cached and new are empty).

        When both existing_data and new_data are empty, the function should
        return an empty DataFrame without errors.
        """
        # Empty cached data
        cached_data = pd.DataFrame(columns=['code', 'date', 'discharge'])
        cached_data['date'] = pd.to_datetime(cached_data['date'])
        cached_data['discharge'] = cached_data['discharge'].astype(float)

        # Empty new data
        new_data = pd.DataFrame(columns=['code', 'date', 'discharge'])
        new_data['date'] = pd.to_datetime(new_data['date'])
        new_data['discharge'] = new_data['discharge'].astype(float)

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Result should be empty
        assert len(result) == 0, f"Expected 0 rows, got {len(result)}"

        # Result should have the expected columns
        assert 'code' in result.columns, "Result should have 'code' column"
        assert 'date' in result.columns, "Result should have 'date' column"
        assert 'discharge' in result.columns, "Result should have 'discharge' column"


class TestMergeWithUpdateMultipleStations:
    """Additional tests for multi-station scenarios."""

    def test_merge_multiple_stations_independent(self):
        """
        Test that merge handles multiple stations independently.

        Each station's data should be merged separately - updates for station A
        should not affect station B.
        """
        # Cached data for two stations
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'B', 'B'],
            'date': pd.to_datetime([
                '2024-01-01', '2024-01-02',
                '2024-01-01', '2024-01-02'
            ]),
            'discharge': [100.0, 110.0, 200.0, 210.0]
        })

        # New data only for station A
        new_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-03']),
            'discharge': [105.0, 120.0]  # Update Jan 1, add Jan 3 for A only
        })

        result = src._merge_with_update(
            cached_data, new_data, 'code', 'date', 'discharge'
        )

        # Should have 5 rows: 3 for A (updated), 2 for B (unchanged)
        assert len(result) == 5, f"Expected 5 rows, got {len(result)}"

        # Station A should have updated and new data
        result_a = result[result['code'] == 'A'].sort_values('date').reset_index(drop=True)
        assert len(result_a) == 3, f"Station A should have 3 rows, got {len(result_a)}"
        assert result_a.iloc[0]['discharge'] == 105.0, "A Jan 1 should be updated"
        assert result_a.iloc[1]['discharge'] == 110.0, "A Jan 2 should be preserved"
        assert result_a.iloc[2]['discharge'] == 120.0, "A Jan 3 should be added"

        # Station B should be completely unchanged
        result_b = result[result['code'] == 'B'].sort_values('date').reset_index(drop=True)
        assert len(result_b) == 2, f"Station B should have 2 rows, got {len(result_b)}"
        assert result_b.iloc[0]['discharge'] == 200.0, "B Jan 1 should be unchanged"
        assert result_b.iloc[1]['discharge'] == 210.0, "B Jan 2 should be unchanged"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
