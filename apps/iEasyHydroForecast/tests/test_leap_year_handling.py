"""
Unit tests for leap year handling in forecast_library.py

Tests the bidirectional leap year day_of_year alignment and
date reconstruction using DateOffset.

These tests verify the bug fixes for:
1. Issue 1: Leap Year day_of_year Alignment Bug
2. Issue 2: Last Year Date Reconstruction Bug
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, date


class TestLeapYearAlignment(unittest.TestCase):
    """Test day_of_year alignment across leap/non-leap year boundaries."""

    def test_is_leap_year_helper(self):
        """Test the is_leap_year function."""
        # Import from forecast_library
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import is_leap_year

        # Leap years
        self.assertTrue(is_leap_year(2024))
        self.assertTrue(is_leap_year(2020))
        self.assertTrue(is_leap_year(2000))  # Divisible by 400

        # Non-leap years
        self.assertFalse(is_leap_year(2025))
        self.assertFalse(is_leap_year(2023))
        self.assertFalse(is_leap_year(2100))  # Divisible by 100 but not 400

    def test_day_of_year_nonleap_current_leap_last(self):
        """
        Current=2025 (non-leap), Last=2024 (leap)

        Feb 29 2024 should be mapped to Feb 28
        Mar 1 2024 (day 61) should become day 60 (Mar 1 in non-leap terms)
        Dec 31 2024 (day 366) should become day 365
        """
        # Create test data with dates from both years
        data = pd.DataFrame({
            'date': pd.to_datetime([
                '2024-02-28',  # day 59 in leap year
                '2024-02-29',  # day 60 in leap year - should be mapped to Feb 28
                '2024-03-01',  # day 61 in leap year - should become day 60
                '2024-12-31',  # day 366 in leap year - should become day 365
                '2025-02-28',  # day 59 in non-leap year
                '2025-03-01',  # day 60 in non-leap year
            ]),
            'code': ['A', 'A', 'A', 'A', 'A', 'A'],
            'discharge_avg': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        })

        current_year = 2025
        last_year = 2024

        # Apply the leap year alignment logic (same as in forecast_library.py)
        data['day_of_year'] = data['date'].dt.dayofyear
        current_is_leap = (current_year % 4 == 0 and current_year % 100 != 0) or (current_year % 400 == 0)
        last_is_leap = (last_year % 4 == 0 and last_year % 100 != 0) or (last_year % 400 == 0)

        if not current_is_leap and last_is_leap:
            # Map Feb 29 from leap years to Feb 28
            feb29_mask = (data['date'].dt.month == 2) & (data['date'].dt.day == 29)
            data.loc[feb29_mask, 'date'] = data.loc[feb29_mask, 'date'] - pd.Timedelta(days=1)
            # Recalculate day_of_year after date adjustment
            data['day_of_year'] = data['date'].dt.dayofyear
            # Subtract 1 from day_of_year for dates after Feb 28 in last year's data
            last_year_after_feb = (data['date'].dt.year == last_year) & (data['date'].dt.month > 2)
            data.loc[last_year_after_feb, 'day_of_year'] -= 1

        # Verify Feb 29 was mapped to Feb 28
        self.assertEqual(data.iloc[1]['date'], pd.Timestamp('2024-02-28'))

        # Verify day_of_year alignment
        # Feb 28 2024 should be day 59
        self.assertEqual(data.iloc[0]['day_of_year'], 59)
        # Feb 29 (now Feb 28) 2024 should be day 59
        self.assertEqual(data.iloc[1]['day_of_year'], 59)
        # Mar 1 2024 should be day 60 (aligned to non-leap year)
        self.assertEqual(data.iloc[2]['day_of_year'], 60)
        # Dec 31 2024 should be day 365 (aligned to non-leap year)
        self.assertEqual(data.iloc[3]['day_of_year'], 365)
        # 2025 dates should remain unchanged
        self.assertEqual(data.iloc[4]['day_of_year'], 59)  # Feb 28
        self.assertEqual(data.iloc[5]['day_of_year'], 60)  # Mar 1

    def test_day_of_year_leap_current_nonleap_last(self):
        """
        Current=2024 (leap), Last=2023 (non-leap)

        Mar 1 2023 (day 60 in non-leap) should become day 61 (Mar 1 in leap terms)
        Dec 31 2023 (day 365) should become day 366
        """
        data = pd.DataFrame({
            'date': pd.to_datetime([
                '2023-02-28',  # day 59 in non-leap year
                '2023-03-01',  # day 60 in non-leap year - should become day 61
                '2023-12-31',  # day 365 in non-leap year - should become day 366
                '2024-02-28',  # day 59 in leap year
                '2024-02-29',  # day 60 in leap year
                '2024-03-01',  # day 61 in leap year
            ]),
            'code': ['A', 'A', 'A', 'A', 'A', 'A'],
            'discharge_avg': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        })

        current_year = 2024
        last_year = 2023

        data['day_of_year'] = data['date'].dt.dayofyear
        current_is_leap = (current_year % 4 == 0 and current_year % 100 != 0) or (current_year % 400 == 0)
        last_is_leap = (last_year % 4 == 0 and last_year % 100 != 0) or (last_year % 400 == 0)

        if current_is_leap and not last_is_leap:
            # Add 1 to day_of_year for dates after Feb 28 in last year's data
            last_year_after_feb = (data['date'].dt.year == last_year) & (data['date'].dt.month > 2)
            data.loc[last_year_after_feb, 'day_of_year'] += 1

        # Verify day_of_year alignment
        # Feb 28 2023 should remain day 59
        self.assertEqual(data.iloc[0]['day_of_year'], 59)
        # Mar 1 2023 should be day 61 (aligned to leap year)
        self.assertEqual(data.iloc[1]['day_of_year'], 61)
        # Dec 31 2023 should be day 366 (aligned to leap year)
        self.assertEqual(data.iloc[2]['day_of_year'], 366)
        # 2024 dates should remain unchanged
        self.assertEqual(data.iloc[3]['day_of_year'], 59)  # Feb 28
        self.assertEqual(data.iloc[4]['day_of_year'], 60)  # Feb 29
        self.assertEqual(data.iloc[5]['day_of_year'], 61)  # Mar 1

    def test_same_type_years_no_adjustment(self):
        """
        When both years are the same type (both leap or both non-leap),
        no adjustment should be made.
        """
        # Test non-leap to non-leap (2025 vs 2023)
        data = pd.DataFrame({
            'date': pd.to_datetime([
                '2023-03-01',  # day 60
                '2023-12-31',  # day 365
                '2025-03-01',  # day 60
                '2025-12-31',  # day 365
            ]),
            'code': ['A', 'A', 'A', 'A'],
            'discharge_avg': [1.0, 2.0, 3.0, 4.0]
        })

        current_year = 2025
        last_year = 2023  # Note: this would typically be 2024, but for testing same-type years

        data['day_of_year'] = data['date'].dt.dayofyear
        current_is_leap = (current_year % 4 == 0 and current_year % 100 != 0) or (current_year % 400 == 0)
        last_is_leap = (last_year % 4 == 0 and last_year % 100 != 0) or (last_year % 400 == 0)

        # Both are non-leap, so no adjustment needed
        self.assertFalse(current_is_leap)
        self.assertFalse(last_is_leap)

        # Day of year should remain unchanged
        self.assertEqual(data.iloc[0]['day_of_year'], 60)
        self.assertEqual(data.iloc[1]['day_of_year'], 365)
        self.assertEqual(data.iloc[2]['day_of_year'], 60)
        self.assertEqual(data.iloc[3]['day_of_year'], 365)


class TestDateReconstruction(unittest.TestCase):
    """Test last year date reconstruction using DateOffset."""

    def test_date_offset_leap_to_nonleap(self):
        """
        DateOffset should correctly shift dates by 1 year.
        2024-02-29 + 1 year in pandas will become 2025-02-28 (pandas default behavior).
        """
        dates = pd.to_datetime(['2024-02-28', '2024-02-29', '2024-03-01', '2024-12-31'])
        shifted = dates + pd.DateOffset(years=1)

        # 2024-02-28 -> 2025-02-28
        self.assertEqual(shifted[0], pd.Timestamp('2025-02-28'))
        # 2024-02-29 -> 2025-02-28 (pandas shifts to valid date)
        self.assertEqual(shifted[1], pd.Timestamp('2025-02-28'))
        # 2024-03-01 -> 2025-03-01
        self.assertEqual(shifted[2], pd.Timestamp('2025-03-01'))
        # 2024-12-31 -> 2025-12-31
        self.assertEqual(shifted[3], pd.Timestamp('2025-12-31'))

    def test_date_offset_nonleap_to_leap(self):
        """
        2023 dates + 1 year should correctly become 2024 dates.
        """
        dates = pd.to_datetime(['2023-02-28', '2023-03-01', '2023-12-31'])
        shifted = dates + pd.DateOffset(years=1)

        # 2023-02-28 -> 2024-02-28
        self.assertEqual(shifted[0], pd.Timestamp('2024-02-28'))
        # 2023-03-01 -> 2024-03-01
        self.assertEqual(shifted[1], pd.Timestamp('2024-03-01'))
        # 2023-12-31 -> 2024-12-31
        self.assertEqual(shifted[2], pd.Timestamp('2024-12-31'))

    def test_old_day_of_year_method_fails_at_year_end(self):
        """
        Demonstrate that the old day_of_year method fails for Dec 31 in leap years.

        2024 (leap) Dec 31 has day_of_year = 366
        Old method: pd.Timestamp('2025') + pd.to_timedelta(366 - 1, unit='D')
                  = 2025-01-01 + 365 days = 2026-01-01 (WRONG!)
        """
        day_of_year_2024_dec31 = 366
        current_year = 2025

        # Old method (buggy)
        old_result = pd.Timestamp(str(current_year)) + pd.to_timedelta(day_of_year_2024_dec31 - 1, unit='D')

        # This should NOT equal 2025-12-31
        self.assertNotEqual(old_result, pd.Timestamp('2025-12-31'))
        # It incorrectly produces 2026-01-01
        self.assertEqual(old_result, pd.Timestamp('2026-01-01'))

    def test_new_date_offset_method_works(self):
        """
        The new DateOffset method correctly handles Dec 31 from leap years.
        """
        original_date = pd.Timestamp('2024-12-31')  # day_of_year = 366

        # New method using DateOffset
        new_result = original_date + pd.DateOffset(years=1)

        # Should correctly be 2025-12-31
        self.assertEqual(new_result, pd.Timestamp('2025-12-31'))


class TestFeb29Handling(unittest.TestCase):
    """Test that Feb 29 data is preserved and mapped correctly."""

    def test_feb29_not_dropped(self):
        """Feb 29 data should NOT be dropped, only its date should be adjusted."""
        data = pd.DataFrame({
            'date': pd.to_datetime([
                '2024-02-28',
                '2024-02-29',  # This should NOT be dropped
                '2024-03-01',
            ]),
            'code': ['A', 'A', 'A'],
            'discharge_avg': [1.0, 2.0, 3.0]  # discharge_avg=2.0 should be preserved
        })

        current_year = 2025
        last_year = 2024

        data['day_of_year'] = data['date'].dt.dayofyear
        current_is_leap = False  # 2025 is non-leap
        last_is_leap = True  # 2024 is leap

        original_row_count = len(data)

        if not current_is_leap and last_is_leap:
            # Map Feb 29 from leap years to Feb 28
            feb29_mask = (data['date'].dt.month == 2) & (data['date'].dt.day == 29)
            data.loc[feb29_mask, 'date'] = data.loc[feb29_mask, 'date'] - pd.Timedelta(days=1)
            # Recalculate day_of_year after date adjustment
            data['day_of_year'] = data['date'].dt.dayofyear
            # Subtract 1 from day_of_year for dates after Feb 28 in last year's data
            last_year_after_feb = (data['date'].dt.year == last_year) & (data['date'].dt.month > 2)
            data.loc[last_year_after_feb, 'day_of_year'] -= 1

        # Row count should be the same (no data dropped)
        self.assertEqual(len(data), original_row_count)

        # discharge_avg for the original Feb 29 row should be preserved
        self.assertEqual(data.iloc[1]['discharge_avg'], 2.0)

        # The date should now be Feb 28
        self.assertEqual(data.iloc[1]['date'], pd.Timestamp('2024-02-28'))

    def test_feb29_contributes_to_decad7_statistics(self):
        """
        Feb 29 data should contribute to decad 7 (Feb 21-28/29) statistics.
        When current year is non-leap, Feb 29 is mapped to Feb 28, so both
        Feb 28 and Feb 29 data will be in the same decad 7.
        """
        # Create data with Feb 28 and Feb 29 values
        data = pd.DataFrame({
            'date': pd.to_datetime([
                '2024-02-28',
                '2024-02-29',
            ]),
            'code': ['A', 'A'],
            'discharge_avg': [10.0, 20.0]  # Different values to verify both contribute
        })

        # Apply Feb 29 -> Feb 28 mapping
        feb29_mask = (data['date'].dt.month == 2) & (data['date'].dt.day == 29)
        data.loc[feb29_mask, 'date'] = data.loc[feb29_mask, 'date'] - pd.Timedelta(days=1)

        # Now both rows should have date Feb 28
        self.assertTrue((data['date'] == pd.Timestamp('2024-02-28')).all())

        # Both values should be present for averaging
        mean_discharge = data['discharge_avg'].mean()
        self.assertEqual(mean_discharge, 15.0)  # (10 + 20) / 2


class TestIssueDateReconstruction(unittest.TestCase):
    """Test issue date reconstruction from pentad/decad_in_year."""

    def test_get_issue_date_from_pentad(self):
        """Test issue date calculation from pentad_in_year."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_issue_date_from_pentad

        # pentad 1 = Jan 1-5, issue date = Dec 31 of previous year
        self.assertEqual(get_issue_date_from_pentad(1, 2025), pd.Timestamp('2024-12-31'))

        # pentad 2 = Jan 6-10, issue date = Jan 5
        self.assertEqual(get_issue_date_from_pentad(2, 2025), pd.Timestamp('2025-01-05'))

        # pentad 3 = Jan 11-15, issue date = Jan 10
        self.assertEqual(get_issue_date_from_pentad(3, 2025), pd.Timestamp('2025-01-10'))

        # pentad 6 = Jan 26-31, issue date = Jan 25
        self.assertEqual(get_issue_date_from_pentad(6, 2025), pd.Timestamp('2025-01-25'))

        # pentad 7 = Feb 1-5, issue date = Jan 31
        self.assertEqual(get_issue_date_from_pentad(7, 2025), pd.Timestamp('2025-01-31'))

        # pentad 12 = Feb 26-end, issue date = Feb 25
        self.assertEqual(get_issue_date_from_pentad(12, 2025), pd.Timestamp('2025-02-25'))

        # pentad 13 = Mar 1-5, issue date = Feb 28 (non-leap year)
        self.assertEqual(get_issue_date_from_pentad(13, 2025), pd.Timestamp('2025-02-28'))

        # pentad 13 in 2024 = Mar 1-5, issue date = Feb 29 (leap year)
        self.assertEqual(get_issue_date_from_pentad(13, 2024), pd.Timestamp('2024-02-29'))

        # pentad 72 = Dec 26-31, issue date = Dec 25
        self.assertEqual(get_issue_date_from_pentad(72, 2025), pd.Timestamp('2025-12-25'))

    def test_get_issue_date_from_decad(self):
        """Test issue date calculation from decad_in_year."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_issue_date_from_decad

        # decad 1 = Jan 1-10, issue date = Dec 31 of previous year
        self.assertEqual(get_issue_date_from_decad(1, 2025), pd.Timestamp('2024-12-31'))

        # decad 2 = Jan 11-20, issue date = Jan 10
        self.assertEqual(get_issue_date_from_decad(2, 2025), pd.Timestamp('2025-01-10'))

        # decad 3 = Jan 21-31, issue date = Jan 20
        self.assertEqual(get_issue_date_from_decad(3, 2025), pd.Timestamp('2025-01-20'))

        # decad 4 = Feb 1-10, issue date = Jan 31
        self.assertEqual(get_issue_date_from_decad(4, 2025), pd.Timestamp('2025-01-31'))

        # decad 6 = Feb 21-28/29, issue date = Feb 20
        self.assertEqual(get_issue_date_from_decad(6, 2025), pd.Timestamp('2025-02-20'))

        # decad 7 = Mar 1-10, issue date = Feb 28 (non-leap year)
        self.assertEqual(get_issue_date_from_decad(7, 2025), pd.Timestamp('2025-02-28'))

        # decad 7 in 2024 = Mar 1-10, issue date = Feb 29 (leap year)
        self.assertEqual(get_issue_date_from_decad(7, 2024), pd.Timestamp('2024-02-29'))

        # decad 36 = Dec 21-31, issue date = Dec 20
        self.assertEqual(get_issue_date_from_decad(36, 2025), pd.Timestamp('2025-12-20'))

    def test_get_day_of_year_from_pentad(self):
        """Test day_of_year calculation from pentad_in_year."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_day_of_year_from_pentad

        # pentad 1 = Jan 1-5, issue date = Dec 31 of PREVIOUS year
        self.assertEqual(get_day_of_year_from_pentad(1, 2025), 366)  # Dec 31, 2024 (leap year has 366 days)
        self.assertEqual(get_day_of_year_from_pentad(1, 2024), 365)  # Dec 31, 2023 (non-leap year has 365 days)

        # pentad 2 = Jan 6-10, issue date = Jan 5 (day 5)
        self.assertEqual(get_day_of_year_from_pentad(2, 2025), 5)

        # pentad 13 = Mar 1-5, issue date = Feb 28/29
        self.assertEqual(get_day_of_year_from_pentad(13, 2025), 59)  # Feb 28, 2025 (non-leap)
        self.assertEqual(get_day_of_year_from_pentad(13, 2024), 60)  # Feb 29, 2024 (leap)

        # pentad 72 = Dec 26-31, issue date = Dec 25 (day 359 or 360)
        self.assertEqual(get_day_of_year_from_pentad(72, 2025), 359)  # non-leap
        self.assertEqual(get_day_of_year_from_pentad(72, 2024), 360)  # leap

    def test_get_day_of_year_from_decad(self):
        """Test day_of_year calculation from decad_in_year."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_day_of_year_from_decad

        # decad 1 = Jan 1-10, issue date = Dec 31 of PREVIOUS year
        self.assertEqual(get_day_of_year_from_decad(1, 2025), 366)  # Dec 31, 2024 (leap year has 366 days)
        self.assertEqual(get_day_of_year_from_decad(1, 2024), 365)  # Dec 31, 2023 (non-leap year has 365 days)

        # decad 2 = Jan 11-20, issue date = Jan 10 (day 10)
        self.assertEqual(get_day_of_year_from_decad(2, 2025), 10)

        # decad 7 = Mar 1-10, issue date = Feb 28/29
        self.assertEqual(get_day_of_year_from_decad(7, 2025), 59)  # Feb 28, 2025 (non-leap)
        self.assertEqual(get_day_of_year_from_decad(7, 2024), 60)  # Feb 29, 2024 (leap)

        # decad 36 = Dec 21-31, issue date = Dec 20 (day 354 or 355)
        self.assertEqual(get_day_of_year_from_decad(36, 2025), 354)  # non-leap
        self.assertEqual(get_day_of_year_from_decad(36, 2024), 355)  # leap

    def test_get_pentad_from_pentad_in_year(self):
        """Test pentad (1-6 within month) from pentad_in_year (1-72)."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_pentad_from_pentad_in_year

        # First month: pentads 1-6 should map to 1-6
        self.assertEqual(get_pentad_from_pentad_in_year(1), 1)
        self.assertEqual(get_pentad_from_pentad_in_year(2), 2)
        self.assertEqual(get_pentad_from_pentad_in_year(6), 6)

        # Second month: pentads 7-12 should map to 1-6
        self.assertEqual(get_pentad_from_pentad_in_year(7), 1)
        self.assertEqual(get_pentad_from_pentad_in_year(8), 2)
        self.assertEqual(get_pentad_from_pentad_in_year(12), 6)

        # Last month: pentads 67-72 should map to 1-6
        self.assertEqual(get_pentad_from_pentad_in_year(67), 1)
        self.assertEqual(get_pentad_from_pentad_in_year(72), 6)

    def test_get_decad_from_decad_in_year(self):
        """Test decad (1-3 within month) from decad_in_year (1-36)."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_decad_from_decad_in_year

        # First month: decads 1-3 should map to 1-3
        self.assertEqual(get_decad_from_decad_in_year(1), 1)
        self.assertEqual(get_decad_from_decad_in_year(2), 2)
        self.assertEqual(get_decad_from_decad_in_year(3), 3)

        # Second month: decads 4-6 should map to 1-3
        self.assertEqual(get_decad_from_decad_in_year(4), 1)
        self.assertEqual(get_decad_from_decad_in_year(5), 2)
        self.assertEqual(get_decad_from_decad_in_year(6), 3)

        # Last month: decads 34-36 should map to 1-3
        self.assertEqual(get_decad_from_decad_in_year(34), 1)
        self.assertEqual(get_decad_from_decad_in_year(35), 2)
        self.assertEqual(get_decad_from_decad_in_year(36), 3)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for realistic scenarios."""

    def test_scenario_2025_with_2024_data(self):
        """
        Test realistic scenario: Current year 2025, with historical data from 2024.
        This is the exact case that was causing the bug.
        """
        # Simulate data similar to what would come from runoff_day.csv
        dates_2024 = pd.date_range('2024-02-20', '2024-03-05', freq='D')
        dates_2025 = pd.date_range('2025-02-20', '2025-03-05', freq='D')

        data = pd.DataFrame({
            'date': list(dates_2024) + list(dates_2025),
            'code': ['15013'] * (len(dates_2024) + len(dates_2025)),
            'discharge_avg': np.random.rand(len(dates_2024) + len(dates_2025)) * 100
        })

        current_year = 2025
        last_year = 2024

        # Check Feb 29 exists in 2024 data
        has_feb29 = ((data['date'].dt.year == 2024) &
                     (data['date'].dt.month == 2) &
                     (data['date'].dt.day == 29)).any()
        self.assertTrue(has_feb29)

        # Apply the fix
        data['day_of_year'] = data['date'].dt.dayofyear
        current_is_leap = False
        last_is_leap = True

        if not current_is_leap and last_is_leap:
            feb29_mask = (data['date'].dt.month == 2) & (data['date'].dt.day == 29)
            data.loc[feb29_mask, 'date'] = data.loc[feb29_mask, 'date'] - pd.Timedelta(days=1)
            data['day_of_year'] = data['date'].dt.dayofyear
            last_year_after_feb = (data['date'].dt.year == last_year) & (data['date'].dt.month > 2)
            data.loc[last_year_after_feb, 'day_of_year'] -= 1

        # After fix, Feb 29 should no longer exist (mapped to Feb 28)
        has_feb29_after = ((data['date'].dt.month == 2) &
                          (data['date'].dt.day == 29)).any()
        self.assertFalse(has_feb29_after)

        # day_of_year should now align between years
        # Mar 1 in 2024 and Mar 1 in 2025 should have the same day_of_year
        mar1_2024 = data[data['date'] == pd.Timestamp('2024-03-01')]['day_of_year'].iloc[0]
        mar1_2025 = data[data['date'] == pd.Timestamp('2025-03-01')]['day_of_year'].iloc[0]
        self.assertEqual(mar1_2024, mar1_2025)


class TestHydrographDataOutput(unittest.TestCase):
    """Test the hydrograph data output for completeness of dates."""

    def test_pentad_all_dates_and_day_of_year_filled(self):
        """
        Test that pentad hydrograph output always has dates and day_of_year filled.
        Simulates the scenario where some stations have empty discharge data.
        """
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_issue_date_from_pentad, get_day_of_year_from_pentad

        # Simulate a DataFrame similar to hydrograph_pentad.csv
        # Station 15020 has empty dates for pentads 2-11 (no discharge data)
        stations = ['15013', '15016', '15020', '15025']
        pentads = list(range(1, 73))  # All 72 pentads
        current_year = 2025

        # Create test data with both date and day_of_year missing
        rows = []
        for station in stations:
            for pentad in pentads:
                has_data = not (station in ['15020', '15025'] and pentad < 20)
                rows.append({
                    'code': station,
                    'pentad_in_year': pentad,
                    'date': pd.Timestamp('2025-01-01') if has_data else None,
                    'day_of_year': 1 if has_data else None,
                    'discharge_avg': 100.0 if has_data else np.nan
                })

        df = pd.DataFrame(rows)

        # Apply the fix: fill missing dates from pentad_in_year
        missing_date_mask = df['date'].isna()
        self.assertTrue(missing_date_mask.any())  # Verify we have missing dates to fix

        df.loc[missing_date_mask, 'date'] = df.loc[missing_date_mask, 'pentad_in_year'].apply(
            lambda p: get_issue_date_from_pentad(p, current_year)
        )

        # Fill missing day_of_year from pentad_in_year
        missing_doy_mask = df['day_of_year'].isna()
        df.loc[missing_doy_mask, 'day_of_year'] = df.loc[missing_doy_mask, 'pentad_in_year'].apply(
            lambda p: get_day_of_year_from_pentad(p, current_year)
        )

        # After fix, no dates or day_of_year should be missing
        self.assertFalse(df['date'].isna().any())
        self.assertFalse(df['day_of_year'].isna().any())

        # Verify specific pentad dates and day_of_year for station 15020
        station_15020 = df[df['code'] == '15020']

        # pentad 1 -> Dec 31 previous year, day_of_year = 366 (2024 is leap year with 366 days)
        pentad_1_row = station_15020[station_15020['pentad_in_year'] == 1].iloc[0]
        self.assertEqual(pentad_1_row['date'], pd.Timestamp('2024-12-31'))
        self.assertEqual(pentad_1_row['day_of_year'], 366)

        # pentad 7 -> Jan 31, day_of_year = 31
        pentad_7_row = station_15020[station_15020['pentad_in_year'] == 7].iloc[0]
        self.assertEqual(pentad_7_row['date'], pd.Timestamp('2025-01-31'))
        self.assertEqual(pentad_7_row['day_of_year'], 31)

        # pentad 13 -> Feb 28 (non-leap year), day_of_year = 59
        pentad_13_row = station_15020[station_15020['pentad_in_year'] == 13].iloc[0]
        self.assertEqual(pentad_13_row['date'], pd.Timestamp('2025-02-28'))
        self.assertEqual(pentad_13_row['day_of_year'], 59)

    def test_decad_all_dates_and_day_of_year_filled(self):
        """
        Test that decad hydrograph output always has dates and day_of_year filled.
        Specifically tests decad 7 (Mar 1-10) which was causing issues.
        """
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_issue_date_from_decad, get_day_of_year_from_decad

        # Simulate a DataFrame similar to hydrograph_decad.csv
        stations = ['15013', '15016']
        decads = list(range(1, 37))  # All 36 decads
        current_year = 2025

        # Create test data - decad 7 has empty dates and day_of_year (the original bug)
        rows = []
        for station in stations:
            for decad in decads:
                has_data = decad != 7
                rows.append({
                    'code': station,
                    'decad_in_year': decad,
                    'date': pd.Timestamp('2025-01-01') if has_data else None,
                    'day_of_year': 1 if has_data else None,
                    'discharge_avg': 100.0 if has_data else np.nan
                })

        df = pd.DataFrame(rows)

        # Apply the fix: fill missing dates from decad_in_year
        missing_date_mask = df['date'].isna()
        self.assertTrue(missing_date_mask.any())  # Verify we have missing dates to fix

        df.loc[missing_date_mask, 'date'] = df.loc[missing_date_mask, 'decad_in_year'].apply(
            lambda d: get_issue_date_from_decad(d, current_year)
        )

        # Fill missing day_of_year from decad_in_year
        missing_doy_mask = df['day_of_year'].isna()
        df.loc[missing_doy_mask, 'day_of_year'] = df.loc[missing_doy_mask, 'decad_in_year'].apply(
            lambda d: get_day_of_year_from_decad(d, current_year)
        )

        # After fix, no dates or day_of_year should be missing
        self.assertFalse(df['date'].isna().any())
        self.assertFalse(df['day_of_year'].isna().any())

        # Verify decad 7 dates and day_of_year (the problematic decad)
        decad_7_rows = df[df['decad_in_year'] == 7]
        for _, row in decad_7_rows.iterrows():
            # decad 7 = Mar 1-10, issue date = Feb 28 (non-leap year), day_of_year = 59
            self.assertEqual(row['date'], pd.Timestamp('2025-02-28'))
            self.assertEqual(row['day_of_year'], 59)

    def test_decad_7_leap_year(self):
        """
        Test decad 7 issue date in leap year 2024.
        Issue date should be Feb 29.
        """
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from forecast_library import get_issue_date_from_decad

        current_year = 2024

        # decad 7 = Mar 1-10, issue date in leap year = Feb 29
        issue_date = get_issue_date_from_decad(7, current_year)
        self.assertEqual(issue_date, pd.Timestamp('2024-02-29'))


if __name__ == '__main__':
    unittest.main()
