import os
import unittest
import datetime as dt
import pandas as pd
import logging
from unittest.mock import patch, MagicMock
import tempfile
import sys

import datetime_utils as du


class TestDatetimeUtils(unittest.TestCase):
    """Test cases for datetime_utils module functions"""

    def setUp(self):
        """Set up test data"""
        self.test_date_strings = [
            "2023-01-15",
            "2023/01/15", 
            "15-01-2023",
            "15/01/2023",
            "01-15-2023",
            "01/15/2023"
        ]
        self.test_datetime_strings = [
            "2023-01-15 12:30:45",
            "2023/01/15 12:30:45",
            "15-01-2023 12:30:45",
            "15/01/2023 12:30:45"
        ]
        self.expected_date = dt.date(2023, 1, 15)
        self.expected_datetime = dt.datetime(2023, 1, 15, 12, 30, 45)
        self.expected_date_string = "2023-01-15"

    def test_to_standard_date_string(self):
        """Test conversion of various date objects to standard string format"""
        # Test with datetime.date
        result = du.to_standard_date_string(self.expected_date)
        self.assertEqual(result, self.expected_date_string)
        
        # Test with datetime.datetime
        result = du.to_standard_date_string(self.expected_datetime)
        self.assertEqual(result, self.expected_date_string)
        
        # Test with string input
        result = du.to_standard_date_string("2023-01-15")
        self.assertEqual(result, self.expected_date_string)
        
        # Test with pandas Timestamp
        pd_timestamp = pd.Timestamp('2023-01-15')
        result = du.to_standard_date_string(pd_timestamp)
        self.assertEqual(result, self.expected_date_string)
        
        # Test with invalid input
        with self.assertRaises(ValueError):
            du.to_standard_date_string("invalid-date")
            
        with self.assertRaises(ValueError):
            du.to_standard_date_string(12345)

    def test_parse_date_string(self):
        """Test parsing of date strings in various formats"""
        for date_str in self.test_date_strings:
            with self.subTest(date_string=date_str):
                result = du.parse_date_string(date_str)
                self.assertEqual(result, self.expected_date)
                self.assertIsInstance(result, dt.date)
        
        # Test datetime strings (should extract date part)
        for datetime_str in self.test_datetime_strings:
            with self.subTest(datetime_string=datetime_str):
                result = du.parse_date_string(datetime_str)
                self.assertEqual(result, self.expected_date)
                self.assertIsInstance(result, dt.date)
        
        # Test invalid inputs
        with self.assertRaises(ValueError):
            du.parse_date_string("invalid-date")
            
        with self.assertRaises(ValueError):
            du.parse_date_string("32-01-2023")  # Invalid day

    def test_parse_datetime_string(self):
        """Test parsing of datetime strings in various formats"""
        for datetime_str in self.test_datetime_strings:
            with self.subTest(datetime_string=datetime_str):
                result = du.parse_datetime_string(datetime_str)
                self.assertEqual(result, self.expected_datetime)
                self.assertIsInstance(result, dt.datetime)
        
        # Test date-only strings (should assume 00:00:00)
        result = du.parse_datetime_string("2023-01-15")
        expected = dt.datetime(2023, 1, 15, 0, 0, 0)
        self.assertEqual(result, expected)
        
        # Test invalid inputs
        with self.assertRaises(ValueError):
            du.parse_datetime_string("invalid-datetime")

    def test_ensure_date_object(self):
        """Test conversion of various inputs to datetime.date objects"""
        # Test with datetime.date (should return as-is)
        result = du.ensure_date_object(self.expected_date)
        self.assertEqual(result, self.expected_date)
        self.assertIsInstance(result, dt.date)
        
        # Test with datetime.datetime (should extract date part)
        result = du.ensure_date_object(self.expected_datetime)
        self.assertEqual(result, self.expected_date)
        self.assertIsInstance(result, dt.date)
        
        # Test with string
        result = du.ensure_date_object("2023-01-15")
        self.assertEqual(result, self.expected_date)
        self.assertIsInstance(result, dt.date)
        
        # Test with pandas Timestamp
        pd_timestamp = pd.Timestamp('2023-01-15')
        result = du.ensure_date_object(pd_timestamp)
        self.assertEqual(result, self.expected_date)
        self.assertIsInstance(result, dt.date)
        
        # Test invalid input
        with self.assertRaises(ValueError):
            du.ensure_date_object("invalid-date")

    def test_ensure_datetime_object(self):
        """Test conversion of various inputs to datetime.datetime objects"""
        # Test with datetime.datetime (should return as-is)
        result = du.ensure_datetime_object(self.expected_datetime)
        self.assertEqual(result, self.expected_datetime)
        self.assertIsInstance(result, dt.datetime)
        
        # Test with datetime.date (should add 00:00:00 time)
        result = du.ensure_datetime_object(self.expected_date)
        expected = dt.datetime.combine(self.expected_date, dt.time())
        self.assertEqual(result, expected)
        self.assertIsInstance(result, dt.datetime)
        
        # Test with string
        result = du.ensure_datetime_object("2023-01-15 12:30:45")
        self.assertEqual(result, self.expected_datetime)
        self.assertIsInstance(result, dt.datetime)
        
        # Test with pandas Timestamp
        pd_timestamp = pd.Timestamp('2023-01-15 12:30:45')
        result = du.ensure_datetime_object(pd_timestamp)
        self.assertEqual(result, self.expected_datetime)
        self.assertIsInstance(result, dt.datetime)

    def test_safe_date_comparison(self):
        """Test safe comparison of dates in different formats"""
        date1 = "2023-01-15"
        date2 = dt.date(2023, 1, 16)
        date3 = pd.Timestamp('2023-01-15')
        
        # Test less than or equal
        self.assertTrue(du.safe_date_comparison(date1, date2, 'le'))
        self.assertTrue(du.safe_date_comparison(date1, date3, 'le'))
        self.assertFalse(du.safe_date_comparison(date2, date1, 'le'))
        
        # Test greater than or equal
        self.assertFalse(du.safe_date_comparison(date1, date2, 'ge'))
        self.assertTrue(du.safe_date_comparison(date1, date3, 'ge'))
        self.assertTrue(du.safe_date_comparison(date2, date1, 'ge'))
        
        # Test equality
        self.assertTrue(du.safe_date_comparison(date1, date3, 'eq'))
        self.assertFalse(du.safe_date_comparison(date1, date2, 'eq'))
        
        # Test invalid comparison type
        with self.assertRaises(ValueError):
            du.safe_date_comparison(date1, date2, 'invalid')

    def test_parse_dates_robust_pandas(self):
        """Test robust parsing of pandas Series with dates"""
        # Test with valid date strings
        date_series = pd.Series(self.test_date_strings)
        result = du.parse_dates_robust_pandas(date_series, 'test_column')
        
        # All should parse to the same date
        for parsed_date in result:
            if pd.notna(parsed_date):
                self.assertEqual(parsed_date.date(), self.expected_date)
        
        # Test with mixed valid and invalid dates (more valid than invalid to pass 80% threshold)
        mixed_series = pd.Series(["2023-01-15", "2023-01-16", "2023-01-17", "2023-01-18", "2023-01-19", "invalid-date"])
        result = du.parse_dates_robust_pandas(mixed_series, 'mixed_column')
        
        # Check that valid dates were parsed and invalid ones are NaT
        self.assertEqual(result.iloc[0].date(), dt.date(2023, 1, 15))
        self.assertEqual(result.iloc[1].date(), dt.date(2023, 1, 16))
        self.assertEqual(result.iloc[2].date(), dt.date(2023, 1, 17))
        self.assertEqual(result.iloc[3].date(), dt.date(2023, 1, 18))
        self.assertEqual(result.iloc[4].date(), dt.date(2023, 1, 19))
        self.assertTrue(pd.isna(result.iloc[5]))  # invalid-date should be NaT
        
        # Test with already datetime series
        datetime_series = pd.to_datetime(pd.Series(["2023-01-15", "2023-01-16"]))
        result = du.parse_dates_robust_pandas(datetime_series, 'datetime_column')
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result))
        
        # Test with mostly invalid dates (should raise error)
        invalid_series = pd.Series(["invalid1", "invalid2", "invalid3", "2023-01-15"])
        with self.assertRaises(ValueError):
            du.parse_dates_robust_pandas(invalid_series, 'invalid_column')

    def test_get_today_string(self):
        """Test getting today's date as string"""
        result = du.get_today_string()
        # Just check that it returns a valid date string format
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2}$')
        # Verify it can be parsed back
        parsed = du.parse_date_string(result)
        self.assertIsInstance(parsed, dt.date)

    def test_get_yesterday_string(self):
        """Test getting yesterday's date as string"""
        result = du.get_yesterday_string()
        # Just check that it returns a valid date string format
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2}$')
        # Verify it can be parsed back
        parsed = du.parse_date_string(result)
        self.assertIsInstance(parsed, dt.date)

    def test_standard_format_constants(self):
        """Test that standard format constants are properly defined"""
        self.assertEqual(du.STANDARD_DATE_FORMAT, '%Y-%m-%d')
        self.assertEqual(du.STANDARD_DATETIME_FORMAT, '%Y-%m-%d %H:%M:%S')

    def test_edge_cases(self):
        """Test edge cases and boundary conditions"""
        # Test leap year
        leap_date = "2024-02-29"
        result = du.parse_date_string(leap_date)
        self.assertEqual(result, dt.date(2024, 2, 29))
        
        # Test end of year
        eoy_date = "2023-12-31"
        result = du.parse_date_string(eoy_date)
        self.assertEqual(result, dt.date(2023, 12, 31))
        
        # Test beginning of year
        boy_date = "2023-01-01"
        result = du.parse_date_string(boy_date)
        self.assertEqual(result, dt.date(2023, 1, 1))
        
        # Test with whitespace
        whitespace_date = "  2023-01-15  "
        result = du.parse_date_string(whitespace_date)
        self.assertEqual(result, dt.date(2023, 1, 15))


class TestDatetimeUtilsIntegration(unittest.TestCase):
    """Integration tests for datetime_utils with other modules"""
    
    def test_integration_with_forecast_library_functions(self):
        """Test that datetime_utils integrates properly with forecast library functions"""
        # This test ensures that the datetime utilities work with the types
        # expected by forecast library functions
        
        test_dates = [
            dt.date(2023, 1, 15),
            dt.datetime(2023, 1, 15, 12, 0, 0),
            "2023-01-15",
            pd.Timestamp('2023-01-15')
        ]
        
        for test_date in test_dates:
            with self.subTest(date_input=test_date):
                # Test that all date types can be converted to standard string
                standard_str = du.to_standard_date_string(test_date)
                self.assertEqual(standard_str, "2023-01-15")
                
                # Test that they can be converted to date objects
                date_obj = du.ensure_date_object(test_date)
                self.assertEqual(date_obj, dt.date(2023, 1, 15))
                
                # Test that they can be converted to datetime objects
                datetime_obj = du.ensure_datetime_object(test_date)
                self.assertIsInstance(datetime_obj, dt.datetime)
                self.assertEqual(datetime_obj.date(), dt.date(2023, 1, 15))

    def test_get_last_day_of_month_vectorized(self):
        """Test vectorized last day of month calculation"""
        import pandas as pd
        
        # Test with various dates
        test_dates = pd.Series([
            pd.Timestamp('2023-01-15'),  # January -> 31st
            pd.Timestamp('2023-02-10'),  # February non-leap -> 28th
            pd.Timestamp('2024-02-15'),  # February leap year -> 29th
            pd.Timestamp('2023-04-05'),  # April -> 30th
            pd.Timestamp('2023-12-25'),  # December -> 31st
        ])
        
        expected_results = pd.Series([
            pd.Timestamp('2023-01-31'),
            pd.Timestamp('2023-02-28'),
            pd.Timestamp('2024-02-29'),
            pd.Timestamp('2023-04-30'),
            pd.Timestamp('2023-12-31'),
        ])
        
        results = du.get_last_day_of_month_vectorized(test_dates)
        
        # Check that results match expectations
        pd.testing.assert_series_equal(results, expected_results)
        
        # Test with string dates
        string_dates = pd.Series(['2023-01-15', '2023-02-10', '2023-03-20'])
        string_results = du.get_last_day_of_month_vectorized(string_dates)
        expected_string_results = pd.Series([
            pd.Timestamp('2023-01-31'),
            pd.Timestamp('2023-02-28'),
            pd.Timestamp('2023-03-31'),
        ])
        pd.testing.assert_series_equal(string_results, expected_string_results)

    def test_get_last_day_of_month_vectorized_performance_vs_apply(self):
        """Test that vectorized version is faster than apply"""
        import pandas as pd
        import time
        
        # Create a larger dataset for performance testing
        large_date_series = pd.Series([
            pd.Timestamp('2023-01-15') + pd.Timedelta(days=i) 
            for i in range(1000)  # 1000 dates
        ])
        
        # Time the vectorized version
        start_time = time.time()
        vectorized_result = du.get_last_day_of_month_vectorized(large_date_series)
        vectorized_time = time.time() - start_time
        
        # Time the apply version (using the original function)
        import forecast_library as fl
        start_time = time.time()
        # Convert to date objects first as the original function expects
        date_series = large_date_series.dt.date
        apply_result = date_series.apply(fl.get_last_day_of_month)
        apply_time = time.time() - start_time
        
        print(f"\nPerformance comparison:")
        print(f"Vectorized function: {vectorized_time:.4f} seconds")
        print(f"Apply function: {apply_time:.4f} seconds")
        print(f"Speedup: {apply_time/vectorized_time:.2f}x")
        
        # The vectorized version should be significantly faster
        self.assertLess(vectorized_time, apply_time)
        
        # Results should be equivalent (convert apply result to datetime for comparison)
        apply_result_datetime = pd.to_datetime(apply_result)
        pd.testing.assert_series_equal(vectorized_result, apply_result_datetime)


if __name__ == '__main__':
    # Set up logging for tests
    logging.basicConfig(level=logging.DEBUG)
    
    # Run the tests
    unittest.main(verbosity=2)