"""
Edge case and boundary condition tests for linear_regression.py.

Covers: date boundaries (century leap years, year/month transitions),
empty data handling, NaN handling, float→string code conversion, duplicates.
"""

import datetime as dt
import os
import sys
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from linear_regression import (
    get_forecast_days_for_month,
    get_hindcast_start_date_from_output,
    get_last_forecast_dates_per_gauge,
    get_next_forecast_day,
)

# ============================================================================
# Date boundaries
# ============================================================================


class TestDateBoundaries:
    """Unusual calendar edge cases."""

    def test_century_leap_year_2000(self):
        """Year 2000 is divisible by 400 → leap year, Feb has 29 days."""
        result = get_forecast_days_for_month(2000, 2, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 29]

    def test_century_non_leap_1900(self):
        """Year 1900 is divisible by 100 but not 400 → NOT leap, Feb 28."""
        result = get_forecast_days_for_month(1900, 2, "PENTAD")
        assert result == [5, 10, 15, 20, 25, 28]

    def test_year_boundary_dec_31_to_jan(self):
        """Dec 31 is a forecast day; Jan 1 advances to Jan 5."""
        assert get_next_forecast_day(dt.date(2024, 12, 31), "PENTAD") == dt.date(2024, 12, 31)
        assert get_next_forecast_day(dt.date(2025, 1, 1), "PENTAD") == dt.date(2025, 1, 5)

    def test_feb_to_mar_transition_non_leap(self):
        """Feb 28 (non-leap last day) is forecast day; Mar 1 → Mar 5."""
        assert get_next_forecast_day(dt.date(2023, 2, 28), "PENTAD") == dt.date(2023, 2, 28)
        assert get_next_forecast_day(dt.date(2023, 3, 1), "PENTAD") == dt.date(2023, 3, 5)

    def test_feb_to_mar_transition_leap(self):
        """Feb 29 (leap last day) is forecast day; Mar 1 → Mar 5."""
        assert get_next_forecast_day(dt.date(2024, 2, 29), "PENTAD") == dt.date(2024, 2, 29)
        assert get_next_forecast_day(dt.date(2024, 3, 1), "PENTAD") == dt.date(2024, 3, 5)


# ============================================================================
# Empty data handling
# ============================================================================


class TestEmptyDataHandling:
    """Functions gracefully handle missing/empty inputs."""

    def test_no_csv_files_returns_empty(self, tmp_path):
        """No CSV files in directory → empty dict."""
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("BOTH")
            assert result == {}

    def test_csv_exists_but_empty_returns_empty(self, tmp_path):
        """CSV with headers only → empty dict."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        csv_file.write_text("date,code,value\n")
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            assert result == {}

    def test_hindcast_no_files_no_site_list(self, tmp_path):
        """No files, no site_list → None."""
        with patch.dict(
            os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}, clear=False
        ):
            # Remove START_DATE if set
            env = {"ieasyforecast_intermediate_data_path": str(tmp_path)}
            with patch.dict(os.environ, env, clear=False):
                os.environ.pop("ieasyhydroforecast_START_DATE", None)
                result = get_hindcast_start_date_from_output("BOTH", site_list=None)
                assert result is None

    def test_hindcast_site_list_with_env_default_no_files(self, tmp_path):
        """site_list provided + env default, but no CSV files → env date."""
        with patch.dict(
            os.environ,
            {
                "ieasyforecast_intermediate_data_path": str(tmp_path),
                "ieasyhydroforecast_START_DATE": "2000-01-01",
            },
        ):
            result = get_hindcast_start_date_from_output("BOTH", site_list=["12345"])
            assert result == dt.date(2000, 1, 1)

    def test_hindcast_site_list_no_env_default_no_files(self, tmp_path):
        """site_list provided, no env default, no files → None."""
        env = {"ieasyforecast_intermediate_data_path": str(tmp_path)}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("ieasyhydroforecast_START_DATE", None)
            result = get_hindcast_start_date_from_output("BOTH", site_list=["12345"])
            assert result is None


# ============================================================================
# Code conversion (float → string)
# ============================================================================


class TestCodeConversion:
    """Float code values in CSV are converted to clean string keys."""

    def test_float_code_becomes_string(self, tmp_path):
        """Code 15013.0 in CSV → string key '15013'."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05"],
                "code": [15013.0],
                "value": [100.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            assert "15013" in result
            assert result["15013"] == dt.date(2024, 1, 5)

    def test_non_numeric_string_code_skips_csv(self, tmp_path):
        """Non-numeric string codes cause float() to fail, skipping CSV.

        The code conversion lambda uses float(x) which raises ValueError
        for non-numeric strings. The broad except catches this and logs a
        warning, returning an empty dict for that CSV file.
        """
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05"],
                "code": ["ABC123"],
                "value": [100.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            # Non-numeric code causes the entire CSV to be skipped
            assert result == {}

    def test_integer_string_code_converted(self, tmp_path):
        """Integer string codes (e.g. '15013') are handled via float→int."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05", "2024-01-10"],
                "code": ["15013", "16100"],
                "value": [100.0, 200.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            assert "15013" in result
            assert "16100" in result


# ============================================================================
# Duplicate handling
# ============================================================================


class TestDuplicateHandling:
    """Duplicate dates per gauge → returns max date."""

    def test_duplicate_dates_returns_max(self, tmp_path):
        """Multiple rows per gauge → latest date wins."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05", "2024-03-15", "2024-02-10"],
                "code": [15013.0, 15013.0, 15013.0],
                "value": [100.0, 150.0, 120.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            assert result["15013"] == dt.date(2024, 3, 15)


# ============================================================================
# NaN handling
# ============================================================================


class TestNaNHandling:
    """NaN values in CSV are handled gracefully."""

    def test_nan_dates_ignored(self, tmp_path):
        """NaN dates are dropped; only valid dates contribute."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05", None, "2024-02-10"],
                "code": [15013.0, 15013.0, 15013.0],
                "value": [100.0, 200.0, 150.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            # Should have the max of the two valid dates
            assert result["15013"] == dt.date(2024, 2, 10)

    def test_nan_codes_handled(self, tmp_path):
        """NaN code rows don't crash; valid rows still processed."""
        csv_file = tmp_path / "forecast_pentad_linreg.csv"
        df = pd.DataFrame(
            {
                "date": ["2024-01-05", "2024-01-10"],
                "code": [15013.0, float("nan")],
                "value": [100.0, 200.0],
            }
        )
        df.to_csv(csv_file, index=False)
        with patch.dict(os.environ, {"ieasyforecast_intermediate_data_path": str(tmp_path)}):
            result = get_last_forecast_dates_per_gauge("PENTAD")
            assert "15013" in result
            assert result["15013"] == dt.date(2024, 1, 5)
