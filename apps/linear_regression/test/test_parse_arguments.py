"""
Unit tests for parse_arguments() in linear_regression.py.

Tests CLI argument parsing for forecast (default) and hindcast modes,
including date validation, default values, and error handling.
"""

import datetime as dt
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from linear_regression import parse_arguments

# ============================================================================
# Forecast mode (no --hindcast flag)
# ============================================================================


class TestParseArgumentsForecastMode:
    """Default forecast mode returns expected defaults."""

    def test_no_arguments_returns_forecast_defaults(self):
        """No arguments → hindcast=False, start_date=None, end_date=None."""
        with patch("sys.argv", ["linear_regression.py"]):
            args = parse_arguments()
            assert args.hindcast is False
            assert args.start_date is None
            assert args.end_date is None


# ============================================================================
# Hindcast mode
# ============================================================================


class TestParseArgumentsHindcastMode:
    """Hindcast flag and date parsing."""

    def test_hindcast_flag_only(self):
        """--hindcast alone → hindcast=True, start_date=None,
        end_date=yesterday."""
        with patch("sys.argv", ["linear_regression.py", "--hindcast"]):
            args = parse_arguments()
            assert args.hindcast is True
            assert args.start_date is None
            assert args.end_date == dt.date.today() - dt.timedelta(days=1)

    def test_hindcast_with_start_date(self):
        """--hindcast --start-date → start parsed to date object."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--start-date",
                "2024-03-15",
            ],
        ):
            args = parse_arguments()
            assert args.hindcast is True
            assert args.start_date == dt.date(2024, 3, 15)
            assert args.end_date == dt.date.today() - dt.timedelta(days=1)

    def test_hindcast_with_start_and_end_date(self):
        """--hindcast --start-date --end-date → both parsed to date."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-06-30",
            ],
        ):
            args = parse_arguments()
            assert args.start_date == dt.date(2024, 1, 1)
            assert args.end_date == dt.date(2024, 6, 30)

    def test_hindcast_short_flags(self):
        """-H -s -e short flags work the same as long forms."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "-H",
                "-s",
                "2024-01-01",
                "-e",
                "2024-06-30",
            ],
        ):
            args = parse_arguments()
            assert args.hindcast is True
            assert args.start_date == dt.date(2024, 1, 1)
            assert args.end_date == dt.date(2024, 6, 30)

    def test_end_date_defaults_to_yesterday(self):
        """End date defaults to exactly date.today() - 1 day."""
        fixed_today = dt.date(2025, 7, 15)
        expected_yesterday = dt.date(2025, 7, 14)

        with (
            patch("sys.argv", ["linear_regression.py", "--hindcast"]),
            patch("linear_regression.dt") as mock_dt,
        ):
            # Keep the real datetime class behavior but override today()
            mock_dt.date.today.return_value = fixed_today
            mock_dt.timedelta = dt.timedelta
            mock_dt.datetime = dt.datetime
            args = parse_arguments()
            assert args.end_date == expected_yesterday


# ============================================================================
# Validation errors
# ============================================================================


class TestParseArgumentsValidation:
    """Invalid arguments produce SystemExit(2)."""

    def test_invalid_start_date_format(self):
        """Non-date string for --start-date → SystemExit(2)."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--start-date",
                "not-a-date",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                parse_arguments()
            assert exc_info.value.code == 2

    def test_invalid_end_date_format(self):
        """Wrong date format for --end-date → SystemExit(2)."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--end-date",
                "2024/01/01",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                parse_arguments()
            assert exc_info.value.code == 2

    def test_end_date_today_exits(self):
        """End date = today → SystemExit(2) (must be before today)."""
        today_str = dt.date.today().strftime("%Y-%m-%d")
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--end-date",
                today_str,
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                parse_arguments()
            assert exc_info.value.code == 2

    def test_end_date_future_exits(self):
        """End date in the future → SystemExit(2)."""
        future = (dt.date.today() + dt.timedelta(days=30)).strftime("%Y-%m-%d")
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--end-date",
                future,
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                parse_arguments()
            assert exc_info.value.code == 2

    def test_start_after_end_exits(self):
        """Start date after end date → SystemExit(2)."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--start-date",
                "2024-06-30",
                "--end-date",
                "2024-01-01",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                parse_arguments()
            assert exc_info.value.code == 2

    def test_start_equals_end_is_valid(self):
        """Same date for start and end is allowed."""
        with patch(
            "sys.argv",
            [
                "linear_regression.py",
                "--hindcast",
                "--start-date",
                "2024-06-15",
                "--end-date",
                "2024-06-15",
            ],
        ):
            args = parse_arguments()
            assert args.start_date == dt.date(2024, 6, 15)
            assert args.end_date == dt.date(2024, 6, 15)
