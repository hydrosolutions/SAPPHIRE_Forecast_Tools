"""Tests for lt_utils module."""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lt_utils import check_valid_forecast_issue_date, nearest_scheduled_issue_date


class TestNearestScheduledIssueDate:
    """Tests for nearest_scheduled_issue_date function."""

    def test_finds_distant_valid_month(self):
        """Test that function searches beyond +-1 month to find valid dates."""
        today = pd.Timestamp("2024-10-15")
        issue_day = 10

        # Only allow April-June - function should find June 10 (nearest valid)
        result = nearest_scheduled_issue_date(today, issue_day, [4, 5, 6])
        assert result == pd.Timestamp("2024-06-10")  # ~127 days ago

    def test_empty_months_raises(self):
        """Test error when possible_forecast_months is empty."""
        with pytest.raises(ValueError, match="cannot be empty"):
            nearest_scheduled_issue_date(pd.Timestamp("2024-10-15"), 10, [])

    def test_all_months_valid(self):
        """Test default behavior when all months valid."""
        result = nearest_scheduled_issue_date(pd.Timestamp("2024-10-15"), 10, list(range(1, 13)))
        assert result == pd.Timestamp("2024-10-10")

    def test_picks_closest_valid(self):
        """Test that nearest valid month is chosen when multiple exist."""
        today = pd.Timestamp("2024-07-15")
        # Valid: March, September - September is closer
        result = nearest_scheduled_issue_date(today, 10, [3, 9])
        assert result == pd.Timestamp("2024-09-10")

    def test_year_boundary_forward(self):
        """Test year rollover when looking ahead."""
        today = pd.Timestamp("2024-11-15")
        # Only allow January - should find January next year
        result = nearest_scheduled_issue_date(today, 10, [1])
        assert result == pd.Timestamp("2025-01-10")

    def test_year_boundary_backward(self):
        """Test year rollover when looking back."""
        today = pd.Timestamp("2024-02-15")
        # Only allow November-December - should find December last year
        result = nearest_scheduled_issue_date(today, 10, [11, 12])
        assert result == pd.Timestamp("2023-12-10")

    def test_issue_day_clamped_to_month_length(self):
        """Test that issue_day is clamped to valid day for short months."""
        today = pd.Timestamp("2024-02-15")
        # February 2024 has 29 days (leap year), issue_day=31 should clamp to 29
        result = nearest_scheduled_issue_date(today, 31, [2])
        assert result == pd.Timestamp("2024-02-29")

    def test_current_month_valid(self):
        """Test that current month is preferred when valid."""
        today = pd.Timestamp("2024-06-12")
        result = nearest_scheduled_issue_date(today, 10, [3, 6, 9])
        assert result == pd.Timestamp("2024-06-10")

    def test_single_valid_month(self):
        """Test with only one valid month in the year."""
        today = pd.Timestamp("2024-07-15")
        # Only April is valid
        result = nearest_scheduled_issue_date(today, 10, [4])
        # April 10 2024 is closer than April 10 2025
        assert result == pd.Timestamp("2024-04-10")


class TestCheckValidForecastIssueDate:
    """Tests for check_valid_forecast_issue_date."""

    def _make_mock_config(self, issue_day, forecast_months=None):
        """Create a mock ForecastConfig."""
        config = MagicMock()
        config.get_operational_issue_day.return_value = issue_day
        config.get_forecast_months.return_value = forecast_months or list(range(1, 13))
        return config

    @patch("lt_utils.get_today")
    def test_returns_none_when_outside_window(self, mock_get_today):
        """When >10 days from issue date, returns None (not raises).

        The tolerance is temporarily widened from 5 to 10 days
        (see lt_utils.py check_valid_forecast_issue_date).
        """
        mock_get_today.return_value = pd.Timestamp("2024-03-25")
        config = self._make_mock_config(issue_day=10)

        result = check_valid_forecast_issue_date(config, "LR_Base")

        assert result is None

    @patch("lt_utils.get_today")
    def test_returns_date_when_within_window(self, mock_get_today):
        """When within 5 days of issue date, returns the adjusted date."""
        mock_get_today.return_value = pd.Timestamp("2024-03-12")
        config = self._make_mock_config(issue_day=10)

        result = check_valid_forecast_issue_date(config, "LR_Base")

        assert result is not None
        # Should snap back to issue date since we're late
        assert result == pd.Timestamp("2024-03-10")

    @patch("lt_utils.get_today")
    def test_returns_today_when_on_issue_day(self, mock_get_today):
        """When exactly on issue day, returns today."""
        mock_get_today.return_value = pd.Timestamp("2024-03-10")
        config = self._make_mock_config(issue_day=10)

        result = check_valid_forecast_issue_date(config, "LR_Base")

        assert result == pd.Timestamp("2024-03-10")

    @patch("lt_utils.get_today")
    def test_returns_today_when_early(self, mock_get_today):
        """When before issue day (within window), returns today unchanged."""
        mock_get_today.return_value = pd.Timestamp("2024-03-07")
        config = self._make_mock_config(issue_day=10)

        result = check_valid_forecast_issue_date(config, "LR_Base")

        # Early runs keep their date (no snap-back)
        assert result == pd.Timestamp("2024-03-07")
