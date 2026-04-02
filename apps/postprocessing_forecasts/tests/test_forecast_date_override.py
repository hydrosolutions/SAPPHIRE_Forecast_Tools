"""Unit tests for SAPPHIRE_FORECAST_DATE env var parsing in postprocessing.

Tests the date override logic added in FD-009. The parsing block is inside
postprocessing_operational.py and cannot be called independently, so we test
the pattern directly.
"""

import datetime as dt
import os

import pytest


def parse_forecast_date_override():
    """Replicate the SAPPHIRE_FORECAST_DATE parsing from postprocessing_operational.py:189."""
    _env_date = os.getenv("SAPPHIRE_FORECAST_DATE", "").strip()
    if _env_date:
        try:
            return dt.datetime.strptime(_env_date, "%Y-%m-%d").date()
        except ValueError:
            return None  # signals fallback to today
    return None  # signals fallback to today


class TestForecastDateOverride:
    """Tests for SAPPHIRE_FORECAST_DATE env var parsing."""

    def test_no_env_var_returns_none(self, monkeypatch):
        """No env var set → returns None (fallback to today)."""
        monkeypatch.delenv("SAPPHIRE_FORECAST_DATE", raising=False)

        result = parse_forecast_date_override()

        assert result is None

    def test_empty_string_returns_none(self, monkeypatch):
        """Empty string → returns None (fallback to today)."""
        monkeypatch.setenv("SAPPHIRE_FORECAST_DATE", "")

        result = parse_forecast_date_override()

        assert result is None

    def test_whitespace_only_returns_none(self, monkeypatch):
        """Whitespace-only value → returns None (strip() handles this)."""
        monkeypatch.setenv("SAPPHIRE_FORECAST_DATE", "  ")

        result = parse_forecast_date_override()

        assert result is None

    @pytest.mark.parametrize(
        "env_value, expected",
        [
            ("2026-03-25", dt.date(2026, 3, 25)),
            (" 2026-03-25 ", dt.date(2026, 3, 25)),
            ("2026-01-05", dt.date(2026, 1, 5)),
            ("2026-02-28", dt.date(2026, 2, 28)),
            ("2024-02-29", dt.date(2024, 2, 29)),
        ],
    )
    def test_valid_dates_parsed_correctly(self, monkeypatch, env_value, expected):
        """Valid date strings (with or without surrounding whitespace) parse correctly."""
        monkeypatch.setenv("SAPPHIRE_FORECAST_DATE", env_value)

        result = parse_forecast_date_override()

        assert result == expected

    @pytest.mark.parametrize(
        "env_value",
        [
            "25-03-2026",  # wrong order (DD-MM-YYYY)
            "2026-02-30",  # invalid day for February
            "2026-02-29",  # Feb 29 in non-leap year
            "not-a-date",  # garbage
        ],
    )
    def test_invalid_values_return_none(self, monkeypatch, env_value):
        """Invalid or malformed date strings → returns None (fallback to today)."""
        monkeypatch.setenv("SAPPHIRE_FORECAST_DATE", env_value)

        result = parse_forecast_date_override()

        assert result is None
