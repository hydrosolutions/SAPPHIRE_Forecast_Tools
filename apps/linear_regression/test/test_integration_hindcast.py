"""
Integration tests for hindcast date detection pipeline.

Tests get_last_forecast_dates_per_gauge and get_hindcast_start_date_from_output
using real CSV files written to tmp_path, with only env vars patched.
"""
import datetime as dt
import os
import sys

import pandas as pd
import pytest
from unittest.mock import patch

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)
from linear_regression import (
    get_last_forecast_dates_per_gauge,
    get_hindcast_start_date_from_output,
    get_next_forecast_day,
)


def _write_csv(path, dates, codes, filename='forecast_pentad_linreg.csv'):
    """Helper to write a forecast CSV with date and code columns."""
    df = pd.DataFrame({'date': dates, 'code': codes, 'value': [1.0] * len(dates)})
    df.to_csv(path / filename, index=False)


# ============================================================================
# get_last_forecast_dates_per_gauge integration
# ============================================================================

class TestLastForecastDatesIntegration:
    """Integration tests: real CSVs → get_last_forecast_dates_per_gauge."""

    def test_single_gauge_pentad(self, tmp_path):
        """Single gauge in pentad CSV → correct last date."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05', '2024-01-10', '2024-01-15'],
            codes=[15013, 15013, 15013],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path)
        }):
            result = get_last_forecast_dates_per_gauge('PENTAD')
            assert result == {'15013': dt.date(2024, 1, 15)}

    def test_single_gauge_decad(self, tmp_path):
        """Single gauge in decad CSV → correct last date."""
        _write_csv(
            tmp_path,
            dates=['2024-01-10', '2024-01-20', '2024-01-31'],
            codes=[15013, 15013, 15013],
            filename='forecast_decad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path)
        }):
            result = get_last_forecast_dates_per_gauge('DECAD')
            assert result == {'15013': dt.date(2024, 1, 31)}

    def test_both_mode_merges_pentad_and_decad(self, tmp_path):
        """BOTH mode takes max date across pentad and decad CSVs."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05', '2024-01-10'],
            codes=[15013, 15013],
            filename='forecast_pentad_linreg.csv',
        )
        _write_csv(
            tmp_path,
            dates=['2024-02-10', '2024-02-20'],
            codes=[15013, 15013],
            filename='forecast_decad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path)
        }):
            result = get_last_forecast_dates_per_gauge('BOTH')
            assert result == {'15013': dt.date(2024, 2, 20)}

    def test_multiple_gauges(self, tmp_path):
        """Multiple gauges → per-gauge last dates."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05', '2024-02-10', '2024-01-15', '2024-03-20'],
            codes=[15013, 15013, 16100, 16100],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path)
        }):
            result = get_last_forecast_dates_per_gauge('PENTAD')
            assert result == {
                '15013': dt.date(2024, 2, 10),
                '16100': dt.date(2024, 3, 20),
            }

    def test_float_codes_in_csv_cleaned(self, tmp_path):
        """Float codes like 15013.0 are cleaned to string '15013'."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05'],
            codes=[15013.0],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path)
        }):
            result = get_last_forecast_dates_per_gauge('PENTAD')
            assert '15013' in result
            # Not '15013.0'
            assert '15013.0' not in result


# ============================================================================
# get_hindcast_start_date_from_output integration
# ============================================================================

class TestHindcastStartDateIntegration:
    """Full pipeline: real CSV → start date detection."""

    def test_one_gauge_start_is_last_date_plus_one(self, tmp_path):
        """1 gauge: start = last_date + 1 day."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05', '2024-01-10'],
            codes=[15013, 15013],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=None
            )
            assert result == dt.date(2024, 1, 11)

    def test_three_gauges_uses_second_earliest(self, tmp_path):
        """3 gauges: uses second earliest start date (skips outlier)."""
        _write_csv(
            tmp_path,
            dates=['2020-01-05', '2024-06-10', '2024-06-15'],
            codes=[10001, 20002, 30003],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=None
            )
            # Gauge starts: 2020-01-06, 2024-06-11, 2024-06-16
            # Second earliest = 2024-06-11
            assert result == dt.date(2024, 6, 11)

    def test_two_gauges_second_earliest_is_later(self, tmp_path):
        """2 gauges: second earliest = the later of the two."""
        _write_csv(
            tmp_path,
            dates=['2024-01-10', '2024-03-20'],
            codes=[15013, 16100],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=None
            )
            # Starts: 2024-01-11, 2024-03-21 → second = 2024-03-21
            assert result == dt.date(2024, 3, 21)

    def test_new_gauge_with_env_default(self, tmp_path):
        """New gauge (in site_list, not in CSV) + env default → env date
        enters the pool of start dates."""
        _write_csv(
            tmp_path,
            dates=['2024-06-10'],
            codes=[15013],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyhydroforecast_START_DATE': '2000-01-01',
        }):
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=['15013', '99999']
            )
            # Starts: 2024-06-11 (existing), 2000-01-01 (new gauge env)
            # Second earliest = 2024-06-11
            assert result == dt.date(2024, 6, 11)

    def test_all_new_gauges_returns_env_default(self, tmp_path):
        """All gauges are new (no CSV data) → returns env default."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyhydroforecast_START_DATE': '2000-01-01',
        }):
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=['99999']
            )
            assert result == dt.date(2000, 1, 1)

    def test_new_gauges_without_env_default_returns_none(self, tmp_path):
        """New gauges detected but no env default → None."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=['99999']
            )
            assert result is None

    def test_invalid_env_date_format_returns_none(self, tmp_path):
        """Invalid date format in env var → None (no files, new gauge)."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyhydroforecast_START_DATE': 'not-a-date',
        }):
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=['99999']
            )
            # Invalid date → default_start_date is None → new gauge
            # with no default → returns None
            assert result is None

    def test_pentad_mode_ignores_decad_csv(self, tmp_path):
        """PENTAD mode only reads pentad CSV, ignores decad."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05'],
            codes=[15013],
            filename='forecast_pentad_linreg.csv',
        )
        _write_csv(
            tmp_path,
            dates=['2024-06-20'],
            codes=[15013],
            filename='forecast_decad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'PENTAD', site_list=None
            )
            # Only pentad considered → start = 2024-01-06
            assert result == dt.date(2024, 1, 6)

    def test_decad_mode_ignores_pentad_csv(self, tmp_path):
        """DECAD mode only reads decad CSV, ignores pentad."""
        _write_csv(
            tmp_path,
            dates=['2024-06-15'],
            codes=[15013],
            filename='forecast_pentad_linreg.csv',
        )
        _write_csv(
            tmp_path,
            dates=['2024-01-10'],
            codes=[15013],
            filename='forecast_decad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            result = get_hindcast_start_date_from_output(
                'DECAD', site_list=None
            )
            # Only decad considered → start = 2024-01-11
            assert result == dt.date(2024, 1, 11)

    def test_full_pipeline_csv_to_next_forecast_day(self, tmp_path):
        """End-to-end: CSV → start date → get_next_forecast_day → valid day."""
        _write_csv(
            tmp_path,
            dates=['2024-01-05', '2024-01-10', '2024-01-15'],
            codes=[15013, 15013, 15013],
            filename='forecast_pentad_linreg.csv',
        )
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
        }):
            os.environ.pop('ieasyhydroforecast_START_DATE', None)
            start_date = get_hindcast_start_date_from_output(
                'PENTAD', site_list=None
            )
            assert start_date == dt.date(2024, 1, 16)

            next_day = get_next_forecast_day(start_date, 'PENTAD')
            assert next_day == dt.date(2024, 1, 20)

            # Verify the returned date is actually a forecast day
            forecast_days = [5, 10, 15, 20, 25, 31]  # Jan PENTAD
            assert next_day.day in forecast_days
