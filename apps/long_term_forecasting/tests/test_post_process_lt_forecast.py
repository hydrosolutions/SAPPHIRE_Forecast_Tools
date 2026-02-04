"""
Tests for the post_process_lt_forecast module.

Tests both operational (single year) and hindcast (multiple years) settings.
"""
import pytest
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from post_process_lt_forecast import (
    infer_q_columns,
    calculate_lt_statistics_fc_period,
    calculate_lt_statistics_calendar_month,
    map_forecasted_period_to_calendar_month,
    adjust_forecast_to_calendar_month,
    post_process_lt_forecast
)


class MockForecastConfig:
    """Mock ForecastConfig for testing without environment dependencies."""
    def __init__(self, operational_month_lead_time: int = 2):
        self.operational_month_lead_time = operational_month_lead_time


def generate_discharge_data(codes: list, start_year: int, end_year: int,
                            base_values: dict = None, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic discharge data for testing.

    Parameters
    ----------
    codes : list
        List of basin codes
    start_year : int
        Start year for data generation
    end_year : int
        End year for data generation (inclusive)
    base_values : dict
        Base discharge values per code (optional)
    seed : int
        Random seed for reproducibility

    Returns
    -------
    pd.DataFrame
        Discharge data with columns: date, code, discharge
    """
    np.random.seed(seed)

    if base_values is None:
        base_values = {code: 100 + i * 50 for i, code in enumerate(codes)}

    records = []
    for year in range(start_year, end_year + 1):
        for code in codes:
            # Generate daily data for the year
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            dates = pd.date_range(start_date, end_date, freq='D')

            for date in dates:
                # Seasonal pattern: higher in summer months
                month = date.month
                seasonal_factor = 1 + 0.5 * np.sin((month - 4) * np.pi / 6)

                # Add some year-to-year variability
                if end_year > start_year:
                    year_factor = 1 + 0.1 * (year - start_year) / (end_year - start_year)
                else:
                    year_factor = 1.0

                # Base value with seasonal and random variation
                base = base_values[code]
                discharge = base * seasonal_factor * year_factor * (1 + 0.2 * np.random.randn())
                discharge = max(0, discharge)  # Non-negative

                records.append({
                    'date': date,
                    'code': code,
                    'discharge': discharge
                })

    return pd.DataFrame(records)


def generate_forecast_data(codes: list, issue_dates: list,
                           valid_from_offset: int = 35,
                           valid_to_offset: int = 64,
                           seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic forecast data for testing.

    Parameters
    ----------
    codes : list
        List of basin codes
    issue_dates : list
        List of forecast issue dates
    valid_from_offset : int
        Days from issue date to valid_from
    valid_to_offset : int
        Days from issue date to valid_to
    seed : int
        Random seed for reproducibility

    Returns
    -------
    pd.DataFrame
        Forecast data with Q columns and metadata
    """
    np.random.seed(seed)

    records = []
    for issue_date in issue_dates:
        issue_dt = pd.to_datetime(issue_date)
        valid_from = issue_dt + timedelta(days=valid_from_offset)
        valid_to = issue_dt + timedelta(days=valid_to_offset)

        for code in codes:
            # Generate forecast values (percentiles should be ordered)
            base = 100 + np.random.randn() * 20
            q5 = base * 0.7
            q10 = base * 0.8
            q25 = base * 0.9
            q50 = base
            q75 = base * 1.1
            q90 = base * 1.2
            q95 = base * 1.3
            q_mc_ald = base * (1 + 0.05 * np.random.randn())
            q_loc = base * (1 + 0.03 * np.random.randn())

            records.append({
                'date': issue_dt,
                'code': code,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'Q5': q5,
                'Q10': q10,
                'Q25': q25,
                'Q50': q50,
                'Q75': q75,
                'Q90': q90,
                'Q95': q95,
                'Q_MC_ALD': q_mc_ald,
                'Q_loc': q_loc,
                'flag': 0  # operational
            })

    return pd.DataFrame(records)


class TestInferQColumns:
    """Tests for infer_q_columns function."""

    def test_infer_standard_q_columns(self):
        """Test that standard Q columns are correctly identified."""
        df = pd.DataFrame({
            'date': [1, 2],
            'code': ['A', 'B'],
            'Q5': [10, 20],
            'Q50': [50, 60],
            'Q95': [90, 100],
            'Q_MC_ALD': [55, 65],
            'Q_loc': [52, 62],
            'other_col': [1, 2]
        })

        q_cols = infer_q_columns(df)

        assert 'Q5' in q_cols
        assert 'Q50' in q_cols
        assert 'Q95' in q_cols
        assert 'Q_MC_ALD' in q_cols
        assert 'Q_loc' in q_cols
        assert 'other_col' not in q_cols
        assert 'date' not in q_cols
        assert 'code' not in q_cols

    def test_excludes_q_obs(self):
        """Test that Q_obs is excluded from Q columns."""
        df = pd.DataFrame({
            'Q50': [50, 60],
            'Q_obs': [48, 58]
        })

        q_cols = infer_q_columns(df)

        assert 'Q50' in q_cols
        assert 'Q_obs' not in q_cols


class TestCalculateLtStatisticsFcPeriod:
    """Tests for calculate_lt_statistics_fc_period function."""

    @pytest.fixture
    def sample_data(self):
        """Create sample discharge and prediction data."""
        codes = ['BASIN_A', 'BASIN_B']
        discharge_data = generate_discharge_data(codes, 2010, 2020)

        # Single forecast for 2020
        forecast_data = generate_forecast_data(
            codes, ['2020-04-25'],
            valid_from_offset=36,  # May 31
            valid_to_offset=65     # June 29
        )

        return discharge_data, forecast_data

    def test_basic_calculation(self, sample_data):
        """Test basic statistics calculation."""
        discharge_data, forecast_data = sample_data

        result = calculate_lt_statistics_fc_period(discharge_data, forecast_data)

        # Check output columns
        assert 'code' in result.columns
        assert 'valid_from' in result.columns
        assert 'valid_to' in result.columns
        assert 'year' in result.columns
        assert 'fc_period_lt_mean' in result.columns
        assert 'fc_period_lt_std' in result.columns
        assert 'fc_period_lt_n' in result.columns

        # Check that we have results for each code
        assert len(result) == 2  # 2 codes

        # Check that statistics are calculated
        for _, row in result.iterrows():
            assert not pd.isna(row['fc_period_lt_mean'])
            assert row['fc_period_lt_n'] > 0

    def test_leave_one_out(self, sample_data):
        """Test that prediction year is excluded from statistics."""
        discharge_data, forecast_data = sample_data

        result = calculate_lt_statistics_fc_period(discharge_data, forecast_data)

        # The prediction year is 2020, so we should have 10 years (2010-2019)
        for _, row in result.iterrows():
            assert row['fc_period_lt_n'] == 10  # 2010-2019, excluding 2020


class TestCalculateLtStatisticsCalendarMonth:
    """Tests for calculate_lt_statistics_calendar_month function."""

    @pytest.fixture
    def sample_discharge(self):
        """Create sample discharge data."""
        codes = ['BASIN_A']
        return generate_discharge_data(codes, 2010, 2020)

    def test_leave_one_out_per_year(self, sample_discharge):
        """Test that each prediction year gets its own statistics."""
        prediction_years = [2018, 2019, 2020]

        result = calculate_lt_statistics_calendar_month(
            sample_discharge, prediction_years
        )

        # Check output columns
        assert 'code' in result.columns
        assert 'month' in result.columns
        assert 'year' in result.columns
        assert 'calendar_month_lt_mean' in result.columns

        # Should have 3 years * 12 months * 1 code = 36 rows
        assert len(result) == 3 * 12 * 1

        # Check that each year has different exclusion
        for year in prediction_years:
            year_stats = result[result['year'] == year]
            assert len(year_stats) == 12  # 12 months

    def test_n_values_differ_by_one(self, sample_discharge):
        """Test that N differs appropriately for different exclusion years."""
        prediction_years = [2015, 2020]

        result = calculate_lt_statistics_calendar_month(
            sample_discharge, prediction_years
        )

        # Both should have same N (excluding one year each from 2010-2020)
        stats_2015 = result[(result['year'] == 2015) & (result['month'] == 6)]
        stats_2020 = result[(result['year'] == 2020) & (result['month'] == 6)]

        assert stats_2015['calendar_month_lt_n'].values[0] == 10
        assert stats_2020['calendar_month_lt_n'].values[0] == 10


class TestMapForecastedPeriodToCalendarMonth:
    """Tests for map_forecasted_period_to_calendar_month function."""

    def test_target_month_calculation(self):
        """Test that target month is calculated correctly."""
        # Create minimal test data
        prediction_data = pd.DataFrame({
            'date': pd.to_datetime(['2020-04-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2020-05-31']),
            'valid_to': pd.to_datetime(['2020-06-29']),
            'Q50': [100.0]
        })

        fc_period_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2020-05-31']),
            'valid_to': pd.to_datetime(['2020-06-29']),
            'year': [2020],
            'fc_period_lt_mean': [95.0],
            'fc_period_lt_std': [10.0],
            'fc_period_lt_n': [10]
        })

        calendar_month_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'month': [6],  # June
            'year': [2020],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = map_forecasted_period_to_calendar_month(
            prediction_data, fc_period_stats, calendar_month_stats,
            operational_month_lead_time=2
        )

        # April (4) + 2 = June (6)
        assert result['target_month'].values[0] == 6

        # Check that statistics are merged
        assert 'fc_period_lt_mean' in result.columns
        assert 'calendar_month_lt_mean' in result.columns

    def test_month_overflow(self):
        """Test that month calculation handles year boundary."""
        prediction_data = pd.DataFrame({
            'date': pd.to_datetime(['2020-11-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2021-01-01']),
            'valid_to': pd.to_datetime(['2021-01-30']),
            'Q50': [100.0]
        })

        fc_period_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2021-01-01']),
            'valid_to': pd.to_datetime(['2021-01-30']),
            'year': [2020],
            'fc_period_lt_mean': [50.0],
            'fc_period_lt_std': [5.0],
            'fc_period_lt_n': [10]
        })

        calendar_month_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'month': [1],  # January
            'year': [2020],
            'calendar_month_lt_mean': [55.0],
            'calendar_month_lt_std': [8.0],
            'calendar_month_lt_n': [10]
        })

        result = map_forecasted_period_to_calendar_month(
            prediction_data, fc_period_stats, calendar_month_stats,
            operational_month_lead_time=2
        )

        # November (11) + 2 = January (1)
        assert result['target_month'].values[0] == 1


class TestAdjustForecastToCalendarMonth:
    """Tests for adjust_forecast_to_calendar_month function."""

    def test_basic_adjustment(self):
        """Test basic ratio adjustment."""
        mapped_data = pd.DataFrame({
            'Q50': [100.0],
            'fc_period_lt_mean': [90.0],
            'fc_period_lt_std': [10.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50'])

        # Q = cal_mean * (Q_raw / fc_mean) = 100 * (100/90) ≈ 111.11
        # But clipping may apply
        assert 'Q50' in result.columns
        assert not pd.isna(result['Q50'].values[0])
        assert result['Q50'].values[0] > 0

    def test_case_a_no_statistics(self):
        """Test Case A: N=0 keeps raw forecast."""
        mapped_data = pd.DataFrame({
            'Q50': [100.0],
            'fc_period_lt_mean': [np.nan],
            'fc_period_lt_std': [np.nan],
            'fc_period_lt_n': [0],
            'calendar_month_lt_mean': [np.nan],
            'calendar_month_lt_std': [np.nan],
            'calendar_month_lt_n': [0]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50'])

        # Should keep raw forecast
        assert result['Q50'].values[0] == 100.0

    def test_non_negative_output(self):
        """Test that output is always non-negative."""
        mapped_data = pd.DataFrame({
            'Q50': [10.0],  # Very low forecast
            'fc_period_lt_mean': [100.0],  # High historical mean
            'fc_period_lt_std': [10.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50'])

        assert result['Q50'].values[0] >= 0

    def test_all_q_columns_adjusted(self):
        """Test that all Q columns are adjusted."""
        mapped_data = pd.DataFrame({
            'Q5': [70.0],
            'Q50': [100.0],
            'Q95': [130.0],
            'Q_MC_ALD': [105.0],
            'fc_period_lt_mean': [95.0],
            'fc_period_lt_std': [10.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        q_columns = ['Q5', 'Q50', 'Q95', 'Q_MC_ALD']
        result = adjust_forecast_to_calendar_month(mapped_data, q_columns)

        for q_col in q_columns:
            assert q_col in result.columns
            assert not pd.isna(result[q_col].values[0])


class TestPostProcessLtForecastOperational:
    """Tests for operational (single year) forecast processing."""

    @pytest.fixture
    def operational_setup(self):
        """Set up operational forecast scenario."""
        codes = ['BASIN_A', 'BASIN_B']

        # Historical discharge data (2010-2023)
        discharge_data = generate_discharge_data(codes, 2010, 2023)

        # Single operational forecast for 2024
        forecast_data = generate_forecast_data(
            codes, ['2024-04-25'],
            valid_from_offset=36,
            valid_to_offset=65
        )

        config = MockForecastConfig(operational_month_lead_time=2)

        return config, discharge_data, forecast_data

    def test_operational_forecast_processing(self, operational_setup):
        """Test processing of operational forecast."""
        config, discharge_data, forecast_data = operational_setup

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Check that Q columns exist (now overwritten with adjusted values)
        q_columns = infer_q_columns(forecast_data)
        for q_col in q_columns:
            assert q_col in result.columns

        # Check that metadata is preserved
        assert 'date' in result.columns
        assert 'code' in result.columns

        # Check non-negative values
        for q_col in q_columns:
            assert (result[q_col].dropna() >= 0).all()

    def test_operational_uses_all_historical_years(self, operational_setup):
        """Test that operational mode uses all historical years except prediction year."""
        config, discharge_data, forecast_data = operational_setup

        # The prediction year is 2024, so all years 2010-2023 should be used
        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # fc_period_lt_n should be 14 (2010-2023)
        assert (result['fc_period_lt_n'] == 14).all()


class TestPostProcessLtForecastHindcast:
    """Tests for hindcast (multiple years) forecast processing."""

    @pytest.fixture
    def hindcast_setup(self):
        """Set up hindcast scenario."""
        codes = ['BASIN_A']

        # Historical discharge data (2010-2020)
        discharge_data = generate_discharge_data(codes, 2010, 2020)

        # Hindcast forecasts for 2018, 2019, 2020
        issue_dates = ['2018-04-25', '2019-04-25', '2020-04-25']
        forecast_data = generate_forecast_data(
            codes, issue_dates,
            valid_from_offset=36,
            valid_to_offset=65
        )
        forecast_data['flag'] = 1  # hindcast flag

        config = MockForecastConfig(operational_month_lead_time=2)

        return config, discharge_data, forecast_data

    def test_hindcast_processing(self, hindcast_setup):
        """Test processing of hindcast forecasts."""
        config, discharge_data, forecast_data = hindcast_setup

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Check that we have 3 forecasts (one per year)
        assert len(result) == 3

        # Check Q columns exist (now overwritten with adjusted values)
        q_columns = infer_q_columns(forecast_data)
        for q_col in q_columns:
            assert q_col in result.columns

    def test_hindcast_leave_one_out(self, hindcast_setup):
        """Test that each hindcast year excludes only its own year."""
        config, discharge_data, forecast_data = hindcast_setup

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Each forecast should have N = 10 (11 years - 1 excluded)
        # 2018 forecast: uses 2010-2017, 2019-2020 (10 years)
        # 2019 forecast: uses 2010-2018, 2020 (10 years)
        # 2020 forecast: uses 2010-2019 (10 years)
        assert (result['fc_period_lt_n'] == 10).all()
        assert (result['calendar_month_lt_n'] == 10).all()

    def test_hindcast_no_data_leakage(self, hindcast_setup):
        """Test that prediction year data is not used in statistics."""
        config, discharge_data, forecast_data = hindcast_setup

        # Modify discharge for 2020 to be very different
        discharge_data_modified = discharge_data.copy()
        mask_2020 = discharge_data_modified['date'].dt.year == 2020
        discharge_data_modified.loc[mask_2020, 'discharge'] *= 10  # 10x higher

        result = post_process_lt_forecast(config, discharge_data_modified, forecast_data)

        # Get the 2020 forecast result
        result['forecast_year'] = pd.to_datetime(result['date']).dt.year
        result_2020 = result[result['forecast_year'] == 2020]

        # The fc_period_lt_mean for 2020 should NOT include the modified 2020 data
        # It should be similar to other years' fc_period_lt_mean
        result_2019 = result[result['forecast_year'] == 2019]

        # The means should be similar (within reasonable range)
        # since 2020 data is excluded from 2020's statistics
        mean_diff_ratio = abs(
            result_2020['fc_period_lt_mean'].values[0] -
            result_2019['fc_period_lt_mean'].values[0]
        ) / result_2019['fc_period_lt_mean'].values[0]

        # Difference should be small (< 50%) if no leakage
        assert mean_diff_ratio < 0.5


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_forecast_data(self):
        """Test handling of empty forecast data."""
        discharge_data = generate_discharge_data(['BASIN_A'], 2010, 2020)
        forecast_data = pd.DataFrame(columns=[
            'date', 'code', 'valid_from', 'valid_to', 'Q50'
        ])
        config = MockForecastConfig(operational_month_lead_time=2)

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        assert len(result) == 0

    def test_missing_discharge_data(self):
        """Test handling of discharge data with missing values."""
        codes = ['BASIN_A']
        discharge_data = generate_discharge_data(codes, 2010, 2020)

        # Add some missing values
        discharge_data.loc[discharge_data.index[:100], 'discharge'] = np.nan

        forecast_data = generate_forecast_data(codes, ['2020-04-25'])
        config = MockForecastConfig(operational_month_lead_time=2)

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Should still produce results
        assert len(result) == 1
        assert 'Q50' in result.columns

    def test_single_year_historical_data(self):
        """Test with only one year of historical data."""
        codes = ['BASIN_A']
        discharge_data = generate_discharge_data(codes, 2020, 2020)
        forecast_data = generate_forecast_data(codes, ['2021-04-25'])
        config = MockForecastConfig(operational_month_lead_time=2)

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Should have N=1 for statistics
        assert len(result) == 1
        # With only 1 year, std should be NaN
        assert pd.isna(result['fc_period_lt_std'].values[0])


# Run tests with pytest
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
