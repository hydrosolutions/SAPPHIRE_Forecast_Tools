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
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from post_process_lt_forecast import (
    infer_q_columns,
    calculate_lt_statistics_fc_period,
    calculate_lt_statistics_calendar_month,
    map_forecasted_period_to_calendar_month,
    adjust_forecast_to_calendar_month,
    adjust_forecast_dates_only,
    adjust_forecast_dates_dynamic,
    post_process_lt_forecast
)


class MockForecastConfig:
    """Mock ForecastConfig for testing without environment dependencies."""
    def __init__(
        self,
        operational_month_lead_time: int = 2,
        calendar_month_adjustment: bool = True,
        target_start_month: int | None = None,
        target_end_month: int | None = None,
        forecast_horizon_months: int | None = None
    ):
        self._operational_month_lead_time = operational_month_lead_time
        self._calendar_month_adjustment = calendar_month_adjustment
        self._target_start_month = target_start_month
        self._target_end_month = target_end_month
        self._forecast_horizon_months = forecast_horizon_months

    def get_operational_month_lead_time(self) -> int:
        """Return the operational month lead time."""
        return self._operational_month_lead_time

    def get_calendar_month_adjustment(self) -> bool:
        """Return whether to apply ratio-based calendar month adjustment."""
        return self._calendar_month_adjustment

    def get_target_start_month(self) -> int | None:
        """Return the fixed target start month for seasonal forecasts."""
        return self._target_start_month

    def get_target_end_month(self) -> int | None:
        """Return the fixed target end month for seasonal forecasts."""
        return self._target_end_month

    def get_forecast_horizon_months(self) -> int | None:
        """Return the forecast horizon in months for dynamic multi-month mode."""
        return self._forecast_horizon_months


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
        """Test that month calculation handles year boundary correctly.

        Scenario: Forecast issued Nov 25, 2020 for January 2021 (lead_time=2)
        - target_month should be 1 (January)
        - target_year should be 2021 (crossed year boundary)
        - valid_from should be 2021-01-01
        - valid_to should be 2021-01-31
        """
        prediction_data = pd.DataFrame({
            'date': pd.to_datetime(['2020-11-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2020-12-01']),  # Raw forecast period in Dec 2020
            'valid_to': pd.to_datetime(['2020-12-30']),
            'Q50': [100.0]
        })

        fc_period_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2020-12-01']),
            'valid_to': pd.to_datetime(['2020-12-30']),
            'year': [2020],  # Issue year for fc_period stats
            'fc_period_lt_mean': [50.0],
            'fc_period_lt_std': [5.0],
            'fc_period_lt_n': [10]
        })

        calendar_month_stats = pd.DataFrame({
            'code': ['BASIN_A'],
            'month': [1],  # January
            'year': [2021],  # Target year for calendar month stats (excludes 2021 data)
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
        # Target year should be 2021 (crossed year boundary)
        assert result['target_year'].values[0] == 2021
        # valid_from should be adjusted to 2021-01-01
        assert result['valid_from'].values[0] == pd.Timestamp('2021-01-01')
        # valid_to should be adjusted to 2021-01-31
        assert result['valid_to'].values[0] == pd.Timestamp('2021-01-31')


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


class TestQuantileSpreadPreservation:
    """Tests for quantile spread preservation in adjust_forecast_to_calendar_month.

    Verifies that the hybrid approach:
    - Clips central estimates (Q50, Q_model_name) independently
    - Applies delta correction to quantiles (Q5, Q10, Q25, Q75, Q90, Q95)
      to preserve their spread relative to Q50
    """

    def test_spread_preserved_when_clipping_upper(self):
        """Test that Q95/Q50 ratio is preserved when both would clip to upper bound.

        Scenario: fc_mean=100, upper_bound clips at ~0.34 in log-space
        - Q50_raw=160 → log_ratio=0.47 → clipped → Q50_adj
        - Q95_raw=200 → log_ratio=0.69 → with delta → Q95_adj
        - Expected: Q95_adj/Q50_adj ≈ 200/160 = 1.25 (spread preserved)
        """
        # Set up data where both Q50 and Q95 exceed the upper bound
        mapped_data = pd.DataFrame({
            'Q50': [160.0],  # log_ratio = log(160/100) = 0.47
            'Q95': [200.0],  # log_ratio = log(200/100) = 0.69
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],  # Small std → tight bounds
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50', 'Q95'])

        # Original ratio
        original_ratio = 200.0 / 160.0  # 1.25

        # Adjusted ratio should be preserved
        adjusted_ratio = result['Q95'].values[0] / result['Q50'].values[0]

        # Spread should be preserved (ratios should match)
        assert abs(adjusted_ratio - original_ratio) < 0.01, (
            f"Spread not preserved: expected ratio {original_ratio:.4f}, "
            f"got {adjusted_ratio:.4f}"
        )

    def test_spread_preserved_when_clipping_lower(self):
        """Test that Q5/Q50 ratio is preserved when both would clip to lower bound.

        Scenario: Low forecast values relative to climatology
        - Q50_raw=20 → log_ratio < lower_bound → clipped
        - Q5_raw=10 → log_ratio < lower_bound → with delta → preserved spread
        """
        mapped_data = pd.DataFrame({
            'Q50': [20.0],  # Very low: log_ratio = log(20/100) = -1.61
            'Q5': [10.0],   # Even lower: log_ratio = log(10/100) = -2.30
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50', 'Q5'])

        # Original ratio
        original_ratio = 10.0 / 20.0  # 0.5

        # Adjusted ratio should be preserved
        adjusted_ratio = result['Q5'].values[0] / result['Q50'].values[0]

        assert abs(adjusted_ratio - original_ratio) < 0.01, (
            f"Spread not preserved: expected ratio {original_ratio:.4f}, "
            f"got {adjusted_ratio:.4f}"
        )

    def test_q50_clipped_independently(self):
        """Test that Q50 is clipped to climatology bounds (independent clipping)."""
        # High Q50 that should be clipped
        mapped_data = pd.DataFrame({
            'Q50': [300.0],  # Very high relative to fc_mean
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],  # Tight bounds
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50'])

        # Q50 should be clipped (not equal to simple ratio adjustment)
        simple_ratio_adjustment = 100.0 * (300.0 / 100.0)  # = 300
        assert result['Q50'].values[0] < simple_ratio_adjustment, (
            "Q50 should be clipped below simple ratio adjustment"
        )
        assert result['Q50'].values[0] > 0, "Q50 should be positive"

    def test_q_model_name_clipped_independently(self):
        """Test that Q_MC_ALD (model name column) is clipped independently."""
        mapped_data = pd.DataFrame({
            'Q_MC_ALD': [300.0],  # High forecast
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q_MC_ALD'])

        # Should be clipped
        simple_ratio_adjustment = 100.0 * (300.0 / 100.0)
        assert result['Q_MC_ALD'].values[0] < simple_ratio_adjustment, (
            "Q_MC_ALD should be clipped"
        )

    def test_no_q50_no_delta(self):
        """Test that when Q50 is missing, quantiles get standard ratio adjustment."""
        mapped_data = pd.DataFrame({
            'Q95': [130.0],  # No Q50 in data
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q95'])

        # Without Q50, delta=0, so Q95 gets standard ratio adjustment
        # log_ratio = log(130/100) = 0.262, within bounds
        expected = 100.0 * (130.0 / 100.0)  # ~130
        assert abs(result['Q95'].values[0] - expected) < 1.0, (
            f"Without Q50, Q95 should get ratio adjustment: expected ~{expected}, "
            f"got {result['Q95'].values[0]}"
        )

    def test_multiple_rows_independent_delta(self):
        """Test that each row gets its own delta calculation."""
        mapped_data = pd.DataFrame({
            'Q50': [160.0, 80.0],   # Row 0: high, Row 1: low
            'Q95': [200.0, 100.0],  # Maintain 1.25x ratio in each row
            'fc_period_lt_mean': [100.0, 100.0],
            'fc_period_lt_std': [15.0, 15.0],
            'fc_period_lt_n': [10, 10],
            'calendar_month_lt_mean': [100.0, 100.0],
            'calendar_month_lt_std': [15.0, 15.0],
            'calendar_month_lt_n': [10, 10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50', 'Q95'])

        # Each row should preserve its own Q95/Q50 ratio
        for i in range(2):
            original_ratio = mapped_data['Q95'].values[i] / mapped_data['Q50'].values[i]
            adjusted_ratio = result['Q95'].values[i] / result['Q50'].values[i]
            assert abs(adjusted_ratio - original_ratio) < 0.01, (
                f"Row {i}: spread not preserved"
            )

    def test_all_quantiles_preserve_relative_spread(self):
        """Test that all quantile columns preserve their spread relative to Q50."""
        # High forecast where all quantiles would be clipped independently
        mapped_data = pd.DataFrame({
            'Q5': [120.0],
            'Q10': [130.0],
            'Q25': [145.0],
            'Q50': [160.0],
            'Q75': [175.0],
            'Q90': [190.0],
            'Q95': [200.0],
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        q_columns = ['Q5', 'Q10', 'Q25', 'Q50', 'Q75', 'Q90', 'Q95']
        result = adjust_forecast_to_calendar_month(mapped_data, q_columns)

        q50_raw = mapped_data['Q50'].values[0]
        q50_adj = result['Q50'].values[0]

        # Each quantile should preserve its ratio to Q50
        for q_col in ['Q5', 'Q10', 'Q25', 'Q75', 'Q90', 'Q95']:
            original_ratio = mapped_data[q_col].values[0] / q50_raw
            adjusted_ratio = result[q_col].values[0] / q50_adj
            assert abs(adjusted_ratio - original_ratio) < 0.01, (
                f"{q_col}: spread not preserved. "
                f"Expected ratio {original_ratio:.4f}, got {adjusted_ratio:.4f}"
            )

    def test_q50_nan_row_gets_zero_delta(self):
        """Test that rows where Q50 is NaN get delta=0."""
        mapped_data = pd.DataFrame({
            'Q50': [160.0, np.nan],  # Row 1 has NaN Q50
            'Q95': [200.0, 130.0],
            'fc_period_lt_mean': [100.0, 100.0],
            'fc_period_lt_std': [15.0, 15.0],
            'fc_period_lt_n': [10, 10],
            'calendar_month_lt_mean': [100.0, 100.0],
            'calendar_month_lt_std': [15.0, 15.0],
            'calendar_month_lt_n': [10, 10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50', 'Q95'])

        # Row 0: spread preserved
        ratio_0 = result['Q95'].values[0] / result['Q50'].values[0]
        assert abs(ratio_0 - 1.25) < 0.01

        # Row 1: Q95 gets standard ratio adjustment (no delta)
        # log_ratio = log(130/100) = 0.262, within bounds
        expected_q95_row1 = 100.0 * (130.0 / 100.0)
        assert abs(result['Q95'].values[1] - expected_q95_row1) < 1.0

    def test_q50_nonpositive_row_gets_zero_delta(self):
        """Test that rows where Q50 <= 0 get delta=0."""
        mapped_data = pd.DataFrame({
            'Q50': [0.0],  # Non-positive Q50
            'Q95': [50.0],
            'fc_period_lt_mean': [100.0],
            'fc_period_lt_std': [15.0],
            'fc_period_lt_n': [10],
            'calendar_month_lt_mean': [100.0],
            'calendar_month_lt_std': [15.0],
            'calendar_month_lt_n': [10]
        })

        result = adjust_forecast_to_calendar_month(mapped_data, ['Q50', 'Q95'])

        # Q50 should be 0 (non-positive handling)
        assert result['Q50'].values[0] == 0

        # Q95 should get standard ratio adjustment (delta=0)
        # log_ratio = log(50/100) = -0.69, check if within bounds
        assert result['Q95'].values[0] > 0


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
        """Test that operational mode uses all historical years except prediction year.

        Note: The final output doesn't include internal statistics columns.
        We verify this indirectly by testing calculate_lt_statistics_fc_period.
        """
        config, discharge_data, forecast_data = operational_setup

        # Test using the internal function directly
        fc_period_stats = calculate_lt_statistics_fc_period(
            discharge_data=discharge_data,
            prediction_data=forecast_data
        )

        # fc_period_lt_n should be 14 (2010-2023, excluding 2024)
        assert (fc_period_stats['fc_period_lt_n'] == 14).all()

        # Also verify main function runs successfully
        result = post_process_lt_forecast(config, discharge_data, forecast_data)
        assert len(result) == 2  # 2 codes


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
        """Test that each hindcast year excludes only its own year.

        Note: The final output doesn't include internal statistics columns.
        We verify this indirectly by testing the internal functions.
        """
        config, discharge_data, forecast_data = hindcast_setup

        # Test using the internal function directly for fc_period stats
        fc_period_stats = calculate_lt_statistics_fc_period(
            discharge_data=discharge_data,
            prediction_data=forecast_data
        )

        # Each forecast should have N = 10 (11 years - 1 excluded)
        # 2018 forecast: uses 2010-2017, 2019-2020 (10 years)
        # 2019 forecast: uses 2010-2018, 2020 (10 years)
        # 2020 forecast: uses 2010-2019 (10 years)
        assert (fc_period_stats['fc_period_lt_n'] == 10).all()

        # Test calendar month stats
        prediction_years = [2018, 2019, 2020]
        calendar_month_stats = calculate_lt_statistics_calendar_month(
            discharge_data=discharge_data,
            prediction_years=prediction_years
        )
        # For June (target month from April + 2), each year should have N=10
        june_stats = calendar_month_stats[calendar_month_stats['month'] == 6]
        assert (june_stats['calendar_month_lt_n'] == 10).all()

        # Also verify main function runs successfully
        result = post_process_lt_forecast(config, discharge_data, forecast_data)
        assert len(result) == 3  # 3 years

    def test_hindcast_no_data_leakage(self, hindcast_setup):
        """Test that prediction year data is not used in statistics.

        Note: The final output doesn't include internal statistics columns.
        We verify this by testing the internal functions directly.
        """
        config, discharge_data, forecast_data = hindcast_setup

        # Modify discharge for 2020 to be very different
        discharge_data_modified = discharge_data.copy()
        mask_2020 = discharge_data_modified['date'].dt.year == 2020
        discharge_data_modified.loc[mask_2020, 'discharge'] *= 10  # 10x higher

        # Test using the internal function directly
        fc_period_stats = calculate_lt_statistics_fc_period(
            discharge_data=discharge_data_modified,
            prediction_data=forecast_data
        )

        # Add forecast year to stats for filtering
        fc_period_stats['forecast_year'] = pd.to_datetime(
            fc_period_stats['valid_from']
        ).dt.year

        # Get stats for 2020 forecast (which should exclude 2020 data)
        stats_2020 = fc_period_stats[fc_period_stats['year'] == 2020]
        # Get stats for 2019 forecast (which should exclude 2019 data)
        stats_2019 = fc_period_stats[fc_period_stats['year'] == 2019]

        # The means should be similar (within reasonable range)
        # since 2020 data is excluded from 2020's statistics
        mean_diff_ratio = abs(
            stats_2020['fc_period_lt_mean'].values[0] -
            stats_2019['fc_period_lt_mean'].values[0]
        ) / stats_2019['fc_period_lt_mean'].values[0]

        # Difference should be small (< 50%) if no leakage
        assert mean_diff_ratio < 0.5

        # Also verify main function runs without error
        result = post_process_lt_forecast(config, discharge_data_modified, forecast_data)
        assert len(result) == 3


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
        """Test with only one year of historical data.

        Note: The final output doesn't include internal statistics columns.
        We verify this by testing the internal functions directly.
        """
        codes = ['BASIN_A']
        discharge_data = generate_discharge_data(codes, 2020, 2020)
        forecast_data = generate_forecast_data(codes, ['2021-04-25'])
        config = MockForecastConfig(operational_month_lead_time=2)

        # Test using the internal function directly
        fc_period_stats = calculate_lt_statistics_fc_period(
            discharge_data=discharge_data,
            prediction_data=forecast_data
        )

        # Should have N=1 for statistics
        assert fc_period_stats['fc_period_lt_n'].values[0] == 1
        # With only 1 year, std should be NaN
        assert pd.isna(fc_period_stats['fc_period_lt_std'].values[0])

        # Also verify main function runs successfully
        result = post_process_lt_forecast(config, discharge_data, forecast_data)
        assert len(result) == 1


class TestAdjustForecastDatesOnly:
    """Tests for adjust_forecast_dates_only function (multi-month seasonal mode)."""

    def test_adjust_dates_only_same_year(self):
        """Test Apr-Sep case where period stays within same year."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-03-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-04-24']),  # Raw model period
            'valid_to': pd.to_datetime(['2024-05-23']),
            'flag': [0],
            'Q5': [70.0],
            'Q50': [100.0],
            'Q95': [130.0]
        })

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=4,
            target_end_month=9
        )

        # Check dates adjusted to calendar month boundaries
        assert result['valid_from'].values[0] == pd.Timestamp('2024-04-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2024-09-30')

        # Check Q values unchanged
        assert result['Q5'].values[0] == 70.0
        assert result['Q50'].values[0] == 100.0
        assert result['Q95'].values[0] == 130.0

    def test_adjust_dates_only_year_boundary(self):
        """Test Nov-Feb case where period spans year boundary."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-10-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-11-24']),
            'valid_to': pd.to_datetime(['2024-12-23']),
            'flag': [0],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=11,
            target_end_month=2
        )

        # Check dates: starts in 2024, ends in 2025
        assert result['valid_from'].values[0] == pd.Timestamp('2024-11-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2025-02-28')

        # Q values unchanged
        assert result['Q50'].values[0] == 100.0

    def test_adjust_dates_only_single_month(self):
        """Test single month case (e.g., June only)."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-05-15']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-06-14']),
            'valid_to': pd.to_datetime(['2024-07-13']),
            'flag': [0],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=6,
            target_end_month=6
        )

        # Check dates adjusted to June 1-30
        assert result['valid_from'].values[0] == pd.Timestamp('2024-06-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2024-06-30')

    def test_adjust_dates_only_leap_year(self):
        """Test that leap year February is handled correctly."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-01-15']),  # 2024 is a leap year
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-02-14']),
            'valid_to': pd.to_datetime(['2024-03-14']),
            'flag': [0],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=2,
            target_end_month=2
        )

        # Check February 2024 (leap year) ends on 29th
        assert result['valid_from'].values[0] == pd.Timestamp('2024-02-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2024-02-29')

    def test_missing_target_start_month_raises_error(self):
        """Test that missing target_start_month raises ValueError."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-03-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-04-24']),
            'valid_to': pd.to_datetime(['2024-05-23']),
            'flag': [0],
            'Q50': [100.0]
        })

        with pytest.raises(ValueError, match="target_start_month is required"):
            adjust_forecast_dates_only(
                raw_forecast=forecast_data,
                target_start_month=None,
                target_end_month=9
            )

    def test_missing_target_end_month_raises_error(self):
        """Test that missing target_end_month raises ValueError."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-03-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-04-24']),
            'valid_to': pd.to_datetime(['2024-05-23']),
            'flag': [0],
            'Q50': [100.0]
        })

        with pytest.raises(ValueError, match="target_end_month is required"):
            adjust_forecast_dates_only(
                raw_forecast=forecast_data,
                target_start_month=4,
                target_end_month=None
            )

    def test_invalid_month_raises_error(self):
        """Test that invalid month values raise ValueError."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-03-25']),
            'code': ['BASIN_A'],
            'valid_from': pd.to_datetime(['2024-04-24']),
            'valid_to': pd.to_datetime(['2024-05-23']),
            'flag': [0],
            'Q50': [100.0]
        })

        with pytest.raises(ValueError, match="target_start_month must be 1-12"):
            adjust_forecast_dates_only(
                raw_forecast=forecast_data,
                target_start_month=0,
                target_end_month=9
            )

        with pytest.raises(ValueError, match="target_end_month must be 1-12"):
            adjust_forecast_dates_only(
                raw_forecast=forecast_data,
                target_start_month=4,
                target_end_month=13
            )

    def test_empty_forecast_returns_empty(self):
        """Test that empty forecast returns empty DataFrame."""
        forecast_data = pd.DataFrame(columns=[
            'date', 'code', 'valid_from', 'valid_to', 'flag', 'Q50'
        ])

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=4,
            target_end_month=9
        )

        assert len(result) == 0

    def test_multiple_forecasts(self):
        """Test that multiple forecasts are handled correctly."""
        forecast_data = pd.DataFrame({
            'date': pd.to_datetime(['2024-03-25', '2025-03-25']),
            'code': ['BASIN_A', 'BASIN_B'],
            'valid_from': pd.to_datetime(['2024-04-24', '2025-04-24']),
            'valid_to': pd.to_datetime(['2024-05-23', '2025-05-23']),
            'flag': [0, 0],
            'Q50': [100.0, 150.0]
        })

        result = adjust_forecast_dates_only(
            raw_forecast=forecast_data,
            target_start_month=4,
            target_end_month=9
        )

        assert len(result) == 2
        assert result['valid_from'].values[0] == pd.Timestamp('2024-04-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2024-09-30')
        assert result['valid_from'].values[1] == pd.Timestamp('2025-04-01')
        assert result['valid_to'].values[1] == pd.Timestamp('2025-09-30')


class TestCalendarMonthAdjustmentIntegration:
    """Integration tests for calendar_month_adjustment flag."""

    @pytest.fixture
    def setup_data(self):
        """Set up test data."""
        codes = ['BASIN_A']
        discharge_data = generate_discharge_data(codes, 2010, 2020)
        forecast_data = generate_forecast_data(
            codes, ['2020-03-25'],
            valid_from_offset=36,
            valid_to_offset=65
        )
        return discharge_data, forecast_data

    def test_calendar_month_adjustment_false_skips_ratio(self, setup_data):
        """Verify Q values unchanged when calendar_month_adjustment=False."""
        discharge_data, forecast_data = setup_data

        # Store original Q values
        original_q50 = forecast_data['Q50'].values[0]

        config = MockForecastConfig(
            operational_month_lead_time=2,
            calendar_month_adjustment=False,
            target_start_month=4,
            target_end_month=9
        )

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Q values should be unchanged
        assert result['Q50'].values[0] == original_q50

        # Dates should be adjusted
        assert result['valid_from'].values[0] == pd.Timestamp('2020-04-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2020-09-30')

    def test_calendar_month_adjustment_true_uses_ratio(self, setup_data):
        """Verify existing ratio adjustment behavior when flag is True (default)."""
        discharge_data, forecast_data = setup_data

        config = MockForecastConfig(
            operational_month_lead_time=2,
            calendar_month_adjustment=True  # Default behavior
        )

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Q values should be adjusted (not same as original in most cases)
        # The exact value depends on climatology ratios, so we just check
        # that the result is valid and non-negative
        assert 'Q50' in result.columns
        assert result['Q50'].values[0] >= 0

        # Valid_from should be adjusted to calendar month (single month mode)
        # Issue March + lead_time 2 = May
        valid_from = pd.to_datetime(result['valid_from'].values[0])
        assert valid_from.month == 5

    def test_default_calendar_month_adjustment_is_true(self, setup_data):
        """Verify default behavior uses ratio adjustment."""
        discharge_data, forecast_data = setup_data

        # Config without specifying calendar_month_adjustment
        config = MockForecastConfig(operational_month_lead_time=2)

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Should complete successfully with ratio adjustment
        assert len(result) == 1
        assert 'Q50' in result.columns
        # Valid_from should be calendar month adjusted (May for March+2)
        valid_from = pd.to_datetime(result['valid_from'].values[0])
        assert valid_from.month == 5


class TestAdjustForecastDatesDynamic:
    """Tests for dynamic multi-month mode."""

    def test_three_month_forecast(self):
        """Issue March 25, lead_time=1, horizon=3 -> Apr 1 to Jun 30."""
        raw_forecast = pd.DataFrame({
            'date': [pd.Timestamp('2024-03-25')],
            'code': ['A'],
            'valid_from': [pd.Timestamp('2024-04-01')],
            'valid_to': [pd.Timestamp('2024-04-30')],
            'flag': ['O'],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=1, horizon_length=3)

        assert result['valid_from'].iloc[0] == pd.Timestamp('2024-04-01')
        assert result['valid_to'].iloc[0] == pd.Timestamp('2024-06-30')
        assert result['Q50'].iloc[0] == 100.0  # Q unchanged

    def test_year_boundary_crossing(self):
        """Issue Nov 25, lead_time=1, horizon=3 -> Dec 1 to Feb 28."""
        raw_forecast = pd.DataFrame({
            'date': [pd.Timestamp('2024-11-25')],
            'code': ['A'],
            'valid_from': [pd.Timestamp('2024-12-01')],
            'valid_to': [pd.Timestamp('2024-12-31')],
            'flag': ['O'],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=1, horizon_length=3)

        assert result['valid_from'].iloc[0] == pd.Timestamp('2024-12-01')
        assert result['valid_to'].iloc[0] == pd.Timestamp('2025-02-28')

    def test_single_month_horizon(self):
        """Issue March 25, lead_time=2, horizon=1 -> May 1 to May 31."""
        raw_forecast = pd.DataFrame({
            'date': [pd.Timestamp('2024-03-25')],
            'code': ['A'],
            'valid_from': [pd.Timestamp('2024-05-01')],
            'valid_to': [pd.Timestamp('2024-05-30')],
            'flag': ['O'],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=2, horizon_length=1)

        assert result['valid_from'].iloc[0] == pd.Timestamp('2024-05-01')
        assert result['valid_to'].iloc[0] == pd.Timestamp('2024-05-31')

    def test_six_month_horizon(self):
        """Issue March 25, lead_time=1, horizon=6 -> Apr 1 to Sep 30."""
        raw_forecast = pd.DataFrame({
            'date': [pd.Timestamp('2024-03-25')],
            'code': ['A'],
            'valid_from': [pd.Timestamp('2024-04-01')],
            'valid_to': [pd.Timestamp('2024-04-30')],
            'flag': ['O'],
            'Q50': [100.0]
        })

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=1, horizon_length=6)

        assert result['valid_from'].iloc[0] == pd.Timestamp('2024-04-01')
        assert result['valid_to'].iloc[0] == pd.Timestamp('2024-09-30')

    def test_empty_forecast(self):
        """Test that empty forecast returns empty DataFrame."""
        raw_forecast = pd.DataFrame(columns=[
            'date', 'code', 'valid_from', 'valid_to', 'flag', 'Q50'
        ])

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=1, horizon_length=3)

        assert len(result) == 0

    def test_multiple_forecasts(self):
        """Test that multiple forecasts are handled correctly."""
        raw_forecast = pd.DataFrame({
            'date': [pd.Timestamp('2024-03-25'), pd.Timestamp('2024-04-25')],
            'code': ['A', 'B'],
            'valid_from': [pd.Timestamp('2024-04-01'), pd.Timestamp('2024-05-01')],
            'valid_to': [pd.Timestamp('2024-04-30'), pd.Timestamp('2024-05-31')],
            'flag': ['O', 'O'],
            'Q50': [100.0, 150.0]
        })

        result = adjust_forecast_dates_dynamic(raw_forecast, lead_time=1, horizon_length=3)

        assert len(result) == 2
        # March issue -> Apr 1 to Jun 30
        assert result['valid_from'].iloc[0] == pd.Timestamp('2024-04-01')
        assert result['valid_to'].iloc[0] == pd.Timestamp('2024-06-30')
        # April issue -> May 1 to Jul 31
        assert result['valid_from'].iloc[1] == pd.Timestamp('2024-05-01')
        assert result['valid_to'].iloc[1] == pd.Timestamp('2024-07-31')


class TestDynamicMultiMonthIntegration:
    """Integration tests for dynamic multi-month mode via post_process_lt_forecast."""

    @pytest.fixture
    def setup_data(self):
        """Set up test data."""
        codes = ['BASIN_A']
        discharge_data = generate_discharge_data(codes, 2010, 2020)
        forecast_data = generate_forecast_data(
            codes, ['2020-03-25'],
            valid_from_offset=36,
            valid_to_offset=65
        )
        return discharge_data, forecast_data

    def test_dynamic_mode_three_month_forecast(self, setup_data):
        """Test dynamic mode with 3-month horizon."""
        discharge_data, forecast_data = setup_data

        # Store original Q values
        original_q50 = forecast_data['Q50'].values[0]

        config = MockForecastConfig(
            operational_month_lead_time=1,
            calendar_month_adjustment=False,
            target_start_month=None,  # Trigger dynamic mode
            forecast_horizon_months=3
        )

        result = post_process_lt_forecast(config, discharge_data, forecast_data)

        # Q values should be unchanged
        assert result['Q50'].values[0] == original_q50

        # Dates should be dynamically calculated
        # Issue March + lead_time 1 = April start
        # horizon 3 = April, May, June
        assert result['valid_from'].values[0] == pd.Timestamp('2020-04-01')
        assert result['valid_to'].values[0] == pd.Timestamp('2020-06-30')

    def test_dynamic_mode_raises_error_without_horizon(self, setup_data):
        """Test that missing forecast_horizon_months raises ValueError."""
        discharge_data, forecast_data = setup_data

        config = MockForecastConfig(
            operational_month_lead_time=1,
            calendar_month_adjustment=False,
            target_start_month=None,  # Trigger dynamic mode
            forecast_horizon_months=None  # Missing!
        )

        with pytest.raises(ValueError, match="requires forecast_horizon_months"):
            post_process_lt_forecast(config, discharge_data, forecast_data)

    def test_dynamic_mode_raises_error_with_zero_horizon(self, setup_data):
        """Test that horizon_months < 1 raises ValueError."""
        discharge_data, forecast_data = setup_data

        config = MockForecastConfig(
            operational_month_lead_time=1,
            calendar_month_adjustment=False,
            target_start_month=None,
            forecast_horizon_months=0  # Invalid!
        )

        with pytest.raises(ValueError, match="requires forecast_horizon_months"):
            post_process_lt_forecast(config, discharge_data, forecast_data)


# Run tests with pytest
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
