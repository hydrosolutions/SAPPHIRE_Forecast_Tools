"""
Integration tests for PREPQ-005: Maintenance Mode Data Gaps

These tests verify that the preprocessing_runoff module correctly handles
data gaps in maintenance mode. The bug manifests as large data gaps
(e.g., March-November) appearing in output files when:
1. Historical data (from Excel/cache) has gaps
2. API data only covers a limited lookback window

Test strategy:
1. Create test history CSV with 3 years of data but with a large gap
2. Mock API to return only recent data (within lookback_days)
3. Run the preprocessing workflow
4. Verify output file has/doesn't have gaps depending on the scenario
"""

import os
import sys
import json
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import src


class TestMaintenanceModeDataGaps:
    """Integration tests for maintenance mode gap handling (PREPQ-005)."""

    @pytest.fixture
    def test_env(self, tmp_path):
        """Set up a test environment with temporary directories and files."""
        # Create directory structure
        intermediate_dir = tmp_path / "intermediate_data"
        intermediate_dir.mkdir()

        daily_discharge_dir = tmp_path / "daily_discharge"
        daily_discharge_dir.mkdir()

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Store original env vars
        original_env = {
            'ieasyforecast_intermediate_data_path': os.environ.get('ieasyforecast_intermediate_data_path'),
            'ieasyforecast_daily_discharge_path': os.environ.get('ieasyforecast_daily_discharge_path'),
            'ieasyforecast_daily_discharge_file': os.environ.get('ieasyforecast_daily_discharge_file'),
            'ieasyforecast_hydrograph_day_file': os.environ.get('ieasyforecast_hydrograph_day_file'),
            'ieasyforecast_configuration_path': os.environ.get('ieasyforecast_configuration_path'),
            'ieasyforecast_virtual_stations': os.environ.get('ieasyforecast_virtual_stations'),
            'ieasyhydroforecast_organization': os.environ.get('ieasyhydroforecast_organization'),
        }

        # Set test env vars
        os.environ['ieasyforecast_intermediate_data_path'] = str(intermediate_dir)
        os.environ['ieasyforecast_daily_discharge_path'] = str(daily_discharge_dir)
        os.environ['ieasyforecast_daily_discharge_file'] = 'runoff_day.csv'
        os.environ['ieasyforecast_hydrograph_day_file'] = 'hydrograph_day.csv'
        os.environ['ieasyforecast_configuration_path'] = str(config_dir)
        os.environ['ieasyhydroforecast_organization'] = 'test'
        # Ensure virtual stations is not set to avoid that code path
        if 'ieasyforecast_virtual_stations' in os.environ:
            del os.environ['ieasyforecast_virtual_stations']

        yield {
            'tmp_path': tmp_path,
            'intermediate_dir': intermediate_dir,
            'daily_discharge_dir': daily_discharge_dir,
            'config_dir': config_dir,
        }

        # Restore original env vars
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _create_history_csv_with_gap(self, output_path, site_codes,
                                      start_date, end_date,
                                      gap_start, gap_end):
        """
        Create a history CSV file with a data gap.

        Args:
            output_path: Path to write the CSV
            site_codes: List of site codes to include
            start_date: Start date of data (date object)
            end_date: End date of data (date object)
            gap_start: Start date of the gap (date object)
            gap_end: End date of the gap (date object)

        Returns:
            DataFrame of the created data
        """
        rows = []
        current = start_date

        while current <= end_date:
            # Skip dates in the gap
            if gap_start <= current <= gap_end:
                current += timedelta(days=1)
                continue

            for code in site_codes:
                # Generate realistic discharge values (seasonal pattern)
                day_of_year = current.timetuple().tm_yday
                base_discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                noise = np.random.normal(0, 5)
                discharge = max(10, base_discharge + noise)

                rows.append({
                    'code': str(code),
                    'date': current.strftime('%Y-%m-%d'),
                    'discharge': round(discharge, 2),
                    'name': f'Station {code}'
                })

            current += timedelta(days=1)

        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        return df

    def _create_mock_api_data(self, site_codes, start_date, end_date):
        """
        Create mock API response data.

        Args:
            site_codes: List of site codes
            start_date: Start date (date object)
            end_date: End date (date object)

        Returns:
            DataFrame with mock API data
        """
        rows = []
        current = start_date

        while current <= end_date:
            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                base_discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                noise = np.random.normal(0, 5)
                discharge = max(10, base_discharge + noise)

                rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': round(discharge, 2),
                })

            current += timedelta(days=1)

        return pd.DataFrame(rows)

    def test_merge_alone_preserves_gaps_outside_api_range(self, test_env):
        """
        Verify that _merge_with_update alone does NOT fill historical gaps.

        This documents the expected behavior of _merge_with_update:
        - It only adds/updates rows that exist in new_data
        - Gaps outside the new_data date range are preserved

        The PREPQ-005 FIX uses additional functions (_detect_gaps_in_data,
        _fill_gaps_from_api) to handle historical gaps. This test verifies
        _merge_with_update's baseline behavior has not changed.
        """
        today = date.today()

        # Create simulated cached data with gap (similar to what _load_cached_data returns)
        # Gap: March 10 to November 25 of previous year
        gap_year = today.year - 1
        gap_start = date(gap_year, 3, 10)
        gap_end = date(gap_year, 11, 25)

        site_codes = ['16059', '15189']
        history_start = today - timedelta(days=3*365)
        history_end = today - timedelta(days=1)

        # Build cached data with the gap
        cached_rows = []
        current = history_start
        while current <= history_end:
            if gap_start <= current <= gap_end:
                current += timedelta(days=1)
                continue
            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                cached_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': round(discharge, 2),
                })
            current += timedelta(days=1)

        cached_data = pd.DataFrame(cached_rows)

        # Count gap days
        gap_days = (gap_end - gap_start).days + 1

        # Verify cached data has gap
        site_16059_cached = cached_data[cached_data['code'] == '16059']
        gap_in_cached = site_16059_cached[
            (site_16059_cached['date'] >= pd.Timestamp(gap_start)) &
            (site_16059_cached['date'] <= pd.Timestamp(gap_end))
        ]
        assert len(gap_in_cached) == 0, "Cached data should have the gap"

        # Create API data (only recent, within lookback window)
        lookback_days = 50
        api_start = today - timedelta(days=lookback_days)
        api_end = today

        api_rows = []
        current = api_start
        while current <= api_end:
            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                api_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': round(discharge, 2),
                })
            current += timedelta(days=1)

        api_data = pd.DataFrame(api_rows)

        # Call the merge function alone (without gap detection/filling)
        result = src._merge_with_update(
            cached_data, api_data, 'code', 'date', 'discharge'
        )

        # Check if the gap still exists
        site_16059_result = result[result['code'] == '16059']
        gap_in_result = site_16059_result[
            (site_16059_result['date'] >= pd.Timestamp(gap_start)) &
            (site_16059_result['date'] <= pd.Timestamp(gap_end))
        ]

        # _merge_with_update alone should preserve gaps outside the API range
        # This is expected behavior - the fix uses additional functions
        assert len(gap_in_result) == 0, (
            f"Expected gap to persist: _merge_with_update only adds rows from new_data. "
            f"Gap from {gap_start} to {gap_end} ({gap_days} days) should remain empty."
        )

    def test_hydrograph_reflects_data_gaps(self, test_env):
        """
        Test that hydrograph output reflects data gaps from runoff data.

        When runoff_day.csv has gaps, hydrograph_day.csv should also show
        those gaps in the statistics (or have NaN/missing values).
        """
        today = date.today()

        # Create simple test data with a gap
        site_codes = ['16059']

        # Data for current year only, with a 2-month gap in the middle
        rows = []
        current_year = today.year

        for month in [1, 2, 11, 12]:  # Skip months 3-10 (gap)
            for day in range(1, 29):  # Use 28 days to avoid Feb 29 issues
                try:
                    d = date(current_year, month, day)
                except ValueError:
                    continue

                for code in site_codes:
                    day_of_year = d.timetuple().tm_yday
                    discharge = 100 + 20 * np.sin(2 * np.pi * day_of_year / 365)

                    rows.append({
                        'code': str(code),
                        'date': d,
                        'discharge': round(discharge, 2),
                        'name': f'Station {code}'
                    })

        runoff_df = pd.DataFrame(rows)
        runoff_df['date'] = pd.to_datetime(runoff_df['date'])

        # Generate hydrograph from this data
        hydrograph = src.from_daily_time_series_to_hydrograph(
            data_df=runoff_df.copy(),
            date_col='date',
            discharge_col='discharge',
            code_col='code'
        )

        # Check that the hydrograph has data for January/February and November/December
        # but lower counts or NaN for March-October (the gap period)

        site_hydrograph = hydrograph[hydrograph['code'] == '16059']

        # Days in January should have count > 0
        jan_days = site_hydrograph[site_hydrograph['date'].dt.month == 1]
        assert len(jan_days) > 0, "Should have January data"

        # Days in gap period (e.g., June) should have count = 0 or be missing
        june_days = site_hydrograph[site_hydrograph['date'].dt.month == 6]

        # Either no June data, or count should be 0
        if len(june_days) > 0:
            # If June days exist, they should have count 0 or very low
            assert june_days['count'].sum() == 0 or june_days['count'].max() <= 1, (
                f"Gap period (June) should have no historical data, but found count={june_days['count'].max()}"
            )


class TestCachedDataLoading:
    """Tests for the _load_cached_data function behavior with gaps."""

    @pytest.fixture
    def test_env(self, tmp_path):
        """Set up test environment."""
        intermediate_dir = tmp_path / "intermediate"
        intermediate_dir.mkdir()

        original_env = {
            'ieasyforecast_intermediate_data_path': os.environ.get('ieasyforecast_intermediate_data_path'),
            'ieasyforecast_daily_discharge_file': os.environ.get('ieasyforecast_daily_discharge_file'),
        }

        os.environ['ieasyforecast_intermediate_data_path'] = str(intermediate_dir)
        os.environ['ieasyforecast_daily_discharge_file'] = 'runoff_day.csv'

        yield {
            'intermediate_dir': intermediate_dir,
        }

        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_cached_data_preserves_gaps(self, test_env):
        """Test that loading cached data preserves existing gaps."""
        # Create CSV with gaps
        today = date.today()
        rows = []

        # Data range: 100 days ago to today
        # Gap: days 40-70 from start (i.e., 60-30 days ago from today)
        # Gap dates: today - 60 to today - 30
        gap_start_days_ago = 60
        gap_end_days_ago = 30

        for i in range(100):
            d = today - timedelta(days=100-i)
            days_ago = (today - d).days

            # Create gap from 60 to 30 days ago
            if gap_end_days_ago <= days_ago <= gap_start_days_ago:
                continue

            rows.append({
                'code': '16059',
                'date': d.strftime('%Y-%m-%d'),
                'discharge': 100 + i,
            })

        df = pd.DataFrame(rows)
        csv_path = test_env['intermediate_dir'] / 'runoff_day.csv'
        df.to_csv(csv_path, index=False)

        # Load the cached data
        result = src._load_cached_data(
            date_col='date',
            discharge_col='discharge',
            name_col='name',
            code_col='code',
            code_list=['16059']
        )

        # Verify the gap is preserved
        assert len(result) == len(rows), f"Expected {len(rows)} rows, got {len(result)}"

        # Check gap period (middle of gap is 45 days ago)
        result['date'] = pd.to_datetime(result['date'])
        gap_middle = pd.Timestamp(today - timedelta(days=45))
        gap_data = result[
            (result['date'] >= gap_middle - pd.Timedelta(days=10)) &
            (result['date'] <= gap_middle + pd.Timedelta(days=10))
        ]

        # Gap should have no data
        assert len(gap_data) == 0, f"Gap should be preserved, but found {len(gap_data)} rows"


class TestMergeWithUpdateGapBehavior:
    """Tests for _merge_with_update function and how it handles gaps."""

    def test_merge_does_not_fill_gaps_outside_new_data_range(self):
        """
        Test that merge only adds rows that exist in new_data.

        If existing data has a gap and new_data doesn't cover that period,
        the gap will remain. This is the root cause of PREPQ-005.
        """
        # Existing data with a large gap (simulating cached/Excel data)
        existing = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-06-01', '2024-06-02']),
            'discharge': [10.0, 20.0, 60.0, 70.0]  # Gap from Jan 3 to May 31
        })

        # New data only covers recent period (simulating API with lookback)
        # Does NOT cover the gap period
        new_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-05-30', '2024-05-31']),
            'discharge': [55.0, 58.0]
        })

        result = src._merge_with_update(existing, new_data, 'code', 'date', 'discharge')

        # Result should have 6 rows (4 original + 2 new)
        # The gap from Jan 3 to May 29 still exists
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

        # Check that the gap period still has no data
        gap_data = result[
            (result['date'] > pd.Timestamp('2024-01-02')) &
            (result['date'] < pd.Timestamp('2024-05-30'))
        ]

        # This test documents the current behavior that leads to PREPQ-005
        assert len(gap_data) == 0, (
            "PREPQ-005 root cause: _merge_with_update doesn't fill gaps "
            "that are outside the new_data range"
        )


class TestGapDetection:
    """Tests for the _detect_gaps_in_data function."""

    def test_detect_single_day_gap(self):
        """Test that single missing days are detected as gaps."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=10)

        # Data with a single day gap (day 2 is missing)
        data = pd.DataFrame({
            'code': ['A', 'A', 'A'],
            'date': pd.to_datetime([
                base_date,
                base_date + timedelta(days=2),
                base_date + timedelta(days=3)
            ]),
            'discharge': [10.0, 30.0, 40.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        # Should detect 1 gap: day 1
        assert len(gaps) == 1, f"Expected 1 gap, found {len(gaps)}"
        site, start, end = gaps[0]
        assert site == 'A'
        assert start == pd.Timestamp(base_date + timedelta(days=1))
        assert end == pd.Timestamp(base_date + timedelta(days=1))

    def test_detect_multi_day_gap(self):
        """Test that multiple consecutive missing days form a single gap."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=15)

        # Data with a 5-day gap (days 3-7 missing)
        data = pd.DataFrame({
            'code': ['A', 'A', 'A'],
            'date': pd.to_datetime([
                base_date,
                base_date + timedelta(days=1),
                base_date + timedelta(days=7)
            ]),
            'discharge': [10.0, 20.0, 80.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        # Should detect 1 gap: days 2-6
        assert len(gaps) == 1, f"Expected 1 gap, found {len(gaps)}"
        site, start, end = gaps[0]
        assert site == 'A'
        assert start == pd.Timestamp(base_date + timedelta(days=2))
        assert end == pd.Timestamp(base_date + timedelta(days=6))

    def test_detect_multiple_gaps(self):
        """Test that multiple non-consecutive gaps are detected separately."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=15)

        # Data with multiple gaps
        data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                base_date,
                base_date + timedelta(days=2),
                base_date + timedelta(days=4),
                base_date + timedelta(days=5),
                base_date + timedelta(days=9)
            ]),
            'discharge': [10.0, 30.0, 50.0, 60.0, 100.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        # Should detect 3 gaps: day 1, day 3, days 6-8
        assert len(gaps) == 3, f"Expected 3 gaps, found {len(gaps)}: {gaps}"

    def test_detect_gaps_multiple_sites(self):
        """Test gap detection works independently for each site."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=10)

        # Site A has gap on day 1, Site B has gap on day 2
        data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'B', 'B', 'B'],
            'date': pd.to_datetime([
                base_date, base_date + timedelta(days=2), base_date + timedelta(days=3),  # Site A: gap on day 1
                base_date, base_date + timedelta(days=1), base_date + timedelta(days=3)   # Site B: gap on day 2
            ]),
            'discharge': [10.0, 30.0, 40.0, 10.0, 20.0, 40.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        # Should detect 2 gaps: one for A (day 1), one for B (day 2)
        assert len(gaps) == 2, f"Expected 2 gaps, found {len(gaps)}"

        sites_with_gaps = {g[0] for g in gaps}
        assert sites_with_gaps == {'A', 'B'}

    def test_no_gaps_in_continuous_data(self):
        """Test that continuous data has no gaps."""
        data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A'],
            'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']),
            'discharge': [10.0, 20.0, 30.0, 40.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        assert len(gaps) == 0, f"Expected no gaps, found {len(gaps)}"

    def test_empty_data_returns_no_gaps(self):
        """Test that empty data returns no gaps."""
        data = pd.DataFrame(columns=['code', 'date', 'discharge'])

        gaps = src._detect_gaps_in_data(data, 'code', 'date')

        assert len(gaps) == 0

    def test_max_gap_age_respected(self):
        """Test that gaps older than max_gap_age_days are not detected."""
        today = pd.Timestamp.now().normalize()

        # Create data with a gap 100 days ago
        old_date = today - pd.Timedelta(days=100)
        recent_date = today - pd.Timedelta(days=10)

        data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': [old_date, recent_date],
            'discharge': [10.0, 20.0]
        })

        # With max_gap_age_days=50, the gap should be partially ignored
        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=50)

        # The gap should only include dates within the last 50 days
        for site, start, end in gaps:
            cutoff = today - pd.Timedelta(days=50)
            assert start >= cutoff, f"Gap start {start} is older than cutoff {cutoff}"


class TestGapGrouping:
    """Tests for the _group_gaps_for_api function."""

    def test_group_overlapping_gaps(self):
        """Test that overlapping gaps from different sites are grouped."""
        gaps = [
            ('A', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-05')),
            ('B', pd.Timestamp('2024-01-03'), pd.Timestamp('2024-01-07')),
        ]

        batches = src._group_gaps_for_api(gaps)

        # Should be grouped into 1 batch covering Jan 1-7 for both sites
        assert len(batches) == 1, f"Expected 1 batch, got {len(batches)}"
        start, end, sites = batches[0]
        assert start == pd.Timestamp('2024-01-01')
        assert end == pd.Timestamp('2024-01-07')
        assert set(sites) == {'A', 'B'}

    def test_separate_non_overlapping_gaps(self):
        """Test that non-overlapping gaps are in separate batches."""
        gaps = [
            ('A', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-05')),
            ('B', pd.Timestamp('2024-02-01'), pd.Timestamp('2024-02-05')),
        ]

        batches = src._group_gaps_for_api(gaps)

        # Should be 2 separate batches
        assert len(batches) == 2, f"Expected 2 batches, got {len(batches)}"

    def test_empty_gaps_returns_empty(self):
        """Test that empty gaps list returns empty batches."""
        batches = src._group_gaps_for_api([])
        assert len(batches) == 0


class TestGapFillingIntegration:
    """Integration tests for the gap filling workflow."""

    def test_gap_filling_with_mock_api(self):
        """Test the complete gap filling workflow with mocked API."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=10)

        # Create cached data with a gap (days 2-3 missing)
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                base_date,
                base_date + timedelta(days=1),
                base_date + timedelta(days=4),
                base_date + timedelta(days=5)
            ]),
            'discharge': [10.0, 20.0, 50.0, 60.0]
        })

        # Detect gaps
        gaps = src._detect_gaps_in_data(cached_data, 'code', 'date', max_gap_age_days=730)
        assert len(gaps) == 1, f"Expected 1 gap, found {len(gaps)}"

        # The gap should be days 2-3
        site, start, end = gaps[0]
        assert site == 'A'
        assert start == pd.Timestamp(base_date + timedelta(days=2))
        assert end == pd.Timestamp(base_date + timedelta(days=3))

        # Create mock API data that fills the gap
        api_gap_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime([
                base_date + timedelta(days=2),
                base_date + timedelta(days=3)
            ]),
            'discharge': [30.0, 40.0]
        })

        # Merge the gap data
        result = src._merge_with_update(cached_data, api_gap_data, 'code', 'date', 'discharge')

        # Result should now have 6 rows (continuous data)
        assert len(result) == 6, f"Expected 6 rows, got {len(result)}"

        # Verify no gaps remain
        result_sorted = result.sort_values('date')
        result_gaps = src._detect_gaps_in_data(result_sorted, 'code', 'date', max_gap_age_days=730)
        assert len(result_gaps) == 0, f"Expected no gaps after fill, found {len(result_gaps)}"

    def test_gap_filling_partial_api_response(self):
        """Test that partial API responses are handled correctly."""
        # Use recent dates to avoid max_gap_age_days filter
        today = date.today()
        base_date = today - timedelta(days=10)

        # Cached data with a 4-day gap (days 2-5 missing)
        cached_data = pd.DataFrame({
            'code': ['A', 'A', 'A'],
            'date': pd.to_datetime([
                base_date,
                base_date + timedelta(days=1),
                base_date + timedelta(days=6)
            ]),
            'discharge': [10.0, 20.0, 70.0]
        })

        # API only returns data for some of the gap days (legitimate gap for days 4-5)
        api_gap_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime([
                base_date + timedelta(days=2),
                base_date + timedelta(days=3)
            ]),
            'discharge': [30.0, 40.0]
        })

        # Merge
        result = src._merge_with_update(cached_data, api_gap_data, 'code', 'date', 'discharge')

        # Result should have 5 rows (gap partially filled)
        assert len(result) == 5, f"Expected 5 rows, got {len(result)}"

        # Days 2-3 should have data, days 4-5 still missing
        filled_days = result[
            (result['date'] >= pd.Timestamp(base_date + timedelta(days=2))) &
            (result['date'] <= pd.Timestamp(base_date + timedelta(days=3)))
        ]
        assert len(filled_days) == 2, "Days 2-3 should have data after partial fill"

        missing_days = result[
            (result['date'] >= pd.Timestamp(base_date + timedelta(days=4))) &
            (result['date'] <= pd.Timestamp(base_date + timedelta(days=5)))
        ]
        assert len(missing_days) == 0, "Days 4-5 should still be missing (legitimate gap)"


class TestEndToEndMaintenanceModeGapFilling:
    """
    End-to-end integration tests for the complete maintenance mode gap filling workflow.

    These tests mock the SDK and verify that the entire flow from gap detection
    through API request to final merge works correctly.
    """

    @pytest.fixture
    def test_env(self, tmp_path):
        """Set up test environment with required directories and env vars."""
        intermediate_dir = tmp_path / "intermediate_data"
        intermediate_dir.mkdir()

        daily_discharge_dir = tmp_path / "daily_discharge"
        daily_discharge_dir.mkdir()

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        original_env = {
            'ieasyforecast_intermediate_data_path': os.environ.get('ieasyforecast_intermediate_data_path'),
            'ieasyforecast_daily_discharge_path': os.environ.get('ieasyforecast_daily_discharge_path'),
            'ieasyforecast_daily_discharge_file': os.environ.get('ieasyforecast_daily_discharge_file'),
        }

        os.environ['ieasyforecast_intermediate_data_path'] = str(intermediate_dir)
        os.environ['ieasyforecast_daily_discharge_path'] = str(daily_discharge_dir)
        os.environ['ieasyforecast_daily_discharge_file'] = 'runoff_day.csv'

        yield {
            'tmp_path': tmp_path,
            'intermediate_dir': intermediate_dir,
            'daily_discharge_dir': daily_discharge_dir,
            'config_dir': config_dir,
        }

        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _create_mock_sdk_response(self, site_codes, start_date, end_date):
        """
        Create a mock SDK response in the format returned by get_data_values_for_site.

        The SDK returns a dict with 'count' and 'results' keys.
        Each result has 'site_code', 'local_date_time', 'value', 'variable_name'.
        """
        results = []
        current = start_date

        while current <= end_date:
            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                base_discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                noise = np.random.normal(0, 3)
                discharge = max(10, base_discharge + noise)

                # Format like the real API response
                results.append({
                    'site_code': str(code),
                    'local_date_time': current.strftime('%Y-%m-%dT20:00:00+00:00'),
                    'value': round(discharge, 2),
                    'variable_name': 'WDDA',
                    'site': {'site_code': str(code), 'site_name': f'Station {code}'}
                })
            current += timedelta(days=1)

        return {'count': len(results), 'results': results}

    def test_fill_gaps_from_api_calls_sdk_correctly(self, test_env):
        """
        Test that _fill_gaps_from_api correctly calls the SDK for gap periods.

        This test mocks get_daily_average_discharge_from_iEH_HF_for_multiple_sites
        and verifies it's called with the correct parameters.
        """
        import pytz

        today = date.today()
        base_date = today - timedelta(days=20)

        # Create gaps to fill
        gaps = [
            ('16059', pd.Timestamp(base_date + timedelta(days=5)), pd.Timestamp(base_date + timedelta(days=8))),
            ('15189', pd.Timestamp(base_date + timedelta(days=5)), pd.Timestamp(base_date + timedelta(days=8))),
        ]

        # Create mock API response data
        mock_api_response = pd.DataFrame({
            'code': ['16059', '16059', '16059', '16059', '15189', '15189', '15189', '15189'],
            'date': pd.to_datetime([
                base_date + timedelta(days=5),
                base_date + timedelta(days=6),
                base_date + timedelta(days=7),
                base_date + timedelta(days=8),
                base_date + timedelta(days=5),
                base_date + timedelta(days=6),
                base_date + timedelta(days=7),
                base_date + timedelta(days=8),
            ]),
            'discharge': [50.0, 55.0, 60.0, 65.0, 45.0, 48.0, 52.0, 55.0]
        })

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=mock_api_response) as mock_api_func:
            result = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

            # Verify API was called
            assert mock_api_func.called, "API function should have been called"

            # Verify result contains the expected data
            assert len(result) == 8, f"Expected 8 rows, got {len(result)}"

            # Verify both sites have data
            sites_in_result = set(result['code'].unique())
            assert sites_in_result == {'16059', '15189'}, f"Expected both sites, got {sites_in_result}"

    def test_fill_gaps_from_api_handles_empty_response(self, test_env):
        """
        Test that _fill_gaps_from_api handles empty API responses gracefully.

        When the API returns no data (legitimate gap), the function should
        return an empty DataFrame without error.
        """
        import pytz

        today = date.today()
        base_date = today - timedelta(days=20)

        gaps = [
            ('16059', pd.Timestamp(base_date + timedelta(days=5)), pd.Timestamp(base_date + timedelta(days=8))),
        ]

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Return empty DataFrame (no data available for gap period)
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=pd.DataFrame()) as mock_api_func:
            result = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

            # Should return empty DataFrame, not error
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0, "Should return empty DataFrame when API has no data"

    def test_fill_gaps_from_api_handles_api_error(self, test_env):
        """
        Test that _fill_gaps_from_api handles API errors gracefully.

        When the API call fails, the function should log a warning and
        return an empty DataFrame, not crash.
        """
        import pytz

        today = date.today()
        base_date = today - timedelta(days=20)

        gaps = [
            ('16059', pd.Timestamp(base_date + timedelta(days=5)), pd.Timestamp(base_date + timedelta(days=8))),
        ]

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Simulate API error
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          side_effect=Exception("API connection timeout")) as mock_api_func:
            result = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

            # Should return empty DataFrame, not raise exception
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0, "Should return empty DataFrame when API errors"

    def test_complete_maintenance_mode_gap_filling_flow(self, test_env):
        """
        End-to-end test of the complete maintenance mode gap filling workflow.

        This test verifies that:
        1. Cached data with gaps is loaded
        2. Gaps are detected
        3. API is called for gap periods
        4. Gap data is merged correctly
        5. Final result has no gaps (when API has data)

        This is the key test that proves PREPQ-005 is fixed.
        """
        import pytz

        today = date.today()

        # Create cached data with a significant gap (simulating the PREPQ-005 scenario)
        # Data from 60 days ago to today, with a 20-day gap in the middle
        data_start = today - timedelta(days=60)
        gap_start = today - timedelta(days=40)
        gap_end = today - timedelta(days=21)
        data_end = today

        site_codes = ['16059', '15189']

        # Build cached data with the gap
        cached_rows = []
        current = data_start
        while current <= data_end:
            # Skip the gap period
            if gap_start <= current <= gap_end:
                current += timedelta(days=1)
                continue

            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                cached_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': round(discharge, 2),
                })
            current += timedelta(days=1)

        cached_data = pd.DataFrame(cached_rows)

        # Verify cached data has gap
        gap_days_expected = (gap_end - gap_start).days + 1
        site_16059 = cached_data[cached_data['code'] == '16059']
        gap_in_cached = site_16059[
            (site_16059['date'] >= pd.Timestamp(gap_start)) &
            (site_16059['date'] <= pd.Timestamp(gap_end))
        ]
        assert len(gap_in_cached) == 0, "Cached data should have the gap"

        # Step 1: Detect gaps (as maintenance mode would)
        detected_gaps = src._detect_gaps_in_data(cached_data, 'code', 'date', max_gap_age_days=730)

        # Should detect gaps for both sites
        assert len(detected_gaps) > 0, "Should detect gaps in cached data"

        # Verify the gap period is detected
        gap_sites = {g[0] for g in detected_gaps}
        assert '16059' in gap_sites, "Gap for site 16059 should be detected"
        assert '15189' in gap_sites, "Gap for site 15189 should be detected"

        # Step 2: Create mock API response for the gap period
        gap_filling_rows = []
        current = gap_start
        while current <= gap_end:
            for code in site_codes:
                day_of_year = current.timetuple().tm_yday
                discharge = 100 + 50 * np.sin(2 * np.pi * day_of_year / 365)
                gap_filling_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': round(discharge, 2),
                })
            current += timedelta(days=1)

        mock_gap_data = pd.DataFrame(gap_filling_rows)

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Step 3: Fill gaps using mocked API
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=mock_gap_data):
            gap_data = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=detected_gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

        # Verify gap data was returned
        assert not gap_data.empty, "Gap data should be returned from API"

        # Step 4: Merge gap data into cached data (as maintenance mode would)
        result = src._merge_with_update(cached_data, gap_data, 'code', 'date', 'discharge')

        # Step 5: Verify no gaps remain
        result_gaps = src._detect_gaps_in_data(result, 'code', 'date', max_gap_age_days=730)

        assert len(result_gaps) == 0, (
            f"PREPQ-005 FIX VERIFICATION: After gap filling, no gaps should remain. "
            f"Found {len(result_gaps)} gaps: {result_gaps}"
        )

        # Verify total row count
        expected_days = (data_end - data_start).days + 1
        expected_rows = expected_days * len(site_codes)
        assert len(result) == expected_rows, (
            f"Expected {expected_rows} rows (continuous data), got {len(result)}"
        )

        # Verify the previously missing dates now have data
        site_16059_result = result[result['code'] == '16059']
        gap_in_result = site_16059_result[
            (site_16059_result['date'] >= pd.Timestamp(gap_start)) &
            (site_16059_result['date'] <= pd.Timestamp(gap_end))
        ]

        assert len(gap_in_result) == gap_days_expected, (
            f"Gap period should now have {gap_days_expected} days of data, got {len(gap_in_result)}"
        )

    def test_maintenance_mode_gap_filling_with_partial_api_data(self, test_env):
        """
        Test that partial API responses result in partial gap filling.

        If the API only has data for some of the gap days (legitimate missing data),
        those days should remain as gaps while available days are filled.
        """
        import pytz

        today = date.today()

        # Create cached data with a 10-day gap
        data_start = today - timedelta(days=30)
        gap_start = today - timedelta(days=20)
        gap_end = today - timedelta(days=11)  # 10-day gap
        data_end = today

        site_codes = ['16059']

        # Build cached data with the gap
        cached_rows = []
        current = data_start
        while current <= data_end:
            if gap_start <= current <= gap_end:
                current += timedelta(days=1)
                continue

            for code in site_codes:
                cached_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': 100.0,
                })
            current += timedelta(days=1)

        cached_data = pd.DataFrame(cached_rows)

        # Detect gaps
        detected_gaps = src._detect_gaps_in_data(cached_data, 'code', 'date', max_gap_age_days=730)
        assert len(detected_gaps) > 0

        # Create API response with only PARTIAL gap data (first 5 days of the 10-day gap)
        partial_gap_rows = []
        partial_end = gap_start + timedelta(days=4)  # Only first 5 days
        current = gap_start
        while current <= partial_end:
            for code in site_codes:
                partial_gap_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': 150.0,  # Different value to verify it's from API
                })
            current += timedelta(days=1)

        mock_gap_data = pd.DataFrame(partial_gap_rows)

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Fill gaps with partial data
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=mock_gap_data):
            gap_data = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=detected_gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

        # Merge
        result = src._merge_with_update(cached_data, gap_data, 'code', 'date', 'discharge')

        # Check remaining gaps - should be 5 days (second half of original gap)
        remaining_gaps = src._detect_gaps_in_data(result, 'code', 'date', max_gap_age_days=730)

        # There should be 1 remaining gap (days 6-10 of the original gap)
        assert len(remaining_gaps) == 1, f"Expected 1 remaining gap, found {len(remaining_gaps)}"

        site, remaining_start, remaining_end = remaining_gaps[0]
        remaining_gap_days = (remaining_end - remaining_start).days + 1
        assert remaining_gap_days == 5, (
            f"Expected 5-day remaining gap (legitimate missing data), got {remaining_gap_days}"
        )

        # Verify the filled portion has the API data (discharge=150)
        filled_portion = result[
            (result['date'] >= pd.Timestamp(gap_start)) &
            (result['date'] <= pd.Timestamp(partial_end))
        ]
        assert len(filled_portion) == 5, f"Expected 5 filled days, got {len(filled_portion)}"
        assert all(filled_portion['discharge'] == 150.0), "Filled data should have API values"

    def test_maintenance_mode_no_gaps_no_api_calls(self, test_env):
        """
        Test that when there are no gaps, no unnecessary API calls are made.

        Performance optimization: if data is continuous, gap filling should
        not make any API calls.
        """
        import pytz

        today = date.today()

        # Create continuous data with no gaps
        data_start = today - timedelta(days=30)
        data_end = today

        site_codes = ['16059']

        continuous_rows = []
        current = data_start
        while current <= data_end:
            for code in site_codes:
                continuous_rows.append({
                    'code': str(code),
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': 100.0,
                })
            current += timedelta(days=1)

        continuous_data = pd.DataFrame(continuous_rows)

        # Detect gaps - should be none
        detected_gaps = src._detect_gaps_in_data(continuous_data, 'code', 'date', max_gap_age_days=730)

        assert len(detected_gaps) == 0, "Continuous data should have no gaps"

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Call gap filling with no gaps
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites') as mock_api_func:
            result = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=detected_gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

            # API should NOT have been called
            assert not mock_api_func.called, "API should not be called when there are no gaps"

            # Result should be empty
            assert len(result) == 0, "Should return empty DataFrame when no gaps"

    def test_maintenance_mode_multiple_sites_independent_gaps(self, test_env):
        """
        Test that gaps are handled independently for different sites.

        Site A might have a gap on days 5-10, while Site B has a gap on days 15-20.
        Both should be detected and filled independently.
        """
        import pytz

        today = date.today()
        base_date = today - timedelta(days=30)

        # Site A: gap on days 5-10
        # Site B: gap on days 15-20

        cached_rows = []
        for day_offset in range(31):
            current = base_date + timedelta(days=day_offset)

            # Site A data (skip days 5-10)
            if not (5 <= day_offset <= 10):
                cached_rows.append({
                    'code': 'A',
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': 100.0,
                })

            # Site B data (skip days 15-20)
            if not (15 <= day_offset <= 20):
                cached_rows.append({
                    'code': 'B',
                    'date': pd.Timestamp(current).normalize(),
                    'discharge': 200.0,
                })

        cached_data = pd.DataFrame(cached_rows)

        # Detect gaps
        detected_gaps = src._detect_gaps_in_data(cached_data, 'code', 'date', max_gap_age_days=730)

        # Should detect 2 gaps (one for each site)
        assert len(detected_gaps) == 2, f"Expected 2 gaps (one per site), found {len(detected_gaps)}"

        gap_sites = {g[0] for g in detected_gaps}
        assert gap_sites == {'A', 'B'}, f"Expected gaps for sites A and B, got {gap_sites}"

        # Create mock API responses for each batch call
        # The API is called once per batch, and _group_gaps_for_api creates separate batches
        # for non-overlapping gaps
        def mock_api_response(*args, **kwargs):
            """Return data based on which sites are requested."""
            id_list = kwargs.get('id_list', [])

            rows = []
            if 'A' in id_list:
                # Data for Site A's gap (days 5-10)
                for day_offset in range(5, 11):
                    rows.append({
                        'code': 'A',
                        'date': pd.Timestamp(base_date + timedelta(days=day_offset)).normalize(),
                        'discharge': 150.0,
                    })
            if 'B' in id_list:
                # Data for Site B's gap (days 15-20)
                for day_offset in range(15, 21):
                    rows.append({
                        'code': 'B',
                        'date': pd.Timestamp(base_date + timedelta(days=day_offset)).normalize(),
                        'discharge': 250.0,
                    })

            return pd.DataFrame(rows) if rows else pd.DataFrame()

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Fill gaps with dynamic mock that returns appropriate data per call
        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          side_effect=mock_api_response):
            gap_data = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=detected_gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

        # Verify gap data was retrieved
        assert not gap_data.empty, "Should have gap data"

        # Remove any duplicates that might occur from the mock
        gap_data = gap_data.drop_duplicates(subset=['code', 'date'], keep='last')

        # Merge
        result = src._merge_with_update(cached_data, gap_data, 'code', 'date', 'discharge')

        # Verify no gaps remain
        remaining_gaps = src._detect_gaps_in_data(result, 'code', 'date', max_gap_age_days=730)
        assert len(remaining_gaps) == 0, f"Expected no remaining gaps, found {len(remaining_gaps)}"

        # Verify each site has 31 days of data
        site_a = result[result['code'] == 'A']
        site_b = result[result['code'] == 'B']

        assert len(site_a) == 31, f"Site A should have 31 days, got {len(site_a)}"
        assert len(site_b) == 31, f"Site B should have 31 days, got {len(site_b)}"

        # Verify the filled gaps have the correct values
        site_a_filled = site_a[
            (site_a['date'] >= pd.Timestamp(base_date + timedelta(days=5))) &
            (site_a['date'] <= pd.Timestamp(base_date + timedelta(days=10)))
        ]
        assert all(site_a_filled['discharge'] == 150.0), "Site A gap should have API value 150"

        site_b_filled = site_b[
            (site_b['date'] >= pd.Timestamp(base_date + timedelta(days=15))) &
            (site_b['date'] <= pd.Timestamp(base_date + timedelta(days=20)))
        ]
        assert all(site_b_filled['discharge'] == 250.0), "Site B gap should have API value 250"


class TestEdgeCases:
    """
    Edge case tests identified during code review.

    These tests cover boundary conditions, special cases, and scenarios
    that could cause unexpected behavior.
    """

    def test_single_row_data_no_gaps_detected(self):
        """
        Test that sites with only 1 data point don't cause errors.

        The implementation at line 2377 skips sites with < 2 data points
        because you need at least 2 points to define a gap.
        """
        today = date.today()

        # Data with only 1 row for site A
        data = pd.DataFrame({
            'code': ['A'],
            'date': pd.to_datetime([today - timedelta(days=5)]),
            'discharge': [10.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=730)

        # Should return empty - can't detect gaps with single data point
        assert len(gaps) == 0, "Single-row data should not produce gaps"

    def test_adjacent_gaps_merged_correctly(self):
        """
        Test that adjacent gaps (1 day apart) are merged into single batches.

        The implementation at line 2466 treats gaps within 1 day as adjacent
        and merges them for API efficiency.
        """
        # Site A gap: days 1-3, Site B gap: days 4-6 (adjacent, 1 day apart)
        gaps = [
            ('A', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-03')),
            ('B', pd.Timestamp('2024-01-04'), pd.Timestamp('2024-01-06')),
        ]

        batches = src._group_gaps_for_api(gaps)

        # Should be merged into 1 batch since they're adjacent
        assert len(batches) == 1, f"Adjacent gaps should be merged, got {len(batches)} batches"
        start, end, sites = batches[0]
        assert start == pd.Timestamp('2024-01-01')
        assert end == pd.Timestamp('2024-01-06')
        assert set(sites) == {'A', 'B'}

    def test_sdk_none_returns_empty_dataframe(self):
        """
        Test that _fill_gaps_from_api handles None SDK gracefully.

        The implementation at line 2514 returns empty DataFrame when SDK is None.
        """
        import pytz

        gaps = [
            ('A', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-05')),
        ]

        result = src._fill_gaps_from_api(
            ieh_hf_sdk=None,  # SDK is None
            gaps=gaps,
            target_timezone=pytz.timezone('Asia/Bishkek'),
            date_col='date',
            discharge_col='discharge',
            code_col='code'
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0, "Should return empty DataFrame when SDK is None"

    def test_gap_boundary_at_cutoff_date(self):
        """
        Test gap detection at the exact max_gap_age_days boundary.

        When a gap spans the cutoff date, only the portion within the
        cutoff should be detected.
        """
        today = pd.Timestamp.now().normalize()

        # Data points: 60 days ago and 5 days ago (55-day gap in between)
        old_date = today - pd.Timedelta(days=60)
        recent_date = today - pd.Timedelta(days=5)

        data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': [old_date, recent_date],
            'discharge': [10.0, 20.0]
        })

        # With max_gap_age_days=30, only gaps within last 30 days should be detected
        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=30)

        # Should have a gap, but it should start at the cutoff (30 days ago), not 59 days ago
        if gaps:
            site, gap_start, gap_end = gaps[0]
            cutoff = today - pd.Timedelta(days=30)
            assert gap_start >= cutoff, (
                f"Gap start {gap_start} should be >= cutoff {cutoff}"
            )
            # Gap should end at recent_date - 1 day
            expected_gap_end = recent_date - pd.Timedelta(days=1)
            assert gap_end == expected_gap_end, (
                f"Gap end {gap_end} should be {expected_gap_end}"
            )

    def test_api_call_parameters_verified(self):
        """
        Test that _fill_gaps_from_api calls the API with correct parameters.

        This test verifies the actual call arguments, not just that the API was called.
        """
        import pytz

        today = date.today()
        gap_start = pd.Timestamp(today - timedelta(days=10))
        gap_end = pd.Timestamp(today - timedelta(days=5))

        gaps = [
            ('16059', gap_start, gap_end),
        ]

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=pd.DataFrame()) as mock_api_func:
            src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code'
            )

            # Verify API was called
            assert mock_api_func.called, "API should have been called"

            # Verify call arguments
            call_kwargs = mock_api_func.call_args.kwargs
            assert call_kwargs['ieh_hf_sdk'] == mock_sdk, "SDK should be passed correctly"
            assert '16059' in call_kwargs['id_list'], "Site code should be in id_list"
            assert call_kwargs['target_timezone'] == target_timezone, "Timezone should match"

            # Verify date range
            start_dt = call_kwargs['start_datetime']
            end_dt = call_kwargs['end_datetime']
            assert start_dt.date() == gap_start.date(), "Start date should match gap start"
            assert end_dt.date() == gap_end.date(), "End date should match gap end"

    def test_manual_site_filtering(self):
        """
        Test that _fill_gaps_from_api filters gaps to only include manual sites.

        When manual_site_codes is provided, gaps for sites not in that list
        should be filtered out before making API calls.
        """
        import pytz

        # Gaps for sites A, B, C
        gaps = [
            ('A', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-03')),
            ('B', pd.Timestamp('2024-01-02'), pd.Timestamp('2024-01-04')),
            ('C', pd.Timestamp('2024-01-03'), pd.Timestamp('2024-01-05')),
        ]

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Only A and B are manual sites
        manual_site_codes = ['A', 'B']

        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=pd.DataFrame()) as mock_api_func:
            src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code',
                manual_site_codes=manual_site_codes
            )

            # Verify API was called
            assert mock_api_func.called, "API should have been called"

            # Verify that only manual sites (A, B) are in id_list, not C
            call_kwargs = mock_api_func.call_args.kwargs
            id_list = call_kwargs['id_list']
            assert 'A' in id_list, "Manual site A should be in id_list"
            assert 'B' in id_list, "Manual site B should be in id_list"
            assert 'C' not in id_list, "Non-manual site C should NOT be in id_list"

    def test_manual_site_filtering_all_filtered_out(self):
        """
        Test that _fill_gaps_from_api returns empty when all gaps are non-manual.

        When manual_site_codes is provided and none of the gap sites are in
        that list, no API call should be made.
        """
        import pytz

        # Gaps for sites X, Y, Z (none are manual)
        gaps = [
            ('X', pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-03')),
            ('Y', pd.Timestamp('2024-01-02'), pd.Timestamp('2024-01-04')),
        ]

        mock_sdk = MagicMock()
        target_timezone = pytz.timezone('Asia/Bishkek')

        # Only A and B are manual sites (none of the gap sites)
        manual_site_codes = ['A', 'B']

        with patch.object(src, 'get_daily_average_discharge_from_iEH_HF_for_multiple_sites',
                          return_value=pd.DataFrame()) as mock_api_func:
            result = src._fill_gaps_from_api(
                ieh_hf_sdk=mock_sdk,
                gaps=gaps,
                target_timezone=target_timezone,
                date_col='date',
                discharge_col='discharge',
                code_col='code',
                manual_site_codes=manual_site_codes
            )

            # Should return empty DataFrame and NOT call API
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0, "Should return empty when no manual sites have gaps"
            assert not mock_api_func.called, "API should NOT be called when all gaps are filtered out"


class TestDeterministicDates:
    """
    Tests using fixed dates for reproducibility.

    These tests use hardcoded dates to ensure consistent behavior
    regardless of when the tests are run.
    """

    def test_gap_detection_with_fixed_dates(self):
        """Test gap detection with fixed, reproducible dates."""
        # Fixed dates that will always be valid
        data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                '2024-06-01',
                '2024-06-02',
                # Gap: 2024-06-03, 2024-06-04
                '2024-06-05',
                '2024-06-06'
            ]),
            'discharge': [10.0, 20.0, 50.0, 60.0]
        })

        # Use large max_gap_age_days to ensure old dates are included
        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=3650)

        assert len(gaps) == 1, f"Expected 1 gap, found {len(gaps)}"
        site, start, end = gaps[0]
        assert site == 'A'
        assert start == pd.Timestamp('2024-06-03')
        assert end == pd.Timestamp('2024-06-04')

    def test_gap_grouping_with_fixed_dates(self):
        """Test gap grouping with fixed, reproducible dates."""
        gaps = [
            ('A', pd.Timestamp('2024-06-01'), pd.Timestamp('2024-06-03')),
            ('B', pd.Timestamp('2024-06-02'), pd.Timestamp('2024-06-05')),
            ('C', pd.Timestamp('2024-07-01'), pd.Timestamp('2024-07-05')),  # Separate
        ]

        batches = src._group_gaps_for_api(gaps)

        # First two overlap, third is separate
        assert len(batches) == 2, f"Expected 2 batches, got {len(batches)}"

        # Check first batch (merged A and B)
        start1, end1, sites1 = batches[0]
        assert start1 == pd.Timestamp('2024-06-01')
        assert end1 == pd.Timestamp('2024-06-05')
        assert set(sites1) == {'A', 'B'}

        # Check second batch (C alone)
        start2, end2, sites2 = batches[1]
        assert start2 == pd.Timestamp('2024-07-01')
        assert end2 == pd.Timestamp('2024-07-05')
        assert sites2 == ['C']

    def test_merge_with_fixed_dates(self):
        """Test merge behavior with fixed, reproducible dates."""
        existing = pd.DataFrame({
            'code': ['A', 'A', 'A'],
            'date': pd.to_datetime(['2024-06-01', '2024-06-02', '2024-06-05']),
            'discharge': [10.0, 20.0, 50.0]
        })

        new_data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-06-03', '2024-06-04']),
            'discharge': [30.0, 40.0]
        })

        result = src._merge_with_update(existing, new_data, 'code', 'date', 'discharge')

        # Should have 5 rows (3 existing + 2 new)
        assert len(result) == 5, f"Expected 5 rows, got {len(result)}"

        # Verify all dates present
        result_dates = set(result['date'].dt.strftime('%Y-%m-%d'))
        expected_dates = {'2024-06-01', '2024-06-02', '2024-06-03', '2024-06-04', '2024-06-05'}
        assert result_dates == expected_dates, f"Missing dates: {expected_dates - result_dates}"

        # Verify new data has correct values
        new_rows = result[result['date'].isin(pd.to_datetime(['2024-06-03', '2024-06-04']))]
        assert set(new_rows['discharge']) == {30.0, 40.0}

    def test_same_site_multiple_gaps(self):
        """
        Test that same site with multiple non-adjacent gaps is handled correctly.

        Site A has gaps on days 2-3 AND days 6-7 (two separate gaps).
        """
        data = pd.DataFrame({
            'code': ['A', 'A', 'A', 'A', 'A'],
            'date': pd.to_datetime([
                '2024-06-01',  # Present
                # Gap: 2024-06-02, 2024-06-03
                '2024-06-04',  # Present
                '2024-06-05',  # Present
                # Gap: 2024-06-06, 2024-06-07
                '2024-06-08',  # Present
                '2024-06-09',  # Present
            ]),
            'discharge': [10.0, 40.0, 50.0, 80.0, 90.0]
        })

        gaps = src._detect_gaps_in_data(data, 'code', 'date', max_gap_age_days=3650)

        # Should detect 2 gaps for site A
        assert len(gaps) == 2, f"Expected 2 gaps, found {len(gaps)}"

        # Verify gap boundaries
        gap_ranges = [(g[1].strftime('%Y-%m-%d'), g[2].strftime('%Y-%m-%d')) for g in gaps]
        assert ('2024-06-02', '2024-06-03') in gap_ranges
        assert ('2024-06-06', '2024-06-07') in gap_ranges


class TestCSVRoundTrip:
    """
    Tests that verify gap detection works correctly after CSV write/read cycles.

    This is critical because production data is often loaded from cached CSV files,
    and type mismatches between pd.Timestamp and string dates could cause
    gap detection to silently fail.
    """

    def test_gap_detection_after_csv_round_trip(self, tmp_path):
        """
        Test that gaps are still detected after writing to CSV and reading back.

        This simulates the production scenario where:
        1. Data is written to CSV (dates converted to strings)
        2. Data is read back from CSV
        3. Gap detection is run on the loaded data
        """
        # Create test data with a gap
        original_data = pd.DataFrame({
            'code': ['16059', '16059', '16059', '16059'],
            'date': pd.to_datetime([
                '2024-06-01',
                '2024-06-02',
                # Gap: 2024-06-03, 2024-06-04
                '2024-06-05',
                '2024-06-06'
            ]),
            'discharge': [10.0, 20.0, 50.0, 60.0]
        })

        # Write to CSV (simulating production write)
        csv_path = tmp_path / "test_data.csv"
        original_data.to_csv(csv_path, index=False, date_format='%Y-%m-%d')

        # Read from CSV (simulating production read)
        loaded_data = pd.read_csv(csv_path)
        loaded_data['date'] = pd.to_datetime(loaded_data['date']).dt.normalize()
        loaded_data['code'] = loaded_data['code'].astype(str)

        # Detect gaps
        gaps = src._detect_gaps_in_data(loaded_data, 'code', 'date', max_gap_age_days=3650)

        # Should still detect the gap
        assert len(gaps) == 1, f"Expected 1 gap after CSV round-trip, found {len(gaps)}"
        site, start, end = gaps[0]
        assert site == '16059'
        assert start == pd.Timestamp('2024-06-03')
        assert end == pd.Timestamp('2024-06-04')

    def test_gap_detection_with_mixed_date_types(self):
        """
        Test that gap detection handles potential type mismatches gracefully.

        This tests the internal type conversion in _detect_gaps_in_data.
        """
        # Create data with string dates (as would come from CSV)
        data_with_string_dates = pd.DataFrame({
            'code': ['A', 'A', 'A'],
            'date': ['2024-06-01', '2024-06-02', '2024-06-05'],  # String dates
            'discharge': [10.0, 20.0, 50.0]
        })

        # Gap detection should convert strings to timestamps internally
        gaps = src._detect_gaps_in_data(data_with_string_dates, 'code', 'date', max_gap_age_days=3650)

        assert len(gaps) == 1, f"Should detect gap even with string dates, found {len(gaps)}"
        site, start, end = gaps[0]
        assert start == pd.Timestamp('2024-06-03')
        assert end == pd.Timestamp('2024-06-04')

    def test_type_consistency_in_gap_detection(self):
        """
        Verify that date types are consistent throughout gap detection.

        Explicitly check that:
        1. pd.date_range returns Timestamps
        2. existing_dates set contains Timestamps
        3. Comparison works correctly
        """
        data = pd.DataFrame({
            'code': ['A', 'A'],
            'date': pd.to_datetime(['2024-06-01', '2024-06-05']),
            'discharge': [10.0, 50.0]
        })

        # Normalize dates as the function does
        data['date'] = pd.to_datetime(data['date']).dt.normalize()

        # Create date range as the function does
        all_dates = pd.date_range(start='2024-06-01', end='2024-06-05', freq='D')

        # Create existing_dates set as the function does
        existing_dates = set(data['date'])

        # Verify types
        sample_all = all_dates[0]
        sample_existing = next(iter(existing_dates))

        assert type(sample_all) == type(sample_existing), (
            f"Type mismatch: all_dates has {type(sample_all)}, "
            f"existing_dates has {type(sample_existing)}"
        )

        # Verify comparison works
        assert sample_all in existing_dates, (
            f"Date {sample_all} should be in existing_dates but comparison failed. "
            f"This indicates a type mismatch issue."
        )

        # Verify we can find missing dates
        missing = [d for d in all_dates if d not in existing_dates]
        assert len(missing) == 3, f"Should find 3 missing dates, found {len(missing)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
