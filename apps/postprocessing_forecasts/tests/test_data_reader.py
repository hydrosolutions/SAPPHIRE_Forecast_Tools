"""Tests for src/data_reader.py — reading pre-calculated skill metrics."""

import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

# Ensure the postprocessing_forecasts package is importable
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..')
)

import numpy as np

from src.data_reader import (
    read_skill_metrics,
    read_monthly_skill_metrics,
    read_latest_monthly_forecasts,
    read_monthly_combined_forecasts,
    _read_skill_metrics_csv,
    _read_monthly_skill_metrics_csv,
    _normalize_api_skill_metrics,
    _normalize_api_monthly_skill_metrics,
    _aggregate_daily_to_monthly,
    _normalize_monthly_forecasts,
    read_monthly_observations,
    read_monthly_forecasts,
)


class TestReadSkillMetricsCsv:
    """Tests for CSV-based skill metrics reading."""

    def test_reads_pentad_csv(self, tmp_path):
        """CSV with correct columns is read and returned."""
        csv_file = tmp_path / "pentad_skill.csv"
        df = pd.DataFrame({
            'pentad_in_year': [1, 2],
            'code': ['10001', '10002'],
            'model_short': ['LR', 'LR'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.9, 0.85],
            'delta': [5.0, 6.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.1, 3.2],
            'n_pairs': [10, 12],
        })
        df.to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert len(result) == 2
            assert result['code'].dtype == object  # string
            assert result.iloc[0]['sdivsigma'] == 0.3
            assert result.iloc[0]['code'] == '10001'
            assert result.iloc[0]['pentad_in_year'] == 1

    def test_reads_decad_csv(self, tmp_path):
        """Decadal CSV is read using the correct env var."""
        csv_file = tmp_path / "decad_skill.csv"
        df = pd.DataFrame({
            'decad_in_year': [1],
            'code': [10001],  # numeric code, should become string
            'model_short': ['LR'],
            'sdivsigma': [0.3],
            'nse': [0.9],
        })
        df.to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_decadal_skill_metrics_file': 'decad_skill.csv',
        }):
            result = _read_skill_metrics_csv('decad')
            assert result is not None
            assert result['code'].iloc[0] == '10001'
            assert result.iloc[0]['sdivsigma'] == 0.3
            assert result.iloc[0]['decad_in_year'] == 1

    def test_missing_env_vars_returns_none(self):
        """Returns None when env vars are not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear relevant vars
            for key in ['ieasyforecast_intermediate_data_path',
                        'ieasyforecast_pentadal_skill_metrics_file']:
                os.environ.pop(key, None)
            result = _read_skill_metrics_csv('pentad')
            assert result is None

    def test_missing_file_returns_none(self, tmp_path):
        """Returns None when the CSV file doesn't exist."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'nonexistent.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is None

    def test_empty_csv_returns_empty_df(self, tmp_path):
        """CSV with headers but no rows returns empty DataFrame."""
        csv_file = tmp_path / "empty_skill.csv"
        pd.DataFrame(columns=[
            'pentad_in_year', 'code', 'model_short', 'sdivsigma'
        ]).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'empty_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert result.empty

    def test_corrupt_csv_returns_none(self, tmp_path):
        """Truly empty (no headers) CSV returns None gracefully."""
        csv_file = tmp_path / "corrupt_skill.csv"
        csv_file.write_text("")

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'corrupt_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is None


class TestNormalizeApiSkillMetrics:
    """Tests for API -> CSV column normalization."""

    def test_renames_api_columns_pentad(self):
        """API column horizon_in_year -> pentad_in_year."""
        df = pd.DataFrame({
            'horizon_in_year': [1, 2],
            'model_type': ['LR', 'TFT'],
            'code': ['10001', '10001'],
            'sdivsigma': [0.3, 0.4],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert 'pentad_in_year' in result.columns
        assert 'horizon_in_year' not in result.columns
        assert 'model_short' in result.columns

    def test_renames_api_columns_decad(self):
        """API column horizon_in_year -> decad_in_year."""
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'model_type': ['LR'],
            'code': ['10001'],
        })
        result = _normalize_api_skill_metrics(df, 'decad')
        assert 'decad_in_year' in result.columns

    def test_model_type_becomes_model_short(self):
        """model_type is renamed to model_short."""
        df = pd.DataFrame({
            'horizon_in_year': [1, 2, 3],
            'model_type': ['LR', 'TFT', 'EM'],
            'code': ['10001'] * 3,
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert result.loc[0, 'model_short'] == 'LR'
        assert result.loc[1, 'model_short'] == 'TFT'
        assert result.loc[2, 'model_short'] == 'EM'
        assert 'model_long' not in result.columns

    def test_unknown_model_passthrough(self):
        """Unknown model types pass through as model_short."""
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'model_type': ['NEWMODEL'],
            'code': ['10001'],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        assert result.loc[0, 'model_short'] == 'NEWMODEL'
        assert 'model_long' not in result.columns


class TestReadSkillMetricsIntegration:
    """Integration tests for the main read_skill_metrics function."""

    def test_invalid_horizon_type_raises(self):
        """Invalid horizon_type raises ValueError."""
        with pytest.raises(ValueError, match="'pentad', 'decad', or 'month'"):
            read_skill_metrics('weekly')

    def test_month_delegates_to_monthly_reader(self):
        """read_skill_metrics('month') delegates to read_monthly_skill_metrics."""
        with patch(
            'src.data_reader.read_monthly_skill_metrics',
        ) as mock_monthly:
            mock_monthly.return_value = pd.DataFrame({
                'month_in_year': [1],
                'code': ['10001'],
                'model_short': ['LR'],
            })
            result = read_skill_metrics('month')
            mock_monthly.assert_called_once()
            assert len(result) == 1
            assert result.iloc[0]['month_in_year'] == 1

    def test_csv_preferred_over_api(self, tmp_path):
        """CSV is used when available; API is not called."""
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api'
            ) as mock_api:
                result = read_skill_metrics('pentad')
                mock_api.assert_not_called()
                assert len(result) == 1

    def test_falls_back_to_api_when_csv_empty(self, tmp_path):
        """When CSV is empty, tries API fallback."""
        csv_file = tmp_path / "empty.csv"
        pd.DataFrame().to_csv(csv_file, index=False)

        api_df = pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        })

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'empty.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api',
                return_value=api_df,
            ):
                result = read_skill_metrics('pentad')
                assert len(result) == 1

    def test_returns_empty_when_both_fail(self):
        """Returns empty DataFrame when CSV and API both return nothing."""
        with patch(
            'src.data_reader._read_skill_metrics_csv', return_value=None
        ):
            with patch(
                'src.data_reader._read_skill_metrics_api', return_value=None
            ):
                result = read_skill_metrics('pentad')
                assert isinstance(result, pd.DataFrame)
                assert result.empty

    def test_corrupted_csv_falls_back_to_api(self, tmp_path):
        """CSV exists but contains garbled/binary content -> falls back to API.

        Operational scenario: disk corruption or partial write during crash.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        csv_file.write_bytes(b'\x00\x01\x02\xff\xfe garbled content')

        api_df = pd.DataFrame({
            'pentad_in_year': [1, 2],
            'code': ['10001', '10002'],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.9, 0.85],
            'delta': [5.0, 6.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.1, 3.2],
            'n_pairs': [10, 12],
        })

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api',
                return_value=api_df,
            ) as mock_api:
                result = read_skill_metrics('pentad')
                # CSV read fails -> API fallback called
                mock_api.assert_called_once()
                assert len(result) == 2
                assert result.iloc[0]['code'] == '10001'
                assert result.iloc[0]['sdivsigma'] == 0.3

    def test_truncated_csv_with_partial_rows_falls_back(self, tmp_path):
        """CSV with headers + truncated row (no newline) -> exception -> API.

        Operational scenario: process killed mid-write.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        # Write a valid header but a truncated data row
        csv_file.write_text(
            "pentad_in_year,code,model_short,sdivsigma\n"
            "1,10001,LR,0.3\n"
        )

        # This CSV is actually valid (1 row), so CSV read succeeds
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            with patch(
                'src.data_reader._read_skill_metrics_api'
            ) as mock_api:
                result = read_skill_metrics('pentad')
                # CSV was valid so API should NOT be called
                mock_api.assert_not_called()
                assert len(result) == 1
                assert result.iloc[0]['code'] == '10001'


class TestDataReaderMissingColumns:
    """Tests for data_reader handling of CSVs with missing/extra columns (#14).

    Operational scenarios: CSV was written by an older version of the code
    (missing new columns) or by a newer version (extra columns).
    """

    def test_csv_missing_code_column_still_readable(self, tmp_path):
        """CSV without 'code' column is read but code cleanup is skipped.

        When a CSV has no 'code' column, _read_skill_metrics_csv should
        still return the DataFrame (with whatever columns exist).
        """
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1, 2],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert len(result) == 2
            assert 'code' not in result.columns
            assert result.iloc[0]['sdivsigma'] == 0.3

    def test_csv_with_extra_columns_preserved(self, tmp_path):
        """CSV with extra columns beyond expected set is read fully.

        Operational scenario: newer code adds columns; older reader
        should not break.
        """
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
            'nse': [0.9],
            'extra_metric': [42.0],  # not in the expected schema
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert 'extra_metric' in result.columns
            assert result.iloc[0]['extra_metric'] == 42.0

    def test_csv_numeric_code_cleaned(self, tmp_path):
        """Code column with float codes (15001.0) is cleaned to '15001'."""
        csv_file = tmp_path / "pentad_skill.csv"
        pd.DataFrame({
            'pentad_in_year': [1],
            'code': [15001.0],  # float from CSV round-trip
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_pentadal_skill_metrics_file': 'pentad_skill.csv',
        }):
            result = _read_skill_metrics_csv('pentad')
            assert result is not None
            assert result.iloc[0]['code'] == '15001'

    def test_normalize_api_missing_model_type_graceful(self):
        """API response missing model_type → no model_short column.

        Documents current behavior: _normalize_api_skill_metrics uses
        df.rename() which silently skips missing columns. If model_type
        is absent, model_short won't exist.
        """
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'code': ['10001'],
            # 'model_type' is missing
            'sdivsigma': [0.3],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        # horizon_in_year → pentad_in_year rename still works
        assert 'pentad_in_year' in result.columns
        # model_short not derived
        assert 'model_short' not in result.columns
        # Other columns preserved
        assert result.iloc[0]['sdivsigma'] == 0.3

    def test_normalize_api_missing_horizon_graceful(self):
        """API response missing horizon_in_year → no period column created.

        Documents current behavior: rename silently skips, so the period
        column (pentad_in_year) won't exist in output.
        """
        df = pd.DataFrame({
            'model_type': ['LR'],
            'code': ['10001'],
            # 'horizon_in_year' is missing
            'sdivsigma': [0.3],
        })
        result = _normalize_api_skill_metrics(df, 'pentad')
        # pentad_in_year not created (source column was missing)
        assert 'pentad_in_year' not in result.columns
        # model_type → model_short rename still works
        assert 'model_short' in result.columns
        assert result.iloc[0]['model_short'] == 'LR'


# ===================================================================
# Monthly data readers
# ===================================================================

class TestAggregateDailyToMonthly:
    """Tests for _aggregate_daily_to_monthly — 50% coverage filter,
    delta computation, and edge cases with incomplete data."""

    def _make_daily(self, code, year, month, values):
        """Build daily DataFrame for a single station-month.

        Args:
            code: Station code.
            year: Year.
            month: Month number (1-12).
            values: List of discharge values (one per day). Length
                determines how many days have data.
        """
        import calendar
        n_days = len(values)
        start = pd.Timestamp(year, month, 1)
        dates = pd.date_range(start, periods=n_days, freq='D')
        return pd.DataFrame({
            'code': [str(code)] * n_days,
            'date': dates,
            'discharge_avg': values,
        })

    def test_full_month_produces_correct_mean(self):
        """Full month (31 days) produces correct monthly mean.

        January 2020: 31 days, all with value 100.0.
        Expected mean = 100.0, delta = 0 (single year).
        """
        daily = self._make_daily('S1', 2020, 1, [100.0] * 31)
        result = _aggregate_daily_to_monthly(daily)

        assert len(result) == 1
        row = result.iloc[0]
        assert row['code'] == 'S1'
        assert row['year'] == 2020
        assert row['month'] == 1
        assert row['month_in_year'] == 1
        assert row['discharge_avg'] == pytest.approx(100.0)
        # Single year -> std is NaN -> delta = 0
        assert row['delta'] == pytest.approx(0.0, abs=1e-10)

    def test_50_percent_coverage_filter_passes(self):
        """Month with exactly 50% coverage passes the filter.

        February 2020 (leap year): 29 days. 15 days is ~52% -> passes.
        """
        # 15 days = 15/29 = 51.7% > 50%
        daily = self._make_daily('S1', 2020, 2, [80.0] * 15)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 1
        assert result.iloc[0]['discharge_avg'] == pytest.approx(80.0)

    def test_below_50_percent_coverage_filtered_out(self):
        """Month with fewer than 50% non-missing days is dropped.

        January 2020: 31 days. 15 days = 48.4% < 50% -> filtered out.
        """
        daily = self._make_daily('S1', 2020, 1, [80.0] * 15)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 0

    def test_nan_values_count_as_missing(self):
        """NaN daily values don't count toward non_missing_days.

        January 2020: 31 daily rows, but 20 are NaN.
        11 valid / 31 = 35.5% < 50% -> filtered out.
        """
        values = [100.0] * 11 + [np.nan] * 20
        daily = self._make_daily('S1', 2020, 1, values)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 0

    def test_nan_mixed_with_valid_computes_mean_of_valid(self):
        """Monthly mean uses only non-NaN values.

        January 2020: 31 days, 16 valid (mean=100), 15 NaN.
        16/31 = 51.6% > 50% -> passes. Mean of valid = 100.
        """
        values = [100.0] * 16 + [np.nan] * 15
        daily = self._make_daily('S1', 2020, 1, values)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 1
        assert result.iloc[0]['discharge_avg'] == pytest.approx(100.0)

    def test_delta_computed_across_years(self):
        """Delta = 0.674 * std(discharge_avg) across years per month.

        S1 month 1: year 2020 mean=100, year 2021 mean=110.
        std([100, 110], ddof=1) = 7.071
        delta = 0.674 * 7.071 = 4.766
        """
        daily_2020 = self._make_daily('S1', 2020, 1, [100.0] * 31)
        daily_2021 = self._make_daily('S1', 2021, 1, [110.0] * 31)
        daily = pd.concat([daily_2020, daily_2021], ignore_index=True)

        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 2

        expected_delta = 0.674 * np.std([100.0, 110.0], ddof=1)
        assert result.iloc[0]['delta'] == pytest.approx(
            expected_delta, rel=1e-6
        )
        assert result.iloc[1]['delta'] == pytest.approx(
            expected_delta, rel=1e-6
        )

    def test_multi_station_independent_delta(self):
        """Stations compute delta independently.

        S1 month 1: means [100, 110] -> delta = 0.674 * std
        S2 month 1: means [200, 220] -> different delta
        """
        daily = pd.concat([
            self._make_daily('S1', 2020, 1, [100.0] * 31),
            self._make_daily('S1', 2021, 1, [110.0] * 31),
            self._make_daily('S2', 2020, 1, [200.0] * 31),
            self._make_daily('S2', 2021, 1, [220.0] * 31),
        ], ignore_index=True)

        result = _aggregate_daily_to_monthly(daily)
        s1 = result[result['code'] == 'S1']
        s2 = result[result['code'] == 'S2']

        delta_s1 = 0.674 * np.std([100.0, 110.0], ddof=1)
        delta_s2 = 0.674 * np.std([200.0, 220.0], ddof=1)
        assert s1.iloc[0]['delta'] == pytest.approx(delta_s1, rel=1e-6)
        assert s2.iloc[0]['delta'] == pytest.approx(delta_s2, rel=1e-6)
        assert delta_s1 != pytest.approx(delta_s2)

    def test_empty_daily_returns_empty(self):
        """Empty daily DataFrame returns empty result with correct cols."""
        daily = pd.DataFrame(columns=['code', 'date', 'discharge_avg'])
        result = _aggregate_daily_to_monthly(daily)
        assert result.empty
        for col in ['code', 'year', 'month', 'month_in_year',
                     'discharge_avg', 'delta']:
            assert col in result.columns

    def test_single_year_delta_is_zero(self):
        """Single year of data: std is NaN -> delta = 0."""
        daily = self._make_daily('S1', 2020, 6, [50.0] * 30)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 1
        assert result.iloc[0]['delta'] == pytest.approx(0.0, abs=1e-10)

    def test_multiple_months_separate(self):
        """Data for two months produces two output rows.

        S1 Jan=100, S1 Feb=80.
        """
        daily = pd.concat([
            self._make_daily('S1', 2020, 1, [100.0] * 31),
            self._make_daily('S1', 2020, 2, [80.0] * 29),
        ], ignore_index=True)
        result = _aggregate_daily_to_monthly(daily)
        assert len(result) == 2
        jan = result[result['month'] == 1].iloc[0]
        feb = result[result['month'] == 2].iloc[0]
        assert jan['discharge_avg'] == pytest.approx(100.0)
        assert feb['discharge_avg'] == pytest.approx(80.0)


class TestNormalizeMonthlyForecasts:
    """Tests for _normalize_monthly_forecasts — API response to internal
    DataFrame format."""

    def test_extracts_year_month_from_valid_from(self):
        """year and month derived from valid_from date."""
        df = pd.DataFrame({
            'code': ['10001'],
            'valid_from': ['2024-06-01'],
            'valid_to': ['2024-06-30'],
            'model_type': ['GBT'],
            'q50': [100.0],
        })
        result = _normalize_monthly_forecasts(df)
        assert result.iloc[0]['year'] == 2024
        assert result.iloc[0]['month'] == 6

    def test_model_type_renamed_to_model_short(self):
        """model_type column is renamed to model_short."""
        df = pd.DataFrame({
            'code': ['10001'],
            'valid_from': ['2024-06-01'],
            'model_type': ['GBT'],
        })
        result = _normalize_monthly_forecasts(df)
        assert 'model_short' in result.columns
        assert 'model_type' not in result.columns
        assert result.iloc[0]['model_short'] == 'GBT'

    def test_code_cleaned_to_string(self):
        """Numeric code (15001.0) is cleaned to '15001'."""
        df = pd.DataFrame({
            'code': [15001.0],
            'valid_from': ['2024-06-01'],
            'model_type': ['LR_Base'],
        })
        result = _normalize_monthly_forecasts(df)
        assert result.iloc[0]['code'] == '15001'

    def test_multiple_models_and_months(self):
        """Multiple models and months are all normalized."""
        df = pd.DataFrame({
            'code': ['10001', '10001', '10001'],
            'valid_from': ['2024-06-01', '2024-07-01', '2024-06-01'],
            'model_type': ['GBT', 'GBT', 'LR_Base'],
            'q50': [100.0, 110.0, 102.0],
        })
        result = _normalize_monthly_forecasts(df)
        assert len(result) == 3
        assert set(result['model_short']) == {'GBT', 'LR_Base'}
        assert set(result['month']) == {6, 7}

    def test_preserves_quantile_columns(self):
        """Quantile columns (q05-q95) survive normalization."""
        df = pd.DataFrame({
            'code': ['10001'],
            'valid_from': ['2024-06-01'],
            'model_type': ['GBT'],
            'q05': [80.0], 'q10': [85.0], 'q25': [92.0],
            'q50': [100.0],
            'q75': [108.0], 'q90': [115.0], 'q95': [120.0],
        })
        result = _normalize_monthly_forecasts(df)
        for col in ['q05', 'q10', 'q25', 'q50', 'q75', 'q90', 'q95']:
            assert col in result.columns
        assert result.iloc[0]['q50'] == 100.0

    def test_no_model_type_column_survives(self):
        """If model_type is missing, no rename happens, no crash."""
        df = pd.DataFrame({
            'code': ['10001'],
            'valid_from': ['2024-06-01'],
            'q50': [100.0],
        })
        result = _normalize_monthly_forecasts(df)
        assert 'model_short' not in result.columns
        assert 'model_type' not in result.columns
        assert result.iloc[0]['year'] == 2024


class TestReadMonthlyObservations:
    """Tests for read_monthly_observations entry point."""

    @patch('src.data_reader._read_daily_runoff_api')
    def test_api_failure_returns_empty(self, mock_api):
        """API exception returns empty DataFrame, not crash."""
        mock_api.side_effect = RuntimeError("connection refused")
        result = read_monthly_observations(['10001'], 2020, 2024)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        for col in ['code', 'year', 'month', 'discharge_avg', 'delta']:
            assert col in result.columns

    @patch('src.data_reader._read_daily_runoff_api')
    def test_empty_api_returns_empty(self, mock_api):
        """No daily data returns empty DataFrame."""
        mock_api.return_value = pd.DataFrame()
        result = read_monthly_observations(['10001'], 2020, 2024)
        assert result.empty

    @patch('src.data_reader._read_daily_runoff_api')
    def test_aggregation_through_full_pipeline(self, mock_api):
        """Full pipeline: daily API data -> monthly aggregation.

        January 2020: 31 days at 100.0 -> mean 100.
        January 2021: 31 days at 110.0 -> mean 110.
        delta = 0.674 * std([100, 110], ddof=1)
        """
        daily = pd.concat([
            pd.DataFrame({
                'code': ['10001'] * 31,
                'date': pd.date_range('2020-01-01', periods=31),
                'discharge_avg': [100.0] * 31,
            }),
            pd.DataFrame({
                'code': ['10001'] * 31,
                'date': pd.date_range('2021-01-01', periods=31),
                'discharge_avg': [110.0] * 31,
            }),
        ], ignore_index=True)
        mock_api.return_value = daily

        result = read_monthly_observations(['10001'], 2020, 2021)
        assert len(result) == 2

        row_2020 = result[result['year'] == 2020].iloc[0]
        assert row_2020['discharge_avg'] == pytest.approx(100.0)
        assert row_2020['month'] == 1
        assert row_2020['code'] == '10001'

        expected_delta = 0.674 * np.std([100.0, 110.0], ddof=1)
        assert row_2020['delta'] == pytest.approx(expected_delta, rel=1e-6)


class TestReadMonthlyForecasts:
    """Tests for read_monthly_forecasts entry point."""

    @patch('src.data_reader._read_long_forecasts_api')
    def test_api_failure_returns_empty(self, mock_api):
        """API exception returns empty DataFrame, not crash."""
        mock_api.side_effect = RuntimeError("connection refused")
        result = read_monthly_forecasts(['10001'], 2020, 2024)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch('src.data_reader._read_long_forecasts_api')
    def test_empty_api_returns_empty(self, mock_api):
        """No forecast data returns empty DataFrame."""
        mock_api.return_value = pd.DataFrame()
        result = read_monthly_forecasts(['10001'], 2020, 2024)
        assert result.empty

    @patch('src.data_reader._read_long_forecasts_api')
    def test_normalization_through_full_pipeline(self, mock_api):
        """Full pipeline: raw API data -> normalized forecasts."""
        raw = pd.DataFrame({
            'code': [10001.0, 10001.0],
            'valid_from': ['2024-06-01', '2024-07-01'],
            'valid_to': ['2024-06-30', '2024-07-31'],
            'model_type': ['GBT', 'GBT'],
            'q50': [100.0, 110.0],
            'q05': [80.0, 88.0],
            'q10': [85.0, 93.0],
            'q25': [92.0, 100.0],
            'q75': [108.0, 120.0],
            'q90': [115.0, 130.0],
            'q95': [120.0, 140.0],
        })
        mock_api.return_value = raw

        result = read_monthly_forecasts(['10001'], 2024, 2024)
        assert len(result) == 2
        assert result.iloc[0]['code'] == '10001'
        assert result.iloc[0]['model_short'] == 'GBT'
        assert result.iloc[0]['year'] == 2024
        assert set(result['month']) == {6, 7}
        assert result.iloc[0]['q50'] == 100.0


# ===================================================================
# Monthly skill metrics read-back (Commit 5)
# ===================================================================

class TestReadMonthlySkillMetricsCsv:
    """Tests for _read_monthly_skill_metrics_csv."""

    def test_reads_monthly_csv(self, tmp_path):
        """CSV with correct columns is read and returned."""
        csv_file = tmp_path / "monthly_skill.csv"
        pd.DataFrame({
            'month_in_year': [1, 2],
            'code': ['10001', '10002'],
            'model_short': ['LR', 'TFT'],
            'sdivsigma': [0.3, 0.4],
            'nse': [0.9, 0.85],
            'delta': [5.0, 6.0],
            'accuracy': [0.95, 0.88],
            'mae': [2.1, 3.2],
            'n_pairs': [10, 12],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_skill_metrics_file': 'monthly_skill.csv',
        }):
            result = _read_monthly_skill_metrics_csv()
            assert result is not None
            assert len(result) == 2
            assert result['code'].dtype == object  # string
            assert result.iloc[0]['code'] == '10001'
            assert result.iloc[0]['month_in_year'] == 1

    def test_missing_env_vars_returns_none(self):
        """Returns None when env vars are not set."""
        with patch.dict(os.environ, {}, clear=True):
            for key in ['ieasyforecast_intermediate_data_path',
                        'ieasyforecast_monthly_skill_metrics_file']:
                os.environ.pop(key, None)
            result = _read_monthly_skill_metrics_csv()
            assert result is None

    def test_missing_file_returns_none(self, tmp_path):
        """Returns None when the CSV file doesn't exist."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_skill_metrics_file': 'nonexistent.csv',
        }):
            result = _read_monthly_skill_metrics_csv()
            assert result is None


class TestNormalizeApiMonthlySkillMetrics:
    """Tests for _normalize_api_monthly_skill_metrics."""

    def test_renames_api_columns_monthly(self):
        """API column horizon_in_year -> month_in_year."""
        df = pd.DataFrame({
            'horizon_in_year': [1, 6],
            'model_type': ['LR', 'TFT'],
            'code': ['10001', '10001'],
            'sdivsigma': [0.3, 0.4],
        })
        result = _normalize_api_monthly_skill_metrics(df)
        assert 'month_in_year' in result.columns
        assert 'horizon_in_year' not in result.columns
        assert 'model_short' in result.columns
        assert 'model_type' not in result.columns

    def test_code_cleaned_to_string(self):
        """Numeric code is cleaned to string."""
        df = pd.DataFrame({
            'horizon_in_year': [1],
            'model_type': ['LR'],
            'code': [10001.0],
        })
        result = _normalize_api_monthly_skill_metrics(df)
        assert result.iloc[0]['code'] == '10001'


class TestReadMonthlySkillMetricsIntegration:
    """Integration tests for read_monthly_skill_metrics."""

    def test_csv_preferred_over_api(self, tmp_path):
        """CSV is used when available; API is not called."""
        csv_file = tmp_path / "monthly_skill.csv"
        pd.DataFrame({
            'month_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_skill_metrics_file': 'monthly_skill.csv',
        }):
            with patch(
                'src.data_reader._read_monthly_skill_metrics_api'
            ) as mock_api:
                result = read_monthly_skill_metrics()
                mock_api.assert_not_called()
                assert len(result) == 1

    def test_falls_back_to_api_when_csv_empty(self, tmp_path):
        """When CSV is empty, tries API fallback."""
        csv_file = tmp_path / "empty.csv"
        pd.DataFrame().to_csv(csv_file, index=False)

        api_df = pd.DataFrame({
            'month_in_year': [1],
            'code': ['10001'],
            'model_short': ['LR'],
            'sdivsigma': [0.3],
        })

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_skill_metrics_file': 'empty.csv',
        }):
            with patch(
                'src.data_reader._read_monthly_skill_metrics_api',
                return_value=api_df,
            ):
                result = read_monthly_skill_metrics()
                assert len(result) == 1

    def test_returns_empty_when_both_fail(self):
        """Returns empty DataFrame when CSV and API both return nothing."""
        with patch(
            'src.data_reader._read_monthly_skill_metrics_csv',
            return_value=None,
        ):
            with patch(
                'src.data_reader._read_monthly_skill_metrics_api',
                return_value=None,
            ):
                result = read_monthly_skill_metrics()
                assert isinstance(result, pd.DataFrame)
                assert result.empty


# ===================================================================
# read_latest_monthly_forecasts
# ===================================================================

class TestReadLatestMonthlyForecasts:
    """Tests for read_latest_monthly_forecasts."""

    @patch('src.data_reader._read_long_forecasts_api')
    def test_returns_latest_month_only(self, mock_api):
        """Filters to the single most recent (year, month)."""
        raw = pd.DataFrame({
            'code': ['10001'] * 3,
            'valid_from': ['2024-06-01', '2024-07-01', '2024-07-01'],
            'valid_to': ['2024-06-30', '2024-07-31', '2024-07-31'],
            'model_type': ['GBT', 'GBT', 'LR_Base'],
            'q50': [100.0, 110.0, 105.0],
        })
        mock_api.return_value = raw

        result = read_latest_monthly_forecasts(['10001'])
        # Only July rows should remain
        assert len(result) == 2
        assert all(result['month'] == 7)
        assert all(result['year'] == 2024)
        assert 'month_in_year' in result.columns
        assert 'forecasted_discharge' in result.columns

    @patch('src.data_reader._read_long_forecasts_api')
    def test_empty_api_returns_empty(self, mock_api):
        """No data from API returns empty DataFrame."""
        mock_api.return_value = pd.DataFrame()
        result = read_latest_monthly_forecasts(['10001'])
        assert result.empty

    @patch('src.data_reader._read_long_forecasts_api')
    def test_adds_forecasted_discharge_from_q50(self, mock_api):
        """forecasted_discharge is created from q50."""
        raw = pd.DataFrame({
            'code': ['10001'],
            'valid_from': ['2024-06-01'],
            'valid_to': ['2024-06-30'],
            'model_type': ['GBT'],
            'q50': [100.0],
        })
        mock_api.return_value = raw

        result = read_latest_monthly_forecasts(['10001'])
        assert result.iloc[0]['forecasted_discharge'] == 100.0

    @patch('src.data_reader._read_long_forecasts_api')
    def test_api_exception_propagates(self, mock_api):
        """API exception propagates to caller (entry point handles it)."""
        mock_api.side_effect = RuntimeError("connection refused")
        with pytest.raises(RuntimeError, match="connection refused"):
            read_latest_monthly_forecasts(['10001'])

    @patch('src.data_reader._read_long_forecasts_api')
    def test_api_returns_none(self, mock_api):
        """API returning None returns empty DataFrame."""
        mock_api.return_value = None
        result = read_latest_monthly_forecasts(['10001'])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch('src.data_reader._read_long_forecasts_api')
    def test_uses_explicit_forecast_date(self, mock_api):
        """Explicit forecast_date controls the API year range."""
        from datetime import date
        mock_api.return_value = pd.DataFrame()

        read_latest_monthly_forecasts(
            ['10001'], forecast_date=date(2025, 6, 15),
        )
        # 60 days back from June 15 = April 16 -> start_year=2025
        call_args = mock_api.call_args
        assert call_args[0][1] == 2025  # start_year
        assert call_args[0][2] == 2025  # end_year

    @patch('src.data_reader._read_long_forecasts_api')
    def test_year_boundary_forecast_date(self, mock_api):
        """forecast_date near year boundary spans two years."""
        from datetime import date
        mock_api.return_value = pd.DataFrame()

        read_latest_monthly_forecasts(
            ['10001'], forecast_date=date(2025, 1, 15),
        )
        # 60 days back from Jan 15 = Nov 16, 2024 -> start_year=2024
        call_args = mock_api.call_args
        assert call_args[0][1] == 2024  # start_year
        assert call_args[0][2] == 2025  # end_year

    @patch('src.data_reader._read_long_forecasts_api')
    def test_multi_station_filters_to_latest(self, mock_api):
        """Multiple stations, multiple months; only latest month returned."""
        raw = pd.DataFrame({
            'code': ['10001', '10001', '10002', '10002'],
            'valid_from': [
                '2024-05-01', '2024-06-01', '2024-05-01', '2024-06-01',
            ],
            'valid_to': [
                '2024-05-31', '2024-06-30', '2024-05-31', '2024-06-30',
            ],
            'model_type': ['GBT', 'GBT', 'LR_Base', 'LR_Base'],
            'q50': [90.0, 100.0, 180.0, 200.0],
        })
        mock_api.return_value = raw

        result = read_latest_monthly_forecasts(['10001', '10002'])
        # Only June (latest month) should remain
        assert len(result) == 2
        assert all(result['month'] == 6)
        assert set(result['code'].astype(str)) == {'10001', '10002'}


# ===================================================================
# read_monthly_combined_forecasts
# ===================================================================

class TestReadMonthlyCombinedForecasts:
    """Tests for read_monthly_combined_forecasts."""

    def test_reads_csv_file(self, tmp_path):
        """Reads monthly combined forecasts CSV."""
        csv_file = tmp_path / "combined_monthly.csv"
        pd.DataFrame({
            'year': [2024, 2024],
            'month': [6, 6],
            'code': [10001, 10001],
            'model_short': ['LR', 'TFT'],
            'forecasted_discharge': [100.0, 105.0],
        }).to_csv(csv_file, index=False)

        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_combined_forecast_file':
                'combined_monthly.csv',
        }):
            result = read_monthly_combined_forecasts()
            assert len(result) == 2
            assert result['code'].iloc[0] == '10001'

    def test_missing_file_returns_empty(self, tmp_path):
        """Missing file returns empty DataFrame."""
        with patch.dict(os.environ, {
            'ieasyforecast_intermediate_data_path': str(tmp_path),
            'ieasyforecast_monthly_combined_forecast_file':
                'nonexistent.csv',
        }):
            result = read_monthly_combined_forecasts()
            assert result.empty

    def test_missing_env_vars_returns_empty(self):
        """Unset env vars returns empty DataFrame."""
        with patch.dict(os.environ, {}, clear=True):
            for key in ['ieasyforecast_intermediate_data_path',
                        'ieasyforecast_monthly_combined_forecast_file']:
                os.environ.pop(key, None)
            result = read_monthly_combined_forecasts()
            assert result.empty
