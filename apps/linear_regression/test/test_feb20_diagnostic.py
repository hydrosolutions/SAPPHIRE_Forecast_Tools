"""Diagnostic test to trace why Feb 20, 2026 data goes missing in the LR pipeline.

This test creates mock daily discharge data through Feb 22, 2026, runs it
through the same pipeline functions used by linear_regression.py, and checks
whether the Feb 20 row survives each stage.

Also tests the forecast_date parameter for stale-data detection.
"""

import os
import sys
import datetime as dt
import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Path setup
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
os.environ.setdefault("SAPPHIRE_TEST_ENV", "True")

import forecast_library as fl
import tag_library as tl


def _make_daily_discharge(
    station_codes, start_date, end_date, base_value=50.0
):
    """Create mock daily discharge data for multiple stations."""
    dates = pd.date_range(start_date, end_date, freq='D')
    rows = []
    for code in station_codes:
        for d in dates:
            rows.append({
                'code': str(code),
                'date': d,
                'discharge': base_value + np.random.uniform(-5, 5),
            })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df


class TestFeb20DataFlow:
    """Trace whether Feb 20 data survives each pipeline stage."""

    @pytest.fixture
    def daily_data(self):
        """Daily discharge for 2 stations, 2022-01-01 through 2026-02-22."""
        return _make_daily_discharge(
            station_codes=['S1', 'S2'],
            start_date='2022-01-01',
            end_date='2026-02-22',
            base_value=50.0,
        )

    def test_stage1_daily_data_has_feb20(self, daily_data):
        """Stage 1: Raw daily data includes Feb 20, 2026."""
        feb20 = daily_data[daily_data['date'] == pd.Timestamp('2026-02-20')]
        assert len(feb20) == 2, (
            f"Expected 2 rows for Feb 20 (one per station), got {len(feb20)}"
        )

    def test_stage2_add_pentad_issue_date(self, daily_data):
        """Stage 2: add_pentad_issue_date preserves Feb 20 with issue_date=True."""
        for code in ['S1', 'S2']:
            station_data = daily_data[daily_data['code'] == code].copy()
            result = fl.add_pentad_issue_date(station_data, 'date')

            feb20 = result[result['date'] == pd.Timestamp('2026-02-20')]
            assert len(feb20) == 1, f"Feb 20 row missing for {code}"
            assert feb20['issue_date'].iloc[0] is True or feb20['issue_date'].iloc[0] == True, (
                f"Feb 20 should be issue_date=True for {code}"
            )

            # Also check Feb 19 is NOT an issue date
            feb19 = result[result['date'] == pd.Timestamp('2026-02-19')]
            assert len(feb19) == 1, f"Feb 19 row missing for {code}"
            assert feb19['issue_date'].iloc[0] is False or feb19['issue_date'].iloc[0] == False, (
                f"Feb 19 should be issue_date=False for {code}"
            )

    def test_stage3_discharge_sum(self, daily_data):
        """Stage 3: calculate_3daydischargesum preserves Feb 20."""
        for code in ['S1', 'S2']:
            station_data = daily_data[daily_data['code'] == code].copy()
            station_data = fl.add_pentad_issue_date(station_data, 'date')
            result = fl.calculate_3daydischargesum(station_data, 'date', 'discharge')

            feb20 = result[result['date'] == pd.Timestamp('2026-02-20')]
            assert len(feb20) == 1, f"Feb 20 row missing after discharge_sum for {code}"
            assert pd.notna(feb20['discharge_sum'].iloc[0]), (
                f"Feb 20 discharge_sum should not be NaN for {code}"
            )

    def test_stage4_discharge_avg(self, daily_data):
        """Stage 4: calculate_pentadaldischargeavg preserves Feb 20.

        Feb 20 needs data from Feb 21-25 for the target variable.
        Data goes through Feb 22, so partial average expected.
        """
        for code in ['S1', 'S2']:
            station_data = daily_data[daily_data['code'] == code].copy()
            station_data = fl.add_pentad_issue_date(station_data, 'date')
            station_data = fl.calculate_3daydischargesum(
                station_data, 'date', 'discharge'
            )
            result = fl.calculate_pentadaldischargeavg(
                station_data, 'date', 'discharge'
            )

            feb20 = result[result['date'] == pd.Timestamp('2026-02-20')]
            assert len(feb20) == 1, (
                f"Feb 20 row missing after discharge_avg for {code}"
            )
            # Check if discharge_avg is NaN or has a value
            avg_val = feb20['discharge_avg'].iloc[0]
            # Log for diagnostics
            print(
                f"\n[{code}] Feb 20 discharge_avg = {avg_val} "
                f"(NaN={pd.isna(avg_val)})"
            )

    def test_stage5_generate_issue_and_forecast_dates(self, daily_data):
        """Stage 5: Full generate_issue_and_forecast_dates preserves Feb 20."""
        # Need forecast_flags
        class MockFlags:
            pentad = True
            decad = True

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data,
            datetime_col='date',
            station_col='code',
            discharge_col='discharge',
            forecast_flags=MockFlags(),
        )

        # Check Feb 20 exists
        feb20 = data_pentad[
            data_pentad['date'] == pd.Timestamp('2026-02-20')
        ]
        assert len(feb20) == 2, (
            f"Expected 2 rows for Feb 20 in data_pentad, got {len(feb20)}. "
            f"Max date in data_pentad: {data_pentad['date'].max()}"
        )

        # Check pentad_in_year
        for _, row in feb20.iterrows():
            piy = row['pentad_in_year']
            # get_pentad_in_year returns string
            assert str(piy) == '10' or float(piy) == 10.0, (
                f"Feb 20 pentad_in_year should be 10, got {piy}"
            )

    def test_stage6_filter_for_feb20(self, daily_data):
        """Stage 6: filter + pentad_in_year filter retains Feb 20."""
        class MockFlags:
            pentad = True
            decad = True

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data,
            datetime_col='date',
            station_col='code',
            discharge_col='discharge',
            forecast_flags=MockFlags(),
        )

        # Simulate what linear_regression.py does
        filter_date = dt.date(2026, 2, 20)
        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=['S1', 'S2'],
            filter_date=filter_date,
            code_col='code',
            date_col='date',
        )

        # Filter for pentad_in_year == 10
        discharge_pentad['pentad_in_year'] = discharge_pentad['pentad_in_year'].astype(float)
        pentad10 = discharge_pentad[
            discharge_pentad['pentad_in_year'] == 10.0
        ]

        # Check if Feb 20, 2026 is in the result
        feb20 = pentad10[
            pentad10['date'] == pd.Timestamp('2026-02-20')
        ]
        date_max = pentad10['date'].max()
        print(f"\n[Stage 6] date_max in pentad10 = {date_max}")
        print(f"[Stage 6] Feb 20 rows = {len(feb20)}")
        print(f"[Stage 6] Last 5 dates in pentad10: "
              f"{sorted(pentad10['date'].unique())[-5:]}")

        assert len(feb20) == 2, (
            f"Expected 2 rows for Feb 20 in pentad10 filter, got {len(feb20)}. "
            f"date_max={date_max}"
        )

    def test_stage7_issue_date_filter_retains_feb20(self, daily_data):
        """Stage 7: After issue_date filter, Feb 20 is the last row per station."""
        class MockFlags:
            pentad = True
            decad = True

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data,
            datetime_col='date',
            station_col='code',
            discharge_col='discharge',
            forecast_flags=MockFlags(),
        )

        filter_date = dt.date(2026, 2, 20)
        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=['S1', 'S2'],
            filter_date=filter_date,
            code_col='code',
            date_col='date',
        )

        discharge_pentad['pentad_in_year'] = discharge_pentad['pentad_in_year'].astype(float)
        pentad10 = discharge_pentad[
            discharge_pentad['pentad_in_year'] == 10.0
        ]

        # Filter for issue_date == True (mimics write function)
        issue_rows = pentad10[pentad10['issue_date'] == True]

        # Check Feb 20, 2026
        feb20 = issue_rows[
            issue_rows['date'] == pd.Timestamp('2026-02-20')
        ]
        print(f"\n[Stage 7] issue_date=True rows for pentad 10, last 5 dates: "
              f"{sorted(issue_rows['date'].unique())[-5:]}")
        print(f"[Stage 7] Feb 20 rows with issue_date=True: {len(feb20)}")

        # Check discharge_sum and discharge_avg for Feb 20
        for _, row in feb20.iterrows():
            print(
                f"  {row['code']}: discharge_sum={row.get('discharge_sum')}, "
                f"discharge_avg={row.get('discharge_avg')}"
            )

        assert len(feb20) == 2, (
            f"Expected 2 rows for Feb 20 with issue_date=True, got {len(feb20)}"
        )

    def test_stage8_dropna_effect(self, daily_data):
        """Stage 8: Check if dropna() removes Feb 20 due to NaN discharge_avg.

        This is the critical test. perform_linear_regression drops rows
        where ANY column is NaN. If discharge_avg is NaN for Feb 20
        (because future data is incomplete), the row would be dropped.
        """
        class MockFlags:
            pentad = True
            decad = True

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data,
            datetime_col='date',
            station_col='code',
            discharge_col='discharge',
            forecast_flags=MockFlags(),
        )

        filter_date = dt.date(2026, 2, 20)
        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=['S1', 'S2'],
            filter_date=filter_date,
            code_col='code',
            date_col='date',
        )

        discharge_pentad['pentad_in_year'] = discharge_pentad['pentad_in_year'].astype(float)
        pentad10 = discharge_pentad[
            discharge_pentad['pentad_in_year'] == 10.0
        ]

        # Simulate what perform_linear_regression does per station
        for code in ['S1', 'S2']:
            station_data = pentad10[pentad10['code'] == code].copy()
            before_count = len(station_data)

            # The dropna() in perform_linear_regression (line 1430)
            station_data_clean = station_data.dropna()
            after_count = len(station_data_clean)

            # Check if Feb 20 survived
            feb20_before = station_data[
                station_data['date'] == pd.Timestamp('2026-02-20')
            ]
            feb20_after = station_data_clean[
                station_data_clean['date'] == pd.Timestamp('2026-02-20')
            ]

            print(f"\n[Stage 8 - {code}] Rows before dropna: {before_count}")
            print(f"[Stage 8 - {code}] Rows after dropna: {after_count}")
            print(f"[Stage 8 - {code}] Feb 20 before dropna: {len(feb20_before)}")
            print(f"[Stage 8 - {code}] Feb 20 after dropna: {len(feb20_after)}")

            if len(feb20_before) > 0:
                row = feb20_before.iloc[0]
                nan_cols = [
                    col for col in row.index if pd.isna(row[col])
                ]
                print(f"[Stage 8 - {code}] Feb 20 NaN columns: {nan_cols}")

            assert len(feb20_after) > 0, (
                f"Feb 20 row for {code} was DROPPED by dropna()! "
                f"NaN columns: {nan_cols if len(feb20_before) > 0 else 'N/A'}"
            )


def _make_write_data(codes, date_str, pentad_in_year=10):
    """Helper: create minimal DataFrame for write_linreg_pentad_forecast_data."""
    n = len(codes)
    return pd.DataFrame({
        'code': codes,
        'date': pd.to_datetime([date_str] * n),
        'discharge': [50.0] * n,
        'discharge_avg': [11.0] * n,
        'predictor': [12.0] * n,
        'forecasted_discharge': [13.0] * n,
        'issue_date': [True] * n,
        'pentad_in_year': [pentad_in_year] * n,
        'pentad_in_month': [1] * n,
        'q_mean': [14.0] * n,
        'q_std_sigma': [1.5] * n,
        'delta': [1.0] * n,
        'slope': [0.5] * n,
        'intercept': [5.0] * n,
    })


class TestWriteSkipsStaleData:
    """Write functions skip output when data doesn't match forecast year."""

    @pytest.fixture(autouse=True)
    def setup_env(self, tmp_path):
        self.tmp_dir = str(tmp_path)
        os.environ["ieasyforecast_intermediate_data_path"] = self.tmp_dir
        os.environ["ieasyforecast_analysis_pentad_file"] = "pentad.csv"
        os.environ["ieasyforecast_analysis_decad_file"] = "decad.csv"
        self.pentad_path = os.path.join(self.tmp_dir, "pentad.csv")
        self.decad_path = os.path.join(self.tmp_dir, "decad.csv")
        yield

    @patch.object(fl, '_write_lr_forecast_to_api')
    def test_pentad_skips_when_no_current_year_data(self, mock_api):
        """Data from 2025 only, forecast_date=2026-02-20 -> skip write."""
        data = _make_write_data(['S1', 'S2'], '2025-02-15')
        fl.write_linreg_pentad_forecast_data(
            data, forecast_date=dt.date(2026, 2, 20))

        assert not os.path.exists(self.pentad_path), (
            "CSV should NOT be written when data is stale"
        )
        mock_api.assert_not_called()

    @patch.object(fl, '_write_lr_forecast_to_api')
    def test_pentad_writes_when_current_year_data(self, mock_api):
        """Data from 2026, forecast_date=2026-02-20 -> write succeeds."""
        data = _make_write_data(['S1', 'S2'], '2026-02-15')
        fl.write_linreg_pentad_forecast_data(
            data, forecast_date=dt.date(2026, 2, 20))

        assert os.path.exists(self.pentad_path), (
            "CSV should be written when data is current"
        )
        # Verify the date in the CSV matches the forecast date
        result = pd.read_csv(self.pentad_path)
        assert all(result['date'] == '2026-02-20'), (
            f"All dates should be 2026-02-20, got {result['date'].tolist()}"
        )
        mock_api.assert_called_once()

    @patch.object(fl, '_write_lr_forecast_to_api')
    def test_pentad_backward_compat_no_forecast_date(self, mock_api):
        """Without forecast_date, legacy year-derivation logic works."""
        data = _make_write_data(['S1'], '2025-03-10')
        fl.write_linreg_pentad_forecast_data(data)

        assert os.path.exists(self.pentad_path), (
            "CSV should be written with legacy logic (no forecast_date)"
        )
        mock_api.assert_called_once()

    @patch.object(fl, '_write_lr_forecast_to_api')
    def test_decad_skips_when_no_current_year_data(self, mock_api):
        """Decad write: stale data -> skip."""
        n = 2
        data = pd.DataFrame({
            'code': ['S1', 'S2'],
            'date': pd.to_datetime(['2025-02-10'] * n),
            'discharge': [50.0] * n,
            'discharge_avg': [11.0] * n,
            'predictor': [12.0] * n,
            'forecasted_discharge': [13.0] * n,
            'issue_date': [True] * n,
            'decad_in_year': [5] * n,
            'q_mean': [14.0] * n,
            'q_std_sigma': [1.5] * n,
            'delta': [1.0] * n,
            'slope': [0.5] * n,
            'intercept': [5.0] * n,
        })
        fl.write_linreg_decad_forecast_data(
            data, forecast_date=dt.date(2026, 2, 20))

        assert not os.path.exists(self.decad_path), (
            "Decad CSV should NOT be written when data is stale"
        )
        mock_api.assert_not_called()

    @patch.object(fl, '_write_lr_forecast_to_api')
    def test_decad_writes_when_current_year_data(self, mock_api):
        """Decad write: current data -> write with correct date."""
        n = 2
        data = pd.DataFrame({
            'code': ['S1', 'S2'],
            'date': pd.to_datetime(['2026-02-10'] * n),
            'discharge': [50.0] * n,
            'discharge_avg': [11.0] * n,
            'predictor': [12.0] * n,
            'forecasted_discharge': [13.0] * n,
            'issue_date': [True] * n,
            'decad_in_year': [5] * n,
            'q_mean': [14.0] * n,
            'q_std_sigma': [1.5] * n,
            'delta': [1.0] * n,
            'slope': [0.5] * n,
            'intercept': [5.0] * n,
        })
        fl.write_linreg_decad_forecast_data(
            data, forecast_date=dt.date(2026, 2, 20))

        assert os.path.exists(self.decad_path), (
            "Decad CSV should be written when data is current"
        )
        result = pd.read_csv(self.decad_path)
        assert all(result['date'] == '2026-02-20'), (
            f"All dates should be 2026-02-20, got {result['date'].tolist()}"
        )


class TestPerformLinearRegressionForecastDate:
    """perform_linear_regression uses explicit year from forecast_date."""

    def test_uses_explicit_year_for_pentad_date(self):
        """forecast_date=date(2025, 2, 20) -> year 2025 used for pentad
        date calculation, not datetime.now().year."""
        # Create minimal data with pentad column
        data = pd.DataFrame({
            'station': ['A'] * 6,
            'pentad': [1, 2, 3, 4, 5, 6] * 1,
            'discharge_sum': [100, 200, 300, 400, 500, 600],
            'discharge_avg': [10, 20, 30, 40, 50, 60],
            'date': pd.to_datetime([
                '2024-01-05', '2024-01-10', '2024-01-15',
                '2024-01-20', '2024-01-25', '2024-01-31',
            ]),
        })
        # Duplicate rows to have enough data points (>2)
        data = pd.concat([data] * 3, ignore_index=True)

        with patch.object(
            tl, 'get_date_for_last_day_in_pentad',
            return_value='2025-01-10'
        ) as mock_fn:
            fl.perform_linear_regression(
                data, 'station', 'pentad', 'discharge_sum',
                'discharge_avg', 2,
                forecast_date=dt.date(2025, 2, 20))

            # Verify year=2025 was passed, not the current year
            mock_fn.assert_called_once_with(2, year=2025)

    def test_uses_explicit_year_for_decad_date(self):
        """forecast_date=date(2025, 3, 10) -> year 2025 for decad."""
        data = pd.DataFrame({
            'station': ['A'] * 3,
            'decad': [1, 2, 3],
            'discharge_sum': [100, 200, 300],
            'discharge_avg': [10, 20, 30],
            'date': pd.to_datetime([
                '2024-01-10', '2024-01-20', '2024-01-31',
            ]),
        })
        data = pd.concat([data] * 3, ignore_index=True)

        with patch.object(
            tl, 'get_date_for_last_day_in_decad',
            return_value='2025-01-20'
        ) as mock_fn:
            fl.perform_linear_regression(
                data, 'station', 'decad', 'discharge_sum',
                'discharge_avg', 2,
                forecast_date=dt.date(2025, 3, 10))

            mock_fn.assert_called_once_with(2, year=2025)

    def test_backward_compat_no_forecast_date(self):
        """Without forecast_date, function still works (uses now().year)."""
        data = pd.DataFrame({
            'station': ['A'] * 6,
            'pentad': [1, 2, 3, 4, 5, 6],
            'discharge_sum': [100, 200, 300, 400, 500, 600],
            'discharge_avg': [10, 20, 30, 40, 50, 60],
            'date': pd.to_datetime([
                '2024-01-05', '2024-01-10', '2024-01-15',
                '2024-01-20', '2024-01-25', '2024-01-31',
            ]),
        })
        data = pd.concat([data] * 3, ignore_index=True)

        # Should not raise — uses datetime.now().year fallback
        result = fl.perform_linear_regression(
            data, 'station', 'pentad', 'discharge_sum',
            'discharge_avg', 2)

        assert isinstance(result, pd.DataFrame)
        assert 'forecasted_discharge' in result.columns
