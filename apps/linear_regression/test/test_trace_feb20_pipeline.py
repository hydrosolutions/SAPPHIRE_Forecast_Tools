"""End-to-end pipeline trace for Feb 20, 2026 linear regression.

Creates synthetic daily discharge data through Feb 23, 2026, runs each
pipeline stage, and prints/asserts the state of Feb 20 data at every step.
This identifies exactly where the forecast data is lost.
"""

import os
import sys
import datetime as dt

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


def _make_daily_discharge(codes, start_date, end_date, base=50.0):
    """Create daily discharge for multiple stations."""
    np.random.seed(42)
    dates = pd.date_range(start_date, end_date, freq='D')
    rows = []
    for code in codes:
        for d in dates:
            rows.append({
                'code': str(code),
                'date': d,
                'discharge': base + np.random.uniform(-5, 5),
            })
    return pd.DataFrame(rows)


class TestTraceFeb20Pipeline:
    """Trace every value through the pipeline for Feb 20, 2026."""

    CODES = ['S1', 'S2']
    FORECAST_DATE = dt.date(2026, 2, 20)

    @pytest.fixture
    def daily_data(self):
        return _make_daily_discharge(
            self.CODES, '2022-01-01', '2026-02-23', base=50.0
        )

    # ------------------------------------------------------------------
    # Stage 1: What pentad is Feb 20?
    # ------------------------------------------------------------------
    def test_stage1_pentad_identity(self):
        """Feb 20 should be pentad 10 of the year, pentad 4 of Feb."""
        d = self.FORECAST_DATE
        pentad_in_year = tl.get_pentad_in_year(d)
        pentad_in_month = tl.get_pentad(d)
        print(f"\n[Stage 1] Feb 20 pentad_in_year={pentad_in_year}, "
              f"pentad_in_month={pentad_in_month}")
        # get_pentad_in_year returns a string
        assert str(pentad_in_year) == '10', \
            f"Expected pentad '10', got {pentad_in_year!r}"

    # ------------------------------------------------------------------
    # Stage 2: generate_issue_and_forecast_dates
    # ------------------------------------------------------------------
    def test_stage2_generate_dates(self, daily_data):
        """Check that Feb 20, 2026 row has valid discharge_sum and
        discharge_avg after full preprocessing."""
        import setup_library as sl
        forecast_flags = sl.ForecastFlags(pentad=True, decad=False)

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data.copy(), datetime_col='date', station_col='code',
            discharge_col='discharge', forecast_flags=forecast_flags)

        # Check Feb 20, 2026 for station S1
        feb20 = data_pentad[
            (data_pentad['date'] == pd.Timestamp('2026-02-20'))
            & (data_pentad['code'] == 'S1')
        ]
        print(f"\n[Stage 2] Feb 20 S1 row:\n{feb20.to_string()}")
        assert len(feb20) == 1, f"Expected 1 row, got {len(feb20)}"

        row = feb20.iloc[0]
        print(f"  issue_date={row['issue_date']}")
        print(f"  discharge_sum={row.get('discharge_sum', 'MISSING')}")
        print(f"  discharge_avg={row.get('discharge_avg', 'MISSING')}")
        print(f"  pentad_in_year={row.get('pentad_in_year', 'MISSING')}")

        assert row['issue_date'] == True, "Feb 20 should be issue_date=True"
        assert pd.notna(row['discharge_sum']), \
            f"discharge_sum is NaN! This means 3-day sum failed."
        assert pd.notna(row['discharge_avg']), \
            f"discharge_avg is NaN! This means pentadal avg failed."
        assert str(row['pentad_in_year']) == '10', \
            f"Expected pentad_in_year='10', got {row['pentad_in_year']!r}"

    # ------------------------------------------------------------------
    # Stage 3: filter_discharge_data_for_code_and_date
    # ------------------------------------------------------------------
    def test_stage3_filter_data(self, daily_data):
        """After filtering to date <= Feb 20, the Feb 20 row should survive
        with all columns intact."""
        import setup_library as sl
        forecast_flags = sl.ForecastFlags(pentad=True, decad=False)

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data.copy(), datetime_col='date', station_col='code',
            discharge_col='discharge', forecast_flags=forecast_flags)

        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=self.CODES,
            filter_date=self.FORECAST_DATE,
            code_col='code',
            date_col='date')

        # Check Feb 20 rows exist
        feb20 = discharge_pentad[
            (discharge_pentad['date'] == pd.Timestamp('2026-02-20'))
        ]
        print(f"\n[Stage 3] Feb 20 rows after filter (both stations):")
        print(feb20[['date', 'code', 'discharge_sum', 'discharge_avg',
                      'issue_date', 'pentad_in_year']].to_string())

        assert len(feb20) == 2, f"Expected 2 rows (2 stations), got {len(feb20)}"

        for _, row in feb20.iterrows():
            assert pd.notna(row['discharge_sum']), \
                f"discharge_sum NaN for {row['code']}"
            assert pd.notna(row['discharge_avg']), \
                f"discharge_avg NaN for {row['code']}"

    # ------------------------------------------------------------------
    # Stage 4: perform_linear_regression for pentad 10
    # ------------------------------------------------------------------
    def test_stage4_perform_linear_regression(self, daily_data):
        """perform_linear_regression should return data including 2026 rows
        with valid forecasted_discharge."""
        import setup_library as sl
        forecast_flags = sl.ForecastFlags(pentad=True, decad=False)

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data.copy(), datetime_col='date', station_col='code',
            discharge_col='discharge', forecast_flags=forecast_flags)

        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=self.CODES,
            filter_date=self.FORECAST_DATE,
            code_col='code',
            date_col='date')

        pentad_of_year = tl.get_pentad_in_year(self.FORECAST_DATE)
        assert str(pentad_of_year) == '10'

        result = fl.perform_linear_regression(
            data_df=discharge_pentad,
            station_col='code',
            horizon_col='pentad_in_year',
            predictor_col='discharge_sum',
            discharge_avg_col='discharge_avg',
            forecast_horizon_int=int(pentad_of_year),
            forecast_date=self.FORECAST_DATE)

        print(f"\n[Stage 4] Linear regression result shape: {result.shape}")
        print(f"[Stage 4] Result columns: {list(result.columns)}")
        print(f"[Stage 4] Result years: "
              f"{sorted(result['date'].dt.year.unique())}")

        # Check for 2026 rows
        result_2026 = result[result['date'].dt.year == 2026]
        print(f"\n[Stage 4] 2026 rows ({len(result_2026)}):")
        if not result_2026.empty:
            print(result_2026[['date', 'code', 'discharge_sum',
                               'discharge_avg', 'forecasted_discharge',
                               'issue_date']].to_string())
        else:
            # Show what pentad_in_year values exist
            print(f"[Stage 4] ALL pentad_in_year values in data: "
                  f"{sorted(discharge_pentad['pentad_in_year'].unique())}")
            # Show the last few rows per station
            for code in self.CODES:
                code_data = discharge_pentad[
                    discharge_pentad['code'] == code
                ].tail(5)
                print(f"\n[Stage 4] Last 5 rows for {code}:")
                print(code_data[['date', 'pentad_in_year',
                                 'discharge_sum', 'discharge_avg',
                                 'issue_date']].to_string())

        assert not result.empty, "Regression returned empty DataFrame!"
        assert len(result_2026) > 0, (
            "No 2026 rows in regression output! "
            "Check if pentad_in_year filter or dropna() removed them."
        )

        # Only check issue_date=True rows (non-issue-date rows correctly
        # have NaN since discharge_sum/avg are NaN for them)
        issue_2026 = result_2026[result_2026['issue_date'] == True]
        assert len(issue_2026) > 0, "No issue_date=True rows for 2026!"
        for _, row in issue_2026.iterrows():
            assert pd.notna(row['forecasted_discharge']), \
                f"forecasted_discharge is NaN for {row['code']} on {row['date']}"

    # ------------------------------------------------------------------
    # Stage 5: Write function with forecast_date
    # ------------------------------------------------------------------
    def test_stage5_write_function(self, daily_data, tmp_path):
        """The write function should produce output with date=2026-02-20."""
        import setup_library as sl
        forecast_flags = sl.ForecastFlags(pentad=True, decad=False)

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily_data.copy(), datetime_col='date', station_col='code',
            discharge_col='discharge', forecast_flags=forecast_flags)

        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad,
            filter_sites=self.CODES,
            filter_date=self.FORECAST_DATE,
            code_col='code',
            date_col='date')

        pentad_of_year = tl.get_pentad_in_year(self.FORECAST_DATE)

        result = fl.perform_linear_regression(
            data_df=discharge_pentad,
            station_col='code',
            horizon_col='pentad_in_year',
            predictor_col='discharge_sum',
            discharge_avg_col='discharge_avg',
            forecast_horizon_int=int(pentad_of_year),
            forecast_date=self.FORECAST_DATE)

        # Rename columns as the caller does
        result.rename(columns={
            'discharge_sum': 'predictor',
            'pentad': 'pentad_in_month'
        }, inplace=True)

        # Set up env for write function
        os.environ["ieasyforecast_intermediate_data_path"] = str(tmp_path)
        os.environ["ieasyforecast_analysis_pentad_file"] = "pentad.csv"
        output_path = tmp_path / "pentad.csv"

        print(f"\n[Stage 5] Data going to write function:")
        print(f"  Shape: {result.shape}")
        print(f"  Columns: {list(result.columns)}")
        issue_rows = result[result['issue_date'] == True]
        print(f"  issue_date=True rows: {len(issue_rows)}")
        issue_2026 = issue_rows[issue_rows['date'].dt.year == 2026]
        print(f"  issue_date=True AND year=2026 rows: {len(issue_2026)}")
        if not issue_2026.empty:
            print(issue_2026[['date', 'code', 'predictor',
                              'discharge_avg', 'forecasted_discharge',
                              'issue_date']].to_string())

        with patch.object(fl, '_write_lr_forecast_to_api'):
            fl.write_linreg_pentad_forecast_data(
                result, forecast_date=self.FORECAST_DATE)

        assert output_path.exists(), (
            f"CSV file was NOT written! This means the write function "
            f"returned early. Check stale-data validation."
        )

        written = pd.read_csv(str(output_path))
        print(f"\n[Stage 5] Written CSV:")
        print(written.to_string())

        assert len(written) > 0, "Written CSV is empty!"
        assert all(written['date'] == '2026-02-20'), (
            f"Dates in CSV: {written['date'].tolist()}"
        )

    # ------------------------------------------------------------------
    # Stage 6: Trace discharge_avg computation for Feb 20
    # ------------------------------------------------------------------
    def test_stage6_discharge_avg_detail(self):
        """Directly trace what calculate_pentadaldischargeavg produces
        for the last few dates of a single station."""
        # Create a simple single-station timeseries
        dates = pd.date_range('2026-02-10', '2026-02-23', freq='D')
        data = pd.DataFrame({
            'date': dates,
            'code': 'S1',
            'discharge': [10 + i for i in range(len(dates))],
        })
        # Add issue_date
        data = fl.add_pentad_issue_date(data, 'date')

        print(f"\n[Stage 6] Input data with issue_date:")
        print(data[['date', 'discharge', 'issue_date']].to_string())

        # Calculate discharge_avg
        result = fl.calculate_pentadaldischargeavg(
            data, 'date', 'discharge')

        print(f"\n[Stage 6] After calculate_pentadaldischargeavg:")
        print(result[['date', 'discharge', 'issue_date',
                       'discharge_avg']].to_string())

        # Check Feb 20 specifically
        feb20 = result[result['date'] == pd.Timestamp('2026-02-20')]
        print(f"\n[Stage 6] Feb 20 discharge_avg: "
              f"{feb20['discharge_avg'].values}")

        assert len(feb20) == 1
        avg_val = feb20['discharge_avg'].iloc[0]
        print(f"[Stage 6] discharge_avg value: {avg_val}, "
              f"is_nan: {pd.isna(avg_val)}")

        # Also check Feb 15 (issue_date=True, day 15)
        feb15 = result[result['date'] == pd.Timestamp('2026-02-15')]
        if not feb15.empty:
            print(f"[Stage 6] Feb 15 discharge_avg: "
                  f"{feb15['discharge_avg'].values}")

    # ------------------------------------------------------------------
    # Stage 7: Trace discharge_sum computation for Feb 20
    # ------------------------------------------------------------------
    def test_stage7_discharge_sum_detail(self):
        """Directly trace what calculate_3daydischargesum produces
        for the last few dates."""
        dates = pd.date_range('2026-02-10', '2026-02-23', freq='D')
        data = pd.DataFrame({
            'date': dates,
            'code': 'S1',
            'discharge': [10 + i for i in range(len(dates))],
        })
        data = fl.add_pentad_issue_date(data, 'date')

        print(f"\n[Stage 7] Input data:")
        print(data[['date', 'discharge', 'issue_date']].to_string())

        result = fl.calculate_3daydischargesum(data, 'date', 'discharge')

        print(f"\n[Stage 7] After calculate_3daydischargesum:")
        print(result[['date', 'discharge', 'issue_date',
                       'discharge_sum']].to_string())

        feb20 = result[result['date'] == pd.Timestamp('2026-02-20')]
        print(f"\n[Stage 7] Feb 20 discharge_sum: "
              f"{feb20['discharge_sum'].values}")
        assert len(feb20) == 1
        sum_val = feb20['discharge_sum'].iloc[0]
        assert pd.notna(sum_val), f"discharge_sum is NaN for Feb 20!"

        # Verify: sum of Feb 18 (18), Feb 19 (19), Feb 20 (20)
        # Values are 10+8=18, 10+9=19, 10+10=20 -> sum=57
        print(f"[Stage 7] Expected sum: 18+19+20=57, got: {sum_val}")

    # ------------------------------------------------------------------
    # Stage 8: CRITICAL - data ending AT Feb 20 (operational scenario)
    # ------------------------------------------------------------------
    def test_stage8_data_ends_at_feb20(self, tmp_path):
        """When data ends at Feb 20 (pipeline runs on Feb 20 before future
        data arrives), discharge_avg for Feb 20 2026 should be NaN but the
        forecast should still be produced from the predictor (discharge_sum).
        """
        # Data through Feb 20 only (no future data)
        daily = _make_daily_discharge(
            self.CODES, '2022-01-01', '2026-02-20', base=50.0)

        import setup_library as sl
        forecast_flags = sl.ForecastFlags(pentad=True, decad=False)

        data_pentad, _ = fl.generate_issue_and_forecast_dates(
            daily.copy(), datetime_col='date', station_col='code',
            discharge_col='discharge', forecast_flags=forecast_flags)

        # Check discharge_avg for Feb 20, 2026
        feb20 = data_pentad[
            (data_pentad['date'] == pd.Timestamp('2026-02-20'))
            & (data_pentad['code'] == 'S1')
        ]
        row = feb20.iloc[0]
        print(f"\n[Stage 8] Data ends at Feb 20:")
        print(f"  discharge_sum={row.get('discharge_sum', 'MISSING')}")
        print(f"  discharge_avg={row.get('discharge_avg', 'MISSING')}")
        print(f"  issue_date={row['issue_date']}")

        # Filter and run regression
        discharge_pentad = fl.filter_discharge_data_for_code_and_date(
            df=data_pentad, filter_sites=self.CODES,
            filter_date=self.FORECAST_DATE, code_col='code',
            date_col='date')

        pentad_of_year = int(tl.get_pentad_in_year(self.FORECAST_DATE))

        result = fl.perform_linear_regression(
            data_df=discharge_pentad, station_col='code',
            horizon_col='pentad_in_year', predictor_col='discharge_sum',
            discharge_avg_col='discharge_avg',
            forecast_horizon_int=pentad_of_year,
            forecast_date=self.FORECAST_DATE)

        # Check 2026 issue_date=True rows
        result_2026 = result[
            (result['date'].dt.year == 2026)
            & (result['issue_date'] == True)
        ]
        print(f"\n[Stage 8] Regression output for 2026 issue dates:")
        if not result_2026.empty:
            print(result_2026[['date', 'code', 'discharge_sum',
                               'discharge_avg', 'forecasted_discharge']
                              ].to_string())
        else:
            print("  NO 2026 issue_date=True rows!")
            # Show what we DO have
            all_2026 = result[result['date'].dt.year == 2026]
            print(f"  All 2026 rows: {len(all_2026)}")
            print(all_2026[['date', 'code', 'discharge_sum',
                            'discharge_avg', 'issue_date']].to_string())

        # Now test the write function
        if not result.empty:
            result.rename(columns={
                'discharge_sum': 'predictor',
                'pentad': 'pentad_in_month'
            }, inplace=True)

            os.environ["ieasyforecast_intermediate_data_path"] = str(tmp_path)
            os.environ["ieasyforecast_analysis_pentad_file"] = "pentad8.csv"
            output_path = tmp_path / "pentad8.csv"

            with patch.object(fl, '_write_lr_forecast_to_api'):
                fl.write_linreg_pentad_forecast_data(
                    result, forecast_date=self.FORECAST_DATE)

            if output_path.exists():
                written = pd.read_csv(str(output_path))
                print(f"\n[Stage 8] Written CSV:")
                print(written.to_string())
                assert all(written['date'] == '2026-02-20')
            else:
                print(f"\n[Stage 8] CSV NOT written!")
                pytest.fail(
                    "Write function skipped output when data ends at Feb 20"
                )
