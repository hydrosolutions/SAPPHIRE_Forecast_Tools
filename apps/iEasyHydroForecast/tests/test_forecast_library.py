import datetime
import numpy as np
import pandas as pd
import unittest
import datetime as dt
import math
import os
import sys

from pandas._testing import assert_frame_equal

from iEasyHydroForecast import forecast_library as fl
from iEasyHydroForecast import setup_library as sl

print(sys.path)

class TestGetLastDayOfMonth(unittest.TestCase):
    def test_get_last_day_of_month_with_valid_date(self):
        # Test a valid date
        date = dt.date(2022, 5, 15)
        expected_last_day_of_month = dt.date(2022, 5, 31).day
        self.assertEqual(fl.get_last_day_of_month(date).day, expected_last_day_of_month)

    def test_get_last_day_of_month_with_non_date_input(self):
        # Test a non-date input
        date = 'not a date'
        with self.assertRaises(TypeError):
            fl.get_last_day_of_month(date)

    def test_get_last_day_of_month(self):
        # Test the last day of January 2022
        date = datetime.date(2022, 1, 15)
        last_day_of_month = fl.get_last_day_of_month(date)
        self.assertEqual(last_day_of_month, datetime.date(2022, 1, 31))

        # Test the last day of February 2022
        date = datetime.date(2022, 2, 15)
        last_day_of_month = fl.get_last_day_of_month(date)
        self.assertEqual(last_day_of_month, datetime.date(2022, 2, 28))

        # Test the last day of a leap year February
        date = datetime.date(2020, 2, 15)
        last_day_of_month = fl.get_last_day_of_month(date)
        self.assertEqual(last_day_of_month, datetime.date(2020, 2, 29))


class TestGetPredictorDates_DEPRECATING(unittest.TestCase):
    def test_valid_input(self):
        # Test with valid input
        input_date = datetime.date(2022, 1, 1).strftime('%Y-%m-%d')
        n = 5
        expected_output = [
            datetime.date(2021, 12, 31),
            datetime.date(2021, 12, 30),
            datetime.date(2021, 12, 29),
            datetime.date(2021, 12, 28),
            datetime.date(2021, 12, 27)
        ]
        to_test = fl.get_predictor_dates_deprecating(input_date, n)
        self.assertEqual(to_test, expected_output)
        # Second test with valid input
        self.assertEqual(
            fl.get_predictor_dates_deprecating('2022-01-05', 3),
            [datetime.date(2022, 1, 4),
             datetime.date(2022, 1, 3),
             datetime.date(2022, 1, 2)])

    def test_invalid_input_date(self):
        # Test with invalid input_date
        input_date = datetime.date(2022, 1, 1)
        n = 5
        expected_output = None
        self.assertEqual(fl.get_predictor_dates_deprecating(input_date, n), expected_output)

    def test_invalid_n(self):
        # Test with invalid n
        input_date = datetime.date(2022, 1, 1)
        n = -5
        expected_output = None
        self.assertEqual(fl.get_predictor_dates_deprecating(input_date, n), expected_output)

    def test_invalid_n_type(self):
        # Test with invalid n type
        input_date = datetime.date(2022, 1, 1)
        n = '5'
        expected_output = None
        self.assertEqual(fl.get_predictor_dates_deprecating(input_date, n), expected_output)


class TestRoundDischarge(unittest.TestCase):
    def test_round_discharge_with_string_input(self):
        # Test that the function returns none when passed a string
        self.assertEqual(fl.round_discharge('test'), None)

    def test_round_discharge_tiny_values(self):
        self.assertEqual(fl.round_discharge(0.0001), "0.00")
        self.assertEqual(fl.round_discharge(0.9), "0.90")

    def test_round_discharge_small_value(self):
        # Test rounding a small discharge value
        value = 10.1234
        expected = "10.1"
        result = fl.round_discharge(value)
        self.assertEqual(result, expected)

    def test_round_discharge_medium_value(self):
        # Test rounding a large discharge value
        value = 30.1234
        expected = "30.1"
        result = fl.round_discharge(value)
        self.assertEqual(result, expected)

    def test_round_discharge_large_value(self):
        self.assertEqual(fl.round_discharge(100.1234), "100")
        self.assertEqual(fl.round_discharge(1000.8234), "1001")

class TestRoundDischargeToFloat(unittest.TestCase):
    def test_round_discharge_to_float(self):
        self.assertEqual(fl.round_discharge_to_float(0.0), 0.0)
        self.assertEqual(fl.round_discharge_to_float(0.12345), 0.123)
        self.assertEqual(fl.round_discharge_to_float(0.012345), 0.0123)
        self.assertEqual(fl.round_discharge_to_float(0.0062315), 0.00623)
        self.assertEqual(fl.round_discharge_to_float(1.089), 1.09)
        self.assertEqual(fl.round_discharge_to_float(1.238), 1.24)
        self.assertEqual(fl.round_discharge_to_float(1.0123), 1.01)
        self.assertEqual(fl.round_discharge_to_float(10.123), 10.1)
        self.assertEqual(fl.round_discharge_to_float(100.123), 100)
        self.assertEqual(fl.round_discharge_to_float(1005.123), 1005)

    def test_round_discharge_to_float_with_negative_value(self):
        self.assertEqual(fl.round_discharge_to_float(-1.0), 0.0)

    def test_round_discharge_to_float_with_non_float_value(self):
        with self.assertRaises(TypeError):
            fl.round_discharge_to_float('1.0')

class TestPerformLinearRegression(unittest.TestCase):
    def test_perform_linear_regression_with_wrong_input_type(self):
        # Create a test DataFrame
        data = {'station': ['A', 'A', 'B', 'B', 'C', 'C'],
                'pentad': [1, 2, 1, 2, 1, 2],
                'discharge_sum': [100, 200, 150, 250, 120, 180],
                'discharge_avg': [10, 20, 15, 25, 12, 18]}
        df = pd.DataFrame(data)

        # Test that the call to perform_linear_regression throws a type error
        with self.assertRaises(TypeError):
            fl.perform_linear_regression('test', 'station', 'pentad', 'discharge_sum', 'discharge_avg', 2)
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 2, 'pentad', 'discharge_sum', 'discharge_avg', 2)
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 'station', 2.0, 'discharge_sum', 'discharge_avg', 2)
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 'station', 'pentad', 1, 'discharge_avg', 2)
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 1, 2)
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', '2')
        with self.assertRaises(TypeError):
            fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 2.0)

    def test_perform_linear_regression_with_simple_data(self):
        # Create a test DataFrame
        data = {'station': ['A', 'A', 'B', 'B', 'C', 'C'],
                'pentad': [1, 2, 1, 2, 1, 2],
                'discharge_sum': [100, 200, 150, 250, 120, 180],
                'discharge_avg': [10, 20, 15, 25, 12, 18]}
        df = pd.DataFrame(data)

        # Call the perform_linear_regression method
        result = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 2)

        # Check that the result is a DataFrame
        assert isinstance(result, pd.DataFrame)

        # Check that the result has the expected columns
        expected_columns = [
            'station', 'pentad', 'discharge_sum', 'discharge_avg', 'slope',
            'intercept', 'forecasted_discharge']
        assert all(col in result.columns for col in expected_columns)

        # Check that the slope and intercept are correct for each station
        expected_slopes = {'A': 0.0, 'B': 0.0, 'C': 0.0}
        expected_intercepts_p2 = {'A': 20.0, 'B': 25.0, 'C': 18.0}
        for station in expected_slopes.keys():
            slope = result.loc[(result['station'] == station) & (result['pentad'] == 2), 'slope'].values[0]
            intercept = result.loc[(result['station'] == station) & (result['pentad'] == 2), 'intercept'].values[0]
            forecast_exp = df.loc[(df['station'] == station) & (df['pentad'] == 2), 'discharge_avg'].values[0]
            forecast_calc = slope * df.loc[
                (df['station'] == station) & (df['pentad'] == 2),
                'discharge_avg'].values[0] + intercept
            assert np.isclose(slope, expected_slopes[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p2[station], atol=1e-3)
            assert np.isclose(forecast_exp, forecast_calc, atol=1e-3)

    def test_perform_linear_regression_with_complex_data(self):
        # Create a test DataFrame
        data = {'station': ['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                            'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                            'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                            'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                            'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B', 'B'],
                'pentad': [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
                           3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4,
                           5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6,
                           7, 7, 7, 7, 7, 7, 72, 72, 72, 72, 72, 72,
                           1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2],
                'discharge_sum': [100, np.nan, 200, np.nan, 150, 200, 180, np.nan, 220, np.nan, 170, 230,
                                  100, np.nan, 200, np.nan, 150, 200, 180, np.nan, 220, np.nan, 170, 230,
                                  100, np.nan, 200, np.nan, 150, 200, 180, np.nan, 220, np.nan, 170, 230,
                                  100, np.nan, 200, np.nan, 150, 200, 180, np.nan, 220, np.nan, 170, 230,
                                  120, np.nan, 180, np.nan, 150, 150, 140, np.nan, 160, np.nan, 130, 170],
                'discharge_avg': [10, np.nan, 20, np.nan, 15, 18, 16, np.nan, 22, np.nan, 20, 24,
                                  10, np.nan, 20, np.nan, 15, 18, 16, np.nan, 22, np.nan, 20, 24,
                                  10, np.nan, 20, np.nan, 15, 18, 16, np.nan, 22, np.nan, 20, 24,
                                  10, np.nan, 20, np.nan, 15, 18, 16, np.nan, 22, np.nan, 20, 24,
                                  12, np.nan, 18, np.nan, 14, 16, 14, np.nan, 16, np.nan, 13, 17],
                'forecast_exp': [10.18, -1, 19.09, -1, 14.64, 19.09, 18.65, -1, 22.34, -1, 17.73, 23.27,
                                 10.18, -1, 19.09, -1, 14.64, 19.09, 18.65, -1, 22.34, -1, 17.73, 23.27,
                                 10.18, -1, 19.09, -1, 14.64, 19.09, 18.65, -1, 22.34, -1, 17.73, 23.27,
                                 10.18, -1, 19.09, -1, 14.64, 19.09, 18.65, -1, 22.34, -1, 17.73, 23.27,
                                 12.0, -1, 18.0, -1, 15.0, 15.0, 14.0, -1, 16.0, -1, 13.0, 17.0]}
        df = pd.DataFrame(data)

        '''
        # Group the DataFrame by station
        grouped = df.groupby('station')

        # Create a scatter plot for each station
        for name, group in grouped:
            plt.scatter(x=group['discharge_avg'], y=group['discharge_sum'], c=group['pentad'], cmap='viridis', label=name)

        # Add a colorbar
        plt.colorbar()

        # Add labels and title
        plt.xlabel('Discharge Average')
        plt.ylabel('Discharge Sum')
        plt.title('Discharge Sum vs. Discharge Average by Pentad')

        # Show the plot
        #plt.show()
        '''

        # Call the perform_linear_regression method
        result_p1 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 1)
        result_p2 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 2)
        result_p3 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 3)
        result_p4 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 4)
        result_p5 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 5)
        result_p6 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 6)
        result_p7 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 7)
        result_p72 = fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 72)

        # Calling perform_linear_regression with a pentad of 73 should raise a
        # ValueError. Test that this is the case.
        with self.assertRaises(ValueError):
            fl.perform_linear_regression(df, 'station', 'pentad', 'discharge_sum', 'discharge_avg', 73)

        # Check that the result is a DataFrame
        assert isinstance(result_p1, pd.DataFrame)
        assert isinstance(result_p2, pd.DataFrame)
        assert isinstance(result_p3, pd.DataFrame)
        assert isinstance(result_p4, pd.DataFrame)
        assert isinstance(result_p5, pd.DataFrame)
        assert isinstance(result_p6, pd.DataFrame)
        assert isinstance(result_p7, pd.DataFrame)
        assert isinstance(result_p72, pd.DataFrame)

        # Check that the result has the expected columns
        expected_columns = [
            'station', 'pentad', 'discharge_sum', 'discharge_avg', 'slope',
            'intercept', 'forecasted_discharge']
        assert all(col in result_p1.columns for col in expected_columns)
        assert all(col in result_p2.columns for col in expected_columns)
        assert all(col in result_p3.columns for col in expected_columns)
        assert all(col in result_p4.columns for col in expected_columns)
        assert all(col in result_p5.columns for col in expected_columns)
        assert all(col in result_p6.columns for col in expected_columns)
        assert all(col in result_p7.columns for col in expected_columns)
        assert all(col in result_p72.columns for col in expected_columns)

        # Check that the slope and intercept are correct for each station, allowing
        # for rounding errors
        expected_slopes_p1 = {'A': 0.0891, 'B': 0.1}
        expected_intercepts_p1 = {'A': 1.2727, 'B': 0.0}
        expected_slopes_p2 = {'A': 0.0923, 'B': 0.1}
        expected_intercepts_p2 = {'A': 2.0385, 'B': 0.0}
        expected_slopes_p3 = {'A': 0.0891}
        expected_intercepts_p3 = {'A': 1.2727}
        expected_slopes_p4 = {'A': 0.0923}
        expected_intercepts_p4 = {'A': 2.0385}

        for station in expected_slopes_p1.keys():
            slope = result_p1.loc[(result_p1['station'] == station) & (result_p1['pentad'] == 1), 'slope'].values[0]
            intercept = result_p1.loc[(result_p1['station'] == station) & (result_p1['pentad'] == 1), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 1), 'discharge_sum'].values[0] + intercept
            assert np.isclose(slope, expected_slopes_p1[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p1[station], atol=1e-3)
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 1),
                       'forecast_exp'].values[0], atol=1e-2)

        for station in expected_slopes_p2.keys():
            slope = result_p2.loc[(result_p2['station'] == station) & (result_p2['pentad'] == 2), 'slope'].values[0]
            intercept = result_p2.loc[(result_p2['station'] == station) & (result_p2['pentad'] == 2), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 2), 'discharge_sum'].values[0] + intercept
            assert np.isclose(slope, expected_slopes_p2[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p2[station], atol=1e-3)
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 2),
                       'forecast_exp'].values[0], atol=1e-2)

        for station in expected_slopes_p3.keys():
            slope = result_p3.loc[(result_p3['station'] == station) & (result_p3['pentad'] == 3), 'slope'].values[0]
            intercept = result_p3.loc[(result_p3['station'] == station) & (result_p3['pentad'] == 3), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 3), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 3),
                       'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p3[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p3[station], atol=1e-3)
            slope = result_p5.loc[(result_p5['station'] == station) & (result_p5['pentad'] == 5), 'slope'].values[0]
            intercept = result_p5.loc[(result_p5['station'] == station) & (result_p5['pentad'] == 5), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 5), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 5),
                       'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p3[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p3[station], atol=1e-3)
            slope = result_p7.loc[(result_p7['station'] == station) & (result_p7['pentad'] == 7), 'slope'].values[0]
            intercept = result_p7.loc[(result_p7['station'] == station) & (result_p7['pentad'] == 7), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 7), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 7),
                       'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p3[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p3[station], atol=1e-3)

        for station in expected_slopes_p4.keys():
            slope = result_p4.loc[(result_p4['station'] == station) & (result_p4['pentad'] == 4), 'slope'].values[0]
            intercept = result_p4.loc[(result_p4['station'] == station) & (result_p4['pentad'] == 4), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 4), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[(df['station'] == station) & (df['pentad'] == 4),
                       'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p4[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p4[station], atol=1e-3)
            slope = result_p6.loc[(result_p6['station'] == station) & (result_p6['pentad'] == 6), 'slope'].values[0]
            intercept = result_p6.loc[(result_p6['station'] == station) & (result_p6['pentad'] == 6), 'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 6), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[
                    (df['station'] == station) & (df['pentad'] == 6),
                    'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p4[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p4[station], atol=1e-3)
            slope = result_p72.loc[(result_p72['station'] == station) & (result_p72['pentad'] == 72), 'slope'].values[0]
            intercept = result_p72.loc[
                (result_p72['station'] == station) & (result_p72['pentad'] == 72),
                'intercept'].values[0]
            forecast = slope * df.loc[(df['station'] == station) & (df['pentad'] == 72), 'discharge_sum'].values[0] + intercept
            assert np.isclose(
                forecast,
                df.loc[
                    (df['station'] == station) & (df['pentad'] == 72),
                    'forecast_exp'].values[0], atol=1e-2)
            assert np.isclose(slope, expected_slopes_p4[station], atol=1e-3)
            assert np.isclose(intercept, expected_intercepts_p4[station], atol=1e-3)


class TestCalculateForecastSkill(unittest.TestCase):
    def test_calculate_forecast_skill(self):
        # Test case 1: Normal input
        data_df1 = pd.DataFrame({
            'station': ['A', 'A', 'A', 'A', 'B', 'B', 'B', 'B'],
            'pentad': [1, 2, 1, 2, 1, 2, 1, 2],
            'observation': [10.0, 12.0, 10.0, 12.0, 8.0, 9.0, 8.0, 9.0],
            'simulation': [9.0, 11.0, 9.0, 11.0, 7.0, 8.0, 7.0, 8.0]
        })
        result_df1 = fl.calculate_forecast_skill(data_df1, 'station', 'pentad', 'observation', 'simulation')
        assert result_df1['absolute_error'].tolist() == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        assert result_df1['observation_std0674'].tolist() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        assert result_df1['flag'].tolist() == [False, False, False, False, False, False, False, False]

        data_df2 = pd.DataFrame({
            'station': ['A', 'A', 'A', 'A'],
            'pentad': [1, 1, 1, 1],
            'observation': [9.0, 10.0, 11.0, 12.0],
            'simulation': [9.0, 11.0, 9.0, 11.0]
        })
        result_df2 = fl.calculate_forecast_skill(data_df2, 'station', 'pentad', 'observation', 'simulation')
        # print(result_df2['observation_std0674'].tolist())
        assert result_df2['absolute_error'].tolist() == [0.0, 1.0, 2.0, 1.0]
        assert result_df2['observation_std0674'].tolist() == [
            0.870130258447933, 0.870130258447933, 0.870130258447933,
            0.870130258447933]
        assert result_df2['flag'].tolist() == [True, False, False, False]

"""
class TestGenerateIssueAndForecastDates(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        self.data_df = pd.concat([
            pd.DataFrame({
                'datetime': pd.date_range('2022-01-01', periods=11, freq='D'),
                'station': ['A']*11,
                'discharge': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            }),
            pd.DataFrame({
                'datetime': pd.date_range('2022-01-01', periods=11, freq='D'),
                'station': ['B']*11,
                'discharge': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            }),
        ])
        self.data_df_nan = pd.concat([
            pd.DataFrame({
                'datetime': pd.date_range('2022-01-01', periods=11, freq='D'),
                'station': ['A']*11,
                'discharge': [1, 2, np.nan, np.nan, 5, 6, 7, 8, 9, np.nan, 11]
            }),
            pd.DataFrame({
                'datetime': pd.date_range('2022-01-01', periods=11, freq='D'),
                'station': ['B']*11,
                'discharge': [np.nan, np.nan, 3, 4, 5, 6, np.nan, 8, 9, 10, np.nan]
            }),
        ])

    def test_output_type(self):
        # Test that the output is a pandas DataFrame
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'datetime', 'station', 'discharge')
        self.assertIsInstance(output, pd.DataFrame)

    def test_output_columns(self):
        # Test that the output DataFrame has the expected columns
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'datetime', 'station', 'discharge')
        expected_columns = ['datetime', 'station', 'discharge', 'Date', 'issue_date', 'discharge_sum', 'discharge_avg']
        self.assertCountEqual(output.columns, expected_columns)

    def test_output_values(self):
        # Test that the output DataFrame has the expected values
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'datetime', 'station', 'discharge')
        expected_values = [
            ('2022-01-01', 'A', 1, '2022-01-01', np.nan, np.nan, np.nan),
            ('2022-01-02', 'A', 2, '2022-01-02', np.nan, np.nan, np.nan),
            ('2022-01-03', 'A', 3, '2022-01-03', np.nan, np.nan, np.nan),
            ('2022-01-04', 'A', 4, '2022-01-04', np.nan, np.nan, np.nan),
            ('2022-01-05', 'A', 5, '2022-01-05',   True, 9.0, 8.0),
            ('2022-01-06', 'A', 6, '2022-01-06', np.nan, np.nan, np.nan),
            ('2022-01-07', 'A', 7, '2022-01-07', np.nan, np.nan, np.nan),
            ('2022-01-08', 'A', 8, '2022-01-08', np.nan, np.nan, np.nan),
            ('2022-01-09', 'A', 9, '2022-01-09', np.nan, np.nan, np.nan),
            ('2022-01-10', 'A', 10, '2022-01-10',  True, 24.0, 11.0),
            ('2022-01-11', 'A', 11, '2022-01-11', np.nan, np.nan, np.nan),
            ('2022-01-01', 'B', 1, '2022-01-01', np.nan, np.nan, np.nan),
            ('2022-01-02', 'B', 2, '2022-01-02', np.nan, np.nan, np.nan),
            ('2022-01-03', 'B', 3, '2022-01-03', np.nan, np.nan, np.nan),
            ('2022-01-04', 'B', 4, '2022-01-04', np.nan, np.nan, np.nan),
            ('2022-01-05', 'B', 5, '2022-01-05',   True, 9.0, 8.0),
            ('2022-01-06', 'B', 6, '2022-01-06', np.nan, np.nan, np.nan),
            ('2022-01-07', 'B', 7, '2022-01-07', np.nan, np.nan, np.nan),
            ('2022-01-08', 'B', 8, '2022-01-08', np.nan, np.nan, np.nan),
            ('2022-01-09', 'B', 9, '2022-01-09', np.nan, np.nan, np.nan),
            ('2022-01-10', 'B', 10, '2022-01-10',  True, 24.0, 11.0),
            ('2022-01-11', 'B', 11, '2022-01-11', np.nan, np.nan, np.nan)
        ]
        counter = 0
        for i, row in output.iterrows():
            self.assertEqual(math.isnan(row[4]), math.isnan(expected_values[counter][4]))
            self.assertEqual(math.isnan(row[5]), math.isnan(expected_values[counter][5]))
            self.assertEqual(math.isnan(row[6]), math.isnan(expected_values[counter][6]))
            counter += 1

    def test_output_values_with_nan_input(self):
        # Test that the output DataFrame has the expected values
        output = fl.generate_issue_and_forecast_dates(self.data_df_nan, 'datetime', 'station', 'discharge')
        expected_values = [
            ('2022-01-01', 'A', 1, '2022-01-01', np.nan, np.nan, np.nan),
            ('2022-01-02', 'A', 2, '2022-01-02', np.nan, np.nan, np.nan),
            ('2022-01-03', 'A', np.nan, '2022-01-03', np.nan, np.nan, np.nan),
            ('2022-01-04', 'A', np.nan, '2022-01-04', np.nan, np.nan, np.nan),
            ('2022-01-05', 'A', 5, '2022-01-05',   True, 9.0, 8.0),
            ('2022-01-06', 'A', 6, '2022-01-06', np.nan, np.nan, np.nan),
            ('2022-01-07', 'A', 7, '2022-01-07', np.nan, np.nan, np.nan),
            ('2022-01-08', 'A', 8, '2022-01-08', np.nan, np.nan, np.nan),
            ('2022-01-09', 'A', 9, '2022-01-09', np.nan, np.nan, np.nan),
            ('2022-01-10', 'A', np.nan, '2022-01-10',  True, 24.0, 11.0),
            ('2022-01-11', 'A', 11, '2022-01-11', np.nan, np.nan, np.nan),
            ('2022-01-01', 'B', np.nan, '2022-01-01', np.nan, np.nan, np.nan),
            ('2022-01-02', 'B', np.nan, '2022-01-02', np.nan, np.nan, np.nan),
            ('2022-01-03', 'B', 3, '2022-01-03', np.nan, np.nan, np.nan),
            ('2022-01-04', 'B', 4, '2022-01-04', np.nan, np.nan, np.nan),
            ('2022-01-05', 'B', 5, '2022-01-05',   True, 9.0, 8.0),
            ('2022-01-06', 'B', 6, '2022-01-06', np.nan, np.nan, np.nan),
            ('2022-01-07', 'B', np.nan, '2022-01-07', np.nan, np.nan, np.nan),
            ('2022-01-08', 'B', 8, '2022-01-08', np.nan, np.nan, np.nan),
            ('2022-01-09', 'B', 9, '2022-01-09', np.nan, np.nan, np.nan),
            ('2022-01-10', 'B', 10, '2022-01-10',  True, 24.0, 11.0),
            ('2022-01-11', 'B', np.nan, '2022-01-11', np.nan, np.nan, np.nan)
        ]
        counter = 0
        for i, row in output.iterrows():
            self.assertEqual(math.isnan(row[4]), math.isnan(expected_values[counter][4]))
            self.assertEqual(math.isnan(row[5]), math.isnan(expected_values[counter][5]))
            self.assertEqual(math.isnan(row[6]), math.isnan(expected_values[counter][6]))
            counter += 1

    def test_negative_discharge(self):
        # Test that negative discharge values are handled correctly
        self.data_df.loc[3, 'discharge'] = -1
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'datetime', 'station', 'discharge')
        self.assertEqual((output.iloc[4, 5]), 9.0)
        self.assertEqual((output.iloc[4, 6]), 8.0)

    def test_missing_data(self):
        # Test that missing data is handled correctly
        self.data_df = self.data_df.iloc[:5]  # Only keep the first 5 rows
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'datetime', 'station', 'discharge')
        self.assertEqual(len(output), 5)
        self.assertEqual(output['discharge_sum'].tolist()[4], 9.0)
        self.assertTrue(output['issue_date'].tolist()[4])
        self.assertTrue(math.isnan(output['discharge_avg'].tolist()[4]))

    def test_demo_data_test_case(self):
        self.data_df = pd.DataFrame({
            'Date': ['2022-04-26', '2022-04-27', '2022-04-28', '2022-04-29', '2022-04-30',
                     '2022-05-01', '2022-05-02', '2022-05-03', '2022-05-04', '2022-05-05'],
            'Q_m3s': [0.74, 0.74, 0.77, 0.84, 0.80, 0.78, 0.81, 0.85, 0.77, 0.74],
            'Year': [2022]*10,
            'Code': [12256]*10})
        output = fl.generate_issue_and_forecast_dates(self.data_df, 'Date', 'Code', 'Q_m3s')
        print("\n\nDEBUG: test_demo_data_test_case:\n", output)
"""

class TestLoadAllStationDataFromJSON(unittest.TestCase):
    def test_load(self):
        # Test that the output is a pandas DataFrame
        testjsonpath = os.path.join(
            os.path.dirname(__file__),
            'test_data', 'test_config_all_stations_file.json')
        output = fl.load_all_station_data_from_JSON(testjsonpath)
        self.assertIsInstance(output, pd.DataFrame)

        # Test that the output has the expected columns
        expected_columns = ['name_ru', 'river_ru', 'punkt_ru',
                            'name_eng', 'river_eng', 'punkt_eng',
                            'lat', 'long', 'code', 'display_p',
                            'header', 'site_code']
        self.assertCountEqual(output.columns, expected_columns)

        # Test that a ValueError is thrown if the JSON file does not exist
        with self.assertRaises(FileNotFoundError):
            fl.load_all_station_data_from_JSON('not_a_real_file.json')


class TestSite(unittest.TestCase):
    def setUp(self):
        self.site = fl.Site(code='ABC123', name='Site 1', river_name='River A',
                            punkt_name='Punkt B', lat=45.0, lon=-120.0,
                            region='Region X', basin='Basin Y')
        self.df = pd.DataFrame({
            'code': ['15194', '15195', 'ABC123', '15194', '15195', 'ABC123', '15194', '15195', 'ABC123', 'ABC123'],
            'pentad_in_year': ['1', '1', '1', '2', '2', '2', '3', '3', '3', '4'],
            'decad_in_year': ['1', '1', '1', '2', '2', '2', '3', '3', '3', '4'],
            'discharge_avg': [10, 20, 30, 40, 50, 6.5, 70, 80, 0.9123, 103.8]
        })
        # For testing perform_linear_regression
        self.datadf = pd.DataFrame({
            'Code': ['15194', '15195', 'ABC123', '15194', '15195', 'ABC123', '15194', '15195', 'ABC123', 'ABC123'],
            'discharge_sum': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
            'discharge_avg': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        })
        # For testing calculate_forecast_skill
        self.input_data = pd.DataFrame({
            'group_col': ['A', 'A', 'B', 'B'],
            'observation_col': [1.0, 2.0, 3.0, 4.0],
            'simulation_col': [1.1, 1.9, 3.1, 4.2]
        })
        # For testing from_df_get_predictor
        # Create a DataFrame with some sample data
        data = {'Code': ['ABC123', 'DEF', 'ABC123', 'JKL', 'ABC123'],
                'discharge_sum': [1, 2, 3, 4, 5],
                'Date': [datetime.date(2022, 5, 1), datetime.date(2022, 5, 2),
                         datetime.date(2022, 5, 3), datetime.date(2022, 5, 4),
                         datetime.date(2022, 5, 5)]}
        self.df_get_predictor = pd.DataFrame(data)

        self.df_slope_intercept = pd.DataFrame({
            'Code': ['ABC123', 'ABC123'],
            'pentad_in_year': [32, 33],
            'slope': [1.0, 1.0],
            'intercept': [0.0, 0.0]
        })

    def test_from_df_calculate_forecast(self):
        # Test that the method returns the correct forecast value
        pentad = 32
        self.site.predictor = 10.0
        forecast = fl.Site.from_df_calculate_forecast_pentad(self.site, pentad, self.df_slope_intercept)
        self.assertEqual(forecast, 10.0)
        self.assertEqual(self.site.slope, 1.0)
        self.assertEqual(self.site.intercept, 0.0)
        self.assertEqual(self.site.fc_qexp, "10.0")

        pentad = 33
        self.site.predictor = 10.0
        forecast = fl.Site.from_df_calculate_forecast_pentad(self.site, pentad, self.df_slope_intercept)
        self.assertEqual(forecast, 10.0)
        self.assertEqual(self.site.fc_qexp, "10.0")

    def test_from_df_get_norm_discharge(self):
        site = self.site
        df = self.df
        dfmin = df
        dfmax = df
        result = site.from_df_get_norm_discharge(site, '1', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(result, 30)
        self.assertEqual(site.qnorm, '30.0')
        result = site.from_df_get_norm_discharge(site, '2', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '6.50')
        result = site.from_df_get_norm_discharge(site, '3', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '0.91')
        result = site.from_df_get_norm_discharge(site, '4', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '104')

    def test_from_df_get_norm_discharge_with_valid_data(self):
        site = self.site
        df = self.df
        dfmin = df
        dfmax = df
        result = site.from_df_get_norm_discharge(site, '1', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(result, 30)
        self.assertEqual(site.qnorm, '30.0')
        result = site.from_df_get_norm_discharge(site, '2', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '6.50')
        result = site.from_df_get_norm_discharge(site, '3', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '0.91')
        result = site.from_df_get_norm_discharge(site, '4', df, dfmin, dfmax,
                                                 code_col='code', group_col='pentad_in_year', value_col='discharge_avg')
        self.assertEqual(site.qnorm, '104')

    def test_from_df_get_predictor(self):
        # Test that the method returns the correct predictor value
        predictor_dates = [datetime.datetime(2022, 5, 3, 0, 0, 0)]
        predictor = fl.Site.from_df_get_predictor(self.site, self.df_get_predictor, predictor_dates,
                                                  date_col='Date', code_col='Code',
                                                  predictor_col='discharge_sum')
        self.assertEqual(predictor, 3)

        predictor_dates = [datetime.date(2022, 5, 5)]
        predictor = fl.Site.from_df_get_predictor(self.site, self.df_get_predictor, predictor_dates,
                                                  date_col='Date', code_col='Code',
                                                  predictor_col='discharge_sum')
        self.assertEqual(predictor, 5)

    def test_from_DB_get_dangerous_discharge(self):
        # We do not have a test for this one as I don't know how to set up a
        # fake connection to the DB. We can test that the method returns " "
        # if the connection fails.
        result = fl.Site.from_DB_get_dangerous_discharge(sdk='s', site=self.site)
        self.assertEqual(result, " ")

    def test_from_DB_get_predictor(self):
        # Same problem for testing here as for from_DB_get_dangerous_discharge.
        # We can test that the method returns none if the connection fails.
        result = fl.Site.from_DB_get_predictor_sum(sdk='s', site=self.site,
                                               dates='a')
        self.assertEqual(result, None)

    def test_from_dataframe(self):
        # Create a test DataFrame
        import pandas as pd
        df = pd.DataFrame({
            'site_code': ['ABC123', 'DEF456'],
            'site_name': ['Site 1', 'Site 2'],
            'river_ru': ['River A', 'River B'],
            'punkt_ru': ['Punkt B', 'Punkt C'],
            'latitude': [45.0, 46.0],
            'longitude': [-120.0, -121.0],
            'region': ['Region X', 'Region Y'],
            'basin': ['Basin Y', 'Basin Z']
        })

        # Call the method and check that the list of Site objects is created correctly
        sites = fl.Site.from_dataframe(df)
        self.assertEqual(len(sites), 2)
        self.assertEqual(sites[0].code, 'ABC123')
        self.assertEqual(sites[1].code, 'DEF456')


class TestCalculatePercentage(unittest.TestCase):
    def test_calculate_percentages_norm(self):
        # Test case 1: Normal input
        site1 = fl.Site('1234', 'Site 1', fc_qexp=100.0, qnorm=200.0)
        fl.Site.calculate_percentages_norm(site1)
        assert site1.perc_norm == '50'

        # Test case 2: fc_qexp is 0
        site2 = fl.Site('5678', 'Site 2', fc_qexp=0.0, qnorm=200.0)
        fl.Site.calculate_percentages_norm(site2)
        assert site2.perc_norm == '0'

        # Test case 3: qnorm is 0
        site3 = fl.Site('9012', 'Site 3', fc_qexp=100.0, qnorm=0.0)
        fl.Site.calculate_percentages_norm(site3)
        assert site3.perc_norm == ' '

        # Test case 4: perc_norm is negative
        site4 = fl.Site('3456', 'Site 4', fc_qexp=1200.0, qnorm=200.0)
        fl.Site.calculate_percentages_norm(site4)
        assert site4.perc_norm == ' '

        # Test case 5: perc_norm is greater than 100
        site5 = fl.Site('7890', 'Site 5', fc_qexp=400.0, qnorm=200.0)
        fl.Site.calculate_percentages_norm(site5)
        assert site5.perc_norm == '200'


class TestQrange(unittest.TestCase):
    def test_from_df_get_qrange_discharge(self):
        # Test case 1: Normal input
        site0 = fl.Site('abc', 'Site 0', fc_qexp=20.0)
        site1 = fl.Site('1234', 'Site 1', fc_qexp=100.0)
        site2 = fl.Site('5678', 'Site 2', fc_qexp=200.0)
        df1 = pd.DataFrame({
            'Code': ['1234', '5678', 'abc'],
            'pentad_in_year': ['1', '2', '1'],
            'observation_std0674': [50.0, 20.0, 2.2],
            'sdivsigma': [1.0, 2.0, 3.0],
            'accuracy': [0.54, 0.55, 0.56],
            'absolute_error': [0.0, 0.0, 0.0],
        })
        result0 = fl.Site.from_df_get_qrange_discharge(site0, '1', df1)
        result1 = fl.Site.from_df_get_qrange_discharge(site1, '1', df1)
        result2 = fl.Site.from_df_get_qrange_discharge(site2, '2', df1)
        print('DEBUG: result0 = ', result0)
        print('DEBUG: result1 = ', result1)
        print('DEBUG: result2 = ', result2)
        print('DEBUG: site0.fc_qmin = ', site0.fc_qmin)
        assert site0.fc_qmin == '17.8'
        assert site0.fc_qmax == '22.2'
        assert site1.fc_qmin == '50.0'
        assert site1.fc_qmax == '150'
        assert site2.fc_qmin == '180'
        assert site2.fc_qmax == '220'


class TestGetPredictorDatetimes(unittest.TestCase):
    def test_get_predictor_datetimes(self):
        # Test case 1: Normal input
        test_input_date = '2022-05-10'
        n = 2
        expected_dates = [dt.datetime(2022, 5, 8, 0, 0),
                          dt.datetime(2022, 5, 10, 12, 0)]
        test_dates = fl.get_predictor_datetimes(test_input_date, n)
        assert test_dates == expected_dates


class TestReadDailyDischargeDataFromCSV(unittest.TestCase):
    def setUp(self):
        os.environ["ieasyforecast_intermediate_data_path"] = "iEasyHydroForecast/tests/test_data"
        os.environ["ieasyforecast_daily_discharge_file"] = "daily_discharge_data_test_file.csv"
        self.original_data_path = os.getenv("ieasyforecast_intermediate_data_path")
        self.original_discharge_file = os.getenv("ieasyforecast_daily_discharge_file")

    def test_no_environment_variables(self):
        os.environ.pop("ieasyforecast_intermediate_data_path", None)
        os.environ.pop("ieasyforecast_daily_discharge_file", None)
        with self.assertRaises(EnvironmentError):
            fl.read_daily_discharge_data_from_csv()

    def test_file_does_not_exist(self):

        os.environ["ieasyforecast_intermediate_data_path"] = "/path/that/does/not/exist"
        os.environ["ieasyforecast_daily_discharge_file"] = "file.csv"
        with self.assertRaises(FileNotFoundError):
            fl.read_daily_discharge_data_from_csv()
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyforecast_daily_discharge_file")


    def test_file_exists(self):
        os.environ["ieasyforecast_intermediate_data_path"] = "iEasyHydroForecast/tests/test_data"
        os.environ["ieasyforecast_daily_discharge_file"] = "daily_discharge_data_test_file.csv"
        expected_output = pd.DataFrame({
            'code': [19213, 19213, 19213, 19213, 19213, 19213, 19213, 19213,
                     11162, 11162, 11162, 11162, 11162, 11162, 11162, 11162, 11162, 11162],
            'date': pd.to_datetime(['2000-01-01', '2000-01-02', '2000-01-03',
                                    '2000-01-04', '2000-01-05', '2000-01-06',
                                    '2000-01-07', '2000-01-08', '2024-05-04',
                                    '2024-05-05', '2024-05-06', '2024-05-07',
                                    '2024-05-08', '2024-05-09', '2024-05-10',
                                    '2024-05-11', '2024-05-12', '2024-05-13']),
            'discharge': [1.9, 1.9, 1.9, 1.9, 1.9, 1.9, 1.9, 1.85, 33.293,
                          33.293, 33.293, 34.405, 34.405, 35.535, 35.535, 35.535, 37.849, 37.849]
        })
        expected_output = expected_output.sort_values(by=['code', 'date']).reset_index(drop=True)

        # Cast the code column to string
        expected_output['code'] = expected_output['code'].astype(str)

        actual_output = fl.read_daily_discharge_data_from_csv().reset_index(drop=True)
        assert_frame_equal(actual_output, expected_output)
        os.environ.pop("ieasyforecast_intermediate_data_path")
        os.environ.pop("ieasyforecast_daily_discharge_file")


class TestCalculate3DayDischargeSum(unittest.TestCase):
    def test_calculate_3daydischargesum(self):
        # Test with valid data
        data = {
            'datetime_col': pd.date_range(start='1/1/2022', end='1/31/2022'),
            'discharge_col': np.random.rand(31),
            'issue_date': [True if i % 5 == 0 else False for i in range(31)]
        }
        df = pd.DataFrame(data)
        result = fl.calculate_3daydischargesum(df, 'datetime_col', 'discharge_col')
        self.assertIn('discharge_sum', result.columns)
        self.assertEqual(result['discharge_sum'].dtype, float)

        # Test with non-datetime datetime_col
        df2 = df.copy(deep=True)
        df2['datetime_col'] = range(1, 32)
        with self.assertRaises(TypeError):
            fl.calculate_3daydischargesum(df2, 'datetime_col', 'discharge_col')

        # Test with missing datetime_col
        with self.assertRaises(KeyError):
            fl.calculate_3daydischargesum(df, 'nonexistent_col', 'discharge_col')

        # Test with missing discharge_col
        with self.assertRaises(KeyError):
            fl.calculate_3daydischargesum(df, 'datetime_col', 'nonexistent_col')

        # Test with reproducible data
        data = {
            'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
            'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
        }
        df = pd.DataFrame(data)
        df = fl.add_pentad_issue_date(df, datetime_col='Dates')

        print("\n\nDEBUG: test_calculate_3daydischargesum: df: \n", df.head(40))

        result = fl.calculate_3daydischargesum(df, 'Dates', 'Values')

        print("\n\nDEBUG: test_calculate_3daydischargesum: result: \n", result.head(40))


class TestCalculatePentadalDischargeAvg(unittest.TestCase):
    def test_calculate_pentadaldischargeavg(self):
        # Test with reproducible data
        data = {
            'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
            'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
        }
        df = pd.DataFrame(data)
        df = fl.add_pentad_issue_date(df, datetime_col='Dates')
        result0 = fl.calculate_3daydischargesum(df, 'Dates', 'Values')
        result = fl.calculate_pentadaldischargeavg(result0, 'Dates', 'Values')

        self.assertIn('discharge_avg', result.columns)
        self.assertEqual(result['discharge_avg'].dtype, float)
        # The first 4 values should be NaN
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[0]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[1]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[2]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[3]))
        # The first value that is not NaN should be 8.0
        self.assertEqual(result['discharge_avg'].iloc[4], 8.0)
        # Then we have another 4 NaN values
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[5]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[6]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[7]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[8]))
        # The next value should be 13.0
        self.assertEqual(result['discharge_avg'].iloc[9], 13.0)
        # The last value should be NaN
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[-1]))
        self.assertEqual(result['discharge_avg'].iloc[-7], 28.5)


class TestCalculateDecadalDischargeAvg(unittest.TestCase):
    def test_calculate_decadaldischargeavg(self):
        # Test with reproducible data
        data = {
            'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
            'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day
        }
        df = pd.DataFrame(data)
        df = fl.add_decad_issue_date(df, datetime_col='Dates')
        result = fl.calculate_decadaldischargeavg(df, 'Dates', 'Values')

        self.assertIn('discharge_avg', result.columns)
        self.assertEqual(result['discharge_avg'].dtype, float)
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[0]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[1]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[2]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[3]))
        self.assertEqual(result['discharge_avg'].iloc[9], 15.5)
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[5]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[6]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[7]))
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[8]))
        self.assertEqual(result['discharge_avg'].iloc[19], 26.0)
        self.assertTrue(pd.isna(result['discharge_avg'].iloc[-1]))

        self.assertIn('predictor', result.columns)
        self.assertEqual(result['predictor'].dtype, float)
        self.assertTrue(pd.isna(result['predictor'].iloc[0]))
        self.assertTrue(pd.isna(result['predictor'].iloc[1]))
        self.assertTrue(pd.isna(result['predictor'].iloc[2]))
        self.assertTrue(pd.isna(result['predictor'].iloc[9]))
        self.assertTrue(pd.isna(result['predictor'].iloc[5]))
        self.assertTrue(pd.isna(result['predictor'].iloc[6]))
        self.assertTrue(pd.isna(result['predictor'].iloc[8]))
        self.assertEqual(result['predictor'].iloc[19], 15.5)
        self.assertEqual(result['predictor'].iloc[30], 26.0)
        self.assertEqual(result['predictor'].iloc[-1], 26.0)


class TestDataProcessing(unittest.TestCase):
    def test_generate_issue_and_forecast_dates(self):
        # Calculate expected result:
        # Test with reproducible data
        data = {
            'Dates': pd.date_range(start='1/1/2022', end='12/31/2022'),
            'Values': pd.date_range(start='1/1/2022', end='12/31/2022').day,
            'Stations': ['12345' for i in range(365)]
        }

        forecast_flags = sl.ForecastFlags(pentad=True, decad=True)

        df = pd.DataFrame(data)
        # Make sure we have floats in the Values column
        df['Values'] = df['Values'].astype(float)
        df = fl.add_pentad_issue_date(df, datetime_col='Dates')
        result0 = fl.calculate_3daydischargesum(df, 'Dates', 'Values')
        expected_result = fl.calculate_pentadaldischargeavg(result0, 'Dates', 'Values')

        df_decad = fl.add_decad_issue_date(df, datetime_col='Dates')
        expected_result_decad = fl.calculate_decadaldischargeavg(df_decad, 'Dates', 'Values')

        # Call the function
        result, result_decad = fl.generate_issue_and_forecast_dates(
            df, 'Dates', 'Stations', 'Values', forecast_flags=forecast_flags)

        # DECAD
        self.assertIsInstance(result_decad, pd.DataFrame)
        self.assertIn('issue_date', result_decad.columns)
        self.assertIn('predictor', result_decad.columns)
        self.assertIn('discharge_avg', result_decad.columns)

        temp = pd.DataFrame({'predictor': result_decad['predictor'].values,
                             'expected_predictor': expected_result_decad['predictor'].values,
                             'difference': result_decad['predictor'].values - expected_result_decad['predictor'].values})
        # Drop rows where all 3 columns have NaN
        temp = temp.dropna(how='all')
        # Drop rows where the difference is 0.0
        temp = temp[temp['difference'] != 0.0]
        print("\n\nDEBUG: test_generate_issue_and_forecast_dates: result['pred'] vs expected_result['pred']: \n",
              temp)
        np.testing.assert_array_equal(result_decad['predictor'].dropna().values, expected_result_decad['predictor'].dropna().values)
        np.testing.assert_array_equal(result_decad['discharge_avg'].dropna().values, expected_result_decad['discharge_avg'].dropna().values)

        # PENTAD
        # Check that the result is a DataFrame with the expected columns
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('issue_date', result.columns)
        self.assertIn('discharge_sum', result.columns)
        self.assertIn('discharge_avg', result.columns)
        # Test if there are any NaNs in the Stations column
        self.assertEqual(result['Stations'].isna().sum(), 0)
        self.assertEqual(expected_result['Stations'].isna().sum(), 0)
        # Test if the datatypes are the same
        self.assertEqual(result['Stations'].dtype, expected_result['Stations'].dtype)
        # Test each column separately. Only compare the values in the columns
        # because the indices may be different
        np.testing.assert_array_equal(result['Stations'].values, expected_result['Stations'].values)
        np.testing.assert_array_equal(result['issue_date'].values, expected_result['issue_date'].values)
        # Print discharge_sum from result and expected_result next to each other in a
        # DataFrame to visually inspect the values. Also add a column with the difference
        # between the two columns.
        temp = pd.DataFrame({'discharge_sum': result['discharge_sum'].values,
                             'expected_discharge_sum': expected_result['discharge_sum'].values,
                             'difference': result['discharge_sum'].values - expected_result['discharge_sum'].values})
        # Drop rows where all 3 columns have NaN
        temp = temp.dropna(how='all')
        # Drop rows where the difference is 0.0
        temp = temp[temp['difference'] != 0.0]
        #print("\n\nDEBUG: test_generate_issue_and_forecast_dates: result['discharge_sum'] vs expected_result['discharge_sum']: \n",
        #      temp)
        np.testing.assert_array_equal(result['discharge_sum'].dropna().values, expected_result['discharge_sum'].dropna().values)
        np.testing.assert_array_equal(result['discharge_avg'].dropna().values, expected_result['discharge_avg'].dropna().values)






if __name__ == '__main__':
    unittest.main()
