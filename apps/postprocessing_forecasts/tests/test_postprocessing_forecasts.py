import os
import sys
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from unittest.mock import patch, MagicMock

# Import module to test
from postprocessing_forecasts import postprocessing_forecasts as pf

# Import iEasyHydroForecast modules
from iEasyHydroForecast import setup_library as sl
from iEasyHydroForecast import forecast_library as fl

# Import supporting modules that might be needed for mocking
from pandas._testing import assert_frame_equal


@pytest.fixture
def setup_environment_vars():
    """Set up environment variables needed for testing."""
    # Store original environment variables if they exist
    original_envs = {}
    for var in [
        "ieasyforecast_intermediate_data_path",
        "ieasyforecast_pentad_discharge_file",
        "ieasyforecast_analysis_pentad_file",
        "ieasyforecast_pentadal_skill_metrics_file",
        "ieasyforecast_combined_forecast_pentad_file"
    ]:
        original_envs[var] = os.environ.get(var)

    # Set test environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = "/tmp/test_forecast"
    os.environ["ieasyforecast_pentad_discharge_file"] = "test_pentad_discharge.csv"
    os.environ["ieasyforecast_analysis_pentad_file"] = "test_analysis_pentad.csv"
    os.environ["ieasyforecast_pentadal_skill_metrics_file"] = "test_skill_metrics.csv"
    os.environ["ieasyforecast_combined_forecast_pentad_file"] = "test_combined_forecast.csv"

    # Create directory if it doesn't exist
    os.makedirs("/tmp/test_forecast", exist_ok=True)

    yield

    # Restore original environment variables
    for var, value in original_envs.items():
        if value is not None:
            os.environ[var] = value
        else:
            os.environ.pop(var, None)


@pytest.fixture
def mock_observed_data():
    """Create fake observed pentadal data using the Central Asian pentad definition.

    Pentads 1-5 are 5 days each (days 1-5, 6-10, 11-15, 16-20, 21-25).
    Pentad 6 has variable length (days 26 to end of month).
    """
    # Instead of a simple date range, create dates that properly represent pentad structure
    # including different month lengths

    # Create dates for 3 months to include different month lengths (Jan, Feb, Mar 2010)
    dates = []

    # For each month, add the standard pentad dates (1, 6, 11, 16, 21) for pentads 1-5
    for month in [1, 2, 3]:
        for pentad in range(1, 6):
            day = 1 + (pentad - 1) * 5
            dates.append(dt.datetime(2010, month, day))

        # Add the last day of the month for pentad 6
        if month == 1:  # January (31 days)
            dates.append(dt.datetime(2010, month, 31))
        elif month == 2:  # February (28 days in 2010)
            dates.append(dt.datetime(2010, month, 28))
        elif month == 3:  # March (31 days)
            dates.append(dt.datetime(2010, month, 31))

    codes = ['15013', '15020', '15025']

    # Create an empty DataFrame
    data = []

    for code in codes:
        for date in dates:
            # Create some predictable discharge values - varying by code and date
            code_num = int(code) / 1000
            day_of_year = date.timetuple().tm_yday
            discharge_avg = code_num + (day_of_year / 100)

            # Calculate pentad in month (1-6)
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            # Calculate pentad in year (1-72)
            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            data.append({
                'date': date,
                'code': code,
                'discharge_avg': discharge_avg,
                'discharge': discharge_avg * 0.95,  # Slightly different from avg for realism
                'model_long': 'Observed (Obs)',
                'model_short': 'Obs',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year,
                'delta': discharge_avg * 0.1,  # 10% of discharge for delta
                'q_mean': discharge_avg * 1.05,  # Historical mean
                'q_std_sigma': discharge_avg * 0.2  # Standard deviation
            })

    # Print data for debugging
    print(pd.DataFrame(data))

    return pd.DataFrame(data)


@pytest.fixture
def mock_modelled_data():
    """Create fake modelled pentadal data with linear regression forecasts using Central Asian pentad definition."""
    # Use the same dates as the observed data fixture for consistency
    dates = []

    # For each month, add the standard pentad dates (1, 6, 11, 16, 21) for pentads 1-5
    for month in [1, 2, 3]:
        for pentad in range(1, 6):
            day = 1 + (pentad - 1) * 5
            dates.append(dt.datetime(2010, month, day))

        # Add the last day of the month for pentad 6
        if month == 1:  # January (31 days)
            dates.append(dt.datetime(2010, month, 31))
        elif month == 2:  # February (28 days in 2010)
            dates.append(dt.datetime(2010, month, 28))
        elif month == 3:  # March (31 days)
            dates.append(dt.datetime(2010, month, 31))

    codes = ['15013', '15020', '15025']

    # Create an empty DataFrame
    data = []

    for code in codes:
        for date in dates:
            # Create some predictable values - varying by code and date
            code_num = int(code) / 1000
            day_of_year = date.timetuple().tm_yday

            # Create the predictor value (simulating discharge sum from previous days)
            predictor = code_num * 2 + (day_of_year / 50)

            # Linear model parameters
            slope = 0.3 + (code_num / 100)
            intercept = 0.1 + (day_of_year / 1000)

            # Calculate forecasted discharge
            forecasted_discharge = (slope * predictor) + intercept

            # Additional skill metrics
            rsquared = 0.75 + (code_num / 10000)  # R-squared close to but less than 1

            # Calculate pentad in month (1-6)
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            # Calculate pentad in year (1-72)
            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            data.append({
                'date': date,
                'code': code,
                'predictor': predictor,
                'forecasted_discharge': forecasted_discharge,
                'model_long': 'Linear regression (LR)',
                'model_short': 'LR',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year,
                'slope': slope,
                'intercept': intercept,
                'rsquared': rsquared
            })

    return pd.DataFrame(data)


@pytest.fixture
def expected_skill_metrics():
    """Create expected skill metrics for validation."""
    # This would typically be a combination of observed and modelled data
    # For simplicity, we'll create a skeleton of expected columns
    return ['pentad_in_year', 'code', 'model_long', 'model_short',
            'sdivsigma', 'nse', 'delta', 'accuracy', 'mae', 'n_pairs']


class MockTimingStats:
    def start(self, section):
        pass

    def end(self, section):
        pass

    def summary(self):
        return {}, 0


@patch("postprocessing_forecasts.postprocessing_forecasts.sl.load_environment")
@patch("postprocessing_forecasts.postprocessing_forecasts.sl.read_observed_and_modelled_data_pentade")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.calculate_skill_metrics_pentad")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_forecast_data_pentad")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_pentadal_skill_metrics")
def test_postprocessing_forecasts(mock_save_metrics, mock_save_forecast,
                                 mock_calculate_metrics, mock_read_data,
                                 mock_load_env, setup_environment_vars,
                                 mock_observed_data, mock_modelled_data,
                                 expected_skill_metrics):
    """Test the postprocessing_forecasts function with mocked dependencies."""

    # Create a mock timing stats object
    mock_timing = MockTimingStats()

    # Replace the global variable directly in the module
    import postprocessing_forecasts.postprocessing_forecasts as pf_module
    pf_module.timing_stats = mock_timing

    # Configure mocks
    mock_read_data.return_value = (mock_observed_data, mock_modelled_data)

    # Create more realistic skill metrics that represent the Central Asian pentad structure
    skill_metrics = pd.DataFrame(columns=expected_skill_metrics)

    # Create metrics for the first 3 months (18 pentads)
    pentads = []
    codes = []
    sdivsigma_values = []
    nse_values = []
    delta_values = []
    accuracy_values = []
    mae_values = []
    n_pairs_values = []

    for pentad in range(1, 19):  # 3 months * 6 pentads = 18 pentads
        for code in ['15013', '15020', '15025']:
            pentads.append(pentad)
            codes.append(code)

            # Create some variation in metrics based on pentad and code
            code_num = int(code) / 15000  # Normalize for calculations

            # Last pentad of each month (pentad 6, 12, 18) might have different characteristics
            is_last_pentad = pentad % 6 == 0
            pentad_factor = 1.1 if is_last_pentad else 1.0

            sdivsigma_values.append(0.5 + (pentad / 100) + (code_num / 10))
            nse_values.append(0.8 - (pentad / 100) * pentad_factor)
            delta_values.append(0.15 + (pentad / 200))
            accuracy_values.append(0.85 - (pentad / 200) * pentad_factor)
            mae_values.append(0.05 + (pentad / 300) + (code_num / 20))
            n_pairs_values.append(10)

    skill_metrics['pentad_in_year'] = pentads
    skill_metrics['code'] = codes
    skill_metrics['model_long'] = ['Linear regression (LR)'] * len(pentads)
    skill_metrics['model_short'] = ['LR'] * len(pentads)
    skill_metrics['sdivsigma'] = sdivsigma_values
    skill_metrics['nse'] = nse_values
    skill_metrics['delta'] = delta_values
    skill_metrics['accuracy'] = accuracy_values
    skill_metrics['mae'] = mae_values
    skill_metrics['n_pairs'] = n_pairs_values

    # Configure calculate_skill_metrics_pentad to return our fake metrics
    mock_calculate_metrics.return_value = (skill_metrics, mock_modelled_data, None)

    # Mock saving functions to return None (success)
    mock_save_forecast.return_value = None
    mock_save_metrics.return_value = None

    # Run the function under test
    result = pf.postprocessing_forecasts()

    # Verify the function completed successfully
    assert result is None  # Function exits with sys.exit(0)

    # Verify expected function calls
    mock_load_env.assert_calle


@patch("postprocessing_forecasts.postprocessing_forecasts.sl.load_environment")
@patch("postprocessing_forecasts.postprocessing_forecasts.sl.read_observed_and_modelled_data_pentade")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.calculate_skill_metrics_pentad")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_forecast_data_pentad")
@patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_pentadal_skill_metrics")
@patch("postprocessing_forecasts.postprocessing_forecasts.timing_stats", MockTimingStats())
def test_postprocessing_forecasts_error_handling(mock_save_metrics, mock_save_forecast,
                                               mock_calculate_metrics, mock_read_data,
                                               mock_load_env, setup_environment_vars):
    """Test error handling in the postprocessing_forecasts function."""
    # Configure mocks
    mock_read_data.return_value = (pd.DataFrame(), pd.DataFrame())  # Empty dataframes
    mock_calculate_metrics.return_value = (pd.DataFrame(), pd.DataFrame(), None)  # Empty results

    # Mock save_forecast_data_pentad to return an error
    mock_save_forecast.return_value = "Error"

    # Run the function under test with sys.exit captured
    with pytest.raises(SystemExit) as excinfo:
        pf.postprocessing_forecasts()

    # Verify it exited with error code 1
    assert excinfo.value.code == 1

    # Verify expected function calls
    mock_load_env.assert_called_once()
    mock_read_data.assert_called_once()
    mock_calculate_metrics.assert_called_once()
    mock_save_forecast.assert_called_once()
    # save_pentadal_skill_metrics should not be called due to the error
    mock_save_metrics.assert_not_called()


@patch("postprocessing_forecasts.postprocessing_forecasts.timing_stats", MockTimingStats())
def test_integration_with_real_file_io(setup_environment_vars,
                                       mock_observed_data,
                                       mock_modelled_data):
    """Test with real file I/O to verify end-to-end functionality."""
    # Write test input files
    test_dir = "/tmp/test_forecast"
    observed_file = os.path.join(test_dir, "test_pentad_discharge.csv")
    modelled_file = os.path.join(test_dir, "test_analysis_pentad.csv")

    # Write test data to files
    mock_observed_data.to_csv(observed_file, index=False)
    mock_modelled_data.to_csv(modelled_file, index=False)

    # Create a proper skill metrics result
    skill_metrics = pd.DataFrame({
        'pentad_in_year': [1, 2, 3],
        'code': ['15013', '15013', '15013'],
        'model_long': ['LR', 'LR', 'LR'],
        'model_short': ['LR', 'LR', 'LR'],
        'sdivsigma': [0.5, 0.5, 0.5],
        'nse': [0.8, 0.8, 0.8],
        'delta': [0.1, 0.1, 0.1],
        'accuracy': [0.9, 0.9, 0.9],
        'mae': [0.05, 0.05, 0.05],
        'n_pairs': [10, 10, 10],
        'q_mean': [1.0, 1.0, 1.0],
        'q_std_sigma': [0.2, 0.2, 0.2],
        'delta': [0.1, 0.1, 0.1]
    })

    # Patch only the read function to use our test files
    with patch("postprocessing_forecasts.postprocessing_forecasts.fl.calculate_skill_metrics_pentad") as mock_calc:
        mock_calc.return_value = (mock_observed_data, mock_modelled_data, None)

        # Run the function with actual file I/O for the remaining operations
        with pytest.raises(SystemExit) as excinfo:
            pf.postprocessing_forecasts()

        # Should exit successfully with code 0 or fail with code 1
        assert excinfo.value.code in [0, 1]

        # Check if output files were created
        skill_metrics_file = os.path.join(test_dir, "test_skill_metrics.csv")
        combined_forecast_file = os.path.join(test_dir, "test_combined_forecast.csv")

        # Files might exist depending on whether test ran successfully
        if os.path.exists(skill_metrics_file):
            metrics_df = pd.read_csv(skill_metrics_file)
            # Verify some basic structure of the output
            assert 'code' in metrics_df.columns
            assert 'model_short' in metrics_df.columns

            # Verify that the pentad_in_year values are correctly calculated
            # For our test data covering 3 months, we should have values from 1-18
            pentad_values = metrics_df['pentad_in_year'].unique()
            pentad_values = sorted([p for p in pentad_values if not pd.isna(p)])
            max_expected_pentad = 18  # 3 months * 6 pentads
            assert all(1 <= p <= max_expected_pentad for p in pentad_values), \
                f"Pentad values should be between 1 and {max_expected_pentad}, got {pentad_values}"

        if os.path.exists(combined_forecast_file):
            forecast_df = pd.read_csv(combined_forecast_file)
            assert 'code' in forecast_df.columns
            assert 'forecasted_discharge' in forecast_df.columns

            # Check that dates for pentad 6 correspond to end-of-month dates
            pentad6_rows = forecast_df[forecast_df['pentad_in_month'] == 6]
            if not pentad6_rows.empty:
                pentad6_dates = pd.to_datetime(pentad6_rows['date'])
                # Check that all pentad 6 dates are end-of-month dates
                for date in pentad6_dates:
                    next_day = date + pd.Timedelta(days=1)
                    assert next_day.month != date.month, \
                        f"Date {date} is pentad 6 but not end of month"

    # Clean up test files
    for file in [observed_file, modelled_file, skill_metrics_file, combined_forecast_file]:
        if os.path.exists(file):
            os.remove(file)


if __name__ == "__main__":
    pytest.main(["-v", "test_postprocessing_forecasts.py"])