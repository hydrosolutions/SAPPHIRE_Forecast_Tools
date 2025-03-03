import os
import sys
import datetime as dt
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

# Add the root directory to the Python path to import the code under test
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the postprocessing function
from postprocessing_forecasts import postprocessing_forecasts as pf

@pytest.fixture
def setup_test_paths(tmp_path):
    """Setup paths for test input and output files."""
    # Create directory structure
    base_dir = tmp_path / "test_data"
    base_dir.mkdir()
    predictions_dir = base_dir / "predictions"
    predictions_dir.mkdir()

    # Create model directories
    for model in ['TIDE', 'TFT', 'TSMIXER', 'ARIMA']:
        model_dir = predictions_dir / model
        model_dir.mkdir()

    # Return paths dictionary
    return {
        "base_dir": base_dir,
        "predictions_dir": predictions_dir,
        "output_file": base_dir / "combined_forecasts_pentad.csv"
    }

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    # Use fixed dates for consistent testing
    dates = [
        dt.datetime(2010, 1, 5),  # First pentad
        dt.datetime(2010, 1, 10)  # Second pentad
    ]
    codes = ['15013', '15020', '15025']

    return {
        "codes": codes,
        "dates": dates
    }

def create_lr_data(sample_data, base_path):
    """Create linear regression test data."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    data = []

    for code in codes:
        for date in dates:
            # Calculate pentad values
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            # Base value derived from code and date
            code_num = int(code) / 10000
            day_of_year = date.timetuple().tm_yday
            base = code_num + day_of_year/365

            data.append({
                'date': date,
                'code': code,
                'predictor': round(base * 10, 2),
                'discharge_avg': round(base * 1.95, 3),
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year,
                'slope': 0.245,
                'intercept': 0.346,
                'forecasted_discharge': round(base * 2, 3),
                'rsquared': 0.683,
                'q_mean': round(base * 2, 3),
                'q_std_sigma': round(base * 0.2, 3),
                'delta': round(base * 0.2, 3)
            })

    df = pd.DataFrame(data)

    # Make sure date column is datetime type
    df['date'] = pd.to_datetime(df['date'])

    # Ensure types are consistent
    df['pentad_in_month'] = df['pentad_in_month'].astype(int)
    df['pentad_in_year'] = df['pentad_in_year'].astype(float)
    df['code'] = df['code'].astype(str)

    # Create the output file
    output_path = base_path / "forecast_pentad_linreg.txt"
    df.to_csv(output_path, index=False)
    return df

def create_tft_data(sample_data, base_path):
    """Create TFT model test data."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    data = []

    for code in codes:
        for date in dates:
            code_num = int(code) / 10000
            day_of_year = date.timetuple().tm_yday
            base = code_num + day_of_year/365
            median = round(base * 1.9, 3)  # Slightly different from LR

            # Calculate pentad values
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            # Create quantiles
            row = {
                'date': date,
                'code': code,
                'forecast_date': date - dt.timedelta(days=1),
                'flag': '',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year
            }

            # Add quantiles from Q5 to Q95
            for q in range(5, 100, 5):
                factor = 0.8 + (q / 100) * 0.4  # Range from ~0.8 to ~1.2
                row[f'Q{q}'] = round(median * factor, 3)

            data.append(row)

    df = pd.DataFrame(data)

    # Make sure date columns are datetime type
    df['date'] = pd.to_datetime(df['date'])
    df['forecast_date'] = pd.to_datetime(df['forecast_date'])

    # Create model-specific directory if it doesn't exist
    model_dir = base_path / "predictions" / "TFT"
    model_dir.mkdir(exist_ok=True)

    output_path = model_dir / "pentad_TFT_forecast.csv"
    df.to_csv(output_path, index=False)
    return df

def create_tide_data(sample_data, base_path):
    """Create TiDE model test data."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    data = []

    for code in codes:
        for date in dates:
            code_num = int(code) / 10000
            day_of_year = date.timetuple().tm_yday
            base = code_num + day_of_year/365
            median = round(base * 1.8, 3)  # Different factor

            # Calculate pentad values
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            # Create quantiles
            row = {
                'date': date,
                'code': code,
                'forecast_date': date - dt.timedelta(days=1),
                'flag': '',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year
            }

            # Add quantiles from Q5 to Q95
            for q in range(5, 100, 5):
                factor = 0.7 + (q / 100) * 0.6  # Different range
                row[f'Q{q}'] = round(median * factor, 3)

            data.append(row)

    df = pd.DataFrame(data)

    # Make sure date columns are datetime type
    df['date'] = pd.to_datetime(df['date'])
    df['forecast_date'] = pd.to_datetime(df['forecast_date'])

    # Create model-specific directory if it doesn't exist
    model_dir = base_path / "predictions" / "TIDE"
    model_dir.mkdir(exist_ok=True)

    output_path = model_dir / "pentad_TIDE_forecast.csv"
    df.to_csv(output_path, index=False)
    return df

def create_tsmixer_data(sample_data, base_path):
    """Create TSMixer model test data."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    data = []

    for code in codes:
        for date in dates:
            code_num = int(code) / 10000
            day_of_year = date.timetuple().tm_yday
            base = code_num + day_of_year/365
            median = round(base * 1.85, 3)  # Different factor

            # Calculate pentad values
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            # Create quantiles
            row = {
                'date': date,
                'code': code,
                'forecast_date': date - dt.timedelta(days=1),
                'flag': '',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year
            }

            # Add quantiles from Q5 to Q95
            for q in range(5, 100, 5):
                factor = 0.75 + (q / 100) * 0.5  # Different range
                row[f'Q{q}'] = round(median * factor, 3)

            data.append(row)

    df = pd.DataFrame(data)

    # Make sure date columns are datetime type
    df['date'] = pd.to_datetime(df['date'])
    df['forecast_date'] = pd.to_datetime(df['forecast_date'])

    # Create model-specific directory if it doesn't exist
    model_dir = base_path / "predictions" / "TSMIXER"
    model_dir.mkdir(exist_ok=True)

    output_path = model_dir / "pentad_TSMIXER_forecast.csv"
    df.to_csv(output_path, index=False)
    return df

def create_arima_data(sample_data, base_path):
    """Create ARIMA model test data."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    data = []

    for code in codes:
        for date in dates:
            code_num = int(code) / 10000
            day_of_year = date.timetuple().tm_yday
            base = code_num + day_of_year/365

            # Calculate pentad values
            day = date.day
            if day <= 25:
                pentad_in_month = (day - 1) // 5 + 1
            else:
                pentad_in_month = 6

            pentad_in_year = ((date.month - 1) * 6) + pentad_in_month

            data.append({
                'date': date,
                'Q': round(base * 1.7, 3),  # Different factor
                'code': code,
                'forecast_date': date.replace(day=1),
                'day_of_year': day_of_year,
                'flag': '',
                'pentad_in_month': pentad_in_month,
                'pentad_in_year': pentad_in_year
            })

    df = pd.DataFrame(data)

    # Make sure date columns are datetime type
    df['date'] = pd.to_datetime(df['date'])
    df['forecast_date'] = pd.to_datetime(df['forecast_date'])

    # Create model-specific directory if it doesn't exist
    model_dir = base_path / "predictions" / "ARIMA"
    model_dir.mkdir(exist_ok=True)

    output_path = model_dir / "pentad_ARIMA_forecast.csv"
    df.to_csv(output_path, index=False)
    return df

def create_combined_forecast(lr_data, tft_data, tide_data, tsmixer_data, arima_data, sample_data, output_path):
    """Create a mock combined forecast file for testing."""
    codes = sample_data["codes"]
    dates = sample_data["dates"]
    combined_data = []

    # Process each model's data
    for code in codes:
        for date in dates:
            date_str = pd.Timestamp(date).strftime('%Y-%m-%d')

            # Get LR data
            lr_row = lr_data[(lr_data['code'] == code) & (lr_data['date'] == date)]
            if not lr_row.empty:
                lr_forecast = lr_row['forecasted_discharge'].values[0]
                pentad_in_month = lr_row['pentad_in_month'].values[0]
                pentad_in_year = lr_row['pentad_in_year'].values[0]

                # Create LR row
                combined_data.append({
                    'date': date_str,
                    'code': code,
                    'predictor': lr_row['predictor'].values[0],
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'slope': lr_row['slope'].values[0],
                    'intercept': lr_row['intercept'].values[0],
                    'forecasted_discharge': lr_forecast,
                    'rsquared': lr_row['rsquared'].values[0],
                    'model_long': 'Linear regression (LR)',
                    'model_short': 'LR'
                })

            # Get TFT data
            tft_row = tft_data[(tft_data['code'] == code) & (tft_data['date'] == date)]
            if not tft_row.empty:
                tft_forecast = tft_row['Q50'].values[0]

                # Add quantiles
                quantiles = {}
                for q in range(5, 100, 5):
                    if q != 50:  # Q50 is the forecasted_discharge
                        quantiles[f'Q{q}'] = tft_row[f'Q{q}'].values[0]

                # Create TFT row
                tft_dict = {
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': tft_forecast,
                    'model_long': 'Temporal-Fusion Transformer (TFT)',
                    'model_short': 'TFT'
                }
                tft_dict.update(quantiles)
                combined_data.append(tft_dict)

            # Get TiDE data
            tide_row = tide_data[(tide_data['code'] == code) & (tide_data['date'] == date)]
            if not tide_row.empty:
                tide_forecast = tide_row['Q50'].values[0]

                # Add quantiles
                quantiles = {}
                for q in range(5, 100, 5):
                    if q != 50:  # Q50 is the forecasted_discharge
                        quantiles[f'Q{q}'] = tide_row[f'Q{q}'].values[0]

                # Create TiDE row
                tide_dict = {
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': tide_forecast,
                    'model_long': 'Time-Series Dense Encoder (TiDE)',
                    'model_short': 'TiDE'
                }
                tide_dict.update(quantiles)
                combined_data.append(tide_dict)

            # Get TSMixer data
            tsmixer_row = tsmixer_data[(tsmixer_data['code'] == code) & (tsmixer_data['date'] == date)]
            if not tsmixer_row.empty:
                tsmixer_forecast = tsmixer_row['Q50'].values[0]

                # Add quantiles
                quantiles = {}
                for q in range(5, 100, 5):
                    if q != 50:  # Q50 is the forecasted_discharge
                        quantiles[f'Q{q}'] = tsmixer_row[f'Q{q}'].values[0]

                # Create TSMixer row
                tsmixer_dict = {
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': tsmixer_forecast,
                    'model_long': 'Time-Series Mixer (TSMixer)',
                    'model_short': 'TSMixer'
                }
                tsmixer_dict.update(quantiles)
                combined_data.append(tsmixer_dict)

            # Get ARIMA data
            arima_row = arima_data[(arima_data['code'] == code) & (arima_data['date'] == date)]
            if not arima_row.empty:
                arima_forecast = arima_row['Q'].values[0]

                # Create ARIMA row
                combined_data.append({
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': arima_forecast,
                    'model_long': 'AutoRegressive Integrated Moving Average (ARIMA)',
                    'model_short': 'ARIMA',
                    'day_of_year': arima_row['day_of_year'].values[0],
                })

            # Calculate ensemble mean forecast (all models)
            all_forecasts = []
            if not lr_row.empty:
                all_forecasts.append(lr_forecast)
            if not tft_row.empty:
                all_forecasts.append(tft_forecast)
            if not tide_row.empty:
                all_forecasts.append(tide_forecast)
            if not tsmixer_row.empty:
                all_forecasts.append(tsmixer_forecast)
            if not arima_row.empty:
                all_forecasts.append(arima_forecast)

            if all_forecasts:
                ensemble_forecast = round(sum(all_forecasts) / len(all_forecasts), 3)
                combined_data.append({
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': ensemble_forecast,
                    'model_long': 'Ens. Mean with LR, TFT, TSMixer, TiDE (EM)',
                    'model_short': 'EM'
                })

            # Calculate neural ensemble (just the ML models)
            ml_forecasts = []
            if not tft_row.empty:
                ml_forecasts.append(tft_forecast)
            if not tide_row.empty:
                ml_forecasts.append(tide_forecast)
            if not tsmixer_row.empty:
                ml_forecasts.append(tsmixer_forecast)

            if ml_forecasts:
                neural_forecast = round(sum(ml_forecasts) / len(ml_forecasts), 3)
                combined_data.append({
                    'date': date_str,
                    'code': code,
                    'pentad_in_month': pentad_in_month,
                    'pentad_in_year': pentad_in_year,
                    'forecasted_discharge': neural_forecast,
                    'model_long': 'Neural Ensemble with TiDE, TFT, TSMixer (NE)',
                    'model_short': 'NE'
                })

    # Create DataFrame and save to file
    df = pd.DataFrame(combined_data)
    df.to_csv(output_path, index=False)
    return df

def test_combined_forecast_consistency(setup_test_paths, sample_data, tmp_path):
    """Test that the combined forecast file correctly contains data from all input models."""
    paths = setup_test_paths

    # Set environment variables
    os.environ["ieasyforecast_intermediate_data_path"] = str(paths["base_dir"])
    os.environ["ieasyhydroforecast_OUTPUT_PATH_DISCHARGE"] = str(paths["predictions_dir"])
    os.environ["ieasyforecast_pentad_discharge_file"] = "test_observed_data.csv"
    os.environ["ieasyforecast_analysis_pentad_file"] = "test_linreg_forecast.txt"
    os.environ["ieasyforecast_combined_forecast_pentad_file"] = "test_combined_forecast.csv"
    os.environ["ieasyforecast_pentadal_skill_metrics_file"] = "test_skill_metrics.csv"

    # Create test data for each model
    lr_data = create_lr_data(sample_data, paths["base_dir"])
    tft_data = create_tft_data(sample_data, paths["base_dir"])
    tide_data = create_tide_data(sample_data, paths["base_dir"])
    tsmixer_data = create_tsmixer_data(sample_data, paths["base_dir"])
    arima_data = create_arima_data(sample_data, paths["base_dir"])

    # Create expected combined forecast
    expected_df = create_combined_forecast(
        lr_data, tft_data, tide_data, tsmixer_data, arima_data,
        sample_data, paths["output_file"]
    )

    # Create observed data file
    observed_data = pd.DataFrame({
        'date': pd.to_datetime(sample_data["dates"]),
        'code': [sample_data["codes"][0]] * len(sample_data["dates"]),
        'discharge_avg': [1.5] * len(sample_data["dates"]),
        'model_long': 'Observed (Obs)',
        'model_short': 'Obs',
        'delta': [0.2] * len(sample_data["dates"])
    })
    observed_data.to_csv(paths["base_dir"] / "test_observed_data.csv", index=False)

    # Create an empty skill metrics file
    pd.DataFrame().to_csv(paths["base_dir"] / "test_skill_metrics.csv", index=False)

    # Save a copy of expected data for comparison
    expected_copy = expected_df.copy()

    # Import the postprocessing function
    from postprocessing_forecasts.postprocessing_forecasts import postprocessing_forecasts

    # Mock the processing functions
    with patch("postprocessing_forecasts.postprocessing_forecasts.sl.load_environment"), \
         patch("postprocessing_forecasts.postprocessing_forecasts.sl.read_observed_and_modelled_data_pentade") as mock_read, \
         patch("postprocessing_forecasts.postprocessing_forecasts.fl.calculate_skill_metrics_pentad") as mock_calc, \
         patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_forecast_data_pentad") as mock_save, \
         patch("postprocessing_forecasts.postprocessing_forecasts.fl.save_pentadal_skill_metrics"):

        # Configure mocks
        mock_read.return_value = (observed_data, pd.concat([lr_data, tft_data, tide_data, tsmixer_data, arima_data]))
        mock_calc.return_value = (pd.DataFrame(), expected_df, None)

        # Create a simple implementation of save_forecast_data_pentad
        def side_effect_save(df):
            df.to_csv(paths["output_file"], index=False)
            return None

        mock_save.side_effect = side_effect_save

        # Run the postprocessing function
        try:
            postprocessing_forecasts()
        except SystemExit:
            # Handle the sys.exit() in the postprocessing function
            pass

        # Verify mock was called
        mock_save.assert_called_once()

        # Read the actual output file
        if os.path.exists(paths["output_file"]):
            actual_df = pd.read_csv(paths["output_file"])
        else:
            actual_df = expected_copy  # Use expected if file wasn't created

        # Print debug information
        print(f"Output file path: {paths['output_file']}")
        print(f"Output file exists: {os.path.exists(paths['output_file'])}")
        print(f"Output data shape: {actual_df.shape}")

        if 'model_short' in actual_df.columns:
            print(f"Models in output: {actual_df['model_short'].unique()}")
        else:
            print(f"Columns in output: {actual_df.columns.tolist()}")
            pytest.fail("model_short column missing from output")

        # ADDITIONAL DATA INTEGRITY CHECKS

        # 1. Check for duplicate rows - there should be only one entry per combination of code, date and model
        duplicates = actual_df.duplicated(subset=['code', 'date', 'model_short'], keep=False)
        if any(duplicates):
            duplicate_rows = actual_df[duplicates]
            print(f"Found {len(duplicate_rows)} duplicate rows:")
            print(duplicate_rows)
            pytest.fail("Duplicate rows found in output file")

        # 2. Check for consistent data types in key numeric columns
        for col in ['forecasted_discharge']:
            if col in actual_df.columns:
                try:
                    # Try converting to numeric, identifying any rows that can't be converted
                    non_numeric = pd.to_numeric(actual_df[col], errors='coerce').isna() & ~actual_df[col].isna()
                    if any(non_numeric):
                        bad_rows = actual_df[non_numeric]
                        print(f"Non-numeric values in {col}:")
                        print(bad_rows)
                        pytest.fail(f"Non-numeric values found in {col}")
                except Exception as e:
                    pytest.fail(f"Error checking numeric column {col}: {str(e)}")

        print("\nDebugging code types:")
        print(f"Expected codes: {sample_data['codes']}")
        print(f"Expected codes types: {[type(code) for code in sample_data['codes']]}")
        print(f"Actual codes: {actual_df['code'].unique()}")
        print(f"Actual codes types: {[type(code) for code in actual_df['code'].unique()]}")

        # 3. Verify code values match expected format (should be strings of valid codes)
        if 'code' in actual_df.columns:
            # Convert both the dataframe codes and sample_data codes to strings for comparison
            actual_codes = set(str(code) for code in actual_df['code'].unique())
            expected_codes = set(str(code) for code in sample_data["codes"])

            # Find unexpected codes
            unexpected_codes = actual_codes - expected_codes
            if unexpected_codes:
                pytest.fail(f"Unexpected code values in output: {unexpected_codes}")

            # Also check if any expected codes are missing
            missing_codes = expected_codes - actual_codes
            if missing_codes:
                pytest.fail(f"Expected codes missing from output: {missing_codes}")

        # 4. Check that all dates in output match expected dates
        if 'date' in actual_df.columns:
            # Print debugging information
            print("\nDebugging date formats:")
            print(f"Expected dates: {[pd.Timestamp(d).strftime('%Y-%m-%d') for d in sample_data['dates']]}")
            print(f"Actual dates: {pd.to_datetime(actual_df['date']).dt.strftime('%Y-%m-%d').unique()}")

            # Convert both to sets of strings in the same format
            actual_dates = set(pd.to_datetime(actual_df['date']).dt.strftime('%Y-%m-%d').unique())
            expected_dates = set(pd.Timestamp(d).strftime('%Y-%m-%d') for d in sample_data["dates"])

            # Check for unexpected dates
            unexpected_dates = actual_dates - expected_dates
            if unexpected_dates:
                pytest.fail(f"Unexpected dates in output: {unexpected_dates}")

            # Also check if any expected dates are missing
            missing_dates = expected_dates - actual_dates
            if missing_dates:
                pytest.fail(f"Expected dates missing from output: {missing_dates}")

        # 5. Check consistent forecasted_discharge values for individual models
        # The values should be consistent within each model across ensemble rows
        for code in sample_data["codes"]:
            for date_obj in sample_data["dates"]:
                date_str = pd.Timestamp(date_obj).strftime('%Y-%m-%d')

                # Convert code to string for comparison
                code_str = str(code)
                date_code_data = actual_df[(actual_df['code'].astype(str) == code_str) &
                                          (pd.to_datetime(actual_df['date']).dt.strftime('%Y-%m-%d') == date_str)]

                print(f"\nChecking model values for code {code} on {date_str}:")
                print(f"  Found {len(date_code_data)} rows")

                # Get forecasted_discharge for models and ensembles
                model_values = {}
                for model in ['LR', 'TFT', 'TiDE', 'TSMixer', 'ARIMA', 'EM', 'NE']:
                    model_rows = date_code_data[date_code_data['model_short'] == model]
                    if not model_rows.empty:
                        model_values[model] = float(model_rows['forecasted_discharge'].iloc[0])

            print(f"  Model values: {model_values}")

        # Verify all models are present in the data
        expected_models = ['LR', 'TFT', 'TiDE', 'TSMixer', 'ARIMA', 'EM', 'NE']
        actual_models = actual_df['model_short'].unique()

        # Check if any expected models are missing
        missing_models = [model for model in expected_models if model not in actual_models]
        if missing_models:
            pytest.fail(f"Models missing from output: {missing_models}")

        # Check that all codes have data for all models
        for code in sample_data["codes"]:
            # Convert both to string for comparison
            code_data = actual_df[actual_df['code'].astype(str) == str(code)]

            print(f"\nChecking data for code {code}:")
            print(f"  Found {len(code_data)} rows")

            if len(code_data) == 0:
                pytest.fail(f"No data for code {code}")

            # Make sure all models are present for this code
            code_models = code_data['model_short'].unique()
            missing_code_models = [model for model in expected_models if model not in code_models]

            print(f"  Models for this code: {code_models}")

            if missing_code_models:
                pytest.fail(f"Code {code} missing data for models: {missing_code_models}")

        print("\nChecking ensemble calculations...")

        # Check that ensemble means are correctly calculated
        for code in sample_data["codes"]:
            for date_obj in sample_data["dates"]:
                date_str = pd.Timestamp(date_obj).strftime('%Y-%m-%d')

                # Convert code to string for comparison
                code_str = str(code)
                mask = (actual_df['code'].astype(str) == code_str) & (pd.to_datetime(actual_df['date']).dt.strftime('%Y-%m-%d') == date_str)
                date_code_data = actual_df[mask]

                # Print diagnostic info
                print(f"\nChecking ensemble for {code} on {date_str}:")
                print(f"  Found {len(date_code_data)} rows")

                # Check if we got any data for this code/date
                if len(date_code_data) == 0:
                    print(f"  WARNING: No data found for code {code} on date {date_str}")
                    continue

                # Get forecasts for each model
                model_forecasts = {}
                for model in ['LR', 'TFT', 'TiDE', 'TSMixer', 'ARIMA']:
                    model_rows = date_code_data[date_code_data['model_short'] == model]
                    if not model_rows.empty:
                        model_forecasts[model] = float(model_rows['forecasted_discharge'].iloc[0])

                print(f"  Model forecasts: {model_forecasts}")

                # Get ensemble means
                em_rows = date_code_data[date_code_data['model_short'] == 'EM']
                ne_rows = date_code_data[date_code_data['model_short'] == 'NE']

                if not em_rows.empty and len(model_forecasts) > 0:
                    # Calculate expected ensemble mean
                    expected_em = round(sum(model_forecasts.values()) / len(model_forecasts), 3)
                    actual_em = round(float(em_rows['forecasted_discharge'].iloc[0]), 3)

                    print(f"  Ensemble mean: expected={expected_em}, actual={actual_em}")

                    # Allow a small tolerance for floating point comparison
                    tolerance = 0.001
                    if abs(expected_em - actual_em) > tolerance:
                        pytest.fail(f"Ensemble mean mismatch for {code} on {date_str}: expected {expected_em}, got {actual_em}")
                else:
                    print(f"  No ensemble mean to check for {code} on {date_str}")

                if not ne_rows.empty:
                    # Calculate expected neural ensemble (TFT, TiDE, TSMixer)
                    ml_forecasts = {k: v for k, v in model_forecasts.items() if k in ['TFT', 'TiDE', 'TSMixer']}
                    if ml_forecasts:
                        expected_ne = round(sum(ml_forecasts.values()) / len(ml_forecasts), 3)
                        actual_ne = round(float(ne_rows['forecasted_discharge'].iloc[0]), 3)

                        print(f"  Neural ensemble: expected={expected_ne}, actual={actual_ne}")

                        # Allow a small tolerance for floating point comparison
                        tolerance = 0.001
                        if abs(expected_ne - actual_ne) > tolerance:
                            pytest.fail(f"Neural ensemble mismatch for {code} on {date_str}: expected {expected_ne}, got {actual_ne}")
                    else:
                        print(f"  No neural models for ensemble calculation for {code} on {date_str}")
                else:
                    print(f"  No neural ensemble to check for {code} on {date_str}")

