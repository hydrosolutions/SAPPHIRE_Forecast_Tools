import os
import numpy as np
import pandas as pd
import datetime as dt
from pathlib import Path

class HydroTestDataGenerator:
    """
    Generate synthetic hydrological data for testing forecast postprocessing.

    Creates predictable patterns in data to ensure calculated statistics are easy to verify.
    """

    def __init__(self, base_dir, start_date=None, end_date=None):
        """
        Initialize the test data generator.

        Args:
            base_dir: Directory where test data will be stored
            start_date: Start date for the data (default: 3 years ago)
            end_date: End date for the data (default: today)
        """
        self.base_dir = Path(base_dir)

        # Set date range (default: 3 years)
        if start_date is None:
            self.start_date = dt.datetime.now() - dt.timedelta(days=365*3)
        else:
            self.start_date = pd.to_datetime(start_date)

        if end_date is None:
            self.end_date = dt.datetime.now()
        else:
            self.end_date = pd.to_datetime(end_date)

        # Set site codes
        self.site_codes = ['10001', '10002', '10003']

        # Create directories
        self._create_directories()

    def _create_directories(self):
        """Create required directory structure"""
        # Main directories
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Predictions directory for ML models
        self.pred_dir = self.base_dir / "predictions"
        self.pred_dir.mkdir(exist_ok=True)

        # Model subdirectories
        for model in ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']:
            model_dir = self.pred_dir / model
            model_dir.mkdir(exist_ok=True)

        # Conceptual model results directory
        self.cm_dir = self.base_dir / "conceptual_model_results"
        self.cm_dir.mkdir(exist_ok=True)

    def _generate_pentad_dates(self):
        """Generate list of pentad dates"""
        # Generate a date range
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')

        # convert to dates
        date_range = [dt.datetime.strptime(str(date.date()), '%Y-%m-%d') for date in date_range]

        # Filter for pentad dates (5th, 10th, 15th, 20th, 25th and last day of each month)
        pentad_dates = []
        for date in date_range:
            day = date.day
            # Get last day of month
            next_month = date.replace(day=28) + dt.timedelta(days=4)  # This ensures we're in the next month
            last_day = next_month - dt.timedelta(days=next_month.day)

            if day in [5, 10, 15, 20, 25] or date.date() == last_day.date():
                pentad_dates.append(date)

        return pentad_dates

    def _get_pentad_in_month(self, date):
        """Calculate pentad number within a month (1-6)"""
        day = date.day
        if day <= 5:
            return 1
        elif day <= 10:
            return 2
        elif day <= 15:
            return 3
        elif day <= 20:
            return 4
        elif day <= 25:
            return 5
        else:
            return 6

    def _get_pentad_in_year(self, date):
        """Calculate pentad number within a year (1-72)"""
        return (date.month - 1) * 6 + self._get_pentad_in_month(date)

    def generate_observed_data(self):
        """Generate observed discharge data with clear seasonal patterns"""
        pentad_dates = self._generate_pentad_dates()

        data = []
        for code in self.site_codes:
            # Create base flow with seasonal pattern and some noise
            # Convert code to a number for base magnitude
            base_magnitude = float(code) / 10000  # E.g., 10001 becomes 1.0001

            for date in pentad_dates:
                # Create a seasonal pattern with peak in spring-summer
                day_of_year = date.timetuple().tm_yday
                season_factor = np.sin(2 * np.pi * day_of_year / 365) * 0.5 + 0.5  # 0-1 range

                # Add a year-to-year trend
                year_factor = 1.0 + (date.year - self.start_date.year) * 0.05

                # Add systematic noise for each site
                noise = np.random.normal(0, 0.1)

                # Calculate base discharge with seasonal pattern
                discharge = base_magnitude * 10 * season_factor * year_factor + noise

                # Calculate delta based on time of year (larger in high flow season)
                delta = base_magnitude * season_factor * 0.3

                data.append({
                    'date': date,
                    'code': code,
                    'discharge_avg': round(discharge, 3),
                    'model_long': 'Observed (Obs)',
                    'model_short': 'Obs',
                    'pentad_in_month': self._get_pentad_in_month(date),
                    'pentad_in_year': self._get_pentad_in_year(date),
                    'delta': round(delta, 3)
                })

        # Convert to DataFrame and save
        df = pd.DataFrame(data)
        df.to_csv(self.base_dir / "observed_pentadal_data.csv", index=False)
        print(f"Generated observed data with {len(df)} records")

        return df

    def generate_linreg_forecast(self, observed_df):
        """Generate linear regression forecasts with consistent relationship to observed data"""
        data = []

        for code in self.site_codes:
            # Filter observed data for this code
            site_data = observed_df[observed_df['code'] == code]

            for _, row in site_data.iterrows():
                date = row['date']
                pentad_in_year = row['pentad_in_year']
                discharge_avg = row['discharge_avg']

                # Create a predictor that's related to the discharge but with some difference
                predictor = discharge_avg * 0.9 + np.random.normal(0, 0.1)

                # Create a relationship with known parameters for easy verification
                slope = 1.1  # slightly over-predict
                intercept = -0.1

                # Calculate forecasted discharge
                forecasted = slope * predictor + intercept

                # Calculate error standard deviation, we'll make this predictable
                # by using a function of the pentad_in_year
                q_std_sigma = 0.2 + (pentad_in_year % 10) / 100

                data.append({
                    'date': date,
                    'code': code,
                    'predictor': round(predictor, 3),
                    'discharge_avg': round(discharge_avg, 3),
                    'pentad_in_month': row['pentad_in_month'],
                    'pentad_in_year': pentad_in_year,
                    'slope': slope,
                    'intercept': intercept,
                    'forecasted_discharge': round(forecasted, 3),
                    'q_mean': round(discharge_avg, 3),
                    'q_std_sigma': round(q_std_sigma, 3),
                    'delta': round(0.674 * q_std_sigma, 3),
                    'rsquared': 0.85
                })

        # Convert to DataFrame and save
        df = pd.DataFrame(data)
        df.to_csv(self.base_dir / "linreg_forecast.csv", index=False)
        print(f"Generated linear regression forecasts with {len(df)} records")

        return df

    def generate_ml_model_forecast(self, observed_df, model_name, factor=1.0, bias=0.0):
        """Generate ML model forecasts with specific characteristics"""
        model_info = {
            'TFT': {
                'model_long': 'Temporal-Fusion Transformer (TFT)',
                'model_short': 'TFT'
            },
            'TIDE': {
                'model_long': 'Time-Series Dense Encoder (TiDE)',
                'model_short': 'TiDE'
            },
            'TSMIXER': {
                'model_long': 'Time-Series Mixer (TSMixer)',
                'model_short': 'TSMixer'
            },
            'ARIMA': {
                'model_long': 'AutoRegressive Integrated Moving Average (ARIMA)',
                'model_short': 'ARIMA'
            }
        }

        if model_name not in model_info:
            raise ValueError(f"Unknown model: {model_name}")

        data = []

        for code in self.site_codes:
            # Filter observed data for this code
            site_data = observed_df[observed_df['code'] == code]

            for _, row in site_data.iterrows():
                date = row['date']
                discharge_avg = row['discharge_avg']

                # Generate forecast date (1 day before forecast)
                forecast_date = date - dt.timedelta(days=1)

                # Create forecasted discharge with model-specific characteristics
                forecasted = discharge_avg * factor + bias + np.random.normal(0, 0.15)

                # Create quantiles for probabilistic models
                record = {
                    'date': date,
                    'code': code,
                    'forecast_date': forecast_date,
                    #'pentad_in_month': row['pentad_in_month'],
                    #'pentad_in_year': row['pentad_in_year'],
                    #'model_long': model_info[model_name]['model_long'],
                    #'model_short': model_info[model_name]['model_short'],
                    'forecasted_discharge': round(forecasted, 3)
                }

                # Add quantiles for all models except ARIMA
                if model_name != 'ARIMA':
                    for q in range(5, 100, 5):
                        quantile_factor = 0.7 + (q / 100) * 0.6  # Range from 0.7 to 1.3
                        record[f'Q{q}'] = round(forecasted * quantile_factor, 3)
                    # Drop 'forecasted_discharge'
                    record.pop('forecasted_discharge')

                data.append(record)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Save to appropriate location
        if model_name == 'ARIMA':
            # ARIMA has a different format historically
            arima_df = df[['date', 'code', 'forecast_date', 'forecasted_discharge']].copy()
            arima_df.rename(columns={'forecasted_discharge': 'Q'}, inplace=True)
            arima_df['day_of_year'] = arima_df['date'].dt.dayofyear
            arima_df['flag'] = ''
            arima_df.to_csv(self.pred_dir / model_name / f"pentad_{model_name}_forecast.csv", index=False)
        else:
            df.to_csv(self.pred_dir / model_name / f"pentad_{model_name}_forecast.csv", index=False)

        print(f"Generated {model_name} forecasts with {len(df)} records")

        return df

    def generate_all_ml_forecasts(self, observed_df):
        """Generate forecasts for all ML models with specific characteristics"""
        # TFT slightly overpredicts
        tft_df = self.generate_ml_model_forecast(observed_df, 'TFT', factor=1.05, bias=0.1)

        # TIDE slightly underpredicts
        tide_df = self.generate_ml_model_forecast(observed_df, 'TIDE', factor=0.95, bias=-0.1)

        # TSMIXER is in between
        tsmixer_df = self.generate_ml_model_forecast(observed_df, 'TSMIXER', factor=1.02, bias=0.0)

        # ARIMA is less accurate
        arima_df = self.generate_ml_model_forecast(observed_df, 'ARIMA', factor=1.1, bias=0.2)

        return {
            'TFT': tft_df,
            'TIDE': tide_df,
            'TSMIXER': tsmixer_df,
            'ARIMA': arima_df
        }

    def generate_expected_skill_metrics(self, site_codes, models, pentads_in_year):
        """Generate expected skill metrics that would be calculated by postprocessing"""
        data = []

        for code in site_codes:
            for pentad in pentads_in_year:
                for model in models:
                    # For ensemble models, generate appropriate model_long name
                    if model == 'EM':
                        model_long = "Ens. Mean with LR, TFT, TSMixer, TiDE (EM)"
                    elif model == 'NE':
                        model_long = "Neural Ensemble with TiDE, TFT, TSMixer (NE)"
                    else:
                        # For regular models, use standard names
                        model_map = {
                            'LR': 'Linear regression (LR)',
                            'TFT': 'Temporal-Fusion Transformer (TFT)',
                            'TiDE': 'Time-Series Dense Encoder (TiDE)',
                            'TSMixer': 'Time-Series Mixer (TSMixer)',
                            'ARIMA': 'AutoRegressive Integrated Moving Average (ARIMA)'
                        }
                        model_long = model_map.get(model, model)

                    # Generate metrics with predictable patterns based on inputs
                    # This makes verification easy
                    code_factor = float(code) / 10000  # For site-specific variation
                    pentad_factor = pentad / 72.0  # For seasonal variation

                    # Create model-specific performance patterns
                    # EM and NE are the best, LR is next, ARIMA is the worst
                    if model in ('EM', 'NE'):
                        base_sdivsigma = 0.3
                        base_nse = 0.9
                        base_accuracy = 0.9
                    elif model == 'LR':
                        base_sdivsigma = 0.5
                        base_nse = 0.75
                        base_accuracy = 0.8
                    elif model == 'ARIMA':
                        base_sdivsigma = 0.8
                        base_nse = 0.4
                        base_accuracy = 0.6
                    else:
                        # Other ML models
                        base_sdivsigma = 0.4
                        base_nse = 0.8
                        base_accuracy = 0.85

                    # Calculate metrics with some predictable variation
                    sdivsigma = base_sdivsigma + code_factor + pentad_factor * 0.2
                    nse = base_nse - pentad_factor * 0.1 - code_factor * 0.05
                    delta = 0.2 + code_factor + pentad_factor * 0.1
                    accuracy = base_accuracy - pentad_factor * 0.1
                    mae = 0.1 + code_factor * 2 + pentad_factor * 0.1

                    # Ensure values are in proper ranges
                    sdivsigma = max(0.1, min(0.99, sdivsigma))
                    nse = max(0.1, min(0.99, nse))
                    accuracy = max(0.5, min(0.99, accuracy))

                    data.append({
                        'pentad_in_year': pentad,
                        'code': code,
                        'model_long': model_long,
                        'model_short': model,
                        'sdivsigma': round(sdivsigma, 4),
                        'nse': round(nse, 4),
                        'delta': round(delta, 3),
                        'accuracy': round(accuracy, 4),
                        'mae': round(mae, 4),
                        'n_pairs': 36  # 3 years of data
                    })

        # Convert to DataFrame and save
        df = pd.DataFrame(data)
        df.to_csv(self.base_dir / "expected_skill_metrics.csv", index=False)
        print(f"Generated expected skill metrics with {len(df)} records")

        return df

    def generate_expected_combined_forecasts(self, observed_df, linreg_df, ml_forecasts):
        """Generate expected combined forecast output"""
        # Start with all individual forecasts
        data = []

        # Add LR data
        lr_data = linreg_df.copy()
        lr_data['model_long'] = 'Linear regression (LR)'
        lr_data['model_short'] = 'LR'
        data.append(lr_data)

        # Add ML model data
        for model_name, model_df in ml_forecasts.items():
            data.append(model_df)

        # Concatenate all dataframes
        combined_df = pd.concat(data, ignore_index=True)

        # Generate ensemble means
        # For each unique date, code, pentad combination
        unique_combos = combined_df[['date', 'code', 'pentad_in_month', 'pentad_in_year']].drop_duplicates()

        ensemble_rows = []
        for _, combo in unique_combos.iterrows():
            # Filter for this combination
            combo_data = combined_df[
                (combined_df['date'] == combo['date']) &
                (combined_df['code'] == combo['code']) &
                (combined_df['pentad_in_month'] == combo['pentad_in_month']) &
                (combined_df['pentad_in_year'] == combo['pentad_in_year'])
            ]

            # Calculate ensemble mean
            em_discharge = round(combo_data['forecasted_discharge'].mean(), 3)

            # Calculate neural ensemble mean
            ne_models = ['TFT', 'TiDE', 'TSMixer']
            ne_data = combo_data[combo_data['model_short'].isin(ne_models)]
            if not ne_data.empty:
                ne_discharge = round(ne_data['forecasted_discharge'].mean(), 3)
            else:
                ne_discharge = em_discharge  # Fallback

            # Add ensemble mean row
            ensemble_rows.append({
                'date': combo['date'],
                'code': combo['code'],
                'pentad_in_month': combo['pentad_in_month'],
                'pentad_in_year': combo['pentad_in_year'],
                'model_long': "Ens. Mean with LR, TFT, TSMixer, TiDE (EM)",
                'model_short': "EM",
                'forecasted_discharge': em_discharge
            })

            # Add neural ensemble row
            ensemble_rows.append({
                'date': combo['date'],
                'code': combo['code'],
                'pentad_in_month': combo['pentad_in_month'],
                'pentad_in_year': combo['pentad_in_year'],
                'model_long': "Neural Ensemble with TiDE, TFT, TSMixer (NE)",
                'model_short': "NE",
                'forecasted_discharge': ne_discharge
            })

        # Add ensemble rows to combined data
        ensemble_df = pd.DataFrame(ensemble_rows)
        combined_df = pd.concat([combined_df, ensemble_df], ignore_index=True)

        # Save to file
        combined_df.to_csv(self.base_dir / "expected_combined_forecasts.csv", index=False)
        print(f"Generated expected combined forecasts with {len(combined_df)} records")

        return combined_df

    def generate_all_test_data(self):
        """Generate all test data files in one step"""
        # Generate observed data
        observed_df = self.generate_observed_data()

        # Generate linear regression forecasts
        linreg_df = self.generate_linreg_forecast(observed_df)

        # Generate ML model forecasts
        ml_forecasts = self.generate_all_ml_forecasts(observed_df)

        # Generate expected outputs
        pentads_in_year = sorted(observed_df['pentad_in_year'].unique())
        models = ['LR', 'TFT', 'TiDE', 'TSMixer', 'ARIMA', 'EM', 'NE']

        expected_metrics = self.generate_expected_skill_metrics(self.site_codes, models, pentads_in_year)
        expected_combined = self.generate_expected_combined_forecasts(observed_df, linreg_df, ml_forecasts)

        # Create environment settings
        env_settings = {
            "ieasyforecast_intermediate_data_path": str(self.base_dir),
            "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "predictions",
            "ieasyforecast_pentad_discharge_file": "observed_pentadal_data.csv",
            "ieasyforecast_analysis_pentad_file": "linreg_forecast.csv",
            "ieasyforecast_combined_forecast_pentad_file": "combined_forecasts_pentad.csv",
            "ieasyforecast_pentadal_skill_metrics_file": "pentadal_skill_metrics.csv",
            "ieasyhydroforecast_PATH_TO_RESULT": str(self.cm_dir)
        }

        # Save environment settings
        with open(self.base_dir / "env_settings.txt", "w") as f:
            for key, value in env_settings.items():
                f.write(f"{key}={value}\n")

        return {
            "observed": observed_df,
            "linreg": linreg_df,
            "ml_forecasts": ml_forecasts,
            "expected_metrics": expected_metrics,
            "expected_combined": expected_combined,
            "env_settings": env_settings
        }


def create_test_data(base_dir="tests/test_data/systematic_test"):
    """Create a comprehensive set of test data in the specified directory"""
    generator = HydroTestDataGenerator(base_dir)
    result = generator.generate_all_test_data()

    print(f"\nTest data generation complete. Files created in {base_dir}")
    print(f"Use these environment variables in your test:")
    for key, value in result["env_settings"].items():
        print(f"os.environ['{key}'] = '{value}'")

    return result


if __name__ == "__main__":
    # When run directly, generate test data
    create_test_data()