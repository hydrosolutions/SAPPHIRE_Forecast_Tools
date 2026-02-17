import datetime as dt

import pandas as pd
import os
import time
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def forecast_target_date(forecast_date):
    """Map forecast production date to target period start date.

    Short-term forecasts store `date` as the production date (last day
    of the previous period). The target period starts on `date + 1`.

    Note: Long-term forecasts use `valid_from` / `valid_to` directly
    and do not need this mapping.

    Args:
        forecast_date: A datetime.date, datetime.datetime, or
            pandas Series of datetimes.

    Returns:
        Same type as input, shifted forward by one day.
    """
    return forecast_date + dt.timedelta(days=1)


class TimingStats:
    def __init__(self):
        self.timings = {}
        self.start_times = {}

    def start(self, section):
        self.start_times[section] = time.time()

    def end(self, section):
        if section in self.start_times:
            elapsed = time.time() - self.start_times[section]
            if section not in self.timings:
                self.timings[section] = []
            self.timings[section].append(elapsed)
            del self.start_times[section]

    def summary(self):
        results = []
        total_time = 0
        for section, times in self.timings.items():
            total = sum(times)
            total_time += total
            avg = total / len(times) if times else 0
            results.append({
                'section': section,
                'total_time': total,
                'avg_time': avg,
                'calls': len(times)
            })

        # Sort by total time
        results.sort(key=lambda x: x['total_time'], reverse=True)

        # Calculate percentages
        for result in results:
            result['percentage'] = (
                (result['total_time'] / total_time) * 100
                if total_time else 0
            )

        return results, total_time


@contextmanager
def timer(stats, section):
    if stats is not None:
        stats.start(section)
    try:
        yield
    finally:
        if stats is not None:
            stats.end(section)

def log_most_recent_forecasts_pentad(modelled_data):
    """
    Extract and log the most recent forecast for each module (model).
    Creates a CSV file with stations as rows and models as columns,
    showing all possible combinations even when no forecast exists.

    Args:
        modelled_data (pd.DataFrame): DataFrame containing the modelled forecast data

    Returns:
        pd.DataFrame: Pivoted DataFrame with stations as rows and models as columns
    """
    # Check if modelled_data is empty
    if modelled_data.empty:
        logger.warning("No forecast data available to log")
        return pd.DataFrame()

    logger.info("\n\n------ Most Recent Forecasts by Module -------")

    # Get the most recent date in the dataset
    most_recent_date = modelled_data['date'].max()
    logger.info(f"Most recent forecast date: {most_recent_date.strftime('%Y-%m-%d')}")

    # Filter data for the most recent date
    recent_forecasts = modelled_data[modelled_data['date'] == most_recent_date]

    # If no forecasts are available for the most recent date, return empty DataFrame
    if recent_forecasts.empty:
        logger.warning(f"No forecasts available for {most_recent_date.strftime('%Y-%m-%d')}")
        return pd.DataFrame()

    # Get all unique station codes and models across the entire dataset
    all_codes = modelled_data['code'].unique()
    all_models = modelled_data['model_short'].unique()

    logger.info(f"Found {len(all_codes)} unique station codes in the entire dataset")
    logger.info(f"Found {len(all_models)} unique models in the entire dataset")

    # Create a pivot table with the recent forecasts
    pivoted_forecasts = pd.pivot_table(
        recent_forecasts,
        values='forecasted_discharge',
        index=['code', 'date', 'pentad_in_month'],
        columns=['model_short'],
        aggfunc='first',  # Take the first value if there are duplicates
        fill_value=None  # Use None for missing values
    )

    # Reset index to make code, date, pentad regular columns
    pivoted_forecasts = pivoted_forecasts.reset_index()

    # Check if there are models in all_models that are not in the pivoted data
    missing_models = set(all_models) - set(pivoted_forecasts.columns[3:])
    if missing_models:
        logger.info(f"Models from historical data not present in recent forecasts: {', '.join(missing_models)}")
        # Add columns for missing models (will be all NaN)
        for model in missing_models:
            pivoted_forecasts[model] = None

    # Check if there are codes in all_codes that are not in the pivoted data
    codes_in_pivot = set(pivoted_forecasts['code'])
    missing_codes = set(all_codes) - codes_in_pivot

    # If there are missing codes, add rows for them with NaN values
    if missing_codes:
        logger.info(f"Found {len(missing_codes)} station codes from historical data not present in recent forecasts")

        # Create rows for missing codes
        missing_rows = []
        for code in missing_codes:
            # Find the most recent pentad for this code in the full dataset
            code_data = modelled_data[modelled_data['code'] == code]
            if not code_data.empty:
                most_recent_code_date = code_data['date'].max()
                # Filter by the most recent date and check if result is non-empty
                # (handles NaT dates or missing data scenarios)
                filtered_by_date = code_data[code_data['date'] == most_recent_code_date]
                if filtered_by_date.empty:
                    logger.warning(f"No data found for code {code} on date {most_recent_code_date}")
                    continue
                pentad = filtered_by_date['pentad_in_month'].iloc[0]

                # Create a row with just the code, date, and pentad
                new_row = {'code': code, 'date': most_recent_date, 'pentad_in_month': pentad}

                # Add NaN (float) for all model columns so dtypes match
                for model in all_models:
                    new_row[model] = float('nan')

                missing_rows.append(new_row)

        # Add the missing rows to the pivoted data if any were created
        if missing_rows:
            missing_df = pd.DataFrame(missing_rows)
            pivoted_forecasts = pd.concat([pivoted_forecasts, missing_df], ignore_index=True)

    # Create directory if it doesn't exist
    forecast_dir = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        "forecast_logs"
    )
    os.makedirs(forecast_dir, exist_ok=True)

    # Save pivoted data to CSV
    forecast_file = os.path.join(
        forecast_dir,
        f"recent_model_forecasts_{most_recent_date.strftime('%Y%m%d')}.csv"
    )
    pivoted_forecasts.to_csv(forecast_file, index=False)

    logger.info(f"Recent forecasts by model saved to: {forecast_file}")
    logger.info(f"Number of stations with forecasts: {len(pivoted_forecasts)}")

    # Log models with forecasts
    if not pivoted_forecasts.empty and len(pivoted_forecasts.columns) > 3:
        logger.info(f"Models with forecasts: {', '.join(pivoted_forecasts.columns[3:])}")

    # Log some sample rows
    '''sample_size = min(5, len(pivoted_forecasts))
    if sample_size > 0:
        logger.info(f"Sample of forecasts (showing {sample_size} stations):")
        for _, row in pivoted_forecasts.head(sample_size).iterrows():
            logger.info(f"  Station {row['code']} (Pentad {int(row['pentad_in_month'])}):")
            for model in pivoted_forecasts.columns[3:]:  # Skip code, date, pentad columns
                if pd.notna(row[model]):
                    logger.info(f"    {model}: {row[model]:.2f}")
                else:
                    logger.info(f"    {model}: No forecast available")'''

    return pivoted_forecasts

def log_most_recent_forecasts_monthly(modelled_data):
    """Extract and log the most recent monthly forecast for each model.

    Creates a CSV file with stations as rows and models as columns,
    showing the most recent (year, month) combination.

    Args:
        modelled_data: DataFrame with monthly joint forecast data.
            Expected columns: code, year, month_in_year, model_short,
            forecasted_discharge.

    Returns:
        pd.DataFrame: Pivoted DataFrame with stations as rows and
        models as columns.
    """
    if modelled_data is None or modelled_data.empty:
        logger.warning("No monthly forecast data available to log")
        return pd.DataFrame()

    logger.info(
        "\n\n------ Most Recent Monthly Forecasts by Module -------"
    )

    # Find most recent (year, month) combination
    if 'year' not in modelled_data.columns:
        logger.warning("No 'year' column in monthly forecast data")
        return pd.DataFrame()

    max_year = int(modelled_data['year'].max())
    latest_year_data = modelled_data[
        modelled_data['year'] == max_year
    ]
    if latest_year_data.empty:
        logger.warning("No data for most recent year %d", max_year)
        return pd.DataFrame()

    max_month = int(latest_year_data['month_in_year'].max())
    logger.info(
        "Most recent monthly forecast: year=%d, month=%d",
        max_year, max_month,
    )

    # Filter to most recent (year, month)
    recent = modelled_data[
        (modelled_data['year'] == max_year)
        & (modelled_data['month_in_year'] == max_month)
    ]

    if recent.empty:
        logger.warning(
            "No forecasts for year=%d month=%d", max_year, max_month
        )
        return pd.DataFrame()

    # Pivot: rows=code, columns=model_short, values=forecasted_discharge
    # joint_forecasts may have q50 (from API) or forecasted_discharge
    # (from EM calculation); use whichever is available.
    value_col = 'forecasted_discharge'
    if value_col not in recent.columns and 'q50' in recent.columns:
        recent = recent.copy()
        recent['forecasted_discharge'] = recent['q50']
    pivoted = pd.pivot_table(
        recent,
        values='forecasted_discharge',
        index=['code', 'year', 'month_in_year'],
        columns=['model_short'],
        aggfunc='first',
        fill_value=None,
    )
    pivoted = pivoted.reset_index()

    # Create directory if it doesn't exist
    forecast_dir = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        "forecast_logs"
    )
    os.makedirs(forecast_dir, exist_ok=True)

    # Save pivoted data to CSV
    forecast_file = os.path.join(
        forecast_dir,
        f"recent_model_forecasts_monthly_{max_year}{max_month:02d}.csv"
    )
    pivoted.to_csv(forecast_file, index=False)

    logger.info(
        "Recent monthly forecasts saved to: %s", forecast_file
    )
    logger.info(
        "Number of stations with forecasts: %d", len(pivoted)
    )

    return pivoted


def log_most_recent_forecasts_decade(modelled_data):
    """
    Extract and log the most recent forecast for each module (model).
    Creates a CSV file with stations as rows and models as columns,
    showing all possible combinations even when no forecast exists.

    Args:
        modelled_data (pd.DataFrame): DataFrame containing the modelled forecast data

    Returns:
        pd.DataFrame: Pivoted DataFrame with stations as rows and models as columns
    """
    # Check if modelled_data is empty
    if modelled_data.empty:
        logger.warning("No forecast data available to log")
        return pd.DataFrame()

    logger.info("\n\n------ Most Recent Forecasts by Module -------")

    # Get the most recent date in the dataset
    most_recent_date = modelled_data['date'].max()
    logger.info(f"Most recent forecast date: {most_recent_date.strftime('%Y-%m-%d')}")

    # Filter data for the most recent date
    recent_forecasts = modelled_data[modelled_data['date'] == most_recent_date]

    # If no forecasts are available for the most recent date, return empty DataFrame
    if recent_forecasts.empty:
        logger.warning(f"No forecasts available for {most_recent_date.strftime('%Y-%m-%d')}")
        return pd.DataFrame()

    # Get all unique station codes and models across the entire dataset
    all_codes = modelled_data['code'].unique()
    all_models = modelled_data['model_short'].unique()

    logger.info(f"Found {len(all_codes)} unique station codes in the entire dataset")
    logger.info(f"Found {len(all_models)} unique models in the entire dataset")

    # Create a pivot table with the recent forecasts
    pivoted_forecasts = pd.pivot_table(
        recent_forecasts,
        values='forecasted_discharge',
        index=['code', 'date', 'decad_in_month'],
        columns=['model_short'],
        aggfunc='first',  # Take the first value if there are duplicates
        fill_value=None  # Use None for missing values
    )

    # Reset index to make code, date, pentad regular columns
    pivoted_forecasts = pivoted_forecasts.reset_index()

    # Check if there are models in all_models that are not in the pivoted data
    missing_models = set(all_models) - set(pivoted_forecasts.columns[3:])
    if missing_models:
        logger.info(f"Models from historical data not present in recent forecasts: {', '.join(missing_models)}")
        # Add columns for missing models (will be all NaN)
        for model in missing_models:
            pivoted_forecasts[model] = None

    # Check if there are codes in all_codes that are not in the pivoted data
    codes_in_pivot = set(pivoted_forecasts['code'])
    missing_codes = set(all_codes) - codes_in_pivot

    # If there are missing codes, add rows for them with NaN values
    if missing_codes:
        logger.info(f"Found {len(missing_codes)} station codes from historical data not present in recent forecasts")

        # Create rows for missing codes
        missing_rows = []
        for code in missing_codes:
            # Find the most recent decade for this code in the full dataset
            code_data = modelled_data[modelled_data['code'] == code]
            if not code_data.empty:
                most_recent_code_date = code_data['date'].max()
                # Filter by the most recent date and check if result is non-empty
                # (handles NaT dates or missing data scenarios)
                filtered_by_date = code_data[code_data['date'] == most_recent_code_date]
                if filtered_by_date.empty:
                    logger.warning(f"No data found for code {code} on date {most_recent_code_date}")
                    continue
                decade = filtered_by_date['decad_in_month'].iloc[0]

                # Create a row with just the code, date, and decade
                new_row = {'code': code, 'date': most_recent_date, 'decad_in_month': decade}

                # Add NaN (float) for all model columns so dtypes match
                for model in all_models:
                    new_row[model] = float('nan')

                missing_rows.append(new_row)

        # Add the missing rows to the pivoted data if any were created
        if missing_rows:
            missing_df = pd.DataFrame(missing_rows)
            pivoted_forecasts = pd.concat([pivoted_forecasts, missing_df], ignore_index=True)

    # Create directory if it doesn't exist
    forecast_dir = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        "forecast_logs"
    )
    os.makedirs(forecast_dir, exist_ok=True)

    # Save pivoted data to CSV
    forecast_file = os.path.join(
        forecast_dir,
        f"recent_model_forecasts_decade_{most_recent_date.strftime('%Y%m%d')}.csv"
    )
    pivoted_forecasts.to_csv(forecast_file, index=False)

    logger.info(f"Recent forecasts by model saved to: {forecast_file}")
    logger.info(f"Number of stations with forecasts: {len(pivoted_forecasts)}")

    # Log models with forecasts
    if not pivoted_forecasts.empty and len(pivoted_forecasts.columns) > 3:
        logger.info(f"Models with forecasts: {', '.join(pivoted_forecasts.columns[3:])}")

    # Log some sample rows
    '''sample_size = min(5, len(pivoted_forecasts))
    if sample_size > 0:
        logger.info(f"Sample of forecasts (showing {sample_size} stations):")
        for _, row in pivoted_forecasts.head(sample_size).iterrows():
            logger.info(f"  Station {row['code']} (Pentad {int(row['pentad_in_month'])}):")
            for model in pivoted_forecasts.columns[3:]:  # Skip code, date, pentad columns
                if pd.notna(row[model]):
                    logger.info(f"    {model}: {row[model]:.2f}")
                else:
                    logger.info(f"    {model}: No forecast available")'''

    return pivoted_forecasts
