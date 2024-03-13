# This python script reads the last successful run date from a file, identifies
# the date of the last forecast and reruns the forecasts since then. This allows
# the user to rerun the forecasts for a specific period of time. The last
# successful run date is stored in the file ieasyforecast_last_successful_run_file.

import datetime
import os
from dotenv import load_dotenv

def load_environment():
    '''
    Load environment variables from .env or .env_develop file, depending on
    whether the script is running in a docker container or locally.

    Parameters:
    None

    Returns:
    None

    Raises:
    FileNotFoundError: If the .env file does not exist at the specified path.
    '''
    # Check if IN_DOCKER_CONTAINER environment variable is set
    in_docker = os.getenv("IN_DOCKER_CONTAINER")
    # Determine .env file path
    if in_docker == "True":
        print("Running in docker container. Loading environment variables from .env")
        env_file_path = "/app/apps/config/.env"
    else:
        print("Running locally. Loading environment variables from .env_develop")
        env_file_path = "../config/.env_develop"

    # Check if .env file exists
    if not os.path.isfile(env_file_path):
        raise FileNotFoundError(f"No .env file found at {env_file_path}")

    # Load environment variables
    load_dotenv(env_file_path)
    # Print if a file exists at the env_file_path location
    # print(f"File exists at {env_file_path}: {os.path.isfile(env_file_path)}")

def get_last_run_file():
    '''
    Creates a path to the file ieasyforecast_last_successful_run_file in the
    ieasyforecast_intermediate_data_path which are stored in environment
    variables.

    Parameters:
    None

    Returns:
    last_run_file (str): Path to the file ieasyforecast_last_successful_run_file

    Raises:
    EnvironmentError: If the required environment variables are not set.
    FileNotFoundError: If the file does not exist at the constructed path.
    '''
    # Get environment variables
    data_path = os.getenv("ieasyforecast_intermediate_data_path")
    run_file = os.getenv("ieasyforecast_last_successful_run_file")

    # Check if environment variables are set
    if not data_path or not run_file:
        raise EnvironmentError("Required environment variables are not set.")

    # Construct file path
    last_run_file = os.path.join(data_path, run_file)

    # Check if file exists
    if not os.path.isfile(last_run_file):
        raise FileNotFoundError(f"No file found at {last_run_file}")

    return last_run_file

def parse_last_successful_run_date(last_run_file):
    '''
    Reads the last successful run date from the file ieasyforecast_last_successful_run_file.

    Parameters:
    last_run_file (str): Path to the file ieasyforecast_last_successful_run_file

    Returns:
    last_successful_run_date (datetime.date): Last successful run date

    Raises:
    ValueError: If the date in the file is not in the expected format.
    '''
    try:
        with open(last_run_file, "r") as file:
            last_successful_run_date = file.read().strip()
            last_successful_run_date = last_successful_run_date.replace("_", "-")
            try:
                last_successful_run_date = datetime.datetime.strptime(last_successful_run_date, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Date in {last_run_file} is not in the expected format: {last_successful_run_date}")
    except FileNotFoundError:
        print(f"Warning: {last_run_file} not found. Defaulting to yesterday's date.")
        last_successful_run_date = datetime.date.today() - datetime.timedelta(days=1)

    return last_successful_run_date

def calculate_new_forecast_date(last_successful_run_date):
    '''
    Calculates the date to trigger the most recent forecast.

    Parameters:
    last_successful_run_date (datetime.date): Last successful run date

    Returns:
    rerun_forecast_date (datetime.date): Date to trigger the most recent forecast
    '''
    # Identify the date of the last forecast.
    # Forecasts are produced on the 5th, 10th, 15th, 20th, 25th and last day of each
    # month. To re-run a forecast, the last_successful_run_date is set to the date
    # before the last forecast date.
    forecast_days = [27, 22, 17, 12, 7, 2]

    # We need to find the largest day in forecast_days that is smaller than the day
    # of the last successful run date. If the day of the last successful run date is
    # smaller than 5, we need to find the last forecast date of the previous month.
    # We can do this by subtracting 1 from the month and then finding the largest
    # day in forecast_days.
    if last_successful_run_date.day < 5:
        # Go back one month
        rerun_forecast_date = last_successful_run_date.replace(day=1) - datetime.timedelta(days=1)
        # Set the day to the largest day in forecast_days (26)
        rerun_forecast_date = rerun_forecast_date.replace(day=max(forecast_days))
    else:
        # Find the largest day in forecast_days that is smaller than the day of the
        # last successful run date
        rerun_day = max(day for day in forecast_days if day < last_successful_run_date.day)
        rerun_forecast_date = last_successful_run_date.replace(day=rerun_day)

    return rerun_forecast_date

def write_date(date, file_path):
    '''
    Writes the date to the file.

    Parameters:
    date (datetime.date): Date to write to the file
    file_path (str): Path to the file

    Returns:
    None

    Raises:
    TypeError: If the date is not a datetime.date object or if the file_path is not a string.
    IOError: If the file cannot be opened for writing.
    '''
    # Check parameter types
    if not isinstance(date, datetime.date):
        raise TypeError("date must be a datetime.date object.")
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string.")

    # Write date to file
    try:
        with open(file_path, "w") as file:
            file.write(date.strftime("%Y-%m-%d"))
    except IOError as e:
        raise IOError(f"Could not write to file at {file_path}: {e}")

if __name__ == "__main__":

    # Load environment variables
    load_environment()
    # Get path to the last run file
    last_run_file = get_last_run_file()
    # Read last successful run date from file
    last_successful_run_date = parse_last_successful_run_date(last_run_file)
    # Calculate the new forecast date
    rerun_forecast_date = calculate_new_forecast_date(last_successful_run_date)
    # Write the rerun_forecast_date to file
    write_date(rerun_forecast_date, last_run_file)

    print("INFO - Triggered manual re-run of the pentadal forecast with following dates:")
    print("INFO - Previous last successful run date: ", last_successful_run_date)
    print("INFO - Re-run forecast date: ", rerun_forecast_date)

