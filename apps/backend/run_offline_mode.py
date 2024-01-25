# This python script takes start year, month, day and end year, month, day as
# command-line arguments. It then calls forecast_script.py for each day in the
# time period between start date and end date.
# Note: do not start the program with dates before 2000-01-01, if the first data
# available in the dataset is from 2000-01-01. We need a few years of data to
# build the linear regression model.

import datetime
import subprocess
import sys
import os
from dotenv import load_dotenv

# Load environment variables
if os.getenv("IN_DOCKER_CONTAINER") == "True":
    print(f"Running in docker container. Loading environment variables from .env")
    env_file_path = "apps/config/.env"
    load_dotenv(env_file_path)
else:
    print(f"Running locally. Loading environment variables from .env_develop")
    env_file_path = "../config/.env_develop"
    load_dotenv(env_file_path)
# Print if a file exists at the env_file_path location
print(f"File exists at {env_file_path}: {os.path.isfile(env_file_path)}")

# Check if the file ieasyforecast_last_successful_run_file is available in
# ieasyforecast_intermediate_data_path.
# If it is available, read the last successful run date from the file
# If it is not available, we set the last successful run date to yesterday
# Should you wish to produce hindcasts for a period of time, you can create the
# file ieasyforecast_last_successful_run_file and set the date to 1 day before
# the start date of the hindcast period.
last_run_file = os.path.join(
    os.getenv("ieasyforecast_intermediate_data_path"),
    os.getenv("ieasyforecast_last_successful_run_file")
    )
try:
    with open(last_run_file, "r") as file:
        last_successful_run_date = file.read()
        last_successful_run_date = datetime.datetime.strptime(last_successful_run_date, "%Y-%m-%d").date()
except FileNotFoundError:
    last_successful_run_date = datetime.date.today() - datetime.timedelta(days=1)

# Check if the forecasts have already been run for today
# If yes, exit the program
# If no, run the forecasts for today
if last_successful_run_date == datetime.date.today():
    print("Forecasts have already been run for today. Exiting the program.")
    sys.exit()

# Set the start and end dates
# Start date is the last successful run date + 1 day
# End date is today
date_start = last_successful_run_date + datetime.timedelta(days=1)
date_end = datetime.date.today()

print("Running the forecast script for the following dates:")
print("Last successful run date: ", last_successful_run_date)
print("Start date for forecasts: ", date_start)
print("End date for forecasts: ", date_end)

# Iterate over the dates
current_day = date_start
while current_day <= date_end:
    # Call main.py with the current date as a command-line argument
    # Add the file as argument to forecast.py can identify if it is called from
    # run_offline_mode.py or from run_online_mode.py
    if os.getenv("IN_DOCKER_CONTAINER") == "True":
        subprocess.run(["python3", "apps/backend/forecast_script.py", str(current_day), __file__])
    else:
        subprocess.run(["python", "forecast_script.py", str(current_day), __file__])
    # Increment the current day by one day
    current_day += datetime.timedelta(days=1)