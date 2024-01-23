import datetime
import subprocess
import sys

# This python script takes start year, month, day and end year, month, day as
# command-line arguments. It then calls forecast_script.py for each day in the 
# time period between start date and end date. 
# Note: do not start the program with dates before 2000-01-01, as the first data
# available in the dataset is from 2000-01-01 and we need a few years of data to 
# build the linear regression.

#=== Call pentadal forecasting with linear regression ===#
# Set the start and end dates
day_start = datetime.date(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
day_end = datetime.date(int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
#day_start = datetime.date(2020, 1, 1)
#day_end = datetime.date(2020, 9, 25)

print("Start date: ", day_start)
print("End date: ", day_end)

# Iterate over the dates
current_day = day_start
while current_day <= day_end:
    # Call main.py with the current date as a command-line argument
    # Add the file as argument to forecast.py can identify if it is called from 
    # run_offline_mode.py or from run_online_mode.py
    subprocess.run(["python", "forecast_script.py", str(current_day), __file__])
    # Increment the current day by one day
    current_day += datetime.timedelta(days=1)