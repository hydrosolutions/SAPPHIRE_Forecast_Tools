import datetime
import subprocess

# Get today's date in YYYY-MM-DD format
today = datetime.date.today().strftime("%Y-%m-%d")

# Call main.py with today's date as a command-line argument
# Add the file as argument to forecast.py can identify if it is called from 
# run_offline_mode.py or from run_online_mode.py
subprocess.run(["python", "forecast_script.py", today, __file__])