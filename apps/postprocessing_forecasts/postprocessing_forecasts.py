# region Libraries
import os
import sys
import pandas as pd
import datetime as dt
import logging
from logging.handlers import TimedRotatingFileHandler

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl
import forecast_library as fl
import tag_library as tl

# endregion


# region Logging
# Configure the logging level and formatter
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
file_handler = TimedRotatingFileHandler('logs/log', when='midnight',
                                        interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# endregion

def postprocessing_forecasts():

    logger.info(f"\n\n====== Post-processing forecasts =================")
    logger.debug(f"Script started at {dt.datetime.now()}.")
    logger.info(f"\n\n------ Setting up --------------------------------")

    # Configuration
    sl.load_environment()

    logger.info(f"\n\n------ Reading observed and modelled data -------")
    # Data processing
    observed, modelled = sl.read_observed_and_modelled_data_pentade()

    # Save the observed and modelled data to CSV files
    ret = fl.save_forecast_data_pentad(modelled)
    if ret is None:
        logger.info(f"Pentadal forecast results for all models saved successfully.")
    else:
        logger.error(f"Error saving the pentadal forecast results.")

    logger.info(f"\n\n------ Calculating skill metrics -----------------")
    # Calculate forecast skill metrics
    skill_metrics = fl.calculate_skill_metrics_pentad(observed, modelled)

    logger.info(f"\n\n------ Saving results ----------------------")
    # Save the skill metrics to a CSV file
    ret = fl.save_pentadal_skill_metrics(skill_metrics)

    if ret is None:
        logger.info(f"Script finished at {dt.datetime.now()}.")
        sys.exit(0) # Success
    else:
        logger.error(f"Error saving the skill metrics.")
        sys.exit(1)


if __name__ == "__main__":
    # Post-process the forecasts
    postprocessing_forecasts()

