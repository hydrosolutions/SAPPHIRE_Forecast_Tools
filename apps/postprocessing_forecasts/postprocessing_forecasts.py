# region Libraries
import os
import sys
import pandas as pd
import datetime as dt
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from contextlib import contextmanager

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

# region Timing Tools
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
            result['percentage'] = (result['total_time'] / total_time) * 100 if total_time else 0

        return results, total_time

@contextmanager
def timer(stats, section):
    stats.start(section)
    try:
        yield
    finally:
        stats.end(section)
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

# Initialize the timing stats object
timing_stats = TimingStats()

def postprocessing_forecasts():
    global timing_stats

    logger.info(f"\n\n====== Post-processing forecasts =================")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    with timer(timing_stats, 'total execution'):

        with timer(timing_stats, 'setup'):
            logger.info(f"\n\n------ Setting up --------------------------------")
            # Configuration
            sl.load_environment()

        with timer(timing_stats, 'reading data'):
            logger.info(f"\n\n------ Reading observed and modelled data -------")
            # Data processing
            observed, modelled = sl.read_observed_and_modelled_data_pentade()

        with timer(timing_stats, 'calculating skill metrics'):
            logger.info(f"\n\n------ Calculating skill metrics -----------------")
            # Calculate forecast skill metrics, adds ensemble forecast to modelled
            skill_metrics, modelled, timing_stats = fl.calculate_skill_metrics_pentad(
                observed, modelled, timing_stats)
            logger.debug(f"Skill metrics: {skill_metrics.columns}")
            logger.debug(f"Skill metrics: {skill_metrics.tail()}")

        with timer(timing_stats, 'saving results'):
            logger.info(f"\n\n------ Saving results ----------------------")
            # Save the observed and modelled data to CSV files
            ret = fl.save_forecast_data_pentad(modelled)
            if ret is None:
                logger.info(f"Pentadal forecast results for all models saved successfully.")
            else:
                logger.error(f"Error saving the pentadal forecast results.")

            # Save the skill metrics to a CSV file
            ret = fl.save_pentadal_skill_metrics(skill_metrics)

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessin_forecasts:")
    logger.info("Total execution time: {:.2f} seconds".format(total))
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(f"  Total time: {entry['total_time']:.2f} seconds ({entry['percentage']:.1f}%)")
        logger.info(f"  Average time per call: {entry['avg_time']:.2f} seconds")
        logger.info(f"  Number of calls: {entry['calls']}")

    if ret is None:
        logger.info(f"Script finished at {dt.datetime.now()}.")
        sys.exit(0) # Success
    else:
        logger.error(f"Error saving the skill metrics.")
        sys.exit(1)


if __name__ == "__main__":
    # Post-process the forecasts
    postprocessing_forecasts()

