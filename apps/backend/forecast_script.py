# Python 3.10

# I/O
import logging
import os
import sys

# ieasyreports, installed with
# pip install git+https://github.com/hydrosolutions/ieasyreports.git@main
from ieasyreports.settings import Settings


# SDK library for accessing the DB, installed with
# pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk
from ieasyhydro_sdk.sdk import IEasyHydroSDK

from src import config, data_processing, output_generation, forecasting

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)


# Configure the logging level and formatter
logging.basicConfig(level=os.getenv("log_level"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a file handler to write logs to a file
file_handler = logging.FileHandler(os.getenv("log_file"))
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def main():
    # configuration
    start_date, forecast_flags = config.parse_command_line_args()
    env_file_path = config.load_environment()
    bulletin_date = config.get_bulletin_date(start_date) # First day for which the forecast is valid.

    # data processing
    # - set up
    ieh_sdk = IEasyHydroSDK()  # ieasyhydro
    backend_has_access_to_db = data_processing.check_database_access(ieh_sdk)
    # - identify sites for which to produce forecasts
    #   reading sites from DB and config files
    db_sites = data_processing.get_db_sites(ieh_sdk, backend_has_access_to_db)
    #   writing sites information to as list of Site objects
    fc_sites = data_processing.get_fc_sites(ieh_sdk, backend_has_access_to_db, db_sites)
    # - identify dates for which to aggregate predictor data
    predictor_dates = data_processing.get_predictor_datetimes(start_date, forecast_flags)
    # Read discharge data from excel and iEasyHydro database
    modified_data = data_processing.get_station_data(ieh_sdk, backend_has_access_to_db, start_date, fc_sites)

    forecast_pentad_of_year = data_processing.get_forecast_pentad_of_year(bulletin_date)
    data_processing.save_discharge_avg(modified_data, fc_sites, forecast_pentad_of_year)

    # modelling
    # The linear regression is performed on past data. Here, the slope and
    # intercept of the linear regression model are calculated for each site for
    # the current forecast.
    result_df = forecasting.perform_linear_regression(modified_data, forecast_pentad_of_year)

    # forecasting
    # - get predictor from the complete data and write it to site.predictor
    forecasting.get_predictor(modified_data, start_date, fc_sites, ieh_sdk, backend_has_access_to_db, predictor_dates.pentad)
    forecasting.perform_forecast(fc_sites, forecast_pentad_of_year, result_df)
    result2_df = forecasting.calculate_forecast_boundaries(result_df, fc_sites, forecast_pentad_of_year)

    # output generation
    settings = Settings(_env_file=env_file_path)  # ieasyreports
    output_generation.write_hydrograph_data(modified_data)
    output_generation.write_forecast_bulletin(settings, start_date, bulletin_date, fc_sites)
    output_generation.write_forecast_sheets(settings, start_date, bulletin_date, fc_sites, result2_df)


# region Main
'''
The main forecasting script.

It is run once a day on the 5th, 10th, 15th, 20th, 25th and last day of each
month. It produces a forecast bulletin for the next pentad.

The script is run with the following command:
python forecast.py <date>
It is typically called from one of either files run_offline_mode.py or
run_online_mode.py to run it in offline or online mode respectively.

The online mode is used to produce the forecast bulletin for the current day.
It requires access to the database.

The offline mode is used to produce hindcasts. It is based on daily data read
from both the database and excel sheets. It can be configured to run on excel-
based data only to avoid network interruptions.

You may need to install the library iEasyHydroForcast before running this script
using the command (-e is for installing the iEasyHydroLibrary library in
editable mode):
    $ pip install -e ./iEasyHydroForecast
'''
if __name__ == "__main__":
    main()
