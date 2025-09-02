# forecast_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055
#

# region load_libraries
import sys
import panel as pn
import pandas as pd
import datetime as dt
from datetime import timedelta

import logging
from logging.handlers import TimedRotatingFileHandler

import holoviews as hv
# Set the default extension
pn.extension('tabulator')

# Local sources
from src.environment import load_configuration
import src.gettext_config as localize
import src.processing as processing
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel
import src.layout as layout
from src.file_downloader import FileDownloader
from src.auth_utils import *

import calendar

from src.gettext_config import _

# Get the absolute path of the directory containing the current script
cwd = os.getcwd()

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
# Test if the forecast dir exists and print a warning if it does not
if not os.path.isdir(forecast_dir):
    raise Exception("Directory not found: " + forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)
# Import the modules from the forecast library
import tag_library as tl

# endregion

# region set up the logger

# Configure the logging level and formatter
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Remove all handlers associated with the logger
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a file handler to write logs to a file
# A new log file is created every <interval> day at <when>. It is kept for <backupCount> days.
file_handler = TimedRotatingFileHandler('logs/log', when='midnight', interval=1, backupCount=30)
file_handler.setFormatter(formatter)

# Create a stream handler to print logs to the console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Get the root logger and add the handlers to it
logger.addHandler(file_handler)
logger.addHandler(console_handler) 

# endregion



# region load_configuration

# Set primary color to be consistent with the icon color
# Set the primary color to be consistent with the icon color
# Trying to fix the issue with the checkbox label not wrapping
# Loads the font-awesome icons (used for the language icon)
pn.extension(
    'tabulator',
    raw_css=[
        ':root { --design-primary-color: #307096; }',
        """
        .checkbox-label {
            white-space: normal !important;
            word-wrap: break-word !important;
            width: 100%; /* Adjust as needed */
        }
        """
    ]
)

# CSS for language widget text color
language_button_css = """
header .bk.pn-widget-button {
    color: #307086 !important;  /* Change text color of button widget */
}
"""

# Inject the custom CSS
pn.config.raw_css.append(language_button_css)


# Get path to .env file from the environment variable
env_file_path = os.getenv("ieasyhydroforecast_env_file_path")

# Load .env file
# Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
# file to use
in_docker_flag = load_configuration(env_file_path)

# Get icon path from config
icon_path = processing.get_icon_path(in_docker_flag)

# The current date is displayed as the title of each visualization.
today = dt.datetime.now()

# Get folder to store visible points for linear regression
# Test if the path to the configuration folder is set
if not os.getenv('ieasyforecast_configuration_path'):
    raise ValueError("The path to the configuration folder is not set.")

# Define the directory to save the data
SAVE_DIRECTORY = os.path.join(
    os.getenv('ieasyforecast_configuration_path'),
    os.getenv('ieasyforecast_linreg_point_selection', 'linreg_point_selection')
)
os.makedirs(SAVE_DIRECTORY, exist_ok=True)

BULLETIN_CSV_PATH = os.path.join(SAVE_DIRECTORY, 'bulletin.csv')  # Added for CSV storage

# Set time until user is logged out automatically
minutes_inactive_until_logout = int(os.getenv('ieasyforecast_minutes_inactive_until_logout', 10))
INACTIVITY_TIMEOUT = timedelta(minutes=minutes_inactive_until_logout)
last_activity_time = None
# endregion



# region localization
# We'll use pn.state.location to get query parameters from the URL
# This allows us to pass the selected language via the URL

# Default language from .env file
default_locale = os.getenv("ieasyforecast_locale", "en_CH")
print(f"INFO: Default locale: {default_locale}")

# Get the language from the URL query parameter
selected_language = pn.state.location.query_params.get('lang', default_locale)
print(f"INFO: Selected language: {selected_language}")

# Localization, translation to different languages.
localedir = os.getenv("ieasyforecast_locale_dir")
print(f"DEBUG: Translation directory: {localedir}")
print(f"DEBUG: Translation directory exists: {os.path.exists(localedir)}")
print(f"DEBUG: Files in translation directory: {os.listdir(localedir)}")

# Set the locale directory in the translation manager
localize.translation_manager.set_locale_dir(locale_dir=localedir)

# Set the current language
localize.translation_manager.language = selected_language

# Load translations globally
localize.translation_manager.load_translation_forecast_dashboard()
print(f"DEBUG: Selected language: {selected_language}")
print(f"DEBUG: Translation manager language: {localize.translation_manager.language}")

print(f"DEBUG: Translation test - 'Hydropost:' translates to: {_('Hydropost:')}")
print(f"DEBUG: Translation test - 'Forecast' translates to: {_('Forecast')}")

# Check for cached translations
if hasattr(pn.state, 'cache') and 'translations' in pn.state.cache:
    print(f"DEBUG: Cached translations found.")

# Import visualization module after setting up localization
import src.vizualization as viz

# Print forecast horizon variable from environment
sapphire_forecast_horizon = os.getenv('sapphire_forecast_horizon')
if sapphire_forecast_horizon:
    print(f"INFO: Forecast horizon: {sapphire_forecast_horizon}")
else:
    print("WARNING: Forecast horizon not set. Assuming default 'decad'.")
    sapphire_forecast_horizon = 'decad'

# Initialize the downloader with a specific directory
bulletin_folder = os.path.join(
    os.getenv('ieasyreports_report_output_path'),
    'bulletins', sapphire_forecast_horizon)
downloader = FileDownloader(bulletin_folder)
bulletin_download_panel = downloader.panel()


# endregion

# region load_data

# Find out which forecasts have to be displayed
display_ML_forecasts = os.getenv('ieasyhydroforecast_run_ML_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')
display_CM_forecasts = os.getenv('ieasyhydroforecast_run_CM_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')

# If both display_ML_forecasts and display_CM_forecasts are False, we don't need 
# to display data gateway results. 
if not display_ML_forecasts and not display_CM_forecasts:
    display_weather_data = False
if display_ML_forecasts == False and display_CM_forecasts == False:
    display_weather_data = False
else:
    display_weather_data = True

print(f"Display ML forecasts: {display_ML_forecasts}")
print(f"Display CM forecasts: {display_CM_forecasts}")
print(f"Display weather data: {display_weather_data}")

def get_directory_mtime(directory_path):
    mtimes = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            filepath = os.path.join(root, file)
            mtimes.append(os.path.getmtime(filepath))
    # Combine mtimes into a single value, e.g., take the maximum
    combined_mtime = max(mtimes) if mtimes else 0
    return combined_mtime

def load_data():
    global hydrograph_day_all, hydrograph_pentad_all, linreg_predictor, \
        forecasts_all, forecast_stats, all_stations, station_dict, station_df, \
        station_list, linreg_datatable, stations_iehhf, \
        rram_forecast, ml_forecast, rain, temp, latest_data_is_current_year
    # File modification times
    hydrograph_day_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))
    hydrograph_day_file_mtime = os.path.getmtime(hydrograph_day_file)

    hydrograph_pentad_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))
    hydrograph_pentad_file_mtime = os.path.getmtime(hydrograph_pentad_file)

    linreg_forecast_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file"))
    linreg_forecast_file_mtime = os.path.getmtime(linreg_forecast_file)

    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_pentad_file"))
    # Append _latest to the filename to get the latest file
    forecast_results_file = forecast_results_file.replace('.csv', '_latest.csv')
    forecast_results_file_mtime = os.path.getmtime(forecast_results_file)

    forecast_stats_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentadal_skill_metrics_file"))
    forecast_stats_file_mtime = os.path.getmtime(forecast_stats_file)

    if display_CM_forecasts == True:
        # For rram and ml forecasts, get the directory modification times
        rram_file_path = os.getenv('ieasyhydroforecast_PATH_TO_RESULT')
        rram_file_mtime = get_directory_mtime(rram_file_path)

    if display_ML_forecasts == True:
        ml_forecast_dir = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE"))
        ml_forecast_mtime = get_directory_mtime(ml_forecast_dir)
        
    if display_weather_data == True:
        # Rainfall and temperature data files
        p_file = os.path.join(
            os.getenv('ieasyhydroforecast_PATH_TO_HIND'),
            os.getenv('ieasyhydroforecast_FILE_CF_HIND_P'))
        t_file = os.path.join(
            os.getenv('ieasyhydroforecast_PATH_TO_HIND'),
            os.getenv('ieasyhydroforecast_FILE_CF_HIND_T')
        )
        # replace .csv with _dashboard.csv
        p_file = p_file.replace('.csv', '_dashboard.csv')
        t_file = t_file.replace('.csv', '_dashboard.csv')
        p_file_mtime = os.path.getmtime(p_file)
        t_file_mtime = os.path.getmtime(t_file)

    # Hydrograph day and pentad data
    hydrograph_day_all = processing.read_hydrograph_day_data_for_pentad_forecasting(
        stations_iehhf, hydrograph_day_file_mtime)
    # print head and tail of hydrograph_day_all where code == 15194
    #print(f"DEBUG: forecast_dashboard.py: hydrograph_day_all head:\n{hydrograph_day_all[hydrograph_day_all['code'] == '15194'].head()}")
    #print(f"DEBUG: forecast_dashboard.py: hydrograph_day_all tail:\n{hydrograph_day_all[hydrograph_day_all['code'] == '15194'].tail()}")

    hydrograph_pentad_all = processing.read_hydrograph_pentad_data_for_pentad_forecasting(
        stations_iehhf, hydrograph_pentad_file_mtime)

    # Daily forecasts from RRAM & ML models
    if display_CM_forecasts == True:
        rram_forecast = processing.read_rram_forecast_data(rram_file_mtime)
    if display_ML_forecasts == True:
        ml_forecast = processing.read_ml_forecast_data(ml_forecast_mtime)
        
    # Pentadal forecast data
    # - linreg_predictor: for displaying predictor in predictor tab
    # Set multi-index for faster lookups
    linreg_predictor = processing.read_linreg_forecast_data(
        stations_iehhf, linreg_forecast_file_mtime)
    # Print tail of linreg_predictor where code == '15149'
    #print(f"DEBUG: forecast_dashboard.py: linreg_predictor tail:\n{linreg_predictor[linreg_predictor['code'] == '15149'].tail()}")
    # - forecast results from all models
    # Reads in latest forecast results (_latest.csv) for all models
    forecasts_all = processing.read_forecast_results_file(
        stations_iehhf, forecast_results_file_mtime)
    # Forecast statistics
    forecast_stats = processing.read_forecast_stats_file(
        stations_iehhf, forecast_stats_file_mtime)

    # Hydroposts metadata
    station_list, all_stations, station_df, station_dict = processing.read_all_stations_metadata_from_iehhf(
        hydrograph_day_all['code'].unique().tolist())
    #station_list, all_stations, station_df, station_dict = processing.read_all_stations_metadata_from_file(
    #    hydrograph_day_all['code'].unique().tolist())
    if not station_list:
        raise ValueError("The station list is empty. Please check the data source and ensure it contains valid stations.")
    #print("DEBUG: forecast_dashboard.py: Station list:\n", station_list)
    #print("DEBUG: forecast_dashboard.py: All stations: \n", all_stations)
    #logger.debug(f"DEBUG: forecast_dashboard.py: Station list:\n{station_list}")
    #print(f"DEBUG: columns of all_stations:\n{all_stations.columns}")
    #logger.debug(f"DEBUG: forecast_dashboard.py: All stations:\n{all_stations}")
    #print(f"DEBUG: forecast_dashboard.py: Station dataframe:\n{station_df}")
    #print(f"DEBUG: forecast_dashboard.py: Station dictionary:\n{station_dict}")
    #print(f"DEBUG: First dictionary entry: {next(iter(station_dict))}")
    #print(f"DEBUG: First station: {station_dict[next(iter(station_dict))][0]}")

    # Add the station_labels column to the hydrograph_day_all DataFrame
    hydrograph_day_all = processing.add_labels_to_hydrograph(hydrograph_day_all, all_stations)
    hydrograph_pentad_all = processing.add_labels_to_hydrograph(hydrograph_pentad_all, all_stations)

    # Define a flag to indicate if the latest data in hydrograph_pentad_all is for the current year
    current_year_temp = dt.datetime.now().year
    if str(current_year_temp) in hydrograph_pentad_all.columns:
        latest_data_is_current_year = True
    else:
        latest_data_is_current_year = False
    del current_year_temp

    #print(f"DEBUG: linreg_predictor raw: {linreg_predictor.tail()}")
    linreg_predictor = processing.add_labels_to_forecast_pentad_df(linreg_predictor, all_stations)
    #print(f"DEBUG: linreg_predictor with labels: {linreg_predictor.tail()}")
    #print(f"DEBUG: forecast_dashboard.py: linreg_predictor tail:\n{linreg_predictor[linreg_predictor['code'] == '15149'].tail()}")
    linreg_datatable = processing.shift_date_by_n_days(linreg_predictor, 1)
    #print(f"DEBUG: forecast_dashboard.py: linreg_predictor tail:\n{linreg_predictor[linreg_predictor['code'] == '15149'].tail()}")
    #print(f"DEBUG: linreg_datatable.columns: {linreg_datatable.columns}")
    #print(f"DEBUG: linreg_datatable: {linreg_datatable.tail()}")
    #print(f"DEBUG: linreg_predictor.columns: {linreg_predictor.columns}")
    #print(f"DEBUG: linreg_predictor: {linreg_predictor.tail()}")
    forecasts_all = processing.add_labels_to_forecast_pentad_df(forecasts_all, all_stations)
    #print('column names of forecasts_all:\n', forecasts_all.columns)
    #print('\n\nhead of forecasts_all:\n', forecasts_all.head(), '\n\n')
    # Determine the horizon (pentad or decad)
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    horizon_in_year = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
    #print('horizon_in_year_col:', horizon_in_year)

    # Find the latest year in the data
    latest_year = forecasts_all['date'].dt.year.max()
    #print('latest_year:', latest_year)

    # Filter the DataFrame to include only the latest year
    forecasts_latest_year = forecasts_all[forecasts_all['date'].dt.year == latest_year]

    # Find the maximum pentad_in_year or decad_in_year value in the latest year
    latest_horizon_in_year = forecasts_latest_year[horizon_in_year].max()
    #print('latest_horizon_in_year:', latest_horizon_in_year)

    # Filter the DataFrame to include only the latest pentad/decad in the latest year
    forecasts_current = forecasts_latest_year[forecasts_latest_year[horizon_in_year] == latest_horizon_in_year]
    #print('forecasts_current:\n', forecasts_current)
    
    # Get valid codes and normalize to string integers reliably
    valid_codes = (
        pd.to_numeric(forecasts_current['code'], errors='coerce')
          .dropna()
          .astype('int64')
          .astype(str)
          .tolist()
    )
    print('valid_codes:', valid_codes)

    # Normalize 'code' columns in related DataFrames so filtering is consistent
    def _normalize_code_col(df):
        if df is None or 'code' not in df.columns:
            return df
        df = df.copy()
        df['code'] = pd.to_numeric(df['code'], errors='coerce')
        df = df[df['code'].notna()].copy()
        df['code'] = df['code'].astype('int64').astype(str)
        return df

    forecasts_all = _normalize_code_col(forecasts_all)
    forecast_stats = _normalize_code_col(forecast_stats)
    station_df = _normalize_code_col(station_df)
    all_stations = _normalize_code_col(all_stations)

    # Filter forecasts_all and station_list based on valid codes (if any)
    if len(valid_codes) > 0:
        forecasts_all = forecasts_all[forecasts_all['code'].isin(valid_codes)]
        forecast_stats = forecast_stats[forecast_stats['code'].isin(valid_codes)]
        station_df = station_df[station_df['code'].isin(valid_codes)]
        all_stations = all_stations[all_stations['code'].isin(valid_codes)]
    print('station_list prior: ', station_list)
    # Update station_list after potential filtering
    station_list = station_df['station_labels'].unique().tolist()
    print('station_dict prior: ', station_dict)
    # Create a new station_dict that only includes stations in station_list
    new_station_dict = {}
    for basin, stations in station_dict.items():
        # Only filter if we have valid_codes; otherwise keep prior stations
        if len(valid_codes) > 0:
            new_stations = [s for s in stations if any(code in s for code in valid_codes)]  # Corrected line
        else:
            new_stations = stations
        if new_stations:  # Only add the basin if there are valid stations
            new_station_dict[basin] = new_stations
    station_dict = new_station_dict

    print('station_dict after: ', station_dict)
    #print('Updated data:')
    #print('forecasts_all:\n', forecasts_all)
    #print('station_list:', station_list)
    #print('station_df:\n', station_df)
    
    if display_CM_forecasts == True: 
        rram_forecast = processing.add_labels_to_forecast_pentad_df(rram_forecast, all_stations)
    else: 
        rram_forecast = None
    if display_ML_forecasts == True:
        ml_forecast = processing.add_labels_to_forecast_pentad_df(ml_forecast, all_stations)
    else: 
        ml_forecast = None

    # Replace model names with translation strings
    forecasts_all = processing.internationalize_forecast_model_names(_, forecasts_all)
    forecast_stats = processing.internationalize_forecast_model_names(_, forecast_stats)

    # Merge forecast stats with forecasts by code and pentad_in_year and model_short
    #print(f'\n\nhead of forecasts_all:\n{forecasts_all.head()}')
    #print(f'column names of forecasts_all:\n{forecasts_all.columns}')
    #print(f'type of forecasts_all[pentad_in_year]: {forecasts_all[horizon_in_year].dtype}')
    #print(f'\n\nhead of forecast_stats:\n{forecast_stats.head()}')
    #print(f'type of forecast_stats[pentad_in_year]: {forecast_stats[horizon_in_year].dtype}')
    forecasts_all = forecasts_all.merge(
        forecast_stats,
        on=['code', horizon_in_year, 'model_short', 'model_long'],
        how='left',
        suffixes=('', '_stats'))

    # weather data
    if display_weather_data == True:
        rain = processing.read_rainfall_data(p_file_mtime)  # (max(hind_p_file_mtime, cf_p_file_mtime))
        temp = processing.read_temperature_data(t_file_mtime)  # max(hind_t_file_mtime, cf_t_file_mtime))
    else: 
        rain = None
        temp = None

    # Debugging prints to see if we have read data correctly
    #print(f"--- display_weather_data: {display_weather_data}")
    #print(f"--- rain: {rain}")
    #print(f"--- display_ML_forecasts: {display_ML_forecasts}")
    #print(f"--- ml_forecast: {ml_forecast}")
    #print(f"--- display_CM_forecasts: {display_CM_forecasts}")
    #print(f"--- rram_forecast: {rram_forecast}")

stations_iehhf = None

load_data()


# Test if we have sites in stations_iehhf which are not present in forecasts_all
# Placeholder for a message pane
message_pane = pn.pane.Markdown("", width=300)
if stations_iehhf is not None:
    missing_sites = set(stations_iehhf) - set(forecasts_all['code'].unique())
    if missing_sites:
        missing_sites_message = f"_('WARNING: The following sites are missing from the forecast results:') {missing_sites}. _('No forecasts are currently available for these sites. Please make sure your forecast models are configured to produce results for these sites, re-run hindcasts manually and re-run the forecast.')"
        message_pane.object = missing_sites_message

# Add message to message_pane, depending on the status of recent data availability
if not latest_data_is_current_year:
    message_pane.object += "\n\n" + _("WARNING: The latest data available is not for the current year. Forecast Tools may not have access to iEasyHydro. Please contact the system administrator.")

sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)

hydropost_to_basin = {site.code: site.basin_ru for site in sites_list}

bulletin_table = pn.Column()

bulletin_sites = []

tabs_container = pn.Column()

# Create a dictionary of the model names and the corresponding model labels
model_dict_all = forecasts_all[['model_short', 'model_long']] \
    .drop_duplicates() \
    .set_index('model_long')['model_short'].to_dict()
#print(f"DEBUG: forecast_dashboard.py: station_dict: {station_dict}")

pentads = [
    f"{i+1}{_('st pentad of')} {calendar.month_name[month]}" if i == 0 else
    f"{i+1}{_('nd pentad of')} {calendar.month_name[month]}" if i == 1 else
    f"{i+1}{_('rd pentad of')} {calendar.month_name[month]}" if i == 2 else
    f"{i+1}{_('th pentad of')} {calendar.month_name[month]}"
    for month in range(1, 13) for i in range(6)
]

# Create a dictionary mapping each pentad description to its pentad_in_year value
pentad_options = {f"{i+1}{_('st pentad of')} {calendar.month_name[month]}" if i == 0 else
                  f"{i+1}{_('nd pentad of')} {calendar.month_name[month]}" if i == 1 else
                  f"{i+1}{_('rd pentad of')} {calendar.month_name[month]}" if i == 2 else
                  f"{i+1}{_('th pentad of')} {calendar.month_name[month]}": i + (month-1)*6 + 1
                  for month in range(1, 13) for i in range(6)}

# Create a dictionary mapping each decade description to its decad_in_year value
decad_options = {
    f"{i+1}{_('st decade of')} {calendar.month_name[month]}" if i == 0 else
    f"{i+1}{_('nd decade of')} {calendar.month_name[month]}" if i == 1 else
    f"{i+1}{_('rd decade of')} {calendar.month_name[month]}" if i == 2 else
    f"{i+1}{_('th decade of')} {calendar.month_name[month]}": i + (month - 1) * 3 + 1
    for month in range(1, 13) for i in range(3)
}
# endregion


# region widgets

# Widget for date selection, always visible
forecast_date = linreg_predictor['date'].max().date()  # Dates here refer to the forecast issue day, i.e. 1 day before the first day of the forecast pentad.
date_picker = pn.widgets.DatePicker(name=_("Select date:"),
                                    start=dt.datetime((forecast_date.year-1), 1, 5).date(),
                                    end=forecast_date,
                                    value=forecast_date)


# Get the last available date in the data
last_date = linreg_predictor['date'].max() + dt.timedelta(days=1)

# Determine the corresponding pentad
current_pentad = tl.get_pentad_for_date(last_date)
# The forecast is produced on the day before the first day of the forecast
# pentad, therefore we add 1 to the forecast pentad in linreg_predictor to get
# the pentad of the forecast period.
horizon = os.getenv("sapphire_forecast_horizon", "pentad")
horizon_in_year = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
forecast_horizon_for_saving_bulletin = int(linreg_predictor[horizon_in_year].tail(1).values[0]) + 1
forecast_year_for_saving_bulletin = last_date.year

# Get information for bulletin headers into a dataframe that can be passed to
# the bulletin writer.
bulletin_header_info = processing.get_bulletin_header_info(last_date, sapphire_forecast_horizon)
#print(f"DEBUG: forecast_dashboard.py: bulletin_header_info:\n{bulletin_header_info}")

# Create the dropdown widget for pentad selection
pentad_selector = pn.widgets.Select(
    name=_("Select Pentad"),
    options=pentad_options,
    value=current_pentad,  # Default to the last available pentad
    margin=(0, 0, 0, 0)
)

current_decad = tl.get_decad_for_date(last_date)
# Create the dropdown widget for decad selection
decad_selector = pn.widgets.Select(
    name=_("Select Decad"),
    options=decad_options,
    value=current_decad,  # Default to the last available decad
    margin=(0, 0, 0, 0)
)
print(f"   dbg: current_pentad: {current_pentad}")
print(f"   dbg: current_decad: {current_decad}")

# Widget for station selection, always visible
#print(f"\n\nDEBUG: forecast_dashboard.py: station select name string: {_('Select discharge station:')}\n\n")
#station = layout.create_station_selection_widget(station_dict)
_default_station_value = None
if station_dict:
    try:
        _default_station_value = station_dict[next(iter(station_dict))][0]
    except Exception:
        _default_station_value = None
station = pn.widgets.Select(
    name=_("Select discharge station:"),
    groups=station_dict if station_dict else {_("No stations available"): []},
    value=_default_station_value,
    margin=(0, 0, 0, 0)
    )

# Print the station widget selection
#print(f"DEBUG: forecast_dashboard.py: Station widget selection: {station.value}")

# Update the model_dict with the models we have results for for the selected
# station
print("DEBUG: forecast_dashboard.py: station.value: ", station.value)
model_dict = processing.update_model_dict_date(model_dict_all, forecasts_all, station.value, date_picker.value)
print(f"DEBUG: forecast_dashboard.py: model_dict: {model_dict}")
# Model dict can be empty if no forecasts at all are available for the selected station


#@pn.depends(station, pentad_selector, watch=True)
def get_best_models_for_station_and_pentad(station_value, pentad_value, decad_value):
    return processing.get_best_models_for_station_and_pentad(forecasts_all, station_value, pentad_value, decad_value)
current_model_pre_selection = get_best_models_for_station_and_pentad(station.value, pentad_selector.value, decad_selector.value)

print(f"DEBUG: forecast_dashboard.py: model_dict: \n{model_dict}")
print(f"DEBUG: forecast_dashboard.py: current_model_pre_selection: \n{current_model_pre_selection}")

# Widget for forecast model selection, only visible in forecast tab
# a given hydropost/station.
model_checkbox = pn.widgets.CheckBoxGroup(
    name=_("Select forecast model:"),
    options=model_dict,
    value=[],
    #value=[model_dict[model] for model in current_model_pre_selection],
    #width=200,  # 280
    margin=(0, 0, 0, 0),
    sizing_mode='stretch_width',
    css_classes=['checkbox-label']
)
# Add models to value list safely
for model in current_model_pre_selection:
    if model in model_dict:
        model_checkbox.value.append(model_dict[model])
    elif "Neural Ensemble" in model:
        # Find any Neural Ensemble model in the dictionary
        ensemble_key = next((k for k in model_dict.keys() if "Neural Ensemble" in k), None)
        if ensemble_key:
            model_checkbox.value.append(model_dict[ensemble_key])
    # Skip models that can't be found
print(f"\n\n\nmodel_checkbox: {model_checkbox.value}\n\n\n")

allowable_range_label = pn.pane.Markdown(
    _("Select forecast range (for both summary table and figures):"),
    styles={"white-space": "normal"},  # force wrapping
    margin=(0, 0, -5, 0)
)
allowable_range_selection = pn.widgets.Select(
    options=[_("delta"), _("Manual range, select value below"), _("min[delta, %]")],
    value=_("delta"),
    margin=(0, 0, 0, 0)
)
manual_range = pn.widgets.IntSlider(
    name=_("Manual range (%)"),
    start=0,
    end=100,
    value=20,
    step=1,
    margin=(20, 0, 0, 0)  # martin=(top, right, bottom, left)
)
manual_range.visible = False


def draw_show_forecast_ranges_widget():
    return pn.widgets.RadioButtonGroup(
        name=_("Show ranges in figure:"),
        options=[_("Yes"), _("No")],
        value=_("No"))
show_range_button = draw_show_forecast_ranges_widget()

show_daily_data_widget = pn.widgets.RadioButtonGroup(
    name=_("Show daily data:"),
    options=[_("Yes"), _("No")],
    value=_("No")
)

selected_indices = pn.widgets.CheckBoxGroup(
    name=_("Select Data Points:"),
    options={str(i): i for i in range(len(linreg_datatable))},
    value=[],
    width=280,
    margin=(0, 0, 0, 0)
)

# Write bulletin button
write_bulletin_button = pn.widgets.Button(
    name=_("Write bulletin"),
    button_type='primary',
    description=_("Write bulletin to Excel"))
#indicator = pn.indicators.LoadingSpinner(value=False, size=25)

# Button to remove selected forecasts from the bulletin
remove_bulletin_button = pn.widgets.Button(
    name=_("Remove Selected"),
    button_type='danger',
    margin=(10, 0, 0, 0)  # top, right, bottom, left
)


# Create language selection buttons as links that reload the page with the selected language
def create_language_buttons():
    buttons = []
    for lang_name, lang_code in {'English': 'en_CH', 'Русский': 'ru_KG', 'Кыргызча': 'ky_KG'}.items():
        # Create a hyperlink styled as a button
        href = pn.state.location.pathname + f'?lang={lang_code}'

        current_user = check_current_user()

        if current_user:
            # Log language change before redirecting
            log_user_activity(current_user, 'language_change')

        link = f'<a href="{href}" style="margin-right: 10px; padding: 5px 10px; background-color: white; color: #307086; text-decoration: none; border-radius: 4px;">{lang_name}</a>'
        buttons.append(link)
    # Combine the links into a single Markdown pane
    return pn.pane.Markdown(' '.join(buttons))

language_buttons = create_language_buttons()
language_buttons.visible = False  # Initially hidden

# Put the message into a card with visibility set to off is the message is empty
message_card = pn.Card(
    message_pane,
    title=_("Messages:"),
    width_policy='fit',
    width=station.width
)

# Create a single Tabulator instance
forecast_tabulator = pn.widgets.Tabulator(
    theme='bootstrap',
    show_index=False,
    selection=[],
    selectable='checkbox-single',
    sizing_mode='stretch_both',
    height=None
)

basin_names = list(station_dict.keys())
basin_names.insert(0, _("All basins"))  # Add 'Select all basins' as the first option

# Create the 'Select Basin' widget
select_basin_widget = pn.widgets.Select(
    name=_("Select basin:"),
    options=basin_names,
    value=_("All basins"),  # Default value
    margin=(0, 0, 0, 0)
)

# endregion

# region forecast_card

update_forecast_button = pn.widgets.Button(name="Apply changes", button_type="success")
# Forecast card for sidepanel
forecast_model_title = pn.pane.Markdown(
    _("Select forecast model (for figures only):"), margin=(0, 0, -15, 0))  # margin=(top, right, bottom, left)
range_selection_title = pn.pane.Markdown(
    _("Show ranges (for figures only):"), margin=(0, 0, -15, 0))
forecast_card = pn.Card(
    pn.Column(
        allowable_range_label,
        allowable_range_selection,
        manual_range,
        pn.layout.Divider(),
        forecast_model_title,
        model_checkbox,
        range_selection_title,
        show_range_button,
        update_forecast_button
    ),
    title=_('Forecast configuration:'),
    width_policy='fit', width=station.width,
    collapsed=False
)
# Initially hide the card
forecast_card.visible = False


# Pentad card
pentad_card = pn.Card(
    pn.Column(
        pentad_selector
        ),
    title=_('Pentad:'),
    width_policy='fit', width=station.width,
    collapsed=False
)

pentad_card.visible = False


station_card = pn.Card(
    pn.Column(
        station
    ),
    title=_('Hydropost:'),
    width_policy='fit',
    width=station.width,
    collapsed=False
)
station_card.visible = True

# Basin card
basin_card = pn.Card(
    pn.Column(
        select_basin_widget
        ),
    title=_('Select basin:'),
    width_policy='fit', width=station.width,
    collapsed=False
)

basin_card.visible = False


add_to_bulletin_button = pn.widgets.Button(name=_("Add to bulletin"), button_type="primary")

# Initialize the bulletin_tabulator as a global Tabulator with predefined columns and grouping
bulletin_tabulator = pn.widgets.Tabulator(
    value=pd.DataFrame(columns=[
        _('Hydropost'), _('Model'), _('Basin'),
        _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
        _('δ'), _('s/σ'), _('MAE'), _('Accuracy')
    ]),
    theme='bootstrap',
    configuration={
        'columns': [
            {'field': 'station_label', 'title': _('Hydropost')},
            {'field': 'model_short', 'title': _('Model')},
            {'field': 'basin_ru', 'title': _('Basin')},
            {'field': 'forecasted_discharge', 'title': _('Forecasted discharge')},
            {'field': 'fc_lower', 'title': _('Forecast lower bound')},
            {'field': 'fc_upper', 'title': _('Forecast upper bound')},
            {'field': 'delta', 'title': _('δ')},
            {'field': 'sdivsigma', 'title': _('s/σ')},
            {'field': 'mae', 'title': _('MAE')},
            {'field': 'accuracy', 'title': _('Accuracy')},
        ],
        'columnFilters': True  # Enable column filtering if needed
    },
    show_index=False,
    height=300,
    selectable='checkbox',  # Allow multiple selections for removal
    sizing_mode='stretch_width',
    groupby=[_('Basin')],  # Enable grouping by 'Basin'
    layout='fit_columns'
)
# endregion

#endregion


# region update_functions
@pn.depends(pentad_selector, decad_selector, watch=True)
def update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_,
    sites=sites_list, df=hydrograph_pentad_all, pentad=pentad_selector.value, decad=decad_selector.value):
    """Update site attributes with hydrograph statistics for selected pentad"""
    #print(f"\n\n\nDEBUG update_site_attributes_with_hydrograph_statistics_for_selected_pentad: pentad: {pentad}")
    #print(f"column names: {df.columns}")
    # Based on column names and date, figure out which column indicates the
    # last year's Q for the selected pentad
    current_year = dt.datetime.now().year
    #print(f"current year: {current_year}")
    if str(current_year) in df.columns:
        last_year_column = str(current_year - 1)
    else:
        logger.info(f"Column for current year not found. Trying previous year.")
        current_year -= 1
        if str(current_year) in df.columns:
            last_year_column = str(current_year - 1)
        else:
            current_year -= 1
            if str(current_year) in df.columns:
                last_year_column = str(current_year - 1)
            else:
                raise ValueError("No column found for last year's Q.")
    #print(f"\n\nupdate site attributes hydrograph stats: dataframe: {df}")
    # Filter the df for the selected pentad
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_value = pentad
    else:
        horizon_in_year = "decad_in_year"
        horizon_value = decad
    df = df[df[horizon_in_year] == horizon_value].copy()
    # Add a column with the site code
    df['site_code'] = df['station_labels'].str.split(' - ').str[0]
    for site in sites:
        #print(f"site: {site.code}")
        # Test if site.code is in df['site_code']
        if site.code not in df['site_code'].values:
            site.hydrograph_mean = None
            continue
        # Get the hydrograph statistics for each site
        row = df[df['site_code'] == site.code]
        site.hydrograph_mean = row['mean'].values[0]
        site.hydrograph_norm = row['norm'].values[0]
        site.hydrograph_max = row['max'].values[0]
        site.hydrograph_min = row['min'].values[0]
        site.last_year_q_pentad_mean = row[last_year_column].values[0]
        #print(f"site: {site.code}, mean: {site.hydrograph_mean}, max: {site.hydrograph_max}, min: {site.hydrograph_min}, last year mean: {site.last_year_q_pentad_mean}")

    #print(f"Updated sites with hydrograph statistics from DataFrame.")
    return sites


@pn.depends(pentad_selector, watch=True)
def update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=linreg_predictor, pentad=pentad_selector.value, decad=decad_selector.value):
    """Update site attributes with linear regression predictor"""
    # Print pentad
    #print(f"\n\nDEBUGGING update_site_attributes_with_linear_regression_predictor: pentad: {pentad}")
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_value = pentad
    else:
        horizon_in_year = "decad_in_year"
        horizon_value = decad
    # Get row in def for selected pentad
    df = df[df[horizon_in_year] == (horizon_value - 1)].copy()
    #print("\n\nDEBUGGING update_site_attributes_with_linear_regression_predictor")
    #print(f"linreg_predictor: \n{df[df['code'] == '15149']}.tail()")
    # Only keep the last row for each site
    #df = df.drop_duplicates(subset='code', keep='last')
    df = df.sort_values('date').groupby('code').last().reset_index()
    for site in sites:
        #print(f"site: {site.code}")
        # Test if site.code is in df['code']
        if site.code not in df['code'].values:
            site.linreg_predictor = None
            continue
        # Get the linear regression predictor for each site
        row = df[df['code'] == site.code]
        site.linreg_predictor = row['predictor'].values[0]
        print(f"site: {site.code}, linreg predictor: {site.linreg_predictor}")

    #print(f"Updated sites with linear regression predictor from DataFrame.")
    return sites

sites_list = update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=hydrograph_pentad_all, pentad=pentad_selector.value, decad=decad_selector.value)

#print(f"DEBUG: forecast_dashboard.py before update: linreg_predictor tail:\n{linreg_predictor.loc[linreg_predictor['code'] == '15149', ['date', 'code', 'predictor', 'pentad_in_year', 'pentad_in_month']].tail()}")
sites_list = update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=linreg_predictor, pentad=pentad_selector.value, decad=decad_selector.value)


# Adding the watcher logic for disabling the "Add to Bulletin" button
def update_add_to_bulletin_button(event):
    """Update the state of 'Add to Bulletin' button based on pipeline_running status."""
    add_to_bulletin_button.disabled = event.new

# Watch for changes in pipeline_running and update the add_to_bulletin_button
viz.app_state.param.watch(update_add_to_bulletin_button, 'pipeline_running')

# Set the initial state of the button based on whether the pipeline is running
add_to_bulletin_button.disabled = viz.app_state.pipeline_running


# Function to update the model select widget based on the selected station
# Create a callback that only updates the widget when data has actually changed
def create_widget_updater():
    last_values = {'options': None, 'value': None}

    def update_widget(options, values):
        # Only update if there's an actual change
        if (options != last_values['options'] or
            values != last_values['value']):

            # Force a complete widget reset
            #model_checkbox.param.update(
            #    options={},  # Clear options first
            #    value=[]     # Clear values
            #)

            # Then set new values
            model_checkbox.param.update(
                options=options,
                value=values
            )

            # Update last known state
            last_values['options'] = options
            last_values['value'] = values

            print(f"\nDEBUG: Widget Update")
            print(f"Setting options to: {options}")
            print(f"Setting values to: {values}")
            print(f"Widget state after update:")
            print(f"  Options: {model_checkbox.options}")
            print(f"  Value: {model_checkbox.value}")

    return update_widget

widget_updater = create_widget_updater()


def get_pane_alert(msg):
    return pn.pane.Alert(
        "⚠️ Warning: " + msg,
        alert_type="warning",
        sizing_mode="stretch_width"
    )


def get_predictors_warning(station):
    predictors_warning.objects = []  # clear old content
    today_date = today.date()
    filtered = hydrograph_day_all[
        (hydrograph_day_all["station_labels"] == station.value) &
        (hydrograph_day_all["date"] == pd.to_datetime(today_date))
    ]

    if not filtered.empty:
        if pd.notna(filtered["2025"].iloc[0]):
            print("2025 has a value:", filtered["2025"].iloc[0])
        else:
            print("2025 is NaN/empty")
            predictors_warning.append(get_pane_alert(f"No discharge record available today for {station.value}"))
    else:
        print("No record for today and given station")
        predictors_warning.append(get_pane_alert(f"No discharge record available today for {station.value}"))


def get_forecast_warning(station):
    forecast_warning.objects = []  # clear old content
    filtered = forecasts_all[
        (forecasts_all["station_labels"] == station.value) &
        (forecasts_all["date"] == pd.to_datetime(date_picker.value))
    ]
    if not filtered.empty:
        # filter rows where forecasted_discharge is NaN
        missing_forecasts = filtered[filtered["forecasted_discharge"].isna()]

        # collect the model_short values into a list
        missing_models = missing_forecasts["model_short"].tolist()

        if missing_models:
            print("Missing forecasts for models:", missing_models)
            forecast_warning.append(get_pane_alert(
                f"No forecast data available for models {', '.join(missing_models)} at {station.value} on {date_picker.value}."))
        else:
            print("All models have forecast data.")
    else:
        forecast_warning.append(get_pane_alert(f"No forecast data available for {station.value} on {date_picker.value}."))


@pn.depends(station, pentad_selector, decad_selector, watch=True)
def update_model_select(station_value, selected_pentad, selected_decad):
    get_predictors_warning(station)
    get_forecast_warning(station)
    print("\n=== Starting Model Select Update ===")
    print(f"Initial widget state:")
    print(f"  Options: {model_checkbox.options}")
    print(f"  Current value: {model_checkbox.value}")

    # First get the updated model dictionary
    updated_model_dict = processing.update_model_dict_date(model_dict_all, forecasts_all, station_value, date_picker.value)

    print("\nAfter update_model_dict:")
    print(f"  Updated model dict: {updated_model_dict}")

    # Get pre-selected models
    current_model_pre_selection = processing.get_best_models_for_station_and_pentad(
        forecasts_all, station_value, selected_pentad, selected_decad
    )

    print("\nAfter get_best_models:")
    print(f"  Pre-selected models: {current_model_pre_selection}")

    # Create new values list
    new_values = []
    for model in current_model_pre_selection:
        if model in updated_model_dict:
            print(f"  Adding model from dict: {model} -> {updated_model_dict[model]}")
            new_values.append(updated_model_dict[model])
        elif "Ens. Mean" in model:
            ensemble_option = next(
                (updated_model_dict[key] for key in updated_model_dict if "Ens. Mean" in key),
                None
            )
            if ensemble_option:
                print(f"  Adding ensemble model: {model} -> {ensemble_option}")
                new_values.append(ensemble_option)

    print("\nBefore widget update:")
    print(f"  New options to set: {updated_model_dict}")
    print(f"  New values to set: {new_values}")

    # Try updating options first, then values
    #model_checkbox.options = {}  # Clear first
    #model_checkbox.param.trigger('options')
    model_checkbox.options = updated_model_dict
    model_checkbox.param.trigger('options')

    print("\nAfter options update:")
    print(f"  Widget options: {model_checkbox.options}")
    print(f"  Widget value: {model_checkbox.value}")

    #model_checkbox.value = []  # Clear first
    #model_checkbox.param.trigger('value')
    model_checkbox.value = new_values
    model_checkbox.param.trigger('value')

    print("\nFinal widget state:")
    print(f"  Widget options: {model_checkbox.options}")
    print(f"  Widget value: {model_checkbox.value}")

    # Add event monitoring
    #@model_checkbox.param.watch('value')
    #def value_watcher(event):
    #    print(f"\nValue change detected:")
    #    print(f"  Old: {event.old}")
    #    print(f"  New: {event.new}")

    return updated_model_dict


# Create the pop-up notification pane (initially hidden)
add_to_bulletin_popup = pn.pane.Alert(_("Added to bulletin"), alert_type="success", visible=False)


def get_bulletin_csv_path(year, horizon_value):
    """Generate CSV path with pentad information"""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_string = f"{horizon_value:02d}"
    else:
        horizon_string = f"{horizon_value:02d}"
    bulletin_filename = f'bulletin_{horizon}_{year}_{horizon_string}.csv'
    return os.path.join(SAVE_DIRECTORY, bulletin_filename)


# Function to load bulletin data from CSV
def load_bulletin_from_csv():
    """Load bulletin data from CSV file for current pentad"""
    global bulletin_sites

    # Get current pentad
    #current_date = pd.Timestamp.now()
    #current_year = current_date.year
    #current_pentad = tl.get_pentad_for_date(current_date)
    #current_bulletin_path = get_bulletin_csv_path(current_year, current_pentad)
    current_bulletin_path = get_bulletin_csv_path(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin)
    # Print bulletin path
    print(f"DEBUG: forecast_dashboard.py: current_bulletin_path: {current_bulletin_path}")

    if os.path.exists(current_bulletin_path):
        # Print that bulletin path exists
        print(f"DEBUG: forecast_dashboard.py: Bulletin path exists: {current_bulletin_path}")
        try:
            bulletin_df = pd.read_csv(current_bulletin_path, encoding='utf-8-sig')

            # Rename columns from original English to localized names for UI consistency
            bulletin_df_display = bulletin_df.rename(columns={
                'station_label': _('Hydropost'),
                'model_short': _('Model'),
                'basin_ru': _('Basin'),
                'forecasted_discharge': _('Forecasted discharge'),
                'fc_lower': _('Forecast lower bound'),
                'fc_upper': _('Forecast upper bound'),
                'delta': _('δ'),
                'sdivsigma': _('s/σ'),
                'mae': _('MAE'),
                'accuracy': _('Accuracy')
            })

            bulletin_sites = []
            for code in bulletin_df['code'].unique():
                site_data = bulletin_df_display[bulletin_df['code'] == code].copy()
                site = next((s for s in sites_list if s.code == str(code)), None)
                if site:
                    # Assign forecasts to the site
                    site.forecasts = site_data.drop(columns=['code', _('Hydropost'), _('Basin')])
                    # Update site attributes
                    site.get_forecast_attributes_for_site(_, site.forecasts)
                    bulletin_sites.append(site)

            print(f"DEBUG: Loaded bulletin_sites from CSV for pentad {forecast_horizon_for_saving_bulletin}:")
            for site in bulletin_sites:
                print(f"Site '{site.code}' with forecasts: {site.forecasts}")

            logger.info(f"Loaded bulletin data for pentad {forecast_horizon_for_saving_bulletin}")
        except Exception as e:
            logger.error(f"Error loading bulletin CSV: {e}")
            bulletin_sites = []
    else:
        logger.info(f"No bulletin data found for current pentad {forecast_horizon_for_saving_bulletin}")
        bulletin_sites = []


# Function to save bulletin data to CSV
def save_bulletin_to_csv():
    # Get current pentad
    #current_date = pd.Timestamp.now()
    #current_year = current_date.year
    #current_pentad = tl.get_pentad_for_date(current_date)

    # Generate path for current pentad's bulletin
    #current_bulletin_path = get_bulletin_csv_path(current_year, current_pentad)
    current_bulletin_path = get_bulletin_csv_path(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin)

    data = []
    for site in bulletin_sites:
        # We need to extract the forecast data and site information
        for idx, forecast_row in site.forecasts.iterrows():
            row_data = forecast_row.to_dict()
            row_data['code'] = site.code
            row_data['station_label'] = site.station_label
            row_data['basin_ru'] = getattr(site, 'basin_ru', '')
            data.append(row_data)

    if data:
        bulletin_df_display = pd.DataFrame(data)

        # Translate the localized columns back to their original names
        bulletin_df = bulletin_df_display.rename(columns={
            _('Hydropost'): 'station_label',
            _('Model'): 'model_short',
            _('Basin'): 'basin_ru',
            _('Forecasted discharge'): 'forecasted_discharge',
            _('Forecast lower bound'): 'fc_lower',
            _('Forecast upper bound'): 'fc_upper',
            _('δ'): 'delta',
            _('s/σ'): 'sdivsigma',
            _('MAE'): 'mae',
            _('Accuracy'): 'accuracy'
        })

        try:
            bulletin_df.to_csv(current_bulletin_path, index=False, encoding='utf-8-sig')
            print(f"Bulletin saved to CSV for pentad {forecast_horizon_for_saving_bulletin}")
            logger.info(f"Bulletin saved to CSV for pentad {forecast_horizon_for_saving_bulletin}")
        except Exception as e:
            logger.error(f"Error writing bulletin CSV: {e}")
    else:
        # If data is empty, remove the CSV file
        # If data is empty, remove the current pentad's CSV file
        if os.path.exists(current_bulletin_path):
            os.remove(current_bulletin_path)
            print(f"Bulletin CSV file removed for pentad {forecast_horizon_for_saving_bulletin} because bulletin is empty")
            logger.info(f"Bulletin CSV file removed for pentad {forecast_horizon_for_saving_bulletin} because bulletin is empty")

# Call the function to load the bulletin data
load_bulletin_from_csv()


# Function to handle adding the current selection to the bulletin
def add_current_selection_to_bulletin(event=None):
    # Ensure pipeline is not running
    if viz.app_state.pipeline_running:
        print("Cannot add to bulletin while containers are running.")
        return

    selected_indices = forecast_tabulator.selection
    forecast_df = forecast_tabulator.value

    if forecast_df is None or forecast_df.empty:
        print("Forecast summary table is empty.")
        return

    if not selected_indices and len(forecast_df) > 0:
        selected_indices = [0]  # Default to the first row

    selected_rows = forecast_df.iloc[selected_indices]
    selected_station = station.value
    selected_site = next(
        (site for site in sites_list if site.station_label == selected_station),
        None
    )

    if selected_site is None:
        print(f"Site '{selected_station}' not found in sites_list.")
        return

    # Assign forecasts to selected site object
    selected_site.forecasts = selected_rows.reset_index(drop=True)

    # Add forecast attributes to site object
    selected_site.get_forecast_attributes_for_site(_, selected_rows)

    # Debugging: Print site details
    print(f"DEBUG: Added site '{selected_site.code}' to bulletin with forecasts: {selected_site.forecasts}")

    add_to_bulletin_popup.object = _("Added to bulletin table")
    add_to_bulletin_popup.alert_type = "success"
    add_to_bulletin_popup.visible = True
    pn.state.add_periodic_callback(
        lambda: setattr(add_to_bulletin_popup, 'visible', False),
        2000, count=1)

    # Update or add to bulletin_sites
    existing_site = next(
        (site for site in bulletin_sites if site.code == selected_site.code),
        None
    )
    if existing_site is None:
        bulletin_sites.append(selected_site)
        print(f"DEBUG: Added new site '{selected_site.station_label}' to bulletin_sites.")
    else:
        index = bulletin_sites.index(existing_site)
        bulletin_sites[index] = selected_site
        print(f"DEBUG: Updated existing site '{selected_site.station_label}' in bulletin_sites.")

    # Save updated data to CSV for persistence
    save_bulletin_to_csv()

    # Update bulletin table
    update_bulletin_table()


# Function to handle writing bulletin to Excel
def handle_bulletin_write(event):
    try:
        if not bulletin_sites:
            print("DEBUG: No sites in bulletin to write.")
            return

        selected_basin = select_basin_widget.value
        if selected_basin == _("All basins"):
            filtered_bulletin_sites = bulletin_sites.copy()
        else:
            filtered_bulletin_sites = [
                site for site in bulletin_sites if getattr(site, 'basin_ru', '') == selected_basin
            ]

        if not filtered_bulletin_sites:
            print("DEBUG: No sites in bulletin for the selected basin.")
            return

        # Debugging: print the site details being written
        for site in filtered_bulletin_sites:
            print(f"DEBUG: Writing site '{site.code}' with forecasts: {site.forecasts}")

        write_to_excel(
            sites_list, filtered_bulletin_sites, bulletin_header_info,
            env_file_path)
        print("DEBUG: Bulletin written to Excel successfully.")

        # Refresh the file downloader panel
        downloader.refresh_file_list()

    except Exception as e:
        logger.error(f"Error writing bulletin to Excel: {e}")


# Function to create the bulletin table
def create_bulletin_table():
    global bulletin_tabulator  # Declare as global to modify the global variable
    print("Creating/updating bulletin table...")

    if bulletin_sites:
        data = []
        for site in bulletin_sites:
            for idx, forecast_row in site.forecasts.iterrows():
                data.append({
                    _('Hydropost'): site.station_label,
                    _('Model'): forecast_row.get(_('Model'), ''),
                    _('Basin'): getattr(site, 'basin_ru', ''),
                    _('Forecasted discharge'): forecast_row.get(_('Forecasted discharge'), ''),
                    _('Forecast lower bound'): forecast_row.get(_('Forecast lower bound'), ''),
                    _('Forecast upper bound'): forecast_row.get(_('Forecast upper bound'), ''),
                    _('δ'): forecast_row.get('δ', ''),
                    _('s/σ'): forecast_row.get('s/σ', ''),
                    _('MAE'): forecast_row.get('MAE', ''),
                    _('Accuracy'): forecast_row.get(_('Accuracy'), ''),
                    # Add other fields as needed
                })
        bulletin_df = pd.DataFrame(data)

        # Apply 'Select Basin' filter if applicable
        selected_basin = select_basin_widget.value
        if selected_basin != _("All basins"):
            bulletin_df = bulletin_df[bulletin_df['Basin'] == selected_basin]

        bulletin_tabulator.value = bulletin_df
    else:
        # Empty DataFrame with predefined columns
        bulletin_df = pd.DataFrame(columns=[
            _('Hydropost'), _('Model'), _('Basin'),
            _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
            _('δ'), _('s/σ'), _('MAE'), _('Accuracy')
        ])

        # Update the Tabulator's value
        bulletin_tabulator.value = bulletin_df

    print("Bulletin table updated.")


# Function to update the bulletin table
def update_bulletin_table(event=None):
    create_bulletin_table()


bulletin_table = pn.Column(
    bulletin_tabulator,  # Add the global Tabulator directly
    pn.Row(remove_bulletin_button, sizing_mode='stretch_width'),
    add_to_bulletin_popup  # Include the popup for success messages
)

update_bulletin_table()

select_basin_widget.param.watch(update_bulletin_table, 'value')

add_to_bulletin_button.on_click(add_current_selection_to_bulletin)


# Function to remove selected forecasts from the bulletin
def remove_selected_from_bulletin(event=None):
    global bulletin_tabulator
    selected = bulletin_tabulator.selection  # List of selected row indices

    if not selected:
        print("No forecasts selected for removal.")
        logger.warning("Remove action triggered, but no forecasts were selected.")
        return

    # Get the indices of the selected rows
    selected_indices = selected

    # Get the bulletin DataFrame from the tabulator
    bulletin_df = bulletin_tabulator.value

    # Get the hydroposts of the selected rows
    selected_rows = bulletin_df.iloc[selected_indices]
    selected_hydroposts = selected_rows[_('Hydropost')].unique()

    # Remove the selected sites from bulletin_sites
    for hydropost in selected_hydroposts:
        site_to_remove = next(
            (site for site in bulletin_sites if site.station_label == hydropost),
            None)
        if site_to_remove:
            bulletin_sites.remove(site_to_remove)
            logger.info(f"Removed site from bulletin: {hydropost}")

    # Save the updated bulletin to CSV
    save_bulletin_to_csv()

    # Update the bulletin table to reflect the changes
    update_bulletin_table()

    # Show a success message
    print("Selected forecasts have been removed from the bulletin.")
    add_to_bulletin_popup.object = _("Selected forecasts have been removed from the bulletin.")
    add_to_bulletin_popup.alert_type = "success"
    add_to_bulletin_popup.visible = True
    pn.state.add_periodic_callback(
        lambda: setattr(add_to_bulletin_popup, 'visible', False),
        2000, count=1)

# Attach the remove function to the remove button click event
remove_bulletin_button.on_click(remove_selected_from_bulletin)
# endregion


# region dashboard_layout
# Dynamically update figures
date_picker_with_pentad_text = viz.create_date_picker_with_pentad_text(date_picker, _)

update_callback = viz.update_forecast_data(_, linreg_datatable, station, pentad_selector)
pentad_selector.param.watch(update_callback, 'value')

# Initial setup: populate the main area with the initial selection
#update_callback(None)  # This does not seem to be needed
daily_hydrograph_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
if rain is None: 
    daily_rainfall_plot = pn.pane.Markdown(_("No precipitation data from SAPPHIRE Data Gateway available."))
    daily_temperature_plot = pn.pane.Markdown(_("No temperature data from SAPPHIRE Data Gatway available."))
else: 
    daily_rainfall_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 
    daily_temperature_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 

forecast_data_and_plot = pn.Column(sizing_mode="stretch_both")


def update_forecast_hydrograph(selected_option, _, hydrograph_day_all,
                               hydrograph_pentad_all, linreg_predictor,
                               forecasts_all, station, title_date,
                               model_selection, range_type, range_slider,
                               range_visibility, rram_forecast, ml_forecast):
    if selected_option == _('Yes'):
        # Show forecasts aggregated to pentadal values
        return viz.plot_pentad_forecast_hydrograph_data(
            _,
            hydrograph_pentad_all=hydrograph_pentad_all,
            forecasts_all=forecasts_all,
            station=station,
            title_date=title_date,
            model_selection=model_selection,
            range_type=range_type,
            range_slider=range_slider,
            range_visibility=range_visibility
        )
    else:
        # Show daily forecasts
        return viz.plot_pentad_forecast_hydrograph_data_v2(
            _,
            hydrograph_day_all=hydrograph_day_all,
            linreg_predictor=linreg_predictor,
            forecasts_all=forecasts_all,
            station=station,
            title_date=title_date,
            model_selection=model_selection,
            range_type=range_type,
            range_slider=range_slider,
            range_visibility=range_visibility,
            rram_forecast=rram_forecast,
            ml_forecast=ml_forecast
        )

pentad_forecast_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
effectiveness_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
accuracy_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
forecast_skill_plot = pn.Column(effectiveness_plot, accuracy_plot)


def update_forecast_plots(event):
    """Updates 2nd, 3rd and 4th plots on Forecast tab"""
    pentad_forecast_plot.object = update_forecast_hydrograph(
        show_daily_data_widget.value,
        _,
        hydrograph_day_all=hydrograph_day_all,
        hydrograph_pentad_all=hydrograph_pentad_all,
        linreg_predictor=linreg_predictor,
        forecasts_all=forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=rram_forecast,
        ml_forecast=ml_forecast
    )
    temp = viz.plot_forecast_skill(
        _,
        hydrograph_pentad_all,
        forecasts_all,
        station_widget=station.value,
        date_picker=date_picker.value,
        model_checkbox=model_checkbox.value,
        range_selection_widget=allowable_range_selection.value,
        manual_range_widget=manual_range.value,
        show_range_button=show_range_button.value
    )
    effectiveness_plot.object = temp[0].object
    accuracy_plot.object = temp[1].object

    forecast_summary_table.object = update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)

update_forecast_button.on_click(update_forecast_plots)

skill_table = pn.panel(
    viz.create_skill_table(_, forecast_stats),
    sizing_mode='stretch_width')

skill_metrics_download_filename, skill_metrics_download_button = skill_table.download_menu(
    text_kwargs={'name': _('Enter filename:'), 'value': 'forecast_skill_metrics.csv'},
    button_kwargs={'name': _('Download currently visible table')}
)
# Define a download button for the skill metrics table
#skill_metrics_download_button = pn.widgets.FileDownload(
#    file='forecast_skill_metrics.csv', button_type='success', auto=False,
#    embed=False, name="Right-click to download using 'Save as' dialog"
#)


# @pn.depends(station, model_checkbox, allowable_range_selection, manual_range, watch=True)
def update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range):
    viz.create_forecast_summary_tabulator(
        _, forecasts_all, station, date_picker,
        model_checkbox, allowable_range_selection, manual_range,
        forecast_tabulator
    )

# Initial update
update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)


def update_visualizations():
    # Re-bind the plots to use the updated data
    #print('---   ---plot_pentad_forecast_hydrograph_data---   ---')
    viz.plot_pentad_forecast_hydrograph_data(
        _,
        hydrograph_pentad_all=hydrograph_pentad_all,
        forecasts_all=forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data---   ---')

    #print('---   ---plot_pentad_forecast_hydrograph_data_v2---   ---')
    viz.plot_pentad_forecast_hydrograph_data_v2(
        _,
        hydrograph_day_all=hydrograph_day_all,
        linreg_predictor=linreg_predictor,
        forecasts_all=forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=rram_forecast,
        ml_forecast=ml_forecast
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data_v2---   ---')

    #print('---   ---update_forecast_tabulator---   ---')
    update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)
    #print('---   ---done with update_forecast_tabulator---   ---')


def on_data_needs_reload_changed(event):
    if event.new:
        print("Triggered rerunning of forecasts.")
        try:
            #print("---loading data---")
            load_data()
            #print("---data loaded---")
            #print("---updating viz---")
            update_visualizations()
            #print("---viz updated---")
            #print("Forecasts produced and visualizations updated successfully.")
        except Exception as e:
            print(f"Error during forecast rerun: {e}")
        finally:
            processing.data_reloader.data_needs_reload = False  # Reset the flag

# Attach watcher only once
if not hasattr(processing.data_reloader, 'watcher_attached'):
    processing.data_reloader.param.watch(on_data_needs_reload_changed, 'data_needs_reload')
    processing.data_reloader.watcher_attached = True

# Same Tabulator in both tabs
forecast_summary_table = pn.panel(
    forecast_tabulator,
    sizing_mode='stretch_width'
)

# Update the site object based on site and forecast selection
#print(f"DEBUG: forecast_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=sites_list,
    site_selection=station,
    tabulator=forecast_summary_table)

# Watch for changes in parameters to update the Tabulator
#station.param.watch(update_forecast_tabulator, 'value')
#date_picker.param.watch(update_forecast_tabulator, 'value')
#model_checkbox.param.watch(update_forecast_tabulator, 'value')
#allowable_range_selection.param.watch(update_forecast_tabulator, 'value')
#manual_range.param.watch(update_forecast_tabulator, 'value')

# Bind the update function to the button
#pn.bind(update_indicator, write_bulletin_button, watch=True)

# Attach the function to the button click event
# Multi-threading does not seem to work
#write_bulletin_button.on_click(lambda event: thread.write_forecast_bulletin_in_background(bulletin_table, env_file_path, status))
# Create a container to hold the invisible download widget
#download_container = pn.pane.HTML()

# Button callback
#write_bulletin_button.on_click(
#    lambda event: setattr(download_container, 'object',
#        write_to_excel(sites_list, bulletin_sites, bulletin_header_info, env_file_path)
#    )
#)


# Use the new handler
write_bulletin_button.on_click(handle_bulletin_write)

# Define the disclaimer of the dashboard
disclaimer = layout.define_disclaimer(_, in_docker_flag)


# Update the layout

# Update the widgets conditional on the active tab
allowable_range_selection.param.watch(lambda event: viz.update_range_slider_visibility(
    _, manual_range, event), 'value')

reload_card = viz.create_reload_button()

# We don't need to update tabs or UI components dynamically since the page reloads

# region authentication
#------------------AUTHENTICATION-----------------------------
# Custom authentication logic by Vjekoslav Večković

# Create widgets for login
username_input = pn.widgets.TextInput(name=_('Username'), placeholder=_('Enter your username'))
password_input = pn.widgets.PasswordInput(name=_('Password'), placeholder=_('Enter your password'))
login_submit_button = pn.widgets.Button(name=_('Login'), button_type='primary')
login_feedback = pn.pane.Markdown("", visible=False)
dashboard_link = pn.pane.Markdown("", visible=False)

# Create logout confirmation widgets
logout_confirm = pn.pane.Markdown("**Are you sure you want to log out?**", visible=False)
logout_yes = pn.widgets.Button(name="Yes", button_type="success", visible=False)
logout_no = pn.widgets.Button(name="No", button_type="danger", visible=False)
logout_button = pn.widgets.Button(name="Logout", button_type="danger")


def update_last_activity():
    global last_activity_time
    last_activity_time = datetime.now()


def check_inactivity():
    """Check if user has been inactive and logout if needed"""
    global last_activity_time
    current_user = check_current_user()

    if current_user:
        if datetime.now() - last_activity_time > INACTIVITY_TIMEOUT:
            print(f"User {current_user} logged out due to inactivity")
            handle_logout_confirm(None)  # Use existing logout function


def logout_user(user):
    """Perform the logout actions: remove user CSV files, clear logs, show login form."""
    if os.path.exists("current_user.csv"):
        os.remove("current_user.csv")
    if os.path.exists("auth_logs.csv"):
        os.remove("auth_logs.csv")
    if os.path.exists("user_activity.csv"):
        os.remove("user_activity.csv")

    # Hide dashboard and show login form
    hide_dashboard()
    show_login_form()
    print(f"User {user} logged out due to inactivity.")


def handle_login(event):
    """Handle login attempts."""
    global last_activity_time
    username = username_input.value
    password = password_input.value
    credentials = load_credentials()

    if username in credentials and credentials[username] == password:
        # Check if another user is currently logged in
        current_user = check_current_user()
        if current_user and current_user != username:
            login_feedback.object = f"Another user ({current_user}) is currently logged in."
            login_feedback.visible = True
            return

        # At this point, either no one is logged in, or the same user is re-logging in.
        last_activity_time = datetime.now()  # Reset activity timer on login

        # Proceed with login
        save_current_user(username)
        log_auth_event(username, 'logged in')
        log_user_activity(username, 'login')

        # Update UI
        login_feedback.object = "Login successful!"
        login_feedback.visible = True
        show_dashboard()

        # Periodically check inactivity every 5 minutes
        pn.state.add_periodic_callback(check_inactivity, 300000)
    else:
        login_feedback.object = "Invalid username or password."
        login_feedback.visible = True


def on_user_interaction(event=None):
    """Update last activity time when user interacts with the dashboard"""
    global last_activity_time
    last_activity_time = datetime.now()


# Add watchers to widgets:
station.param.watch(on_user_interaction, 'value')
pentad_selector.param.watch(on_user_interaction, 'value')
date_picker.param.watch(on_user_interaction, 'value')
model_checkbox.param.watch(on_user_interaction, 'value')
allowable_range_selection.param.watch(on_user_interaction, 'value')
show_range_button.param.watch(on_user_interaction, 'value')
show_daily_data_widget.param.watch(on_user_interaction, 'value')
select_basin_widget.param.watch(on_user_interaction, 'value')

# For buttons that trigger actions, watch the 'clicks' parameter:
add_to_bulletin_button.param.watch(on_user_interaction, 'clicks')
write_bulletin_button.param.watch(on_user_interaction, 'clicks')
remove_bulletin_button.param.watch(on_user_interaction, 'clicks')

# Track when the user interacts with the forecast_tabulator
# (e.g., by selecting rows), you can watch the 'selection' parameter:
forecast_tabulator.param.watch(on_user_interaction, 'selection')


def handle_logout_request(event):
    """Show logout confirmation."""
    logout_confirm.visible = True
    logout_yes.visible = True
    logout_no.visible = True


# Update handle_logout to clear activity logs
def handle_logout_confirm(event):
    """Handle confirmed logout."""
    current_user = check_current_user()
    if current_user:
        log_auth_event(current_user, 'logged out')
        log_user_activity(current_user, 'logout')
        remove_current_user()
        clear_auth_logs()
        clear_activity_log()

        # Update UI
        hide_dashboard()
        show_login_form()
        logout_confirm.visible = False
        logout_yes.visible = False
        logout_no.visible = False


def handle_logout_cancel(event):
    """Handle cancelled logout."""
    logout_confirm.visible = False
    logout_yes.visible = False
    logout_no.visible = False


def show_dashboard():
    """Show the dashboard and hide login form."""
    login_form.visible = False
    dashboard_content.visible = True
    sidebar_content.visible = True
    logout_button.visible = True
    logout_panel.visible = True  # Make sure logout panel is visible
    language_buttons.visible = True


def hide_dashboard():
    """Hide the dashboard and show login form."""
    dashboard_content.visible = False
    sidebar_content.visible = False
    logout_button.visible = False
    logout_panel.visible = False  # Hide logout panel
    language_buttons.visible = False
    login_form.visible = True


def show_login_form():
    """Show login form and reset fields."""
    username_input.value = ''
    password_input.value = ''
    login_feedback.visible = False
    login_form.visible = True
    dashboard_content.visible = False  # Explicitly hide dashboard
    sidebar_content.visible = False
    logout_button.visible = False
    logout_panel.visible = False
    language_buttons.visible = False


# Bind event handlers
login_submit_button.on_click(handle_login)
logout_button.on_click(handle_logout_request)
logout_yes.on_click(handle_logout_confirm)
logout_no.on_click(handle_logout_cancel)


# Create layout components
login_form = pn.Column(
    pn.pane.Markdown(f"# {_('Login')}"),
    username_input,
    password_input,
    login_submit_button,
    login_feedback
)

logout_panel = pn.Column(
    logout_confirm,
    pn.Row(logout_yes, logout_no)
)
predictors_warning = pn.Column()
forecast_warning = pn.Column()
get_predictors_warning(station)
get_forecast_warning(station)

# Create a placeholder for the dashboard content
dashboard_content = layout.define_tabs(_, predictors_warning, forecast_warning,
    daily_hydrograph_plot, daily_rainfall_plot, daily_temperature_plot,
    forecast_data_and_plot,
    forecast_summary_table, pentad_forecast_plot, forecast_skill_plot,
    bulletin_table, write_bulletin_button, bulletin_download_panel, disclaimer,
    station_card, forecast_card, add_to_bulletin_button, basin_card,
    pentad_card, reload_card, add_to_bulletin_popup, show_daily_data_widget,
    skill_table, skill_metrics_download_filename, skill_metrics_download_button
)

latest_predictors = None
latest_forecast = None


def update_active_tab(event):
    """Callback function to handle tab and station changes"""
    global latest_predictors
    global latest_forecast
    active_tab = dashboard_content.active  # 0: Predictors tab, 1: Forecast tab
    if active_tab == 0 and latest_predictors != station.value:
        latest_predictors = station.value
        daily_hydrograph_plot.object = viz.plot_daily_hydrograph_data(_, hydrograph_day_all, linreg_predictor, station.value, date_picker.value)
        if display_weather_data == True: 
            daily_rainfall_plot.object = viz.plot_daily_rainfall_data(_, rain, station.value, date_picker.value, linreg_predictor)
            daily_temperature_plot.object = viz.plot_daily_temperature_data(_, temp, station.value, date_picker.value, linreg_predictor)
    elif active_tab == 1 and latest_forecast != station.value:
        latest_forecast = station.value
        plot = viz.select_and_plot_data(_, linreg_predictor, station.value, pentad_selector.value, decad_selector.value, SAVE_DIRECTORY)
        forecast_data_and_plot[:] = plot.objects
        update_forecast_plots(None)


# Attach the callback to the tabs and station
dashboard_content.param.watch(update_active_tab, 'active')
station.param.watch(update_active_tab, 'value')
update_active_tab(None)

dashboard_content.visible = False


sidebar_content=layout.define_sidebar(_, station_card, forecast_card, basin_card,
                                  message_pane, reload_card)

sidebar_content.visible = False


# Handle language selection and session management
def on_session_start():
    """Handle new session start (including language changes)."""
    current_user = check_auth_state()

    if current_user and check_recent_activity(current_user, 'language_change'):
        show_dashboard()
        return

    # If no valid session or recent activity, show login form
    show_login_form()

# Initialize the app with proper session handling
on_session_start()
# ------------------END OF AUTHENTICATION---------------------
# endregion

# Define dashboard title
if sapphire_forecast_horizon == 'pentad':
    dashboard_title = _('SAPPHIRE Central Asia - Pentadal forecast dashboard')
else:
    dashboard_title = _('SAPPHIRE Central Asia - Decadal forecast dashboard')

dashboard = pn.template.BootstrapTemplate(
    title=dashboard_title,
    logo=icon_path,
    header=[pn.Row(pn.layout.HSpacer(),language_buttons, logout_button, logout_panel)],
    sidebar=pn.Column(sidebar_content),
    collapsed_sidebar=False,
    main=pn.Column(login_form, dashboard_content),
    favicon=icon_path
)

# Initialize visibility states
logout_button.visible = False
logout_panel.visible = False
dashboard_content.visible = False
sidebar_content.visible=False
login_form.visible = True
language_buttons.visible = False

# Make the dashboard servable
dashboard.servable()
# endregion
