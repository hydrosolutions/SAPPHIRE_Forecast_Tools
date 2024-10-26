# pentad_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm SAPPHIRE_OPDEV_ENV=True panel serve pentad_dashboard.py --show --autoreload
#

# region load_libraries
from dotenv import load_dotenv
import os
import sys

import gettext  # For translation
import locale

import panel as pn

from bokeh.models import FixedTicker, CustomJSTickFormatter, LinearAxis, Range1d
from bokeh.models.widgets.tables import NumberFormatter
from holoviews import streams
from holoviews import opts

import numpy as np
import pandas as pd
import datetime as dt
import math
import param
from functools import partial

import logging
from logging.handlers import TimedRotatingFileHandler
import traceback

#import hvplot.pandas  # Enable interactive
import holoviews as hv
from scipy import stats
# Set the default extension
pn.extension('tabulator')

from ieasyreports.settings import ReportGeneratorSettings

# Local sources
from src.environment import load_configuration
import src.gettext_config as localize
import src.processing as processing
import src.vizualization as viz
#import src.multithreading as thread
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel
import src.layout as layout

import calendar

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
import setup_library as sl
import tag_library as tl
import forecast_library as fl

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
pn.extension(global_css=[
    ':root { --design-primary-color: #307096; }',
    """
    .checkbox-label {
        white-space: normal !important;
        word-wrap: break-word !important;
        width: 100%; /* Adjust as needed */
    }
    """
    ])

# CSS for language widget text color
language_button_css = """
header .bk.pn-widget-select .bk-select {
    color: #307086 !important;  /* Change text color of select widget */
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

# endregion



# region localization
# Read the locale from the environment file
current_locale = os.getenv("ieasyforecast_locale")
print(f"INFO: Default locale: {current_locale}")

# Localization, translation to different languages.
localedir = os.getenv("ieasyforecast_locale_dir")

_ = localize.load_translation(current_locale, localedir)

# endregion

# region load_data

def load_data():
    global hydrograph_day_all, hydrograph_pentad_all, linreg_predictor, forecasts_all, forecast_stats, all_stations, station_dict, station_df, station_list, linreg_datatable, stations_iehhf    # Daily runoff data
    hydrograph_day_all = processing.read_hydrograph_day_data_for_pentad_forecasting(stations_iehhf)
    hydrograph_pentad_all = processing.read_hydrograph_pentad_data_for_pentad_forecasting(stations_iehhf)

    # Pentadal forecast data
    # - linreg_predictor: for displaying predictor in predictor tab
    linreg_predictor = processing.read_linreg_forecast_data(stations_iehhf)
    # For site = 16059, show the last 5 rows of the linreg_predictor DataFrame
    #print(f"DEBUG: pentad_dashboard.py: linreg_predictor: {linreg_predictor[linreg_predictor['code'] == '16059'].tail()}")
    # - forecast results from all models
    forecasts_all = processing.read_forecast_results_file(stations_iehhf)
    # Forecast statistics
    forecast_stats = processing.read_forecast_stats_file(stations_iehhf)

    # Hydroposts metadata
    station_list, all_stations, station_df, station_dict = processing.read_all_stations_metadata_from_iehhf(
        hydrograph_day_all['code'].unique().tolist())
    #station_list, all_stations, station_df, station_dict = processing.read_all_stations_metadata_from_file(
    #    hydrograph_day_all['code'].unique().tolist())
    if not station_list:
        raise ValueError("The station list is empty. Please check the data source and ensure it contains valid stations.")
    #print("DEBUG: pentad_dashboard.py: Station list:\n", station_list)
    #print("DEBUG: pentad_dashboard.py: All stations: \n", all_stations)
    #logger.debug(f"DEBUG: pentad_dashboard.py: Station list:\n{station_list}")
    #print(f"DEBUG: columns of all_stations:\n{all_stations.columns}")
    #logger.debug(f"DEBUG: pentad_dashboard.py: All stations:\n{all_stations}")
    #print(f"DEBUG: pentad_dashboard.py: Station dataframe:\n{station_df}")
    #print(f"DEBUG: pentad_dashboard.py: Station dictionary:\n{station_dict}")
    #print(f"DEBUG: First dictionary entry: {next(iter(station_dict))}")
    #print(f"DEBUG: First station: {station_dict[next(iter(station_dict))][0]}")

    # Add the station_labels column to the hydrograph_day_all DataFrame
    hydrograph_day_all = processing.add_labels_to_hydrograph(hydrograph_day_all, all_stations)
    hydrograph_pentad_all = processing.add_labels_to_hydrograph(hydrograph_pentad_all, all_stations)
    #print(f"DEBUG: linreg_predictor raw: {linreg_predictor.tail()}")
    linreg_predictor = processing.add_labels_to_forecast_pentad_df(linreg_predictor, all_stations)
    #print(f"DEBUG: linreg_predictor with labels: {linreg_predictor.tail()}")
    linreg_datatable = processing.shift_date_by_n_days(linreg_predictor, 1)
    #print(f"DEBUG: linreg_datatable.columns: {linreg_datatable.columns}")
    #print(f"DEBUG: linreg_datatable: {linreg_datatable.tail()}")
    #print(f"DEBUG: linreg_predictor.columns: {linreg_predictor.columns}")
    #print(f"DEBUG: linreg_predictor: {linreg_predictor.tail()}")
    forecasts_all = processing.add_labels_to_forecast_pentad_df(forecasts_all, all_stations)

    # Replace model names with translation strings
    forecasts_all = processing.internationalize_forecast_model_names(_, forecasts_all)
    forecast_stats = processing.internationalize_forecast_model_names(_, forecast_stats)

    # Merge forecast stats with forecasts by code and pentad_in_year and model_short
    forecasts_all = forecasts_all.merge(
        forecast_stats,
        on=['code', 'pentad_in_year', 'model_short', 'model_long'],
        how='left')

# Get stations selected for pentadal forecasts
# Not necessary as we write new config files in linear regression module.
#if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'False':
#    stations_iehhf = processing.get_station_codes_selected_for_pentadal_forecasts()
#else:
#    stations_iehhf = None
stations_iehhf = None

# Rainfall
rain = processing.read_rainfall_data()
temp = processing.read_temperature_data()


# Create a list of Site objects from the all_stations DataFrame

load_data()

# Test if we have sites in stations_iehhf which are not present in forecasts_all
# Placeholder for a message pane
message_pane = pn.pane.Markdown("")
if stations_iehhf is not None:
    missing_sites = set(stations_iehhf) - set(forecasts_all['code'].unique())
    if missing_sites:
        missing_sites_message = f"WARNING: The following sites are missing from the forecast results: {missing_sites}. No forecasts are currently available for these sites. Please make sure your forecast models are configured to produce results for these sites, re-run hindcasts manually and re-run the forecast."
        message_pane.object = missing_sites_message

sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)

bulletin_table = pn.Column()

bulletin_sites = []

tabs_container = pn.Column()

# Create a dictionary of the model names and the corresponding model labels
model_dict_all = forecasts_all[['model_short', 'model_long']] \
    .set_index('model_long')['model_short'].to_dict()
#print(f"DEBUG: pentad_dashboard.py: station_dict: {station_dict}")

pentads = [
    f"{i+1}st pentad of {calendar.month_name[month]}" if i == 0 else
    f"{i+1}nd pentad of {calendar.month_name[month]}" if i == 1 else
    f"{i+1}rd pentad of {calendar.month_name[month]}" if i == 2 else
    f"{i+1}th pentad of {calendar.month_name[month]}"
    for month in range(1, 13) for i in range(6)
]

# Create a dictionary mapping each pentad description to its pentad_in_year value
pentad_options = {f"{i+1}st pentad of {calendar.month_name[month]}" if i == 0 else
                  f"{i+1}nd pentad of {calendar.month_name[month]}" if i == 1 else
                  f"{i+1}rd pentad of {calendar.month_name[month]}" if i == 2 else
                  f"{i+1}th pentad of {calendar.month_name[month]}": i + (month-1)*6 + 1
                  for month in range(1, 13) for i in range(6)}

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

# Get information for bulletin headers into a dataframe that can be passed to
# the bulletin writer.
bulletin_header_info = processing.get_bulletin_header_info(last_date)
#print(f"DEBUG: pentad_dashboard.py: bulletin_header_info:\n{bulletin_header_info}")

# Create the dropdown widget for pentad selection
pentad_selector = pn.widgets.Select(
    name=_("Select Pentad"),
    options=pentad_options,
    value=current_pentad,  # Default to the last available pentad
    margin=(0, 0, 0, 0)
)

# Widget for station selection, always visible
#print(f"\n\nDEBUG: pentad_dashboard.py: station select name string: {_('Select discharge station:')}\n\n")
#station = layout.create_station_selection_widget(station_dict)
station = pn.widgets.Select(
    name=_("Select discharge station:"),
    groups=station_dict,
    value=station_dict[next(iter(station_dict))][0],
    margin=(0, 0, 0, 0)
    )

# Print the station widget selection
#print(f"DEBUG: pentad_dashboard.py: Station widget selection: {station.value}")

# Update the model_dict with the models we have results for for the selected
# station
print("DEBUG: pentad_dashboard.py: station.value: ", station.value)
model_dict = processing.update_model_dict(model_dict_all, forecasts_all, station.value, pentad_selector.value)
#print(f"DEBUG: pentad_dashboard.py: model_dict: {model_dict}")

@pn.depends(station, pentad_selector, watch=True)
def get_best_models_for_station_and_pentad(station_value, pentad_value):
    return processing.get_best_models_for_station_and_pentad(forecasts_all, station_value, pentad_value)
current_model_pre_selection = get_best_models_for_station_and_pentad(station.value, pentad_selector.value)

# Widget for forecast model selection, only visible in forecast tab
# a given hydropost/station.
model_checkbox = pn.widgets.CheckBoxGroup(
    name=_("Select forecast model:"),
    options=model_dict,
    #value=[model_dict['Linear regression (LR)']],
    value=[model_dict[model] for model in current_model_pre_selection],
    #width=200,  # 280
    margin=(0, 0, 0, 0),
    sizing_mode='stretch_width',
    css_classes=['checkbox-label']
)
print(f"\n\n\nmodel_checkbox: {model_checkbox.value}\n\n\n")

allowable_range_selection = pn.widgets.Select(
    name=_("Select forecast range for display:"),
    options=[_("delta"), _("Manual range, select value below"), _("max[delta, %]")],
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

# Create a language selection widget
language_select = pn.widgets.Select(
    name='',
    options={'en':'en_CH', 'ru':'ru_KG', 'kg': 'ky_KG'},
    value=current_locale,
    width=50,
    css_classes=['language_button_css']
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

# Forecast card for sidepanel
forecast_model_title = pn.pane.Markdown(
    _("Select forecast model:"), margin=(0, 0, -15, 0))  # martin=(top, right, bottom, left)
range_selection_title = pn.pane.Markdown(
    _("Show ranges in figure:"), margin=(0, 0, -15, 0))
forecast_card = pn.Card(
    pn.Column(
        forecast_model_title,
        model_checkbox,
        allowable_range_selection,
        manual_range,
        range_selection_title,
        show_range_button
    ),
    title=_('Select forecasts:'),
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
# endregion


# region update_functions
@pn.depends(pentad_selector, watch=True)
def update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_,
    sites=sites_list, df=hydrograph_pentad_all, pentad=pentad_selector.value):
    """Update site attributes with hydrograph statistics for selected pentad"""
    #print(f"column names: {df.columns}")
    # Based on column names and date, figure out which column indicates the
    # last year's Q for the selected pentad
    current_year = dt.datetime.now().year
    if str(current_year) in df.columns:
        last_year_column = str(current_year - 1)
    #print(f"\n\nupdate site attributes hydrograph stats: dataframe: {df}")
    # Filter the df for the selected pentad
    df = df[df['pentad_in_year'] == pentad].copy()
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
        site.hydrograph_max = row['max'].values[0]
        site.hydrograph_min = row['min'].values[0]
        site.last_year_q_pentad_mean = row[last_year_column].values[0]
        #print(f"site: {site.code}, mean: {site.hydrograph_mean}, max: {site.hydrograph_max}, min: {site.hydrograph_min}, last year mean: {site.last_year_q_pentad_mean}")

    #print(f"Updated sites with hydrograph statistics from DataFrame.")
    return sites

@pn.depends(pentad_selector, watch=True)
def update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=linreg_predictor, pentad=pentad_selector.value):
    """Update site attributes with linear regression predictor"""
    # Get row in def for selected pentad
    df = df[df['pentad_in_year'] == pentad].copy()
    # Only keep the last row for each site
    df = df.drop_duplicates(subset='code', keep='last')
    for site in sites:
        #print(f"site: {site.code}")
        # Test if site.code is in df['code']
        if site.code not in df['code'].values:
            site.linreg_predictor = None
            continue
        # Get the linear regression predictor for each site
        row = df[df['code'] == site.code]
        site.linreg_predictor = row['predictor'].values[0]
        #print(f"site: {site.code}, linreg predictor: {site.linreg_predictor}")

    #print(f"Updated sites with linear regression predictor from DataFrame.")
    return sites

sites_list = update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=hydrograph_pentad_all, pentad=pentad_selector.value)
sites_list = update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=linreg_predictor, pentad=pentad_selector.value)

# Adding the watcher logic for disabling the "Add to Bulletin" button
def update_add_to_bulletin_button(event):
    """Update the state of 'Add to Bulletin' button based on pipeline_running status."""
    add_to_bulletin_button.disabled = event.new

# Watch for changes in pipeline_running and update the add_to_bulletin_button
viz.app_state.param.watch(update_add_to_bulletin_button, 'pipeline_running')

# Set the initial state of the button based on whether the pipeline is running
add_to_bulletin_button.disabled = viz.app_state.pipeline_running

# Function to update the spinner
#def update_indicator(event):
#    if not event:
#        return
#    indicator.value = not indicator.value

# Update the model select widget based on the selected station
@pn.depends(station, pentad_selector, watch=True)
def update_model_select(station_value, selected_pentad):
    # Update the model_dict with the models we have results for for the selected
    # station
    print(f"DEBUG: pentad_dashboard.py: update_model_select: station_value: {station_value}")
    model_dict = processing.update_model_dict(model_dict_all, forecasts_all, station_value, selected_pentad)
    model_checkbox.options = model_dict
    return model_dict


# Bind the update function to the station selector
#pn.bind(update_model_select, station, watch=True)
#pn.bind(update_model_select, pentad_selector, watch=True)

# Create the pop-up notification pane (initially hidden)
add_to_bulletin_popup = pn.pane.Alert("Added to bulletin", alert_type="success", visible=False)

# Function to handle adding the current selection to the bulletin
def add_current_selection_to_bulletin(event=None):
    if viz.app_state.pipeline_running:
        print("Cannot add to bulletin while containers are running.")
        return  # Prevent the action while containers are running
    
    # Your existing logic for adding selections to bulletin...
    selected_indices = forecast_tabulator.selection
    forecast_df = forecast_tabulator.value
    #print("\n\n\nDEBUG: pentad_dashboard.py: forecast_df:\n", forecast_df)

    if forecast_df is None or forecast_df.empty:
        print("Forecast summary table is empty.")
        logger.warning("Attempted to add to bulletin, but forecast summary table is empty.")
        return

    if not selected_indices and len(forecast_df) > 0:
        selected_indices = [0]
        forecast_tabulator.selection = selected_indices
        print("No forecast selected. Defaulting to the first forecast.")
        logger.info("No forecast selected. Defaulting to the first forecast.")

    selected_rows = forecast_df.iloc[selected_indices]
    selected_station = station.value
    selected_date = date_picker.value

    print(f"Adding station: {selected_station}, date: {selected_date}")
    print(f"Selected models:\n{selected_rows['Model']}")

    final_forecast_table = selected_rows.reset_index(drop=True)

    # Find the Site object for the selected station
    selected_site = next((site for site in sites_list if site.station_label == selected_station), None)
    if selected_site is None:
        print(f"Site {selected_station} not found in sites_list")
        print(f"sites_list: {sites_list}")
        logger.error(f"Site {selected_station} not found in sites_list")
        return

    selected_site.forecasts = final_forecast_table

    existing_site = next((site for site in bulletin_sites if site.code == selected_site.code), None)
    if existing_site is None:
        bulletin_sites.append(selected_site)
        logger.info(f"Added new site to bulletin: {selected_station}")
    else:
        existing_site.forecasts = selected_site.forecasts
        print(f"Overwritten forecast for station: {selected_station}")
        logger.info(f"Overwritten forecast for site in bulletin: {selected_station}")

    update_bulletin_table(None)

    # Show the popup notification
    add_to_bulletin_popup.visible = True
    pn.state.add_periodic_callback(lambda: setattr(add_to_bulletin_popup, 'visible', False), 2000, count=1)

# Ensure the 'Add to Bulletin' button is initially bound
add_to_bulletin_button.on_click(add_current_selection_to_bulletin)
def create_bulletin_table():
    print("Creating bulletin table...")
    bulletin_data = []
    existing_forecasts = set()
    selected_basin = select_basin_widget.value

    for site in bulletin_sites:
        # If a specific basin is selected, skip sites not in that basin
        if selected_basin != _("All basins") and site.basin_ru != selected_basin:
            continue

        if hasattr(site, 'forecasts'):
            # Assuming 'forecasts' is now a single DataFrame per site
            forecast = site.forecasts.copy()
            forecast['Hydropost'] = site.station_label
            bulletin_data.append(forecast)
        else:
            print(f"No forecasts for site {site.code}")

    if bulletin_data:
        bulletin_df = pd.concat(bulletin_data, ignore_index=True)
        columns_order = ['Hydropost'] + [col for col in bulletin_df.columns if col != 'Hydropost']
        bulletin_df = bulletin_df[columns_order]

        bulletin_tabulator = pn.widgets.Tabulator(
            bulletin_df,
            show_index=False,
            height=300,
            sizing_mode='stretch_width'
        )
    else:
        bulletin_tabulator = pn.pane.Markdown(_("No forecasts added to the bulletin."))

    print("Bulletin table: ", bulletin_tabulator)
    print("Bulletin table created.")
    return bulletin_tabulator


def update_bulletin_table(event=None):
    bulletin_table.clear()
    bulletin_table.append(create_bulletin_table())

select_basin_widget.param.watch(update_bulletin_table, 'value')

add_to_bulletin_button.on_click(add_current_selection_to_bulletin)
# endregion


# region dashboard_layout
# Dynamically update figures
date_picker_with_pentad_text = viz.create_date_picker_with_pentad_text(date_picker, _)

update_callback = viz.update_forecast_data(_, linreg_datatable, station, pentad_selector)
pentad_selector.param.watch(update_callback, 'value')

# Initial setup: populate the main area with the initial selection
#update_callback(None)  # This does not seem to be needed


daily_hydrograph_plot = pn.panel(
    pn.bind(
        viz.plot_daily_hydrograph_data,
        _, hydrograph_day_all, linreg_predictor, station, date_picker
        ),
    sizing_mode='stretch_both'
    )
daily_rainfall_plot = pn.panel(
    pn.bind(
        viz.plot_daily_rainfall_data,
        _, rain, station, date_picker, linreg_predictor
    ),
)
daily_temperature_plot = pn.panel(
    pn.bind(
        viz.plot_daily_temperature_data,
        _, temp, station, date_picker, linreg_predictor
    ),
)
'''
daily_rel_to_norm_runoff = pn.panel(
    pn.bind(
        viz.plot_rel_to_norm_runoff,
        _, hydrograph_day_all, linreg_predictor, station, date_picker
    )
)
daily_rel_to_norm_rainfall = pn.panel(
    pn.bind(
        viz.plot_daily_rel_to_norm_rainfall,
        _, rain, station, date_picker, linreg_predictor
    )
)
'''

forecast_data_and_plot = pn.panel(
    pn.bind(
        viz.select_and_plot_data,
        _, linreg_predictor, station, pentad_selector, SAVE_DIRECTORY
    ),
    sizing_mode='stretch_both'
)
pentad_forecast_plot = pn.panel(
    pn.bind(
        viz.plot_pentad_forecast_hydrograph_data,
        _, hydrograph_pentad_all, forecasts_all, station, date_picker,
        model_checkbox, allowable_range_selection, manual_range,
        show_range_button
        ),
    sizing_mode='stretch_both'
)
forecast_skill_plot = pn.panel(
    pn.bind(
        viz.plot_forecast_skill,
        _,
        hydrograph_pentad_all,
        forecasts_all,
        station_widget=station,
        date_picker=date_picker,
        model_checkbox=model_checkbox,
        range_selection_widget=allowable_range_selection,
        manual_range_widget=manual_range,
        show_range_button=show_range_button
    ),
    sizing_mode='stretch_both'
)


def update_forecast_tabulator(event=None):
    viz.create_forecast_summary_tabulator(
        _, forecasts_all, station.value, date_picker.value,
        model_checkbox.value, allowable_range_selection.value, manual_range.value,
        forecast_tabulator
    )

# Initial update
update_forecast_tabulator()


def update_visualizations():
    # Re-bind the plots to use the updated data

    viz.plot_pentad_forecast_hydrograph_data,
    _, hydrograph_pentad_all, forecasts_all, station, date_picker,
    model_checkbox, allowable_range_selection, manual_range,
    show_range_button

    update_forecast_tabulator()


def on_data_needs_reload_changed(event):
    if event.new:
        print("Triggered rerunning of forecasts.")
        try:
            load_data()
            update_visualizations()
            print("Forecasts produced and visualizations updated successfully.")
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
#print(f"DEBUG: pentad_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=sites_list,
    site_selection=station,
    tabulator=forecast_summary_table)

# Watch for changes in parameters to update the Tabulator
station.param.watch(update_forecast_tabulator, 'value')
date_picker.param.watch(update_forecast_tabulator, 'value')
model_checkbox.param.watch(update_forecast_tabulator, 'value')
allowable_range_selection.param.watch(update_forecast_tabulator, 'value')
manual_range.param.watch(update_forecast_tabulator, 'value')

# Bind the update function to the button
#pn.bind(update_indicator, write_bulletin_button, watch=True)

# Attach the function to the button click event
# Multi-threading does not seem to work
#write_bulletin_button.on_click(lambda event: thread.write_forecast_bulletin_in_background(bulletin_table, env_file_path, status))
write_bulletin_button.on_click(lambda event: write_to_excel(sites_list, bulletin_sites, bulletin_header_info, env_file_path))

# Create an icon using HTML for the select language widget
language_icon_html = pn.pane.HTML(
    '<i class="fas fa-language"></i>',
    width=20
)

# Define the disclaimer of the dashboard
disclaimer = layout.define_disclaimer(_, in_docker_flag)


# Update the layout

# Update the widgets conditional on the active tab
allowable_range_selection.param.watch(lambda event: viz.update_range_slider_visibility(
    _, manual_range, event), 'value')


# Function to update the dashboard based on selected language
def tabs_change_language(language):
    try:
        print("\nDEBUG: language: ", language)
        print("\nDEBUG: locale_dir: ", localedir)
        global _
        _ = localize.load_translation(language, localedir)
        # Print the currently selected language
        print(f"Selected language: {language}")
        return layout.define_tabs(
            _,
            daily_hydrograph_plot, daily_rainfall_plot, daily_temperature_plot,
            #daily_rel_to_norm_runoff, daily_rel_to_norm_rainfall,
            forecast_data_and_plot,
            forecast_summary_table, pentad_forecast_plot, forecast_skill_plot,
            bulletin_table, write_bulletin_button, indicator, disclaimer,
            station_card, forecast_card, add_to_bulletin_button, basin_card, pentad_card, reload_card, add_to_bulletin_popup)
    except Exception as e:
        print(f"Error in tabs_change_language: {e}")
        print(traceback.format_exc())

# Partial function to update the station widget based on the selected language
update_station = partial(layout.update_station_widget, station=station)

# Watch for language changes
localize.translation_manager.param.watch(update_station, 'language')

# Link the language_select widget to the TranslationManager
def update_language(event):
    localize.translation_manager.language = event.new

language_select.param.watch(update_language, 'value')

reload_card = viz.create_reload_button()

def sidepane_change_language(language):
    try:
        global _
        _ = localize.load_translation(language, localedir)

        # Update widgets
        #station.name = _("Select discharge station:")
        forecast_card.title = _('Select forecasts:')
        model_checkbox.name = _("Select forecast model:")
        allowable_range_selection.name = _("Select forecast range for display:")
        allowable_range_selection.options = [_("delta"), _("Manual range, select value below"), _("max[delta, %)")]
        manual_range.name = _("Manual range (%)")
        range_selection_title.object = _("Show ranges in figure:")
        show_range_button.name = _("Show ranges in figure:")
        show_range_button.options = [_("Yes"), _("No")]

        return layout.define_sidebar(_, station_card, forecast_card, basin_card,
                                     message_pane, reload_card)
    except Exception as e:
        print(f"Error in sidepane_change_language: {e}")
        print(traceback.format_exc())

# Bind the function to the language selection widget
tabs = tabs_change_language(language_select.value)
sidebar = pn.bind(sidepane_change_language, language_select.param.value)

def update_tabs(event=None):
    # Get the new tabs
    new_tabs = tabs_change_language(language_select.value)
    # Clear the container and add the new tabs
    tabs_container.clear()
    tabs_container.append(new_tabs)
    # Attach the function to the new tabs with all required arguments
    new_tabs.param.watch(
        partial(viz.update_sidepane_card_visibility, new_tabs, station_card, forecast_card, basin_card, pentad_card, reload_card),
        'active'
    )

# Call update_tabs to initialize
update_tabs()

# Define the layout
dashboard = pn.template.BootstrapTemplate(
    title=_('SAPPHIRE Central Asia - Pentadal forecast dashboard'),
    logo=icon_path,
    header=[pn.Row(pn.layout.HSpacer(), language_select)],
    sidebar=sidebar,
    collapsed_sidebar=False,
    main=tabs_container,
    favicon=icon_path
)

# Update function to change the language in the dashboard title
def update_title_language(event):
    global _
    selected_language = event.new
    _ = localize.load_translation(selected_language, localedir)
    dashboard.title = _('SAPPHIRE Central Asia - Pentadal forecast dashboard')

language_select.param.watch(update_title_language, 'value')

# Make the dashboard servable
dashboard.servable()

# Serve the dashboard
#pn.serve(dashboard)

# endregion