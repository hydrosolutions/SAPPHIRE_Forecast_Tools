# forecast_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055

# =========================
# Standard library imports
# =========================
import os
import sys
import datetime as dt
from datetime import datetime, timedelta
from functools import partial
from concurrent.futures import ThreadPoolExecutor

# =========================
# Third-party imports
# =========================
import panel as pn
import pandas as pd
import holoviews as hv

# =========================
# Local application imports
# =========================
from src.gettext_config import _
import src.processing as processing
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel
import src.layout as layout
from src.file_downloader import FileDownloader
from src import db
from src.auth_utils import load_credentials, check_current_user, save_current_user, remove_current_user, log_auth_event, clear_auth_logs, check_auth_state, log_user_activity, clear_activity_log, check_recent_activity

from dashboard.logger import setup_logger
from dashboard import widgets
from dashboard.bulletin_manager import load_bulletin_from_csv, add_current_selection_to_bulletin, remove_selected_from_bulletin, handle_bulletin_write, create_bulletin_table
from dashboard.config import import_tag_library, setup_panel, setup_directories, load_env_and_icons, setup_localization


tl = import_tag_library()

logger = setup_logger()

setup_panel(pn)

env_file_path, in_docker_flag, icon_path = load_env_and_icons()

SAVE_DIRECTORY = setup_directories()

# Set time until user is logged out automatically
minutes_inactive_until_logout = int(os.getenv('ieasyforecast_minutes_inactive_until_logout', 10))
INACTIVITY_TIMEOUT = timedelta(minutes=minutes_inactive_until_logout)
last_activity_time = None

setup_localization(pn)

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

# Find out if snow data is configured and can be displayed
display_snow_data = os.getenv('ieasyhydroforecast_HRU_SNOW_DATA_DASHBOARD', 'False')

# If display_snow_data is not null or empty, and display_weather_data is True, we display snow data
if display_snow_data and display_weather_data:
    display_snow_data = True
else:
    display_snow_data = False

stations_iehhf = None

valid_codes = ['16159', '16159', '16159', '16101', '16159', '16159', '15054', '16101', '16134', '16101', '16101', '16134', '16134', '15081', '15054', '16105', '16101', '16105', '16100', '16105', '15054', '16160', '15149', '15149', '15149', '15149', '15149', '16105', '15149', '16100', '16100', '16100', '16160', '16160', '16101', '16159', '16100', '15054', '15034', '16139', '16143', '16143', '15051', '15051', '15051', '15051', '16151', '15051', '15081', '15081', '16135', '16135', '16135', '15034', '15034', '16160', '16134', '16143', '16143', '16143', '16139', '16139', '16139', '16139', '16151', '16151', '16151', '16151', '16151', '15054', '15054', '15051', '16143', '16146', '16146', '16146', '16146', '16139', '16160', '15283', '16105', '15215', '15215', '15215', '15261', '16936', '15216', '15216', '15215', '15216', '15216', '15216', '15256', '15256', '15256', '15256', '16936', '15216', '15215', '15259', '15215', '15034', '15259', '15259', '16936', '16936', '16936', '16936', '15259', '15259', '15259', '15013', '15283', '15013', '15013', '15013', '15013', '15013', '15283', '16160', '15283', '15090', '16127', '16127', '16127', '16127', '16127', '16127', '16158', '16134', '16158', '16158', '16158', '16158', '15090', '15102', '15102', '16105', '16158', '16134', '15090', '15283', '15090', '16121', '16121', '16121', '16121', '16121', '16121', '15030', '15030', '15030', '15030', '15102', '15102', '15102', '15102', '15090', '15090', '15256', '15034', '15287', '15083', '16096', '16096', '16096', '16096', '16096', '15171', '16161', '15025', '15171', '15171', '15171', '15171', '16146', '15189', '15189', '15960', '15171', '16096', '16161', '16161', '15189', '16161', '16161', '16070', '16070', '16070', '16070', '16070', '16070', '16169', '16169', '16169', '16169', '16169', '16169', '15189', '16161', '15189', '15025', '15194', '15194', '16068', '15020', '16059', '15020', '15020', '15020', '16059', '16068', '16059', '15256', '16146', '17462', '17462', '17462', '17462', '17462', '16059', '16068', '16068', '16068', '16055', '16055', '16055', '16055', '16055', '16055', '16176', '16176', '16176', '16176', '16176', '16176', '15194', '16059', '16059', '16068', '15189', '15194', '15034', '15214', '15287', '16153', '16153', '16100', '15025', '15025', '15016', '15954', '16153', '15954', '15212', '15212', '15212', '15212', '15212', '16487', '16487', '15212', '16153', '16153', '16136', '15083', '15083', '15083', '15083', '15083', '16135', '16135', '16135', '16136', '15081', '15081', '15081', '16136', '16136', '16136', '16136', '16153', '16487', '15287', '16487', '16487', '16681', '15283', '15214', '15214', '15214', '15312', '15016', '16510', '15312', '15312', '15312', '15312', '15287', '15214', '15287', '15287', '15312', '16510', '16510', '16510', '15016', '15954', '15954', '15954', '15954', '15016', '15016', '15016', '15214', '15285', '15285', '15285', '15285', '15285', '15285', '16510', '16510', '16487', '17462']
all_stations, station_dict = processing.get_all_stations_from_file(valid_codes)
iehhf_warning = None

data = db.get_data('15189', all_stations)

linreg_datatable = processing.shift_date_by_n_days(data["linreg_predictor"], 1)

rram_forecast = None

# Test if we have sites in stations_iehhf which are not present in forecasts_all
# Placeholder for a message pane
message_pane = pn.pane.Markdown("", width=300)
if stations_iehhf is not None:
    missing_sites = set(stations_iehhf) - set(data["forecasts_all"]['code'].unique())
    if missing_sites:
        missing_sites_message = f"_('WARNING: The following sites are missing from the forecast results:') {missing_sites}. _('No forecasts are currently available for these sites. Please make sure your forecast models are configured to produce results for these sites, re-run hindcasts manually and re-run the forecast.')"
        message_pane.object = missing_sites_message

# Add message to message_pane, depending on the status of recent data availability
latest_data_is_current_year = True
if not latest_data_is_current_year:
    message_pane.object += "\n\n" + _("WARNING: The latest data available is not for the current year. Forecast Tools may not have access to iEasyHydro. Please contact the system administrator.")

sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)

tabs_container = pn.Column()

# Create a dictionary of the model names and the corresponding model labels
model_dict_all = data["forecasts_all"][['model_short', 'model_long']] \
    .drop_duplicates() \
    .set_index('model_long')['model_short'].to_dict()
#print(f"DEBUG: forecast_dashboard.py: station_dict: {station_dict}")

# region widgets

# Widget for date selection, always visible
date_picker = widgets.create_date_picker(data["forecasts_all"])

# Get the last available date in the data
last_date = data["forecasts_all"]['date'].max() + dt.timedelta(days=1)

# Determine the corresponding pentad
current_pentad = tl.get_pentad_for_date(last_date)
# The forecast is produced on the day before the first day of the forecast
# pentad, therefore we add 1 to the forecast pentad in linreg_predictor to get
# the pentad of the forecast period.
horizon = os.getenv("sapphire_forecast_horizon", "pentad")
horizon_in_year = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
forecast_horizon_for_saving_bulletin = int(data["linreg_predictor"][horizon_in_year].tail(1).values[0]) + 1
forecast_year_for_saving_bulletin = last_date.year

# Get information for bulletin headers into a dataframe that can be passed to
# the bulletin writer.
bulletin_header_info = processing.get_bulletin_header_info(last_date, sapphire_forecast_horizon)
#print(f"DEBUG: forecast_dashboard.py: bulletin_header_info:\n{bulletin_header_info}")

# Create the dropdown widget for pentad selection
pentad_selector = widgets.create_pentad_selector(current_pentad)

current_decad = tl.get_decad_for_date(last_date)
# Create the dropdown widget for decad selection
decad_selector = widgets.create_decad_selector(current_decad)
print(f"   dbg: current_pentad: {current_pentad}")
print(f"   dbg: current_decad: {current_decad}")

# Widget for station selection, always visible
#print(f"\n\nDEBUG: forecast_dashboard.py: station select name string: {_('Select discharge station:')}\n\n")
station = widgets.create_station(station_dict)

# Print the station widget selection
#print(f"DEBUG: forecast_dashboard.py: Station widget selection: {station.value}")

# Update the model_dict with the models we have results for for the selected
# station
print("DEBUG: forecast_dashboard.py: station.value: ", station.value)
model_dict = processing.update_model_dict_date(model_dict_all, data["forecasts_all"], station.value, date_picker.value)
print(f"DEBUG: forecast_dashboard.py: model_dict: {model_dict}")
# Model dict can be empty if no forecasts at all are available for the selected station


#@pn.depends(station, pentad_selector, watch=True)
def get_best_models_for_station_and_pentad(station_value, pentad_value, decad_value):
    return processing.get_best_models_for_station_and_pentad(data["forecasts_all"], station_value, pentad_value, decad_value)
current_model_pre_selection = get_best_models_for_station_and_pentad(station.value, pentad_selector.value, decad_selector.value)

print(f"DEBUG: forecast_dashboard.py: model_dict: \n{model_dict}")
print(f"DEBUG: forecast_dashboard.py: current_model_pre_selection: \n{current_model_pre_selection}")

# Widget for forecast model selection, only visible in forecast tab
# a given hydropost/station.
model_checkbox = widgets.create_model_checkbox(model_dict)
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

allowable_range_selection, manual_range, show_range_button = widgets.create_range_widgets()
show_daily_data_widget = widgets.create_show_daily_data_widget()

# Write bulletin button
remove_bulletin_button, write_bulletin_button = widgets.create_bulletin_buttons()


# Create language selection buttons as links that reload the page with the selected language
language_buttons = widgets.create_language_buttons()

# Put the message into a card with visibility set to off is the message is empty

# Create a single Tabulator instance
forecast_tabulator = widgets.create_forecast_tabulator()

select_basin_widget = widgets.create_select_basin_widget(station_dict)

# endregion

# region forecast_card

# update_forecast_button = pn.widgets.Button(name=_("Apply changes"), button_type="success")
update_forecast_button = widgets.create_update_forecast_button()
# Forecast card for sidepanel
forecast_card = widgets.create_forecast_card(allowable_range_selection, manual_range, model_checkbox, show_range_button, update_forecast_button, station)


# Pentad card
pentad_card = widgets.create_pentad_card(pentad_selector, station)

station_card = widgets.create_station_card(station)

# Basin card
basin_card = widgets.create_basin_card(select_basin_widget, station)


# add_to_bulletin_button = pn.widgets.Button(name=_("Add to bulletin"), button_type="primary")
add_to_bulletin_button = widgets.create_add_to_bulletin_button()

# Initialize the bulletin_tabulator as a global Tabulator with predefined columns and grouping
bulletin_tabulator = widgets.create_bulletin_tabulator()
# endregion

#endregion


# region update_functions
# @pn.depends(pentad_selector, decad_selector, watch=True)
def update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_,
    sites=sites_list, df=data["hydrograph_pentad_all"], pentad=pentad_selector.value, decad=decad_selector.value):
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
def update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=data["linreg_predictor"], pentad=pentad_selector.value, decad=decad_selector.value):
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

sites_list = update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=data["hydrograph_pentad_all"], pentad=pentad_selector.value, decad=decad_selector.value)

#print(f"DEBUG: forecast_dashboard.py before update: linreg_predictor tail:\n{linreg_predictor.loc[linreg_predictor['code'] == '15149', ['date', 'code', 'predictor', 'pentad_in_year', 'pentad_in_month']].tail()}")
sites_list = update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=data["linreg_predictor"], pentad=pentad_selector.value, decad=decad_selector.value)


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
    # today_date = today.date()
    today_date = dt.datetime.now().date()
    filtered = data["hydrograph_day_all"][
        (data["hydrograph_day_all"]["station_labels"] == station.value) &
        (data["hydrograph_day_all"]["date"] == pd.to_datetime(today_date))
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
    filtered = data["forecasts_all"][
        (data["forecasts_all"]["station_labels"] == station.value) &
        (data["forecasts_all"]["date"] == pd.to_datetime(date_picker.value))
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
    global data
    data = db.get_data(station, all_stations)

    update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=data["hydrograph_pentad_all"], pentad=pentad_selector.value, decad=decad_selector.value)

    get_predictors_warning(station)
    get_forecast_warning(station)
    print("\n=== Starting Model Select Update ===")
    print(f"Initial widget state:")
    print(f"  Options: {model_checkbox.options}")
    print(f"  Current value: {model_checkbox.value}")

    # First get the updated model dictionary
    updated_model_dict = processing.update_model_dict_date(model_dict_all, data["forecasts_all"], station_value, date_picker.value)

    print("\nAfter update_model_dict:")
    print(f"  Updated model dict: {updated_model_dict}")

    # Get pre-selected models
    current_model_pre_selection = processing.get_best_models_for_station_and_pentad(
        data["forecasts_all"], station_value, selected_pentad, selected_decad
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
# add_to_bulletin_popup = pn.pane.Alert(_("Added to bulletin"), alert_type="success", visible=False)
add_to_bulletin_popup = widgets.create_add_to_bulletin_popup()


# Call the function to load the bulletin data
bulletin_sites = load_bulletin_from_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, SAVE_DIRECTORY, sites_list)

# Function to update the bulletin table
def update_bulletin_table(event=None):
    create_bulletin_table(bulletin_sites, select_basin_widget, bulletin_tabulator)

bulletin_table = widgets.create_bulletin_table(bulletin_tabulator, remove_bulletin_button, add_to_bulletin_popup)

update_bulletin_table()

select_basin_widget.param.watch(update_bulletin_table, 'value')

# add_to_bulletin_button.on_click(add_current_selection_to_bulletin)
add_to_bulletin_button.on_click(
    partial(
        add_current_selection_to_bulletin,
        viz=viz,
        forecast_tabulator=forecast_tabulator,
        station=station,
        sites_list=sites_list,
        add_to_bulletin_popup=add_to_bulletin_popup,
        bulletin_sites=bulletin_sites,
        forecast_year_for_saving_bulletin=forecast_year_for_saving_bulletin,
        forecast_horizon_for_saving_bulletin=forecast_horizon_for_saving_bulletin,
        save_directory=SAVE_DIRECTORY,
        update_bulletin_table=update_bulletin_table
    )
)


# Attach the remove function to the remove button click event
# remove_bulletin_button.on_click(remove_selected_from_bulletin)
remove_bulletin_button.on_click(
    partial(
        remove_selected_from_bulletin,
        bulletin_tabulator=bulletin_tabulator,
        bulletin_sites=bulletin_sites,
        add_to_bulletin_popup=add_to_bulletin_popup,
        forecast_year_for_saving_bulletin=forecast_year_for_saving_bulletin,
        forecast_horizon_for_saving_bulletin=forecast_horizon_for_saving_bulletin,
        save_directory=SAVE_DIRECTORY,
        update_bulletin_table=update_bulletin_table
    )
)
# endregion


# region dashboard_layout
# Dynamically update figures
# date_picker_with_pentad_text = viz.create_date_picker_with_pentad_text(date_picker, _)

update_callback = viz.update_forecast_data(_, linreg_datatable, station, pentad_selector)
pentad_selector.param.watch(update_callback, 'value')

# Initial setup: populate the main area with the initial selection
#update_callback(None)  # This does not seem to be needed
daily_hydrograph_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
if data["rain"] is None: 
    daily_rainfall_plot = pn.pane.Markdown(_("No precipitation data from SAPPHIRE Data Gateway available."))
    daily_temperature_plot = pn.pane.Markdown(_("No temperature data from SAPPHIRE Data Gatway available."))
else: 
    daily_rainfall_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 
    daily_temperature_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 
if data["snow_data"] is None:
    snow_plot_panes = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))
else:
    snow_plot_panes = {
        'SWE': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width"),
        'HS': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width"),
        'RoF': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width")
    }

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
        hydrograph_day_all=data["hydrograph_day_all"],
        hydrograph_pentad_all=data["hydrograph_pentad_all"],
        linreg_predictor=data["linreg_predictor"],
        forecasts_all=data["forecasts_all"],
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=rram_forecast,
        ml_forecast=data["ml_forecast"]
    )
    temp = viz.plot_forecast_skill(
        _,
        data["hydrograph_pentad_all"],
        data["forecasts_all"],
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
    viz.create_skill_table(_, data["forecast_stats"]),
    sizing_mode='stretch_width')

skill_metrics_download_filename, skill_metrics_download_button = skill_table.download_menu(
    text_kwargs={'name': _('Enter filename:'), 'value': 'forecast_skill_metrics.csv'},
    button_kwargs={'name': _('Download currently visible table')}
)


# @pn.depends(station, model_checkbox, allowable_range_selection, manual_range, watch=True)
def update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range):
    viz.create_forecast_summary_tabulator(
        _, data["forecasts_all"], station, date_picker,
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
        hydrograph_pentad_all=data["hydrograph_pentad_all"],
        forecasts_all=data["forecasts_all"],
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
        hydrograph_day_all=data["hydrograph_day_all"],
        linreg_predictor=data["linreg_predictor"],
        forecasts_all=data["forecasts_all"],
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=rram_forecast,
        ml_forecast=data["ml_forecast"]
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
forecast_summary_table = widgets.create_forecast_summary_table(forecast_tabulator)

# Update the site object based on site and forecast selection
#print(f"DEBUG: forecast_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=sites_list,
    site_selection=station,
    tabulator=forecast_summary_table)



# Use the new handler
# write_bulletin_button.on_click(handle_bulletin_write)
write_bulletin_button.on_click(
    partial(
        handle_bulletin_write,
        bulletin_sites=bulletin_sites,
        select_basin_widget=select_basin_widget,
        write_to_excel=write_to_excel,
        sites_list=sites_list,
        bulletin_header_info=bulletin_header_info,
        env_file_path=env_file_path,
        downloader=downloader
    )
)

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
username_input, password_input, login_submit_button, login_feedback = widgets.create_login_widgets()

# Create logout confirmation widgets
logout_confirm, logout_yes, logout_no = widgets.create_logout_confirm_widgets()
logout_button = widgets.create_logout_button()


# def update_last_activity():
#     global last_activity_time
#     last_activity_time = datetime.now()


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
login_form = widgets.create_login_form(username_input, password_input, login_submit_button, login_feedback)

logout_panel = widgets.create_logout_panel(logout_confirm, logout_yes, logout_no)
predictors_warning = pn.Column()
forecast_warning = pn.Column()
get_predictors_warning(station)
get_forecast_warning(station)
if iehhf_warning is not None:
    predictors_warning.append(get_pane_alert(iehhf_warning))
    forecast_warning.append(get_pane_alert(iehhf_warning))

# Create a placeholder for the dashboard content
dashboard_content = layout.define_tabs(_, predictors_warning, forecast_warning,
    daily_hydrograph_plot, daily_rainfall_plot, daily_temperature_plot, snow_plot_panes,
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
        daily_hydrograph_plot.object = viz.plot_daily_hydrograph_data(_, data["hydrograph_day_all"], data["linreg_predictor"], station.value, date_picker.value)
        if display_weather_data == True: 
            daily_rainfall_plot.object = viz.plot_daily_rainfall_data(_, data["rain"], station.value, date_picker.value, data["linreg_predictor"])
            daily_temperature_plot.object = viz.plot_daily_temperature_data(_, data["temp"], station.value, date_picker.value, data["linreg_predictor"])
        if display_snow_data == True:
            for var in data["snow_data"].keys():
                if data["snow_data"][var] is not None:
                    snow_plot_panes[var].object = viz.plot_daily_snow_data(_, data["snow_data"], var, station.value, date_picker.value, data["linreg_predictor"])
                else: 
                    snow_plot_panes[var].object = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))
    elif active_tab == 1 and latest_forecast != station.value:
        latest_forecast = station.value
        plot = viz.select_and_plot_data(_, data["linreg_predictor"], station.value, pentad_selector.value, decad_selector.value, SAVE_DIRECTORY)
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

def on_stations_loaded(fut):
    try:
        global all_stations, sites_list
        _all_stations, _station_dict = fut.result()
        print("@@@@@@@@@@ Stations loaded from iehhf")
        print(f"Number of stations loaded: {len(_all_stations) if _all_stations is not None else 0}")
        # print(type(_all_stations))
        if _all_stations is not None:
            # print("Stations: ", _all_stations)
            all_stations = _all_stations
            station.groups = _station_dict

            sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)
            sites_list = update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=data["hydrograph_pentad_all"], pentad=pentad_selector.value, decad=decad_selector.value)
            sites_list = update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=data["linreg_predictor"], pentad=pentad_selector.value, decad=decad_selector.value)

    except Exception as e:
        print(f"Failed to load stations: {e}")

executor = ThreadPoolExecutor(max_workers=1)
future = executor.submit(processing.get_all_stations_from_iehhf, valid_codes)
future.add_done_callback(on_stations_loaded)
