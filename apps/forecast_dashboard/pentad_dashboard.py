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

from bokeh.models import FixedTicker, CustomJSTickFormatter, LinearAxis
from bokeh.models.widgets.tables import NumberFormatter
from holoviews import streams

import numpy as np
import pandas as pd
import datetime as dt
import math
import param

import logging
from logging.handlers import TimedRotatingFileHandler

#import hvplot.pandas  # Enable interactive
import holoviews as hv
from scipy import stats
# Set the default extension
pn.extension('tabulator')

from ieasyreports.settings import ReportGeneratorSettings

# Local sources
from src.environment import load_configuration
from src.gettext_config import configure_gettext
import src.processing as processing
import src.vizualization as viz
#import src.multithreading as thread
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel

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

# Get path to .env file from the environment variable
env_file_path = os.getenv("ieasyhydroforecast_env_file_path")

# Load .env file
# Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
# file to use
in_docker_flag = load_configuration()

# Get icon path from config
icon_path = processing.get_icon_path(in_docker_flag)

# The current date is displayed as the title of each visualization.
today = dt.datetime.now()

# endregion



# region localization
# Read the locale from the environment file
current_locale = os.getenv("ieasyforecast_locale")

# Localization, translation to different languages.
localedir = os.getenv("ieasyforecast_locale_dir")

_ = configure_gettext(current_locale, localedir)

# endregion

# region load_data

# Daily runoff data
hydrograph_day_all = processing.read_hydrograph_day_data_for_pentad_forecasting()
hydrograph_pentad_all = processing.read_hydrograph_pentad_data_for_pentad_forecasting()

# Pentadal forecast data
# - linreg_predictor: for displaying predictor in predictor tab
linreg_predictor = processing.read_linreg_forecast_data()
# - forecast results from all models
forecasts_all = processing.read_forecast_results_file()
# Forecast statistics
forecast_stats = processing.read_forecast_stats_file()

# Hydroposts metadata
station_list, all_stations, station_df, station_dict = processing.read_all_stations_metadata_from_file(
    hydrograph_day_all['code'].unique().tolist())
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
#print(f"DEBUG: linreg_datatable: {linreg_datatable.tail()}")
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
print(f"DEBUG: pentad_dashboard.py: forecasts_all: columns of forecasts_all:\n{forecasts_all.columns}")
#print(f"DEBUG: pentad_dashboard.py: forecasts_all: Models in forecasts_all:\n{forecasts_all['model_long'].unique()}")
print(f"DEBUG: pentad_dashboard.py: forecasts_all: Tail of forecasts_all:\n{forecasts_all[['code', 'date', 'Q25', 'Q75', 'pentad_in_month', 'pentad_in_year']].tail()}")

#print(f"DEBUG: pentad_dashboard.py: linreg_predictor: columns of linreg_predictor:\n{linreg_predictor.columns}")
#print(f"DEBUG: pentad_dashboard.py: linreg_predictor: Tail of linreg_predictor:\n{linreg_predictor[['code', 'date', 'pentad_in_year']].tail()}")

#print(f"DEBUG: pentad_dashboard.py: linreg_datatable: columns of linreg_datatable:\n{linreg_datatable.columns}")
#print(f"DEBUG: pentad_dashboard.py: linreg_datatable: Tail of linreg_datatable:\n{linreg_datatable[['code', 'pentad_in_year']].tail()}")

# Create a list of Site objects from the all_stations DataFrame
sites_list = Site.get_site_attributes_from_stations_dataframe(all_stations)


# Create a dictionary of the model names and the corresponding model labels
model_dict_all = forecasts_all[['model_short', 'model_long']] \
    .set_index('model_long')['model_short'].to_dict()
print(f"DEBUG: pentad_dashboard.py: model_dict_all: {model_dict_all}")


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
last_date = linreg_predictor['date'].max()

# Determine the corresponding pentad
current_pentad = tl.get_pentad_for_date(last_date)

# Get information for bulletin headers into a dataframe that can be passed to
# the bulletin writer.
bulletin_header_info = processing.get_bulletin_header_info(last_date)
print(f"DEBUG: pentad_dashboard.py: bulletin_header_info:\n{bulletin_header_info}")

#print(f"DEBUG: pentad_dashboard.py: first site: {sites_list[0]}")


# Create the dropdown widget for pentad selection
pentad_selector = pn.widgets.Select(
    name="Select Pentad",
    options=pentad_options,
    value=current_pentad,  # Default to the last available pentad
    margin=(0, 0, 0, 0)
)

# Widget for station selection, always visible
station = pn.widgets.Select(
    name=_("Select discharge station:"),
    groups=station_dict,
    value=station_dict[next(iter(station_dict))][0])

# Print the station widget selection
print(f"DEBUG: pentad_dashboard.py: Station widget selection: {station.value}")

# Update the model_dict with the models we have results for for the selected
# station
model_dict = processing.update_model_dict(model_dict_all, forecasts_all, station.value)
print(f"DEBUG: pentad_dashboard.py: model_dict: {model_dict}")


# Widget for forecast model selection, only visible in forecast tab
# a given hydropost/station.
model_checkbox = pn.widgets.CheckBoxGroup(
    name=_("Select forecast model:"),
    options=model_dict,
    value=[model_dict['Linear regression (LR)']],
    #width=200,  # 280
    margin=(0, 0, 0, 0),
    sizing_mode='stretch_width',
    css_classes=['checkbox-label']
)
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

show_range_button = pn.widgets.RadioButtonGroup(
    name=_("Show ranges in figure:"),
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
indicator = pn.indicators.LoadingSpinner(value=False, size=25)

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

# endregion


# region update_functions

# Function to update the spinner
def update_indicator(event):
    if not event:
        return
    indicator.value = not indicator.value

# Update the model select widget based on the selected station
def update_model_select(station_value):
    # Update the model_dict with the models we have results for for the selected
    # station
    print(f"DEBUG: pentad_dashboard.py: update_model_select: station_value: {station_value}")
    model_dict = processing.update_model_dict(model_dict_all, forecasts_all, station_value)
    model_checkbox.options = model_dict
    return model_dict


# Bind the update function to the station selector
pn.bind(update_model_select, station, watch=True)



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
forecast_data_and_plot = pn.panel(
    pn.bind(
        viz.select_and_plot_data,
        _, linreg_predictor, station, pentad_selector
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

# TODO Implement, write to site object and display bulletin in bulletin tab
forecast_tabulator = viz.create_forecast_summary_tabulator(
    _, forecasts_all, station.value, date_picker.value, model_checkbox.value,
    allowable_range_selection.value, manual_range.value
)
print(f"DEBUG: pentad_dashboard.py: forecast_tabulator: {forecast_tabulator}")
print(f"DEBUG: type of forecast_tabulator: {type(forecast_tabulator)}")

bulletin_table = pn.panel(
    pn.bind(
        viz.create_forecast_summary_tabulator,
        _, forecasts_all, station, date_picker, model_checkbox,
        allowable_range_selection, manual_range
        ),
    sizing_mode='stretch_width'
)

# Bind the forecast summary table to the bulletin table
forecast_summary_tabulator = pn.bind(
    viz.create_forecast_summary_tabulator,
    _, forecasts_all, station, date_picker, model_checkbox,
    allowable_range_selection, manual_range
    )
forecast_summary_table = pn.panel(
    forecast_summary_tabulator,
    #pn.bind(
    #    viz.create_forecast_summary_tabulator,
    #    _, forecasts_all, station, date_picker, model_checkbox,
    #    allowable_range_selection, manual_range
    #    ),
    sizing_mode='stretch_width'
    )

# Update the site object based on site and forecast selection
print(f"DEBUG: pentad_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=sites_list,
    site_selection=station,
    tabulator=forecast_summary_tabulator)

# Watch for changes in the site selector and in the forecast tabulator
station.param.watch(update_site_object, 'value')
forecast_tabulator.param.watch(forecast_summary_table, 'selection')
forecast_tabulator.param.watch(update_site_object, 'selection')

# Bind the update function to the button
pn.bind(update_indicator, write_bulletin_button, watch=True)

# Attach the function to the button click event
# Multi-threading does not seem to work
#write_bulletin_button.on_click(lambda event: thread.write_forecast_bulletin_in_background(bulletin_table, env_file_path, status))
write_bulletin_button.on_click(lambda event: write_to_excel(sites_list, bulletin_header_info, env_file_path, indicator))


## Footer
# Define the footer of the dashboard
if in_docker_flag == "True":
    logos = pn.Row(
        pn.pane.Image(os.path.join(
            "apps", "forecast_dashboard", "www", "sapphire_project_logo.jpg"),
            width=70),
        pn.pane.Image(os.path.join(
            "apps", "forecast_dashboard", "www", "hydrosolutionsLogo.jpg"),
            width=100),
        pn.pane.Image(os.path.join(
            "apps", "forecast_dashboard", "www", "sdc.jpeg"),
            width=150))
else:
    logos = pn.Row(
        pn.pane.Image(os.path.join(
            "www", "sapphire_project_logo.jpg"),
            width=70),
        pn.pane.Image(os.path.join(
            "www", "hydrosolutionsLogo.jpg"),
            width=100),
        pn.pane.Image(os.path.join(
            "www", "sdc.jpeg"),
            width=150))

footer = pn.Column(
    pn.pane.HTML(_('disclaimer_who')),
    pn.pane.Markdown(_("disclaimer_waranty")),
    pn.pane.HTML("<p> </p>"),
    logos,
    pn.pane.Markdown(_("Last updated on ") + dt.datetime.now().strftime("%b %d, %Y") + ".")
)

# Organize the panes in tabs
no_date_overlap_flag = True
if no_date_overlap_flag == False:
    tabs = pn.Tabs(
        # Predictors tab
        (_('Predictors'),
         pn.Column(
             pn.Row(
                 pn.Card(daily_hydrograph_plot, title=_("Hydrograph"))
             ),
         ),
        ),
        (_('Forecast'),
         #pn.Column(
        #     pn.Row(
        #        pn.Card(data_table, title=_('Data table'), collapsed=True),
        #        pn.Card(linear_regression, title=_("Linear regression"), collapsed=True)
        #        ),
        #     pn.Row(
        #         pn.Card(norm_table, title=_('Norm statistics'), sizing_mode='stretch_width'),),
        #     pn.Row(
        #         pn.Card(forecast_table, title=_('Forecast table'), sizing_mode='stretch_width')),
                 pn.Card(
                     #pentad_forecast_plot,
                     title=_('Hydrograph'),
                 ),
                 pn.Card(
                     forecast_summary_table,
                     title=_('Summary table'),
                     sizing_mode='stretch_width'
                 ),
                 pn.Card(
                     daily_hydrograph_plot,
                     title=_('Analysis of the forecast'))
        #     pn.Row(
        #         pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods"))),
        #     pn.Row(
        #         pn.Card(pentad_skill, title=_("Forecast accuracy")))
        #)
        ),
        (_('Disclaimer'), footer),
        dynamic=True,
        sizing_mode='stretch_both'
    )
else: # If no_date_overlap_flag == True
    tabs = pn.Tabs(
        # Predictors tab
        (_('Predictors'),
         pn.Column(
             pn.Row(
                 pn.Card(daily_hydrograph_plot, title=_("Hydrograph")),
             ),
         ),
        ),
        (_('Forecast'),
         pn.Column(
            pn.Card(
                pn.Row(
                    forecast_data_and_plot
                ),
                title=_('Linear regression'),
                sizing_mode='stretch_width',
                collapsible=True,
                collapsed=False
            ),
            pn.Card(
                forecast_summary_table,
                title=_('Summary table'),
                sizing_mode='stretch_width',
            ),
            pn.Card(
                pentad_forecast_plot,
                title=_('Hydrograph'),
                height=500,
                collapsible=True,
                collapsed=False
            ),
                 #pn.Card(
                 #    pentad_forecast_plot,
                 #    title=_('Analysis'))
        #         #pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods")),
        #         #pn.Card(pentad_skill, title=_("Forecast accuracy")),
            )
        ),
        (_('Bulletin'),
         pn.Column(
             pn.Card(
                 pn.Column(
                     bulletin_table,
                     pn.Row(
                         write_bulletin_button,
                         indicator),
                 ),
                 title='Forecast bulletin',
            ),
         )
        ),
        (_('Disclaimer'), footer),
        dynamic=True,
        sizing_mode='stretch_both'
    )

# Sidebar

sidebar = pn.Column(
    pn.Row(pn.Card(station,
                   title=_('Hydropost:'),)),
    #pn.Row(pentad_card),
    #pn.Row(pn.Card(pentad_selector, title=_('Pentad:'))),
    #pn.Row(pn.Card(date_picker, date_picker_with_pentad_text,
                   #title=_('Date:'),
                   #width_policy='fit', width=station.width,
                   #collapsed=False)),
    pn.Row(forecast_card),
    #pn.Row(range_selection),
    #pn.Row(manual_range),
    #pn.Row(print_button),
    #pn.Row(pn.Card(warning_text_pane, title=_('Notifications'),
    #            width_policy='fit', width=station.width)),
)


# Update the layout

# Update the widgets conditional on the active tab
tabs.param.watch(lambda event: viz.update_sidepane_card_visibility(
    tabs, forecast_card, pentad_card, event), 'active')
allowable_range_selection.param.watch(lambda event: viz.update_range_slider_visibility(
    _, manual_range, event), 'value')

# Update plot if table is changed

# Define the layout
dashboard = pn.template.BootstrapTemplate(
    title=_('SAPPHIRE Central Asia - Pentadal forecast dashboard'),
    logo=icon_path,
    sidebar=sidebar,
    collapsed_sidebar=False,
    main=tabs,
    favicon=icon_path
)


dashboard.servable()

# endregion