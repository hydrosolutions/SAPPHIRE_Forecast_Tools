# pentad_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# panel serve pentad_dashboard.py --show --autoreload --port 5008
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

from src.environment import load_configuration
from src.gettext_config import configure_gettext
import src.processing as processing
import src.vizualization as viz
import src.reports as rep

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
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

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
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# endregion

# region load_configuration

# Set primary color to be consistent with the icon color
pn.extension(global_css=[':root { --design-primary-color: #307096; }'])

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
station_list, all_stations, station_df = processing.read_all_stations_metadata_from_file(
    hydrograph_day_all['code'].unique().tolist())
print("DEBUG: pentad_dashboard.py: All stations: \n", all_stations)

# Add the station_labels column to the hydrograph_day_all DataFrame
hydrograph_day_all = processing.add_labels_to_hydrograph(hydrograph_day_all, all_stations)
hydrograph_pentad_all = processing.add_labels_to_hydrograph(hydrograph_pentad_all, all_stations)
linreg_predictor = processing.add_labels_to_forecast_pentad_df(linreg_predictor, all_stations)
linreg_datatable = processing.shift_date_by_n_days(linreg_predictor, 1)
forecasts_all = processing.add_labels_to_forecast_pentad_df(forecasts_all, all_stations)

# Replace model names with translation strings
forecasts_all = processing.internationalize_forecast_model_names(_, forecasts_all)
forecast_stats = processing.internationalize_forecast_model_names(_, forecast_stats)

# Merge forecast stats with forecasts by code and pentad_in_year and model_short
forecasts_all = forecasts_all.merge(
    forecast_stats,
    on=['code', 'pentad_in_year', 'model_short', 'model_long'],
    how='left')

# Create a dictionary of the model names and the corresponding model labels
model_dict = forecasts_all[['model_short', 'model_long']] \
    .set_index('model_long')['model_short'].to_dict()

# endregion


# region widgets

# Widget for date selection, always visible
forecast_date = linreg_predictor['date'].max().date()
date_picker = pn.widgets.DatePicker(name=_("Select date:"),
                                    start=dt.datetime((forecast_date.year-1), 1, 5).date(),
                                    end=forecast_date,
                                    value=forecast_date)

# Widget for station selection, always visible
station = pn.widgets.Select(
    name=_("Select discharge station:"),
    options=station_list,
    value=station_list[0])

# Widget for forecast model selection, only visible in forecast tab
model_checkbox = pn.widgets.CheckBoxGroup(
    name=_("Select forecast model:"),
    options=model_dict,
    value=[model_dict['Forecast models Linear regression (LR)']],
    width=280,
    margin=(0, 0, 0, 0)
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

# Write bulletin button
write_bulletin_button = pn.widgets.Button(name=_("Write bulletin"), button_type='primary')
status = pn.pane.Markdown('Status: Ready')

# endregion

# region forecast_card

# Forecast card for sidepanel
forecast_model_title = pn.pane.Markdown(
    _("Select forecast model:"), margin=(0, 0, -15, 0))  # martin=(top, right, bottom, left)
forecast_card = pn.Card(
    pn.Column(
        forecast_model_title,
        model_checkbox,
        allowable_range_selection,
        manual_range
    ),
    title=_('Configure forecasts:'),
    width_policy='fit', width=station.width,
    collapsed=False
)
# Initially hide the card
forecast_card.visible = False

# endregion


# region update_functions



# endregion


# region dashboard_layout
# Dynamically update figures
daily_hydrograph_plot = pn.panel(
    pn.bind(
        viz.plot_daily_hydrograph_data,
        _, hydrograph_day_all, linreg_predictor, station, date_picker
        ),
    sizing_mode='stretch_both'
    )
forecast_data_table = pn.panel(
    pn.bind(
        viz.draw_forecast_raw_data,
        _, linreg_datatable, station, date_picker
        ),
)
#linear_regressino_plot = pn.panel(
#    pn.bind(
#        viz.plot_linear_regression,
#        _, linreg_datatable
#    )
#)
pentad_forecast_plot = pn.panel(
    pn.bind(
        viz.plot_pentad_forecast_hydrograph_data,
        _, hydrograph_pentad_all, forecasts_all, station, date_picker,
        model_checkbox, allowable_range_selection, manual_range
        ),
    sizing_mode='stretch_both'
    )
forecast_summary_table = pn.panel(
    pn.bind(
        viz.create_forecast_summary_table,
        _, forecasts_all, station, date_picker, model_checkbox,
        allowable_range_selection, manual_range
        ),
    sizing_mode='stretch_width'
    )

bulletin_table = pn.panel(
    pn.bind(
        viz.create_forecast_summary_table,
        _, forecasts_all, station, date_picker, model_checkbox,
        allowable_range_selection, manual_range
        ),
    sizing_mode='stretch_width'
)
# Dynamically update sidepanel

# Attach the function to the button click event
write_bulletin_button.on_click(lambda event: rep.run_in_background(bulletin_table, status))


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
                     pentad_forecast_plot,
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
                    forecast_data_table,
                    #linear_regression_plot
                ),
                title=_('Linear regression'),
                sizing_mode='stretch_width',
                collapsible=True,
                collapsed=True
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
                 pn.Row(
                     pn.pane.Markdown("Placeholder for the forecast bulletin"),
                     pn.Column(
                         write_bulletin_button,
                         status),
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
    pn.Row(pn.Card(date_picker,
                   title=_('Date:'),
                   width_policy='fit', width=station.width,
                   collapsed=False)),
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
    tabs, forecast_card, event), 'active')
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