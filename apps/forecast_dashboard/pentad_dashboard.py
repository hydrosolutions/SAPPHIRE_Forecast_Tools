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

#import hvplot.pandas  # Enable interactive
import holoviews as hv
from scipy import stats
# Set the default extension
pn.extension('tabulator')

from src.environment import load_configuration
from src.gettext_config import configure_gettext
import src.processing as processing
import src.vizualization as viz

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

# region load_configuration

# Set primary color to be consistent with the icon color
pn.extension(global_css=[':root { --design-primary-color: #307096; }'])

# Load .env file
# Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
# file to use
in_docker_flag = load_configuration()

# Get icon path from config
icon_path = processing.get_icon_path(in_docker_flag)

# Set the browser tab icon
#pn.config.favicon = icon_path

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

# Hydroposts metadata
station_list, all_stations, station_df = processing.read_all_stations_metadata_from_file(
    hydrograph_day_all['code'].unique().tolist())

# Add the station_labels column to the hydrograph_day_all DataFrame
hydrograph_day_all = processing.add_labels_to_hydrograph(hydrograph_day_all, all_stations)
hydrograph_pentad_all = processing.add_labels_to_hydrograph(hydrograph_pentad_all, all_stations)
linreg_predictor = processing.add_labels_to_forecast_pentad_df(linreg_predictor, all_stations)
print("hydrograph_day_all:\n", hydrograph_day_all.columns)
print("hydrograph_pentad_all:\n", hydrograph_pentad_all.columns)

# endregion


# region widgets
forecast_date = linreg_predictor['date'].max().date()

date_picker = pn.widgets.DatePicker(name=_("Select date:"),
                                    start=dt.datetime((forecast_date.year-1), 1, 5).date(),
                                    end=forecast_date,
                                    value=forecast_date)

station = pn.widgets.Select(
    name=_("Select discharge station:"),
    options=station_list,
    value=station_list[0])

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
    min_height=300, sizing_mode='stretch_both'
    )
pentad_forecast_plot = pn.panel(
    pn.bind(
        viz.plot_pentad_forecast_hydrograph_data,
        _, hydrograph_pentad_all, linreg_predictor, station, date_picker
        ),
    min_height=300, sizing_mode='stretch_both'
    )

# Dynamically update sidepanel


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
         pn.Column(
        #     pn.Row(
        #        pn.Card(data_table, title=_('Data table'), collapsed=True),
        #        pn.Card(linear_regression, title=_("Linear regression"), collapsed=True)
        #        ),
        #     pn.Row(
        #         pn.Card(norm_table, title=_('Norm statistics'), sizing_mode='stretch_width'),),
        #     pn.Row(
        #         pn.Card(forecast_table, title=_('Forecast table'), sizing_mode='stretch_width')),
             pn.Row(
                 pn.Card(pentad_forecast_plot, title=_('Forecast'))
        ),
        #     pn.Row(
        #         pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods"))),
        #     pn.Row(
        #         pn.Card(pentad_skill, title=_("Forecast accuracy")))
        )
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
             pn.Row(
                 pn.Card(pentad_forecast_plot, title=_('Forecast')),
            ),
        #    pn.Row(
        #         #pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods")),
        #    ),
        #    pn.Row(
        #         #pn.Card(pentad_skill, title=_("Forecast accuracy")),
            )
        ),
        (_('Disclaimer'), footer),
        dynamic=True,
        sizing_mode='stretch_both'
    )

sidebar = pn.Column(
    pn.Row(pn.Card(station,
                   title=_('Hydropost:'),)),
    pn.Row(pn.Card(date_picker,
                   title=_('Date:'),
                   width_policy='fit', width=station.width,
                   collapsed=False)),
    #pn.Row(range_selection),
    #pn.Row(manual_range),
    #pn.Row(print_button),
    #pn.Row(pn.Card(warning_text_pane, title=_('Notifications'),
    #            width_policy='fit', width=station.width)),
)

dashboard = pn.template.BootstrapTemplate(
    title=_('SAPPHIRE Central Asia - Pentadal forecast dashboard'),
    logo=icon_path,
    sidebar=sidebar,
    collapsed_sidebar=False,
    main=tabs,
)


dashboard.servable()

# endregion