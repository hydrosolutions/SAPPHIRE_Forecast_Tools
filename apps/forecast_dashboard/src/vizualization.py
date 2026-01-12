# vizualization.py
import os
import param
import random
import sys
import math
import pandas as pd
import numpy as np
import datetime as dt
import time
import calendar
from calendar import month_abbr
import holoviews as hv
from holoviews import streams
import panel as pn
from bokeh.models import Label, Title, HoverTool, FixedTicker, FuncTickFormatter, CustomJSTickFormatter, LinearAxis, \
    NumberFormatter, DateFormatter, CustomJS
from bokeh.models.formatters import DatetimeTickFormatter
from bokeh.models.widgets.tables import CheckboxEditor, BooleanFormatter
from scipy import stats
from sklearn.linear_model import LinearRegression
from functools import partial
from dotenv import load_dotenv
import re
import docker
import threading
import platform

import logging
from contextlib import contextmanager

# Get logger
logger = logging.getLogger("vizualizations")

from .gettext_config import translation_manager, _
from . import processing
import subprocess

# Import local library
# Get the absolute path of the directory containing the current script
cwd = os.getcwd()

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', '..', 'iEasyHydroForecast')
# Test if the forecast dir exists and print a warning if it does not
if not os.path.isdir(forecast_dir):
    raise Exception("Directory not found: " + forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)
# Import the modules from the forecast library
import tag_library as tl
import forecast_library as fl

# Class to store cached plots
# Used for plot components that are expensive to generate and can be reused
_PLOT_CACHE = {}


class PlotCache:
    """Class to manage plot caching"""

    @staticmethod
    def get(key):
        """Get a cached plot"""
        return _PLOT_CACHE.get(key)

    @staticmethod
    def set(key, value):
        """Set a cached plot"""
        _PLOT_CACHE[key] = value

    @staticmethod
    def clear():
        """Clear all cached plots"""
        _PLOT_CACHE.clear()

    @staticmethod
    def contains(key):
        """Check if key exists in cache"""
        return key in _PLOT_CACHE


# Defining colors (as global variables)
# https://www.color-hex.com/color/307096
# Blue color palette for the norm runoff
runoff_full_range_color = "#eaf0f4"
runoff_90percentile_range_color = "#d5e2ea"
runoff_50percentile_range_color = "#c0d4df"
runoff_mean_color = "#307096"
runoff_norm_color = "#9F5F9F"
observed_runoff_palette = "black"
if observed_runoff_palette == "purple":
    # Purple color palette for the runoff last year and current year
    runoff_last_year_color = "#ca97b7"
    runoff_current_year_color = "#963070"
    # Green color palette for the runoff forecast
    runoff_forecast_color_list = ["#455c1d", "#536f24", "#62832a", "#709630", "#7ea936", "#8dbd3c", "#99c64d"]
elif observed_runoff_palette == "black":
    # Black and grey color palette for the runoff last year and current year
    runoff_last_year_color = "#808080"
    runoff_current_year_color = "#000000"
    # Purple color palette for the runoff forecast
    # runoff_forecast_color_list = ["#5c1d45", "#6f2453", "#832a62", "#963070", "#a9367e", "#bd3c8d", "#c64d99"]
    # Red color palette for the runoff forecast
    runoff_forecast_color_list = ["#b61212", "#ce1414", "#e51717", "#ea2b2b", "#ec4242", "#ef5959", "#f17171"]


# Update visibility of sidepane widgets
def update_sidepane_card_visibility(tabs, station_card, forecast_card, basin_card, pentad_card, reload_card, event):
    active_tab = tabs.active
    # Assuming tabs are ordered as ['Predictors', 'Forecast', 'Bulletin', 'Disclaimer']
    if active_tab == 0:  # 'Predictors' tab
        station_card.visible = True
        forecast_card.visible = False
        pentad_card.visible = False
        basin_card.visible = False
        reload_card.visible = True
    elif active_tab == 1:  # 'Forecast' tab
        station_card.visible = True
        forecast_card.visible = True
        pentad_card.visible = False
        basin_card.visible = False
        reload_card.visible = True
    elif active_tab == 2:  # 'Bulletin' tab
        station_card.visible = False
        forecast_card.visible = False
        pentad_card.visible = False
        basin_card.visible = True
        reload_card.visible = True
    else:  # 'Disclaimer' tab
        station_card.visible = False
        forecast_card.visible = False
        pentad_card.visible = False
        basin_card.visible = False
        reload_card.visible = False


def update_range_slider_visibility(_, range_slider, event):
    range_type = event.new
    if range_type == _("delta"):
        range_slider.visible = False
    else:
        range_slider.visible = True


# Customization of the Bokeh plots
def remove_bokeh_logo(plot, element):
    try:
        if hasattr(plot.state, 'toolbar') and plot.state.toolbar is not None:
            plot.state.toolbar.logo = None
    except Exception:
        # Non-fatal if toolbar structure differs
        pass


def make_frame_attribution_hook(text,
                                corner='top_left',
                                x_offset=8,
                                y_offset=8,
                                text_color='gray',
                                text_font_size='10pt',
                                text_alpha=0.7):
    """Return a hook that pins a text label to the plot frame in screen units.

    Args:
        text: The attribution text to display.
        corner: One of 'bottom_left' or 'top_left'.
        x_offset, y_offset: Pixel offsets from the chosen corner.
    """

    def hook(plot, element):
        fig = plot.state
        # Hide logo (best-effort)
        try:
            fig.toolbar.logo = None
        except Exception:
            pass

        # Determine anchor sides
        top_anchor = 'top' in corner
        left_anchor = 'left' in corner

        # Data-anchored corner: use current ranges and pixel offsets to sit inside the grid
        x_data = fig.x_range.start if left_anchor else fig.x_range.end
        y_data = fig.y_range.end if top_anchor else fig.y_range.start

        baseline = 'top' if top_anchor else 'bottom'
        align = 'left' if left_anchor else 'right'
        # For a top label, move a few pixels down into the grid (negative y offset)
        y_off = -abs(y_offset) if top_anchor else abs(y_offset)
        # For a right label, move a few pixels left into the grid (negative x offset)
        x_off = abs(x_offset) if left_anchor else -abs(x_offset)

        # Reuse existing label if present
        try:
            existing = list(fig.select(name='frame_attribution_label'))
        except Exception:
            existing = []
        if existing:
            lab = existing[0]
            lab.text = text
            lab.x = x_data
            lab.y = y_data
            lab.x_units = 'data'
            lab.y_units = 'data'
            lab.x_offset = x_off
            lab.y_offset = y_off
            lab.text_color = text_color
            lab.text_font_size = text_font_size
            lab.text_alpha = text_alpha
            lab.text_align = align
            lab.text_baseline = baseline
        else:
            lab = Label(
                name='frame_attribution_label',
                x=x_data,
                y=y_data,
                x_units='data',
                y_units='data',
                x_offset=x_off,
                y_offset=y_off,
                text=text,
                text_color=text_color,
                text_font_size=text_font_size,
                text_alpha=text_alpha,
                text_align=align,
                text_baseline=baseline,
                level='annotation',
                background_fill_color='white',
                background_fill_alpha=0.5,
                border_line_alpha=0,
            )
            try:
                fig.add_layout(lab)
            except Exception:
                pass

        # Keep the label pinned to the grid corner across pan/zoom
        try:
            cb_code = (
                'lbl.x = xr.start; lbl.y = yr.end;'
                if (left_anchor and top_anchor) else
                'lbl.x = xr.end; lbl.y = yr.end;'
                if (not left_anchor and top_anchor) else
                'lbl.x = xr.start; lbl.y = yr.start;'
                if (left_anchor and not top_anchor) else
                'lbl.x = xr.end; lbl.y = yr.start;'  # bottom_right
            ) + ' lbl.change.emit();'

            for attr in ('start', 'end'):
                fig.x_range.js_on_change(attr, CustomJS(args=dict(lbl=lab, xr=fig.x_range, yr=fig.y_range), code=cb_code))
                fig.y_range.js_on_change(attr, CustomJS(args=dict(lbl=lab, xr=fig.x_range, yr=fig.y_range), code=cb_code))
        except Exception:
            # If JS hooks fail, static placement still works
            pass

    return hook


def make_side_attribution_hook(text,
                               location='below',
                               text_color='gray',
                               text_font_size='8pt'):
    """Return a hook that adds a small Title on a plot side (above/below/left/right).

    This is very robust and always visible as part of the plot layout.
    location: one of 'above', 'below', 'left', 'right'.
    """

    def hook(plot, element):
        fig = plot.state
        try:
            fig.toolbar.logo = None
        except Exception:
            pass

        # Update existing attribution if present
        try:
            existing = [t for t in fig.select({'type': Title}) if getattr(t, 'name', None) == f'side_attribution_{location}']
        except Exception:
            existing = []
        if existing:
            t = existing[0]
            t.text = text
            t.text_color = text_color
            t.text_font_size = text_font_size
            return

        t = Title(name=f'side_attribution_{location}', text=text, text_color=text_color, text_font_size=text_font_size)
        try:
            fig.add_layout(t, location)
        except Exception:
            # Fallback to below if invalid location
            fig.add_layout(t, 'below')

    return hook


def add_custom_xticklabels_pentad(_, plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        ticks = list(range(1, 73))
        labels = {}

        for i in range(12):
            month_label = _(month_abbr[i + 1])  # Translatable short month name
            for j in range(6):
                pentad_number = i * 6 + j + 1
                labels[pentad_number] = f"{month_label}, {j + 1}" if j == 0 else f"{j + 1}"
    else:
        ticks = list(range(1, 37))
        labels = {}

        for i in range(12):
            month_label = _(month_abbr[i + 1])
            for j in range(3):
                decade_number = i * 3 + j + 1
                labels[decade_number] = f"{month_label}, {j + 1}" if j == 0 else f"{j + 1}"

    # Create a FixedTicker and a FuncTickFormatter with the specified ticks and labels
    ticker = FixedTicker(ticks=ticks)
    formatter = CustomJSTickFormatter(code="""
        var labels = %s;
        return labels[tick];
    """ % labels)

    # Set the x-axis ticker and formatter
    plot.handles['xaxis'].ticker = ticker
    plot.handles['xaxis'].major_label_overrides = labels
    plot.handles['xaxis'].formatter = formatter
    plot.handles['xaxis'].major_label_orientation = math.pi / 2


# Plots for dashboard
# region recyclable_plot_components
def plot_runoff_line(data, date_col, line_data_col, label_text, color):
    """
    Creats a line plot for runoff values. Can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    line_data_col String name of the column containing the runoff values
    label_text String legend entry for the line
    color String color of the line

    Returns:
    hv.Curve plot of the runoff values
    """

    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        logger.debug("plot_runoff_line: Data is empty")
        return hv.Curve([])

    # Return an empty plot if date_col or line_data_col only contain NaN values
    if data[date_col].isnull().all() or data[line_data_col].isnull().all():
        logger.debug("plot_runoff_line: Date or line data column only contains NaN values")
        return hv.Curve([])

    # Create the curve
    line = hv.Curve(
        data,
        kdims=[date_col],
        vdims=[line_data_col],
        label=label_text) \
        .opts(color=color,
              line_width=2,
              tools=['hover'],
              show_legend=True)

    return line


def plot_runoff_forecasts(data, date_col, forecast_data_col,
                          forecast_name_col, runoff_forecast_colors, unit_string):
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
        line_types = ['solid' for i in range(len(models))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
        line_types = ['solid']
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed']
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed', 'dotdash']
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        # latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        # legend_entry = model + ": " + latest_forecast + " " + unit_string
        legend_entry = model + ": " + _("Past forecasts")

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            (horizon.capitalize(), f'@{date_col}'),
            ('Value', f'@{forecast_data_col}'),
            ('Model', f'@{forecast_name_col}')
        ])

        # Create the curve
        line = hv.Curve(
            model_data,
            kdims=[date_col],
            vdims=[forecast_data_col, forecast_name_col],
            label=legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  line_width=2,
                  line_dash=line_types[i],
                  tools=[hover],
                  show_legend=True,
                  muted=True)

        if overlay is None:
            overlay = line
        else:
            overlay *= line

    return overlay


def plot_runoff_forecasts_steps(data, date_col, forecast_data_col,
                                forecast_name_col, runoff_forecast_colors, unit_string):
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
        line_types = ['solid' for i in range(len(models))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
        line_types = ['solid']
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed']
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed', 'dotdash']
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        # latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        # legend_entry = model + ": " + latest_forecast + " " + unit_string
        legend_entry = model + ": " + _("Past forecasts")

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            (horizon.capitalize(), f'@{date_col}'),
            ('Value', f'@{forecast_data_col}'),
            ('Model', f'@{forecast_name_col}')
        ])

        # Create the curve
        line = hv.Curve(
            model_data,
            kdims=[date_col],
            vdims=[forecast_data_col, forecast_name_col],
            label=legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  line_width=2,
                  line_dash=line_types[i],
                  interpolation='steps-mid',
                  tools=[hover],
                  show_legend=True,
                  muted=True)

        if overlay is None:
            overlay = line
        else:
            overlay *= line

    return overlay


def plot_runoff_forecasts_v2(data, date_col, forecast_data_col,
                             forecast_name_col, runoff_forecast_colors, unit_string):
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
        line_types = ['solid' for i in range(len(models))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
        line_types = ['solid']
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed']
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed', 'dotdash']
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        # latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        # legend_entry = model + ": " + latest_forecast + " " + unit_string
        legend_entry = model + ": " + _("Past forecasts")

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            (horizon.capitalize(), f'@{date_col}'),
            ('Value', f'@{forecast_data_col}'),
            ('Model', f'@{forecast_name_col}')
        ])

        # Create the curve
        if model == 'LR':
            line = hv.Curve(
                model_data,
                kdims=[date_col],
                vdims=[forecast_data_col, forecast_name_col],
                label=legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      line_width=2,
                      line_dash=line_types[i],
                      interpolation='steps-post',
                      tools=[hover],
                      show_legend=True,
                      muted=True)
        else:
            line = hv.Curve(
                model_data,
                kdims=[date_col],
                vdims=[forecast_data_col, forecast_name_col],
                label=legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      line_width=2,
                      line_dash=line_types[i],
                      interpolation='steps-post',
                      tools=[hover],
                      show_legend=True,
                      muted=True)

        if overlay is None:
            overlay = line
        else:
            overlay *= line

    return overlay


def plot_current_runoff_forecasts(data, date_col, forecast_data_col,
                                  forecast_name_col, runoff_forecast_colors, unit_string):
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    # print(f"DEBUG: MODEL: Number of models in plot_current_runoff_forecasts: {len(models)}")
    # print(f"DEBUG: MODEL: namely: {models}")
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # print(f"Debug: model_data\n{model_data}")
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        legend_entry = model + ": " + latest_forecast + " " + unit_string

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            (horizon.capitalize(), f'@{date_col}'),
            ('Value', f'@{forecast_data_col}'),
            ('Model', f'@{forecast_name_col}')
        ])

        # Create a point
        point = hv.Scatter(
            model_data,
            kdims=[date_col],
            vdims=[forecast_data_col, forecast_name_col],
            label=legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  size=7,
                  tools=[hover],
                  show_legend=True,
                  )

        if overlay is None:
            overlay = point
        else:
            overlay *= point

    return overlay


def plot_runoff_range_area(
        data, date_col, min_col, max_col, range_legend_entry, range_color):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    range_area = hv.Area(
        data,
        kdims=[date_col],
        vdims=[min_col, max_col],
        label=range_legend_entry) \
        .opts(color=range_color,
              alpha=0.5, muted_alpha=0.1,
              line_width=0,
              show_legend=True, )

    return range_area


def plot_runoff_forecast_range_area(
        data, date_col, forecast_name_col, min_col, max_col, runoff_forecast_colors, unit_string):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    # print(f"\nDEBUG: forecast_name_col: {forecast_name_col}")
    # print(f"data.columns: {data.columns}")
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        # legend_entry = model + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        legend_entry = model + ": Past forecast range"

        range_area = hv.Area(
            model_data,
            kdims=[date_col],
            vdims=[min_col, max_col],
            label=legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  alpha=0.2, muted_alpha=0.05,
                  line_width=0,
                  show_legend=True, )

        if overlay is None:
            overlay = range_area
        else:
            overlay *= range_area

    return overlay


def create_stepped_area_plot(x, y1, y2, range_color, legend_entry):
    # Convert to steps-post format
    xs = []
    y1s = []
    y2s = []

    # Convert Series to lists
    x = x.tolist()
    y1 = y1.tolist()
    y2 = y2.tolist()

    # Create steps-post interpolation
    for i in range(len(x) - 1):
        xs.extend([x[i], x[i + 1]])
        y1s.extend([y1[i], y1[i]])
        y2s.extend([y2[i], y2[i]])

    # Add last point
    xs.append(x[-1])
    y1s.append(y1[-1])
    y2s.append(y2[-1])

    # Create DataFrames for stepped lines
    df = pd.DataFrame({
        'x': xs,
        'y1': y1s,
        'y2': y2s
    })

    # Create area plot
    area = hv.Area(
        df,
        kdims=['x'],
        vdims=['y2', 'y1'],
        label=legend_entry)

    # Combine plots with styling
    plot = (area).opts(
        hv.opts.Area(
            color=range_color,
            alpha=0.2, muted_alpha=0.05,
            line_alpha=0,
            line_width=0,
            show_legend=True,
        ),
    )

    return plot


def plot_runoff_forecast_range_area_v2(
        data, date_col, forecast_name_col, min_col, max_col, runoff_forecast_colors, unit_string):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    # print(f"\nDEBUG: forecast_name_col: {forecast_name_col}")
    # print(f"data.columns: {data.columns}")
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        # legend_entry = model + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        legend_entry = model + ": Past forecast range"

        range_area = create_stepped_area_plot(
            model_data[date_col],
            model_data[min_col],
            model_data[max_col],
            runoff_forecast_color[i],
            legend_entry)
        # range_area = hv.Area(
        #    model_data,
        #    kdims=[date_col],
        #    vdims=[min_col, max_col],
        #    label=legend_entry) \
        #        .opts(color=runoff_forecast_color[i],
        #              alpha=0.2, muted_alpha=0.05,
        #              line_width=0,
        #              show_legend=True,)

        if overlay is None:
            overlay = range_area
        else:
            overlay *= range_area

    return overlay


def plot_current_runoff_forecast_range(
        data, date_col, forecast_name_col, mean_col, min_col, max_col, runoff_forecast_colors, unit_string):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    def cap_color_hook(plot, element, color):
        plot.handles["glyph"].upper_head.line_color = color
        plot.handles["glyph"].lower_head.line_color = color

    # Define jitter width (in minutes)
    jitter_width = 0.2  # Jitter by 0.2 pentads or decads

    # Apply jitter to the datetime column
    data.loc[:, date_col] = data[date_col] + np.linspace(-jitter_width, jitter_width, len(data))

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # Drop all columns except the date, mean, min, and max columns
        model_data = model_data[[date_col, mean_col, min_col, max_col]]
        # Get errors instead of lower and upper bounds for the range
        model_data[min_col] = model_data[mean_col] - model_data[min_col]
        model_data[max_col] = model_data[max_col] - model_data[mean_col]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        range_legend_entry = model + " " + _("range") + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        # print(f"Debug: model_data\n{model_data}")
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # print("LEGEND ENTRY: ", range_legend_entry)
        # print("lower_bound: ", lower_bound)
        # print("upper_bound: ", upper_bound)

        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[mean_col].iloc[-1])
        point_legend_entry = model + ": " + latest_forecast + " " + unit_string

        # Create a point
        point = hv.Scatter(
            model_data,
            kdims=[date_col],
            vdims=[mean_col],
            label=point_legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  size=7,
                  tools=['hover'],
                  show_legend=True)

        range_segment = hv.ErrorBars(
            model_data,
            label=range_legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  alpha=0.2,
                  line_width=4,
                  show_legend=True,
                  hooks=[partial(cap_color_hook, color=runoff_forecast_color[i])])

        if overlay is None:
            overlay = range_segment
            overlay *= point
        else:
            overlay *= range_segment
            overlay *= point

    return overlay


def plot_current_runoff_forecast_range_date_format(
        data, date_col, forecast_name_col, mean_col, min_col, max_col,
        runoff_forecast_colors, unit_string):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    def cap_color_hook(plot, element, color):
        plot.handles["glyph"].upper_head.line_color = color
        plot.handles["glyph"].lower_head.line_color = color

    # Define jitter width (in minutes)
    jitter_width = 2  # Jitter by 2 hours

    # Convert jitter width to timedelta
    jitter_timedelta = pd.to_timedelta(np.linspace(-jitter_width, jitter_width, len(data)), unit='h')

    # Apply jitter to the datetime column
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data.loc[:, date_col] = data[date_col] + jitter_timedelta

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        # Drop all columns except the date, mean, min, and max columns
        model_data = model_data[[date_col, mean_col, min_col, max_col]]
        # Get errors instead of lower and upper bounds for the range
        model_data[min_col] = model_data[mean_col] - model_data[min_col]
        model_data[max_col] = model_data[max_col] - model_data[mean_col]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        range_legend_entry = model + " " + _("range") + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        # print(f"Debug: model_data\n{model_data}")
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # print("LEGEND ENTRY: ", range_legend_entry)
        # print("lower_bound: ", lower_bound)
        # print("upper_bound: ", upper_bound)

        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[mean_col].iloc[-1])
        point_legend_entry = model + ": " + latest_forecast + " " + unit_string

        # print(f"\n\n\n\n\n\n\n\nDebug: latest_forecast\n{latest_forecast}")

        # Create a point
        point = hv.Scatter(
            model_data,
            kdims=[date_col],
            vdims=[mean_col],
            label=point_legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  size=7,
                  tools=['hover'],
                  show_legend=True)

        range_segment = hv.ErrorBars(
            model_data,
            label=range_legend_entry) \
            .opts(color=runoff_forecast_color[i],
                  alpha=0.2,
                  line_width=4,
                  show_legend=True,
                  hooks=[partial(cap_color_hook, color=runoff_forecast_color[i])])

        if overlay is None:
            overlay = range_segment
            overlay *= point
        else:
            overlay *= range_segment
            overlay *= point

    return overlay


def plot_current_runoff_forecast_range_date_format_v2(
        data, date_col, title_date_end, forecast_name_col, mean_col, min_col,
        max_col, runoff_forecast_colors, unit_string):
    """
    Creates an area plot for the range of runoff values. can be used for daily or pentadal data.

    Args:
    data DataFrame containing the data to be plotted
    date_col String name of the column containing the dates
    min_col String name of the column containing the minimum runoff values
    max_col String name of the column containing the maximum runoff values
    range_legend_entry String legend entry for the range area
    range_color String color of the range area

    Returns:
    hv.Area plot of the range of runoff values
    """
    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    # print(f"DEBUG: MODEL: Number of models in hydrograph plot: {len(models)}")
    # print(f"           {models}")
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in
                                                          range(len(models) - len(runoff_forecast_colors))]
        line_types = ['solid' for i in range(len(models))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
        line_types = ['solid']
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed']
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
        line_types = ['solid', 'dashed', 'dotdash']
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4],
                                 runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3],
                                 runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2],
                                 runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    def cap_color_hook(plot, element, color):
        plot.handles["glyph"].upper_head.line_color = color
        plot.handles["glyph"].lower_head.line_color = color

    # Define jitter width (in minutes)
    jitter_width = 2  # Jitter by 2 hours

    # Convert jitter width to timedelta
    jitter_timedelta = pd.to_timedelta(np.linspace(-jitter_width, jitter_width, len(data)), unit='h')

    # Apply jitter to the datetime column
    data.loc[:, date_col] = data[date_col]  # + jitter_timedelta

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model].copy()
        # Drop all columns except the date, mean, min, and max columns
        model_data = model_data[[date_col, mean_col, min_col, max_col]]
        # print('original model_data:', model_data)
        # Get errors instead of lower and upper bounds for the range
        # if model == 'LR':
        #    pass
        # else:
        #    model_data[min_col] = model_data[mean_col] - model_data[min_col]
        #    model_data[max_col] = model_data[max_col] - model_data[mean_col]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        if lower_bound is None:
            lower_bound = "N/A"
        if upper_bound is None:
            upper_bound = "N/A"
        range_legend_entry = model + " " + _("range") + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        # print(f"Debug: model_data\n{model_data}")
        # print("MODEL: ", model)
        # print("COLOR: ", runoff_forecast_color[i])
        # print("LEGEND ENTRY: ", range_legend_entry)
        # print("lower_bound: ", lower_bound)
        # print("upper_bound: ", upper_bound)

        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[mean_col].iloc[-1])
        point_legend_entry = model + ": " + latest_forecast + " " + unit_string

        if model == 'LR':
            # Duplicate the row in model_data and replace the date in the second row with the title_date_end
            # Append the last row to the DataFrame
            model_data_temp = pd.concat([model_data, model_data], ignore_index=True)
            model_data_temp.loc[model_data_temp.index[-1], date_col] = pd.Timestamp(
                title_date_end + dt.timedelta(hours=24))
            # print('\n\n\n\n\nmodel_data:\n', model_data_temp)

            point = hv.Curve(
                model_data_temp,
                kdims=[date_col],
                vdims=[mean_col],
                label=point_legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      line_width=2,
                      line_dash=line_types[i],
                      interpolation='steps-mid',
                      show_legend=True,
                      muted=False)
            range_segment = plot_runoff_range_area(
                model_data_temp, date_col, min_col, max_col, range_legend_entry,
                runoff_forecast_color[i]).opts(alpha=0.2, muted_alpha=0.05)
            lower_bound = plot_runoff_range_bound(
                model_data_temp, date_col, min_col, runoff_forecast_color[i])
            upper_bound = plot_runoff_range_bound(
                model_data_temp, date_col, max_col, runoff_forecast_color[i])
            range_segment = range_segment * lower_bound * upper_bound

        else:
            # Duplicate the row in model_data and replace the date in the second row with the title_date_end
            # Append the last row to the DataFrame
            model_data_temp = pd.concat([model_data, model_data], ignore_index=True)
            model_data_temp.loc[model_data_temp.index[-1], date_col] = pd.Timestamp(
                title_date_end + dt.timedelta(hours=24))
            # print('\n\n\n\n\nmodel_data:\n', model_data_temp)

            point = hv.Curve(
                model_data_temp,
                kdims=[date_col],
                vdims=[mean_col],
                label=point_legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      line_width=2,
                      line_dash=line_types[i],
                      interpolation='steps-mid',
                      show_legend=True,
                      muted=False)
            range_segment = plot_runoff_range_area(
                model_data_temp, date_col, min_col, max_col, range_legend_entry,
                runoff_forecast_color[i]).opts(alpha=0.2, muted_alpha=0.05)
            lower_bound = plot_runoff_range_bound(
                model_data_temp, date_col, min_col, runoff_forecast_color[i])
            upper_bound = plot_runoff_range_bound(
                model_data_temp, date_col, max_col, runoff_forecast_color[i])
            range_segment = range_segment * lower_bound * upper_bound

            '''# Create a point
            point = hv.Scatter(
            model_data,
            kdims=[date_col],
            vdims=[mean_col],
            label=point_legend_entry) \
                .opts(color=runoff_forecast_color[i],
                        size=7,
                        tools=['hover'],
                        show_legend=True)

            range_segment = hv.ErrorBars(
            model_data,
            label=range_legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      alpha=0.2,
                      line_width=4,
                      show_legend=True,
                      hooks=[partial(cap_color_hook, color=runoff_forecast_color[i])]
                )'''

        if overlay is None:
            overlay = range_segment
            overlay *= point
        else:
            overlay *= range_segment
            overlay *= point

    return overlay


def plot_runoff_range_bound(data, date_col, range_bound_col, range_color, hover_tool=True):
    """
    Creates a line plot for the range of runoff values. can be used for daily or pentadal data.
    """

    if hover_tool:
        boundary_line = hv.Curve(
            data,
            kdims=[date_col],
            vdims=[range_bound_col]) \
            .opts(
            color=range_color,
            line_width=0,
            line_alpha=0,
            show_legend=False,
            tools=['hover'])
    else:
        boundary_line = hv.Curve(
            data,
            kdims=[date_col],
            vdims=[range_bound_col]) \
            .opts(
            color=range_color,
            line_width=0,
            line_alpha=0,
            show_legend=False,
            tools=[])

    return boundary_line


def plot_runoff_range_bound_v2(data, date_col, range_bound_col, range_color, hover_tool=True):
    """
    Creates a line plot for the range of runoff values. can be used for daily or pentadal data.
    """

    if hover_tool:
        boundary_line = hv.Curve(
            data,
            kdims=[date_col],
            vdims=[range_bound_col]) \
            .opts(
            color=range_color,
            line_width=0,
            line_alpha=0,
            interpolation='steps-post',
            show_legend=False,
            tools=['hover'])
    else:
        boundary_line = hv.Curve(
            data,
            kdims=[date_col],
            vdims=[range_bound_col]) \
            .opts(
            color=range_color,
            line_width=0,
            line_alpha=0,
            interpolation='steps-post',
            show_legend=False,
            tools=[])

    return boundary_line


def create_cached_vlines(_, for_dates=True, y_text=1):
    """Create and cache vertical lines for pentad markers"""
    # Check if already in cache
    cache_key = 'vlines_dates' if for_dates else 'vlines_pentad'

    if PlotCache.contains(cache_key):
        return PlotCache.get(cache_key)

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if for_dates:
        if horizon == "pentad":
            days = [1, 6, 11, 16, 21, 26]
            labels_per_month = ['1', '2', '3', '4', '5', '6'] * 12
            position = 2.2
        else:
            days = [1, 11, 21]
            labels_per_month = ['1', '2', '3'] * 12
            position = 5
        # Create lines for date-based plots
        year = dt.datetime.now().year
        # Generate list of datetime objects for the chosen forecast horizon
        time_horizons = [dt.datetime(year, month, day) for month in range(1, 13) for day in days]

        # Create vertical lines
        path_data = []
        for i in range(len(time_horizons)):
            if i % 2 == 0:
                path_data.append((time_horizons[i], -100))  # Start at -100
                path_data.append((time_horizons[i], 6000))  # Move to 3000
            else:
                path_data.append((time_horizons[i], 6000))  # Stay at 3000
                path_data.append((time_horizons[i], -100))  # Move to -100
        vlines = hv.Path(path_data).opts(
            color='gray', line_width=1,
            line_dash='dotted', line_alpha=0.5,
            show_legend=False
        )

        # Add text labels
        labels_df = pd.DataFrame({
            'x': [date + dt.timedelta(days=position) for date in time_horizons],
            'y': [y_text for _ in time_horizons],
            'text': labels_per_month
        })
        text_overlay = hv.Labels(labels_df, ['x', 'y'], 'text').opts(
            text_baseline='bottom', text_align='center',
            text_font_size='9pt', text_color='gray',
            text_alpha=0.5, text_font_style='italic',
            show_legend=False
        )

    else:
        # Create lines for pentad-number-based plots
        pentad_numbers = range(1, 73)

        # Create vertical lines
        vlines = hv.Overlay([
            hv.VLine(pentad).opts(color='gray', line_width=1,
                                  line_dash='dotted', line_alpha=0.5,
                                  show_legend=False)
            for pentad in pentad_numbers
        ])

        # Add text labels
        text_overlay = hv.Overlay([
            hv.Text(pentad + 0.5, y_text, str((pentad - 1) % 6 + 1)).opts(
                text_baseline='bottom', text_align='center',
                text_font_size='9pt', text_color='gray',
                text_alpha=0.5, text_font_style='italic',
                show_legend=False)
            for pentad in pentad_numbers
        ])

    # Combine lines and text
    combined = vlines * text_overlay

    # Store in cache
    PlotCache.set(cache_key, combined)

    return combined

def create_cached_vlines_hs_special_case(_, for_dates=True, y_text=0.01):
    """Create and cache vertical lines for pentad markers"""
    # Check if already in cache
    cache_key = 'vlines_dates_hs_special_case' if for_dates else 'vlines_pentad_hs_special_case'

    if PlotCache.contains(cache_key):
        return PlotCache.get(cache_key)

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if for_dates:
        if horizon == "pentad":
            days = [1, 6, 11, 16, 21, 26]
            labels_per_month = ['1', '2', '3', '4', '5', '6'] * 12
            position = 2.2
        else:
            days = [1, 11, 21]
            labels_per_month = ['1', '2', '3'] * 12
            position = 5
        # Create lines for date-based plots
        year = dt.datetime.now().year
        # Generate list of datetime objects for the chosen forecast horizon
        time_horizons = [dt.datetime(year, month, day) for month in range(1, 13) for day in days]

        # Create vertical lines
        path_data = []
        for i in range(len(time_horizons)):
            if i % 2 == 0:
                path_data.append((time_horizons[i], -1))  # Start at -1
                path_data.append((time_horizons[i], 6))  # Move to 6
            else:
                path_data.append((time_horizons[i], 6))  # Stay at 6
                path_data.append((time_horizons[i], -1))  # Move to -1
        vlines = hv.Path(path_data).opts(
            color='gray', line_width=1,
            line_dash='dotted', line_alpha=0.5,
            show_legend=False
        )

        # Add text labels
        labels_df = pd.DataFrame({
            'x': [date + dt.timedelta(days=position) for date in time_horizons],
            'y': [y_text for _ in time_horizons],
            'text': labels_per_month
        })
        text_overlay = hv.Labels(labels_df, ['x', 'y'], 'text').opts(
            text_baseline='bottom', text_align='center',
            text_font_size='9pt', text_color='gray',
            text_alpha=0.5, text_font_style='italic',
            show_legend=False
        )

    else:
        # Create lines for pentad-number-based plots
        pentad_numbers = range(1, 73)

        # Create vertical lines
        vlines = hv.Overlay([
            hv.VLine(pentad).opts(color='gray', line_width=1,
                                  line_dash='dotted', line_alpha=0.5,
                                  show_legend=False)
            for pentad in pentad_numbers
        ])

        # Add text labels
        text_overlay = hv.Overlay([
            hv.Text(pentad + 0.5, y_text, str((pentad - 1) % 6 + 1)).opts(
                text_baseline='bottom', text_align='center',
                text_font_size='9pt', text_color='gray',
                text_alpha=0.5, text_font_style='italic',
                show_legend=False)
            for pentad in pentad_numbers
        ])

    # Combine lines and text
    combined = vlines * text_overlay

    # Store in cache
    PlotCache.set(cache_key, combined)

    return combined

def update_pentad_text(date_picker, _):
    """
    Function to calculate and return the dynamic pentad text based on the selected date.
    """
    # Calculate the next day's date (to align with the logic used earlier)
    selected_date = date_picker

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        # Calculate pentad, month, and day range
        title_pentad = tl.get_pentad(selected_date)
        title_month = tl.get_month_str_case2_viz(_, selected_date)
        title_day_start = tl.get_pentad_first_day(selected_date.strftime("%Y-%m-%d"))
        title_day_end = tl.get_pentad_last_day(selected_date.strftime("%Y-%m-%d"))
        title_year = selected_date.year

        # Create the formatted text for display
        text = (f"**{_('Selected Pentad')}**: {title_pentad} {_('pentad')} {_('of')} "
                f"{title_month} {title_year} "
                f"({_('days')} {title_day_start}-{title_day_end})")
    else:
        title_decad = tl.get_decad_in_month(selected_date)
        title_month = tl.get_month_str_case2_viz(_, selected_date)
        title_day_start = tl.get_decad_first_day(selected_date.strftime("%Y-%m-%d"))
        title_day_end = tl.get_decad_last_day(selected_date.strftime("%Y-%m-%d"))
        title_year = selected_date.year

        # Create the formatted text for display
        text = (f"**{_('Selected Pentad')}**: {title_decad} {_('pentad')} {_('of')} "
                f"{title_month} {title_year} "
                f"({_('days')} {title_day_start}-{title_day_end})")

    return text


def create_date_picker_with_pentad_text(date_picker, _):
    # Create a Markdown pane to display the dynamic pentad text
    pentad_text_pane = pn.pane.Markdown("")

    # Define the callback function to update the pentad text when the date changes
    def update_pentad_widget(event=None):
        # Get the updated pentad text from the visualization function
        new_text = update_pentad_text(date_picker.value, _)

        # Update the Markdown pane's object with the new text
        pentad_text_pane.object = new_text

    # Bind the update function to the DatePicker's value change
    date_picker.param.watch(update_pentad_widget, 'value')

    # Call the update function once initially to set the text
    update_pentad_widget()

    # Return the layout with the DatePicker and the dynamic text pane
    return pentad_text_pane


# endregion


# region predictor_tab
def plot_daily_hydrograph_data(_, hydrograph_day_all, linreg_predictor, station, title_date):
    # print(f"\n\nDEBUG: plot_daily_hydrograph_data")
    # print(f"title_date: {title_date}")

    # Custom hover tool tip for the daily hydrograph
    date_col = _('date column name')
    mean_col = _('mean column name')
    last_year_col = _('Last year column name')
    current_year_col = _('Current year column name')
    min_col = _('min column name')
    max_col = _('max column name')
    q05_col = _('5% column name')
    q95_col = _('95% column name')
    q25_col = _('25% column name')
    q75_col = _('75% column name')

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    title_pentad = tl.get_pentad(title_date + dt.timedelta(days=1))
    title_month = tl.get_month_str_case2_viz(_, title_date)
    # print(f"title_pentad: {title_pentad}")
    # print(f"title_month: {title_month}")

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station].copy()
    # print("\n\n\ncolumns of data: ", data.columns)
    # print("head and tail of data: \n", data.head(), "\n", data.tail())
    current_year = int(data['date'].dt.year.max())
    last_year = current_year - 1

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        period = "3 day sum"
    else:
        period = "10 day average"

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    predictor_string = _(f"Current year, {period}: ") + f"{linreg_predictor['predictor'].values[0]}" + " " + _("m3/s")
    forecast_string = _("Forecast horizon for ") + title_pentad + _(" pentad of ") + title_month

    # Rename columns to be used in the plot to allow internationalization
    data = data.rename(columns={
        'date': date_col,
        'day_of_year': _('day_of_year column name'),
        'min': min_col,
        'max': max_col,
        '5%': q05_col,
        '95%': q95_col,
        '25%': q25_col,
        '75%': q75_col,
        'mean': mean_col,
        str(last_year): last_year_col,
        str(current_year): current_year_col
    })
    linreg_predictor = linreg_predictor.rename({
        'date': date_col,
        'day_of_year': _('day_of_year column name'),
        'predictor': _('Predictor column name')
    })

    # Create a holoviews bokeh plots of the daily hydrograph
    hvspan_predictor = hv.VSpan(
        linreg_predictor['predictor_start_date'].values[0],
        linreg_predictor['predictor_end_date'].values[0]) \
        .opts(color=runoff_current_year_color, alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=True)

    hvspan_forecast = hv.VSpan(
        linreg_predictor['forecast_start_date'].values[0],
        linreg_predictor['forecast_end_date'].values[0]) \
        .opts(color=runoff_forecast_color_list[3], alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=False)

    vlines = create_cached_vlines(_, for_dates=True)

    full_range_area = plot_runoff_range_area(
        data, date_col, min_col, max_col, _("Full range legend entry"),
        runoff_full_range_color)
    lower_bound = plot_runoff_range_bound(
        data, date_col, min_col, runoff_full_range_color)
    upper_bound = plot_runoff_range_bound(
        data, date_col, max_col, runoff_full_range_color)

    area_05_95 = plot_runoff_range_area(
        data, date_col, q05_col, q95_col,
        _("90-percentile range legend entry"), runoff_90percentile_range_color)
    line_05 = plot_runoff_range_bound(
        data, date_col, q05_col, runoff_90percentile_range_color)
    line_95 = plot_runoff_range_bound(
        data, date_col, q95_col, runoff_90percentile_range_color)

    area_25_75 = plot_runoff_range_area(
        data, date_col, q25_col, q75_col,
        _("50-percentile range legend entry"), runoff_50percentile_range_color)
    line_25 = plot_runoff_range_bound(
        data, date_col, q25_col, runoff_50percentile_range_color)
    line_75 = plot_runoff_range_bound(
        data, date_col, q75_col, runoff_50percentile_range_color)

    mean = plot_runoff_line(
        data, date_col, mean_col, _('Mean legend entry'), runoff_mean_color)
    last_year = plot_runoff_line(
        data, date_col, last_year_col,
        _('Last year legend entry'), runoff_last_year_color)
    current_year = plot_runoff_line(
        data, date_col, current_year_col,
        predictor_string,
        # _('Current year legend entry'),
        runoff_current_year_color)

    # Overlay the plots
    daily_hydrograph = full_range_area * lower_bound * upper_bound * \
                       area_05_95 * line_05 * line_95 * \
                       area_25_75 * line_25 * line_75 * \
                       vlines * \
                       last_year * hvspan_forecast * hvspan_predictor * \
                       mean * current_year

    daily_hydrograph.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Discharge (m³/s)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, data[max_col].max() * 1.1),
        tools=['hover'],
        toolbar='right')

    # print("\n\n")

    return daily_hydrograph


def get_copyright_text(data, station_data, column):
    # Create copyright text using plot bounds for better "fixed" positioning
    # Use the xlim and ylim values that you're setting in figure.opts()
    plot_date_min = min(data['date'])
    plot_date_max = max(data['date'])
    plot_precip_max = max([station_data[column].max(), station_data[f'{column}_norm'].max()]) * 1.1  # Same as ylim

    # Calculate position based on plot bounds (not just data bounds)
    if hasattr(plot_date_min, 'to_pydatetime'):
        date_range = (plot_date_max - plot_date_min).total_seconds()
        copyright_x = plot_date_min + pd.Timedelta(seconds=date_range * 0.02)  # 2% from left
    else:
        date_range = (plot_date_max - plot_date_min).total_seconds()
        copyright_x = plot_date_min + dt.timedelta(seconds=date_range * 0.02)  # 2% from left

    # Position near top of the plot area
    copyright_y = plot_precip_max - 5  # 5 units from top

    # Keep returning an empty overlay; actual frame-pinned label is added via a hook
    return hv.Overlay([])


def plot_daily_rainfall_data(_, daily_rainfall, station, date_picker,
                             linreg_predictor):
    # Extract code from station
    station_code = station.split(' - ')[0]

    # Convert date column to datetime
    daily_rainfall['date'] = pd.to_datetime(daily_rainfall['date'])

    # Convert date_picker to datetime[ns]
    date_picker = pd.to_datetime(date_picker)

    # Filter data for the selected station
    station_data = daily_rainfall[daily_rainfall['code'] == station_code].copy()
    # print(f"Tail of station_data\n{station_data.tail(10)}")

    # Return an empty plot if the data DataFrame is empty
    if station_data.empty:
        return hv.Curve([]). \
            opts(title=_("No data available for this station"),
                 hooks=[remove_bokeh_logo])

    # Get the forecasts for the selected date
    forecasts = station_data[station_data['date'] >= date_picker].copy()
    # print(f"Head of P forecasts\n{forecasts.head(10)}")
    # print(f"Tail of P forecasts\n{forecasts.tail(10)}")

    # Get current year rainfall
    station_data['year'] = pd.to_datetime(station_data['date']).dt.year
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    # Sort by date
    current_year = current_year.sort_values('date')
    # print(f"Tail of current_year\n{current_year.tail(10)}")

    # Accumulate rainfall over the predictor period
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)
    predictor_start_date = linreg_predictor['predictor_start_date'].values[0]
    predictor_end_date = linreg_predictor['predictor_end_date'].values[0]
    predictor_rainfall = current_year[(current_year['date'] >= predictor_start_date) &
                                      (current_year['date'] <= predictor_end_date)].copy()

    norm_rainfall = station_data.drop(columns=['P']).rename(columns={'P_norm': 'P'}).copy()
    '''
    # Calculate norm rainfall, excluding the current year
    norm_rainfall = station_data[station_data['year'] != date_picker.year].copy()
    norm_rainfall['doy'] = pd.to_datetime(norm_rainfall['date']).dt.dayofyear
    norm_rainfall = norm_rainfall.groupby(['code', 'doy']).mean().reset_index()
    norm_rainfall['date'] = pd.to_datetime(norm_rainfall['date'], format='%Y-%m-%d', errors='coerce').dt.date
    # Replace year of date with the current year
    current_year_value = date_picker.year
    norm_rainfall['date'] = norm_rainfall['doy'].apply(lambda x: (dt.datetime(current_year_value, 1, 1) + pd.Timedelta(days=x-1)).date())
    # Drop doy and year columns and sort by date
    norm_rainfall = norm_rainfall.drop(columns=['doy', 'year']).sort_values('date')
    # Convert date column to datetime
    norm_rainfall['date'] = pd.to_datetime(norm_rainfall['date'])
    '''

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        current_period = "3 day sum"
        forecast_period = "5 day sum"
    else:
        current_period = "10 day sum"
        forecast_period = "10 day sum"

    # Plot the daily rainfall data using holoviews
    title_text = f"{_('Daily precipitation sums for basin of')} {station} {_('on')} {date_picker.strftime('%Y-%m-%d')}"
    current_year_text = f"{_(f'Current year, {current_period}: ')}{predictor_rainfall['P'].sum().round()} mm"
    forecast_text = f"{_(f'Precipitation forecast, {forecast_period}: ')} {forecasts['P'].sum().round()} mm"

    hvspan_predictor = hv.VSpan(
        linreg_predictor['predictor_start_date'].values[0],
        linreg_predictor['predictor_end_date'].values[0]) \
        .opts(color=runoff_current_year_color, alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=True)

    hvspan_forecast = hv.VSpan(
        linreg_predictor['forecast_start_date'].values[0],
        linreg_predictor['forecast_end_date'].values[0]) \
        .opts(color=runoff_forecast_color_list[3], alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=False)

    vlines = create_cached_vlines(_, for_dates=True, y_text=station_data['P'].max() * 1.05)

    # A bar plot for the norm rainfall
    hv_norm_rainfall = hv.Curve(
        norm_rainfall,
        kdims='date',
        vdims='P',
        label=_('Norm'))
    hv_norm_rainfall.opts(
        interpolation='steps-mid',
        color=runoff_mean_color,
        show_legend=True)
    # A bar plot for the current_year rainfall
    hv_current_year = hv.Curve(
        current_year,
        kdims='date',
        vdims='P',
        label=current_year_text)
    hv_current_year.opts(
        interpolation='steps-mid',
        color=runoff_current_year_color,
        show_legend=True)
    # Build an attribution hook that pins the label to the plot frame
    attribution_hook = make_frame_attribution_hook(
        text="Data: ECMWF IFS HRES via Open Data",
        corner='top_left',
        x_offset=10,
        y_offset=10,
        text_color='gray',
        text_font_size='10pt',
        text_alpha=0.7
    )
    # A bar plot for the forecasted rainfall
    # if forecasts is not empty
    if not forecasts.empty:
        hv_forecast = hv.Curve(
            forecasts,
            kdims='date',
            vdims='P',
            label=forecast_text)
        hv_forecast.opts(
            interpolation='steps-mid',
            color=runoff_forecast_color_list[3],
            show_legend=True)
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm_rainfall * hv_current_year * hv_forecast
    else:
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm_rainfall * hv_current_year

    figure.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Precipitation (mm/d)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo, attribution_hook],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, max([station_data['P'].max(), station_data['P_norm'].max()]) * 1.1),
        xlim=(min(norm_rainfall['date']), max(norm_rainfall['date'])),
        tools=['hover'],
        toolbar='right',
        shared_axes=False
    )

    return figure


def plot_daily_temperature_data(_, daily_rainfall, station, date_picker,
                                linreg_predictor):
    # Extract code from station
    station_code = station.split(' - ')[0]

    # Convert date column to datetime
    daily_rainfall['date'] = pd.to_datetime(daily_rainfall['date'])

    # Convert date_picker to datetime[ns]
    date_picker = pd.to_datetime(date_picker)

    # Filter data for the selected station
    station_data = daily_rainfall[daily_rainfall['code'] == station_code].copy()
    # print(f"Tail of station_data\n{station_data.tail(10)}")

    # Return an empty plot if the data DataFrame is empty
    if station_data.empty:
        return hv.Curve([]). \
            opts(title=_("No data available for this station"),
                 hooks=[remove_bokeh_logo])

    # Get the forecasts for the selected date
    forecasts = station_data[station_data['date'] >= date_picker].copy()
    # print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Get current year rainfall
    station_data['year'] = pd.to_datetime(station_data['date']).dt.year
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    # Sort by date
    current_year = current_year.sort_values('date')
    # print(f"Tail of current_year\n{current_year.tail(10)}")

    # Accumulate rainfall over the predictor period
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)
    predictor_start_date = linreg_predictor['predictor_start_date'].values[0]
    predictor_end_date = linreg_predictor['predictor_end_date'].values[0]
    predictor_rainfall = current_year[(current_year['date'] >= predictor_start_date) &
                                      (current_year['date'] <= predictor_end_date)].copy()

    norm_rainfall = station_data.drop(columns=['T']).rename(columns={'T_norm': 'T'}).copy()
    '''
    # Calculate norm rainfall, excluding the current year
    norm_rainfall = station_data[station_data['year'] != date_picker.year].copy()
    norm_rainfall['doy'] = pd.to_datetime(norm_rainfall['date']).dt.dayofyear
    norm_rainfall = norm_rainfall.groupby(['code', 'doy']).mean().reset_index()
    # Replace year of date with the current year
    current_year_value = date_picker.year
    norm_rainfall['date'] = norm_rainfall['doy'].apply(lambda x: (dt.datetime(current_year_value, 1, 1) + pd.Timedelta(days=x-1)).date())
    # Drop doy and year columns and sort by date
    norm_rainfall = norm_rainfall.drop(columns=['doy', 'year']).sort_values('date')
    # Convert date column to datetime
    norm_rainfall['date'] = pd.to_datetime(norm_rainfall['date'])
    '''

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        current_period = _("3 day mean")
        forecast_period = _("5 day mean")
    else:
        current_period = _("10 day mean")
        forecast_period = _("10 day mean")

    # Plot the daily rainfall data using holoviews
    title_text = f"{_('Daily average temperature for basin of')} {station} {_('on')} {date_picker.strftime('%Y-%m-%d')}"
    current_year_text = f"{_('Current year')}, {current_period}: {predictor_rainfall['T'].mean()} °C"
    forecast_text = f"{_('Forecast')}, {forecast_period}: {forecasts['T'].mean()} °C"

    hvspan_predictor = hv.VSpan(
        linreg_predictor['predictor_start_date'].values[0],
        linreg_predictor['predictor_end_date'].values[0]) \
        .opts(color=runoff_current_year_color, alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=True)

    hvspan_forecast = hv.VSpan(
        linreg_predictor['forecast_start_date'].values[0],
        linreg_predictor['forecast_end_date'].values[0]) \
        .opts(color=runoff_forecast_color_list[3], alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=False)

    vlines = create_cached_vlines(_, for_dates=True, y_text=station_data['T'].max() * 1.05)

    # A bar plot for the norm rainfall
    hv_norm_rainfall = hv.Curve(
        norm_rainfall,
        kdims='date',
        vdims='T',
        label=_('Norm'))
    hv_norm_rainfall.opts(
        interpolation='linear',
        color=runoff_mean_color,
        show_legend=True)
    # A bar plot for the current_year rainfall
    hv_current_year = hv.Curve(
        current_year,
        kdims='date',
        vdims='T',
        label=current_year_text)
    hv_current_year.opts(
        interpolation='linear',
        color=runoff_current_year_color,
        show_legend=True)
    # Build an attribution hook that pins the label to the plot frame
    attribution_hook = make_frame_attribution_hook(
        text="Data: ECMWF IFS HRES via Open Data",
        corner='top_left',
        x_offset=10,
        y_offset=10,
        text_color='gray',
        text_font_size='10pt',
        text_alpha=0.7
    )
    # A bar plot for the forecasted rainfall
    # if forecasts is not empty
    if not forecasts.empty:
        hv_forecast = hv.Curve(
            forecasts,
            kdims='date',
            vdims='T',
            label=forecast_text)
        hv_forecast.opts(
            interpolation='linear',
            color=runoff_forecast_color_list[3],
            show_legend=True)
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm_rainfall * hv_current_year * hv_forecast
    else:
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm_rainfall * hv_current_year

    figure.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Temperature (°C)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo, attribution_hook],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(min([station_data['T'].min(), station_data['T_norm'].min()]) * 0.9,
              max([station_data['T'].max(), station_data['T_norm'].max()]) * 1.1),
        xlim=(min(norm_rainfall['date']), max(norm_rainfall['date'])),
        tools=['hover'],
        toolbar='right',
        shared_axes=False
    )

    return figure

def plot_daily_snow_data(_, snow_data, variable, station, date_picker, linreg_predictor):
    """
    Plot snow data for a specific variable.
    """
    # Get the data for this variable
    daily_snow = snow_data.get(variable)
    
    # Return empty plot if no data for this variable
    if daily_snow is None or daily_snow.empty:
        return hv.Curve([]).opts(
            title=_(f"No {variable} data available"),
            hooks=[remove_bokeh_logo])
    
    # Variable-specific settings
    variable_config = {
        'SWE': {'label': _('Snow Water Equivalent'), 'unit': 'mm', 'ylabel': _('SWE (mm)'), 'decimals': 1},
        'HS': {'label': _('Snow Height'), 'unit': 'm', 'ylabel': _('Snow Height (m)'), 'decimals': 2},
        'RoF': {'label': _('Snowmelt Runoff & P Runoff'), 'unit': 'mm', 'ylabel': _('Snowmelt & P Runoff (mm)'), 'decimals': 1}
    }
    config = variable_config.get(variable, {'label': variable, 'unit': '', 'ylabel': variable, 'decimals': 1})
    
    # Extract code from station
    station_code = station.split(' - ')[0]

    # Convert date column to datetime
    daily_snow['date'] = pd.to_datetime(daily_snow['date'])

    # Convert date_picker to datetime[ns]
    date_picker = pd.to_datetime(date_picker)

    # Filter data for the selected station
    station_data = daily_snow[daily_snow['code'] == station_code].copy()

    # Return an empty plot if the data DataFrame is empty
    if station_data.empty:
        return hv.Curve([]).opts(
            title=_("No data available for this station"),
            hooks=[remove_bokeh_logo])

    # Add year and day of year columns (do this once)
    station_data['year'] = station_data['date'].dt.year
    station_data['doy'] = station_data['date'].dt.dayofyear

    # Get current year data
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    current_year = current_year.sort_values('date')

    # Get the forecasts for the selected date
    forecasts = current_year[current_year['date'] >= date_picker].copy()

    # Calculate norm: mean by day-of-year, excluding current year
    historical_data = station_data[station_data['year'] != date_picker.year]
    norm_snow = historical_data.groupby('doy')[variable].mean().reset_index()
    
    # Map doy back to dates in the current year for plotting
    current_year_value = date_picker.year
    norm_snow['date'] = norm_snow['doy'].apply(
        lambda x: pd.Timestamp(current_year_value, 1, 1) + pd.Timedelta(days=x - 1))
    norm_snow = norm_snow.sort_values('date')

    # Get predictor period data
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)
    predictor_start_date = linreg_predictor['predictor_start_date'].values[0]
    predictor_end_date = linreg_predictor['predictor_end_date'].values[0]
    predictor_snow = current_year[(current_year['date'] >= predictor_start_date) &
                                  (current_year['date'] <= predictor_end_date)].copy()

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        current_period = _("3 day mean")
        forecast_period = _("5 day mean")
    else:
        current_period = _("10 day mean")
        forecast_period = _("10 day mean")

    # Plot title and labels
    title_text = f"{config['label']} {_('for basin of')} {station} {_('on')} {date_picker.strftime('%Y-%m-%d')}"
    mean_value = predictor_snow[variable].mean()
    decimals = config['decimals']
    current_year_text = f"{_('Current year')}, {current_period}: {mean_value:.{decimals}f} {config['unit']}" if not pd.isna(mean_value) else _('Current year')

    # Forecast label
    forecast_mean = forecasts[variable].mean() if not forecasts.empty else float('nan')
    forecast_text = f"{_('Forecast')}, {forecast_period}: {forecast_mean:.{decimals}f} {config['unit']}" if not pd.isna(forecast_mean) else _('Forecast')

    hvspan_predictor = hv.VSpan(
        linreg_predictor['predictor_start_date'].values[0],
        linreg_predictor['predictor_end_date'].values[0]) \
        .opts(color=runoff_current_year_color, alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=True)

    hvspan_forecast = hv.VSpan(
        linreg_predictor['forecast_start_date'].values[0],
        linreg_predictor['forecast_end_date'].values[0]) \
        .opts(color=runoff_forecast_color_list[3], alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=False)

    # Calculate y-axis limits safely
    all_values = pd.concat([current_year[variable], norm_snow[variable]]).dropna()
    if not all_values.empty:
        y_min = all_values.min() * 0.9
        y_max = all_values.max() * 1.1
    else:
        y_min, y_max = 0, 1

    if variable == 'HS':
        vlines = create_cached_vlines_hs_special_case(_, for_dates=True)
    else:
        vlines = create_cached_vlines(_, for_dates=True, y_text=y_min * 1.05)

    # Norm curve
    hv_norm = hv.Curve(
        norm_snow,
        kdims='date',
        vdims=variable,
        label=_('Norm'))
    hv_norm.opts(
        interpolation='linear',
        color=runoff_mean_color,
        show_legend=True)

    # Current year curve
    hv_current_year = hv.Curve(
        current_year,
        kdims='date',
        vdims=variable,
        label=current_year_text)
    hv_current_year.opts(
        interpolation='linear',
        color=runoff_current_year_color,
        show_legend=True)

    # Forecast curve (if forecasts exist)
    if not forecasts.empty:
        hv_forecast = hv.Curve(
            forecasts,
            kdims='date',
            vdims=variable,
            label=forecast_text)
        hv_forecast.opts(
            interpolation='linear',
            color=runoff_forecast_color_list[3],
            show_legend=True)
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm * hv_current_year * hv_forecast
    else:
        figure = hvspan_predictor * hvspan_forecast * vlines * hv_norm * hv_current_year

    figure.opts(
        title=title_text,
        xlabel="",
        ylabel=config['ylabel'],
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(y_min, y_max),
        xlim=(min(norm_snow['date']), max(norm_snow['date'])),
        tools=['hover'],
        toolbar='right',
        shared_axes=False
    )

    return figure

# endregion


# region forecast_tab
def plot_pentad_forecast_hydrograph_data_v2(_, hydrograph_day_all, linreg_predictor, forecasts_all,
                                            station, title_date, model_selection,
                                            range_type, range_slider, range_visibility,
                                            rram_forecast, ml_forecast):
    forecast_date = pd.to_datetime(title_date + dt.timedelta(days=1))
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_in_month = 'pentad_in_month'
        title_day_end = tl.get_pentad_last_day(forecast_date.strftime("%Y-%m-%d"))
        horizon_column_name = _('pentad_of_year column name')
    else:
        horizon_in_year = "decad_in_year"
        horizon_in_month = 'decad_in_year'
        title_day_end = tl.get_decad_last_day(forecast_date.strftime("%Y-%m-%d"))
        horizon_column_name = _('decad_of_year column name')

    title_pentad = tl.get_pentad(forecast_date)
    title_month = tl.get_month_str_case2_viz(_, forecast_date)
    title_day_start = tl.get_pentad_first_day(forecast_date.strftime("%Y-%m-%d"))
    # Take forecast_date and replace the day in that date with the string title_day_end
    title_date_end = pd.to_datetime(forecast_date.replace(day=int(title_day_end)))

    # filter hydrograph_day_all & linreg_predictor by station
    # linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    # Filter forecasts for the current year and station
    forecasts = forecasts_all[(forecasts_all['station_labels'] == station) &
                              (forecasts_all['year'] == title_date.year) &
                              (forecasts_all['model_short'].isin(model_selection))].copy()

    # Cast date to datetime
    forecasts['date'] = pd.to_datetime(forecasts['date'])

    # The date in the forecasts dataframe refers to the date the forecast was
    # produced. Here, we need the first day of the pentad for which the forecast
    # was produced (forecast horizon)
    forecasts['date'] = pd.to_datetime(forecasts['date'] + dt.timedelta(days=1))

    # Filter forecasts dataframe for dates smaller than the title date
    forecasts = forecasts[forecasts['date'] <= pd.to_datetime(title_date + dt.timedelta(days=1))]
    # print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Calculate the forecast ranges depending on the values of range_type and range_slider
    if range_type == _('delta'):
        if 'Q25' in forecasts.columns and 'Q75' in forecasts.columns:
            # If we have values in columns 'Q25' and 'Q75', we calculate 'fc_lower'
            # and 'fc_upper' as the 25th and 75th percentiles of the forecasted discharge
            forecasts['fc_lower'] = forecasts['Q25'].where(
                ~forecasts['Q25'].isna(),
                forecasts['forecasted_discharge'] - forecasts['delta'])
            forecasts['fc_upper'] = forecasts['Q75'].where(
                ~forecasts['Q75'].isna(),
                forecasts['forecasted_discharge'] + forecasts['delta'])
        else: 
            forecasts['fc_lower'] = forecasts['forecasted_discharge'] - forecasts['delta']
            forecasts['fc_upper'] = forecasts['forecasted_discharge'] + forecasts['delta']
        # forecasts.loc[:, 'fc_lower'] = forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta']
        # forecasts.loc[:, 'fc_upper'] = forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta']
    elif range_type == _("Manual range, select value below"):
        forecasts['fc_lower'] = (1 - range_slider / 100.0) * forecasts['forecasted_discharge']
        forecasts['fc_upper'] = (1 + range_slider / 100.0) * forecasts['forecasted_discharge']
    elif range_type == _("min[delta, %]"):
        forecasts.loc[:, 'fc_lower'] = np.maximum(forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta'],
                                                  (1 - range_slider / 100.0) * forecasts.loc[:, 'forecasted_discharge'])
        forecasts.loc[:, 'fc_upper'] = np.minimum(forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta'],
                                                  (1 + range_slider / 100.0) * forecasts.loc[:, 'forecasted_discharge'])

    # print tail of forecasts
    # print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Custom hover tool tip for the daily hydrograph
    date_col = _('date column name')
    mean_col = _('mean column name')
    last_year_col = _('Last year column name')
    current_year_col = _('Current year column name')
    min_col = _('min column name')
    max_col = _('max column name')
    q05_col = _('5% column name')
    q95_col = _('95% column name')
    q25_col = _('25% column name')
    q75_col = _('75% column name')

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    title_pentad = tl.get_pentad(title_date + dt.timedelta(days=1))
    title_month = tl.get_month_str_case2_viz(_, title_date)
    # print(f"title_pentad: {title_pentad}")
    # print(f"title_month: {title_month}")

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station].copy()
    data['date'] = pd.to_datetime(data['date'])
    current_year = int(data['date'].dt.year.max())
    last_year = current_year - 1
    # print(f"current_year: {current_year}")
    # print(f"last_year: {last_year}")

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    predictor_string = _("Current year, 3 day sum: ") + f"{linreg_predictor['predictor'].values[0]}" + " " + _("m3/s")
    forecast_string = _("Forecast horizon for ") + title_pentad + _(" pentad of ") + title_month

    # Rename columns to be used in the plot to allow internationalization
    data = data.rename(columns={
        'date': date_col,
        'day_of_year': _('day_of_year column name'),
        'min': min_col,
        'max': max_col,
        '5%': q05_col,
        '95%': q95_col,
        '25%': q25_col,
        '75%': q75_col,
        'mean': mean_col,
        str(last_year): last_year_col,
        str(current_year): current_year_col
    })
    linreg_predictor = linreg_predictor.rename({
        'date': date_col,
        'day_of_year': _('day_of_year column name'),
        'predictor': _('Predictor column name')
    })

    forecasts = forecasts.rename(columns={
        horizon: horizon_column_name,
        'forecasted_discharge': _('forecasted_discharge column name'),
        'fc_lower': _('forecast lower bound column name'),
        'fc_upper': _('forecast upper bound column name'),
        'model_short': _('forecast model short column name'),
        'model_long': _('forecast model long column name')
    })
    # The last row of forecasts corresponds to the forecast for the title date.
    # We want to display past and current forecasts slighly differently.
    # Print tail of forecasts
    # print(f"Tail of forecasts\n{forecasts.tail(10)}")
    # print title_date
    # print(f"title_date: {title_date}")
    forecasts_current = forecasts[forecasts['date'] == pd.to_datetime((title_date) + dt.timedelta(days=1))]
    forecasts_past = forecasts[forecasts['date'] <= pd.to_datetime((title_date) + dt.timedelta(days=1))]

    if rram_forecast is not None:
        # Filter for the current station (Only few stations have rram forecasts)
        current_rram_forecasts = rram_forecast[rram_forecast['station_labels'] == station].copy()
        # Filter for the latest forecast
        latest_rram_forecast = current_rram_forecasts[
            current_rram_forecasts['forecast_date'] == current_rram_forecasts['forecast_date'].max()]
        # Rename columns in latest_rram_forecast to fit forecasts_current
        latest_rram_forecast = latest_rram_forecast.rename(
            columns={'Q25': 'Lower bound',
                     'Q75': 'Upper bound',
                     'Q50': 'E[Q]',
                    'model_short': 'Model'})
        # Add pentad_in_month and pentad_in_year to latest_rram_forecast
        latest_rram_forecast[horizon_in_month] = forecasts_current[horizon_in_month].values[
            0] if not forecasts_current.empty else None
        latest_rram_forecast[horizon_in_year] = forecasts_current[horizon_in_year].values[
            0] if not forecasts_current.empty else None
        latest_rram_forecast['Model name'] = 'Rainfall runoff assimilation model (RRAM)'

    if ml_forecast is not None:
        # Filter for the current station
        current_ml_forecasts = ml_forecast[ml_forecast['station_labels'] == station].copy()
        # Filter for the latest forecast
        latest_ml_forecast = current_ml_forecasts[
            current_ml_forecasts['forecast_date'] == current_ml_forecasts['forecast_date'].max()]
        # Rename columns in latest_ml_forecast to fit forecasts_current
        latest_ml_forecast = latest_ml_forecast.rename(
            columns={'Q25': 'Lower bound',
                     'Q75': 'Upper bound',
                     'forecasted_discharge': 'E[Q]',
                     'model_short': 'Model'})
        # Add pentad_in_month and pentad_in_year to latest_ml_forecast
        latest_ml_forecast[horizon_in_month] = forecasts_current[horizon_in_month].values[
            0] if not forecasts_current.empty else None
        latest_ml_forecast[horizon_in_year] = forecasts_current[horizon_in_year].values[
            0] if not forecasts_current.empty else None
        # Filter for selected models
        latest_ml_forecast = latest_ml_forecast[latest_ml_forecast['Model'].isin(model_selection)]

    # Create a holoviews bokeh plots of the daily hydrograph
    hvspan_predictor = hv.VSpan(
        pd.to_datetime(linreg_predictor['predictor_start_date'].values[0]),
        pd.to_datetime(linreg_predictor['predictor_end_date'].values[0])) \
        .opts(color=runoff_current_year_color, alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=True)

    hvspan_forecast = hv.VSpan(
        pd.to_datetime(linreg_predictor['forecast_start_date'].values[0]),
        pd.to_datetime(linreg_predictor['forecast_end_date'].values[0])) \
        .opts(color=runoff_forecast_color_list[3], alpha=0.2, line_width=0,
              muted_alpha=0.05, show_legend=False)

    vlines = create_cached_vlines(_, for_dates=True)

    full_range_area = plot_runoff_range_area(
        data, date_col, min_col, max_col, _("Full range legend entry"),
        runoff_full_range_color)
    lower_bound = plot_runoff_range_bound(
        data, date_col, min_col, runoff_full_range_color)
    upper_bound = plot_runoff_range_bound(
        data, date_col, max_col, runoff_full_range_color)

    area_05_95 = plot_runoff_range_area(
        data, date_col, q05_col, q95_col,
        _("90-percentile range legend entry"), runoff_90percentile_range_color)
    line_05 = plot_runoff_range_bound(
        data, date_col, q05_col, runoff_90percentile_range_color)
    line_95 = plot_runoff_range_bound(
        data, date_col, q95_col, runoff_90percentile_range_color)

    area_25_75 = plot_runoff_range_area(
        data, date_col, q25_col, q75_col,
        _("50-percentile range legend entry"), runoff_50percentile_range_color)
    line_25 = plot_runoff_range_bound(
        data, date_col, q25_col, runoff_50percentile_range_color)
    line_75 = plot_runoff_range_bound(
        data, date_col, q75_col, runoff_50percentile_range_color)

    mean = plot_runoff_line(
        data, date_col, mean_col, _('Mean legend entry'), runoff_mean_color)
    # print("head and tail fo data\n", data.head(), data.tail())
    last_year = plot_runoff_line(
        data, date_col, last_year_col,
        _('Last year legend entry'), runoff_last_year_color)
    current_year = plot_runoff_line(
        data, date_col, current_year_col,
        predictor_string,
        # _('Current year legend entry'),
        runoff_current_year_color)

    forecast_area = plot_runoff_forecast_range_area_v2(
        data=forecasts_past,
        date_col=_('date'),
        forecast_name_col=_('forecast model short column name'),
        min_col=_('forecast lower bound column name'),
        max_col=_('forecast upper bound column name'),
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string=_("m³/s"))
    forecast_lower_bound = plot_runoff_range_bound_v2(
        forecasts_past, _('date'), _('forecast lower bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_upper_bound = plot_runoff_range_bound_v2(
        forecasts_past, _('date'), _('forecast upper bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_line = plot_runoff_forecasts_v2(
        forecasts_past, _('date'), _('forecasted_discharge column name'),
        _('forecast model short column name'), runoff_forecast_color_list, _('m³/s'))

    # print("\n\n")
    # print('Debugging current forecast range point')
    # print('columns of forecasts_current:\n', forecasts_current.columns)
    # print('forecasts_current:\n', forecasts_current)
    current_forecast_range_point = plot_current_runoff_forecast_range_date_format_v2(
        forecasts_current, _('date'), title_date_end, _('forecast model short column name'),
        _('forecasted_discharge column name'), _('forecast lower bound column name'),
        _('forecast upper bound column name'),
        runoff_forecast_color_list, _('m³/s'))

    if 'RRAM' in forecasts_current[_('forecast model short column name')].values:
        rram_forecast_range_point = plot_current_runoff_forecast_range_date_format(
            latest_rram_forecast, _('date'), _('forecast model short column name'),
            _('forecasted_discharge column name'), _('forecast lower bound column name'),
            _('forecast upper bound column name'),
            runoff_forecast_color_list, _('m³/s'))
    else:
        rram_forecast_range_point = hv.Curve([])

    # if either of the following 'TFT', 'TiDE', 'TSMixer', 'NE', 'ARIMA', 'EM'
    if 'TFT' in forecasts_current[_('forecast model short column name')].values or 'TiDE' in forecasts_current[
        _('forecast model short column name')].values or 'TSMixer' in forecasts_current[
        _('forecast model short column name')].values or 'NE' in forecasts_current[
        _('forecast model short column name')].values or 'ARIMA' in forecasts_current[
        _('forecast model short column name')].values or 'EM' in forecasts_current[
        _('forecast model short column name')].values:
        ml_forecast_range_point = plot_current_runoff_forecast_range_date_format(
            latest_ml_forecast,
            date_col='date',
            forecast_name_col='Model',
            mean_col='E[Q]',
            min_col='Q5',
            max_col='Q95',
            runoff_forecast_colors=runoff_forecast_color_list,
            unit_string='m³/s'
        )
    else:
        ml_forecast_range_point = hv.Curve([])

    # Overlay the plots
    if range_visibility == _('Yes'):
        daily_hydrograph = forecast_area * forecast_lower_bound * \
                           forecast_upper_bound * \
                           vlines * \
                           last_year * \
                           mean * current_year * forecast_line * current_forecast_range_point * \
                           rram_forecast_range_point * ml_forecast_range_point
    else:
        daily_hydrograph = vlines * last_year * \
                           mean * current_year * forecast_line * current_forecast_range_point * \
                           rram_forecast_range_point * ml_forecast_range_point

    daily_hydrograph.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Discharge (m³/s)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, data[max_col].max() * 1.1),
        xlim=(pd.to_datetime(min(data[date_col])), pd.to_datetime(max(data[date_col]))),
        tools=['hover'],
        toolbar='right',
        shared_axes=False)

    # print("\n\n")

    return daily_hydrograph


def plot_pentad_forecast_hydrograph_data(_, hydrograph_pentad_all, forecasts_all,
                                         station, title_date, model_selection,
                                         range_type, range_slider, range_visibility):
    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    # print(f"\n\nDEBUG: plot_pentad_forecast_hydrograph_data")
    # print(f"title_date: {title_date}")
    forecast_date = title_date + dt.timedelta(days=1)
    # print(f"forecast_date: {forecast_date}")

    # Get variables depending on forecast horizon (pentad or decad)
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_column_name = _('pentad_of_year column name')
        horizon_of_year = _('Pentad of the year')

        title_pentad = tl.get_pentad(forecast_date)
        title_month = tl.get_month_str_case2_viz(_, forecast_date)
        title_day_start = tl.get_pentad_first_day(forecast_date.strftime("%Y-%m-%d"))
        title_day_end = tl.get_pentad_last_day(forecast_date.strftime("%Y-%m-%d"))

        # Define strings
        title_text = (f"{_('Hydropost')} {station}: {_('Forecast')} {_('for')} "
                      f"{title_pentad} {_('pentad')} {_('of')} {title_month} "
                      f"({_('days')} {title_day_start}-{title_day_end})")
    else:
        horizon_in_year = "decad_in_year"
        horizon_column_name = _('decad_of_year column name')
        horizon_of_year = _('Decad of the year')

        title_decad = tl.get_decad_in_month(forecast_date)
        title_month = tl.get_month_str_case2_viz(_, forecast_date)
        title_day_start = tl.get_decad_first_day(forecast_date.strftime("%Y-%m-%d"))
        title_day_end = tl.get_decad_last_day(forecast_date.strftime("%Y-%m-%d"))

        # Define strings
        title_text = (f"{_('Hydropost')} {station}: {_('Forecast')} {_('for')} "
                      f"{title_decad} {_('decad')} {_('of')} {title_month} "
                      f"({_('days')} {title_day_start}-{title_day_end})")

    # filter hydrograph_day_all & linreg_predictor by station
    # linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    # Filter forecasts for the current year and station
    forecasts = forecasts_all[(forecasts_all['station_labels'] == station) &
                              (forecasts_all['year'] == title_date.year) &
                              (forecasts_all['model_short'].isin(model_selection))].copy()

    # Test if forecasts is empty
    if forecasts.empty:
        print(f"DEBUG: No forecasts found for station {station} for date {title_date}")

    # Cast date to datetime
    forecasts['date'] = pd.to_datetime(forecasts['date'])

    # Filter forecasts dataframe for dates smaller than the title date
    forecasts = forecasts[forecasts['date'] <= pd.Timestamp(title_date) + dt.timedelta(days=1)]
    # print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Calculate the forecast ranges depending on the values of range_type and range_slider
    if range_type == _('delta'):
        # If we have values in columns 'Q25' and 'Q75', we calculate 'fc_lower'
        # and 'fc_upper' as the 25th and 75th percentiles of the forecasted discharge
        forecasts['fc_lower'] = forecasts['Q25'].where(
            ~forecasts['Q25'].isna(),
            forecasts['forecasted_discharge'] - forecasts['delta'])
        forecasts['fc_upper'] = forecasts['Q75'].where(
            ~forecasts['Q75'].isna(),
            forecasts['forecasted_discharge'] + forecasts['delta'])
        # forecasts.loc[:, 'fc_lower'] = forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta']
        # forecasts.loc[:, 'fc_upper'] = forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta']
    elif range_type == _("Manual range, select value below"):
        forecasts['fc_lower'] = (1 - range_slider / 100.0) * forecasts['forecasted_discharge']
        forecasts['fc_upper'] = (1 + range_slider / 100.0) * forecasts['forecasted_discharge']
    elif range_type == _("min[delta, %]"):
        forecasts.loc[:, 'fc_lower'] = np.maximum(forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta'],
                                                  (1 - range_slider / 100.0) * forecasts.loc[:, 'forecasted_discharge'])
        forecasts.loc[:, 'fc_upper'] = np.minimum(forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta'],
                                                  (1 + range_slider / 100.0) * forecasts.loc[:, 'forecasted_discharge'])

    # Print column names of hydrograph_pentad_all
    # print("hydrograph_pentad_all.columns\n", hydrograph_pentad_all.columns)
    # Print unique station_labels for hydrograph_pentad_all
    # print("hydrograph_pentad_all['station_labels'].unique()\n", hydrograph_pentad_all['station_labels'].unique())
    # Print station
    # print("station\n", station)

    # Filter hydrograph data for the current station
    data = hydrograph_pentad_all[hydrograph_pentad_all['station_labels'] == station].copy()
    # print("\n\nFirst getting data for station:")
    # print("head of data:\n", data.head()[['code', horizon_in_year, 'mean', '2024', '2025', 'date']])
    # print("tail of data:\n", data.tail()[['code', horizon_in_year, 'mean', '2024', '2025', 'date']])

    current_year = int(data['date'].dt.year.max())
    last_year = current_year - 1

    if str(current_year) not in data.columns:
        print(f"DEBUG: '{str(current_year)}' column missing in 'data'. Creating it with NaNs.")
        data[str(current_year)] = np.nan  # Initialize the column

    # Convert date column of data dataframe to datetime
    data['date'] = pd.to_datetime(data['date'])

    # If the first date is NaT, set it to the first day of the year of the current year
    if pd.isna(data['date'].iloc[0]):
        data['date'].iloc[0] = pd.Timestamp(f"{current_year}-01-01")

    # Print pentad of first date in data
    # print(f"\n\n\n\n\n{horizon} of first date in data: {data[horizon_in_year].iloc[0]} of {data['date'].iloc[0]}")
    # print(f"head(data): {data.head()[['code', horizon_in_year, 'norm', 'mean', '2024', '2025', 'date']]}")
    # print(f"data.columns: {data.columns}")

    # Set values after the title date to NaN
    # WHY DO WE DO THAT?
    # print("title_date: ", title_date)
    # print("str(current_year): ", str(current_year))
    # print(pd.Timestamp(title_date))
    # print("data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)]: ", data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)])
    data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)] = np.nan
    # print(f"Tail of data\n{data.tail(25).head(10)}")

    # Rename columns to be used in the plot to allow internationalization
    data = data.rename(columns={
        horizon_in_year: horizon_column_name,
        'min': _('min column name'),
        'max': _('max column name'),
        'q05': _('5% column name'),
        'q95': _('95% column name'),
        'q25': _('25% column name'),
        'q75': _('75% column name'),
        'mean': _('mean column name'),
        'norm': _('norm column name'),
        str(last_year): _('Last year column name'),
        str(current_year): _('Current year column name')
    })
    forecasts = forecasts.rename(columns={
        horizon_in_year: horizon_column_name,
        'forecasted_discharge': _('forecasted_discharge column name'),
        'fc_lower': _('forecast lower bound column name'),
        'fc_upper': _('forecast upper bound column name'),
        'model_short': _('forecast model short column name'),
        'model_long': _('forecast model long column name')
    })
    # The last row of forecasts corresponds to the forecast for the title date.
    # We want to display past and current forecasts slighly differently.
    forecasts_current = forecasts[forecasts['date'] == pd.Timestamp(title_date)]
    forecasts_past = forecasts[forecasts['date'] < pd.Timestamp(title_date)]

    # print("forecasts.columns\n", forecasts.columns)
    # print("forecasts.head(10)\n", forecasts.head(10))
    # print("forecasts.tail(10)\n", forecasts.tail(10))
    # linreg_predictor = linreg_predictor.rename({
    #    'day_of_year': _('day_of_year column name'),
    #    'predictor': _('Predictor column name')
    #    })

    # Create a holoviews bokeh plots of the daily hydrograph
    full_range_area = plot_runoff_range_area(
        data, horizon_column_name, _('min column name'), _('max column name'),
        _("Full range legend entry"), runoff_full_range_color)
    lower_bound = plot_runoff_range_bound(
        data, horizon_column_name, _('min column name'),
        runoff_full_range_color, hover_tool=False)
    upper_bound = plot_runoff_range_bound(
        data, horizon_column_name, _('max column name'),
        runoff_full_range_color, hover_tool=False)

    area_05_95 = plot_runoff_range_area(
        data, horizon_column_name, _('5% column name'), _('95% column name'),
        _("90-percentile range legend entry"), runoff_90percentile_range_color)
    line_05 = plot_runoff_range_bound(
        data, horizon_column_name, _('5% column name'),
        runoff_90percentile_range_color, hover_tool=False)
    line_95 = plot_runoff_range_bound(
        data, horizon_column_name, _('95% column name'),
        runoff_90percentile_range_color, hover_tool=False)

    area_25_75 = plot_runoff_range_area(
        data, horizon_column_name, _('25% column name'), _('75% column name'),
        _("50-percentile range legend entry"), runoff_50percentile_range_color)
    line_25 = plot_runoff_range_bound(
        data, horizon_column_name, _('25% column name'),
        runoff_50percentile_range_color, hover_tool=False)
    line_75 = plot_runoff_range_bound(
        data, horizon_column_name, _('75% column name'),
        runoff_50percentile_range_color, hover_tool=False)

    norm = plot_runoff_line(
        data, horizon_column_name, _('norm column name'),
        _('Norm legend entry'), runoff_norm_color)
    mean = plot_runoff_line(
        data, horizon_column_name, _('mean column name'),
        _('Mean legend entry'), runoff_mean_color)
    """last_year = plot_runoff_line(
        data, horizon_column_name, _('Last year column name'),
        _('Last year legend entry'), runoff_last_year_color)"""
    current_year = plot_runoff_line(
        data, horizon_column_name, _('Current year column name'),
        _('Current year legend entry'), runoff_current_year_color)

    forecast_area = plot_runoff_forecast_range_area(
        data=forecasts_past,
        date_col=horizon_column_name,
        forecast_name_col=_('forecast model short column name'),
        min_col=_('forecast lower bound column name'),
        max_col=_('forecast upper bound column name'),
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string=_("m³/s"))
    forecast_lower_bound = plot_runoff_range_bound(
        forecasts_past, horizon_column_name, _('forecast lower bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_upper_bound = plot_runoff_range_bound(
        forecasts_past, horizon_column_name, _('forecast upper bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_line = plot_runoff_forecasts(
        forecasts_past, horizon_column_name, _('forecasted_discharge column name'),
        _('forecast model short column name'), runoff_forecast_color_list, _('m³/s'))

    current_forecast_range_point = plot_current_runoff_forecast_range(
        forecasts_current, horizon_column_name, _('forecast model short column name'),
        _('forecasted_discharge column name'), _('forecast lower bound column name'),
        _('forecast upper bound column name'),
        runoff_forecast_color_list, _('m³/s'))

    # Overlay the plots
    if range_visibility == 'Yes':
        pentad_hydrograph = full_range_area * lower_bound * upper_bound * \
                            area_05_95 * line_05 * line_95 * \
                            area_25_75 * line_25 * line_75 * \
                            forecast_area * forecast_lower_bound * forecast_upper_bound * \
                            norm * mean * current_year * forecast_line * \
                            current_forecast_range_point
    else:
        pentad_hydrograph = norm * mean * current_year * forecast_line * \
                            current_forecast_range_point

    pentad_hydrograph.opts(
        title=title_text,
        xlabel=horizon_of_year,
        ylabel=_('Discharge (m³/s)'),
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo,
               lambda p, e: add_custom_xticklabels_pentad(_, p, e),
               ],
        # tools=['hover'],
        toolbar='right')

    # print("\n\n")

    return pentad_hydrograph


def create_forecast_summary_table(_, forecasts_all, station, date_picker,
                                  model_selection, range_type, range_slider):
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
    else:
        horizon_in_year = "decad_in_year"

    # Filter forecasts_all for selected station, date and models
    if hasattr(station, 'value'):
        forecast_table = forecasts_all[(forecasts_all['station_labels'] == station.value)].copy()
    else:
        forecast_table = forecasts_all[(forecasts_all['station_labels'] == station)].copy()

    # Cast date column to datetime
    forecast_table['date'] = pd.to_datetime(forecast_table['date'])

    # print(f"create_forecast_summary_table: model_selection: {model_selection}")
    all_models = list()
    for key, value in model_selection.options.items():
        all_models.append(value)

    # List of unique models in forecast_table before filtering.
    # print(f"create_forecast_summary_table: forecast_table['model_short'].unique(): {forecast_table['model_short'].unique()}")
    # print(f"columns of forecast_table: {forecast_table.columns}")

    # Filter further for the selected date
    if hasattr(date_picker, 'value'):
        forecast_table = forecast_table[
            (forecast_table['date'] <= (pd.Timestamp(date_picker.value) + pd.Timedelta(days=1))) &
            (forecast_table['model_short'].isin(all_models))].copy().reset_index(drop=True)
    else:
        forecast_table = forecast_table[(forecast_table['date'] <= (pd.Timestamp(date_picker) + pd.Timedelta(days=1))) &
                                        (forecast_table['model_short'].isin(all_models))].copy().reset_index(drop=True)

    # List of unique models in forecast_table after filtering.
    # print(f"create_forecast_summary_table: forecast_table['model_short'].unique(): {forecast_table['model_short'].unique()}")
    # print columns model_short, date, and forecasted_discharge
    # print(f"create_forecast_summary_table: forecast_table[['model_short', 'date', 'forecasted_discharge']].tail(10): {forecast_table[['model_short', 'date', 'forecasted_discharge']].tail(10)}")

    # Select the row with the maximum date
    if not forecast_table.empty and not forecast_table['date'].empty:
        max_date = forecast_table['date'].max()
        if not pd.isna(max_date):
            forecast_table = forecast_table.loc[forecast_table['date'] == max(forecast_table['date'])]
        else:
            print("max_date is nan")
    # print("forecast_table\n", forecast_table)

    # Drop a couple of columns
    forecast_table.drop(
        columns=['code', 'date', 'Date', 'year', horizon_in_year,
                 horizon, 'model_long', 'station_labels'],
        inplace=True)

    # Calculate the forecast range depending on the values of range_type and range_slider
    forecast_table = processing.calculate_forecast_range(_, forecast_table, range_type, range_slider)

    # Get columns in the desired sequence
    forecast_table = forecast_table[
        ['model_short', 'forecasted_discharge', 'fc_lower', 'fc_upper', 'delta', 'sdivsigma', 'mae', 'accuracy']]

    # Round delta, sdivsigma and mae to 2 decimals each
    forecast_table['delta'] = forecast_table['delta'].round(2)
    forecast_table['sdivsigma'] = forecast_table['sdivsigma'].round(2)
    forecast_table['mae'] = forecast_table['mae'].round(2)

    # Round lower and  upper bound to 3 decimals each
    forecast_table['fc_lower'] = forecast_table['fc_lower'].round(3)
    forecast_table['fc_upper'] = forecast_table['fc_upper'].round(3)

    # Rename columns to be used in the plot to allow internationalization
    forecast_table = forecast_table.rename(columns={
        'forecasted_discharge': _('Forecasted discharge'),
        'delta': _('δ'),
        'month': _('Month'),
        'station_labels': _('Hydropost'),
        'sdivsigma': _('s/σ'),
        'mae': _('MAE'),
        'nse': _('NSE'),
        'accuracy': _('Accuracy'),
        'fc_lower': _('Forecast lower bound'),
        'fc_upper': _('Forecast upper bound'),
        'model_short': _('Model')
    })

    # Order the rows in alphabetical order (column Model)
    final_forecast_table = forecast_table.sort_values(by=[_('Model')]) \
        .copy().reset_index(drop=True)

    return final_forecast_table


def create_forecast_summary_tabulator(_, forecasts_all, station, date_picker,
                                      model_selection, range_type, range_slider, forecast_tabulator):
    '''Put table data into a Tabulator widget'''

    final_forecast_table = create_forecast_summary_table(_, forecasts_all, station, date_picker,
                                                         model_selection, range_type, range_slider).reset_index(
        drop=True)

    # Return empty Tabulator if the table is empty
    if final_forecast_table.empty:
        forecast_tabulator.value = pd.DataFrame({_('Please select a forecast model.'): []})
        return forecast_tabulator

    # Get the row with the maximum accuracy. If the table has 2 rows, the
    # index is either 0 or 1. If the table has 1 row, the index is 0.
    # If two rows have the same accuracy, the first row is selected.
    max_accuracy_index = final_forecast_table[_('Accuracy')].idxmax()
    # if max_accuracy_index is nan, set it to 0
    if pd.isna(max_accuracy_index):
        max_accuracy_index = 0
    # print("max_accuracy_index\n", max_accuracy_index)

    # print("final_forecast_table\n", final_forecast_table)

    # Update the Tabulator's value
    forecast_tabulator.value = final_forecast_table
    # Set the selection to the row with max accuracy
    forecast_tabulator.selection = [max_accuracy_index]

    # Enforce single selection
    def enforce_single_selection(event):
        selected_rows = forecast_tabulator.selection
        if len(selected_rows) > 1:
            forecast_tabulator.selection = [selected_rows[-1]]  # Keep only the last selected row

    forecast_tabulator.param.watch(enforce_single_selection, 'selection')

    return forecast_tabulator


pentads = [
    f"{i + 1}{_('st pentad of')} {calendar.month_name[month]}" if i == 0 else
    f"{i + 1}{_('nd pentad of')} {calendar.month_name[month]}" if i == 1 else
    f"{i + 1}{_('rd pentad of')} {calendar.month_name[month]}" if i == 2 else
    f"{i + 1}{_('th pentad of')} {calendar.month_name[month]}"
    for month in range(1, 13) for i in range(6)
]

# Create a dictionary mapping each pentad description to its pentad_in_year value
pentad_options = {f"{i + 1}{_('st pentad of')} {calendar.month_name[month]}" if i == 0 else
                  f"{i + 1}{_('nd pentad of')} {calendar.month_name[month]}" if i == 1 else
                  f"{i + 1}{_('rd pentad of')} {calendar.month_name[month]}" if i == 2 else
                  f"{i + 1}{_('th pentad of')} {calendar.month_name[month]}": i + (month - 1) * 6 + 1
                  for month in range(1, 13) for i in range(6)}

# Create a dictionary mapping each decade description to its decad_in_year value
decad_options = {
    f"{i + 1}{_('st decade of')} {calendar.month_name[month]}" if i == 0 else
    f"{i + 1}{_('nd decade of')} {calendar.month_name[month]}" if i == 1 else
    f"{i + 1}{_('rd decade of')} {calendar.month_name[month]}" if i == 2 else
    f"{i + 1}{_('th decade of')} {calendar.month_name[month]}": i + (month - 1) * 3 + 1
    for month in range(1, 13) for i in range(3)
}


class Environment:
    def __init__(self, dotenv_path):
        print(f"Current working directory: {os.getcwd()}")
        print(f"Loading environment variables from {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)

    def get(self, key, default=None):
        return os.getenv(key, default)


# Initialize the Environment class with the path to your .env file
env_file_path = os.getenv('ieasyhydroforecast_env_file_path')
env = Environment(env_file_path)
# Get the tag of the docker image to use
TAG = env.get('ieasyhydroforecast_backend_docker_image_tag')
# Get the organization for which to run the forecast tools
ORGANIZATION = env.get('ieasyhydroforecast_organization')
# URL of the sapphire data gateway
SAPPHIRE_DG_HOST = env.get('SAPPHIRE_DG_HOST')
# open ssh tunnel connection
SSH_TUNNEL_SCRIPT_PATH = env.get('SSH_TUNNEL_SCRIPT_PATH',
                                 '../../../sensitive_data_forecast_tools/bin/.ssh/open_ssh_tunnel.sh')
# If the dashboard is running in a container, the SSH tunnel script path needs to be adjusted
if os.getenv('IN_DOCKER_CONTAINER'):
    # instead of filename 'open_ssh_tunnel.sh' use filename
    # 'open_ssh_tunnel_docker.sh' which has an addapted path to the .pem file
    # accessible from within docker containers
    SSH_TUNNEL_SCRIPT_PATH = re.sub(r'open_ssh_tunnel.sh', 'open_ssh_tunnel_docker.sh', SSH_TUNNEL_SCRIPT_PATH)


# Function to convert a relative path to an absolute path
def get_absolute_path(relative_path):
    # print("In get_absolute_path: ")
    # print(" - Relative path: ", relative_path)

    # Test if there environment variable "ieasyforecast_data_root_dir" is set
    data_root_dir = os.getenv('ieasyhydroforecast_data_root_dir')
    # print(f"\n\n\n\n\nget_absolute_path: data_root_dir: {data_root_dir}")
    if data_root_dir:
        # If it is set, use it as the root directory
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

        return data_root_dir + relative_path

    else:
        # Current working directory. Should be one above the root of the project
        cwd = os.getcwd()
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

        return os.path.join(cwd, relative_path)


"""# TODO: use this function for local development instead of initial get_absolute_path function
def get_absolute_path(relative_path):
    # function for local development
    project_root = '/home/vjeko/Desktop/Projects/sapphire_forecast'
    # Remove leading ../../../ from the relative path
    relative_path = re.sub(r'^\.\./\.\./\.\./', '', relative_path)
    return os.path.join(project_root, relative_path)"""

def get_bind_path(relative_path):
    # Strip the relative path from ../../.. to get the path to bind to the container
    relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

    return relative_path


# Test if the path to the configuration folder is set
# if not os.getenv('ieasyforecast_configuration_path'):
#    raise ValueError("The path to the configuration folder is not set.")

# Define the directory to save the data
# SAVE_DIRECTORY = os.path.join(
#    os.getenv('ieasyforecast_configuration_path'),
#    os.getenv('ieasyforecast_linreg_point_selection', 'linreg_point_selection')
# )
# os.makedirs(SAVE_DIRECTORY, exist_ok=True)

class AppState(param.Parameterized):
    pipeline_running = param.Boolean(default=False)


# Instantiate the shared state
app_state = AppState()


@contextmanager
def establish_ssh_tunnel(ssh_script_path):
    """
    Context manager to establish and manage SSH tunnel lifecycle.

    Args:
        ssh_script_path (str): Path to the SSH tunnel script
    """
    # Initialize tunnel_process to None before the try block
    tunnel_process = None
    try:
        # Start SSH tunnel
        tunnel_process = subprocess.Popen(['bash', ssh_script_path])
        # Give the tunnel time to establish
        time.sleep(2)
        yield tunnel_process
    finally:
        if tunnel_process:
            tunnel_process.terminate()
            tunnel_process.wait()


def select_and_plot_data(_, linreg_predictor, station_widget, pentad_selector, decad_selector,
                         SAVE_DIRECTORY):
    # Define a variable to hold the visible data across functions
    global visible_data

    # print(f"\n\nDEBUG: select_and_plot_data")
    # Print tail of linreg_predictor for code == '16059'
    # print(f"linreg_predictor[linreg_predictor['code'] == '16059'].head(10):\n",
    #      linreg_predictor[linreg_predictor['code'] == '16059'].head(10))
    # print(f"linreg_predictor[linreg_predictor['code'] == '16059'].tail(10):\n",
    #      linreg_predictor[linreg_predictor['code'] == '16059'].tail(10))

    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"

        # Check if pentad_selector is a widget or an integer
        if isinstance(pentad_selector, int):
            selected_pentad = pentad_selector  # If it's an integer, use it directly
        else:
            selected_pentad = pentad_selector.value  # If it's a widget, get the value

        # Find out the month and pentad for display purposes
        selected_pentad_text = [k for k, v in pentad_options.items() if v == selected_pentad][0]
        title_pentad = selected_pentad_text.split(' ')[0]
        title_month = selected_pentad_text.split('of ')[-1]
        partial_title_text = f"{_(title_pentad)} {_('pentad of ' + title_month)}"

        pentad_in_month = selected_pentad % 6 if selected_pentad % 6 != 0 else 6
        partial_file_name = f"{pentad_in_month}_pentad_of_{title_month}"

        horizon_value = selected_pentad
        add_date_column = lambda horizon_value: tl.get_date_for_pentad(horizon_value)

    else:
        horizon_in_year = "decad_in_year"

        # Check if decad_selector is a widget or an integer
        if isinstance(decad_selector, int):
            selected_decad = decad_selector  # If it's an integer, use it directly
        else:
            selected_decad = decad_selector.value  # If it's a widget, get the value

        selected_decad_text = [k for k, v in decad_options.items() if v == selected_decad][0]
        title_decad = selected_decad_text.split(' ')[0]
        title_month = selected_decad_text.split('of ')[-1]
        partial_title_text = f"{_(title_decad)} {_('decade of ' + title_month)}"

        decad_in_month = selected_decad % 3 if selected_decad % 3 != 0 else 3
        partial_file_name = f"{decad_in_month}_decad_of_{title_month}"

        horizon_value = selected_decad
        add_date_column = lambda horizon_value: tl.get_date_for_decad(horizon_value)

    if isinstance(station_widget, str):
        station_code = station_widget.split(' - ')[0]
    else:
        station_code = station_widget.value.split(' - ')[0]

    # Generate dynamic filename based on the selected pentad and station
    station_name = station_widget
    station_data = linreg_predictor.loc[linreg_predictor['code'] == station_code]
    if station_data.empty:
        print(f"Error: No data found for station code '{station_code}'")
        return None

    save_file_name = f"{station_code}_{partial_file_name}.csv"
    save_file_path = os.path.join(SAVE_DIRECTORY, save_file_name)

    # Add the 'date' column if it doesn't exist
    if 'date' not in linreg_predictor.columns:
        # Map the 'pentad_in_year' to the corresponding date using get_date_for_pentad
        linreg_predictor['date'] = linreg_predictor[horizon_in_year].apply(add_date_column)

    # Extract the year from the date column and create a 'year' column
    linreg_predictor['year'] = pd.to_datetime(linreg_predictor['date']).dt.year.astype(int)

    # **Always read from linreg_predictor**
    # Filter data for the selected station and pentad across all years
    forecast_table = linreg_predictor[
        (linreg_predictor['station_labels'] == station_widget) &
        (linreg_predictor[horizon_in_year] == horizon_value)
        ].copy().reset_index(drop=True)

    # Check if the saved CSV file exists
    if os.path.exists(save_file_path):
        # Read the saved CSV file
        saved_forecast_table = pd.read_csv(save_file_path)
        # Ensure 'visible' is boolean
        saved_forecast_table['visible'] = saved_forecast_table['visible'].astype(bool)
        # Merge the 'visible' column into forecast_table based on 'year'
        forecast_table = forecast_table.merge(
            saved_forecast_table[['year', 'visible']],
            on='year',
            how='left',
            suffixes=('', '_saved')
        )
        # Fill any missing 'visible' values with True
        forecast_table['visible'] = forecast_table['visible'].fillna(True)
        # Store the initial visible data points
        initial_visible_data = forecast_table[forecast_table['visible'] == True].copy()
        initial_visible_data = initial_visible_data.dropna(subset=['predictor', 'discharge_avg'])
        # Load the initial regression parameters if they exist
        if 'slope' in saved_forecast_table.columns and 'intercept' in saved_forecast_table.columns and 'rsquared' in saved_forecast_table.columns:
            initial_slope = saved_forecast_table['slope'].iloc[0]
            initial_intercept = saved_forecast_table['intercept'].iloc[0]
            initial_rsquared = saved_forecast_table['rsquared'].iloc[0]
        else:
            # Compute initial regression parameters based on saved visible data
            if len(initial_visible_data) > 1:
                initial_slope, initial_intercept, r_value, p_value, std_err = stats.linregress(
                    initial_visible_data['predictor'], initial_visible_data['discharge_avg']
                )
                initial_rsquared = r_value ** 2
    else:
        # Initialize 'visible' column as before
        forecast_table['visible'] = (~forecast_table['predictor'].isna()) & (~forecast_table['discharge_avg'].isna())
        # Store the initial visible data points
        initial_visible_data = forecast_table[forecast_table['visible'] == True].copy()
        initial_visible_data = initial_visible_data.dropna(subset=['predictor', 'discharge_avg'])
        # Compute initial regression parameters based on all data
        if len(initial_visible_data) > 1:
            initial_slope, initial_intercept, r_value, p_value, std_err = stats.linregress(
                initial_visible_data['predictor'], initial_visible_data['discharge_avg']
            )
            initial_rsquared = r_value ** 2

    forecast_table = forecast_table.drop(columns=['index', 'level_0'], errors='ignore')
    forecast_table = forecast_table.reset_index()

    visible_data = forecast_table[forecast_table['visible'] == True]  # Initialize the visible data
    # print(f"visible_data.head(10):\n", visible_data.head(10))
    visible_data = visible_data.dropna(subset=['predictor', 'discharge_avg'])

    # Create a localized copy of the forecast_table for display purposes
    forecast_table_display = forecast_table.copy()

    # Rename columns for better display (using localization)
    forecast_table_display.rename(columns={
        'index': _('index'),
        'year': _('year'),
        'predictor': _('predictor'),
        'discharge_avg': _('discharge average'),
        'visible': _('visible')
    }, inplace=True)

    # Create Tabulator for displaying forecast data using localized column names and header filters
    forecast_data_table = pn.widgets.Tabulator(
        value=forecast_table_display[['index', _('year'), _('predictor'), _('discharge average'), _('visible')]],
        theme='bootstrap',
        editors={_('visible'): CheckboxEditor()},  # Checkbox editor for the 'visible' column
        formatters={_('visible'): BooleanFormatter(),
                    _('year'): NumberFormatter(format='0')},
        # Format the date column  # Enable column filtering if necessary
        layout='fit_data_stretch',  # Adjust column sizing
        sizing_mode='stretch_width',
        height=450,
        show_index=False
    )
    '''
    # Create Tabulator for displaying forecast data
    forecast_data_table = pn.widgets.Tabulator(
        value=forecast_table[['index', 'year', 'predictor', 'discharge_avg', 'visible']],
        theme='bootstrap',
        show_index=False,  # Do not show the index column
        editors={'visible': CheckboxEditor()},  # Checkbox editor for the 'visible' column
        formatters={'visible': BooleanFormatter(),
                    'year': NumberFormatter(format='0')},  # Format the date column
        height=450,
        sizing_mode='stretch_width'  # Stretch the table to fill the available space
    )'''

    # Create the title text
    title_text = (f"{_('Hydropost')} {station_code}: {_('Regression for')} {partial_title_text}")

    # Define the plot
    plot_pane = pn.pane.HoloViews(sizing_mode='stretch_width')

    # Create the selection1d stream to capture point selections on the plot
    selection_stream = streams.Selection1D(source=None)

    # Update plot based on visibility
    def update_plot(event=None):
        nonlocal initial_slope, initial_intercept, initial_rsquared, initial_visible_data
        global visible_data

        # Ensure 'visible' is of boolean type
        forecast_table['visible'] = forecast_table['visible'].astype(bool)

        # Update only the 'visible' column based on the table interaction
        forecast_table.loc[forecast_data_table.value[_('index')], 'visible'] = forecast_data_table.value[
            _('visible')].values

        # Filter the data based on visibility
        visible_data = forecast_table[forecast_table['visible'] == True]

        # Drop rows with NaNs in 'predictor' or 'discharge_avg'
        visible_data = visible_data.dropna(subset=['predictor', 'discharge_avg'])

        # If no data is visible, show an empty plot
        if visible_data.empty:
            scatter = hv.Curve([])  # Define an empty plot to avoid errors
            plot = scatter  # initialize plot
        else:
            hover = HoverTool(
                tooltips=[
                    (_('Year'), '@year'),
                    (_('Predictor'), '@predictor'),
                    (_('Discharge'), '@discharge_avg'),
                ],
                formatters={'@year': 'numeral'},  # Show year in the hover tool
            )

            # Calculate dynamic x and y ranges with a margin
            x_min, x_max = visible_data['predictor'].min(), visible_data['predictor'].max()
            y_min, y_max = visible_data['discharge_avg'].min(), visible_data['discharge_avg'].max()

            # Add a 40% margin to the ranges
            x_margin = (x_max - x_min) * 0.4
            y_margin = (y_max - y_min) * 0.4

            # Set dynamic x and y ranges
            x_range = (x_min - x_margin, x_max + x_margin)
            y_range = (y_min - y_margin, y_max + y_margin)

            # Create the dynamic scatter plot (updates during interaction)
            scatter = hv.Scatter(visible_data, kdims='predictor', vdims=['discharge_avg', 'year']) \
                .opts(xlabel=_('predictor'), ylabel=_('discharge_avg'), color='blue', size=5, tools=[hover, 'tap'], xlim=x_range, ylim=y_range)

            if len(visible_data) > 1:
                # Compute dynamic regression parameters
                new_slope, new_intercept, new_r_value, new_p_value, new_std_err = stats.linregress(
                    visible_data['predictor'], visible_data['discharge_avg'])
                new_rsquared = new_r_value ** 2 
                print(f"\n\n\n\n\n\nnew_slope: {new_slope}, new_intercept: {new_intercept}, new_rsquared: {new_rsquared}")
                
                # sklearn linear regression yields the same results
                # as scipy.stats.linregress
                # from sklearn.linear_model import LinearRegression
                # new2_model = LinearRegression().fit(
                #     visible_data['predictor'].values.reshape(-1, 1), visible_data['discharge_avg'])
                # new2_slope = new2_model.coef_[0]
                # new2_intercept = new2_model.intercept_
                # print(f"new2_slope: {new2_slope}, new2_intercept: {new2_intercept}\n\n\n")

                # Prepare x values for regression lines
                x = np.linspace(x_min, x_max, 100)

                # Unicode characters
                SOLID_LINE = "––"
                DASHED_LINE = "- -"

                # Modify the text content creation to use HTML
                def create_html_content(equation_initial, r2_initial, equation_new, r2_new):
                    html_content = "<div style='font-family: monospace; font-size: 12px;'>"
                    if equation_initial:
                        html_content += f"<span style='color: red;'>{SOLID_LINE}</span> {equation_initial}<br>{r2_initial}<br>"
                    html_content += f"<span style='color: red;'>{DASHED_LINE}</span> {equation_new}<br>{r2_new}"
                    html_content += "</div>"
                    return html_content

                # Compute y values for regression lines
                if initial_slope is not None and initial_intercept is not None:
                    y_initial = initial_slope * x + initial_intercept
                    line_initial = hv.Curve((x, y_initial)).opts(color='red', line_width=2)
                    equation_initial = f"y = {initial_slope:.2f}x + {initial_intercept:.2f}"
                    r2_initial = f"R² = {initial_rsquared:.2f}"
                else:
                    line_initial = hv.Curve([])  # Empty curve
                    equation_initial = ""
                    r2_initial = ""

                y_new = new_slope * x + new_intercept
                line_new = hv.Curve((x, y_new)).opts(color='red', line_width=2,
                                                     line_dash='dashed', line_alpha=0.7)
                equation_new = f"new y = {new_slope:.2f}x + {new_intercept:.2f}"
                r2_new = f"new R² = {new_rsquared:.2f}"

                # Prepare the text annotation
                text_content = ""
                if equation_initial:
                    text_content += f"{equation_initial}\n{r2_initial}\n"
                text_content += f"{equation_new}\n{r2_new}"

                # Create the HTML content
                html_content = create_html_content(equation_initial, r2_initial, equation_new, r2_new)

                # Create a Div element with the HTML content
                text_div = hv.Div(html_content)

                def make_add_label_hook_from_div(div_element):
                    def hook(plot, element):
                        fig = plot.state

                        # Remove Bokeh logo
                        fig.toolbar.logo = None

                        # Calculate positions for labels (adjust these as needed)
                        x_pos = fig.x_range.start + (fig.x_range.end - fig.x_range.start) * 0.05
                        y_pos = fig.y_range.end - (fig.y_range.end - fig.y_range.start) * 0.15

                        # Function to add a pair of labels (colored line and equation)
                        def add_equation_labels(line, equation, r2, y):
                            line_label = Label(
                                x=x_pos, y=y, x_units='data', y_units='data',
                                text=line, text_color='red',
                                text_font_size='12px', text_font_style='bold'
                            )
                            eq_label = Label(
                                x=x_pos + (fig.x_range.end - fig.x_range.start) * 0.04, y=y,
                                x_units='data', y_units='data',
                                text=equation,
                                text_font_size='12px'
                            )

                            r2_label = Label(
                                x=x_pos + (fig.x_range.end - fig.x_range.start) * 0.04,
                                y=y - (fig.y_range.end - fig.y_range.start) * 0.04,  # Slightly below the equation
                                x_units='data', y_units='data',
                                text=r2,
                                text_font_size='12px'  # Slightly smaller font for R²
                            )

                            fig.add_layout(line_label)
                            fig.add_layout(eq_label)
                            fig.add_layout(r2_label)

                        # Add labels for initial equation if it exists
                        if equation_initial:
                            add_equation_labels(SOLID_LINE, equation_initial, r2_initial, y_pos)
                            y_pos -= (fig.y_range.end - fig.y_range.start) * 0.10  # Move down for next label

                        # Add labels for new equation
                        if equation_new:
                            add_equation_labels(DASHED_LINE, equation_new, r2_new, y_pos)

                    return hook

                # Create the hook function with the text content
                # hook = make_add_label_hook(text_content)
                hook = make_add_label_hook_from_div(text_div)
                # Overlay the scatter plot and the regression line(s)
                plot = scatter * line_initial * line_new
                plot.opts(
                    title=title_text,
                    show_grid=True,
                    show_legend=True,
                    width=1000,
                    height=450,
                    hooks=[remove_bokeh_logo, hook],
                    fontsize={'title': 10}
                )
            else:
                plot = scatter.opts(
                    width=1000,
                    height=450,
                    hooks=[remove_bokeh_logo]
                )

        # Attach the plot to the selection stream
        selection_stream.source = scatter
        plot_pane.object = plot

    # Function to handle plot selections and update the table
    def handle_selection(event):
        global visible_data  # Use the visible data in the selection handler

        if not selection_stream.index:
            # Clear table selection if no points are selected
            forecast_data_table.selection = []
            return

        # Get the selected index from the scatter plot (relative to visible_data)
        selected_indices = selection_stream.index

        # Use the original index from the visible_data to map to the correct row in the table
        selected_rows = visible_data.iloc[selected_indices]['index'].tolist()

        # Select the corresponding rows in the Tabulator
        forecast_data_table.selection = selected_rows

    # Watch for selection events on the plot
    selection_stream.param.watch(handle_selection, 'index')

    # Watch for changes in the table's value (which includes visibility toggling) and update the plot
    forecast_data_table.param.watch(update_plot, 'value')

    # Initial plot update
    update_plot()

    # Create the pop-up notification pane (initially hidden)
    popup = pn.pane.Alert(_("Changes Saved Successfully"), alert_type="success", visible=False)

    # Adjust the sizing modes of your components
    forecast_data_table.sizing_mode = 'stretch_both'
    plot_pane.sizing_mode = 'stretch_both'

    progress_bar = pn.indicators.Progress(name="Progress", value=0, width=300, visible=False)
    progress_message = pn.pane.Alert(_("Processing..."), alert_type="info", visible=False)

    def run_docker_container(client, full_image_name, volumes, environment, container_name, progress_bar):
        """
        Runs a Docker container and blocks until it completes.

        Args:
            client (docker.DockerClient): The Docker client instance.
            full_image_name (str): The full name of the Docker image to run.
            volumes (dict): A dictionary of volumes to bind.
            environment (list): A list of environment variables.
            container_name (str): The name to assign to the Docker container.
            progress_bar (Optional): Progress bar widget if available.

        Raises:
            docker.errors.ContainerError: If the container exits with a non-zero status.
        """
        # Define network_name at the function level so it's accessible everywhere
        network_name = 'ssh-tunnel-network'

        # Convert the SSH tunnel script path to an absolute path
        SSH_TUNNEL_SCRIPT_ABSOLUTE = os.path.join(
            get_absolute_path(os.getenv('ieasyhydroforecast_bin_path')),
            SSH_TUNNEL_SCRIPT_PATH
        )
        print(f"Using SSH tunnel script at: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
        try:
            with establish_ssh_tunnel(SSH_TUNNEL_SCRIPT_ABSOLUTE):
                # Check if a container with the specified name already exists
                try:
                    existing_container = client.containers.get(container_name)
                    print(f"Removing existing container '{container_name}' (ID: {existing_container.id})...")
                    existing_container.remove(force=True)
                    print(f"Container '{container_name}' removed.")
                except docker.errors.NotFound:
                    # Container does not exist, so we can proceed
                    pass
                except docker.errors.APIError as e:
                    print(f"Error removing existing container '{container_name}': {e}")
                    raise

                # Add important environment variables
                # if os is macOS, add the host.docker.internal and the port for the tunnel, for unix systems use 'host'
                # This does not actually seem to be used anywhere.
                if platform.system() == 'Darwin':
                    print(f"In select_and_plot_data: platform.system() == 'Darwin'")
                    environment.extend([
                        'SSH_TUNNEL_HOST=host.docker.internal',  # For macOS
                        f'SSH_TUNNEL_PORT={os.getenv("IEASYHYDRO_PORT")}'  # Your tunnel port
                    ])
                else:  # For Linux
                    print(f"In select_and_plot_data: platform.system() == 'Linux'")
                    environment.extend([
                        'SSH_TUNNEL_HOST=localhost',  # For Linux
                        f'SSH_TUNNEL_PORT={os.getenv("IEASYHYDRO_PORT")}'  # Your tunnel port
                    ])

                try:
                    client.networks.get(network_name)
                except docker.errors.NotFound:
                    client.networks.create(network_name, driver='bridge')

                # Now run the new container
                container = client.containers.run(
                    full_image_name,
                    detach=True,
                    environment=environment,
                    volumes=volumes,
                    name=container_name,
                    network_mode='host',
                    extra_hosts={
                        'host.docker.internal': 'host-gateway',
                        'localhost': '127.0.0.1'
                    }
                )
                print(f"Container '{container_name}' (ID: {container.id}) is running.")

                # Stream logs in real-time
                for line in container.logs(stream=True, follow=True):
                    print(line.decode().strip())

                # Monitor the container's progress
                progress_bar.value = 0
                while container.status != 'exited':
                    container.reload()  # Refresh the container's status
                    progress_bar.value += 10  # Increment progress
                    if progress_bar.value > 90:
                        progress_bar.value = 90  # Limit progress to 90% until the process finishes
                    time.sleep(1)
                result = container.wait()  # Ensure the container has finished
                progress_bar.value = 100  # Set progress to 100% after the container is done
                if result['StatusCode'] != 0:
                    print(f"Container '{container_name}' exited with status code {result['StatusCode']}.")
                    raise docker.errors.ContainerError(
                        container=container,
                        exit_status=result['StatusCode'],
                        command=None,
                        image=full_image_name
                    )
                else:
                    print(f"Container '{container_name}' has stopped successfully.")

        except Exception as e:
            print(f"Error running container '{container_name}': {e}")
        finally:
            # Cleanup: remove the network if it exists
            try:
                network = client.networks.get(network_name)
                network.remove()
            except (docker.errors.NotFound, docker.errors.APIError):
                pass

    # Create a save button
    save_button = pn.widgets.Button(name=_("Save Changes"), button_type="success")

    # Function to save table data to CSV and run Docker containers
    def save_to_csv(event):
        # Disable the save button and show the progress bar and message
        save_button.disabled = True
        progress_bar.visible = True
        progress_message.object = _("Processing...")
        progress_message.visible = True
        progress_bar.value = 0

        # Convert the table value back to a DataFrame
        updated_forecast_display = pd.DataFrame(forecast_data_table.value)

        # Translate the localized columns back to their original names
        updated_forecast_table = updated_forecast_display.rename(columns={
            _('year'): 'year',
            _('predictor'): 'predictor',
            _('discharge average'): 'discharge_avg',
            _('visible'): 'visible',
            _('index'): 'index'
        })

        # Explicitly reset the index before saving, so it becomes a column
        updated_forecast_table = updated_forecast_table.reset_index(drop=True)

        # Add the selected pentad information
        updated_forecast_table[horizon] = horizon_value

        # Save DataFrame to CSV, ensuring the index is saved
        updated_forecast_table[['index', 'year', 'visible']].to_csv(save_file_path, index=False)
        print(f"Data saved to {save_file_path}")

        # Show the pop-up notification
        popup.visible = True

        # Hide the popup after a short delay (optional)
        pn.state.onload(lambda: pn.state.add_periodic_callback(lambda: setattr(popup, 'visible', False), 2000, count=1))

        try:
            absolute_volume_path_config = get_absolute_path(env.get('ieasyforecast_configuration_path'))
            absolute_volume_path_internal_data = get_absolute_path(env.get('ieasyforecast_intermediate_data_path'))
            absolute_volume_path_discharge = get_absolute_path(env.get('ieasyforecast_daily_discharge_path'))
            absolute_volume_path_bin = get_absolute_path(env.get('ieasyhydroforecast_bin_path'))

            bind_volume_path_config = get_bind_path(env.get('ieasyforecast_configuration_path'))
            bind_volume_path_internal_data = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
            bind_volume_path_discharge = get_bind_path(env.get('ieasyforecast_daily_discharge_path'))
            bind_volume_path_bin = get_bind_path(env.get('ieasyhydroforecast_bin_path'))

            # Initialize Docker client
            print("In save_to_csv: Initializing Docker client...")
            try: 
                print("DOCKER_HOST:", os.environ.get("DOCKER_HOST"))
                client = docker.from_env()
                #client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
                print("#####################################")
                print(client.ping())
                print("Successfully connected to Docker daemon via Unix socket.")
                containers = client.containers.list()
                print("List of containers:", [c.name for c in containers])
            except Exception as e:
                print(f"Error initializing Docker client: {e}")

            # Define environment variables
            environment = [
                'IN_DOCKER_CONTAINER=True',
                'SAPPHIRE_PREDICTION_MODE=' + horizon.upper(),
                f'ieasyhydroforecast_env_file_path={bind_volume_path_config}/.env_develop_kghm'
            ]

            # Define volumes
            volumes = {
                absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
                absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
                absolute_volume_path_discharge: {'bind': bind_volume_path_discharge, 'mode': 'rw'},
                absolute_volume_path_bin: {'bind': bind_volume_path_bin, 'mode': 'rw'}
            }
            print("volumes: ", volumes)

            start_docker_runs = time.time()

            # Run the reset rundate module to update the rundate for the linear regression module
            run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate",
                                 progress_bar)
            temp_docker_end = time.time()
            print(f"Time taken to run reset_rundate: {temp_docker_end - start_docker_runs:.2f} seconds")
            temp_docker_start = time.time()

            # Run the linear_regression container with a hardcoded full image name
            run_docker_container(client, "mabesa/sapphire-linreg:latest", volumes, environment, "linreg", progress_bar)
            temp_docker_end = time.time()
            print(f"Time taken to run linreg: {temp_docker_end - temp_docker_start:.2f} seconds")
            temp_docker_start = time.time()

            # After linear_regression finishes, run the postprocessing container with a hardcoded full image name
            run_docker_container(client, "mabesa/sapphire-postprocessing:latest", volumes, environment,
                                 "postprocessing", progress_bar)
            temp_docker_end = time.time()
            print(f"Time taken to run postprocessing: {temp_docker_end - temp_docker_start:.2f} seconds")
            overall_time = temp_docker_end - start_docker_runs
            print(f"Overall time taken to run all containers: {overall_time:.2f} seconds")

            # When the container is finished, set progress to 100 and update message
            progress_bar.value = 100
            progress_message.object = _("Processing finished in {overall_time:.2f} seconds").format(overall_time=overall_time)

            # Wait a moment before hiding the progress bar and message
            time.sleep(2)
            progress_bar.visible = False
            progress_message.visible = False

        except docker.errors.DockerException as e:
            print(f"Error interacting with Docker: {e}")

        finally:
            # Re-enable the save button after the container finishes
            save_button.disabled = False
            progress_bar.value = 100  # Set progress to complete when done
            print("Docker container completed.")
            processing.data_reloader.data_needs_reload = True

        pn.state.onload(
            lambda: pn.state.add_periodic_callback(lambda: setattr(progress_bar, 'visible', False), 2000, count=1))
        pn.state.onload(
            lambda: pn.state.add_periodic_callback(lambda: setattr(progress_message, 'visible', False), 4000, count=1))

    # Attach the save_to_csv function to the button's click event
    save_button.on_click(save_to_csv)

    # **Bind the Save Changes button's disabled property to the shared state**
    def update_save_button(event):
        save_button.disabled = event.new

    # Watch for changes in pipeline_running and update the save_button accordingly
    app_state.param.watch(update_save_button, 'pipeline_running')

    # Set the initial state of the save_button
    save_button.disabled = app_state.pipeline_running

    # Adjust the layout to include the progress bar and message
    layout = pn.Column(
        pn.Row(forecast_data_table, plot_pane, sizing_mode='stretch_width'),
        pn.Row(save_button),
        pn.Row(popup),
        pn.Row(progress_bar),
        pn.Row(progress_message),

        sizing_mode='stretch_both'
    )

    return layout


def update_forecast_data(_, linreg_predictor, station, pentad_selector):
    def callback(event):
        return select_and_plot_data(_, linreg_predictor, station, pentad_selector)

    return callback


def create_reload_button():
    reload_button = pn.widgets.Button(name=_("Trigger forecasts"), button_type="danger")

    # Loading spinner and messages
    loading_spinner = pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='success', visible=False)
    progress_message = pn.pane.Alert("Processing...", alert_type="info", visible=False)
    warning_message = pn.pane.Alert("Please do not reload this page until processing is done!", alert_type='danger',
                                    visible=False)

    # Function to check if any containers are running
    def check_containers_running():
        try:
            print("In create_reload_button: Checking if containers are running...")
            try: 
                print("DOCKER_HOST:", os.environ.get("DOCKER_HOST"))
                client = docker.from_env()
                #client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
                print("#####################################")
                print(client.ping())
                print("Successfully connected to Docker daemon via Unix socket.")
                containers = client.containers.list()
                print("List of containers:", [c.name for c in containers])
            except Exception as e:
                print(f"Error initializing Docker client: {e}")

            container_names = [
                "preprunoff",
                "reset_rundate",
                "linreg",
                "prepgateway",
                "conceptmod",
                "postprocessing",
            ]
            # Add the ML model containers
            for model in ["TFT", "TIDE", "TSMIXER", "ARIMA"]:
                for mode in ["PENTAD", "DECAD"]:
                    container_names.append(f"ml_{model}_{mode}")
            running_containers = client.containers.list(filters={"status": "running"})
            running_container_names = [container.name for container in running_containers]
            for name in container_names:
                if name in running_container_names:
                    return True
            return False
        except FileNotFoundError as fnfe:
            print(f"No containers up: {fnfe}")
            return False
        except Exception as e:
            print(f"Error checking containers: {e}")
            return False

    # Function to update button status when page is loaded or when triggered
    @pn.io.with_lock  # Ensure thread safety
    def update_button_status():
        if app_state.pipeline_running or check_containers_running():
            reload_button.disabled = True
            loading_spinner.visible = True
            progress_message.object = _("Processing...")
            progress_message.visible = True
            warning_message.object = _("Please do not reload this page until processing is done!")
            warning_message.visible = True
        else:
            reload_button.disabled = False
            loading_spinner.visible = False
            progress_message.visible = False
            warning_message.visible = False

    # Check container status when the page is first loaded
    pn.state.onload(update_button_status)

    # Function to run the pipeline and update the UI properly
    def run_pipeline(event):
        # Check containers again when button is clicked
        if check_containers_running():
            warning_message.object = _("Containers are still running. Please wait.")
            warning_message.visible = True
            return

        # Update shared state to indicate the pipeline is running
        app_state.pipeline_running = True

        # Disable the reload button and show loading spinner
        reload_button.disabled = True
        loading_spinner.visible = True
        progress_message.object = _("Processing...")
        progress_message.visible = True
        warning_message.object = _("Please do not reload this page until processing is done!")
        warning_message.visible = True

        def run_docker_pipeline():
            try:
                # Initialize Docker client
                try: 
                    print("DOCKER_HOST:", os.environ.get("DOCKER_HOST"))
                    client = docker.from_env()
                    #client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
                    print("#####################################")
                    print(client.ping())
                    print("Successfully connected to Docker daemon via Unix socket.")
                    containers = client.containers.list()
                    print("List of containers:", [c.name for c in containers])
                except Exception as e:
                    print(f"Error initializing Docker client: {e}")

                # Define environment variables
                environment = [
                    'SAPPHIRE_OPDEV_ENV=True',
                    'IN_DOCKER_CONTAINER=True',
                    f'SAPPHIRE_PREDICTION_MODE={os.getenv("sapphire_forecast_horizon", "pentad").upper()}',
                    f'ieasyhydroforecast_env_file_path={get_bind_path(env.get("ieasyforecast_configuration_path"))}/.env_develop_kghm'
                ]
                print("environment: \n", environment)

                # Define volumes
                volumes = {
                    get_absolute_path(env.get("ieasyforecast_configuration_path")): {
                        'bind': get_bind_path(env.get("ieasyforecast_configuration_path")),
                        'mode': 'rw'
                    },
                    get_absolute_path(env.get("ieasyforecast_intermediate_data_path")): {
                        'bind': get_bind_path(env.get("ieasyforecast_intermediate_data_path")),
                        'mode': 'rw'
                    },
                    get_absolute_path(env.get("ieasyforecast_daily_discharge_path")): {
                        'bind': get_bind_path(env.get("ieasyforecast_daily_discharge_path")),
                        'mode': 'rw'
                    },
                    get_absolute_path(env.get("ieasyhydroforecast_conceptual_model_path")): {
                        'bind': get_bind_path(env.get("ieasyhydroforecast_conceptual_model_path")),
                        'mode': 'rw'
                    },
                    get_absolute_path(env.get("ieasyhydroforecast_bin_path")): {
                        'bind': get_bind_path(env.get("ieasyhydroforecast_bin_path")),
                        'mode': 'rw'
                    },
                    "/var/run/docker.sock": {
                        'bind': "/var/run/docker.sock",
                        'mode': 'rw'
                    }
                }

                # Run the preprunoff container
                start_docker_runs = time.time()
                run_docker_container(client, "mabesa/sapphire-preprunoff:latest", volumes, environment, "preprunoff")
                temp_docker_end = time.time()
                print(f"Time taken to run preprunoff: {temp_docker_end - start_docker_runs:.2f} seconds")
                temp_docker_start = time.time()

                # Run the reset_rundate container
                run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate")
                temp_docker_end = time.time()
                print(f"Time taken to run reset_rundate: {temp_docker_end - temp_docker_start:.2f} seconds")
                temp_docker_start = time.time()

                # Run the linear_regression container
                run_docker_container(client, "mabesa/sapphire-linreg:latest", volumes, environment, "linreg")
                temp_docker_end = time.time()
                print(f"Time taken to run linreg: {temp_docker_end - temp_docker_start:.2f} seconds")
                temp_docker_start = time.time()

                # Run the prepgateway container
                # No need to re-run prepgateway as there is no new data
                #run_docker_container(client, "mabesa/sapphire-prepgateway:latest", volumes, environment, "prepgateway")

                # Only run the ML models if required to run and for available models
                run_ML_models = os.getenv("ieasyhydroforecast_run_ML_models", "True")
                if run_ML_models == "True":
                    model_list = os.getenv("ieasyhydroforecast_available_ML_models", "TFT,TIDE,TSMIXER").split(",")
                    print(f"Available ML models: {model_list}")
                    mode = os.getenv("sapphire_forecast_horizon", "pentad").upper()

                    for model in model_list:
                        container_name = f"ml_{model}_{mode}"
                        run_docker_container(client, f"mabesa/sapphire-ml:latest", volumes,
                                             environment + [f"SAPPHIRE_MODEL_TO_USE={model}",
                                                            f"SAPPHIRE_PREDICTION_MODE={mode}", f"RUN_MODE=forecast"],
                                             container_name)
                temp_docker_end = time.time()
                print(f"Time taken to run ML models: {temp_docker_end - temp_docker_start:.2f} seconds")
                temp_docker_start = time.time()

                # Run the conceptmod container
                # No need to re-run this one as it is very slow
                #run_docker_container(client, "mabesa/sapphire-conceptmod:latest", volumes, environment, "conceptmod")

                # Run the postprocessing container
                run_docker_container(client, "mabesa/sapphire-postprocessing:latest", volumes, environment,
                                     "postprocessing")
                temp_docker_end = time.time()
                print(f"Time taken to run postprocessing: {temp_docker_end - temp_docker_start:.2f} seconds")
                overall_time = temp_docker_end - start_docker_runs
                print(f"Overall time taken to run all containers: {overall_time:.2f} seconds")

                # Update message after all containers have run
                progress_message.object = _("Processing finished in {overall_time:.2f} seconds").format(
                    overall_time=overall_time)
                time.sleep(2)
            except docker.errors.ContainerError as ce:
                progress_message.object = f"Container Error: {ce}"
                print(f"Container Error: {ce}")
            except docker.errors.DockerException as de:
                progress_message.object = f"Docker Error: {de}"
                print(f"Docker Error: {de}")
            except ValueError as ve:
                progress_message.object = f"Configuration Error: {ve}"
                print(f"Configuration Error: {ve}")
            finally:
                # Ensure thread-safe update of UI
                @pn.io.with_lock
                def reset_ui_after_pipeline():
                    # Hide progress indicators and re-enable the reload button
                    loading_spinner.visible = False
                    progress_message.visible = False
                    warning_message.visible = False
                    reload_button.disabled = False
                    app_state.pipeline_running = False  # Update shared state

                reset_ui_after_pipeline()  # Safely update the UI after the process

        # Run the pipeline in a separate thread to keep the UI responsive
        threading.Thread(target=run_docker_pipeline).start()

    # Attach the run_pipeline function to the reload button's click event
    reload_button.on_click(run_pipeline)

    # Create a card for the reload button
    reload_card = pn.Card(
        pn.Column(
            pn.pane.Markdown(
                _("Click the button below to trigger the forecast pipeline.\nThis will re-run all forecasts for the latest data.\nThis process may take a few minutes to complete.\nNote: Current forecasts will be overwritten.")),
            reload_button,
            loading_spinner,
            progress_message,
            warning_message,
        ),
        title=_('Manual re-run of latest forecasts'),
        width_policy='fit',
        collapsed=True,
    )

    return reload_card


def run_docker_container(client, full_image_name, volumes, environment, container_name):
    """
    Runs a Docker container and blocks until it completes.

    Args:
        client (docker.DockerClient): The Docker client instance.
        full_image_name (str): The full name of the Docker image to run.
        volumes (dict): A dictionary of volumes to bind.
        environment (list): A list of environment variables.
        container_name (str): The name to assign to the Docker container.

    Raises:
        docker.errors.ContainerError: If the container exits with a non-zero status.
    """
    # Convert the SSH tunnel script path to an absolute path
    SSH_TUNNEL_SCRIPT_ABSOLUTE = os.path.join(
        get_absolute_path(os.getenv('ieasyhydroforecast_bin_path')),
        SSH_TUNNEL_SCRIPT_PATH
    )
    # Test if file exists
    if not os.path.isfile(SSH_TUNNEL_SCRIPT_ABSOLUTE):
        print(f"SSH tunnel script not found at: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
        print(f"trying a different path")
        # Try to access the file at ieasyhydroforecast_container_data_ref_dir
        SSH_TUNNEL_SCRIPT_ABSOLUTE = os.path.join(
            os.getenv('ieasyhydroforecast_container_data_ref_dir'),
            'bin', 
            SSH_TUNNEL_SCRIPT_PATH
        )
        if not os.path.isfile(SSH_TUNNEL_SCRIPT_ABSOLUTE):
            print(f"SSH tunnel script not found at: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
            return
        
    # Test if file is executable
    if not os.access(SSH_TUNNEL_SCRIPT_ABSOLUTE, os.X_OK):
        print(f"SSH tunnel script is not executable: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
        return
    print(f"Using SSH tunnel script at: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
    try:
        # Establish SSH tunnel before running the container
        # subprocess.run([SSH_TUNNEL_SCRIPT_ABSOLUTE], check=True)
        subprocess.run(['bash', SSH_TUNNEL_SCRIPT_ABSOLUTE], check=True)
        # Remove existing container with the same name if it exists
        try:
            existing_container = client.containers.get(container_name)
            print(f"Removing existing container '{container_name}' (ID: {existing_container.id})...")
            existing_container.remove(force=True)
            print(f"Container '{container_name}' removed.")
        except docker.errors.NotFound:
            # Container does not exist, proceed to run
            pass
        except docker.errors.APIError as e:
            print(f"Error removing existing container '{container_name}': {e}")
            # Optionally, you can decide to continue even if the container couldn't be removed

        # Run the new container
        container = client.containers.run(
            full_image_name,
            detach=True,
            environment=environment,
            volumes=volumes,
            name=container_name,
            network='host'
        )
        print(f"Container '{container_name}' (ID: {container.id}) is running.")

        # Wait for the container to finish
        result = container.wait()  # This will block until the container exits
        if result['StatusCode'] != 0:
            print(f"Container '{container_name}' exited with status code {result['StatusCode']}.")
            # Optionally log the error or add to a list of failed containers
        else:
            print(f"Container '{container_name}' has stopped successfully.")

        # Remove the container after it has finished
        try: 
            container.remove(force=True)
            print(f"Container '{container_name}' removed after completion.")
        except docker.errors.APIError as e:
            print(f"Error removing container '{container_name}': {e}")
            
    except Exception as e:
        print(f"Error running container '{container_name}': {e}")


# endregion


# region skill metrics
def plot_forecast_skill(
        _, hydrograph_pentad_all, forecasts_all, station_widget, date_picker,
        model_checkbox, range_selection_widget, manual_range_widget,
        show_range_button):
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_x_label = _("Pentad of the month (starting from January 1)")
        area_x_lim = 72
    else:
        horizon_in_year = "decad_in_year"
        horizon_x_label = _("Decad of the month (starting from January 1)")
        area_x_lim = 36

    # Convert the date from the date picker to a pandas Timestamp
    selected_date = pd.Timestamp(date_picker)
    title_date = pd.to_datetime(date_picker)
    title_date_str = selected_date.strftime('%Y-%m-%d')

    # Filter the forecasts for the selected station and models
    forecast_pentad = forecasts_all[forecasts_all['model_short'].isin(model_checkbox)].copy()
    forecast_pentad = forecast_pentad[forecast_pentad['station_labels'] == station_widget]
    ## We only need the values of the past 365 days (or the past year)
    ##forecast_pentad = forecast_pentad[forecast_pentad['date'] >= selected_date - pd.Timedelta(days=365)]
    # For each model and pentad/decad of year, get the most recent non-NaN value
    forecast_pentad = (forecast_pentad
                  .sort_values('date', ascending=False)  # Sort by date, most recent first
                  .groupby(['model_short', horizon_in_year])  # Group by model and pentad/decad
                  .apply(lambda x: x.dropna(subset=['sdivsigma', 'accuracy']).head(1))  # Get first non-NaN row
                  .reset_index(drop=True))  # Reset index

    # Multiply accruacy by 100 to get percentage
    forecast_pentad['accuracy'] = forecast_pentad['accuracy'] * 100
    # Drop any duplicates in date and model_short
    current_forecast_pentad = forecast_pentad[forecast_pentad['date'] == selected_date]
    # Sort by model_short and pentad_in_year
    forecast_pentad = forecast_pentad.sort_values(by=['model_short', horizon_in_year])

    # Plot the effectiveness of the forecast method
    # Plot a green area between y = 0 and y = 0.6
    hv_06a = hv.Area(pd.DataFrame({"x": [1, area_x_lim], "y": [0.6, 0.6]}),
                     kdims=["x"], vdims=["y"], label=_("Effectiveness") + " <= 0.6") \
        .opts(alpha=0.05, color="green", line_width=0)
    hv_08a = hv.Area(pd.DataFrame({"x": [1, area_x_lim], "y": [0.8, 0.8]}),
                     kdims=["x"], vdims=["y"], label=_("Effectiveness") + " <= 0.8") \
        .opts(alpha=0.05, color="orange", line_width=0)
    hv_current_forecast_skill_effectiveness = plot_current_runoff_forecasts(
        data=current_forecast_pentad,
        date_col=horizon_in_year,
        forecast_data_col='sdivsigma',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[-]')
    hv_forecast_skill_effectiveness = plot_runoff_forecasts_steps(
        forecast_pentad, date_col=horizon_in_year,
        forecast_data_col='sdivsigma',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[-]')

    # Plot column 'sdivsigma' over the pentad of the year
    title_effectiveness = _("Station {station} on {date}, data from 2010 - {year}").format(
        station=station_widget, date=title_date_str, year=title_date.year
    )
    effectiveness_plot = hv_08a * hv_06a * hv_forecast_skill_effectiveness * hv_current_forecast_skill_effectiveness
    effectiveness_plot.opts(
        responsive=True,
        hooks=[
            remove_bokeh_logo,
            lambda p, e: add_custom_xticklabels_pentad(_, p, e)
        ],
        title=title_effectiveness, shared_axes=False,
        # legend_position='bottom_left',  # 'right',
        xlabel=horizon_x_label, ylabel=_("Effectiveness [-]"),
        show_grid=True,  
        ylim=(0, 1.4),
        fontsize={'legend': 8, 'title': 10}, fontscale=1.2
    )

    # Plot the forecast accuracy
    title_effectiveness = _("Station {station} on {date}, data from 2010 - {year}").format(
        station=station_widget, date=title_date_str, year=title_date.year
    )

    hv_current_forecast_skill_accuracy = plot_current_runoff_forecasts(
        data=current_forecast_pentad,
        date_col=horizon_in_year,
        forecast_data_col='accuracy',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[%]')
    hv_forecast_skill_accuracy = plot_runoff_forecasts_steps(
        forecast_pentad, date_col=horizon_in_year,
        forecast_data_col='accuracy',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[%]')

    # Plot column 'accuracy' over the pentad of the year
    title_accuracy = _("Station {station} on {date}, data from 2010 - {year}").format(
        station=station_widget, date=title_date_str, year=title_date.year
    )
    accuracy_plot = hv_forecast_skill_accuracy * hv_current_forecast_skill_accuracy
    accuracy_plot.opts(
        responsive=True,
        hooks=[
            remove_bokeh_logo,
            lambda p, e: add_custom_xticklabels_pentad(_, p, e)
        ],
        title=title_accuracy, shared_axes=False,
        # legend_position='bottom_left',  # 'right',
        xlabel=horizon_x_label, ylabel=_("Accuracy [%]"),
        show_grid=True,  
        ylim=(0, 100),
        xlim=(1, area_x_lim),
        fontsize={'legend': 8, 'title': 10}, fontscale=1.2
    )

    # Plot observed runoff against forecasted runoff

    # Create a column layout for the output
    all_skill_figures = pn.Column(
        effectiveness_plot,
        accuracy_plot,
    )

    return all_skill_figures


def add_month_pentad_per_month_to_df(df):
    """Based on column pentad_in_year, add columns month and pentad_in_month."""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_in_month = 'pentad_in_month'
        horizon_fn_month = tl.get_pentad
        horizon_fn_year = tl.get_date_for_pentad
    else:
        horizon_in_year = "decad_in_year"
        horizon_in_month = 'decad_in_month'
        horizon_fn_month = tl.get_decad_in_month
        horizon_fn_year = tl.get_date_for_decad

    # Get date for pentad in year
    df['date'] = df[horizon_in_year].apply(horizon_fn_year)
    # print('df: ', df.head())
    # Get month for date
    df['month'] = pd.to_datetime(df['date']).dt.month
    # Get pentad in month
    df[horizon_in_month] = df['date'].apply(horizon_fn_month)
    return df


def create_skill_table(_, forecast_stats):
    """Creates a tabulator widget for the forecast statistics."""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        horizon_in_month = 'pentad_in_month'
        horizon_title = _('Pentad')
        horizon_placeholder = _('Filter by pentad')
    else:
        horizon_in_year = "decad_in_year"
        horizon_in_month = 'decad_in_month'
        horizon_title = _('Decad')
        horizon_placeholder = _('Filter by decad')

    # Get pentad in month and month
    forecast_stats = add_month_pentad_per_month_to_df(forecast_stats)

    # Do not show column model_long and pentad_in_year
    forecast_stats_loc = forecast_stats.drop(columns=['model_long', horizon_in_year, 'date']).copy()

    # Order the columns in the dataframe as follows:
    # code, model_short, month, pentad_in_month, sdivsigma, nse, delta, accuracy, mae
    forecast_stats_loc = forecast_stats_loc[
        ['code', 'model_short', 'month', horizon_in_month, 'sdivsigma', 'nse', 'delta', 'accuracy', 'mae']]

    # Sort the columns by code, month and pentad_in_month
    forecast_stats_loc = forecast_stats_loc.sort_values(by=['code', 'month', horizon_in_month])

    # Get unique values for each filterable column
    unique_codes = sorted(forecast_stats_loc['code'].unique().tolist())
    unique_models = sorted(forecast_stats_loc['model_short'].unique().tolist())
    unique_months = sorted(forecast_stats_loc['month'].unique().tolist())
    unique_pentads = sorted(forecast_stats_loc[horizon_in_month].unique().tolist())

    # Rename columns for better display
    forecast_stats_loc.rename(columns={
        horizon_in_month: horizon_title,
        'month': _('Month'),
        'code': _('Code'),
        'model_short': _('Model'),
        'sdivsigma': _('s/σ'),
        'nse': _('NSE'),
        'delta': _('δ'),
        'accuracy': _('Accuracy'),
        'mae': _('MAE'),
    }, inplace=True)

    # Define formatters for numeric columns
    formatters = {
        _('s/σ'): {'type': 'number', 'precision': 3},
        _('NSE'): {'type': 'number', 'max': 1.0, 'precision': 3},
        _('δ'): {'type': 'number', 'precision': 3},
        _('Accuracy'): {'type': 'number', 'max': 1.0, 'precision': 3},
        _('MAE'): {'type': 'number', 'precision': 3},
    }

    # Add tabulator editors for the header_filters with predefined values
    filters = {
        horizon_title: {
            'type': 'list',
            'values': unique_pentads,
            'placeholder': horizon_placeholder
        },
        _('Month'): {
            'type': 'list',
            'values': unique_months,
            'placeholder': _('Filter by month')
        },
        _('Model'): {
            'type': 'list',
            'values': unique_models,
            'placeholder': _('Filter by model')
        },
        _('Code'): {
            'type': 'list',
            'values': unique_codes,
            'placeholder': _('Filter by code')
        },
    }

    # Create a Tabulator widget for the forecast statistics
    forecast_stats_table = pn.widgets.Tabulator(
        value=forecast_stats_loc,
        theme='bootstrap',
        editors={col: None for col in forecast_stats_loc.columns},
        configuration={
            'columnFilters': True,
            'pagination': 'local',  # Use local pagination
            'paginationSize': 72000,  # Increased page size to show more rows
            'paginationSizeSelector': [72, 720, 7200, 72000],  # Allow user to change page size
            'filterMode': 'local',  # Use local filtering
            'sortMode': 'local',  # Use local sorting
            'movableColumns': True,  # Allow column reordering
            'headerFilterLiveFilter': True,  # Live filtering as you type
            'selectable': True,  # Allow row selection
            'columnsResizable': True,  # Allow column resizing
        },
        layout='fit_data_stretch',
        sizing_mode='stretch_width',
        height=400,
        show_index=False,
        header_filters=filters,
        formatters=formatters,
        page_size=72000  # Match the pagination size
    )

    return forecast_stats_table
# endregion
