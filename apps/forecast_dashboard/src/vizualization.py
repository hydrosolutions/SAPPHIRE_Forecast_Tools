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
import holoviews as hv
from holoviews import streams
from holoviews.streams import PointDraw, Selection1D
import panel as pn
from bokeh.models import Label, HoverTool, FixedTicker, FuncTickFormatter, CustomJSTickFormatter, LinearAxis, NumberFormatter, DateFormatter
from bokeh.models.formatters import DatetimeTickFormatter
from bokeh.models.widgets.tables import CheckboxEditor, BooleanFormatter
from bokeh.transform import jitter
from scipy import stats
from sklearn.linear_model import LinearRegression
from functools import partial
from dotenv import load_dotenv
import re
import docker
import threading



from . import processing

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


# Defining colors (as global variables)
# https://www.color-hex.com/color/307096
# Blue color palette for the norm runoff
runoff_full_range_color = "#eaf0f4"
runoff_90percentile_range_color = "#d5e2ea"
runoff_50percentile_range_color = "#c0d4df"
runoff_mean_color = "#307096"
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
    #runoff_forecast_color_list = ["#5c1d45", "#6f2453", "#832a62", "#963070", "#a9367e", "#bd3c8d", "#c64d99"]
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
    plot.state.toolbar.logo = None

def add_custom_xticklabels_daily(_, leap_year, plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.
    if leap_year:
        ticks = [1,6,11,16,21,26,  # Jan
             32,37,42,47,52,57,
             61,66,71,76,81,86,  # Mar
             92,97,102,107,112,117,
             122,127,132,137,142,147,  # May
             153,158,163,168,173,178,
             183,188,193,198,203,208,  # Jul
             214,219,224,229,234,239,
             245,250,255,260,265,270,  # Sep
             275,280,285,290,295,300,
             306,311,316,321,326,331,  # Nov
             336,341,346,351,356,361]
        labels = {1:_('Jan')+', 1', 6:'2', 11:'3', 16:'4', 21:'5', 26:'6',
              32:_('Feb')+', 1', 37:'2', 42:'3', 47:'4', 52:'5', 57:'6',
              61:_('Mar')+', 1', 66:'2', 71:'3', 76:'4', 81:'5', 86:'6',
              92:_('Apr')+', 1', 97:'2', 102:'3', 107:'4', 112:'5', 117:'6',
              122:_('May')+', 1', 127:'2', 132:'3', 137:'4', 142:'5', 147:'6',
              153:_('Jun')+', 1', 158:'2', 163:'3', 168:'4', 173:'5', 178:'6',
              183:_('Jul')+', 1', 188:'2', 193:'3', 198:'4', 203:'5', 208:'6',
              214:_('Aug')+', 1', 219:'2', 224:'3', 229:'4', 234:'5', 239:'6',
              245:_('Sep')+', 1', 250:'2', 255:'3', 260:'4', 265:'5', 270:'6',
              275:_('Oct')+', 1', 280:'2', 285:'3', 290:'4', 295:'5', 300:'6',
              306:_('Nov')+', 1', 311:'2', 316:'3', 321:'4', 326:'5', 331:'6',
              336:_('Dec')+', 1', 341:'2', 346:'3', 351:'4', 356:'5', 361:'6'}
    else:
        ticks = [1,6,11,16,21,26,  # Jan
             32,37,42,47,52,57,
             60,65,70,75,80,85,  # Mar
             91,96,101,106,111,116,
             121,126,131,136,141,146,  # May
             152,157,162,167,172,177,
             182,187,192,197,202,207,  # Jul
             213,218,223,228,233,238,
             244,249,254,259,264,269,  # Sep
             274,279,284,289,294,299,
             305,310,315,320,325,330,  # Nov
             335,340,345,350,355,360]
        labels = {1:_('Jan')+', 1', 6:'2', 11:'3', 16:'4', 21:'5', 26:'6',
              32:_('Feb')+', 1', 37:'2', 42:'3', 47:'4', 52:'5', 57:'6',
              60:_('Mar')+', 1', 65:'2', 70:'3', 75:'4', 80:'5', 85:'6',
              91:_('Apr')+', 1', 96:'2', 101:'3', 106:'4', 111:'5', 116:'6',
              121:_('May')+', 1', 126:'2', 131:'3', 136:'4', 141:'5', 146:'6',
              152:_('Jun')+', 1', 157:'2', 162:'3', 167:'4', 172:'5', 177:'6',
              182:_('Jul')+', 1', 187:'2', 192:'3', 197:'4', 202:'5', 207:'6',
              213:_('Aug')+', 1', 218:'2', 223:'3', 228:'4', 233:'5', 238:'6',
              244:_('Sep')+', 1', 249:'2', 254:'3', 259:'4', 264:'5', 269:'6',
              274:_('Oct')+', 1', 279:'2', 284:'3', 289:'4', 294:'5', 299:'6',
              305:_('Nov')+', 1', 310:'2', 315:'3', 320:'4', 325:'5', 330:'6',
              335:_('Dec')+', 1', 340:'2', 345:'3', 350:'4', 355:'5', 360:'6'}

    # Create a FixedTicker and a FuncTickFormatter with the specified ticks and labels
    ticker = FixedTicker(ticks=ticks)
    formatter = CustomJSTickFormatter(code="""
        var labels = %s;
        return labels[tick];
    """ % labels)

    # Create a second x-axis and set its range to match the original x-axis
    second_x_axis = LinearAxis(
        ticker=ticker, formatter=formatter, axis_label=_('Month, pentad in month'),
        major_label_orientation=math.pi/2)

    # Add the second x-axis to the plot
    plot.state.add_layout(second_x_axis, 'below')

def add_custom_xticklabels_daily_dates(_, leap_year, plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.

    # Create date ticks for the current year. Ticks are on the first day of the
    # year, then on the 5th, 10th, 15th, 20th, 25th and on the last day of each
    # month. The last tick is on the last day of the year.
    # Check if the dashboard is opened in a leap year
    year = dt.datetime.now().year

    if leap_year:
        month_day_tuples = [(1, 1), (1, 6), (1, 11), (1, 16), (1, 21), (1, 26),  # Jan
                (2, 5), (2, 10), (2, 15), (2, 20), (2, 25), (2, 29),
                (3, 5), (3, 10), (3, 15), (3, 20), (3, 25), (3, 31),  # Mar
                (4, 5), (4, 10), (4, 15), (4, 20), (4, 25), (4, 30),
                (5, 5), (5, 10), (5, 15), (5, 20), (5, 25), (5, 31),  # May
                (6, 5), (6, 10), (6, 15), (6, 20), (6, 25), (6, 30),
                (7, 5), (7, 10), (7, 15), (7, 20), (7, 25), (7, 31),  # Jul
                (8, 5), (8, 10), (8, 15), (8, 20), (8, 25), (8, 31),
                (9, 5), (9, 10), (9, 15), (9, 20), (9, 25), (9, 30),  # Sep
                (10, 5), (10, 10), (10, 15), (10, 20), (10, 25), (10, 31),
                (11, 5), (11, 10), (11, 15), (11, 20), (11, 25), (11, 30),  # Nov
                (12, 5), (12, 10), (12, 15), (12, 20), (12, 25), (12, 31)]
    else:
        month_day_tuples = [(1, 1), (1, 6), (1, 11), (1, 16), (1, 21), (1, 26),  # Jan
                (2, 5), (2, 10), (2, 15), (2, 20), (2, 25), (2, 28),
                (3, 5), (3, 10), (3, 15), (3, 20), (3, 25), (3, 31),  # Mar
                (4, 5), (4, 10), (4, 15), (4, 20), (4, 25), (4, 30),
                (5, 5), (5, 10), (5, 15), (5, 20), (5, 25), (5, 31),  # May
                (6, 5), (6, 10), (6, 15), (6, 20), (6, 25), (6, 30),
                (7, 5), (7, 10), (7, 15), (7, 20), (7, 25), (7, 31),  # Jul
                (8, 5), (8, 10), (8, 15), (8, 20), (8, 25), (8, 31),
                (9, 5), (9, 10), (9, 15), (9, 20), (9, 25), (9, 30),  # Sep
                (10, 5), (10, 10), (10, 15), (10, 20), (10, 25), (10, 31),
                (11, 5), (11, 10), (11, 15), (11, 20), (11, 25), (11, 30),  # Nov
                (12, 5), (12, 10), (12, 15), (12, 20), (12, 25), (12, 31)]

    # Create a date for each tick, based on days, months (1-12), and the year
    datetimes = [dt.datetime(year, month, day) for month, day in month_day_tuples]
    print("datetimes\n", datetimes[0])
    # Convert datetimes to timestamps
    timestamps = [int(time.mktime(dt.timetuple())) for dt in datetimes]
    print("ticks\n", timestamps[0])

    labels = {timestamps[0]:_('Jan')+', 1', timestamps[1]:'2', timestamps[2]:'3',
              timestamps[3]:'4', timestamps[4]:'5', timestamps[5]:'6',
              timestamps[6]:_('Feb')+', 1', timestamps[7]:'2', timestamps[8]:'3',
              timestamps[9]:'4', timestamps[10]:'5', timestamps[11]:'6',
              timestamps[12]:_('Mar')+', 1', timestamps[13]:'2', timestamps[14]:'3',
              timestamps[15]:'4', timestamps[16]:'5', timestamps[17]:'6',
              timestamps[18]:_('Apr')+', 1', timestamps[19]:'2', timestamps[20]:'3',
              timestamps[21]:'4', timestamps[22]:'5', timestamps[23]:'6',
              timestamps[24]:_('May')+', 1', timestamps[25]:'2', timestamps[26]:'3',
              timestamps[27]:'4', timestamps[28]:'5', timestamps[29]:'6',
              timestamps[30]:_('Jun')+', 1', timestamps[31]:'2', timestamps[32]:'3',
              timestamps[33]:'4', timestamps[34]:'5', timestamps[35]:'6',
              timestamps[36]:_('Jul')+', 1', timestamps[37]:'2', timestamps[38]:'3',
              timestamps[39]:'4', timestamps[40]:'5', timestamps[41]:'6',
              timestamps[42]:_('Aug')+', 1', timestamps[43]:'2', timestamps[44]:'3',
              timestamps[45]:'4', timestamps[46]:'5', timestamps[47]:'6',
              timestamps[48]:_('Sep')+', 1', timestamps[49]:'2', timestamps[50]:'3',
              timestamps[51]:'4', timestamps[52]:'5', timestamps[53]:'6',
              timestamps[54]:_('Oct')+', 1', timestamps[55]:'2', timestamps[56]:'3',
              timestamps[57]:'4', timestamps[58]:'5', timestamps[59]:'6',
              timestamps[60]:_('Nov')+', 1', timestamps[61]:'2', timestamps[62]:'3',
              timestamps[63]:'4', timestamps[64]:'5', timestamps[65]:'6',
              timestamps[66]:_('Dec')+', 1', timestamps[67]:'2', timestamps[68]:'3',
              timestamps[69]:'4', timestamps[70]:'5', timestamps[71]:'6'}

    print("labels\n", labels[0])

    # Create a FixedTicker and a CustomJSTickFormatter with the specified ticks and labels
    ticker = FixedTicker(ticks=list(labels.keys()))

    # Create a CustomJSTickFormatter with the specified ticks and labels
    formatter = CustomJSTickFormatter(code="""
                                  return labels[tick] || tick;
                                  """, args={'labels': labels})

    # Create a second x-axis and set its range to match the original x-axis
    second_x_axis = LinearAxis(
        ticker=ticker, formatter=formatter, axis_label=_('Month, pentad in month'),
        major_label_orientation=math.pi/2)

    # Add the second x-axis to the plot
    plot.state.add_layout(second_x_axis, 'below')

def add_custom_xticklabels_pentad(_, plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.
    ticks = list(range(1,72,1))  # Replace with your desired positions
    labels = {1:_('Jan')+', 1', 2:'2', 3:'3', 4:'4', 5:'5', 6:'6',
              7:_('Feb')+', 1', 8:'2', 9:'3', 10:'4', 11:'5', 12:'6',
              13:_('Mar')+', 1', 14:'2', 15:'3', 16:'4', 17:'5', 18:'6',
              19:_('Apr')+', 1', 20:'2', 21:'3', 22:'4', 23:'5', 24:'6',
              25:_('May')+', 1', 26:'2', 27:'3', 28:'4', 29:'5', 30:'6',
              31:_('Jun')+', 1', 32:'2', 33:'3', 34:'4', 35:'5', 36:'6',
              37:_('Jul')+', 1', 38:'2', 39:'3', 40:'4', 41:'5', 42:'6',
              43:_('Aug')+', 1', 44:'2', 45:'3', 46:'4', 47:'5', 48:'6',
              49:_('Sep')+', 1', 50:'2', 51:'3', 52:'4', 53:'5', 54:'6',
              55:_('Oct')+', 1', 56:'2', 57:'3', 58:'4', 59:'5', 60:'6',
              61:_('Nov')+', 1', 62:'2', 63:'3', 64:'4', 65:'5', 66:'6',
              67:_('Dec')+', 1', 68:'2', 69:'3', 70:'4', 71:'5', 72:'6'}

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
    plot.handles['xaxis'].major_label_orientation = math.pi/2

def add_custom_tooltip(_, plot, element):
    hover = plot.state.select_one(HoverTool)
    hover.tooltips = [
        (_('forecast model short column name'), "@name"),
        (_('pentad_of_year column name'), "@x"),
        (_('forecasted_discharge column name'), "@y")
    ]

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
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in range(len(models) - len(runoff_forecast_colors))]
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
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        print("MODEL: ", model)
        print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        #latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        #legend_entry = model + ": " + latest_forecast + " " + unit_string
        legend_entry = model + ": Past forecasts"

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            ('Pentad', f'@{date_col}'),
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
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in range(len(models) - len(runoff_forecast_colors))]
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
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted']
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot']
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid']
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors
        line_types = ['solid', 'dashed', 'dotdash', 'dotted', 'dashdot', 'solid', 'dashed']

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        print("MODEL: ", model)
        print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        #latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        #legend_entry = model + ": " + latest_forecast + " " + unit_string
        legend_entry = model + ": Past forecasts"

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            ('Pentad', f'@{date_col}'),
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

def plot_current_runoff_forecasts(data, date_col, forecast_data_col,
        forecast_name_col, runoff_forecast_colors, unit_string):

    # Return an empty plot if the data DataFrame is empty
    if data.empty:
        return hv.Curve([])

    overlay = None

    # Decide which colors to display
    # list of unique models in data
    models = data[forecast_name_col].unique()
    print(f"Number of models in plot_current_runoff_forecasts: {len(models)}")
    print(f"Models in plot_current_runoff_forecasts: {models}")
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]
        #print(f"Debug: model_data\n{model_data}")
        #print("MODEL: ", model)
        #print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        legend_entry = model + ": " + latest_forecast + " " + unit_string

        # Create a HoverTool
        hover = HoverTool(tooltips=[
            ('Pentad', f'@{date_col}'),
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
                  show_legend=True,)

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
    #print(f"\nDEBUG: forecast_name_col: {forecast_name_col}")
    #print(f"data.columns: {data.columns}")
    models = data[forecast_name_col].unique()
    if len(models) > len(runoff_forecast_colors):
        # Add some random colors if there are more models than colors
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    # Create the overlay
    for i, model in enumerate(models):
        model_data = data[data[forecast_name_col] == model]

        lower_bound = fl.round_discharge(model_data[min_col].iloc[-1])
        upper_bound = fl.round_discharge(model_data[max_col].iloc[-1])
        #legend_entry = model + ": " + lower_bound + "-" + upper_bound + " " + unit_string
        legend_entry = model + ": Past forecast range"

        range_area = hv.Area(
            model_data,
            kdims=[date_col],
            vdims=[min_col, max_col],
            label=legend_entry) \
                .opts(color=runoff_forecast_color[i],
                      alpha=0.2, muted_alpha=0.05,
                      line_width=0,
                      show_legend=True,)

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
        runoff_forecast_color = runoff_forecast_colors + ['#%06X' % random.randint(0, 0xFFFFFF) for i in range(len(models) - len(runoff_forecast_colors))]
    elif len(models) == 1:
        # Use the middle color in the list
        runoff_forecast_color = [runoff_forecast_colors[3]]
    elif len(models) == 2:
        # Use the second and second from last colors in the list
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[-2]]
    elif len(models) == 3:
        runoff_forecast_color = [runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[-2]]
    elif len(models) == 4:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[2], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 5:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 6:
        runoff_forecast_color = [runoff_forecast_colors[0], runoff_forecast_colors[1], runoff_forecast_colors[2], runoff_forecast_colors[3], runoff_forecast_colors[4], runoff_forecast_colors[-1]]
    elif len(models) == 7:
        runoff_forecast_color = runoff_forecast_colors

    def cap_color_hook(plot, element, color):
        plot.handles["glyph"].upper_head.line_color = color
        plot.handles["glyph"].lower_head.line_color = color

    # Define jitter width (in minutes)
    jitter_width = 0.2  # Jitter by 0.2 pentads

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
        range_legend_entry = model + "range : " + lower_bound + "-" + upper_bound + " " + unit_string
        #print(f"Debug: model_data\n{model_data}")
        #print("MODEL: ", model)
        #print("COLOR: ", runoff_forecast_color[i])
        #print("LEGEND ENTRY: ", range_legend_entry)
        #print("lower_bound: ", lower_bound)
        #print("upper_bound: ", upper_bound)

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

def plot_pentadal_vlines(data, date_col, y_text=1):
    # Based on the date_col, identify the year we are in
    year = data[date_col].dt.year.max()
    # Create a list of the dates for each pentad of the year
    pentads = [dt.datetime(year, month, day) for month, day in [
            (1, 1), (1, 6), (1, 11), (1, 16), (1, 21), (1, 26),  # Jan
            (2, 1), (2, 6), (2, 11), (2, 16), (2, 21), (2, 26),
            (3, 1), (3, 6), (3, 11), (3, 16), (3, 21), (3, 26),  # Mar
            (4, 1), (4, 6), (4, 11), (4, 16), (4, 21), (4, 26),
            (5, 1), (5, 6), (5, 11), (5, 16), (5, 21), (5, 26),  # May
            (6, 1), (6, 6), (6, 11), (6, 16), (6, 21), (6, 26),
            (7, 1), (7, 6), (7, 11), (7, 16), (7, 21), (7, 26),  # Jul
            (8, 1), (8, 6), (8, 11), (8, 16), (8, 21), (8, 26),
            (9, 1), (9, 6), (9, 11), (9, 16), (9, 21), (9, 26),  # Sep
            (10, 1), (10, 6), (10, 11), (10, 16), (10, 21), (10, 26),
            (11, 1), (11, 6), (11, 11), (11, 16), (11, 21), (11, 26),  # Nov
            (12, 1), (12, 6), (12, 11), (12, 16), (12, 21), (12, 26)]]

    # Make sure the pentads dates are in the same format as date_col in data
    pentads = pd.to_datetime(pentads)

    vlines = hv.Overlay(
        [hv.VLine(date).opts(color='gray', line_width=1, line_dash='dotted', line_alpha=0.5,
                             show_legend=False) for date in pentads])

    # Add text halfway between two VLines
    mid_date_text = ['1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6',
                     '1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6',
                     '1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6',
                     '1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6',
                     '1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6',
                     '1', '2', '3', '4', '5', '6', '1', '2', '3', '4', '5', '6']
    for i in range(0, len(pentads)):
        mid_date = pentads[i] + dt.timedelta(days=2.2)
        vlines *= hv.Text(mid_date, y_text, mid_date_text[i]) \
            .opts(text_baseline='bottom', text_align='center', text_font_size='9pt',
                  text_color='gray', text_alpha=0.5, text_font_style='italic',
                  show_legend=False)

    return vlines

def update_pentad_text(date_picker, _):
    """
    Function to calculate and return the dynamic pentad text based on the selected date.
    """
    # Calculate the next day's date (to align with the logic used earlier)
    selected_date = date_picker

    # Calculate pentad, month, and day range
    title_pentad = tl.get_pentad(selected_date)
    title_month = tl.get_month_str_case2_viz(_, selected_date)
    title_day_start = tl.get_pentad_first_day(selected_date.strftime("%Y-%m-%d"))
    title_day_end = tl.get_pentad_last_day(selected_date.strftime("%Y-%m-%d"))
    title_year = selected_date.year

    # Create the formatted text for display
    pentad_text = (f"**{_('Selected Pentad')}**: {title_pentad} {_('pentad')} {_('of')} "
                   f"{title_month} {title_year} "
                   f"({_('days')} {title_day_start}-{title_day_end})")

    return pentad_text

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

    #print(f"\n\nDEBUG: plot_daily_hydrograph_data")
    #print(f"title_date: {title_date}")

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
    print(f"title_pentad: {title_pentad}")
    print(f"title_month: {title_month}")

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station].copy()
    current_year = data['date'].dt.year.max()
    last_year = current_year - 1

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    predictor_string=_("Current year, 3 day sum: ") + f"{linreg_predictor['predictor'].values[0]}" + " " + _("m3/s")
    forecast_string=_("Forecast horizon for ") + title_pentad + _(" pentad of ") + title_month

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

    vlines = plot_pentadal_vlines(data, _('date column name'))

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
        #_('Current year legend entry'),
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
        #       lambda p, e: add_custom_xticklabels_daily_dates(_, linreg_predictor['leap_year'].iloc[0], p, e)],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, data[max_col].max() * 1.1),
        tools=['hover'],
        toolbar='right')

    print("\n\n")

    return daily_hydrograph

def plot_rel_to_norm_runoff(_, hydrograph_day_all, linreg_predictor, station, title_date):

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    title_pentad = tl.get_pentad(title_date + dt.timedelta(days=1))
    title_month = tl.get_month_str_case2_viz(_, title_date)

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station].copy()
    current_year = data['date'].dt.year.max()
    last_year = current_year - 1

    # Calculate relative to norm runoff
    data['current_rel_to_norm'] = (data[str(current_year)] - data['mean']) / data['mean'] * 100
    data['last_rel_to_norm'] = (data[str(last_year)] - data['mean']) / data['mean'] * 100
    data['current_rel_to_last'] = (data[str(current_year)] - data[str(last_year)]) / data[str(last_year)] * 100

    miny = data[['current_rel_to_norm', 'last_rel_to_norm', 'current_rel_to_last']].min().min() * 0.9
    maxy = data[['current_rel_to_norm', 'last_rel_to_norm', 'current_rel_to_last']].max().max() * 1.1
    miny = min(miny, -100)
    maxy = max(maxy, 100)

    # Average the relative values over the predictor period
    avg_current_rel_to_norm = data[(data['date'] >= linreg_predictor['predictor_start_date'].values[0]) &
                                      (data['date'] <= linreg_predictor['predictor_end_date'].values[0])]['current_rel_to_norm'].mean()
    avg_last_rel_to_norm = data[(data['date'] >= linreg_predictor['predictor_start_date'].values[0]) &
                                      (data['date'] <= linreg_predictor['predictor_end_date'].values[0])]['last_rel_to_norm'].mean()
    avg_current_rel_to_last = data[(data['date'] >= linreg_predictor['predictor_start_date'].values[0]) &
                                        (data['date'] <= linreg_predictor['predictor_end_date'].values[0])]['current_rel_to_last'].mean()

    # Round values to 1 decimal and cast to string
    avg_current_rel_to_last_str = str(round(avg_current_rel_to_last, 0))
    avg_current_rel_to_norm_str = str(round(avg_current_rel_to_norm, 0))
    avg_last_rel_to_norm_str = str(round(avg_last_rel_to_norm, 0))

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    current_rel_norm_string=_("Current year") + " " + avg_current_rel_to_norm_str + " " + _("% of mean")
    last_rel_norm_string=_("Last year") + " " + avg_last_rel_to_norm_str + " " + _("% of mean")
    current_rel_last_string=_("Current year") + " " + avg_current_rel_to_last_str + " " + _("% of last year")

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

    vlines = plot_pentadal_vlines(data, 'date', y_text=maxy * 0.95)

    # Horizontal line at 0
    zero_line = hv.HLine(0).opts(color='black', line_width=1,
                                 line_dash='solid', line_alpha=0.5,
                                 show_legend=False)

    last_year = plot_runoff_line(
        data, 'date', 'last_rel_to_norm',
        last_rel_norm_string, runoff_last_year_color)
    last_year.opts(ylim=(miny, maxy))
    current_year = plot_runoff_line(
        data, 'date', 'current_rel_to_norm',
        current_rel_norm_string,
        runoff_mean_color)
    current_year.opts(ylim=(miny, maxy))
    current_rel_norm = plot_runoff_line(
        data, 'date', 'current_rel_to_last',
        current_rel_last_string,
        runoff_current_year_color)
    current_rel_norm.opts(ylim=(miny, maxy))

    # Overlay the plots
    daily_hydrograph = zero_line * vlines * \
        last_year * hvspan_forecast * hvspan_predictor * \
        current_rel_norm * current_year

    daily_hydrograph.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Runoff deviation (%)'),
        height=400,
        responsive=True,
        #show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        #       lambda p, e: add_custom_xticklabels_daily_dates(_, linreg_predictor['leap_year'].iloc[0], p, e)],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(miny, maxy),
        xlim=(min(data['date']), max(data['date'])),
        tools=['hover'],
        toolbar='right')

    return daily_hydrograph


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
    #print(f"Tail of station_data\n{station_data.tail(10)}")

    # Return an empty plot if the data DataFrame is empty
    if station_data.empty:
        return hv.Curve([]).\
            opts(title=_("No data available for this station"),
                 hooks=[remove_bokeh_logo])

    # Get the forecasts for the selected date
    forecasts = station_data[station_data['date'] >= date_picker].copy()
    #print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Get current year rainfall
    station_data['year'] = pd.to_datetime(station_data['date']).dt.year
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    # Sort by date
    current_year = current_year.sort_values('date')
    #print(f"Tail of current_year\n{current_year.tail(10)}")

    # Accumulate rainfall over the predictor period
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)
    predictor_start_date = linreg_predictor['predictor_start_date'].values[0]
    predictor_end_date = linreg_predictor['predictor_end_date'].values[0]
    predictor_rainfall = current_year[(current_year['date'] >= predictor_start_date) &
                                        (current_year['date'] <= predictor_end_date)].copy()

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

    # Plot the daily rainfall data using holoviews
    title_text = f"Daily rainfall sums for basin of {station} on {date_picker.strftime('%Y-%m-%d')}"
    current_year_text = f"Current year, 3 day sum: {predictor_rainfall['P'].sum().round()} mm"

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

    vlines = plot_pentadal_vlines(norm_rainfall, _('date'), y_text=station_data['P'].max() * 1.05)

    # A bar plot for the norm rainfall
    hv_norm_rainfall = hv.Curve(
        norm_rainfall,
        kdims='date',
        vdims='P',
        label='Norm')
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
    # A bar plot for the forecasted rainfall
    # if forecasts is not empty
    if not forecasts.empty:
        hv_forecast = hv.Curve(
            forecasts,
            kdims='date',
            vdims='P',
            label='Forecast')
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
        ylabel=_('Rainfall (mm/d)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, station_data['P'].max() * 1.1),
        xlim=(min(norm_rainfall['date']), max(norm_rainfall['date'])),
        tools=['hover'],
        toolbar='right')

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
    #print(f"Tail of station_data\n{station_data.tail(10)}")

    # Return an empty plot if the data DataFrame is empty
    if station_data.empty:
        return hv.Curve([]).\
            opts(title=_("No data available for this station"),
                 hooks=[remove_bokeh_logo])

    # Get the forecasts for the selected date
    forecasts = station_data[station_data['date'] >= date_picker].copy()
    #print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Get current year rainfall
    station_data['year'] = pd.to_datetime(station_data['date']).dt.year
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    # Sort by date
    current_year = current_year.sort_values('date')
    #print(f"Tail of current_year\n{current_year.tail(10)}")

    # Accumulate rainfall over the predictor period
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)
    predictor_start_date = linreg_predictor['predictor_start_date'].values[0]
    predictor_end_date = linreg_predictor['predictor_end_date'].values[0]
    predictor_rainfall = current_year[(current_year['date'] >= predictor_start_date) &
                                        (current_year['date'] <= predictor_end_date)].copy()

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

    # Plot the daily rainfall data using holoviews
    title_text = f"Daily average temperature for basin of {station} on {date_picker.strftime('%Y-%m-%d')}"
    current_year_text = f"Current year, 3 day mean: {predictor_rainfall['T'].mean().round()} °C"

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

    vlines = plot_pentadal_vlines(norm_rainfall, _('date'), y_text=station_data['T'].max() * 1.05)

    # A bar plot for the norm rainfall
    hv_norm_rainfall = hv.Curve(
        norm_rainfall,
        kdims='date',
        vdims='T',
        label='Norm')
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
    # A bar plot for the forecasted rainfall
    # if forecasts is not empty
    if not forecasts.empty:
        hv_forecast = hv.Curve(
            forecasts,
            kdims='date',
            vdims='T',
            label='Forecast')
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
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(station_data['T'].min() * 0.9, station_data['T'].max() * 1.1),
        xlim=(min(norm_rainfall['date']), max(norm_rainfall['date'])),
        tools=['hover'],
        toolbar='right')

    return figure

def plot_daily_rel_to_norm_rainfall(_, daily_rainfall, station, date_picker,
                             linreg_predictor):

    # Extract code from station
    station_code = station.split(' - ')[0]

    # Convert date column to datetime
    daily_rainfall['date'] = pd.to_datetime(daily_rainfall['date'])

    # Convert date_picker to datetime[ns]
    date_picker = pd.to_datetime(date_picker)

    # Get linreg_predictor dates
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, date_picker)

    # Filter data for the selected station
    station_data = daily_rainfall[daily_rainfall['code'] == station_code].copy()
    #print(f"Tail of station_data\n{station_data.tail(10)}")

    # Get the forecasts for the selected date
    forecasts = station_data[station_data['date'] >= date_picker].copy()
    #print(f"Tail of forecasts\n{forecasts.tail(10)}")

    # Get current year rainfall
    station_data['year'] = pd.to_datetime(station_data['date']).dt.year
    current_year = station_data[station_data['year'] == date_picker.year].copy()
    # Sort by date
    current_year = current_year.sort_values('date')
    #print(f"Tail of current_year\n{current_year.tail(10)}")

    # Calculate norm rainfall, excluding the current year
    norm_rainfall = station_data[station_data['year'] != date_picker.year].copy()
    norm_rainfall['doy'] = pd.to_datetime(norm_rainfall['date']).dt.dayofyear
    norm_rainfall = norm_rainfall.groupby(['code', 'doy']).mean().reset_index()
    # Replace year of date with the current year
    norm_rainfall['date'] = norm_rainfall['date'].apply(lambda x: x.replace(year=date_picker.year))
    # Drop doy and year columns and sort by date
    norm_rainfall = norm_rainfall.drop(columns=['doy', 'year']).sort_values('date')
    # Convert date column to datetime
    norm_rainfall['date'] = pd.to_datetime(norm_rainfall['date']).dt.floor('D')

    # Make sure current_year 'date' has same format at norm_rainfall 'date'
    current_year['date'] = pd.to_datetime(current_year['date']).dt.floor('D')

    # Merge current_year[P] with norm_rainfall[P]
    norm_rainfall = norm_rainfall.merge(
        current_year.rename(columns={'P': 'current_year'}),
        on=['code', 'date'], how='left')
    # Calculate relative to norm rainfall
    norm_rainfall['current_rel_to_norm'] = (norm_rainfall['current_year'] - norm_rainfall['P']) / norm_rainfall['P'] * 100
    # Print tail of current_rel_to_norm
    #print(f"Head of current_rel_to_norm\n{norm_rainfall.head(10)}")

    miny = norm_rainfall['current_rel_to_norm'].min() * 0.9
    maxy = norm_rainfall['current_rel_to_norm'].max() * 1.1
    miny = miny if miny > -100 else -100
    maxy = maxy if maxy < 100 else 100

    # Average the relative values over the predictor period
    avg_current_rel_to_norm = norm_rainfall[(norm_rainfall['date'] >= linreg_predictor['predictor_start_date'].values[0]) &
                                        (norm_rainfall['date'] <= linreg_predictor['predictor_end_date'].values[0])]['current_rel_to_norm'].mean()
    avg_current_rel_to_norm_str = str(round(avg_current_rel_to_norm, 0))

    # Plot the daily rainfall data using holoviews
    title_text = f"Daily rainfall data for {station} on {date_picker.strftime('%Y-%m-%d')}"
    current_year_rel_norm_str = _("Current year") + " " + avg_current_rel_to_norm_str + " " + _("% of norm")

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

    vlines = plot_pentadal_vlines(norm_rainfall, _('date'), y_text=maxy * 0.9)
    # horizontal line
    zero_line = hv.HLine(0).opts(color='black', line_width=1,
                                    line_dash='solid', line_alpha=0.5,
                                    show_legend=False)

    # A bar plot for the current_year rainfall
    hv_current_year = hv.Curve(
        norm_rainfall,
        kdims='date',
        vdims='current_rel_to_norm',
        label=current_year_rel_norm_str)
    hv_current_year.opts(
        interpolation='steps-mid',
        line_width=1,
        color=runoff_current_year_color,
        show_legend=True)
    figure = zero_line * hvspan_predictor * hvspan_forecast * vlines * hv_current_year

    figure.opts(
        title=title_text,
        xlabel="",
        ylabel=_('Rainfall deviation (%)'),
        height=400,
        responsive=True,
        show_grid=True,
        show_legend=True,
        legend_position='top_right',
        hooks=[remove_bokeh_logo],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(miny, maxy),
        xlim=(min(norm_rainfall['date']), max(norm_rainfall['date'])),
        tools=['hover'],
        toolbar='right')

    return figure


# endregion

# region forecast_tab
def plot_pentad_forecast_hydrograph_data(_, hydrograph_pentad_all, forecasts_all,
                                         station, title_date, model_selection,
                                         range_type, range_slider, range_visibility):

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    #print(f"\n\nDEBUG: plot_pentad_forecast_hydrograph_data")
    #print(f"title_date: {title_date}")
    forecast_date = title_date + dt.timedelta(days=1)
    title_pentad = tl.get_pentad(forecast_date)
    title_month = tl.get_month_str_case2_viz(_, forecast_date)
    title_day_start = tl.get_pentad_first_day(forecast_date.strftime("%Y-%m-%d"))
    title_day_end = tl.get_pentad_last_day(forecast_date.strftime("%Y-%m-%d"))
    print(f"forecast_date: {forecast_date}")
    print(f"title_pentad: {title_pentad}")
    print(f"title_month: {title_month}")

    # filter hydrograph_day_all & linreg_predictor by station
    #linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    # Filter forecasts for the current year and station
    forecasts = forecasts_all[(forecasts_all['station_labels'] == station) &
                            (forecasts_all['year'] == title_date.year) &
                            (forecasts_all['model_short'].isin(model_selection))].copy()

    # Cast date to datetime
    forecasts['date'] = pd.to_datetime(forecasts['date'])

    # Filter forecasts dataframe for dates smaller than the title date
    forecasts = forecasts[forecasts['date'] <= pd.Timestamp(title_date)+dt.timedelta(days=1)]
    #print(f"Tail of forecasts\n{forecasts.tail(10)}")

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
        #forecasts.loc[:, 'fc_lower'] = forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta']
        #forecasts.loc[:, 'fc_upper'] = forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta']
    elif range_type == _("Manual range, select value below"):
        forecasts['fc_lower'] = (1 - range_slider/100.0) * forecasts['forecasted_discharge']
        forecasts['fc_upper'] = (1 + range_slider/100.0) * forecasts['forecasted_discharge']
    elif range_type == _("max[delta, %]"):
        forecasts.loc[:, 'fc_lower'] = np.minimum(forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta'],
                                                  (1 - range_slider/100.0) * forecasts.loc[:, 'forecasted_discharge'])
        forecasts.loc[:, 'fc_upper'] = np.maximum(forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta'],
                                                    (1 + range_slider/100.0) * forecasts.loc[:, 'forecasted_discharge'])

    # Filter hydrograph data for the current station
    data = hydrograph_pentad_all[hydrograph_pentad_all['station_labels'] == station].copy()

    current_year = data['date'].dt.year.max()
    last_year = current_year - 1

    if str(current_year) not in data.columns:
        print(f"DEBUG: '{str(current_year)}' column missing in 'data'. Creating it with NaNs.")
        data[str(current_year)] = np.nan  # Initialize the column

    # Convert date column of data dataframe to datetime
    data['date'] = pd.to_datetime(data['date'])

    # Set values after the title date to NaN
    #print("title_date: ", title_date)
    #print("str(current_year): ", str(current_year))
    #print(pd.Timestamp(title_date))
    #print("data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)]: ", data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)])
    data.loc[data['date'] >= pd.Timestamp(title_date), str(current_year)] = np.nan
    #print(f"Tail of data\n{data.tail(25).head(10)}")

    # Define strings
    title_text = (f"{_('Hydropost')} {station}: {_('Forecast')} {_('for')} "
                  f"{title_pentad} {_('pentad')} {_('of')} {title_month} "
                  f"({_('days')} {title_day_start}-{title_day_end})")

    # Rename columns to be used in the plot to allow internationalization
    data = data.rename(columns={
        'pentad_in_year': _('pentad_of_year column name'),
        'min': _('min column name'),
        'max': _('max column name'),
        'q05': _('5% column name'),
        'q95': _('95% column name'),
        'q25': _('25% column name'),
        'q75': _('75% column name'),
        'mean': _('mean column name'),
        str(last_year): _('Last year column name'),
        str(current_year): _('Current year column name')
        })
    forecasts = forecasts.rename(columns={
        'pentad': _('pentad_of_year column name'),
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

    #print("forecasts.columns\n", forecasts.columns)
    #print("forecasts.head(10)\n", forecasts.head(10))
    #print("forecasts.tail(10)\n", forecasts.tail(10))
    #linreg_predictor = linreg_predictor.rename({
    #    'day_of_year': _('day_of_year column name'),
    #    'predictor': _('Predictor column name')
    #    })

    # Create a holoviews bokeh plots of the daily hydrograph
    full_range_area = plot_runoff_range_area(
        data, _('pentad_of_year column name'), _('min column name'), _('max column name'),
        _("Full range legend entry"), runoff_full_range_color)
    lower_bound = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('min column name'),
        runoff_full_range_color, hover_tool=False)
    upper_bound = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('max column name'),
        runoff_full_range_color, hover_tool=False)

    area_05_95 = plot_runoff_range_area(
        data, _('pentad_of_year column name'), _('5% column name'), _('95% column name'),
        _("90-percentile range legend entry"), runoff_90percentile_range_color)
    line_05 = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('5% column name'),
        runoff_90percentile_range_color, hover_tool=False)
    line_95 = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('95% column name'),
        runoff_90percentile_range_color, hover_tool=False)

    area_25_75 = plot_runoff_range_area(
        data, _('pentad_of_year column name'), _('25% column name'), _('75% column name'),
        _("50-percentile range legend entry"), runoff_50percentile_range_color)
    line_25 = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('25% column name'),
        runoff_50percentile_range_color, hover_tool=False)
    line_75 = plot_runoff_range_bound(
        data, _('pentad_of_year column name'), _('75% column name'),
        runoff_50percentile_range_color, hover_tool=False)

    mean = plot_runoff_line(
        data, _('pentad_of_year column name'), _('mean column name'),
        _('Mean legend entry'), runoff_mean_color)
    """last_year = plot_runoff_line(
        data, _('pentad_of_year column name'), _('Last year column name'),
        _('Last year legend entry'), runoff_last_year_color)"""
    current_year = plot_runoff_line(
        data, _('pentad_of_year column name'), _('Current year column name'),
        _('Current year legend entry'), runoff_current_year_color)

    forecast_area = plot_runoff_forecast_range_area(
        data=forecasts_past,
        date_col=_('pentad_of_year column name'),
        forecast_name_col=_('forecast model short column name'),
        min_col=_('forecast lower bound column name'),
        max_col= _('forecast upper bound column name'),
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string=_("m³/s"))
    forecast_lower_bound = plot_runoff_range_bound(
        forecasts_past, _('pentad_of_year column name'), _('forecast lower bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_upper_bound = plot_runoff_range_bound(
        forecasts_past, _('pentad_of_year column name'), _('forecast upper bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_line = plot_runoff_forecasts(
        forecasts_past, _('pentad_of_year column name'), _('forecasted_discharge column name'),
        _('forecast model short column name'), runoff_forecast_color_list, _('m³/s'))

    current_forecast_range_point = plot_current_runoff_forecast_range(
        forecasts_current, _('pentad_of_year column name'), _('forecast model short column name'),
        _('forecasted_discharge column name'), _('forecast lower bound column name'), _('forecast upper bound column name'),
        runoff_forecast_color_list, _('m³/s'))


    # Overlay the plots
    if range_visibility == 'Yes':
        pentad_hydrograph = full_range_area * lower_bound * upper_bound * \
        area_05_95 * line_05 * line_95 * \
        area_25_75 * line_25 * line_75 * \
        forecast_area * forecast_lower_bound * forecast_upper_bound * \
        mean * current_year * forecast_line * \
        current_forecast_range_point
    else:
        pentad_hydrograph = mean * current_year * forecast_line * \
        current_forecast_range_point

    pentad_hydrograph.opts(
        title=title_text,
        xlabel=_('Pentad of the year'),
        ylabel=_('Discharge (m³/s)'),
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo,
               lambda p, e: add_custom_xticklabels_pentad(_, p, e),
        ],
        #tools=['hover'],
        toolbar='right')

    print("\n\n")

    return pentad_hydrograph

def create_forecast_summary_table(_, forecasts_all, station, date_picker,
                                  model_selection, range_type, range_slider):

    # Filter forecasts_all for selected station, date and models
    forecast_table = forecasts_all[(forecasts_all['station_labels'] == station)].copy()

    # Cast date column to datetime
    forecast_table['date'] = pd.to_datetime(forecast_table['date'])

    # Filter further for the selected date and models
    forecast_table = forecast_table[(forecast_table['date'] <= (pd.Timestamp(date_picker) + pd.Timedelta(days=1))) &
                            (forecast_table['model_short'].isin(model_selection))].copy().reset_index(drop=True)

    # Select the row with the maximum date
    forecast_table = forecast_table.loc[forecast_table['date']==max(forecast_table['date'])]
    print("forecast_table\n", forecast_table)

    # Drop a couple of columns
    forecast_table.drop(
        columns=['code', 'date', 'Date', 'year', 'pentad_in_year',
                 'pentad', 'pentad_in_month', 'model_long', 'station_labels'],
        inplace=True)

    # Calculate the forecast range depending on the values of range_type and range_slider
    forecast_table = processing.calculate_forecast_range(_, forecast_table, range_type, range_slider)

    # Get columns in the desired sequence
    forecast_table = forecast_table[['model_short', 'forecasted_discharge', 'fc_lower', 'fc_upper', 'delta', 'sdivsigma', 'mae', 'accuracy']]

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
                                    model_selection, range_type, range_slider).reset_index(drop=True)

    # Get the row with the maximum accuracy. If the table has 2 rows, the
    # index is either 0 or 1. If the table has 1 row, the index is 0.
    # If two rows have the same accuracy, the first row is selected.
    max_accuracy_index = final_forecast_table[_('Accuracy')].idxmax()
    print("max_accuracy_index\n", max_accuracy_index)

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


def draw_forecast_raw_data(_, forecasts_linreg, station_widget, date_picker):

    print("forecasts_linreg.head():\n", forecasts_linreg.head())

    # From date_picker, get the pentad and month of the latest forecast
    forecast_date = date_picker + dt.timedelta(days=1)
    forecast_pentad = int(tl.get_pentad_in_year(forecast_date))

    # Filter forecasts_all for selected station, date and models
    forecast_table = forecasts_linreg[(forecasts_linreg['station_labels'] == station_widget)].copy()

    # Filter further for the selected date (pentad)
    forecast_table = forecast_table[
        (forecast_table['pentad_in_year'] == forecast_pentad)].copy().reset_index(drop=True)

    forecast_data_table = pn.widgets.Tabulator(
        value=forecast_table[['predictor', 'discharge_avg']],
        #formatters={'Pentad': "{:,}"},
        #editors={_('δ'): None},  # Disable editing of the δ column
        theme='bootstrap',
        show_index=False,
        show_grid=True,
        show_legend=True,
        selectable='checkbox',
        height=450,)

    return forecast_data_table


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




# Function to convert a relative path to an absolute path
def get_absolute_path(relative_path):
    #print("In get_absolute_path: ")
    #print(" - Relative path: ", relative_path)

    # Test if there environment variable "ieasyforecast_data_root_dir" is set
    data_root_dir = os.getenv('ieasyhydroforecast_data_root_dir')
    #print(f"\n\n\n\n\nget_absolute_path: data_root_dir: {data_root_dir}")
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


#TODO: use this function for local development instead of initial get_absolute_path function
#def get_absolute_path(relative_path):
#    # function for local development
#    project_root = '/home/vjeko/Desktop/Projects/sapphire_forecast'
#    # Remove leading ../../../ from the relative path
#    relative_path = re.sub(r'^\.\./\.\./\.\./', '', relative_path)
#    return os.path.join(project_root, relative_path)

def get_bind_path(relative_path):
    # Strip the relative path from ../../.. to get the path to bind to the container
    relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

    return relative_path

def get_local_path(relative_path):
    # Strip 2 ../ of the relative path
    relative_path = re.sub(f'\.\./\.\./', '', relative_path)

    return relative_path


# Test if the path to the configuration folder is set
#if not os.getenv('ieasyforecast_configuration_path'):
#    raise ValueError("The path to the configuration folder is not set.")

# Define the directory to save the data
#SAVE_DIRECTORY = os.path.join(
#    os.getenv('ieasyforecast_configuration_path'),
#    os.getenv('ieasyforecast_linreg_point_selection', 'linreg_point_selection')
#)
#os.makedirs(SAVE_DIRECTORY, exist_ok=True)

class AppState(param.Parameterized):
    pipeline_running = param.Boolean(default=False)

# Instantiate the shared state
app_state = AppState()

def select_and_plot_data(_, linreg_predictor, station_widget, pentad_selector,
                         SAVE_DIRECTORY):
    # Define a variable to hold the visible data across functions
    global visible_data

    #print(f"\n\nDEBUG: select_and_plot_data")
    # Print tail of linreg_predictor for code == '16059'
    #print(f"linreg_predictor[linreg_predictor['code'] == '16059'].head(10):\n",
    #      linreg_predictor[linreg_predictor['code'] == '16059'].head(10))
    #print(f"linreg_predictor[linreg_predictor['code'] == '16059'].tail(10):\n",
    #      linreg_predictor[linreg_predictor['code'] == '16059'].tail(10))

    if isinstance(station_widget, str):
        station_code = station_widget.split(' - ')[0]
    else:
        station_code = station_widget.value.split(' - ')[0]

    # Check if pentad_selector is a widget or an integer
    if isinstance(pentad_selector, int):
        selected_pentad = pentad_selector  # If it's an integer, use it directly
    else:
        selected_pentad = pentad_selector.value  # If it's a widget, get the value

    # Find out the month and pentad for display purposes
    selected_pentad_text = [k for k, v in pentad_options.items() if v == selected_pentad][0]
    title_pentad = selected_pentad_text.split(' ')[0]
    title_month = selected_pentad_text.split('of ')[-1]

    # Generate dynamic filename based on the selected pentad and station
    station_name = station_widget
    station_data = linreg_predictor.loc[linreg_predictor['code'] == station_code]
    if station_data.empty:
        print(f"Error: No data found for station code '{station_code}'")
        return None

    pentad_in_month = selected_pentad % 6 if selected_pentad % 6 != 0 else 6

    save_file_name = f"{station_code}_{pentad_in_month}_pentad_of_{title_month}.csv"
    save_file_path = os.path.join(SAVE_DIRECTORY, save_file_name)

    # Add the 'date' column if it doesn't exist
    if 'date' not in linreg_predictor.columns:
        # Map the 'pentad_in_year' to the corresponding date using get_date_for_pentad
        linreg_predictor['date'] = linreg_predictor['pentad_in_year'].apply(lambda pentad: tl.get_date_for_pentad(pentad))

    # Extract the year from the date column and create a 'year' column
    linreg_predictor['year'] = pd.to_datetime(linreg_predictor['date']).dt.year.astype(int)

# **Always read from linreg_predictor**
    # Filter data for the selected station and pentad across all years
    forecast_table = linreg_predictor[
        (linreg_predictor['station_labels'] == station_widget) &
        (linreg_predictor['pentad_in_year'] == selected_pentad)
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

    visible_data = forecast_table[forecast_table['visible'] == True] # Initialize the visible data
    print(f"visible_data.head(10):\n", visible_data.head(10))
    visible_data = visible_data.dropna(subset=['predictor', 'discharge_avg'])

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
    )

    # Create the title text
    title_text = (f"{_('Hydropost')} {station_code}: {_('Regression')} {_('for')} "
                  f"{title_pentad} {_('pentad')} {_('of')} {title_month} ")

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
        forecast_table.loc[forecast_data_table.value['index'], 'visible'] = forecast_data_table.value['visible'].values

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
                    ('Year', '@year'),
                    ('Predictor', '@predictor'),
                    ('Discharge', '@discharge_avg'),
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
                .opts(color='blue', size=5, tools=[hover, 'tap'], xlim=x_range, ylim=y_range)

            if len(visible_data) > 1:
                # Compute dynamic regression parameters
                new_slope, new_intercept, new_r_value, new_p_value, new_std_err = stats.linregress(
                    visible_data['predictor'], visible_data['discharge_avg'])
                new_rsquared = new_r_value ** 2

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
                #hook = make_add_label_hook(text_content)
                hook = make_add_label_hook_from_div(text_div)
                # Overlay the scatter plot and the regression line(s)
                plot = scatter * line_initial * line_new
                plot.opts(
                    title=title_text,
                    show_grid=True,
                    show_legend=True,
                    width=1000,
                    height=450,
                    hooks=[remove_bokeh_logo, hook]
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
    popup = pn.pane.Alert("Changes Saved Successfully", alert_type="success", visible=False)

    # Adjust the sizing modes of your components
    forecast_data_table.sizing_mode = 'stretch_both'
    plot_pane.sizing_mode = 'stretch_both'

    progress_bar = pn.indicators.Progress(name="Progress", value=0, width=300, visible=False)
    progress_message = pn.pane.Alert("Processing...", alert_type="info", visible=False)

    def run_docker_container(client, full_image_name, volumes, environment, container_name, progress_bar):
        """
        Reusable function to run a Docker container and track its progress.
        If a container with the same name exists, it will be removed before running a new one.
        """
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

        # Now run the new container
        container = client.containers.run(
            full_image_name,
            detach=True,
            environment=environment,
            volumes=volumes,
            name=container_name,
            network='host'
        )
        print(f"Container '{container_name}' (ID: {container.id}) is running.")

        # Monitor the container's progress
        progress_bar.value = 0
        while container.status != 'exited':
            container.reload()  # Refresh the container's status
            progress_bar.value += 10  # Increment progress
            if progress_bar.value > 90:
                progress_bar.value = 90  # Limit progress to 90% until the process finishes
            time.sleep(1)
        container.wait()  # Ensure the container has finished
        progress_bar.value = 100  # Set progress to 100% after the container is done
        print(f"Container '{container_name}' has stopped.")


    # Create a save button
    save_button = pn.widgets.Button(name="Save Changes", button_type="success")

    # Function to save table data to CSV and run Docker containers
    def save_to_csv(event):
        # Disable the save button and show the progress bar and message
        save_button.disabled = True
        progress_bar.visible = True
        progress_message.object = "Processing..."
        progress_message.visible = True
        progress_bar.value = 0

        # Convert the table value back to a DataFrame
        updated_forecast_table = pd.DataFrame(forecast_data_table.value)

        # Explicitly reset the index before saving, so it becomes a column
        updated_forecast_table = updated_forecast_table.reset_index(drop=True)

        updated_forecast_table['pentad'] = selected_pentad

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

            bind_volume_path_config = get_bind_path(env.get('ieasyforecast_configuration_path'))
            bind_volume_path_internal_data = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
            bind_volume_path_discharge = get_bind_path(env.get('ieasyforecast_daily_discharge_path'))

            # Initialize Docker client
            client = docker.from_env()

            # Define environment variables
            environment = [
                'IN_DOCKER_CONTAINER=True',
                f'ieasyhydroforecast_env_file_path={bind_volume_path_config}/.env_develop_kghm'
            ]

            # Define volumes
            volumes = {
                absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
                absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
                absolute_volume_path_discharge: {'bind': bind_volume_path_discharge, 'mode': 'rw'}
            }
            print("volumes: ", volumes)

            # Run the reset rundate module to update the rundate for the linear regression module
            run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate", progress_bar)

            # Run the linear_regression container with a hardcoded full image name
            run_docker_container(client, "mabesa/sapphire-linreg:latest", volumes, environment, "linreg", progress_bar)

            # After linear_regression finishes, run the postprocessing container with a hardcoded full image name
            run_docker_container(client, "mabesa/sapphire-postprocessing:latest", volumes, environment, "postprocessing", progress_bar)

            # When the container is finished, set progress to 100 and update message
            progress_bar.value = 100
            progress_message.object = "Processing finished"

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

        pn.state.onload(lambda: pn.state.add_periodic_callback(lambda: setattr(progress_bar, 'visible', False), 2000, count=1))
        pn.state.onload(lambda: pn.state.add_periodic_callback(lambda: setattr(progress_message, 'visible', False), 4000, count=1))

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
    reload_button = pn.widgets.Button(name="Trigger forecasts", button_type="danger")

    # Loading spinner and messages
    loading_spinner = pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='success', visible=False)
    progress_message = pn.pane.Alert("Processing...", alert_type="info", visible=False)
    warning_message = pn.pane.Alert("Please do not reload this page until processing is done!", alert_type='danger', visible=False)

    # Function to check if any containers are running
    def check_containers_running():
        try:
            client = docker.from_env()
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
        except Exception as e:
            print(f"Error checking containers: {e}")
            return False

    # Function to update button status when page is loaded or when triggered
    @pn.io.with_lock  # Ensure thread safety
    def update_button_status():
        if app_state.pipeline_running or check_containers_running():
            reload_button.disabled = True
            loading_spinner.visible = True
            progress_message.object = "Processing..."
            progress_message.visible = True
            warning_message.object = "Please do not reload this page until processing is done!"
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
            warning_message.object = "Containers are still running. Please wait."
            warning_message.visible = True
            return

        # Update shared state to indicate the pipeline is running
        app_state.pipeline_running = True

        # Disable the reload button and show loading spinner
        reload_button.disabled = True
        loading_spinner.visible = True
        progress_message.object = "Processing..."
        progress_message.visible = True
        warning_message.object = "Please do not reload this page until processing is done!"
        warning_message.visible = True

        def run_docker_pipeline():
            try:
                # Initialize Docker client
                client = docker.from_env()

                # Define environment variables
                environment = [
                    'SAPPHIRE_OPDEV_ENV=True',
                    'IN_DOCKER_CONTAINER=True',
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
                    "/var/run/docker.sock": {
                        'bind': "/var/run/docker.sock",
                        'mode': 'rw'
                    }
                }

                # Run the preprunoff container
                run_docker_container(client, "mabesa/sapphire-preprunoff:latest", volumes, environment, "preprunoff")

                # Run the reset_rundate container
                run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate")

                # Run the linear_regression container
                run_docker_container(client, "mabesa/sapphire-linreg:latest", volumes, environment, "linreg")

                # Run the prepgateway container
                run_docker_container(client, "mabesa/sapphire-prepgateway:latest", volumes, environment, "prepgateway")

                # Run all ML model containers
                for model in ["TFT", "TIDE", "TSMIXER", "ARIMA"]:
                    for mode in ["PENTAD", "DECAD"]:
                        container_name = f"ml_{model}_{mode}"
                        run_docker_container(client, f"mabesa/sapphire-ml:latest", volumes, environment + [f"SAPPHIRE_MODEL_TO_USE={model}", f"SAPPHIRE_PREDICTION_MODE={mode}"], container_name)

                # Run the conceptmod container
                run_docker_container(client, "mabesa/sapphire-conceptmod:latest", volumes, environment, "conceptmod")

                # Run the postprocessing container
                run_docker_container(client, "mabesa/sapphire-postprocessing:latest", volumes, environment, "postprocessing")

                # Update message after all containers have run
                progress_message.object = "Processing finished"
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
            pn.pane.Markdown("Click the button below to trigger the forecast pipeline.\nThis will re-run all forecasts for the latest data.\nThis process may take a few minutes to complete.\nNote: Current forecasts will be overwritten."),
            reload_button,
            loading_spinner,
            progress_message,
            warning_message,
        ),
        title='Manual re-run of latest forecasts',
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
    try:
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
    except Exception as e:
        print(f"Error running container '{container_name}': {e}")

def make_add_label_hook(text_content):
    def add_label(plot, element):
        from bokeh.models import Label
        label = Label(
            x=50, y=300,  # Adjust these values to position the text
            x_units='screen', y_units='screen',
            text=text_content,
            text_font_size="10pt",
        )
        plot.state.add_layout(label)
    return add_label

def test_draw_forecast_raw_data(_, selected_data):

    # Only show data for model_short == 'LR'
    selected_data = selected_data[selected_data['model_short'] == _('Forecast models LR')]

    # Only show rows with data in predictor and discharge_avg columns
    #selected_data = selected_data.dropna(subset=['predictor', 'discharge_avg'])

    # Create a Tabulator widget for the selected data
    forecast_data_table = pn.widgets.Tabulator(
        value=selected_data[['year', 'predictor', 'discharge_avg',
                              'forecasted_discharge']],
        formatters={'Pentad': "{:,}",
                    'year': "{:,}"},
        #editors={_('δ'): None},  # Disable editing of the δ column
        theme='bootstrap',
        show_index=False,
        selectable='checkbox')

    return forecast_data_table

def test_plot_linear_regression(_, selected_data):

    # Only show data for model_short == 'LR'
    selected_data = selected_data[selected_data['model_short'] == _('Forecast models LR')]

    scatter = hv.Scatter(selected_data, kdims="predictor",
                         vdims="discharge_avg", label="Measured",
                         #selected=[table_selection]
                         ) \
        .opts(color="#307096", size=5, tools=['hover', 'lasso_select'],
              legend_position='bottom_right',)
    return scatter

def test_perform_regression(selected_data):
    # Perform linear regression on the selected data
    model = LinearRegression()
    model.fit(selected_data[['predictor']], selected_data['target'])

    # Calculate the forecast and difference
    selected_data['forecasted_discharge'] = model.predict(selected_data[['predictor']])
    selected_data['difference'] = selected_data['target'] - selected_data['forecast']

    return selected_data


'''def plot_linear_regression(_, forecast_data_table):

    # Create a scatter plot of the selected data
    scatter = hv.Scatter(forecast_data_table, kdims="Predictor",
                         vdims="Q [m3/s]", label="Measured",
                         selected=[table_selection]) \
        .opts(color="#307096", size=5, tools=['hover', 'tap'], legend_position='bottom_right',)

    # Add a linear regression line to the scatter plot
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        analysis_pentad["Predictor"], analysis_pentad["Q [m3/s]"])
    x = np.linspace(analysis_pentad["Predictor"].min(),
                    analysis_pentad["Predictor"].max(), 100)
    y = slope * x + intercept
    line = hv.Curve((x, y), label="Linear regression") \
        .opts(color="#72D1FA", line_width=2)
    # Print the linear regression equation
    equation = f"y = {slope:.3f}x + {intercept:.3f}"
    r2 = f"R^2 = {r_value**2:.3f}"
    #pval = f"p = {p_value:.3f}"
    #stderr = f"stderr = {std_err:.3f}"
    text = hv.Text(x = analysis_pentad["Predictor"].min(),
                   y = analysis_pentad["Q [m3/s]"].max(),
                   text = f"{equation}\n{r2}") \
        .opts(color="black", text_font_size="10pt", text_align="left",)
              #xlim=(0, analysis_pentad["Predictor"].max()*1.1),
              #ylim=(0, analysis_pentad["Q [m3/s]"].max()*1.1))
    # Add the text to the plot
    scatter = scatter * line * text

    # Add the line to the scatter plot
    scatter = scatter * line
    scatter.opts(hooks=[remove_bokeh_logo]),
        # Create a scatter plot of the predictor vs. the discharge
        scatter = hv.Scatter(
            forecast_data_table,
            kdims=['predictor'],
            vdims=['discharge_avg']) \
                .opts(color='blue', size=5, tools=['hover'])

        # Create a linear regression line
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            forecast_data_table['predictor'], forecast_data_table['discharge_avg'])
        x = np.linspace(forecast_data_table['predictor'].min(), forecast_data_table['predictor'].max(), 100)
        y = slope * x + intercept
        line = hv.Curve((x, y)) \
            .opts(color='red', line_width=2, tools=['hover'])

        # Overlay the scatter plot and the linear regression line
        overlay = scatter * line

        overlay.opts(
            title=_('Linear regression'),
            xlabel=_('Predictor'),
            ylabel=_('Discharge (m³/s)'),
            show_grid=True,
            show_legend=False,
            tools=['hover'],
            toolbar='above',
            hooks=[remove_bokeh_logo])

        return overlay'''
# endregion


# region skill metrics

def plot_forecast_skill(
        _, hydrograph_pentad_all, forecasts_all, station_widget, date_picker,
        model_checkbox, range_selection_widget, manual_range_widget,
        show_range_button):

    # Convert the date from the date picker to a pandas Timestamp
    selected_date = pd.Timestamp(date_picker)
    title_date = pd.to_datetime(date_picker)
    title_date_str = selected_date.strftime('%Y-%m-%d')

    # Filter the forecasts for the selected station and models
    forecast_pentad = forecasts_all[forecasts_all['model_short'].isin(model_checkbox)].copy()
    forecast_pentad = forecast_pentad[forecast_pentad['station_labels'] == station_widget]
    # We only need the values of the past 365 days (or the past year)
    forecast_pentad = forecast_pentad[forecast_pentad['date'] >= selected_date-pd.Timedelta(days=365)]
    # Multiply accruacy by 100 to get percentage
    forecast_pentad['accuracy'] = forecast_pentad['accuracy'] * 100
    # Drop any duplicates in date and model_short
    current_forecast_pentad = forecast_pentad[forecast_pentad['date'] == selected_date]
    # Sort by model_short and pentad_in_year
    forecast_pentad = forecast_pentad.sort_values(by=['model_short', 'pentad_in_year'])

    # Plot the effectiveness of the forecast method
    # Plot a green area between y = 0 and y = 0.6
    hv_06a = hv.Area(pd.DataFrame({"x":[1, 72], "y":[0.6, 0.6]}),
                        kdims=["x"], vdims=["y"], label=_("Effectiveness")+" <= 0.6") \
            .opts(alpha=0.05, color="green", line_width=0)
    hv_08a = hv.Area(pd.DataFrame({"x":[1, 72], "y":[0.8, 0.8]}),
                        kdims=["x"], vdims=["y"], label=_("Effectiveness")+" <= 0.8") \
            .opts(alpha=0.05, color="orange", line_width=0)
    hv_current_forecast_skill_effectiveness = plot_current_runoff_forecasts(
        data=current_forecast_pentad,
        date_col='pentad',
        forecast_data_col='sdivsigma',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[-]')
    hv_forecast_skill_effectiveness = plot_runoff_forecasts_steps(
        forecast_pentad, date_col='pentad',
        forecast_data_col='sdivsigma',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[-]')

    # Plot column 'sdivsigma' over the pentad of the year
    title_effectiveness = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        ", " + _("data from") + " 2010 - "+str(title_date.year)
    effectiveness_plot = hv_08a * hv_06a * hv_forecast_skill_effectiveness * hv_current_forecast_skill_effectiveness
    effectiveness_plot.opts(
        responsive=True,
        hooks=[
            remove_bokeh_logo,
            lambda p, e: add_custom_xticklabels_pentad(_, p, e)
        ],
        xticks=list(range(1,72,6)),
        title=title_effectiveness, shared_axes=False,
        #legend_position='bottom_left',  # 'right',
        xlabel=_("Pentad of the month (starting from January 1)"), ylabel=_("Effectiveness")+" [-]",
        show_grid=True, xlim=(1, 72), ylim=(0,1.4),
        fontsize={'legend':8}, fontscale=1.2
    )

    # Plot the forecast accuracy
    title_accuracy = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        ", " + _("data from") + " 2010 - " + str(title_date.year)

    hv_current_forecast_skill_accuracy = plot_current_runoff_forecasts(
        data=current_forecast_pentad,
        date_col='pentad',
        forecast_data_col='accuracy',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[%]')
    hv_forecast_skill_accuracy = plot_runoff_forecasts_steps(
        forecast_pentad, date_col='pentad',
        forecast_data_col='accuracy',
        forecast_name_col='model_short',
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string='[%]')

    # Plot column 'accuracy' over the pentad of the year
    title_accuracy = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        ", " + _("data from") + " 2010 - "+str(title_date.year)
    accuracy_plot = hv_forecast_skill_accuracy * hv_current_forecast_skill_accuracy
    accuracy_plot.opts(
        responsive=True,
        hooks=[
            remove_bokeh_logo,
            lambda p, e: add_custom_xticklabels_pentad(_, p, e)
        ],
        xticks=list(range(1,72,6)),
        title=title_accuracy, shared_axes=False,
        #legend_position='bottom_left',  # 'right',
        xlabel=_("Pentad of the month (starting from January 1)"), ylabel=_("Accruacy")+" [%]",
        show_grid=True, xlim=(1, 72), ylim=(0,100),
        fontsize={'legend':8}, fontscale=1.2
    )

    # Plot observed runoff against forecasted runoff


    # Create a column layout for the output
    all_skill_figures = pn.Column(
        effectiveness_plot,
        accuracy_plot,
    )

    return all_skill_figures




# endregion

