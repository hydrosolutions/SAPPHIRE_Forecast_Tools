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
from bokeh.models import HoverTool, FixedTicker, FuncTickFormatter, CustomJSTickFormatter, LinearAxis, DatetimeTickFormatter, NumberFormatter, DateFormatter
from scipy import stats
from sklearn.linear_model import LinearRegression
from bokeh.models.widgets.tables import CheckboxEditor, BooleanFormatter

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
runoff_full_range_color = "#eaf0f4"
runoff_90percentile_range_color = "#d5e2ea"
runoff_50percentile_range_color = "#c0d4df"
runoff_mean_color = "#307096"
runoff_last_year_color = "#ca97b7"
runoff_current_year_color = "#963070"
runoff_forecast_color_list = ["#455c1d", "#536f24", "#62832a", "#709630", "#7ea936", "#8dbd3c", "#99c64d"]

# Update visibility of sidepane widgets
def update_sidepane_card_visibility(tabs, card, event):
    if tabs.active == 1:
        card.visible = True
        card.visible = True
    else:
        card.visible = False
        card.visible = False

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
        print("MODEL: ", model)
        print("COLOR: ", runoff_forecast_color[i])
        # Get the latest forecast for the model
        latest_forecast = fl.round_discharge(model_data[forecast_data_col].iloc[-1])
        legend_entry = model + ": " + latest_forecast + " " + unit_string
        # Create the curve
        line = hv.Curve(
            model_data,
            kdims=[date_col],
            vdims=[forecast_data_col],
            label=legend_entry) \
                .opts(color=runoff_forecast_color[i],
                        line_width=2,
                        tools=['hover'],
                        show_legend=True)

        if overlay is None:
            overlay = line
        else:
            overlay *= line

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
        legend_entry = model + ": " + lower_bound + "-" + upper_bound + " " + unit_string

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

def plot_pentadal_vlines(data, date_col):
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
        vlines *= hv.Text(mid_date, 1, mid_date_text[i]) \
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

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station]
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
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo],
        #       lambda p, e: add_custom_xticklabels_daily_dates(_, linreg_predictor['leap_year'].iloc[0], p, e)],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d"),
        ylim=(0, data[max_col].max() * 1.1),
        tools=['hover'],
        toolbar='above')

    return daily_hydrograph

# endregion

# region forecast_tab
def plot_pentad_forecast_hydrograph_data(_, hydrograph_pentad_all, forecasts_all,
                                         station, title_date, model_selection,
                                         range_type, range_slider):

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    forecast_date = title_date + dt.timedelta(days=1)
    title_pentad = tl.get_pentad(forecast_date)
    title_month = tl.get_month_str_case2_viz(_, forecast_date)
    title_day_start = tl.get_pentad_first_day(forecast_date.strftime("%Y-%m-%d"))
    title_day_end = tl.get_pentad_last_day(forecast_date.strftime("%Y-%m-%d"))

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

    # Calculate the forecast ranges depending on the values of range_type and range_slider
    if range_type == _('delta'):
        forecasts.loc[:, 'fc_lower'] = forecasts.loc[:, 'forecasted_discharge'] - forecasts.loc[:, 'delta']
        forecasts.loc[:, 'fc_upper'] = forecasts.loc[:, 'forecasted_discharge'] + forecasts.loc[:, 'delta']
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

    # Convert date column of data dataframe to datetime
    data['date'] = pd.to_datetime(data['date'])

    # Set values after the title date to NaN
    data.loc[data['date'] > pd.Timestamp(title_date), str(current_year)] = np.nan

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
        data=forecasts,
        date_col=_('pentad_of_year column name'),
        forecast_name_col=_('forecast model short column name'),
        min_col=_('forecast lower bound column name'),
        max_col= _('forecast upper bound column name'),
        runoff_forecast_colors=runoff_forecast_color_list,
        unit_string=_("m³/s"))
    forecast_lower_bound = plot_runoff_range_bound(
        forecasts, _('pentad_of_year column name'), _('forecast lower bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_upper_bound = plot_runoff_range_bound(
        forecasts, _('pentad_of_year column name'), _('forecast upper bound column name'),
        runoff_forecast_color_list[3], hover_tool=True)
    forecast_line = plot_runoff_forecasts(
        forecasts, _('pentad_of_year column name'), _('forecasted_discharge column name'),
        _('forecast model short column name'), runoff_forecast_color_list, _('m³/s'))

    # Overlay the plots
    pentad_hydrograph = full_range_area * lower_bound * upper_bound * \
        area_05_95 * line_05 * line_95 * \
        area_25_75 * line_25 * line_75 * \
        forecast_area * forecast_lower_bound * forecast_upper_bound * \
        mean * current_year * forecast_line

    pentad_hydrograph.opts(
        title=title_text,
        xlabel=_('Pentad of the year'),
        ylabel=_('Discharge (m³/s)'),
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo,
               lambda p, e: add_custom_xticklabels_pentad(_, p, e)],
        tools=['hover'],
        toolbar='above')

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

    norm_stats_table = pn.widgets.Tabulator(
        value=forecast_table,
        #formatters={'Pentad': "{:,}"},
        #editors={_('δ'): None},  # Disable editing of the δ column
        theme='bootstrap',
        show_index=False,
        selectable='checkbox')

    return norm_stats_table

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



# Define the directory to save the data
SAVE_DIRECTORY = 'saved_data'
os.makedirs(SAVE_DIRECTORY, exist_ok=True)

def select_and_plot_data(_, linreg_predictor, station_widget, pentad_selector):
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
    save_file_name = f"{station_name}_{selected_pentad}_pentad_of_{title_month}.csv"
    save_file_path = os.path.join(SAVE_DIRECTORY, save_file_name)

    # Add the 'date' column if it doesn't exist
    if 'date' not in linreg_predictor.columns:
        # Map the 'pentad_in_year' to the corresponding date using get_date_for_pentad
        linreg_predictor['date'] = linreg_predictor['pentad_in_year'].apply(lambda pentad: tl.get_date_for_pentad(pentad))

    # Check if the saved CSV file for the specific pentad exists
    if os.path.exists(save_file_path):
        # Load the saved state
        forecast_table = pd.read_csv(save_file_path)
        print(f"Loaded saved state from {save_file_path}")
    else:
        # Filter data for the selected station and pentad across all years
        forecast_table = linreg_predictor[
            (linreg_predictor['station_labels'] == station_widget) & 
            (linreg_predictor['pentad_in_year'] == selected_pentad)
        ].copy().reset_index(drop=True)

        # Add a column to indicate visibility of points in the plot
        if 'visible' not in forecast_table.columns:
            forecast_table['visible'] = True

    # Define a variable to hold the visible data across functions
    global visible_data

    # Create Tabulator for displaying forecast data
    forecast_data_table = pn.widgets.Tabulator(
        value=forecast_table[['date', 'predictor', 'discharge_avg', 'visible']], 
        theme='bootstrap',
        show_index=False,  # Do not show the index column
        editors={'visible': CheckboxEditor()},  # Checkbox editor for the 'visible' column
        formatters={'visible': BooleanFormatter(),
                    'date': DateFormatter()},  # Format the date column
        height=450,
    )

    # Create the title text
    title_text = (f"{_('Hydropost')} {station_name}: {_('Forecast')} {_('for')} "
                  f"{title_pentad} {_('pentad')} {_('of')} {title_month} "
                  f"({_('for all years')})")

    # Define the plot
    plot_pane = pn.pane.HoloViews()

    # Create the selection1d stream to capture point selections on the plot
    selection_stream = streams.Selection1D(source=None)

    # Update plot based on visibility
    def update_plot(event=None):
        global visible_data  # Use global variable to ensure it's accessible in other functions
        
        forecast_table.update(forecast_data_table.value)

        # Filter the data based on visibility
        visible_data = forecast_table[forecast_table['visible'] == True]

        # Drop rows with NaNs in 'predictor' or 'discharge_avg'
        visible_data = visible_data.dropna(subset=['predictor', 'discharge_avg'])
        
        # If no data is visible, show an empty plot
        if visible_data.empty:
            scatter = hv.Curve([])  # Define an empty plot to avoid errors
        else:
            hover = HoverTool(
                tooltips=[
                    ('Date', '@date{%F}'),
                    ('Predictor', '@predictor'),
                    ('Discharge', '@discharge_avg'),
                ],
                formatters={'@date': 'datetime'},  # Format the date for hover
            )
            scatter = hv.Scatter(visible_data, kdims='predictor', vdims=['discharge_avg', 'date']) \
                .opts(color='blue', size=5, tools=['hover', 'tap'], xlabel=_('Predictor'), ylabel=_('Discharge (m³/s)'), title=title_text)

            if len(visible_data) > 1:
                # Add a linear regression line to the scatter plot
                slope, intercept, r_value, p_value, std_err = stats.linregress(visible_data['predictor'], visible_data['discharge_avg'])
                x = np.linspace(visible_data['predictor'].min(), visible_data['predictor'].max(), 100)
                y = slope * x + intercept
                line = hv.Curve((x, y)).opts(color='red', line_width=2)

                # Overlay the scatter plot and the linear regression line
                scatter = scatter * line
                scatter.opts(
                    title=title_text,
                    show_grid=True,
                    show_legend=True,
                    width=1000, 
                    height=450  
                )
            else:
                scatter.opts(
                    width=1000, 
                    height=450  
                )
        
        # Attach the plot to the selection stream
        selection_stream.source = scatter
        plot_pane.object = scatter

    # Function to handle plot selections and update the table
    def handle_selection(event):
        global visible_data  # Use the visible data in the selection handler

        if not selection_stream.index:
            return

        # Get the selected index from the scatter plot (relative to visible_data)
        selected_indices = selection_stream.index

        # Get the actual index from the forecast_table that corresponds to the visible data
        selected_rows = visible_data.iloc[selected_indices].index.tolist()

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

    # Function to save table data to CSV
    def save_to_csv(event):
        # Convert the table value back to a DataFrame
        updated_forecast_table = pd.DataFrame(forecast_data_table.value)

        # Explicitly reset the index before saving, so it becomes a column
        updated_forecast_table = updated_forecast_table.reset_index(drop=True)

        updated_forecast_table['pentad'] = selected_pentad

        # Save DataFrame to CSV, ensuring the index is saved
        updated_forecast_table.to_csv(save_file_path, index=False)
        print(f"Data saved to {save_file_path}")

        # Show the pop-up notification
        popup.visible = True

        # Hide the popup after a short delay (optional)
        pn.state.onload(lambda: pn.state.add_periodic_callback(lambda: setattr(popup, 'visible', False), 2000, count=1))

    # Create a save button
    save_button = pn.widgets.Button(name="Save Changes", button_type="success")

    # Attach the save_to_csv function to the button's click event
    save_button.on_click(save_to_csv)

    # Create the layout with the table, plot, save button, and popup
    layout = pn.Column(
        pn.Row(forecast_data_table, plot_pane),
        pn.Row(save_button), 
        pn.Row(popup)  
    )

    return layout


def update_forecast_data(_, linreg_predictor, station, pentad_selector):
    def callback(event):
        return select_and_plot_data(_, linreg_predictor, station, pentad_selector)

    return callback

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


