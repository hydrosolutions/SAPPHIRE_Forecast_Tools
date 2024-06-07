# vizualization.py
import os
import sys
import math
import pandas as pd
import datetime as dt
import holoviews as hv
from bokeh.models import FixedTicker, CustomJSTickFormatter, LinearAxis, DatetimeTickFormatter
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


# Defining colors (as global variables)
runoff_full_range_color = "#eaf0f4"
runoff_90percentile_range_color = "#d5e2ea"
runoff_50percentile_range_color = "#c0d4df"
runoff_mean_color = "#307096"
runoff_last_year_color = "#ca97b7"
runoff_current_year_color = "#963070"

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
    line = hv.Curve(
        data,
        kdims=[date_col],
        vdims=[line_data_col],
        label=label_text) \
        .opts(color=color,
              line_width=2,
              tools=['hover'])

    return line

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
                  line_width=0)

    return range_area

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

# endregion

# region predictor_tab
def plot_daily_hydrograph_data(_, hydrograph_day_all, linreg_predictor, station, title_date):

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    title_pentad = tl.get_pentad(title_date + dt.timedelta(days=1))
    title_month = tl.get_month_str_case2_viz(_, title_date)

    # filter hydrograph_day_all & linreg_predictor by station
    linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)
    print(linreg_predictor)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station]
    current_year = data['date'].dt.year.max()
    last_year = current_year - 1

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    predictor_string=_("Sum of runoff over the past 3 days: ") + f"{linreg_predictor['predictor'].values[0]}" + _(" m3/s")
    forecast_string=_("Forecast horizon for ") + title_pentad + _(" pentad of ") + title_month

    # Rename columns to be used in the plot to allow internationalization
    data = data.rename(columns={
        'date': _('date column name'),
        'day_of_year': _('day_of_year column name'),
        'min': _('min column name'),
        'max': _('max column name'),
        '5%': _('5% column name'),
        '95%': _('95% column name'),
        '25%': _('25% column name'),
        '75%': _('75% column name'),
        'mean': _('mean column name'),
        str(last_year): _('Last year column name'),
        str(current_year): _('Current year column name')
        })
    linreg_predictor = linreg_predictor.rename({
        'date': _('date column name'),
        'day_of_year': _('day_of_year column name'),
        'predictor': _('Predictor column name')
        })

    # Create a holoviews bokeh plots of the daily hydrograph
    hvspan_predictor = hv.Area(
        pd.DataFrame(
            {"x":[linreg_predictor['predictor_start_day_of_year'].values[0],
                  linreg_predictor['predictor_start_day_of_year'].values[0],
                  linreg_predictor['predictor_end_day_of_year'].values[0],
                  linreg_predictor['predictor_end_day_of_year'].values[0]],
             "y":[0.0, max(data[_('max column name')])*1.1,
                  max(data[_('max column name')])*1.1, 0.0]}),
            kdims=["x"], vdims=["y"], label=predictor_string) \
        .opts(alpha=0.2, color="#963070", line_width=0,
              muted_alpha=0.05)

    hvspan_forecast = hv.Area(
        pd.DataFrame(
            {"x":[linreg_predictor['forecast_start_day_of_year'].values[0],
                  linreg_predictor['forecast_start_day_of_year'].values[0],
                  linreg_predictor['forecast_end_day_of_year'].values[0],
                  linreg_predictor['forecast_end_day_of_year'].values[0]],
             "y":[0.0, max(data[_('max column name')])*1.1,
                  max(data[_('max column name')])*1.1, 0.0]}),
            kdims=["x"], vdims=["y"], label=forecast_string) \
        .opts(alpha=0.2, color="#709630", line_width=0,
              muted_alpha=0.05)

    full_range_area = plot_runoff_range_area(
        data, _('day_of_year column name'), _('min column name'), _('max column name'),
        _("Full range legend entry"), runoff_full_range_color)
    lower_bound = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('min column name'), runoff_full_range_color)
    upper_bound = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('max column name'), runoff_full_range_color)

    area_05_95 = plot_runoff_range_area(
        data, _('day_of_year column name'), _('5% column name'), _('95% column name'),
        _("90-percentile range legend entry"), runoff_90percentile_range_color)
    line_05 = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('5% column name'), runoff_90percentile_range_color)
    line_95 = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('95% column name'), runoff_90percentile_range_color)

    area_25_75 = plot_runoff_range_area(
        data, _('day_of_year column name'), _('25% column name'), _('75% column name'),
        _("50-percentile range legend entry"), runoff_50percentile_range_color)
    line_25 = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('25% column name'), runoff_50percentile_range_color)
    line_75 = plot_runoff_range_bound(
        data, _('day_of_year column name'), _('75% column name'), runoff_50percentile_range_color)

    mean = plot_runoff_line(
        data, _('date column name'), _('mean column name'),
        _('Mean legend entry'), runoff_mean_color)
    last_year = plot_runoff_line(
        data, _('day_of_year column name'), _('Last year column name'),
        _('Last year legend entry'), runoff_last_year_color)
    current_year = plot_runoff_line(
        data, _('day_of_year column name'), _('Current year column name'),
        _('Current year legend entry'), runoff_current_year_color)

    # Overlay the plots
    daily_hydrograph = mean  #full_range_area * lower_bound * upper_bound * \
        #area_05_95 * line_05 * line_95 * \
        #area_25_75 * line_25 * line_75 * \
        #last_year * hvspan_predictor * hvspan_forecast * \
        #mean * current_year

    daily_hydrograph.opts(
        title=title_text,
        xlabel=_('Day of the year (starting from January 1st)'),
        ylabel=_('Discharge (m³/s)'),
        height=400,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo,
               lambda p, e: add_custom_xticklabels_daily(_, linreg_predictor['leap_year'].iloc[0], p, e)],
        xformatter=DatetimeTickFormatter(days="%b %d", months="%b %d", years="%b"),
        tools=['hover'],
        toolbar='above')

    return daily_hydrograph

# endregion

# region forecast_tab
def plot_pentad_forecast_hydrograph_data(_, hydrograph_day_all, forecasts, station, title_date):

    # Date handling
    # Set the title date to the date of the last available data if the forecast date is in the future
    title_pentad = tl.get_pentad(title_date + dt.timedelta(days=1))
    title_month = tl.get_month_str_case2_viz(_, title_date)

    # filter hydrograph_day_all & linreg_predictor by station
    #linreg_predictor = processing.add_predictor_dates(linreg_predictor, station, title_date)

    # Filter forecasts for the current year and station
    forecasts = forecasts[(forecasts['station_labels'] == station) &
                            (forecasts['year'] == title_date.year)]
    #print("forecasts:\n", forecasts)

    data = hydrograph_day_all[hydrograph_day_all['station_labels'] == station]
    current_year = data['date'].dt.year.max()
    last_year = current_year - 1

    print("DEBUG data.pentad 1:\n", data.head(1))

    # Define strings
    title_text = _("Hydropost ") + station + _(" on ") + title_date.strftime("%Y-%m-%d")
    predictor_string=_("Sum of runoff over the past 3 days: ") #+ f"{linreg_predictor['predictor'].values[0]}" + _(" m3/s")
    forecast_string=_("Forecast horizon for ") + title_pentad + _(" pentad of ") + title_month

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
    last_year = plot_runoff_line(
        data, _('pentad_of_year column name'), _('Last year column name'),
        _('Last year legend entry'), runoff_last_year_color)
    current_year = plot_runoff_line(
        data, _('pentad_of_year column name'), _('Current year column name'),
        _('Current year legend entry'), runoff_current_year_color)

    # Overlay the plots
    daily_hydrograph = full_range_area * lower_bound * upper_bound * \
        area_05_95 * line_05 * line_95 * \
        area_25_75 * line_25 * line_75 * \
        last_year *  \
        mean * current_year

    daily_hydrograph.opts(
        title=title_text,
        xlabel=_('Pentad of the year'),
        ylabel=_('Discharge (m³/s)'),
        height=400,
        show_grid=True,
        show_legend=True,
        hooks=[remove_bokeh_logo,
               lambda p, e: add_custom_xticklabels_pentad(_, p, e)],
        tools=['hover'],
        toolbar='above')

    return daily_hydrograph


# endregion


