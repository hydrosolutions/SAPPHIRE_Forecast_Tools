# region Load libraries
from dotenv import load_dotenv
import os
import sys

import gettext  # For translation

import panel as pn

from bokeh.models import FixedTicker, CustomJSTickFormatter, LinearAxis

import numpy as np
import pandas as pd
import datetime as dt
import math

#import hvplot.pandas  # Enable interactive
import holoviews as hv
# Set the default backend to 'plotly'
#pn.extension('bokeh')

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
import forecast_library as fl

# endregion

# region Load configuration
# Set primary color to be consistent with the icon color
pn.extension(global_css=[':root { --design-primary-color: #307096; }'])

# Load .env file
# Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
# file to use
in_docker_flag = str(os.getenv("IN_DOCKER_CONTAINER"))
if in_docker_flag == "True":
    # Test if the .env file exists
    if not os.path.isfile("apps/config/.env"):
        raise Exception("File not found: " + "apps/config/.env")
    print("Running in Docker container")
    res = load_dotenv("apps/config/.env")
    # Test if res read
    if res is None:
        raise Exception("Could not read .env file")
else:
    # Test if the .env file exists
    if not os.path.isfile("../config/.env_develop"):
        raise Exception("File not found: " + "../config/.env_develop")
    print("Running locally")
    # The override flag in read_dotenv is set to allow switching between .env
    # files. Useful when testing different configurations.
    res = load_dotenv("../config/.env_develop", override=True)
    if res is None:
        raise Exception("Could not read .env_develop file")
    # Print ieasyreports_templates_directory_path from the environment
    # variables
    print("Configuration read from : ", os.getenv("ieasyforecast_configuration_path"))

# Test if the environment was loaded successfully
if os.getenv("ieasyforecast_hydrograph_day_file") is None:
    raise Exception("Environment not loaded. Please check if the .env file is available and if the environment variable IN_DOCKER_CONTAINER is set correctly.")

# Load filenames from the environment file
# These files are produced by the linreg (linear regression) tool.
# Daily data
hydrograph_day_file = os.path.join(
    os.getenv("ieasyforecast_intermediate_data_path"),
    os.getenv("ieasyforecast_hydrograph_day_file"))
# Test if file exists and thorw an error if not
if not os.path.isfile(hydrograph_day_file):
    raise Exception("File not found: " + hydrograph_day_file)
# Pentad data
hydrograph_pentad_file = os.path.join(
    os.getenv("ieasyforecast_intermediate_data_path"),
    os.getenv("ieasyforecast_hydrograph_pentad_file"))
# Test if file exists and thorw an error if not
if not os.path.isfile(hydrograph_pentad_file):
    raise Exception("File not found: " + hydrograph_pentad_file)
# Forecast results file
forecast_results_file = os.path.join(
    os.getenv("ieasyforecast_intermediate_data_path"),
    os.getenv("ieasyforecast_results_file")
)
# Test if file exists and thorw an error if not
if not os.path.isfile(forecast_results_file):
    raise Exception("File not found: " + forecast_results_file)
# Read the file listing all station codes available in the iEasyHydro DB.
all_stations_file = os.path.join(
    os.getenv("ieasyforecast_configuration_path"),
    os.getenv("ieasyforecast_config_file_all_stations")
)
# Test if file exists and thorw an error if not
if not os.path.isfile(all_stations_file):
    raise Exception("File not found: " + all_stations_file)

# Icon
if in_docker_flag == "True":
    icon_path = os.path.join("apps", "forecast_dashboard", "www", "Pentad.jpg")
else:
    icon_path = os.path.join("www", "Pentad.jpg")

# Test if file exists and thorw an error if not
if not os.path.isfile(icon_path):
    raise Exception("File not found: " + icon_path)

# Set primary color to be consistent with the icon color
pn.extension(global_css=[':root { --design-primary-color: #307096; }'])

# The current date is displayed as the title of each visualization.
today = dt.datetime.now()

# Read the locale from the environment file
current_locale = os.getenv("ieasyforecast_locale")
os.environ['LANGUAGE'] = current_locale

# Localization, translation to different languages.
localedir = os.getenv("ieasyforecast_locale_dir")
# Test if the directory exists
if not os.path.isdir(localedir):
    raise Exception("Directory not found: " + localedir)
# Create a translation object
gettext.bindtextdomain('pentad_dashboard', localedir)
gettext.textdomain('pentad_dashboard')
# use of _("") to translate strings
_ = gettext.gettext
# How to update the translation file:
# 1. Extract translatable strings from the source code
# xgettext -o ../config/locale/messages.pot pentad_dashboard.py
# 2. Create a new translation file, make sure you have a backup of the old one
# to avoid having to translate everything again.
# msginit -i ../config/locale/messages.pot -o ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.po -l ru_KG
# msginit -i ../config/locale/messages.pot -o ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.po -l en_CH
# 3. Translate the strings in the .po file and make sure that charset is set to
# UTF-8 (charset=UTF-8)
# 4. Compile the .po file to a .mo file
# msgfmt -o ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.mo ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.po
# msgfmt -o ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.mo ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.po
# endregion


# region Local functions
def remove_bokeh_logo(plot, element):
    plot.state.toolbar.logo = None

def add_custom_xticklabels_pentad(plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.
    ticks = list(range(1,72,1))  # Replace with your desired positions
    labels = {1:_('Jan')+', 1', 2:'1', 3:'3', 4:'4', 5:'5', 6:'6',
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

def add_custom_xticklabels_daily(plot, element):
    # Specify the positions and labels of the ticks. Here we use the first day
    # of each month & pentad per month as a tick.
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


# Daily data
def update_warning_text(event):
    warning_text_pane.object = get_current_predictor_and_dates(
        forecast_pentad, station.value).warning

def get_current_predictor_and_dates(forecast_pentad_all: pd.DataFrame,
                          station_widget) -> float:
    '''
    Reads the predictor for the current forecast horizon from the forecast
    dataframe.
    The function also changes, if we have recent forecast data in the forecast
    file and prints a warning if not.

    Parameters
    forecast_pentad_all: pd.DataFrame - DataFrame containing the forecast
    results for all stations and all dates.
    station_widget: str - Name of the selected station.

    Returns
    output: object - Object containing the current predictor and the forecast
    and predictor dates.
    '''
    fcdata_selection = forecast_pentad_all[
        forecast_pentad_all["station_labels"] == station_widget]

    # Define the output object. I want to be able to add attributes to it.
    class output:
        pass

    # Assign a devault warning text.
    output.warning = ""

    # Get the current date
    output.current_date = dt.datetime.now()

    # Get the latest forecast date from the forecast data
    output.latest_forecast_date = fcdata_selection["Date"].max()

    # Based on the latest forecast date, get the current forecast horizon
    output.forecast_start = output.latest_forecast_date + dt.timedelta(days=1)
    output.forecast_end = output.latest_forecast_date + dt.timedelta(days=5)

    # Also get the predictor date range
    output.predictor_start = output.latest_forecast_date - dt.timedelta(days=4.5)
    output.predictor_end = output.latest_forecast_date - dt.timedelta(days=1.5)

    # And get the predictor value from the forecast data. If we do not have a
    # predictor value, we set it to NaN. This can happen if the forecast tools
    # do not have access to recent river runoff data.
    try:
        output.predictor = fcdata_selection[
            fcdata_selection["Date"] == output.latest_forecast_date]["predictor"].values[0]
    except IndexError:
        output.predictor = np.nan

    # Let's also put the forecast & forecast ranges into the output
    try:
        output.fc_exp = fcdata_selection[
            fcdata_selection["Date"] == output.latest_forecast_date]["fc_qexp"].values[0]
    except IndexError:
        output.fc_exp = np.nan
    try:
        output.fc_qmin = fcdata_selection[
            fcdata_selection["Date"] == output.latest_forecast_date]["fc_qmin"].values[0]
    except IndexError:
        output.fc_qmin = np.nan
    try:
        output.fc_qmax = fcdata_selection[
            fcdata_selection["Date"] == output.latest_forecast_date]["fc_qmax"].values[0]
    except IndexError:
        output.fc_qmax = np.nan

    # If the current date is later than the latest forecast date plus 5 days,
    # we write a warnign to the output object.
    if output.current_date > output.latest_forecast_date + dt.timedelta(days=5):
        output.warning = _("Warning: No recent forecast available for the selected station. Showing the latest forecast data available. Please check if the Forecast Tools are running.")
    if np.isnan(output.predictor):
        output.warning = _("Warning: No recent forecast available for the selected station. Showing the latest forecast data available. Please check if the Forecast Tools are running.")

    return output


def preprocess_hydrograph_day_data(hydrograph_day):
    # Drop the Code column
    hydrograph_day = hydrograph_day.drop(columns=["Code", "station_labels"])

    # Melt the DataFrame to simplify the column index
    hydrograph_day = hydrograph_day.melt(id_vars=["day_of_year"], var_name="Year", value_name="value")

    # Set index to day of year
    hydrograph_day = hydrograph_day.set_index("day_of_year")

    # Calculate norm and percentiles for each day_of_year over all Years in the hydrograph
    norm = hydrograph_day.groupby("day_of_year")["value"].mean()
    perc_05 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.05)
    perc_25 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.25)
    perc_75 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.75)
    perc_95 = hydrograph_day.groupby("day_of_year")["value"].quantile(0.95)

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "day_of_year": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values
    })

    # Get the current year from the system date
    current_year = today.year
    current_year_col = str(current_year)

    # Add the current year data to the DataFrame
    # Attention: this can become an empty selection if we don't have any data
    # for the current year yet (e.g. when a new year has just started and no
    # forecasts have been produced for the new year yet).
    # To avoid the error, we need to check if the current year is in the
    # hydrograph_day DataFrame. If it is not, we display the latest year data.
    if current_year_col in hydrograph_day["Year"].values:
        current_year_data = hydrograph_day[hydrograph_day["Year"] == str(current_year_col)]["value"].values
    else:
        current_year_col = hydrograph_day["Year"].values[-1]
        current_year_data = hydrograph_day[hydrograph_day["Year"] == str(current_year_col)]["value"].values

    hydrograph_norm_perc["current_year"] = current_year_data

    return(hydrograph_norm_perc)

# Pentad data
def preprocess_hydrograph_pentad_data(hydrograph_pentad: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculates the norm and percentiles for each pentad over all Years in the
    input hydrograph. Note that the input hydrograph is filtered to only one
    station.
    '''
    # Drop the Code column
    hydrograph_pentad = hydrograph_pentad.drop(columns=["Code", "station_labels"])
    # Melt the DataFrame to simplify the column index
    hydrograph_pentad = hydrograph_pentad.melt(id_vars=["pentad"], var_name="Year", value_name="value")

    # Set index to pentad of year
    hydrograph_pentad = hydrograph_pentad.set_index("pentad")

    # Calculate norm and percentiles for each day_of_year over all Years in the hydrograph
    norm = hydrograph_pentad.groupby("pentad")["value"].mean().reset_index(drop=True)
    perc_05 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.05).reset_index(drop=True)
    perc_25 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.25).reset_index(drop=True)
    perc_75 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.75).reset_index(drop=True)
    perc_95 = hydrograph_pentad.groupby("pentad")["value"].quantile(0.95).reset_index(drop=True)

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "pentad": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values
    })

    # Get the current year from the system date
    current_year = today.year
    current_year_col = str(current_year)

    # If current year data is not available, use the previous year data
    if current_year_col not in hydrograph_pentad["Year"].values:
        current_year_col = hydrograph_pentad["Year"].values[-1]
    current_year_data = hydrograph_pentad[hydrograph_pentad["Year"] == str(current_year_col)]["value"].values

    # Add the current year data to the DataFrame
    hydrograph_norm_perc["current_year"] = current_year_data
    hydrograph_norm_perc["pentad"] = hydrograph_norm_perc["pentad"] + 1

    return(hydrograph_norm_perc)

def calculate_pentad_forecast_accuracy(
        hydrograph_pentad_stat: pd.DataFrame,
        forecast_pentad_stat: pd.DataFrame,
        range_selection_widget, manual_range_widget) -> pd.DataFrame:
    '''
    Calculates the forecast accuracy for each pentad of the year. The forecast
    accuracy is calculated following the Kyrgyz Hydromet method.
    |𝑄_𝑠𝑖𝑚−𝑄_𝑜𝑏𝑠 |≤ 0.674∙𝜎_(𝑄_𝑜𝑏𝑠) and is given in % between 0 and 100. The
    method also allows to calculate the standard error of the forecast.
    We further calculate the forecast accuracy assuming an allowable range of
    plus minus 20% of the expected forecast |𝑄_𝑠𝑖𝑚−𝑄_𝑜𝑏𝑠 |≤ 0.2∙𝑄_𝑜𝑏𝑠.
    '''

    # Drop the Code column
    hydrograph_pentad_stat = hydrograph_pentad_stat.drop(columns=["Code", "station_labels"])

    # Melt the DataFrame to simplify the column index
    hydrograph_pentad_stat = hydrograph_pentad_stat.melt(id_vars=["pentad"], var_name="Year", value_name="value")

    # Rename the column value to q_obs
    hydrograph_pentad_stat = hydrograph_pentad_stat.rename(columns={"value": "q_obs"})

    # Make sure we have the same years of data available in both DataFrames
    # From hydrograph_pentad_stat, drop all rows where Year is < than the
    # year of the first Date in forecast_pentad_stat.
    # Add a column year to hydrograph_pentad_stat where "Year" is converted to
    # integer.
    hydrograph_pentad_stat["year"] = hydrograph_pentad_stat["Year"].astype(int)
    hydrograph_pentad_stat = hydrograph_pentad_stat[hydrograph_pentad_stat["year"]>=int(forecast_pentad_stat["Date"].dt.year.min())]
    # Drop the column year
    hydrograph_pentad_stat = hydrograph_pentad_stat.drop(columns=["year"])

    # Add the column Year to the forecast_pentad DataFrame
    forecast_pentad_stat["Year"] = forecast_pentad_stat["Date"].dt.year

    # Number of years
    noyears = len(forecast_pentad_stat["Year"].unique())

    # Merge forecast_pentad fc_qexp by year and pentad to the hydrograph_pentad DataFrame
    hydrograph_pentad_stat["Year"] = hydrograph_pentad_stat["Year"].astype(int)
    hydrograph_pentad_stat = hydrograph_pentad_stat.merge(forecast_pentad_stat[["Year", "pentad", "fc_qexp"]],
                                            on=["Year", "pentad"], how="left")

    # Calculate forecast accuracy
    # Group hydrograph_pentad by pentad and calculate the standard deviation of the
    # column value for each pentad
    hydrograph_pentad_std = hydrograph_pentad_stat.groupby("pentad")["q_obs"].std().reset_index(drop=False)

    # Rename the column q_obs to q_obs_std
    hydrograph_pentad_std = hydrograph_pentad_std.rename(columns={"q_obs": "q_obs_std"})
    # Add the standard deviation to hydrograph_pentad
    hydrograph_pentad_stat = hydrograph_pentad_stat.merge(hydrograph_pentad_std, on="pentad", how="left")

    # Calculate the absolute difference between the columns q_obs and fc_qexp
    hydrograph_pentad_stat["abs_diff"] = abs(hydrograph_pentad_stat["q_obs"] - hydrograph_pentad_stat["fc_qexp"])

    # Also calculate the so-called standard error (standard deviation of the difference between q_obs and fc_qexp)
    hydrograph_pentad_stderr = hydrograph_pentad_stat.groupby("pentad")["abs_diff"].std().reset_index(drop=False)
     # Rename the column q_obs to q_obs_std
    hydrograph_pentad_stderr = hydrograph_pentad_stderr.rename(columns={"abs_diff": "std_err"})
    hydrograph_pentad_stderr["std_err"] = hydrograph_pentad_stderr["std_err"]/hydrograph_pentad_std["q_obs_std"]

    hydrograph_pentad_stat = hydrograph_pentad_stat.merge(hydrograph_pentad_stderr, on="pentad", how="left")

    # Calculate the forecast accuracy for each pentad. If abs_diff <= 0.674*q_obs_std, accuracy flag is 1, else 0
    hydrograph_pentad_stat["forecast_skill"] = np.where(hydrograph_pentad_stat["abs_diff"] <= 0.674*hydrograph_pentad_stat["q_obs_std"], 1, 0)
    hydrograph_pentad_stat["forecast_skill_20"] = np.where(hydrograph_pentad_stat["abs_diff"] <= float(manual_range_widget)/100.0*hydrograph_pentad_stat["q_obs"], 1, 0)

    # If we do not have observations, we cannot calculate forecast skill.
    # Therefore we set forecast skill to NaN if q_obs is NaN
    hydrograph_pentad_stat["forecast_skill"] = np.where(hydrograph_pentad_stat["q_obs"].isna(), np.nan, hydrograph_pentad_stat["forecast_skill"])
    hydrograph_pentad_stat["forecast_skill_20"] = np.where(hydrograph_pentad_stat["q_obs"].isna(), np.nan, hydrograph_pentad_stat["forecast_skill_20"])
    # Same if we do not have a forecast
    hydrograph_pentad_stat["forecast_skill"] = np.where(hydrograph_pentad_stat["fc_qexp"].isna(), np.nan, hydrograph_pentad_stat["forecast_skill"])
    hydrograph_pentad_stat["forecast_skill_20"] = np.where(hydrograph_pentad_stat["fc_qexp"].isna(), np.nan, hydrograph_pentad_stat["forecast_skill_20"])

    # For each pentad, calculate sum of forecast_skill == 1 and divide by the number of years and calculate the mean of std_err
    hydrograph_pentad_stat_skill = hydrograph_pentad_stat.groupby("pentad")["forecast_skill"].sum().reset_index()
    hydrograph_pentad_stat_skill_20 = hydrograph_pentad_stat.groupby("pentad")["forecast_skill_20"].sum().reset_index()
    hydrograph_pentad_stat_std_err = hydrograph_pentad_stat.groupby("pentad")["std_err"].mean().reset_index(drop=False)
    hydrograph_pentad_stat_skill["forecast_skill"] = hydrograph_pentad_stat_skill["forecast_skill"] / float(noyears) * 100.0
    hydrograph_pentad_stat_skill["forecast_skill_20"] = hydrograph_pentad_stat_skill_20["forecast_skill_20"] / float(noyears) * 100.0

    # Combine hydrograph_pentad_stat_skill and hydrograph_pentad_stat_std_err into hydrograph_pentad_stat
    hydrograph_pentad_stat = hydrograph_pentad_stat_skill.merge(hydrograph_pentad_stat_std_err, on="pentad", how="left")

    return(hydrograph_pentad_stat)

# Update widgets based on active tab
def update_widgets(event):
    if tabs.active == 1:
        range_selection.visible = True
        manual_range.visible = True
    else:
        range_selection.visible = False
        manual_range.visible = False

# endregion

# region Read data

# Read hydrograph data - daily
hydrograph_day_all = pd.read_csv(hydrograph_day_file).reset_index(drop=True)
hydrograph_day_all["day_of_year"] = hydrograph_day_all["day_of_year"].astype(int)
hydrograph_day_all['Code'] = hydrograph_day_all['Code'].astype(str)
# Sort all columns in ascending Code and pentad order
hydrograph_day_all = hydrograph_day_all.sort_values(by=["Code", "day_of_year"])
# Remove the day 366 (for leap years)
hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["day_of_year"] != 366]

# Read hydrograph data - pentad
hydrograph_pentad_all = pd.read_csv(hydrograph_pentad_file).reset_index(drop=True)
hydrograph_pentad_all["pentad"] = hydrograph_pentad_all["pentad"].astype(int)
hydrograph_pentad_all['Code'] = hydrograph_pentad_all['Code'].astype(str)
# Sort all columns in ascending Code and pentad order
hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["Code", "pentad"])

# Read forecast results
forecast_pentad = pd.read_csv(forecast_results_file)
# Convert the date column to datetime. The format of the date string is %Y-%m-%d.
forecast_pentad['Date'] = pd.to_datetime(forecast_pentad['date'], format='%Y-%m-%d')
# Make sure the date column is in datetime64 format
forecast_pentad['Date'] = forecast_pentad['Date'].astype('datetime64[ns]')
# Convert the code column to string
forecast_pentad['code'] = forecast_pentad['code'].astype(str)
# Check if there are duplicates for Date and code columns. If yes, only keep the
# last one
forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code'], keep='last')
# Get the pentad of the year
forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
# Cast pentad column no number
forecast_pentad['pentad'] = forecast_pentad['pentad'].astype(int)

# List of stations with forecast data
station_list = hydrograph_day_all['Code'].unique().tolist()

# Read stations json
with open(all_stations_file, "r") as json_file:
    all_stations = fl.load_all_station_data_from_JSON(all_stations_file)
# Convert the code column to string
all_stations['code'] = all_stations['code'].astype(str)
# Left-join all_stations['code', 'river_ru', 'punkt_ru'] by 'Code' = 'code'
station_df=pd.DataFrame(station_list, columns=['Code'])
station_df=station_df.merge(all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')

# Paste together the columns Code, river_ru and punkt_ru to a new column
# station_labels. river and punkt names are currently only available in Russian.
station_df['station_labels'] = station_df['Code'] + ' - ' + station_df['river_ru'] + ' ' + station_df['punkt_ru']

# Add the station_labels column to the hydrograph_day_all DataFrame
hydrograph_day_all = hydrograph_day_all.merge(
    all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')
hydrograph_day_all['station_labels'] = hydrograph_day_all['Code'] + ' - ' + hydrograph_day_all['river_ru'] + ' ' + hydrograph_day_all['punkt_ru']
# Remove the columns river_ru and punkt_ru
hydrograph_day_all = hydrograph_day_all.drop(columns=['code', 'river_ru', 'punkt_ru'])

# Same for the pentadal data
hydrograph_pentad_all = hydrograph_pentad_all.merge(
    all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='Code', right_on='code', how='left')
hydrograph_pentad_all['station_labels'] = hydrograph_pentad_all['Code'] + ' - ' + hydrograph_pentad_all['river_ru'] + ' ' + hydrograph_pentad_all['punkt_ru']
# Remove the columns river_ru and punkt_ru
hydrograph_pentad_all = hydrograph_pentad_all.drop(columns=['code', 'river_ru', 'punkt_ru'])

# Same for forecast data
forecast_pentad = forecast_pentad.merge(
    all_stations.loc[:,['code','river_ru','punkt_ru']],
    left_on='code', right_on='code', how='left')
forecast_pentad['station_labels'] = forecast_pentad['code'] + ' - ' + forecast_pentad['river_ru'] + ' ' + forecast_pentad['punkt_ru']
forecast_pentad = forecast_pentad.drop(columns=['river_ru', 'punkt_ru'])

# Update the station list to include the station labels
# Write the column station_labels to a list
station_list = station_df['station_labels'].tolist()

# endregion

# region Dashboard widgets

## Create widgets
station = pn.widgets.Select(
    name=_("Select discharge station:"),
    options=station_list,
    value=station_list[0])

range_selection = pn.widgets.Select(
    name=_("Select forecast range for display:"),
    options=[_("0.674 sigma"), _("Manual range, select value below")],
    value=_("0.674 sigma"))
# Initially hide the widget
range_selection.visible = False

manual_range = pn.widgets.IntSlider(
    name=_("Manual range (%)"),
    start=0,
    end=100,
    value=20,
    step=1,
)
# Initially hide the widget
manual_range.visible = False

# Warning text pane
warning_text_pane = pn.pane.Markdown(get_current_predictor_and_dates(
        forecast_pentad, station.value).warning,
        style={'white-space': 'pre-wrap'}, width=station.width)

# endregion

# region Subsetting data

# Subset of the hydrograph data for a selected station
hydrograph_day = hydrograph_day_all[hydrograph_day_all["station_labels"] == station.value]
hydrograph_pentad = hydrograph_pentad_all[hydrograph_pentad_all["station_labels"] == station.value]
forecast_pentad_stat = forecast_pentad[forecast_pentad["station_labels"] == station.value]

# Test if the dates in forecast_pentad_stat overlap with the dates in
# hydrograph_pentad.
# From hydrograph_pentad, return second last column name as integer.
temp_last_year_hydrograph_data = int(hydrograph_pentad.drop(columns='station_labels').columns.values[-1])

# From forecast_pentad_stat, return the year of the lowest date as integer.
temp_first_year_forecast_data = forecast_pentad_stat["Date"].min().year

if temp_first_year_forecast_data > temp_last_year_hydrograph_data:
    print("WARNING: forecast_pentad_stat contains dates before the first date in hydrograph_pentad")
    no_date_overlap_flag = True
else:
    no_date_overlap_flag = False
# endregion

# region Update functions

def plot_daily_hydrograph_data(station_widget, fcdata):

    dates_collection = get_current_predictor_and_dates(forecast_pentad, station_widget)

    hydrograph_day = hydrograph_day_all[hydrograph_day_all["station_labels"] == station_widget]

    # Reformat data to the hydrograph format with 50 and 90 percentiles and average.
    data = preprocess_hydrograph_day_data(hydrograph_day)

    # We need to print a suitable date for the figure titles.
    title_date = dates_collection.latest_forecast_date.strftime('%Y-%m-%d')
    title_pentad = tl.get_pentad(title_date)
    title_month = tl.get_month_str_case2(title_date)

    # The predictor range is the last 3 days before the start of each pentad.
    # We get the start date of the current pentad and subtract 1 to 4 days from
    # it to get the predictor range.
    pentad_start_day = int(tl.get_pentad_first_day_of_year(title_date))

    # Calculate the predictor range from the last_day_of_year with data -1 to -4
    # Note that here we calculate the precictor differently from what we use in
    # the linear regression tool. There we fill data gaps.
    predictor_start = pentad_start_day - 4.5
    predictor_end = pentad_start_day - 1.5

    # Also show the forecast horizon on the figure
    forecast_horizon_start = pentad_start_day
    forecast_horizon_end = pentad_start_day + 5

    # Rename the columns
    data = data.rename(columns={"day_of_year": "День года",
                                "Norm": "Q Норма м3/с",
                                "current_year": "Q Текущий год м3/с",})

    # Add a date column, use the current year and the day of year
    data["Дата"] = pd.to_datetime(data['День года'], format='%j').dt.strftime('%b-%d')

    # Mapping of English month abbreviations to Russian ones
    if current_locale == "ru_KG":
        # Split the 'Дата' column into month and day
        data['month'], data['day'] = zip(*data['Дата'].apply(lambda x: x.split('-', 1)))

        month_mapping = {
            'Jan': 'Янв', 'Feb': 'Фев', 'Mar': 'Мар', 'Apr': 'Апр', 'May': 'Май', 'Jun': 'Июн',
            'Jul': 'Июл', 'Aug': 'Авг', 'Sep': 'Сен', 'Oct': 'Окт', 'Nov': 'Ноя', 'Dec': 'Дек'
        }
        # Convert date data to strings of Russian month abbreviations
        data['month'] = data['month'].map(month_mapping)
        # Combine the month and day back into the 'Дата' column
        data['Дата'] = data['month'] + '-' + data['day']
        # Drop the month and day columns
        data = data.drop(columns=['month', 'day'])

    # Get dates for the predictor range
    predictor_start_date = pd.to_datetime(predictor_start, format='%j').strftime('%b-%d')
    predictor_end_date = pd.to_datetime(predictor_end, format='%j').strftime('%b-%d')
    if current_locale == "ru_KG":
        # Split the 'Дата' column into month and day
        predictor_start_date_month, predictor_start_date_day = predictor_start_date.split('-', 1)
        predictor_end_date_month, predictor_end_date_day = predictor_end_date.split('-', 1)
        # Map the month abbreviations to Russian ones
        predictor_start_date_month = month_mapping[predictor_start_date_month]
        predictor_end_date_month = month_mapping[predictor_end_date_month]
        # Combine the month and day back into the date
        predictor_start_date = predictor_start_date_month + '-' + predictor_start_date_day
        predictor_end_date = predictor_end_date_month + '-' + predictor_end_date_day


    title = _("Station ") + str(station_widget) + _(" on ") + title_date
    predictor_string=_("Sum of runoff over the past 3 days: ") + f"{dates_collection.predictor:.1f}" + _(" m3/s")
    forecast_string=_("Forecast horizon for ") + title_pentad + _(" pentad ") + title_month
    ## Bokeh plot
    hv_empty = hv.Scatter([], [], label=predictor_string) \
        .opts(color="white", size=0.000000001)
    #vpsan = hv.VSpan(predictor_start, predictor_end, label="Predictor range") \
    #    .opts(alpha=0.1, color="red", line_width=0)
    #vpsan_forecast = hv.VSpan(forecast_horizon_start, forecast_horizon_end,
    #                          label="Forecast horizon") \
    #    .opts(alpha=0.2, color="#72D1FA", line_width=0)
    hvspan = hv.Area(pd.DataFrame({"x":[predictor_start, predictor_start,
                                        predictor_end, predictor_end],
                                  "y":[0, max([max(data["Perc_95"]),
                                               max(data["Q Текущий год м3/с"])])*(1.1),
                                       max([max(data["Perc_95"]),
                                            max(data["Q Текущий год м3/с"])])*(1.1), 0]}),
                     kdims=["x"], vdims=["y"], label=predictor_string) \
        .opts(alpha=0.1, color="red", line_width=0)
    hvspan_forecast = hv.Area(pd.DataFrame({"x":[forecast_horizon_start, forecast_horizon_start,
                                        forecast_horizon_end, forecast_horizon_end],
                                  "y":[0, max([max(data["Perc_95"]),
                                               max(data["Q Текущий год м3/с"])])*(1.1),
                                       max([max(data["Perc_95"]),
                                            max(data["Q Текущий год м3/с"])])*(1.1), 0]}),
                     kdims=["x"], vdims=["y"], label=forecast_string) \
        .opts(alpha=0.2, color="#72D1FA", line_width=0)
    curve_norm = hv.Curve(
        data, kdims=["День года"], vdims=["Q Норма м3/с"],
        label=_("Multi-year average")) \
        .opts(line_width=1, color="#307096", tools=['hover'])
    curve_year = hv.Curve(
        data, kdims=["День года"], vdims=["Q Текущий год м3/с"],
        label=_("Current year")) \
        .opts(line_width=1, color="red", tools=['hover'])
    area_05_95 = hv.Area(
        data, kdims=["День года"], vdims=["Perc_05", "Perc_95"],
        label=_("90-percentile range")) \
        .opts(alpha=0.05, color="#307096", line_width=0)
    area_25_75 = hv.Area(
        data, kdims=["День года"], vdims=["Perc_25", "Perc_75"],
        label=_("50-percentile range")) \
        .opts(alpha=0.1, color="#307096", line_width=0)
    p = hvspan * hvspan_forecast * area_05_95 * area_25_75 * curve_norm * curve_year
    p.opts(responsive=True, title=title,
          xlabel=_("Day of the year (starting from January 1)"), ylabel=_('River discharge [m3/s]'),
          yformatter="%.0f", show_grid=True,
          hooks=[remove_bokeh_logo, add_custom_xticklabels_daily],
          xlim=(1, 365), legend_position='top_left',
          fontsize={'legend':9}, fontscale=1.2, shared_axes=False)  # ,
            # xticks=[1,32,13,19,25,31,37,43,49,55,61,67])  #, legend_offset=(10, 120))

    return p

def plot_forecast_data(station_widget, range_selection_widget, manual_range_widget):
    fcdata = forecast_pentad[forecast_pentad["station_labels"] == station_widget]
    hydrograph_pentad = hydrograph_pentad_all[hydrograph_pentad_all["station_labels"] == station_widget]
    hydrograph_pentad = preprocess_hydrograph_pentad_data(hydrograph_pentad)
    dates_collection = get_current_predictor_and_dates(forecast_pentad, station_widget)

    # Rename the columns
    hydrograph_pentad = hydrograph_pentad.rename(columns={"pentad": "Пентада года",
                                                          "Norm": "Q Норма м3/с",
                                                          "current_year": "Q Текущий год м3/с",})
    # Add a manual forecast range to the dataframe
    fcdata["fc_qmin_20p"] = fcdata["fc_qexp"] - float(manual_range_widget)/100.0*fcdata["fc_qexp"]
    fcdata["fc_qmax_20p"] = fcdata["fc_qexp"] + float(manual_range_widget)/100.0*fcdata["fc_qexp"]

    # Filter forecast data for the current year
    fcdata_filtered = fcdata[fcdata["Date"].dt.year == today.year]
    # If fcdata_filtered is empty, use the previous year data
    if fcdata_filtered.empty:
        fcdata_filtered = fcdata[fcdata["Date"].dt.year == today.year - 1]

    # We need to print a suitable date for the figure titles. We use the last
    # date of fcdata_filtered where .
    title_date = dates_collection.latest_forecast_date
    title_date_str = title_date.strftime('%Y-%m-%d')
    title_pentad = tl.get_pentad(title_date_str)
    title_month = tl.get_month_str_case2(title_date_str)

    # Filter forecast data for the last date
    fcdata = fcdata[fcdata["Date"] == fcdata["Date"].max()]
    title = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        _(" (forecast for ") + title_pentad + _(" pentad ") + title_month + _(")")
    forecast_string="Q exp.:" + f"{fcdata['fc_qexp'].values[0]:.1f}" + _(" m3/s") +"\nQ range: " + f"{fcdata['fc_qmin'].values[0]:.1f} - {fcdata['fc_qmax'].values[0]:.1f}" + _("m3/s")
    fcqexp_string = _("Expected forecast: ") + f"{fcdata['fc_qexp'].values[0]:.1f} " + _("m3/s")
    fcqrange_string = _("Forecast range: ") + f"{fcdata['fc_qmin'].values[0]:.1f} - {fcdata['fc_qmax'].values[0]:.1f} "+_("m3/s")

    fcqrange_string_20p = _("Forecast range: ") + f"{fcdata['fc_qmin_20p'].values[0]:.1f} - {fcdata['fc_qmax_20p'].values[0]:.1f} "+_("m3/s")

    fcdata["yerrl"] = fcdata["fc_qexp"] - fcdata["fc_qmin"]
    fcdata["yerru"] = fcdata["fc_qmax"] - fcdata["fc_qexp"]

    fcdata = fcdata.rename(columns={"pentad": "Пентада года",
                                    "fc_qexp": "Ожидаемое значение прогноза м3/с"})
    fcdata_filtered = fcdata_filtered.rename(columns={"pentad": "Пентада года"})

    hv_empty = hv.Scatter([], [], label=forecast_string) \
        .opts(color="white", size=0.000000001)
    hv_err_20p = hv.Area(fcdata, kdims=["Пентада года"], vdims=["fc_qmin_20p", "fc_qmax_20p"],
                     label=fcqrange_string_20p) \
                     .opts(alpha=0.6, color="#72D1FA", line_width=8,
                           line_color="#72D1FA")
    hv_err = hv.Area(fcdata, kdims=["Пентада года"], vdims=["fc_qmin", "fc_qmax"],
                     label=fcqrange_string) \
                     .opts(alpha=0.6, color="#72D1FA", line_width=8,
                           line_color="#72D1FA")
    hv_qexp = hv.Scatter(fcdata, kdims=["Пентада года"], vdims=["Ожидаемое значение прогноза м3/с"],
                         label=fcqexp_string) \
        .opts(color="#72D1FA", size=10, tools=['hover'])
    #hv_err = fcdata.hvplot.errorbars(x="pentad", y="fc_qexp", yerr1="yerrl",
    #                           yerr2="yerru") \
    #    .opts(color="#72D1FA", alpha=0.1, line_color="#72D1FA", line_alpha=0.1,
    #         line_width=5, lower_head=None, upper_head=None)
    curve_norm = hv.Curve(
        hydrograph_pentad, kdims=["Пентада года"], vdims=["Q Норма м3/с"],
        label=_("Multi-year average")) \
            .opts(line_width=1, color="#307096", tools=['hover'])
    curve_year = hv.Curve(
        hydrograph_pentad, kdims=["Пентада года"], vdims=["Q Текущий год м3/с"],
        label=_("Current year")) \
            .opts(line_width=1, color="red", tools=['hover'])
    curve_forecasts = hv.Curve(
        fcdata_filtered, kdims=["Пентада года"], vdims=["fc_qexp"],
        label=_("Current year forecast")) \
            .opts(line_width=1, color="#72D1FA", tools=['hover'])
    area_forecast = hv.Area(
        fcdata_filtered, kdims=["Пентада года"], vdims=["fc_qmin", "fc_qmax"],
        label=_("Current year forecast range")+" (0.674 "+_("sigma")+")") \
            .opts(alpha=0.2, color="#72D1FA", line_width=0)
    area_forecast_20 = hv.Area(
        fcdata_filtered, kdims=["Пентада года"], vdims=["fc_qmin_20p", "fc_qmax_20p"],
        label=_("Current year forecast range")+" ("+str(manual_range_widget)+"%)") \
            .opts(alpha=0.2, color="#72D1FA", line_width=0)
    area_05_95 = hv.Area(
        hydrograph_pentad, kdims=["Пентада года"], vdims=["Perc_05", "Perc_95"],
        label=_("90-percentile range")) \
            .opts(alpha=0.05, color="#307096", line_width=0)
    area_25_75 = hv.Area(
        hydrograph_pentad, kdims=["Пентада года"], vdims=["Perc_25", "Perc_75"],
        label=_("50-percentile range")) \
            .opts(alpha=0.1, color="#307096", line_width=0)
    if range_selection_widget=="0.674 sigma":
        p = area_05_95 * area_25_75 * area_forecast * curve_norm * curve_year * \
            curve_forecasts * hv_err * hv_qexp
    else:
        p = area_05_95 * area_25_75 * area_forecast_20 * curve_norm * curve_year * \
            curve_forecasts * hv_err_20p * hv_qexp
    p.opts(responsive=True, title=title,
              xlabel=_("Pentad of the month (starting from January 1)"), ylabel=_('River discharge [m3/s]'),
              yformatter="%.1f", show_grid=True, legend_muted=False,
            legend_position='top_left',  # 'right',
            legend_cols=1,
            hooks=[remove_bokeh_logo, add_custom_xticklabels_pentad], xlim=(1,72),
            fontsize={'legend':8}, fontscale=1.2, shared_axes=False,
            xticks=[1,7,13,19,25,31,37,43,49,55,61,67])

    return p

def plot_effectiveness_of_forecast_method(station_widget,
                                          range_selection_widget, manual_range_widget):
    forecast_pentad_stat_effectiveness = forecast_pentad[forecast_pentad["station_labels"] == station_widget]
    hydrograph_pentad_effectiveness = hydrograph_pentad_all[hydrograph_pentad_all["station_labels"] == station_widget]
    hydrograph_pentad_stat_effectiveness = calculate_pentad_forecast_accuracy(
        hydrograph_pentad_effectiveness, forecast_pentad_stat_effectiveness,
        range_selection_widget, manual_range_widget)
    dates_collection = get_current_predictor_and_dates(forecast_pentad, station_widget)

    # We need to print a suitable date for the figure titles. We use the last
    # date of fcdata_filtered.
    title_date = dates_collection.latest_forecast_date
    title_date_str = title_date.strftime('%Y-%m-%d')
    title_pentad = tl.get_pentad(title_date_str)
    title_month = tl.get_month_str_case2(title_date_str)

    # Drop the column forecast_skill
    hydrograph_pentad_stat_effectiveness = hydrograph_pentad_stat_effectiveness.drop(columns=["forecast_skill", "forecast_skill_20"])

    # Get the last pentad in the dataframe forecast_pentad_stat
    current_pentad_effectiveness = forecast_pentad_stat_effectiveness["pentad"].tail(1).values[0]
    # Get the forecast_skill for pentad == current_pentad
    current_forecast_skill_effectiveness = hydrograph_pentad_stat_effectiveness[hydrograph_pentad_stat_effectiveness["pentad"] == current_pentad_effectiveness]

    # Rename the columns
    hydrograph_pentad_stat_effectiveness = hydrograph_pentad_stat_effectiveness.rename(columns={"pentad": "Пентада года",
                                                                    "std_err": "Эффективность [-]"})
    current_forecast_skill_effectiveness = current_forecast_skill_effectiveness.rename(columns={"pentad": "Пентада года",
                                                                    "std_err": "Эффективность [-]"})
    title_effectiveness = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        ", " + _("data from") + " 2005 - "+str(title_date.year)
    forecast_string_effectiveness=_("Average effectiveness: ") + f'{hydrograph_pentad_stat_effectiveness["Эффективность [-]"].mean():.1f}' + " [-]"
    # Make a column plot with the effectiveness of the forecast method for each pentad
    # Plot a green area between y = 0 and y = 0.6
    hv_06a = hv.Area(pd.DataFrame({"x":[0, 72], "y":[0.6, 0.6]}),
                        kdims=["x"], vdims=["y"], label=_("Effectiveness")+" <= 0.6") \
            .opts(alpha=0.1, color="green", line_width=0)
    hv_08a = hv.Area(pd.DataFrame({"x":[0, 72], "y":[0.8, 0.8]}),
                        kdims=["x"], vdims=["y"], label=_("Effectiveness")+" <= 0.8") \
            .opts(alpha=0.1, color="orange", line_width=0)

    hv_empty_effectiveness = hv.Scatter([], [], label=forecast_string_effectiveness) \
        .opts(color="white", size=0.000000001)
    hv_current_forecast_skill_effectiveness = hv.Scatter(
        current_forecast_skill_effectiveness,
        kdims="Пентада года", vdims="Эффективность [-]") \
        .opts(color = "#72D1FA", size=10, tools=['hover'])
    hv_forecast_skill_effectiveness = hv.Curve(
        hydrograph_pentad_stat_effectiveness, kdims="Пентада года",
        vdims="Эффективность [-]") \
        .opts(color = "#307096", interpolation='steps-mid',line_width=1, tools=['hover'])

    p = hv_empty_effectiveness * hv_08a * hv_06a * hv_forecast_skill_effectiveness * hv_current_forecast_skill_effectiveness
    p.opts(responsive=True, hooks=[remove_bokeh_logo, add_custom_xticklabels_pentad],
            xticks=list(range(1,72,6)),
            title=title_effectiveness, shared_axes=False,
            legend_position='bottom_left',  # 'right',
            xlabel=_("Pentad of the month (starting from January 1)"), ylabel=_("Effectiveness")+" [-]",
            show_grid=True, xlim=(0, 72), ylim=(0,1.4),
            fontsize={'legend':8}, fontscale=1.2)

    return p

def plot_forecast_accuracy(station_widget, range_selection_widget, manual_range_widget):

    forecast_pentad_stat_accuracy = forecast_pentad[forecast_pentad["station_labels"] == station_widget]
    hydrograph_pentad_accuracy = hydrograph_pentad_all[hydrograph_pentad_all["station_labels"] == station_widget]
    hydrograph_pentad_stat_accuracy = calculate_pentad_forecast_accuracy(
        hydrograph_pentad_accuracy, forecast_pentad_stat_accuracy,
        range_selection_widget, manual_range_widget)
    dates_collection = get_current_predictor_and_dates(forecast_pentad, station_widget)

    # We need to print a suitable date for the figure titles. We use the last
    # date of fcdata_filtered where .
    title_date = dates_collection.latest_forecast_date
    title_date_str = title_date.strftime('%Y-%m-%d')
    title_pentad = tl.get_pentad(title_date_str)
    title_month = tl.get_month_str_case2(title_date_str)

    # Drop the column forecast_skill
    hydrograph_pentad_stat_accuracy = hydrograph_pentad_stat_accuracy.drop(columns=["std_err"])

    # Get the last pentad in the dataframe forecast_pentad_stat
    current_pentad_accuracy = forecast_pentad_stat_accuracy["pentad"].tail(1).values[0]
    # Get the forecast_skill for pentad == current_pentad
    current_forecast_skill_accuracy = hydrograph_pentad_stat_accuracy[hydrograph_pentad_stat_accuracy["pentad"] == current_pentad_accuracy]

    # Rename the columns
    hydrograph_pentad_stat_accuracy = hydrograph_pentad_stat_accuracy.rename(columns={"pentad": "Пентада года",
                                                                    "forecast_skill": "Оправдываемость 0.674 sigma [%]",
                                                                    "forecast_skill_20": "Оправдываемость x Q [%]"})
    current_forecast_skill_accuracy = current_forecast_skill_accuracy.rename(columns={"pentad": "Пентада года",
                                                                    "forecast_skill": "Оправдываемость 0.674 sigma [%]",
                                                                    "forecast_skill_20": "Оправдываемость x Q [%]"})
    title_accuracy = _("Station ") + str(station_widget) + _(" on ") + title_date_str + \
        ", " + _("data from") + " 2005 - " + str(title_date.year)
    forecast_string_accuracy_sigma=_("Average accuracy")+" ("+_("0.674 sigma")+"): " + f"{hydrograph_pentad_stat_accuracy['Оправдываемость 0.674 sigma [%]'].mean():.1f}" + " [%]"
    forecast_string_accuracy_percent=_("Average accuracy")+" ("+str(manual_range_widget)+"%): " + f"{hydrograph_pentad_stat_accuracy['Оправдываемость x Q [%]'].mean():.1f}" + " [%]"
    # Make a column plot with the forecast skill
    #hv_empty_accuracy = hv.Scatter([], [], label=forecast_string_accuracy) \
    #    .opts(color="white", size=0.000000001)
    hv_current_forecast_skill_accuracy_674 = hv.Scatter(
        current_forecast_skill_accuracy,
        kdims="Пентада года",
        vdims="Оправдываемость 0.674 sigma [%]") \
        .opts(color = "#72D1FA", size=10, tools=['hover'])
    hv_current_forecast_skill_accuracy_02 = hv.Scatter(
        current_forecast_skill_accuracy,
        kdims="Пентада года",
        vdims="Оправдываемость x Q [%]") \
        .opts(color = "#1ec31e", size=10, tools=['hover'])
    #hv_forecast_skill_accuracy_674a = hv.Area(
    #    hydrograph_pentad_stat_accuracy,
    #    kdims="Пентада года",
    #    vdims="Оправдываемость 0.674 sigma [%]") \
    #    .opts(color = "#307096", line_width=1,
    #          tools=['hover'], alpha=0.1)
    hv_forecast_skill_accuracy_674 = hv.Curve(
        hydrograph_pentad_stat_accuracy,
        kdims="Пентада года",
        vdims="Оправдываемость 0.674 sigma [%]",
        label=forecast_string_accuracy_sigma) \
        .opts(color = "#307096", interpolation='steps-mid',line_width=1,
              tools=['hover'])
    hv_forecast_skill_accuracy_02 = hv.Curve(
        hydrograph_pentad_stat_accuracy,
        kdims="Пентада года",
        vdims="Оправдываемость x Q [%]",
        label=forecast_string_accuracy_percent) \
        .opts(color = "#169016", interpolation='steps-mid',line_width=1,
              line_dash="dashed",tools=['hover'])
    p = hv_forecast_skill_accuracy_674 *hv_forecast_skill_accuracy_02 * hv_current_forecast_skill_accuracy_674 * hv_current_forecast_skill_accuracy_02
    p.opts(responsive=True, hooks=[remove_bokeh_logo, add_custom_xticklabels_pentad],
                                   #add_minor_pentad_ticks, add_custom_xticklabels_pentad],
            xticks=list(range(1,72,6)),
            title=title_accuracy, shared_axes=False,
            legend_position='bottom_left',  # 'right',
            xlabel=_("Pentad of the month (starting from January 1)"), ylabel=_("Forecast accuracy")+" [%]",
            show_grid=True, xlim=(0, 72), ylim=(0,100),
            fontsize={'legend':8}, fontscale=1.2)

    return p

# endregion


# region Dashboard layout
daily_hydrograph_plotly = pn.panel(pn.bind(plot_daily_hydrograph_data, station,
                                           forecast_pentad),
                                   min_height=300, sizing_mode='stretch_both')
pentad_forecast = pn.panel(pn.bind(plot_forecast_data, station,
                                   range_selection, manual_range),
                           height=500, sizing_mode='stretch_width')
# If the available forecasts do not overlap with the hydrograph data, we cannot
# calculate forecast accuracy and effectiveness. In this case, we do not show
# the plots.
if no_date_overlap_flag == False:
    pentad_skill = pn.panel(pn.bind(plot_forecast_accuracy, station,
                                            range_selection, manual_range),
                            height=300, sizing_mode='stretch_width')
    pentad_effectiveness = pn.panel(pn.bind(plot_effectiveness_of_forecast_method,
                                            station, range_selection,
                                            manual_range),
                                    height=300, sizing_mode='stretch_width')
else:
    # Append a warning to the warning text pane
    warning_text_pane.object = warning_text_pane.object + "\n\n" + \
    _("Note: Forecast accuracy and effectiveness plots are not available for this station.")


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
if no_date_overlap_flag == False:
    tabs = pn.Tabs(
        # Predictors tab
        (_('Predictors'),
         pn.Column(
             pn.Row(
                 pn.Card(daily_hydrograph_plotly, title=_("Hydrograph")),
             ),
         ),
        ),
        (_('Forecast'),
         pn.Column(
             pn.Row(
                 pn.Card(pentad_forecast, title=_('Forecast')),
             ),
             pn.Row(
                 pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods")),
             ),
             pn.Row(
                 pn.Card(pentad_skill, title=_("Forecast accuracy")),
             ))
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
                 pn.Card(daily_hydrograph_plotly, title=_("Hydrograph")),
             ),
         ),
        ),
        (_('Forecast'),
         pn.Column(
             pn.Row(
                 pn.Card(pentad_forecast, title=_('Forecast')),
            ),
            pn.Row(
                 #pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods")),
            ),
            pn.Row(
                 #pn.Card(pentad_skill, title=_("Forecast accuracy")),
            ))
        ),
        (_('Disclaimer'), footer),
        dynamic=True,
        sizing_mode='stretch_both'
    )

sidebar = pn.Column(
    pn.Row(
        station,
    ),
    pn.Row(
        range_selection,
    ),
    pn.Row(
        manual_range
    ),
    pn.Row(
        pn.Card(warning_text_pane, title=_('Notifications'),
                width_policy='fit', width=station.width),
    ),
)


# Update the widgets conditional on the active tab
tabs.param.watch(update_widgets, 'active')

# Update the warning text depending on the station selection
station.param.watch(update_warning_text, 'value')


dashboard = pn.template.BootstrapTemplate(
    # color of title bar

    title=_('SAPPHIRE Central Asia - Pentadal forecast dashboard'),
    logo=icon_path,
    sidebar=sidebar,
    collapsed_sidebar=False,
    main=tabs,
)


dashboard.servable()

# endregion

# panel serve pentad_dashboard.py --show --autoreload --port 5008