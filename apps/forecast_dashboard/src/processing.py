import os
import sys
import pandas as pd
import numpy as np

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


# --- Configuration -----------------------------------------------------------
def get_icon_path(in_docker_flag):
    # Icon
    #if in_docker_flag == "True":
    #    icon_path = os.path.join("apps", "forecast_dashboard", "www", "Pentad.png")
    #else:
    icon_path = os.path.join("www", "Pentad.png")

    # Test if file exists and thorw an error if not
    if not os.path.isfile(icon_path):
        raise Exception("File not found: " + icon_path)

    return icon_path

# --- Reading data -----------------------------------------------------------
def read_hydrograph_day_file():
    """
    Reads the hydrograph_day file from the intermediate data directory.

    Filters the available data to only include the stations selected for
    pentadal forecasting.

    Returns:
        pd.DataFrame: The hydrograph_day data.

    Raises:
        Exception: If the file is not found or empty.
    """
    hydrograph_day_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_day_file):
        raise Exception("File not found: " + hydrograph_day_file)

    hydrograph_day_all = pd.read_csv(hydrograph_day_file, parse_dates=['date']).reset_index(drop=True)
    # Test if hydrograph_day_all is empty
    if hydrograph_day_all.empty:
        raise Exception("File is empty: " + hydrograph_day_file)

    hydrograph_day_all["day_of_year"] = hydrograph_day_all["day_of_year"].astype(int)
    hydrograph_day_all['code'] = hydrograph_day_all['code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_day_all = hydrograph_day_all.sort_values(by=["code", "day_of_year"])

    return hydrograph_day_all

def read_hydrograph_day_data_for_pentad_forecasting():

    # Read hydrograph data with daily values
    hydrograph_day_all = read_hydrograph_day_file()

    # Get station ids of stations selected for forecasting
    filepath = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection"))
    selected_stations = fl.load_selected_stations_from_json(filepath)

    # Filter data for selected stations
    hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["code"].isin(selected_stations)]

    # Test if there is an environment variable ieasyforecast_restrict_stations_file
    if os.getenv("ieasyforecast_restrict_stations_file"):
        filepath = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_file"))
        # Read the restricted stations from the environment variable
        restricted_stations = fl.load_selected_stations_from_json(filepath)
        # Filter data for restricted stations
        hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["code"].isin(restricted_stations)]

    return hydrograph_day_all

def read_hydrograph_pentad_file():
    hydrograph_pentad_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_pentad_file):
        raise Exception("File not found: " + hydrograph_pentad_file)

    # Read hydrograph data - pentad
    hydrograph_pentad_all = pd.read_csv(hydrograph_pentad_file).reset_index(drop=True)
    # Test if hydrograph_pentad_all is empty
    if hydrograph_pentad_all.empty:
        raise Exception("File is empty: " + hydrograph_pentad_file)

    hydrograph_pentad_all["pentad_in_year"] = hydrograph_pentad_all["pentad_in_year"].astype(int)
    hydrograph_pentad_all['code'] = hydrograph_pentad_all['code'].astype(str)
    hydrograph_pentad_all['date'] = pd.to_datetime(hydrograph_pentad_all['date'], format='%Y-%m-%d')
    # Sort all columns in ascending Code and pentad order
    hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["code", "pentad_in_year"])

    return hydrograph_pentad_all

def read_hydrograph_pentad_data_for_pentad_forecasting():

    # Read hydrograph data with daily values
    hydrograph_pentad_all = read_hydrograph_pentad_file()

    # Get station ids of stations selected for forecasting
    filepath = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_station_selection"))
    selected_stations = fl.load_selected_stations_from_json(filepath)

    # Filter data for selected stations
    hydrograph_pentad_all = hydrograph_pentad_all[hydrograph_pentad_all["code"].isin(selected_stations)]

    # Test if there is an environment variable ieasyforecast_restrict_stations_file
    if os.getenv("ieasyforecast_restrict_stations_file"):
        filepath = os.path.join(
            os.getenv("ieasyforecast_configuration_path"),
            os.getenv("ieasyforecast_restrict_stations_file"))
        # Read the restricted stations from the environment variable
        restricted_stations = fl.load_selected_stations_from_json(filepath)
        # Filter data for restricted stations
        hydrograph_pentad_all = hydrograph_pentad_all[hydrograph_pentad_all["code"].isin(restricted_stations)]

    return hydrograph_pentad_all

def read_linreg_forecast_data():
    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )

    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)

    # Read the file
    linreg_forecast = pd.read_csv(forecast_results_file)

    # Test if linreg_forecast is empty
    if linreg_forecast.empty:
        raise Exception("File is empty: " + forecast_results_file)

    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    linreg_forecast['date'] = pd.to_datetime(linreg_forecast['date'], format='%Y-%m-%d')

    # Date is the forecast issue date. To visualize the data for the correct pentad,
    # we need to add 1 day to the date and re-evaluate the pentad & pentad in year
    linreg_forecast['Date'] = linreg_forecast['date'] + pd.Timedelta(days=1)

    # Convert code column to str
    linreg_forecast['code'] = linreg_forecast['code'].astype(str)

    # We are only interested in the predictor & average discharge here. We drop the other columns.
    linreg_forecast = linreg_forecast[['date', 'pentad_in_year', 'code', 'predictor', 'discharge_avg']]

    return linreg_forecast

def shift_date_by_n_days(linreg_predictor_orig, n=1):
    """
    Shift the date column of the linreg_predictor DataFrame by n days.

    Args:
        linreg_predictor (pd.DataFrame): The linreg_predictor DataFrame.
        n (int): The number of days to shift the date column.

    Returns:
        pd.DataFrame: The linreg_predictor DataFrame with the shifted date column.
    """
    # Make a copy of the input data frame in order not to change linreg_predictor
    linreg_predictor = linreg_predictor_orig.copy()
    linreg_predictor['date'] = linreg_predictor['date'] + pd.Timedelta(days=n)

    # If there is a column pentad_in_yer in the DataFrame, we need to update it as well
    if 'pentad_in_year' in linreg_predictor.columns:
        linreg_predictor['pentad_in_year'] = linreg_predictor['date'].apply(tl.get_pentad_in_year)
        # Cast the pentad_in_year column to int
        linreg_predictor['pentad_in_year'] = linreg_predictor['pentad_in_year'].astype(int)

    # If there are columns date, and possibly others, drop these
    if 'date' in linreg_predictor.columns:
        linreg_predictor = linreg_predictor.drop(columns=['date'])

    # Drop rows with NaN values in predictor or discharge_avg columns
    linreg_predictor = linreg_predictor.dropna(subset=['predictor', 'discharge_avg'])

    return linreg_predictor

def read_forecast_results_file():
    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_combined_forecast_pentad_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)
    # Read forecast results
    forecast_pentad = pd.read_csv(forecast_results_file)
    # Test if forecast_pentad is empty
    if forecast_pentad.empty:
        raise Exception("File is empty: " + forecast_results_file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    forecast_pentad['date'] = pd.to_datetime(forecast_pentad['date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    forecast_pentad['date'] = forecast_pentad['date'].astype('datetime64[ns]')
    # The forecast date is the date on which the forecast was produced. For
    # visualization we need to have the date for which the forecast is valid.
    # We add 1 day to the forecast Date to get the valid date.
    forecast_pentad['Date'] = forecast_pentad['date'] + pd.Timedelta(days=1)
    # Convert the code column to string
    forecast_pentad['code'] = forecast_pentad['code'].astype(str)
    # Check if there are duplicates for Date and code columns. If yes, only keep the
    # last one
    # Print all values where column code is 15102. Sort the values by Date in ascending order.
    # Print the last 20 values.
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last').sort_values('Date')
    # Get the pentad of the year. Refers to the pentad the forecast is produced for.
    forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
    # Cast pentad column no number
    forecast_pentad['pentad'] = forecast_pentad['pentad'].astype(int)
    # Add a year column
    forecast_pentad['year'] = forecast_pentad['Date'].dt.year
    # Drop duplicates in Date, code and model_short columns
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code', 'model_short'], keep='last')

    return forecast_pentad

def read_forecast_stats_file():
    forecast_stats_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentadal_skill_metrics_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_stats_file):
        raise Exception("File not found: " + forecast_stats_file)

    # Read forecast results
    forecast_stats = pd.read_csv(forecast_stats_file)

    # Test if forecast_stats is empty
    if forecast_stats.empty:
        raise Exception("File is empty: " + forecast_stats_file)

    # Make sure code column is a string
    forecast_stats['code'] = forecast_stats['code'].astype(str)

    return forecast_stats

def read_analysis_file():
    file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(file):
        raise Exception("File not found: " + file)
    # Read analysis results
    analysis_pentad = pd.read_csv(file)
    # Test if analysis_pentad is empty
    if analysis_pentad.empty:
        raise Exception("File is empty: " + file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    analysis_pentad['Date'] = pd.to_datetime(analysis_pentad['Date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    analysis_pentad['Date'] = analysis_pentad['Date'].astype('datetime64[ns]')
    # Cast Code column to string
    analysis_pentad['Code'] = analysis_pentad['Code'].astype(str)

    return analysis_pentad

def add_predictor_dates(linreg_predictor, station, date):
    """
    Add start and end days for predictor period and forecast period to the
    linreg_predictor DataFrame.

    Args:
        linreg_predictor (pd.DataFrame): The linreg_predictor DataFrame.
        station (str): The station label.
        date (datetime.date): The date for which the predictor data is needed.

    Returns:
        pd.Series: The predictor data for the station.
    """
    print(f"\n\nDEBUG: add_predictor_dates: station: {station}, date: {date}")
    # Filter the predictor data for the hydropost
    predictor = linreg_predictor[linreg_predictor['station_labels'] == station]

    # Cast date column to datetime
    predictor.loc[:, 'date'] = pd.to_datetime(predictor['date'], format='%Y-%m-%d')

    # Make sure predictor is a dataframe
    if isinstance(predictor, pd.Series):
        predictor = predictor.to_frame()

    # Identify the next lowest date in the predictor data
    predictor = predictor[predictor['date'] <= pd.Timestamp(date)]
    # Get the latest date in the predictor data
    predictor = predictor.sort_values('date').iloc[-1:]

    # Add start and end days for predictor period and forecast period
    predictor['predictor_start_date'] = predictor['date'] - pd.DateOffset(days=2)
    predictor['forecast_start_date'] = predictor['date'] + pd.DateOffset(days=1)
    predictor['forecast_end_date'] = predictor['date'] + pd.DateOffset(days=5)

    print(f"DEBUG: add_predictor_dates: predictor:\n{predictor}")

    # Round the predictor according to common rules
    predictor['predictor'] = fl.round_discharge_to_float(predictor['predictor'].values[0])

    # Test if the forecast_start_date is 26, set the forecast_end_date to the end of the month of the forecast_start_date
    if (predictor['forecast_start_date'].dt.day == 26).any():
        predictor['forecast_end_date'] = predictor['forecast_start_date'] + pd.offsets.MonthEnd(0)

    # Add 23 hours and 58 minutes to the forecast_end_date
    predictor['forecast_end_date'] = predictor['forecast_end_date'] + pd.Timedelta(hours=23, minutes=58)

    # Define predictor end date to be 23 hours plus 58 minutes after for 'date'
    predictor['predictor_end_date'] = predictor['date'] + pd.Timedelta(hours=23, minutes=58)

    # Add a day_of_year column to the predictor DataFrame
    predictor['day_of_year'] = predictor['date'].dt.dayofyear
    predictor['predictor_start_day_of_year'] = float(predictor['predictor_start_date'].dt.dayofyear.iloc[0]) - 0.2
    predictor['predictor_end_day_of_year'] = float(predictor['day_of_year'].iloc[0]) + 0.2
    predictor['forecast_start_day_of_year'] = predictor['forecast_start_date'].dt.dayofyear - 0.1
    predictor['forecast_end_day_of_year'] = predictor['forecast_end_date'].dt.dayofyear + 0.8

    # Test if 'date' is a leap year
    if (predictor['date'].dt.year % 4 != 0).any() or (predictor['date'].dt.year % 100 == 0).any() and (predictor['date'].dt.year % 400 != 0).any():
        predictor['leap_year'] = False
    else:
        predictor['leap_year'] = True

    #if isinstance(predictor, pd.Series):
    #    predictor = predictor.to_frame()

    print(f"\n\n")

    return predictor

def get_month_name_for_number(_, month_number):
    # Make sure month_number is an integer
    month_number = int(month_number)
    # Dictionary with month names in different languages
    month_names = {
        1: _("January"),
        2: _("February"),
        3: _("March"),
        4: _("April"),
        5: _("May"),
        6: _("June"),
        7: _("July"),
        8: _("August"),
        9: _("September"),
        10: _("October"),
        11: _("November"),
        12: _("December")
    }
    return month_names[month_number]

def internationalize_forecast_model_names(_, forecasts_all,
                                          model_long_col='model_long',
                                          model_short_col='model_short'):
    """
    Replaces the strings in model_long and model_short columns with the
    corresponding translations.

    Args:
        forecasts_all (pd.DataFrame): The forecast results DataFrame.
        model_long_col (str): The column name for the long model names.
        model_short_col (str): The column name for the short model names.

    Returns:
        pd.DataFrame: The forecast results DataFrame with translated model names.
    """
    forecasts_all[model_long_col] = forecasts_all[model_long_col].apply(lambda x: _(x))
    forecasts_all[model_short_col] = forecasts_all[model_short_col].apply(lambda x: _(x))
    #print("Inernationalized forecast model names:\n", forecasts_all[model_long_col].unique())
    #print(forecasts_all[model_short_col].unique())

    return forecasts_all

def deprecating_read_hydrograph_day_file(today):

    hydrograph_day_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_day_file):
        raise Exception("File not found: " + hydrograph_day_file)

    hydrograph_day_all = pd.read_csv(hydrograph_day_file).reset_index(drop=True)
    # Test if hydrograph_day_all is empty
    if hydrograph_day_all.empty:
        raise Exception("File is empty: " + hydrograph_day_file)

    hydrograph_day_all["day_of_year"] = hydrograph_day_all["day_of_year"].astype(int)
    hydrograph_day_all['Code'] = hydrograph_day_all['Code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_day_all = hydrograph_day_all.sort_values(by=["Code", "day_of_year"])
    # Remove the day 366 (only if we are not in a leap year)
    # Test if current year is a leap year
    if today.year % 4 != 0:
        hydrograph_day_all = hydrograph_day_all[hydrograph_day_all["day_of_year"] != 366]

    return hydrograph_day_all

def deprecating_read_hydrograph_pentad_file():
    hydrograph_pentad_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))
    # Test if file exists and thorw an error if not
    if not os.path.isfile(hydrograph_pentad_file):
        raise Exception("File not found: " + hydrograph_pentad_file)

    # Read hydrograph data - pentad
    hydrograph_pentad_all = pd.read_csv(hydrograph_pentad_file).reset_index(drop=True)
    # Test if hydrograph_pentad_all is empty
    if hydrograph_pentad_all.empty:
        raise Exception("File is empty: " + hydrograph_pentad_file)

    hydrograph_pentad_all["pentad"] = hydrograph_pentad_all["pentad"].astype(int)
    hydrograph_pentad_all['Code'] = hydrograph_pentad_all['Code'].astype(str)
    # Sort all columns in ascending Code and pentad order
    hydrograph_pentad_all = hydrograph_pentad_all.sort_values(by=["Code", "pentad"])

    return hydrograph_pentad_all

def deprecating_read_forecast_results_file():
    forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentad_results_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(forecast_results_file):
        raise Exception("File not found: " + forecast_results_file)
    # Read forecast results
    forecast_pentad = pd.read_csv(forecast_results_file)
    # Test if forecast_pentad is empty
    if forecast_pentad.empty:
        raise Exception("File is empty: " + forecast_results_file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    forecast_pentad['Date'] = pd.to_datetime(forecast_pentad['date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    forecast_pentad['Date'] = forecast_pentad['Date'].astype('datetime64[ns]')
    # The forecast date is the date on which the forecast was produced. For
    # visualization we need to have the date for which the forecast is valid.
    # We add 1 day to the forecast Date to get the valid date.
    forecast_pentad['Date'] = forecast_pentad['Date'] + pd.Timedelta(days=1)
    # Convert the code column to string
    forecast_pentad['code'] = forecast_pentad['code'].astype(str)
    # Check if there are duplicates for Date and code columns. If yes, only keep the
    # last one
    # Print all values where column code is 15102. Sort the values by Date in ascending order.
    # Print the last 20 values.
    forecast_pentad = forecast_pentad.drop_duplicates(subset=['Date', 'code'], keep='last').sort_values('Date')
    # Get the pentad of the year.
    forecast_pentad = tl.add_pentad_in_year_column(forecast_pentad)
    # Cast pentad column no number
    forecast_pentad['pentad'] = forecast_pentad['pentad'].astype(int)

    return forecast_pentad

def deprecating_read_analysis_file():
    file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_analysis_pentad_file")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(file):
        raise Exception("File not found: " + file)
    # Read analysis results
    analysis_pentad = pd.read_csv(file)
    # Test if analysis_pentad is empty
    if analysis_pentad.empty:
        raise Exception("File is empty: " + file)
    # Convert the date column to datetime. The format of the date string is %Y-%m-%d.
    analysis_pentad['Date'] = pd.to_datetime(analysis_pentad['Date'], format='%Y-%m-%d')
    # Make sure the date column is in datetime64 format
    analysis_pentad['Date'] = analysis_pentad['Date'].astype('datetime64[ns]')
    # Cast Code column to string
    analysis_pentad['Code'] = analysis_pentad['Code'].astype(str)

    return analysis_pentad

def read_all_stations_metadata_from_file(station_list):

    all_stations_file = os.path.join(
        os.getenv("ieasyforecast_configuration_path"),
        os.getenv("ieasyforecast_config_file_all_stations")
    )
    # Test if file exists and thorw an error if not
    if not os.path.isfile(all_stations_file):
        raise Exception("File not found: " + all_stations_file)

    # Read stations json
    all_stations = fl.load_all_station_data_from_JSON(all_stations_file)
    # Convert the code column to string
    all_stations['code'] = all_stations['code'].astype(str)
    # Left-join all_stations['code', 'river_ru', 'punkt_ru'] by 'Code' = 'code'
    station_df=pd.DataFrame(station_list, columns=['code'])
    station_df=station_df.merge(all_stations.loc[:,['code','river_ru','punkt_ru', 'basin']],
                            left_on='code', right_on='code', how='left')

    # Paste together the columns Code, river_ru and punkt_ru to a new column
    # station_labels. river and punkt names are currently only available in Russian.
    station_df['station_labels'] = station_df['code'] + ' - ' + station_df['river_ru'] + ' ' + station_df['punkt_ru']

    # Update the station list with the new station labels
    station_list = station_df['station_labels'].tolist()

    # From station_df, get the columns basin and station_lables and convert to a
    # dictionary where we have unique basins in the key and the station_labels
    # in the values
    station_dict = station_df.groupby('basin')['station_labels'].apply(list).to_dict()

    return station_list, all_stations, station_df, station_dict

def add_labels_to_hydrograph(hydrograph, all_stations):
    hydrograph = hydrograph.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='code', right_on='code', how='left')
    hydrograph['station_labels'] = hydrograph['code'] + ' - ' + hydrograph['river_ru'] + ' ' + hydrograph['punkt_ru']
    # Remove the columns river_ru and punkt_ru
    hydrograph = hydrograph.drop(columns=['code', 'river_ru', 'punkt_ru'])

    return hydrograph

def add_labels_to_forecast_pentad_df(forecast_pentad, all_stations):
    forecast_pentad = forecast_pentad.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
        left_on='code', right_on='code', how='left')
    forecast_pentad['station_labels'] = forecast_pentad['code'] + ' - ' + forecast_pentad['river_ru'] + ' ' + forecast_pentad['punkt_ru']
    forecast_pentad = forecast_pentad.drop(columns=['river_ru', 'punkt_ru'])

    return forecast_pentad

# --- Data processing --------------------------------------------------------
def calculate_forecast_range(_, forecast_table, range_type, range_slider):
    """
    Calculate the forecast range based on the selected range type and the
    range slider value.

    Args:
        _ (gettext.gettext): The gettext function.
        forecast_table (pd.DataFrame): The forecast table.
        range_type (str): The selected range type.
        range_slider (float): The selected range slider value.

    Returns:
        pd.DataFrame: The forecast table with the calculated forecast range.
    """
    if range_type == _('delta'):
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']
    elif range_type == _("Manual range, select value below"):
        forecast_table['fc_lower'] = (1 - range_slider/100.0) * forecast_table['forecasted_discharge']
        forecast_table['fc_upper'] = (1 + range_slider/100.0) * forecast_table['forecasted_discharge']
    elif range_type == _("max[delta, %]"):
        forecast_table['fc_lower'] = np.minimum(forecast_table['forecasted_discharge'] - forecast_table['delta'],
                                                (1 - range_slider/100.0) * forecast_table['forecasted_discharge'])
        forecast_table['fc_upper'] = np.maximum(forecast_table['forecasted_discharge'] + forecast_table['delta'],
                                                (1 + range_slider/100.0) * forecast_table['forecasted_discharge'])
    else:
        forecast_table['fc_lower'] = forecast_table['forecasted_discharge'] - forecast_table['delta']
        forecast_table['fc_upper'] = forecast_table['forecasted_discharge'] + forecast_table['delta']

    return forecast_table

def update_model_dict(model_dict, forecasts_all, selected_station):
    """
    Update the model_dict with the models we have results for for the selected station
    """
    test = forecasts_all[forecasts_all['station_labels'] == selected_station]
    print("DEBUG: update_model_dict: unique models for selected station:\n", test['model_long'].unique())

    model_dict = forecasts_all[forecasts_all['station_labels'] == selected_station] \
        .set_index('model_long')['model_short'].to_dict()
    return model_dict








def add_labels_to_hydrograph_pentad_all(hydrograph_pentad_all, all_stations):
    hydrograph_pentad_all = hydrograph_pentad_all.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
                            left_on='code', right_on='code', how='left')
    hydrograph_pentad_all['station_labels'] = hydrograph_pentad_all['code'] + ' - ' + hydrograph_pentad_all['river_ru'] + ' ' + hydrograph_pentad_all['punkt_ru']
    # Remove the columns river_ru and punkt_ru
    hydrograph_pentad_all = hydrograph_pentad_all.drop(columns=['code', 'river_ru', 'punkt_ru'])

    return hydrograph_pentad_all

def add_labels_to_analysis_pentad_df(analysis_pentad, all_stations):
    analysis_pentad = analysis_pentad.merge(
        all_stations.loc[:,['code','river_ru','punkt_ru']],
        left_on='code', right_on='code', how='left')
    analysis_pentad['station_labels'] = analysis_pentad['code'] + ' - ' + analysis_pentad['river_ru'] + ' ' + analysis_pentad['punkt_ru']
    analysis_pentad = analysis_pentad.drop(columns=['river_ru', 'punkt_ru'])

    return analysis_pentad

def deprecating_preprocess_hydrograph_day_data(hydrograph_day, today):

    print("hydrograph_day:\n", hydrograph_day.head())
    print(hydrograph_day.tail())

    # Test that we only have data for one unique code in the data frame.
    # If we have more than one code, we raise an exception.
    if len(hydrograph_day["code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

    # Drop the Code column
    hydrograph_day = hydrograph_day.drop(columns=["code", "station_labels"])

    # Drop the index
    hydrograph_day = hydrograph_day.reset_index(drop=True)

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
    # For validation of the method, also print minimum and maximum values of the
    # hydrograph_day DataFrame
    min_val = hydrograph_day.groupby("day_of_year")["value"].min()
    max_val = hydrograph_day.groupby("day_of_year")["value"].max()

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "day_of_year": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values,
        "Min": min_val.values,
        "Max": max_val.values
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
        # Print a warning that the current year data is not available and that
        # the previous year data is shown as a fallback
        print("WARNING: No river runoff data available from the current year, " + str(current_year) + ". Showing data from the last year, " + current_year_col + ", data is available.")

    hydrograph_norm_perc["current_year"] = current_year_data

    return hydrograph_norm_perc

def preprocess_hydrograph_pentad_data(hydrograph_pentad: pd.DataFrame, today) -> pd.DataFrame:
    '''
    Calculates the norm and percentiles for each pentad over all Years in the
    input hydrograph. Note that the input hydrograph is filtered to only one
    station.
    '''
    # Test if we only have data for one unique code in the data frame.
    # If we have more than one code, we raise an exception.
    if len(hydrograph_pentad["Code"].unique()) > 1:
        raise ValueError("Data frame contains more than one unique code.")

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
    min_val = hydrograph_pentad.groupby("pentad")["value"].min().reset_index(drop=True)
    max_val = hydrograph_pentad.groupby("pentad")["value"].max().reset_index(drop=True)

    # Create a new DataFrame with the calculated values
    hydrograph_norm_perc = pd.DataFrame({
        "pentad": norm.index,
        "Norm": norm.values,
        "Perc_05": perc_05.values,
        "Perc_25": perc_25.values,
        "Perc_75": perc_75.values,
        "Perc_95": perc_95.values,
        "Min": min_val.values,
        "Max": max_val.values
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

    return hydrograph_norm_perc

def select_analysis_data(analysis_pentad_all, station_widget):
    analysis_pentad = analysis_pentad_all[analysis_pentad_all["station_labels"] == station_widget]

    #print(analysis_pentad.head())
    #print(analysis_pentad.columns)

    # Select columns Year, predictor and discharge_avg
    analysis_pentad = analysis_pentad[
        ["Year", "discharge_sum", "discharge_avg", "accuracy", "sdivsigma",
         "pentad"]]

    # Rename the columns
    analysis_pentad = analysis_pentad.rename(
        columns={"discharge_sum": "Predictor",
                 "discharge_avg": "Q [m3/s]",
                 "accuracy": "Accuracy",
                 "sdivsigma": "Efficiency",
                 "pentad": "Pentad"})

    # Make sure Year is an integer
    analysis_pentad["Year"] = analysis_pentad["Year"].astype(int)

    # Reset and drop index
    analysis_pentad = analysis_pentad.reset_index(drop=True)

    # Set the Year column as index
    analysis_pentad = analysis_pentad.set_index("Year")

    return analysis_pentad

def calculate_norm_stats(analysis_data):
    # Calculate the mean and standard deviation of the predictor and discharge_avg
    norm_stats = pd.DataFrame({
        "Pentad": [analysis_data["Pentad"].mean()],
        "Min": [analysis_data["Q [m3/s]"].min()],
        "Norm": [analysis_data["Q [m3/s]"].mean()],
        "Max": [analysis_data["Q [m3/s]"].max()]
    })
    # Set Pentad as index
    norm_stats = norm_stats.set_index("Pentad")

    return norm_stats

def calculate_fc_stats(analysis_data):
    # Calculate the mean and standard deviation of the predictor and discharge_avg
    fc_stats = pd.DataFrame({
        "Pentad": [analysis_data["Pentad"].mean()],
        "Model": "Lin. reg.",
        "Predictor": [analysis_data["Predictor"].mean()],
        "Forecast": "TODO",
        "±δ": "TODO",
        "Lower": "TODO",
        "Upper": "TODO",
        "%P": [analysis_data["Accuracy"].mean()],
        "s/σ": [analysis_data["Efficiency"].mean()],
    })
    # Set Pentad as index
    fc_stats = fc_stats.set_index("Pentad")

    return fc_stats





# --- Bulletin preparation --------------------------------------------------------
def get_bulletin_header_info(date):
    """Get information from date relevant for the bulletin header."""
    df = pd.DataFrame({
        "pentad": [tl.get_pentad(date)],
        "month_str_nom_ru": [tl.get_month_str_case1(date)],
        "month_str_gen_ru": [tl.get_month_str_case2(date)],
        "year": [tl.get_year(date)],
        "day_start_pentad": [tl.get_pentad_first_day(date)],
        "day_end_pentad": [tl.get_pentad_last_day(date)],
    })

    return df

