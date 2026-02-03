# WHY?
# ----------------------------
# The forecasts are generated over the period of [t+k-H + 1, t+k] 
# where k is the offset and H is the prediction horizon.
# So if the forecast is generated on the 25. of April and it should predict two months ahead (k=65), 
# with a horizon of 30 days (H=30), the forecast will cover the period from 31.May  to 29.June.
# Now the hydromets are interested in the monthly mean values, so we do not precisely cover that period. (i.e., June 1st to June 30th).
# Note: This is the setting for month 2 (which means two months ahead).
# For longer lead times this gets more pronounced. April 25th with k=125 and H=30:
# Forecast period: 30.July to 28.August. (3 days missing off in total) 
# Note: This is for motnh 4 (which means four months ahead).
# ----------------------------
# HOW?
# ----------------------------
# To refer the forecasted values to the calendar months, we do the following post-processing steps:
# 1) For each forecasted time period, we calculate the mean value over the period (day-month) over all years.
# 2) We calculate the ratio to the long term mean of the same period (day-month) over all years.
# 3) We multiply this ratio with the long term mean of the calendar month to get the adjusted forecast for the calendar month.
#
# Importantly: This is done for all other years, then the prediction year (no data leakage in hindcast).
def get_lt_configuration(args):
    operational_issue_day = args.operational_issue_day
    operational_month_lead_time = args.operational_month_lead_time
    raise NotImplementedError

def calculate_lt_statistics_fc_period(args):

    # Acces the discharge and prediction data from the args
    discharge_data = args.discharge_data
    prediction_data = args.prediction_data

    # Based on the valid_from and valid_to columns, calculate the day-month mean values for each year 
    # and then the long term mean over all years for the forecasted period (day-month). Except the year of the prediction (no data leakage in hindcast).
    # This function should work for both the hindcast setting (so multiple years) and the operational setting (only one year, so all historical years are considered).
    # for the aggregation atleas 50% of the days in the period should have non-missing values.
    # This should be done for each code (basin) seperately. But in a fast way

    #TODO: Implement the calculation of the long term statistics for the forecasted period (day-month) over all years, except the year of the prediction (no data leakage in hindcast).
    
    raise NotImplementedError

def calculate_lt_statistics_calendar_month(args):

    # Acces the discharge data from the args
    # aggregate to calendar month long term statistics, for the aggregation atleas 50% of the days in the month should have non-missing values.
    # for each code (basin) calculate , mean, std, min, max, q_25, q_75, number_years over all years except the year of the prediction (no data leakage in hindcast).
    raise NotImplementedError

def map_forecasted_period_to_calendar_month(args):

    # Create a reference table mapping the forecasted period (day-month) to the calendar month long term statistics
    # this is done by accessing the forecast_issue date (date column), xtracting the month and adding + operational_month_lead_time
    # then mapping the forecasted period (day-month) to the calendar month long term statistics

    # output should be a dataframe  with columns: date, code, fc_period_lt_prediction, fc_period_lt_mean, calendar_month_lt_mean, calendar_month_std, etc. 
    raise NotImplementedError

def adjust_forecast_to_calendar_month(args):

    # Get the mapped forecasted period to calendar month
    # 1. Calcualte the log ratio between the fc_period_lt_prediction and fc_period_lt_mean.

    # Depending on the amount of historical data we choose a different strategy to validate the ratio:

    # A) N = 0, leave the raw forecast as is.

    # B) N < 5, Use the min max * 2 bounds. 

    # C) N >= 5, Use student t-distribution to calculate the 95% confidence interval bounds.

    # 2. Multiply the ratio with the calendar_month_lt_mean to get the adjusted forecast for the calendar month.

    # 3. Non Negative values only. - set negative values to Nan.
    
    raise NotImplementedError

def post_process_lt_forecast(args):
    

    raise NotImplementedError

    return adjusted_forecast