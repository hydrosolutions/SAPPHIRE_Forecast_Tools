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
def get_lt_configuration():
    raise NotImplementedError

def calculate_lt_statistics_fc_period():
    raise NotImplementedError

def calculate_lt_statistics_calendar_month():
    raise NotImplementedError

def adjust_forecast_to_calendar_month():
    raise NotImplementedError

def post_process_lt_forecast():
    # 1) Calculate long term statistics for forecasted period
    calculate_lt_statistics_fc_period()

    # 2) Calculate long term statistics for calendar month
    calculate_lt_statistics_calendar_month()

    # 3) Adjust forecast to calendar month
    adjust_forecast_to_calendar_month()
